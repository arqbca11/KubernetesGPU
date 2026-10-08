package crosscheck

// Cross-check of Phase 2 step 3 on the kind cluster `kgpu`: the GPU workers as
// a Deployment (deploy/k8s/worker.yaml). Spec: docs/design/phase2-kubernetes.md
// decisions 1, 2, 5, 6, 16 to 19 and the "Graceful shutdown on SIGTERM" flow;
// Phase 1 decisions 36, 37, 42, 48, 49; roadmap Phase 2 (lifecycle, failure
// injection: "drain the stand-in GPU node").
//
// Black box: kubectl, the scheduler API through a port-forward to
// svc/scheduler, the worker's probe port through a port-forward, and a shell
// inside the worker container. The stack is deploy/k8s rendered into
// kgpu-xcheck. The node drain (last test) also evicts the implementer's
// workers in kgpu; their state is recorded before and after, and the node is
// uncordoned in a cleanup no matter what.
//
// Run: KGPU_KIND=1 go test -count=1 -v -run KindWorkers -timeout 40m ./scheduler/crosscheck/

import (
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestKindWorkers(t *testing.T) {
	if !kindEnabled() {
		t.Skip("KGPU_KIND=1 not set: kind tests need the kgpu cluster and take minutes")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Fatalf("kubectl not on PATH: %v", err)
	}
	gpu := gpuNodeName(t)
	t.Logf("[setup] GPU node %s; implementer's workers in %s: %v", gpu, kOrigNS, origWorkers(t))

	deleteXcheckNS()
	t.Cleanup(deleteXcheckNS)
	// Belt and braces: the drain test uncordons too, but never leave the
	// GPU node cordoned whatever happens.
	t.Cleanup(func() {
		so, se, err := kcRun("", "uncordon", gpu)
		t.Logf("[cleanup] kubectl uncordon %s: err=%v %s%s", gpu, err, strings.TrimSpace(so), strings.TrimSpace(se))
	})

	start := time.Now()
	kcApply(t, renderXcheck(t, 1))
	waitPodsReady(t, kNS, "app=postgres", 1, 4*time.Minute)
	waitPodsReady(t, kNS, "app=scheduler", 1, 4*time.Minute)
	waitWorkers(t, kNS, 2, 4*time.Minute)
	t.Logf("[setup] stack ready in %s after %.1fs", kNS, time.Since(start).Seconds())

	logs := startLogCollector(t)
	api := &kwAPI{pf: forward(t, "svc/scheduler", kPortA)}

	t.Run("WorkersPinnedNamedAndTini", func(t *testing.T) { testWorkersShape(t, api, gpu) })
	t.Run("ScaleToThreeThenBackToTwo", func(t *testing.T) { testScaleThree(t, api, logs, gpu) })
	t.Run("TwoSchedulersReapOnceCompleteOnce", func(t *testing.T) { testTwoReapers(t, api, logs) })
	t.Run("PostgresOutageMidBuild", func(t *testing.T) { testWorkerPGOutage(t, api, logs) })
	t.Run("LivenessFreezeRestartsContainer", func(t *testing.T) { testLivenessFreeze(t, api, logs) })
	t.Run("SIGTERMFinishInsideBudgetReleaseOutside", func(t *testing.T) { testSIGTERMBudget(t, api, logs) })
	t.Run("DrainGPUNode", func(t *testing.T) { testDrainGPUNode(t, api, logs, gpu) })
	t.Run("BuildLogLinesCarryIDs", func(t *testing.T) { testLogLineIDs(t, logs) })
	t.Run("ImplementerWorkersBack", func(t *testing.T) {
		so, _, _ := kcRun("", "uncordon", gpu)
		t.Logf("[k8s] uncordon %s: %s", gpu, strings.TrimSpace(so))
		ps := waitWorkers(t, kOrigNS, 2, 4*time.Minute)
		for _, p := range ps {
			if p.Spec.NodeName != gpu || p.Status.Phase != "Running" {
				t.Errorf("%s worker %v not Running on %s", kOrigNS, p, gpu)
			}
		}
		if u := strings.TrimSpace(kc(t, "get", "node", gpu, "-o", "jsonpath={.spec.unschedulable}")); u == "true" {
			t.Errorf("GPU node %s still cordoned", gpu)
		}
		t.Logf("[check] %s: 2 workers Running and Ready on %s; node schedulable", kOrigNS, gpu)
	})
}

// settle waits for two Ready workers and a pool of exactly their pod names.
func settle(t *testing.T, api *kwAPI) []kPod {
	t.Helper()
	ps := waitWorkers(t, kNS, 2, 4*time.Minute)
	want := strings.Join(podNames(ps), ",")
	poll(t, 30*time.Second, 500*time.Millisecond, "pool equals the 2 worker pods", func() bool {
		w := api.workers(t)
		return w.Pool.LiveWorkers == 2 && strings.Join(w.ids(), ",") == want
	})
	return ps
}

func other(ps []kPod, name string) string {
	for _, p := range ps {
		if p.Metadata.Name != name {
			return p.Metadata.Name
		}
	}
	return ""
}

func podByName(ps []kPod, name string) kPod {
	for _, p := range ps {
		if p.Metadata.Name == name {
			return p
		}
	}
	return kPod{}
}

// ---- 1. shape: pinned to the GPU node, WORKER_ID = pod name, tini, probes ----

// Decisions 5, 16, 17, 18 and worker.yaml: both replicas on the labelled and
// tainted node; the pool lists them under their pod names; PID 1 is tini with
// python as its only child; grace period 30 s; both probes answer 200.
func testWorkersShape(t *testing.T, api *kwAPI, gpu string) {
	ps := settle(t, api)
	w := api.workers(t)
	t.Logf("[api] GET /workers -> live_workers=%d ids=%v", w.Pool.LiveWorkers, w.ids())
	if strings.Join(w.ids(), ",") != strings.Join(podNames(ps), ",") {
		t.Errorf("pool ids %v != pod names %v (decision 17)", w.ids(), podNames(ps))
	}
	for _, p := range ps {
		name := p.Metadata.Name
		if p.Spec.NodeName != gpu {
			t.Errorf("%s on node %s, want GPU node %s (decision 5)", name, p.Spec.NodeName, gpu)
		}
		grace := strings.TrimSpace(kc(t, "get", "pod", name, "-n", kNS, "-o", "jsonpath={.spec.terminationGracePeriodSeconds}"))
		if grace != "30" {
			t.Errorf("%s terminationGracePeriodSeconds=%s, want 30", name, grace)
		}
		envID := strings.TrimSpace(kc(t, "exec", "-n", kNS, name, "-c", "worker", "--", "sh", "-c", "echo $WORKER_ID"))
		if envID != name {
			t.Errorf("%s: WORKER_ID=%q, want the pod name (decision 17)", name, envID)
		}
		pr := procs(t, name)
		var pid1 string
		var pythons []kwProc
		for _, x := range pr {
			if x.PID == 1 {
				pid1 = x.Cmd
			}
			if strings.HasPrefix(x.Cmd, "python") {
				pythons = append(pythons, x)
			}
		}
		t.Logf("[exec %s] node=%s WORKER_ID=%s grace=%ss PID1=%q python=%v", name, p.Spec.NodeName, envID, grace, pid1, pythons)
		if !strings.Contains(strings.Fields(pid1 + " x")[0], "tini") {
			t.Errorf("%s PID 1 is %q, want tini (decision 18)", name, pid1)
		}
		if len(pythons) != 1 || pythons[0].PPID != 1 || pythons[0].PID == 1 {
			t.Errorf("%s: want exactly one python process, child of PID 1; got %v", name, pythons)
		}
		pf := forwardProbe(t, name)
		lc, lb := pf.probe("/livez")
		hc, hb := pf.probe("/healthz")
		t.Logf("[probe %s] /livez -> %d %q; /healthz -> %d %q", name, lc, lb, hc, hb)
		if lc != http.StatusOK || hc != http.StatusOK {
			t.Errorf("%s probes /livez=%d /healthz=%d, want 200/200", name, lc, hc)
		}
		pf.pf.stop()
	}
}

// ---- 2. scale to three and back ----

// Decision 1 (a Deployment of pullers): three replicas all land on the GPU
// node, the pool shows three, three builds are leased at once by three
// distinct pods. Scale-down: the removed pod releases its long build (Phase 1
// decision 37: remaining > budget) and deregisters at once (decision 49),
// faster than WORKER_STALE_AFTER=15s.
func testScaleThree(t *testing.T, api *kwAPI, logs *logCollector, gpu string) {
	settle(t, api)
	kc(t, "scale", "deployment/worker", "-n", kNS, "--replicas=3")
	ps := waitWorkers(t, kNS, 3, 3*time.Minute)
	for _, p := range ps {
		if p.Spec.NodeName != gpu {
			t.Errorf("%s landed on %s, want %s", p.Metadata.Name, p.Spec.NodeName, gpu)
		}
	}
	want := strings.Join(podNames(ps), ",")
	took := poll(t, 30*time.Second, 500*time.Millisecond, "pool of 3", func() bool {
		w := api.workers(t)
		return w.Pool.LiveWorkers == 3 && strings.Join(w.ids(), ",") == want
	})
	t.Logf("[api] pool live_workers=3 with ids %s after %.1fs", want, took.Seconds())

	round := api.round(t, "xc-w-scale3", 10)
	var ids []string
	for s := 1; s <= 3; s++ {
		ids = append(ids, api.submit(t, round, s, 6000000).BuildID)
	}
	owners := map[string]string{}
	poll(t, 30*time.Second, 200*time.Millisecond, "3 builds leased by 3 distinct pods", func() bool {
		owners = map[string]string{}
		seen := map[string]bool{}
		for _, id := range ids {
			b := api.build(t, id)
			if b.State != "leased" || b.Attempt != 1 {
				return false
			}
			owners[id] = b.owner()
			seen[b.owner()] = true
		}
		return len(seen) == 3
	})
	t.Logf("[check] three simultaneous leases: %v", owners)
	for id, o := range owners {
		if !strings.Contains(want, o) {
			t.Errorf("build %s owner %s is not one of the pods %s", id, o, want)
		}
	}

	before := podNames(ps)
	kc(t, "scale", "deployment/worker", "-n", kNS, "--replicas=2")
	t0 := time.Now()
	var removed string
	poll(t, 20*time.Second, 200*time.Millisecond, "a worker pod chosen for removal", func() bool {
		live, term := kwPods(t, kNS, "app=worker")
		if len(term) == 1 {
			removed = term[0]
			return true
		}
		if len(live) == 2 {
			for _, n := range before {
				if !contains(podNames(live), n) {
					removed = n
					return true
				}
			}
		}
		return false
	})
	var relID string
	for id, o := range owners {
		if o == removed {
			relID = id
		}
	}
	t.Logf("[k8s] scaled to 2 at t=0: %s removed; it held build %s", removed, relID)
	var relAt, poolAt time.Duration
	poll(t, 30*time.Second, 200*time.Millisecond, "released build and pool of 2", func() bool {
		if relAt == 0 {
			b := api.build(t, relID)
			if b.Attempt > 1 || b.State == "queued" {
				relAt = time.Since(t0)
				t.Logf("[build t=%.1fs] %s", relAt.Seconds(), b)
			}
		}
		if poolAt == 0 {
			w := api.workers(t)
			if w.Pool.LiveWorkers == 2 && !contains(w.ids(), removed) {
				poolAt = time.Since(t0)
				t.Logf("[api t=%.1fs] pool live_workers=2 ids=%v", poolAt.Seconds(), w.ids())
			}
		}
		return relAt > 0 && poolAt > 0
	})
	if relAt > 5*time.Second {
		t.Errorf("released build %s left %s only after %.1fs; a release should be within a second or two (decision 37)", relID, removed, relAt.Seconds())
	}
	if poolAt > 10*time.Second {
		t.Errorf("pool still counted %s for %.1fs after scale-down; decision 49 deregisters on clean exit (stale window is 15s)", removed, poolAt.Seconds())
	}
	for _, id := range ids {
		b := api.waitBuild(t, id, 2*time.Minute, "done", isDone)
		want := 1
		if id == relID {
			want = 2
		}
		logs.narrate(t, id)
		assertOneCompletion(t, logs, b, want)
		if r := logs.reapLines(id); len(r) != 0 {
			t.Errorf("build %s was reaped (%d WARN lines), want release/finish without lease expiry: %v", id, len(r), r[0].Raw)
		}
	}
	settle(t, api)
}

// ---- 3. two scheduler replicas, two crashed leaseholders ----

// Phase 2 decision 2: with two schedulers each running the reaper, an expired
// lease is requeued exactly once and the build completes exactly once.
// Crash = in-container SIGKILL of python (decision 19).
func testTwoReapers(t *testing.T, api *kwAPI, logs *logCollector) {
	ps := settle(t, api)
	kc(t, "scale", "deployment/scheduler", "-n", kNS, "--replicas=2")
	scheds := waitPodsReady(t, kNS, "app=scheduler", 2, 3*time.Minute)
	sr0 := map[string]int{}
	for _, s := range scheds {
		sr0[s.Metadata.Name] = s.restarts()
	}
	defer func() {
		kc(t, "scale", "deployment/scheduler", "-n", kNS, "--replicas=1")
		waitPodsReady(t, kNS, "app=scheduler", 1, 3*time.Minute)
	}()

	round := api.round(t, "xc-w-two-reapers", 10)
	ids := []string{api.submit(t, round, 1, kwBigN).BuildID, api.submit(t, round, 2, kwBigN).BuildID}
	owners := map[string]string{}
	poll(t, 30*time.Second, 200*time.Millisecond, "both builds leased by distinct workers", func() bool {
		for _, id := range ids {
			b := api.build(t, id)
			if b.State != "leased" || b.Attempt != 1 {
				return false
			}
			owners[id] = b.owner()
		}
		return owners[ids[0]] != owners[ids[1]]
	})
	t.Logf("[check] leased: %v", owners)
	r0 := map[string]int{}
	for _, p := range ps {
		r0[p.Metadata.Name] = p.restarts()
	}
	time.Sleep(2 * time.Second)
	for _, id := range ids {
		signalPython(t, owners[id], "KILL")
	}
	t0 := time.Now()
	for _, id := range ids {
		b := api.waitBuild(t, id, 3*time.Minute, "done after crash", isDone)
		t.Logf("[build t=%.1fs] %s", time.Since(t0).Seconds(), b)
	}
	for _, id := range ids {
		b := api.build(t, id)
		logs.narrate(t, id)
		reaps := logs.reapLines(id)
		var by []string
		for _, r := range reaps {
			by = append(by, fmt.Sprintf("%s:%q@attempt%s", r.Pod, r.str("msg"), r.str("attempt")))
		}
		t.Logf("[check] build %s reaper lines across both schedulers: %v", id, by)
		if len(reaps) != 1 {
			t.Errorf("build %s: %d reaper lines across two schedulers, want exactly 1 (decision 2): %v", id, len(reaps), by)
		}
		claims := logs.workerMsgs(id, "claimed")
		if len(claims) != 2 {
			t.Errorf("build %s: %d claimed lines, want 2 (attempt 1 and 2)", id, len(claims))
		}
		assertOneCompletion(t, logs, b, 2)
	}
	for _, id := range ids {
		p, _ := getPod(t, kNS, owners[id])
		var term string
		if len(p.Status.ContainerStatuses) > 0 && p.Status.ContainerStatuses[0].LastState.Terminated != nil {
			lt := p.Status.ContainerStatuses[0].LastState.Terminated
			term = fmt.Sprintf("exit=%d reason=%s", lt.ExitCode, lt.Reason)
		}
		t.Logf("[k8s] crashed %s: restarts %d -> %d, last state %s", owners[id], r0[owners[id]], p.restarts(), term)
		if p.restarts() != r0[owners[id]]+1 {
			t.Errorf("%s restart count %d -> %d, want +1", owners[id], r0[owners[id]], p.restarts())
		}
	}
	for _, s := range pods(t, kNS, "app=scheduler") {
		if s.restarts() != sr0[s.Metadata.Name] || !s.ready() {
			t.Errorf("scheduler %v changed during the test (restarts were %d)", s, sr0[s.Metadata.Name])
		}
	}
}

// ---- 4. Postgres outage while a worker is mid-build ----

// Decision 16: /healthz 200 only if a heartbeat succeeded within 3 intervals
// (9 s); /livez always 200. Decision 6: readiness flips, liveness never
// restarts. Phase 1 decision 48: the worker survives, and the build completes
// exactly once after Postgres returns.
func testWorkerPGOutage(t *testing.T, api *kwAPI, logs *logCollector) {
	ps := settle(t, api)
	round := api.round(t, "xc-w-pg-outage", 10)
	id := api.submit(t, round, 1, kwBigN).BuildID
	b := api.waitBuild(t, id, 30*time.Second, "leased", func(b xBuild) bool { return b.State == "leased" })
	x := b.owner()
	y := other(ps, x)
	pf := forwardProbe(t, x)
	r0 := map[string]int{x: podByName(ps, x).restarts(), y: podByName(ps, y).restarts()}
	time.Sleep(2 * time.Second)

	kc(t, "scale", "statefulset/postgres", "-n", kNS, "--replicas=0")
	t0 := time.Now()
	t.Logf("[outage] statefulset/postgres scaled to 0 at t=0, build %s mid-build on %s (idle: %s)", id, x, y)
	const hold = 40 * time.Second
	healthCodes, livezCodes := map[int]int{}, map[int]int{}
	var firstBadHealth, firstNotReadyX, firstNotReadyY time.Duration
	var lastOKHealth time.Duration
	var livezBad []string
	for time.Since(t0) < hold {
		el := time.Since(t0)
		hc, hb := pf.probe("/healthz")
		healthCodes[hc]++
		if hc == http.StatusOK {
			lastOKHealth = el
		} else if firstBadHealth == 0 {
			firstBadHealth = el
			t.Logf("[outage t=%.1fs] %s /healthz -> %d %q", el.Seconds(), x, hc, hb)
		}
		lc, lb := pf.probe("/livez")
		livezCodes[lc]++
		if lc != http.StatusOK {
			livezBad = append(livezBad, fmt.Sprintf("t=%.1fs %d %s", el.Seconds(), lc, lb))
		}
		live, _ := kwPods(t, kNS, "app=worker")
		for _, p := range live {
			if p.restarts() != r0[p.Metadata.Name] {
				t.Errorf("[outage t=%.1fs] %s restarted (%d -> %d)", el.Seconds(), p.Metadata.Name, r0[p.Metadata.Name], p.restarts())
				r0[p.Metadata.Name] = p.restarts()
			}
			if !p.ready() {
				if p.Metadata.Name == x && firstNotReadyX == 0 {
					firstNotReadyX = el
					t.Logf("[outage t=%.1fs] %s Ready=False", el.Seconds(), x)
				}
				if p.Metadata.Name == y && firstNotReadyY == 0 {
					firstNotReadyY = el
					t.Logf("[outage t=%.1fs] %s Ready=False", el.Seconds(), y)
				}
			}
		}
		time.Sleep(time.Second)
	}
	t.Logf("[outage] %.0fs: %s /healthz codes %v (last 200 at %.1fs, first non-200 at %.1fs); /livez codes %v; NotReady %s at %.1fs, %s at %.1fs",
		hold.Seconds(), x, healthCodes, lastOKHealth.Seconds(), firstBadHealth.Seconds(), livezCodes, x, firstNotReadyX.Seconds(), y, firstNotReadyY.Seconds())
	if firstBadHealth == 0 {
		t.Errorf("/healthz on %s never failed during a %s outage (decision 16)", x, hold)
	} else if firstBadHealth > 14*time.Second {
		t.Errorf("/healthz first failed at %.1fs; 3 heartbeat intervals is 9s (decision 16)", firstBadHealth.Seconds())
	}
	if healthCodes[http.StatusServiceUnavailable] == 0 {
		t.Errorf("/healthz never returned 503 during the outage; codes %v", healthCodes)
	}
	if lastOKHealth > 14*time.Second {
		t.Errorf("/healthz returned 200 at t=%.1fs into the outage, past 3 heartbeat intervals", lastOKHealth.Seconds())
	}
	if len(livezBad) > 0 {
		t.Errorf("/livez not 200 during the outage (decision 16 says always): %v", livezBad)
	}
	if firstNotReadyX == 0 || firstNotReadyY == 0 {
		t.Errorf("Ready condition did not flip for both workers (x at %.1fs, y at %.1fs)", firstNotReadyX.Seconds(), firstNotReadyY.Seconds())
	}

	kc(t, "scale", "statefulset/postgres", "-n", kNS, "--replicas=1")
	t1 := time.Now()
	waitPodsReady(t, kNS, "app=postgres", 1, 3*time.Minute)
	took := poll(t, 90*time.Second, 500*time.Millisecond, x+" Ready again and /healthz 200", func() bool {
		p, _ := getPod(t, kNS, x)
		c, _ := pf.probe("/healthz")
		return p.ready() && c == http.StatusOK
	})
	t.Logf("[recover] %s Ready with /healthz 200 %.1fs after Postgres was scaled up", x, took.Seconds())
	b = api.waitBuild(t, id, 3*time.Minute, "done after the outage", isDone)
	t.Logf("[recover] build done %.1fs after Postgres was scaled up", time.Since(t1).Seconds())
	logs.narrate(t, id)
	assertOneCompletion(t, logs, b, 0)
	for _, l := range logs.workerMsgs(id, "completed") {
		if l.str("attempt") == "1" {
			t.Logf("[note] attempt 1 completed despite the %s outage", hold)
		}
	}
	for _, n := range []string{x, y} {
		p, _ := getPod(t, kNS, n)
		if p.restarts() != r0[n] {
			t.Errorf("%s restart count changed across the outage: %v", n, p)
		}
	}
	settle(t, api)
}

// ---- 5. liveness failure: freeze python ----

// Decisions 6, 16: liveness catches a hung worker. SIGSTOP on python freezes
// the probe thread; the kubelet must restart the container. The lease path
// recovers the build independently.
func testLivenessFreeze(t *testing.T, api *kwAPI, logs *logCollector) {
	ps := settle(t, api)
	round := api.round(t, "xc-w-freeze", 10)
	id := api.submit(t, round, 1, kwBigN).BuildID
	b := api.waitBuild(t, id, 30*time.Second, "leased", func(b xBuild) bool { return b.State == "leased" })
	x := b.owner()
	y := other(ps, x)
	r0 := podByName(ps, x).restarts()
	pf := forwardProbe(t, x)
	time.Sleep(time.Second)
	signalPython(t, x, "STOP")
	t0 := time.Now()

	var livezFail, reapedAt, poolDropAt, restartAt time.Duration
	last := ""
	for time.Since(t0) < 3*time.Minute {
		el := time.Since(t0)
		if livezFail == 0 {
			if c, body := pf.probe("/livez"); c != http.StatusOK {
				livezFail = el
				t.Logf("[freeze t=%.1fs] %s /livez -> %d %s", el.Seconds(), x, c, body)
			}
		}
		bb := api.build(t, id)
		if s := fmt.Sprintf("%s/%d/%s", bb.State, bb.Attempt, bb.owner()); s != last {
			last = s
			t.Logf("[freeze t=%.1fs] %s", el.Seconds(), bb)
			if bb.Attempt > 1 && reapedAt == 0 {
				reapedAt = el
			}
		}
		if poolDropAt == 0 {
			if w := api.workers(t); !contains(w.ids(), x) || w.Pool.LiveWorkers < 2 {
				poolDropAt = el
				t.Logf("[freeze t=%.1fs] pool live_workers=%d ids=%v", el.Seconds(), w.Pool.LiveWorkers, w.ids())
			}
		}
		p, _ := getPod(t, kNS, x)
		if p.restarts() > r0 {
			restartAt = el
			var lt string
			if cs := p.Status.ContainerStatuses; len(cs) > 0 && cs[0].LastState.Terminated != nil {
				lt = fmt.Sprintf("exit=%d reason=%s", cs[0].LastState.Terminated.ExitCode, cs[0].LastState.Terminated.Reason)
			}
			t.Logf("[freeze t=%.1fs] %s restarted by the kubelet: restarts %d -> %d, last state %s", el.Seconds(), x, r0, p.restarts(), lt)
			break
		}
		time.Sleep(time.Second)
	}
	ev, _, _ := kcRun("", "get", "events", "-n", kNS, "--field-selector", "involvedObject.name="+x,
		"-o", `jsonpath={range .items[*]}{.reason}{": "}{.message}{"\n"}{end}`)
	for _, l := range strings.Split(strings.TrimSpace(ev), "\n") {
		if strings.Contains(l, "Unhealthy") || strings.Contains(l, "Killing") || strings.Contains(l, "liveness") {
			t.Logf("[event %s] %s", x, l)
		}
	}
	t.Logf("[freeze] livez failed at %.1fs, build moved off %s at %.1fs, pool dropped it at %.1fs, container restarted at %.1fs",
		livezFail.Seconds(), x, reapedAt.Seconds(), poolDropAt.Seconds(), restartAt.Seconds())
	if livezFail == 0 {
		t.Errorf("/livez kept answering with python stopped")
	}
	if restartAt == 0 {
		t.Errorf("kubelet did not restart %s within 3 minutes of the freeze (liveness: period 10s x 3)", x)
	}
	b = api.waitBuild(t, id, 3*time.Minute, "done", isDone)
	logs.narrate(t, id)
	if reaps := logs.reapLines(id); len(reaps) != 1 {
		t.Errorf("build %s: %d reaper lines, want exactly 1 (lease expiry while frozen)", id, len(reaps))
	}
	assertOneCompletion(t, logs, b, 2)
	if c := logs.workerMsgs(id, "completed"); len(c) == 1 {
		t.Logf("[check] build %s completed by %s (frozen: %s, other worker: %s)", id, c[0].Pod, x, y)
	}
	settle(t, api)
}

// ---- 6. SIGTERM inside and outside the finish budget ----

// Phase 1 decision 37 with SHUTDOWN_FINISH_BUDGET_SECONDS=5, flow "Graceful
// shutdown on SIGTERM": a build with < 5 s left is finished, not released, and
// the pod exits well inside the 30 s grace period; a build with > 5 s left is
// released at once (no lease expiry) and the other worker takes it.
func testSIGTERMBudget(t *testing.T, api *kwAPI, logs *logCollector) {
	settle(t, api)
	round := api.round(t, "xc-w-sigterm", 10)

	// A: 2M build (~6.55 s); SIGTERM at ~3.5 s elapsed, ~3 s left.
	id := api.submit(t, round, 1, kwSmallN).BuildID
	b := api.waitBuild(t, id, 30*time.Second, "leased", func(b xBuild) bool { return b.State == "leased" })
	claimSeen := time.Now()
	x := b.owner()
	time.Sleep(3500*time.Millisecond - time.Since(claimSeen))
	kc(t, "delete", "pod", x, "-n", kNS, "--wait=false")
	t0 := time.Now()
	t.Logf("[sigterm A] deleted %s ~3.5s into a 6.55s build (~3s left, budget 5s)", x)
	gone := poll(t, 45*time.Second, 200*time.Millisecond, x+" gone", func() bool { _, ok := getPod(t, kNS, x); return !ok })
	b = api.waitBuild(t, id, 30*time.Second, "done", isDone)
	t.Logf("[sigterm A] %s gone %.1fs after delete; %s", x, gone.Seconds(), b)
	logs.narrate(t, id)
	assertOneCompletion(t, logs, b, 1)
	if c := logs.workerMsgs(id, "completed"); len(c) == 1 && c[0].Pod != x {
		t.Errorf("build %s completed by %s, want the terminating pod %s (finish path)", id, c[0].Pod, x)
	}
	if gone > 20*time.Second {
		t.Errorf("%s took %.1fs to exit; the finish path needs ~3s, the grace period is 30s", x, gone.Seconds())
	}
	if r := logs.reapLines(id); len(r) > 0 {
		t.Errorf("build %s reaped on the finish path: %v", id, r[0].Raw)
	}

	// B: 4M build (~13.4 s); SIGTERM at ~6 s elapsed, ~7.4 s left > 5 s.
	ps := settle(t, api)
	id2 := api.submit(t, round, 2, kwMidN).BuildID
	b2 := api.waitBuild(t, id2, 30*time.Second, "leased", func(b xBuild) bool { return b.State == "leased" })
	claimSeen = time.Now()
	x2 := b2.owner()
	time.Sleep(6*time.Second - time.Since(claimSeen))
	kc(t, "delete", "pod", x2, "-n", kNS, "--wait=false")
	t1 := time.Now()
	t.Logf("[sigterm B] deleted %s ~6s into a 13.4s build (~7.4s left, budget 5s); idle worker %s", x2, other(ps, x2))
	left := api.waitBuild(t, id2, 20*time.Second, "off the terminating pod", func(b xBuild) bool {
		return b.State == "queued" || b.Attempt > 1
	})
	leftAt := time.Since(t1)
	t.Logf("[sigterm B] build left %s %.1fs after delete: %s", x2, leftAt.Seconds(), left)
	if leftAt > 4*time.Second {
		t.Errorf("build %s left %s only after %.1fs; a release is immediate, a lease expiry is up to 10s", id2, x2, leftAt.Seconds())
	}
	b2 = api.waitBuild(t, id2, time.Minute, "done", isDone)
	logs.narrate(t, id2)
	assertOneCompletion(t, logs, b2, 2)
	if r := logs.reapLines(id2); len(r) > 0 {
		t.Errorf("build %s reaped, want released: %v", id2, r[0].Raw)
	}
	_ = t0
	settle(t, api)
}

// ---- 7. drain the GPU node ----

// Roadmap Phase 2 failure injection: drain the stand-in GPU node. Both workers
// are evicted gracefully (release path: long builds, decision 37), the pool
// empties (decision 49), the queue grows with nothing to claim it, and after
// uncordon the workers return to the GPU node and drain the queue, every
// build completed once.
func testDrainGPUNode(t *testing.T, api *kwAPI, logs *logCollector, gpu string) {
	settle(t, api)
	origPre := origWorkers(t)
	t.Logf("[drain] %s workers before: %v", kOrigNS, origPre)
	t.Cleanup(func() {
		so, se, err := kcRun("", "uncordon", gpu)
		t.Logf("[cleanup] uncordon %s: err=%v %s%s", gpu, err, strings.TrimSpace(so), strings.TrimSpace(se))
	})

	round := api.round(t, "xc-w-drain", 10)
	long := []string{api.submit(t, round, 1, kwBigN).BuildID, api.submit(t, round, 2, kwBigN).BuildID}
	owners := map[string]string{}
	poll(t, 30*time.Second, 200*time.Millisecond, "both long builds leased", func() bool {
		for _, id := range long {
			b := api.build(t, id)
			if b.State != "leased" {
				return false
			}
			owners[id] = b.owner()
		}
		return owners[long[0]] != owners[long[1]]
	})
	time.Sleep(2 * time.Second)
	t0 := time.Now()
	so, se, err := kcRun("", "drain", gpu, "--ignore-daemonsets", "--delete-emptydir-data", "--timeout=150s")
	drainTook := time.Since(t0)
	t.Logf("[drain] kubectl drain %s took %.1fs err=%v\n%s%s", gpu, drainTook.Seconds(), err, so, se)
	if err != nil {
		t.Errorf("kubectl drain failed: %v", err)
	}
	for _, id := range long {
		b := api.build(t, id)
		t.Logf("[drain t=%.1fs] %s (held by %s)", time.Since(t0).Seconds(), b, owners[id])
		if b.State != "queued" || b.Attempt != 1 || b.LeaseOwner != nil {
			t.Errorf("build %s after drain: %s; want queued, attempt 1, no owner (released)", id, b)
		}
	}
	poolAt := poll(t, 20*time.Second, 300*time.Millisecond, "pool empty", func() bool {
		return api.workers(t).Pool.LiveWorkers == 0
	})
	t.Logf("[drain] pool live_workers=0 %.1fs after drain returned", poolAt.Seconds())
	if poolAt > 5*time.Second {
		t.Errorf("pool counted evicted workers for %.1fs after drain (decision 49)", poolAt.Seconds())
	}

	all := append([]string{}, long...)
	for s := 3; s <= 5; s++ {
		all = append(all, api.submit(t, round, s, kwSmallN).BuildID)
	}
	obs := time.Now()
	for time.Since(obs) < 20*time.Second {
		for _, id := range all {
			b := api.build(t, id)
			if b.State != "queued" {
				t.Errorf("[drained t=%.1fs] build %s is %s while no worker can run", time.Since(obs).Seconds(), id, b)
			}
		}
		time.Sleep(2 * time.Second)
	}
	live, _ := kwPods(t, kNS, "app=worker")
	for _, p := range live {
		t.Logf("[drained] %s worker %v; FailedScheduling: %s", kNS, p, schedulingEvents(t, p.Metadata.Name))
		if p.Status.Phase != "Pending" {
			t.Errorf("replacement %s is %s while the GPU node is cordoned, want Pending", p.Metadata.Name, p.Status.Phase)
		}
	}
	t.Logf("[drained] %s workers: %v", kOrigNS, origWorkers(t))
	t.Logf("[drained] all %d builds still queued after 20s; pool %d", len(all), api.workers(t).Pool.LiveWorkers)

	kc(t, "uncordon", gpu)
	t1 := time.Now()
	ps := waitWorkers(t, kNS, 2, 3*time.Minute)
	for _, p := range ps {
		if p.Spec.NodeName != gpu {
			t.Errorf("%s returned on %s, want %s", p.Metadata.Name, p.Spec.NodeName, gpu)
		}
	}
	for _, id := range all {
		b := api.waitBuild(t, id, 3*time.Minute, "done", isDone)
		want := 1
		if id == long[0] || id == long[1] {
			want = 2
		}
		logs.narrate(t, id)
		assertOneCompletion(t, logs, b, want)
		if r := logs.reapLines(id); len(r) > 0 {
			t.Errorf("build %s reaped (lease expiry), want only release: %v", id, r[0].Raw)
		}
	}
	t.Logf("[drain] queue of %d drained %.1fs after uncordon", len(all), time.Since(t1).Seconds())
	orig := waitWorkers(t, kOrigNS, 2, 3*time.Minute)
	t.Logf("[drain] %s workers after: %v", kOrigNS, orig)
}

// ---- 8. log lines ----

// CLAUDE.md conventions: every log line about a build carries build_id,
// attempt and round_id.
func testLogLineIDs(t *testing.T, logs *logCollector) {
	var n int
	bad := map[string]int{}
	var ex []string
	for _, l := range logs.all() {
		if l.str("build_id") == "" {
			continue
		}
		n++
		_, hasA := l.M["attempt"]
		_, hasR := l.M["round_id"]
		if !hasA || !hasR {
			k := l.Pod[:strings.Index(l.Pod, "-")] + ":" + l.str("msg")
			if bad[k] == 0 {
				ex = append(ex, l.Pod+" "+l.Raw)
			}
			bad[k]++
		}
	}
	t.Logf("[check] %d collected log lines carry build_id", n)
	if len(bad) > 0 {
		t.Errorf("log lines with build_id but missing attempt or round_id, by component:msg: %v\nexamples:\n%s", bad, strings.Join(ex, "\n"))
	}
}
