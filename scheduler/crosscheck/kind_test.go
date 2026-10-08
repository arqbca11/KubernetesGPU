package crosscheck

// Cross-check of Phase 2 steps 1 and 2 on the kind cluster `kgpu`
// (docs/design/phase2-kubernetes.md, decisions 2, 5, 6, 9 to 14).
//
// Black box: kubectl, the scheduler's HTTP API through port-forward, psql in
// Postgres, and (SIGKILL only) docker exec into the kind node. The stack under
// test is deploy/k8s rendered into namespace kgpu-xcheck; namespace kgpu is
// only read, and is compared before and after.
//
// Run: KGPU_KIND=1 go test -count=1 -v -run Kind -timeout 40m ./scheduler/crosscheck/

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestKind(t *testing.T) {
	if !kindEnabled() {
		t.Skip("KGPU_KIND=1 not set: kind tests need the kgpu cluster and take minutes")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Fatalf("kubectl not on PATH: %v", err)
	}
	before := snapshotOrig(t)
	t.Logf("[setup] namespace %s snapshot (read only): %d objects", kOrigNS, len(before))

	// A leftover namespace from an aborted run would skew every test.
	deleteXcheckNS()
	t.Cleanup(deleteXcheckNS)

	// Deploy with TWO scheduler replicas from the very start, against an
	// empty volume: both replicas race to migrate (Phase 1 decision 11,
	// Phase 2 decision 2).
	rendered := renderXcheck(t, 2)
	start := time.Now()
	kcApply(t, rendered)
	waitPodsReady(t, kNS, "app=postgres", 1, 4*time.Minute)
	waitPodsReady(t, kNS, "app=scheduler", 2, 4*time.Minute)
	t.Logf("[setup] stack ready in %s after %.1fs", kNS, time.Since(start).Seconds())

	t.Run("GPUNodeShape", testKindGPUNodeShape)
	t.Run("GPUScheduling", testKindGPUScheduling)
	t.Run("GPUResourceRequestPending", testKindGPUResourceRequest)
	t.Run("TwoReplicasMigrateAndServe", testKindTwoReplicas)
	t.Run("DNSAndServices", testKindDNS)
	t.Run("ProcessKillAndPodReplace", testKindKill)
	t.Run("PostgresOutageReadinessNotLiveness", func(t *testing.T) { testKindOutage(t) })
	t.Run("StatefulSetDeletedClaimSurvives", func(t *testing.T) { testKindStatefulSetRecreate(t, rendered) })

	t.Run("OriginalNamespaceUntouched", func(t *testing.T) {
		after := snapshotOrig(t)
		var diffs []string
		for k, v := range before {
			if after[k] != v {
				diffs = append(diffs, fmt.Sprintf("%s: before %q after %q", k, v, after[k]))
			}
		}
		for k, v := range after {
			if _, ok := before[k]; !ok {
				diffs = append(diffs, fmt.Sprintf("%s: new object %q", k, v))
			}
		}
		if len(diffs) > 0 {
			t.Errorf("namespace %s changed during the run:\n%s", kOrigNS, strings.Join(diffs, "\n"))
		}
		t.Logf("[check] namespace %s: %d objects, same UIDs, generations and restart counts as before the run", kOrigNS, len(after))
	})
}

// ---- step 1: the cluster ----

type kNode struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Taints []struct {
			Key    string `json:"key"`
			Value  string `json:"value"`
			Effect string `json:"effect"`
		} `json:"taints"`
	} `json:"spec"`
	Status struct {
		Allocatable map[string]string `json:"allocatable"`
	} `json:"status"`
}

func nodes(t *testing.T) []kNode {
	t.Helper()
	var list struct {
		Items []kNode `json:"items"`
	}
	if err := json.Unmarshal([]byte(kc(t, "get", "nodes", "-o", "json")), &list); err != nil {
		t.Fatal(err)
	}
	return list.Items
}

func gpuNodeName(t *testing.T) string {
	t.Helper()
	var names []string
	for _, n := range nodes(t) {
		if n.Metadata.Labels[kGPUKey] == "true" {
			names = append(names, n.Metadata.Name)
		}
	}
	if len(names) != 1 {
		t.Fatalf("want exactly one node labelled %s=true, got %v", kGPUKey, names)
	}
	return names[0]
}

// Decision 9: one control plane, two general workers, one node labelled
// kgpu.io/gpu=true and tainted kgpu.io/gpu=true:NoSchedule; nothing else tainted
// with that key.
func testKindGPUNodeShape(t *testing.T) {
	ns := nodes(t)
	var cp, gpu, general []string
	for _, n := range ns {
		var taints []string
		hasGPUTaint := false
		for _, tn := range n.Spec.Taints {
			taints = append(taints, fmt.Sprintf("%s=%s:%s", tn.Key, tn.Value, tn.Effect))
			if tn.Key == kGPUKey {
				hasGPUTaint = true
				if tn.Value != "true" || tn.Effect != "NoSchedule" {
					t.Errorf("node %s: taint %s=%s:%s, spec says %s=true:NoSchedule", n.Metadata.Name, tn.Key, tn.Value, tn.Effect, kGPUKey)
				}
			}
		}
		_, isCP := n.Metadata.Labels["node-role.kubernetes.io/control-plane"]
		gpuLabel := n.Metadata.Labels[kGPUKey]
		t.Logf("[node] %s control-plane=%v %s=%q taints=%v allocatable gpu=%q",
			n.Metadata.Name, isCP, kGPUKey, gpuLabel, taints, n.Status.Allocatable["nvidia.com/gpu"])
		switch {
		case isCP:
			cp = append(cp, n.Metadata.Name)
			if hasGPUTaint || gpuLabel != "" {
				t.Errorf("control plane %s carries the GPU label or taint", n.Metadata.Name)
			}
		case gpuLabel == "true":
			gpu = append(gpu, n.Metadata.Name)
			if !hasGPUTaint {
				t.Errorf("GPU node %s is labelled but NOT tainted: anything could land there", n.Metadata.Name)
			}
		default:
			general = append(general, n.Metadata.Name)
			if hasGPUTaint {
				t.Errorf("general node %s carries the GPU taint", n.Metadata.Name)
			}
			for _, tn := range n.Spec.Taints {
				t.Errorf("general node %s has an unexpected taint %s:%s", n.Metadata.Name, tn.Key, tn.Effect)
			}
		}
	}
	if len(cp) != 1 || len(gpu) != 1 || len(general) != 2 {
		t.Errorf("cluster shape: control-plane=%v gpu=%v general=%v; decision 9 says 1/1/2", cp, gpu, general)
	}
}

func pausePod(name string, labels map[string]string, extraSpec string) string {
	var lb strings.Builder
	for k, v := range labels {
		fmt.Fprintf(&lb, "    %s: %q\n", k, v)
	}
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
%s    xcheck: sched
spec:
  terminationGracePeriodSeconds: 0
  containers:
    - name: pause
      image: %s
      imagePullPolicy: IfNotPresent
%s`, name, kNS, lb.String(), kPauseImg, extraSpec)
}

const workerShape = `  nodeSelector:
    kgpu.io/gpu: "true"
  tolerations:
    - key: kgpu.io/gpu
      operator: Equal
      value: "true"
      effect: NoSchedule
`

// Decision 5 and 10: plain pods avoid the GPU node; the worker shape
// (toleration + selector) lands on it; the selector alone stays Pending with
// the taint cited. Also observes (does not assert) a toleration-only pod.
func testKindGPUScheduling(t *testing.T) {
	gpu := gpuNodeName(t)
	var manifests []string
	for i := 0; i < 8; i++ {
		manifests = append(manifests, pausePod(fmt.Sprintf("plain-%d", i), map[string]string{"shape": "plain"}, ""))
	}
	manifests = append(manifests, pausePod("worker-shaped", map[string]string{"shape": "worker"}, workerShape))
	manifests = append(manifests, pausePod("selector-only", map[string]string{"shape": "selector"},
		"  nodeSelector:\n    kgpu.io/gpu: \"true\"\n"))
	for i := 0; i < 6; i++ {
		manifests = append(manifests, pausePod(fmt.Sprintf("toleration-only-%d", i), map[string]string{"shape": "toleration"},
			"  tolerations:\n    - key: kgpu.io/gpu\n      operator: Equal\n      value: \"true\"\n      effect: NoSchedule\n"))
	}
	kcApply(t, strings.Join(manifests, "\n---\n"))
	defer kcRun("", "delete", "pods", "-n", kNS, "-l", "xcheck=sched", "--wait=false")

	ps := waitPodsReady(t, kNS, "shape=plain", 8, 2*time.Minute)
	spread := map[string]int{}
	for _, p := range ps {
		spread[p.Spec.NodeName]++
		if p.Spec.NodeName == gpu {
			t.Errorf("plain pod %s landed on the GPU node %s (decision 5: nothing but workers may land there)", p.Metadata.Name, gpu)
		}
	}
	t.Logf("[check] 8 plain pods spread %v; none on %s", spread, gpu)

	// The stack's own pods are not workers either.
	for _, sel := range []string{"app=postgres", "app=scheduler"} {
		for _, p := range pods(t, kNS, sel) {
			if p.Spec.NodeName == gpu {
				t.Errorf("%s pod %s runs on the GPU node", sel, p.Metadata.Name)
			}
			t.Logf("[check] %s pod %s on %s (not the GPU node)", sel, p.Metadata.Name, p.Spec.NodeName)
		}
	}

	w := waitPodsReady(t, kNS, "shape=worker", 1, 2*time.Minute)[0]
	if w.Spec.NodeName != gpu {
		t.Errorf("worker-shaped pod landed on %s, want the GPU node %s", w.Spec.NodeName, gpu)
	} else {
		t.Logf("[check] worker-shaped pod (selector + toleration) landed on %s", gpu)
	}

	// Selector-only: must stay Pending, and the event must cite the taint.
	var ev string
	poll(t, 60*time.Second, time.Second, "FailedScheduling event for selector-only", func() bool {
		ev = schedulingEvents(t, "selector-only")
		return ev != ""
	})
	time.Sleep(10 * time.Second) // give it time to (wrongly) be scheduled
	p, _ := getPod(t, kNS, "selector-only")
	t.Logf("[check] selector-only after >10s: %v; FailedScheduling: %s", p, ev)
	if p.Status.Phase != "Pending" || p.Spec.NodeName != "" {
		t.Errorf("selector-only pod was scheduled: %v", p)
	}
	if !strings.Contains(ev, "untolerated taint") {
		t.Errorf("FailedScheduling for selector-only does not cite the taint: %q", ev)
	}

	// Toleration-only: observation for the report, not a spec assertion.
	tp := waitPodsReady(t, kNS, "shape=toleration", 6, 2*time.Minute)
	tspread := map[string]int{}
	for _, p := range tp {
		tspread[p.Spec.NodeName]++
	}
	t.Logf("[observe] 6 toleration-only pods (no nodeSelector) spread %v; %d on the GPU node. The taint keeps out pods "+
		"without the toleration; only the worker's own nodeSelector keeps it in. Any pod that copies the toleration can land on %s.",
		tspread, tspread[gpu], gpu)
}

// Decision 5 / Phase 5 note: "the same manifests add a GPU resource request".
// kind has no nvidia.com/gpu resource, so the worker shape plus a GPU request
// must stay Pending with Insufficient nvidia.com/gpu.
func testKindGPUResourceRequest(t *testing.T) {
	gpu := gpuNodeName(t)
	for _, n := range nodes(t) {
		if n.Metadata.Name == gpu {
			t.Logf("[node] %s allocatable: %v", gpu, n.Status.Allocatable)
			if v, ok := n.Status.Allocatable["nvidia.com/gpu"]; ok {
				t.Logf("[observe] GPU node advertises nvidia.com/gpu=%s; this test assumes it does not", v)
			}
		}
	}
	// The resources block must sit under the container, so append it there.
	m := pausePod("worker-gpu-request", map[string]string{"shape": "gpureq"}, "")
	m = strings.TrimRight(m, "\n") + "\n      resources:\n        limits:\n          nvidia.com/gpu: 1\n" + workerShape
	kcApply(t, m)
	defer kcRun("", "delete", "pod", "worker-gpu-request", "-n", kNS, "--wait=false")
	var ev string
	poll(t, 60*time.Second, time.Second, "FailedScheduling for worker-gpu-request", func() bool {
		ev = schedulingEvents(t, "worker-gpu-request")
		return ev != ""
	})
	time.Sleep(10 * time.Second)
	p, _ := getPod(t, kNS, "worker-gpu-request")
	t.Logf("[check] worker shape + nvidia.com/gpu: 1 after >10s: %v; FailedScheduling: %s", p, ev)
	if p.Status.Phase != "Pending" || p.Spec.NodeName != "" {
		t.Errorf("pod requesting nvidia.com/gpu was scheduled on kind: %v", p)
	}
	if !strings.Contains(ev, "Insufficient nvidia.com/gpu") {
		t.Errorf("event does not cite Insufficient nvidia.com/gpu: %q", ev)
	}
	t.Logf("[note] Phase 5 difference: on a real GPU pool the device plugin advertises nvidia.com/gpu and this exact pod schedules; on kind it cannot")
}

// ---- step 2: Postgres and the scheduler ----

func migrationRows(t *testing.T) []string {
	t.Helper()
	out := psql(t, "SELECT * FROM schema_migrations ORDER BY 1")
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

func migrationFiles(t *testing.T) int {
	t.Helper()
	out, err := exec.Command("sh", "-c", "ls "+xcRepoRoot()+"/db/migrations/*.sql | wc -l").Output()
	if err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	return n
}

var migrateRE = regexp.MustCompile(`(?i)migrat`)

// checkMigrations asserts one row per migration file, no duplicate versions.
func checkMigrations(t *testing.T, label string) {
	t.Helper()
	rows := migrationRows(t)
	want := migrationFiles(t)
	seen := map[string]bool{}
	for _, r := range rows {
		v := strings.SplitN(r, "|", 2)[0]
		if seen[v] {
			t.Errorf("%s: migration %s recorded twice", label, v)
		}
		seen[v] = true
	}
	t.Logf("[db] %s: schema_migrations has %d rows (want %d): %v", label, len(rows), want, rows)
	if len(rows) != want {
		t.Errorf("%s: schema_migrations has %d rows, want %d", label, len(rows), want)
	}
	tables := psql(t, "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname='public'")
	t.Logf("[db] %s: tables %s", label, tables)
}

// Phase 1 decision 11 and Phase 2 decision 2: two replicas starting together
// migrate safely (advisory lock) and both serve the API.
func testKindTwoReplicas(t *testing.T) {
	// Round 0: the setup race (both replicas started with Postgres not yet up).
	ps := pods(t, kNS, "app=scheduler")
	if len(ps) != 2 {
		t.Fatalf("want 2 scheduler pods, got %v", ps)
	}
	for _, p := range ps {
		ml := grepLines(podLogs(t, p.Metadata.Name, false), migrateRE)
		t.Logf("[log %s] migration lines: %v", p.Metadata.Name, ml)
		if p.restarts() != 0 {
			t.Errorf("scheduler pod %s restarted %d time(s) during startup race (decision 7: retry, don't crash); previous logs:\n%s",
				p.Metadata.Name, p.restarts(), tail(podLogs(t, p.Metadata.Name, true), 10))
		}
	}
	checkMigrations(t, "after the startup race")

	// Three more fresh races with Postgres already up: empty the schema, then
	// start both replicas at once.
	for i := 1; i <= 3; i++ {
		kc(t, "scale", "deployment/scheduler", "-n", kNS, "--replicas=0")
		poll(t, 60*time.Second, time.Second, "scheduler pods gone", func() bool { return len(pods(t, kNS, "app=scheduler")) == 0 })
		psql(t, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
		t.Logf("[race %d] schema emptied; scaling scheduler 0 -> 2", i)
		kc(t, "scale", "deployment/scheduler", "-n", kNS, "--replicas=2")
		ps = waitPodsReady(t, kNS, "app=scheduler", 2, 2*time.Minute)
		for _, p := range ps {
			if p.restarts() != 0 {
				t.Errorf("race %d: pod %s restarted %d times; previous logs:\n%s", i, p.Metadata.Name, p.restarts(),
					tail(podLogs(t, p.Metadata.Name, true), 10))
			}
			logs := podLogs(t, p.Metadata.Name, false)
			t.Logf("[race %d log %s] %v", i, p.Metadata.Name, grepLines(logs, migrateRE))
			if errs := grepLines(logs, regexp.MustCompile(`"level":"ERROR"`)); len(errs) > 0 {
				t.Errorf("race %d: pod %s logged errors while migrating: %v", i, p.Metadata.Name, errs)
			}
		}
		checkMigrations(t, fmt.Sprintf("race %d", i))
	}

	// The lock itself: hold advisory lock 0x4B475055 (Phase 1 doc, step 1:
	// "holds advisory lock 0x4B475055 for the run") from a psql session, start
	// both replicas on an empty schema, and check neither migrates nor turns
	// Ready until the session ends.
	kc(t, "scale", "deployment/scheduler", "-n", kNS, "--replicas=0")
	poll(t, 60*time.Second, time.Second, "scheduler pods gone", func() bool { return len(pods(t, kNS, "app=scheduler")) == 0 })
	psql(t, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	const lockKey = 0x4B475055
	const holdFor = 25
	holder := exec.Command("kubectl", "--context", kContext, "exec", "-n", kNS, "postgres-0", "-c", "postgres", "--",
		"psql", "-U", "postgres", "-d", "kgpu", "-At", "-c",
		fmt.Sprintf("SELECT pg_advisory_lock(%d); SELECT pg_sleep(%d); SELECT pg_advisory_unlock(%d);", lockKey, holdFor, lockKey))
	holderOut := &syncBuffer{}
	holder.Stdout, holder.Stderr = holderOut, holderOut
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	held := time.Now()
	poll(t, 15*time.Second, 300*time.Millisecond, "advisory lock held by psql", func() bool {
		return psql(t, fmt.Sprintf("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted AND objid=%d", lockKey&0xffffffff)) == "1"
	})
	t.Logf("[lock] psql holds pg_advisory_lock(%d = 0x%X) for %ds; scaling scheduler 0 -> 2", lockKey, lockKey, holdFor)
	kc(t, "scale", "deployment/scheduler", "-n", kNS, "--replicas=2")
	var sawWaiters bool
	var readyEarly []string
	for time.Since(held) < time.Duration(holdFor-5)*time.Second {
		waiting := psql(t, fmt.Sprintf("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted AND objid=%d", lockKey&0xffffffff))
		tables := psql(t, "SELECT count(*) FROM pg_tables WHERE schemaname='public'")
		if waiting == "2" && !sawWaiters {
			sawWaiters = true
			t.Logf("[lock t=%.1fs] 2 sessions waiting on the advisory lock; public tables: %s", time.Since(held).Seconds(), tables)
		}
		if tables != "0" {
			t.Errorf("[lock t=%.1fs] %s tables created while the advisory lock was held elsewhere", time.Since(held).Seconds(), tables)
			break
		}
		for _, p := range pods(t, kNS, "app=scheduler") {
			if p.ready() && !contains(readyEarly, p.Metadata.Name) {
				readyEarly = append(readyEarly, p.Metadata.Name)
				code := "?"
				if pf, ok := tryForwardHealthz(p.Metadata.Name); ok {
					code = pf
				}
				t.Errorf("[lock t=%.1fs] pod %s is Ready (healthz %s) before its migrations could run: traffic would reach a scheduler with no schema", time.Since(held).Seconds(), p.Metadata.Name, code)
			}
		}
		time.Sleep(time.Second)
	}
	if !sawWaiters {
		t.Logf("[observe] never saw two ungranted advisory-lock waiters on 0x%X (a try-lock loop would look like this); the decisive check is that no table appeared while it was held", lockKey)
	}
	_ = holder.Wait()
	t.Logf("[lock] psql session ended after %.1fs: %q", time.Since(held).Seconds(), strings.ReplaceAll(holderOut.String(), "\n", " "))
	ps = waitPodsReady(t, kNS, "app=scheduler", 2, 2*time.Minute)
	for _, p := range ps {
		if p.restarts() != 0 {
			t.Errorf("lock race: pod %s restarted %d times while waiting on the lock; previous logs:\n%s", p.Metadata.Name, p.restarts(),
				tail(podLogs(t, p.Metadata.Name, true), 10))
		}
	}
	checkMigrations(t, "after the held-lock race")

	// Both replicas serve the API and see each other's writes (stateless API).
	a := forward(t, "pod/"+ps[0].Metadata.Name, kPortA)
	b := forward(t, "pod/"+ps[1].Metadata.Name, kPortB)
	for _, pf := range []*portForward{a, b} {
		for _, path := range []string{"/healthz", "/livez"} {
			code, body, err := pf.get(path)
			t.Logf("[api :%d] GET %s -> %d %q err=%v", pf.port, path, code, body, err)
			if code != http.StatusOK {
				t.Errorf("GET %s on :%d = %d, want 200", path, pf.port, code)
			}
		}
	}
	r1 := a.newRound(t, "xcheck-two-a", 11)
	b.mustGetRound(t, r1, "xcheck-two-a")
	r2 := b.newRound(t, "xcheck-two-b", 12)
	a.mustGetRound(t, r2, "xcheck-two-b")
	a.stop()
	b.stop()

	kc(t, "scale", "deployment/scheduler", "-n", kNS, "--replicas=1")
	waitPodsReady(t, kNS, "app=scheduler", 1, 2*time.Minute)
}

// Decision 13: headless Service, so `postgres` resolves to the pod itself; the
// scheduler Service resolves in-cluster and answers.
func testKindDNS(t *testing.T) {
	pg := pods(t, kNS, "app=postgres")
	if len(pg) != 1 {
		t.Fatalf("postgres pods: %v", pg)
	}
	svcIP := strings.TrimSpace(kc(t, "get", "svc", "scheduler", "-n", kNS, "-o", "jsonpath={.spec.clusterIP}"))
	origIP := strings.TrimSpace(kc(t, "get", "svc", "scheduler", "-n", kOrigNS, "-o", "jsonpath={.spec.clusterIP}"))
	pgClusterIP := strings.TrimSpace(kc(t, "get", "svc", "postgres", "-n", kNS, "-o", "jsonpath={.spec.clusterIP}"))
	t.Logf("[k8s] postgres pod IP %s, postgres svc clusterIP %q, scheduler svc %s (%s) / %s (%s)",
		pg[0].Status.PodIP, pgClusterIP, svcIP, kNS, origIP, kOrigNS)
	if pgClusterIP != "None" {
		t.Errorf("postgres Service clusterIP = %q, decision 13 says headless (None)", pgClusterIP)
	}

	script := `set +e
for h in postgres postgres-0.postgres scheduler scheduler.kgpu-xcheck.svc scheduler.kgpu-xcheck.svc.cluster.local scheduler.kgpu.svc; do
  echo "DNS $h => $(getent hosts $h | awk '{print $1}' | tr '\n' ' ')"
done
pg_isready -h postgres -p 5432 -d kgpu && echo PGREADY
for p in /healthz /livez; do
  exec 3<>/dev/tcp/scheduler/8080
  printf 'GET %s HTTP/1.0\r\nHost: scheduler\r\n\r\n' $p >&3
  echo "HTTP $p => $(head -1 <&3)"
  exec 3<&-
done
exit 0`
	// Pin to the node that already has postgres:17 so no registry pull is needed.
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: dnsprobe
  namespace: %s
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: probe
      image: postgres:17
      imagePullPolicy: IfNotPresent
      command: ["bash", "-c", %q]
`, kNS, pg[0].Spec.NodeName, script)
	kcApply(t, manifest)
	defer kcRun("", "delete", "pod", "dnsprobe", "-n", kNS, "--wait=false")
	poll(t, 3*time.Minute, time.Second, "dnsprobe finished", func() bool {
		p, ok := getPod(t, kNS, "dnsprobe")
		return ok && (p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed")
	})
	out := kc(t, "logs", "-n", kNS, "dnsprobe")
	t.Logf("[dnsprobe]\n%s", out)
	resolved := map[string]string{}
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "DNS ") {
			parts := strings.SplitN(strings.TrimPrefix(l, "DNS "), " => ", 2)
			if len(parts) == 2 {
				resolved[parts[0]] = strings.TrimSpace(parts[1])
			}
		}
	}
	expect := map[string]string{
		"postgres":                  pg[0].Status.PodIP,
		"postgres-0.postgres":       pg[0].Status.PodIP,
		"scheduler":                 svcIP,
		"scheduler.kgpu-xcheck.svc": svcIP,
		"scheduler.kgpu-xcheck.svc.cluster.local": svcIP,
		"scheduler.kgpu.svc":                      origIP,
	}
	keys := make([]string, 0, len(expect))
	for k := range expect {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, h := range keys {
		if resolved[h] != expect[h] {
			t.Errorf("DNS %s => %q, want %q", h, resolved[h], expect[h])
		}
	}
	if !strings.Contains(out, "PGREADY") {
		t.Errorf("pg_isready -h postgres failed from a pod in the namespace")
	}
	for _, p := range []string{"/healthz", "/livez"} {
		if !regexp.MustCompile(`HTTP ` + p + ` => HTTP/1\.[01] 200`).MatchString(out) {
			t.Errorf("in-cluster GET %s through the scheduler Service did not return 200", p)
		}
	}
}

func onlySchedulerPod(t *testing.T) kPod {
	t.Helper()
	ps := waitPodsReady(t, kNS, "app=scheduler", 1, 2*time.Minute)
	return ps[0]
}

// SIGKILL of the scheduler process: the kubelet restarts the container in the
// same pod. Force-deleting the pod: the ReplicaSet creates a new pod. In both
// cases nothing is lost (Phase 1 decision 3: the scheduler is stateless).
func testKindKill(t *testing.T) {
	s := onlySchedulerPod(t)
	pf := forward(t, "pod/"+s.Metadata.Name, kPortA)
	round := pf.newRound(t, "xcheck-kill", 21)
	pf.stop()

	// Distroless: no shell, so the kill cannot come from inside the container.
	_, se, err := kcRun("", "exec", "-n", kNS, s.Metadata.Name, "--", "/bin/sh", "-c", "kill -9 1")
	t.Logf("[observe] kubectl exec /bin/sh in %s: err=%v %s", s.Metadata.Name, err, strings.TrimSpace(se))
	if err == nil {
		t.Logf("[observe] the scheduler image has a shell; kill was sent from inside")
	} else {
		// From the kind node: find the container's host PID and SIGKILL it.
		cid := strings.TrimPrefix(s.Status.ContainerStatuses[0].ContainerID, "containerd://")
		insp, err := exec.Command("docker", "exec", s.Spec.NodeName, "crictl", "inspect", cid).Output()
		if err != nil {
			t.Fatalf("crictl inspect on %s: %v", s.Spec.NodeName, err)
		}
		var ci struct {
			Info struct {
				Pid int `json:"pid"`
			} `json:"info"`
		}
		if err := json.Unmarshal(insp, &ci); err != nil || ci.Info.Pid == 0 {
			t.Fatalf("container pid: %v", err)
		}
		comm, _ := exec.Command("docker", "exec", s.Spec.NodeName, "cat", fmt.Sprintf("/proc/%d/comm", ci.Info.Pid)).Output()
		t.Logf("[kill] SIGKILL host pid %d (%s) on node %s, container %s", ci.Info.Pid, strings.TrimSpace(string(comm)), s.Spec.NodeName, short(cid))
		if out, err := exec.Command("docker", "exec", s.Spec.NodeName, "kill", "-9", strconv.Itoa(ci.Info.Pid)).CombinedOutput(); err != nil {
			t.Fatalf("kill -9: %v %s", err, out)
		}
	}
	var p kPod
	took := poll(t, 90*time.Second, 500*time.Millisecond, "scheduler container restarted in place and Ready", func() bool {
		var ok bool
		p, ok = getPod(t, kNS, s.Metadata.Name)
		return ok && p.restarts() == s.restarts()+1 && p.ready()
	})
	t.Logf("[check] after SIGKILL: %v (%.1fs)", p, took.Seconds())
	if p.Metadata.UID != s.Metadata.UID {
		t.Errorf("SIGKILL replaced the pod (uid %s -> %s); expected an in-place container restart", s.Metadata.UID, p.Metadata.UID)
	}
	if term := p.Status.ContainerStatuses[0].LastState.Terminated; term == nil || term.ExitCode != 137 {
		t.Errorf("lastState.terminated = %+v, want exit code 137 (SIGKILL)", term)
	} else {
		t.Logf("[check] lastState.terminated exitCode=%d reason=%s", term.ExitCode, term.Reason)
	}
	pf = forward(t, "pod/"+p.Metadata.Name, kPortA)
	pf.mustGetRound(t, round, "xcheck-kill")
	pf.stop()

	// Force delete: the controller replaces the pod.
	kc(t, "delete", "pod", s.Metadata.Name, "-n", kNS, "--grace-period=0", "--force")
	var np kPod
	took = poll(t, 2*time.Minute, 500*time.Millisecond, "replacement scheduler pod Ready", func() bool {
		for _, q := range pods(t, kNS, "app=scheduler") {
			if q.Metadata.Name != s.Metadata.Name && q.ready() {
				np = q
				return true
			}
		}
		return false
	})
	t.Logf("[check] after force delete: replacement %v (%.1fs)", np, took.Seconds())
	if np.restarts() != 0 {
		t.Errorf("replacement pod has restart count %d, want 0 (a new pod)", np.restarts())
	}
	pf = forward(t, "pod/"+np.Metadata.Name, kPortA)
	pf.mustGetRound(t, round, "xcheck-kill")
	pf.stop()
	poll(t, 60*time.Second, time.Second, "exactly one scheduler pod", func() bool { return len(pods(t, kNS, "app=scheduler")) == 1 })
}

type epState struct{ ready, notReady []string }

// schedulerEndpoints reads the scheduler Service's EndpointSlices.
func schedulerEndpoints(t *testing.T) epState {
	t.Helper()
	var list struct {
		Items []struct {
			Endpoints []struct {
				Conditions struct {
					Ready *bool `json:"ready"`
				} `json:"conditions"`
				TargetRef struct {
					Name string `json:"name"`
				} `json:"targetRef"`
			} `json:"endpoints"`
		} `json:"items"`
	}
	out := kc(t, "get", "endpointslices", "-n", kNS, "-l", "kubernetes.io/service-name=scheduler", "-o", "json")
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatal(err)
	}
	var s epState
	for _, it := range list.Items {
		for _, e := range it.Endpoints {
			if e.Conditions.Ready != nil && *e.Conditions.Ready {
				s.ready = append(s.ready, e.TargetRef.Name)
			} else {
				s.notReady = append(s.notReady, e.TargetRef.Name)
			}
		}
	}
	return s
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Decisions 6, 11, 14: with Postgres gone, /healthz fails, the pod goes
// NotReady and leaves the Service's ready endpoints, but /livez keeps
// answering and the kubelet never restarts it; it comes back on its own.
func testKindOutage(t *testing.T) {
	s := onlySchedulerPod(t)
	r0 := s.restarts()
	pf := forward(t, "pod/"+s.Metadata.Name, kPortA)
	if ep := schedulerEndpoints(t); !contains(ep.ready, s.Metadata.Name) {
		t.Fatalf("before outage, %s not a ready endpoint: %+v", s.Metadata.Name, ep)
	}
	// Hold the outage open longer than liveness would tolerate
	// (initialDelay 5s + period 10s x failureThreshold 3) by scaling the
	// StatefulSet to zero rather than deleting the pod (which returns in seconds).
	const hold = 45 * time.Second
	outage := time.Now()
	kc(t, "scale", "statefulset/postgres", "-n", kNS, "--replicas=0")
	t.Logf("[outage] statefulset/postgres scaled to 0 at t=0")

	var sawNotReady, sawEPDrop bool
	var healthCodes = map[int]int{}
	var livezBad []string
	var firstNotReady, firstEPDrop time.Duration
	for time.Since(outage) < hold {
		p, _ := getPod(t, kNS, s.Metadata.Name)
		if !p.ready() && !sawNotReady {
			sawNotReady, firstNotReady = true, time.Since(outage)
		}
		ep := schedulerEndpoints(t)
		if !contains(ep.ready, s.Metadata.Name) && !sawEPDrop {
			sawEPDrop, firstEPDrop = true, time.Since(outage)
			t.Logf("[outage t=%.1fs] endpoints ready=%v notReady=%v", firstEPDrop.Seconds(), ep.ready, ep.notReady)
		}
		if p.restarts() != r0 {
			t.Errorf("[outage t=%.1fs] scheduler restarted (count %d -> %d): liveness depends on Postgres", time.Since(outage).Seconds(), r0, p.restarts())
			break
		}
		hc, hb, _ := pf.get("/healthz")
		healthCodes[hc]++
		if hc != http.StatusOK && healthCodes[hc] == 1 {
			t.Logf("[outage t=%.1fs] GET /healthz -> %d %q", time.Since(outage).Seconds(), hc, hb)
		}
		lc, lb, lerr := pf.get("/livez")
		if lc != http.StatusOK {
			livezBad = append(livezBad, fmt.Sprintf("t=%.1fs code=%d body=%q err=%v", time.Since(outage).Seconds(), lc, lb, lerr))
		}
		time.Sleep(time.Second)
	}
	p, _ := getPod(t, kNS, s.Metadata.Name)
	t.Logf("[outage] after %.0fs: %v; /healthz codes %v; Ready=false first seen at %.1fs; endpoint dropped at %.1fs",
		hold.Seconds(), p, healthCodes, firstNotReady.Seconds(), firstEPDrop.Seconds())
	if !sawNotReady {
		t.Errorf("scheduler pod never went NotReady during a %s Postgres outage (decision 6/11)", hold)
	}
	if !sawEPDrop {
		t.Errorf("scheduler pod never left the Service's ready endpoints during the outage")
	}
	if healthCodes[http.StatusOK] > 2 {
		t.Errorf("/healthz returned 200 %d times while Postgres was down", healthCodes[http.StatusOK])
	}
	if len(livezBad) > 0 {
		t.Errorf("/livez failed during the outage (decision 11 says unconditional): %v", livezBad)
	}
	if p.restarts() != r0 {
		t.Errorf("restart count %d -> %d across the outage", r0, p.restarts())
	}
	errLines := grepLines(podLogs(t, s.Metadata.Name, false), regexp.MustCompile(`"level":"ERROR"`))
	t.Logf("[log] %d ERROR lines during the outage, last few: %v", len(errLines), errLines[max(0, len(errLines)-3):])

	kc(t, "scale", "statefulset/postgres", "-n", kNS, "--replicas=1")
	back := time.Now()
	waitPodsReady(t, kNS, "app=postgres", 1, 3*time.Minute)
	took := poll(t, 60*time.Second, 500*time.Millisecond, "scheduler Ready again", func() bool {
		p, _ = getPod(t, kNS, s.Metadata.Name)
		return p.ready() && contains(schedulerEndpoints(t).ready, s.Metadata.Name)
	})
	t.Logf("[recover] scheduler Ready and back in endpoints %.1fs after Postgres was scaled up: %v", took.Seconds(), p)
	_ = back
	if p.restarts() != r0 {
		t.Errorf("restart count %d -> %d after recovery", r0, p.restarts())
	}
	code, body, _ := pf.get("/healthz")
	if code != http.StatusOK {
		t.Errorf("/healthz after recovery = %d %q", code, body)
	}
	pf.newRound(t, "xcheck-after-outage", 31)
	pf.stop()
}

type kPVC struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
	Spec struct {
		VolumeName string `json:"volumeName"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

func getPVC(t *testing.T) (kPVC, bool) {
	t.Helper()
	so, se, err := kcRun("", "get", "pvc", "data-postgres-0", "-n", kNS, "-o", "json")
	if err != nil {
		if strings.Contains(se, "NotFound") {
			return kPVC{}, false
		}
		t.Fatalf("get pvc: %v %s", err, se)
	}
	var c kPVC
	if err := json.Unmarshal([]byte(so), &c); err != nil {
		t.Fatal(err)
	}
	return c, true
}

// Decisions 3 and 13: the claim outlives not only the pod but the StatefulSet.
func testKindStatefulSetRecreate(t *testing.T, rendered string) {
	s := onlySchedulerPod(t)
	pf := forward(t, "pod/"+s.Metadata.Name, kPortA)
	round := pf.newRound(t, "xcheck-sts-survive", 41)
	pf.stop()
	nRounds := psql(t, "SELECT count(*) FROM rounds")
	pg0, _ := getPod(t, kNS, "postgres-0")
	c0, ok := getPVC(t)
	if !ok {
		t.Fatal("no PVC data-postgres-0")
	}
	t.Logf("[before] %d rounds; postgres-0 %v; pvc uid=%s volume=%s %s", mustAtoi(nRounds), pg0, short(c0.Metadata.UID), c0.Spec.VolumeName, c0.Status.Phase)

	kc(t, "delete", "statefulset", "postgres", "-n", kNS, "--wait")
	poll(t, 2*time.Minute, time.Second, "postgres-0 deleted", func() bool { _, ok := getPod(t, kNS, "postgres-0"); return !ok })
	c1, ok := getPVC(t)
	t.Logf("[deleted] statefulset and pod gone; pvc present=%v uid=%s phase=%s", ok, short(c1.Metadata.UID), c1.Status.Phase)
	if !ok || c1.Metadata.UID != c0.Metadata.UID {
		t.Fatalf("PVC did not survive StatefulSet deletion: %v %+v", ok, c1)
	}

	kcApply(t, docsOfKind(rendered, "StatefulSet"))
	pg1 := waitPodsReady(t, kNS, "app=postgres", 1, 3*time.Minute)[0]
	c2, _ := getPVC(t)
	claim := ""
	for _, v := range pg1.Spec.Volumes {
		if v.PVC != nil {
			claim = v.PVC.ClaimName
		}
	}
	t.Logf("[recreated] %v mounts claim %s; pvc uid=%s volume=%s", pg1, claim, short(c2.Metadata.UID), c2.Spec.VolumeName)
	if pg1.Metadata.UID == pg0.Metadata.UID {
		t.Errorf("postgres-0 has the same UID; it was not recreated")
	}
	if claim != "data-postgres-0" || c2.Metadata.UID != c0.Metadata.UID || c2.Spec.VolumeName != c0.Spec.VolumeName {
		t.Errorf("recreated pod is not on the original claim/volume: claim=%s uid %s->%s vol %s->%s",
			claim, c0.Metadata.UID, c2.Metadata.UID, c0.Spec.VolumeName, c2.Spec.VolumeName)
	}
	after := psql(t, "SELECT count(*) FROM rounds")
	got := psql(t, fmt.Sprintf("SELECT scenario FROM rounds WHERE round_id=%d", round))
	t.Logf("[db] rounds %s -> %s; round %d scenario %q", nRounds, after, round, got)
	if after != nRounds || got != "xcheck-sts-survive" {
		t.Errorf("data did not survive: rounds %s -> %s, round %d scenario %q", nRounds, after, round, got)
	}
	checkMigrations(t, "after StatefulSet recreate")

	// The scheduler recovers without a restart and serves the old round.
	p := onlySchedulerPod(t)
	if p.restarts() != s.restarts() || p.Metadata.UID != s.Metadata.UID {
		t.Errorf("scheduler pod changed or restarted: %v -> %v", s, p)
	}
	pf = forward(t, "pod/"+p.Metadata.Name, kPortA)
	pf.mustGetRound(t, round, "xcheck-sts-survive")
	pf.stop()
}

func mustAtoi(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

// tryForwardHealthz port-forwards briefly to a pod and returns its /healthz code.
func tryForwardHealthz(pod string) (string, bool) {
	cmd := exec.Command("kubectl", "--context", kContext, "-n", kNS, "port-forward", "pod/"+pod, "28092:8080")
	if err := cmd.Start(); err != nil {
		return "", false
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	pf := &portForward{cmd: cmd, port: 28092}
	for i := 0; i < 20; i++ {
		code, body, err := pf.get("/healthz")
		if err == nil {
			return fmt.Sprintf("%d %s", code, body), true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return "", false
}
