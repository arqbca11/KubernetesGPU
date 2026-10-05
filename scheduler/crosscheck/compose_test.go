package crosscheck

// Black-box tests of the Phase 1 Docker Compose stack (design doc, "Step 4:
// Docker Compose"; roadmap Phase 1 failure tests). Skipped unless
// KGPU_COMPOSE=1. Each test drives the stack through the scheduler's HTTP API
// and the docker CLI only.

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const schedulerName = xcProject + "-scheduler-1"

var reportDone = map[string]any{"queue_depth": 0, "waiting_needs_index": 0, "oldest_wait_ms": 0, "build_progress": 0, "stream_done": true}

func isLeased1(b xBuild) bool   { return b.State == "leased" && b.Attempt == 1 }
func terminal(b xBuild) bool    { return b.State == "done" || b.State == "failed" }
func xcScaled(ms int64) float64 { return float64(ms) / 1000 / 10 }

// assertSingleCompletion checks exactly one worker "completed" line exists for
// the build across every worker container, with the given attempt, and not in
// the container named notIn (if non-empty). Returns the container key.
func assertSingleCompletion(t *testing.T, id string, attempt int, notIn string) {
	t.Helper()
	cl := completedLines(t, id)
	for k, ls := range cl {
		for _, l := range ls {
			t.Logf("[log %s] %s", k, l)
		}
	}
	if n := countLines(cl); n != 1 {
		t.Errorf("expected exactly one `completed` line for %s across all workers, found %d (invariant 3: build finishes exactly once)", id, n)
		return
	}
	for k, ls := range cl {
		if !strings.Contains(ls[0], "attempt="+strconv.Itoa(attempt)) {
			t.Errorf("the completion of %s was not for attempt %d: %s", id, attempt, ls[0])
		}
		if notIn != "" && strings.HasPrefix(k, notIn+"(") {
			t.Errorf("the completion of %s came from %s, which should have lost ownership", id, notIn)
		}
	}
}

// Roadmap failure test 1; decisions 1, 2, 42 (reaper), 47 (restart policy).
func TestComposeKill9LeaseholderMidBuild(t *testing.T) {
	requireStack(t)
	round := newRound(t, 1)
	s := mustSubmitNew(t, round, 0, xcBigN)
	t.Logf("gpu_total_ms=%d, so the fake build lasts about %.2fs at FAKE_TIME_SCALE=10", s.GPUTotalMs, xcScaled(s.GPUTotalMs))
	b := waitBuild(t, s.BuildID, 15*time.Second, "first claim", isLeased1)
	victim := nameOf(t, b.owner())
	startedBefore := inspect(t, victim, "{{.State.StartedAt}}")
	t.Logf("leaseholder is %s (lease_owner=%s); crashing its worker process with SIGKILL 2s into the build", victim, b.owner())
	time.Sleep(2 * time.Second)
	// Spec resolution after this test first ran (decision 47): `docker kill` is a
	// manual stop and Docker does not restart it, and the kernel ignores SIGKILL
	// sent to a namespace's PID 1 from inside. The containers run with init: true,
	// so the worker is a child of tini; killing that pid from inside is a crash.
	crashWorkerProcess(t, victim)
	killedAt := time.Now()
	t.Logf("[docker] SIGKILL to the python process inside %s; %s", victim, psSummary(t))

	done := waitBuild(t, s.BuildID, 45*time.Second, "terminal state after kill", terminal)
	t.Logf("terminal %.1fs after the kill: %s", time.Since(killedAt).Seconds(), done)
	if done.State != "done" || done.Attempt != 2 {
		t.Errorf("spec (roadmap failure test 1, design 'Failure: worker killed mid-build'): reaper requeues after lease expiry, another worker claims attempt 2 and completes. Got %s", done)
	}

	sched := containerLogs(t, schedulerName)
	exp := linesFor(sched, s.BuildID, "lease expired")
	for _, l := range exp {
		t.Logf("[log scheduler] %s", l)
	}
	if len(exp) != 1 || !strings.Contains(exp[0], "attempt=1") || !strings.Contains(exp[0], "lease_owner="+b.owner()) {
		t.Errorf("spec (Step 2, reaper): one warning line per reaped build with build_id, attempt, round_id, lease_owner; expected one `lease expired` line for attempt 1 owned by %s, got %d", b.owner(), len(exp))
	}
	// Decision 50: the victim's container may come back (or wake) under the same WORKER_ID and
	// legitimately claim attempt 2 itself. Fencing is about attempts, not identities: what
	// matters is exactly one completion, at attempt 2, and none at attempt 1.
	assertSingleCompletion(t, s.BuildID, 2, "")
	vlog := containerLogs(t, victim)
	if len(linesFor(vlog, s.BuildID, "claimed")) == 0 {
		t.Errorf("killed container %s never logged `claimed` for %s", victim, s.BuildID)
	}

	// Decision 47: restart: unless-stopped brings a killed worker back.
	deadline := time.Now().Add(30 * time.Second)
	for {
		st := inspect(t, victim, "{{.State.Running}} {{.State.StartedAt}} {{.RestartCount}}")
		f := strings.Fields(st)
		if len(f) == 3 && f[0] == "true" && f[1] != startedBefore {
			t.Logf("[docker] %s came back: running=%s started_at=%s restart_count=%s (decision 47)", victim, f[0], f[1], f[2])
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("decision 47 (the restart policy brings a crashed worker back): after its process was killed from inside, the container should be running again; %s after 30s: running started_at restart_count = %s", victim, st)
			t.Logf("[docker] %s", psSummary(t))
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	w := waitLiveWorkers(t, 2, 30*time.Second)
	t.Logf("[api] GET /workers: live_workers=%d", w.Pool.LiveWorkers)
}

// Roadmap failure test 2; decisions 2, 25, 36. The paused worker wakes while
// attempt 2 is still building elsewhere, and its build clock has run past the
// estimate, so its next write is either a renew or a complete with attempt 1.
func TestComposePauseLeaseholderPastLease(t *testing.T) {
	requireStack(t)
	round := newRound(t, 1)
	s := mustSubmitNew(t, round, 0, xcBigN)
	b := waitBuild(t, s.BuildID, 15*time.Second, "first claim", isLeased1)
	victim := nameOf(t, b.owner())
	t.Cleanup(func() { _ = execQuiet("docker", "unpause", victim) })
	time.Sleep(1 * time.Second)
	dockerCmd(t, "pause", victim)
	pausedAt := time.Now()
	t.Logf("[docker] pause %s (SIGSTOP via the freezer) 1s into a %.2fs build, for 8s against a 5s lease", victim, xcScaled(s.GPUTotalMs))

	b2 := waitBuild(t, s.BuildID, 8*time.Second, "reap and re-claim while the leaseholder is paused", func(b xBuild) bool {
		return b.Attempt >= 2 && (b.State == "leased" || b.State == "done")
	})
	if b2.Attempt != 2 || b2.owner() == b.owner() {
		t.Errorf("spec (roadmap failure test 2): while paused the lease expires, is reaped, and the other worker claims attempt 2; got %s", b2)
	}
	if rest := 8*time.Second - time.Since(pausedAt); rest > 0 {
		time.Sleep(rest)
	}
	dockerCmd(t, "unpause", victim)
	t.Logf("[docker] unpause %s after %.1fs; row now: %s", victim, time.Since(pausedAt).Seconds(), getBuild(t, s.BuildID))

	// Decision 36: the woken worker logs `lost ownership` and writes nothing more.
	var lost []string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		lost = linesFor(containerLogs(t, victim), s.BuildID, "lost ownership")
		if len(lost) > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	vlog := containerLogs(t, victim)
	for _, l := range linesFor(vlog, s.BuildID, "") {
		t.Logf("[log %s] %s", victim, l)
	}
	if len(lost) == 0 || !strings.Contains(lost[0], "attempt=1") {
		t.Errorf("decision 36: the resumed worker should log `lost ownership` for attempt 1 within a few seconds of resuming; got %v", lost)
	}

	done := waitBuild(t, s.BuildID, 20*time.Second, "done", terminal)
	if done.State != "done" || done.Attempt != 2 {
		t.Errorf("expected done with attempt 2 (the paused worker's attempt-1 writes must match zero rows, invariant 3); got %s", done)
	}
	// Decision 50: the victim's container may come back (or wake) under the same WORKER_ID and
	// legitimately claim attempt 2 itself. Fencing is about attempts, not identities: what
	// matters is exactly one completion, at attempt 2, and none at attempt 1.
	assertSingleCompletion(t, s.BuildID, 2, "")
	if ls := linesFor(vlog, s.BuildID, "released back to queue"); len(ls) > 0 {
		t.Errorf("decision 36: after losing the lease the worker neither completes nor releases; got %v", ls)
	}
	time.Sleep(2 * time.Second) // negative observation: nothing touches a done row
	again := getBuild(t, s.BuildID)
	if again.State != "done" || again.Attempt != 2 || again.FinishedAt == nil || !again.FinishedAt.Equal(*done.FinishedAt) {
		t.Errorf("the done row changed afterwards: before %s, after %s", done, again)
	}
	t.Logf("row 2s later is unchanged: %s", again)
}

// Roadmap failure test 3; decision 3 (stateless scheduler), 18 (round
// completion computed from Postgres), invariant 6 across a restart.
func TestComposeSchedulerRestartMidRound(t *testing.T) {
	requireStack(t)
	round := newRound(t, 2)
	s0 := mustSubmitNew(t, round, 0, xcBigN)
	b := waitBuild(t, s0.BuildID, 15*time.Second, "first claim", isLeased1)
	time.Sleep(1 * time.Second)
	t.Logf("restarting the scheduler while %s is leased by %s", s0.BuildID, b.owner())
	compose(t, "restart", "scheduler")
	dt := waitHealthy(t, 60*time.Second)
	t.Logf("[api] /healthz 200 again %.1fs after `compose restart` returned", dt.Seconds())

	done := waitBuild(t, s0.BuildID, 30*time.Second, "done", terminal)
	if done.State != "done" || done.Attempt != 1 {
		t.Errorf("decision 3 / roadmap failure test 3: workers talk to Postgres, so a scheduler restart must not cost the lease; expected done attempt 1, got %s", done)
	}
	assertSingleCompletion(t, s0.BuildID, 1, "")
	if ls := linesFor(containerLogs(t, schedulerName), s0.BuildID, "lease expired"); len(ls) > 0 {
		t.Errorf("the restarted scheduler reaped a live lease: %v", ls)
	}

	// State survived: the duplicate is a no-op, the round cap still counts shard 0.
	code, d := submit(t, round, 0, xcSmallN)
	if code != http.StatusOK || d.Created || d.BuildID != s0.BuildID || d.MemBytes != s0.MemBytes || d.GPUTotalMs != s0.GPUTotalMs {
		t.Errorf("invariant 6 after restart: expected 200 created:false with the stored estimates %+v, got %d %+v", s0, code, d)
	}
	s1 := mustSubmitNew(t, round, 1, xcSmallN)
	waitBuild(t, s1.BuildID, 15*time.Second, "shard 1 done", terminal)
	if code, _ := submit(t, round, 2, xcSmallN); code != http.StatusConflict {
		t.Errorf("decision 27: a third shard in a 2-shard round must be 409 after a restart too; got %d", code)
	}

	for _, sh := range []int{0, 1} {
		var rr struct {
			Build struct {
				State   string `json:"state"`
				Attempt int32  `json:"attempt"`
			} `json:"build"`
		}
		code, raw := callAPI(t, "POST", "/shards/"+strconv.Itoa(int(round))+"/"+strconv.Itoa(sh)+"/report", reportDone, &rr)
		t.Logf("[api] POST /shards/%d/%d/report stream_done=true -> %d %s", round, sh, code, strings.TrimSpace(raw))
		if code != http.StatusOK || rr.Build.State != "done" {
			t.Errorf("report reply should carry the build's state done (decision 29); got %d %s", code, raw)
		}
	}
	var rs xRoundStatus
	code, raw := callAPI(t, "GET", "/rounds/"+strconv.Itoa(int(round)), nil, &rs)
	if code != http.StatusOK {
		t.Fatalf("GET /rounds/%d: %d %s", round, code, raw)
	}
	var latest time.Time
	for _, bb := range rs.Builds {
		if bb.FinishedAt != nil && bb.FinishedAt.After(latest) {
			latest = *bb.FinishedAt
		}
	}
	if rs.Round.FinishedAt == nil {
		t.Errorf("decision 18/30: every shard stream_done, no queries, all builds done: GET /rounds should stamp the round finished; finished_at is nil")
	} else {
		t.Logf("[api] round %d finished_at=%s, latest build finish=%s", round, rs.Round.FinishedAt.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano))
		if !rs.Round.FinishedAt.Equal(latest) {
			t.Errorf("decision 18: finished_at must be the latest child finish (%s), got %s", latest, rs.Round.FinishedAt)
		}
	}
	narrateBuildLogs(t, s0.BuildID)
}

// Decision 3 and invariant 5: the reaper keeps no state, so a lease that
// expired while no scheduler was running is reaped by the next scheduler.
func TestComposeReaperResumesAfterSchedulerOutage(t *testing.T) {
	requireStack(t)
	round := newRound(t, 1)
	s := mustSubmitNew(t, round, 0, xcBigN)
	b := waitBuild(t, s.BuildID, 15*time.Second, "first claim", isLeased1)
	victim := nameOf(t, b.owner())
	t.Cleanup(func() { _ = execQuiet("docker", "start", schedulerName) })
	compose(t, "stop", "scheduler")
	dockerCmd(t, "kill", "--signal", "KILL", victim)
	t.Logf("[docker] scheduler stopped, then leaseholder %s SIGKILLed; waiting 8s so the 5s lease expires with no reaper running", victim)
	time.Sleep(8 * time.Second) // the lease must expire while nothing can reap it
	compose(t, "start", "scheduler")
	t.Logf("[api] /healthz 200 %.1fs after start", waitHealthy(t, 60*time.Second).Seconds())

	done := waitBuild(t, s.BuildID, 30*time.Second, "done", terminal)
	if done.State != "done" || done.Attempt != 2 {
		t.Errorf("the new scheduler's reaper should requeue the expired lease and a worker complete attempt 2; got %s", done)
	}
	exp := linesFor(containerLogs(t, schedulerName), s.BuildID, "lease expired")
	for _, l := range exp {
		t.Logf("[log scheduler] %s", l)
	}
	if len(exp) != 1 {
		t.Errorf("expected exactly one `lease expired` line for %s, got %d (invariant 5: reaping is idempotent)", s.BuildID, len(exp))
	}
	assertSingleCompletion(t, s.BuildID, 2, "")
}

// Roadmap failure test 4; invariant 6; decision 26 (concurrent duplicates).
func TestComposeDuplicateSubmit(t *testing.T) {
	requireStack(t)
	round := newRound(t, 2)
	s := mustSubmitNew(t, round, 0, xcSmallN)
	first := waitBuild(t, s.BuildID, 15*time.Second, "done", terminal)
	if first.State != "done" || first.Attempt != 1 {
		t.Fatalf("smoke: expected done attempt 1, got %s", first)
	}
	code, d := submit(t, round, 0, xcBigN)
	if code != http.StatusOK || d.Created || d.BuildID != s.BuildID || d.Placement != s.Placement ||
		d.MemBytes != s.MemBytes || d.CPUBuildMs != s.CPUBuildMs || d.GPUTotalMs != s.GPUTotalMs {
		t.Errorf("invariant 6 / API table: a repeat returns 200 created:false and every field but reason from the stored row. first %+v, repeat %d %+v", s, code, d)
	}
	time.Sleep(2 * time.Second) // negative observation: no worker picks it up again
	after := getBuild(t, s.BuildID)
	t.Logf("row 2s after the duplicate: %s n_vectors=%d", after, after.NVectors)
	if after.State != "done" || after.Attempt != 1 || after.NVectors != xcSmallN || !after.FinishedAt.Equal(*first.FinishedAt) {
		t.Errorf("the duplicate submit changed the row: before %s, after %s", first, after)
	}

	// Eight simultaneous submits of one new build id, repeated over several
	// rounds: (a) the build takes the round's last slot (n_shards=2, shard 0
	// already in), (b) control: the round has spare capacity (n_shards=8).
	for _, tc := range []struct {
		label   string
		nShards int
	}{{"last slot", 2}, {"spare capacity", 8}} {
		bad := 0
		for rep := 0; rep < 5; rep++ {
			r := round
			if rep > 0 || tc.nShards != 2 {
				r = newRound(t, tc.nShards)
				mustSubmitNew(t, r, 0, xcSmallN)
			}
			id, created, dup, other := concurrentSubmits(t, r, 1, 8)
			t.Logf("[%s, rep %d] 8 simultaneous submits of %s: %d created, %d duplicates (200), others: %v", tc.label, rep, id, created, dup, other)
			if created != 1 || dup != 7 {
				bad++
				t.Errorf("decision 26 + API table (%s): simultaneous submits of one build_id yield exactly one 201 created:true and the rest 200 created:false (a repeat is not a new build, so the round cap's 409 does not apply); got %d created, %d duplicates, others %v", tc.label, created, dup, other)
			}
			got := waitBuild(t, id, 15*time.Second, "done", terminal)
			if got.State != "done" || got.Attempt != 1 {
				t.Errorf("expected the single build to finish once with attempt 1; got %s", got)
			}
			assertSingleCompletion(t, id, 1, "")
		}
		t.Logf("[%s] %d of 5 bursts violated the promise", tc.label, bad)
	}
	assertSingleCompletion(t, s.BuildID, 1, "")
}

// Decision 37 (SIGTERM), decision 47 (stop_grace_period 15s), decision 15
// (release). `docker stop` sends SIGTERM to the leaseholder.
func TestComposeDockerStopLeaseholder(t *testing.T) {
	t.Run("LongRemainderIsReleased", func(t *testing.T) {
		requireStack(t)
		round := newRound(t, 1)
		s := mustSubmitNew(t, round, 0, xcBigN)
		b := waitBuild(t, s.BuildID, 15*time.Second, "first claim", isLeased1)
		victim := nameOf(t, b.owner())
		t.Cleanup(func() { _ = execQuiet("docker", "start", victim) })
		time.Sleep(500 * time.Millisecond)
		t.Logf("[docker] stop %s 0.5s into a %.2fs build: about 6s remain against SHUTDOWN_FINISH_BUDGET_SECONDS=5, so decision 37 says cancel and release", victim, xcScaled(s.GPUTotalMs))
		t0 := time.Now()
		dockerCmd(t, "stop", victim)
		stopDur := time.Since(t0)
		st := inspect(t, victim, "{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}")
		t.Logf("[docker] stop returned after %.1fs; state: %s", stopDur.Seconds(), st)
		if st != "exited 0 false" || stopDur >= 15*time.Second {
			t.Errorf("decision 37 / Step 3 behaviour: exit code 0 promptly after a release, within stop_grace_period=15s (no SIGKILL, which would be 137); got %q after %v", st, stopDur)
		}
		b2 := waitBuild(t, s.BuildID, 10*time.Second, "re-claim by the other worker", func(b xBuild) bool { return b.Attempt >= 2 })
		reclaim := time.Since(t0)
		t.Logf("attempt 2 claimed %.1fs after `docker stop` was issued (lease is 5s)", reclaim.Seconds())
		if b2.owner() == b.owner() && b2.State == "leased" {
			t.Errorf("re-claimed by the stopped worker?! %s", b2)
		}
		vlog := containerLogs(t, victim)
		for _, l := range strings.Split(vlog, "\n") {
			if strings.Contains(l, "shutdown") || strings.Contains(l, "stopped") || buildRE(s.BuildID).MatchString(l) {
				t.Logf("[log %s] %s", victim, strings.TrimSpace(l))
			}
		}
		if len(linesFor(vlog, s.BuildID, "cancelling current build to release it")) != 1 {
			t.Errorf("decision 37 / log vocabulary: expected `cancelling current build to release it` for %s", s.BuildID)
		}
		rel := linesFor(vlog, s.BuildID, "released back to queue")
		if len(rel) != 1 || !strings.Contains(rel[0], "attempt=1") {
			t.Errorf("decision 37: expected one `released back to queue` line for attempt 1; got %v", rel)
		}
		if ls := linesFor(containerLogs(t, schedulerName), s.BuildID, "lease expired"); len(ls) > 0 {
			t.Errorf("the build was reaped rather than released: %v", ls)
		}
		if reclaim >= 5*time.Second {
			t.Errorf("decision 37: a released build is handed back now, so another worker starts it before the lease would have expired (5s); took %v", reclaim)
		}
		done := waitBuild(t, s.BuildID, 20*time.Second, "done", terminal)
		if done.State != "done" || done.Attempt != 2 {
			t.Errorf("expected done attempt 2; got %s", done)
		}
		assertSingleCompletion(t, s.BuildID, 2, victim)
		time.Sleep(1 * time.Second)
		if st := inspect(t, victim, "{{.State.Status}}"); st != "exited" {
			t.Errorf("restart: unless-stopped must not restart a stopped container; state %s", st)
		}
		dockerCmd(t, "start", victim)
		waitLiveWorkers(t, 2, 30*time.Second)
	})

	t.Run("ShortRemainderIsFinished", func(t *testing.T) {
		requireStack(t)
		round := newRound(t, 1)
		s := mustSubmitNew(t, round, 0, xcBigN)
		b := waitBuild(t, s.BuildID, 15*time.Second, "first claim", isLeased1)
		victim := nameOf(t, b.owner())
		t.Cleanup(func() { _ = execQuiet("docker", "start", victim) })
		time.Sleep(3 * time.Second)
		t.Logf("[docker] stop %s 3s into a %.2fs build: about 3.5s remain, within the 5s budget, so decision 37 says finish", victim, xcScaled(s.GPUTotalMs))
		t0 := time.Now()
		dockerCmd(t, "stop", victim)
		stopDur := time.Since(t0)
		st := inspect(t, victim, "{{.State.Status}} {{.State.ExitCode}}")
		t.Logf("[docker] stop returned after %.1fs; state: %s", stopDur.Seconds(), st)
		if st != "exited 0" || stopDur >= 15*time.Second {
			t.Errorf("decision 37: exit 0 after finishing the build, within stop_grace_period; got %q after %v", st, stopDur)
		}
		row := getBuild(t, s.BuildID)
		t.Logf("[api] right after the container exited: %s", row)
		if row.State != "done" || row.Attempt != 1 {
			t.Errorf("decision 37: the build should be complete (attempt 1) by the time the worker exits; got %s", row)
		}
		vlog := containerLogs(t, victim)
		for _, l := range linesFor(vlog, s.BuildID, "") {
			t.Logf("[log %s] %s", victim, l)
		}
		if len(linesFor(vlog, s.BuildID, "finishing current build before exit")) != 1 {
			t.Errorf("log vocabulary: expected `finishing current build before exit` for %s", s.BuildID)
		}
		assertSingleCompletion(t, s.BuildID, 1, "")
		dockerCmd(t, "start", victim)
		waitLiveWorkers(t, 2, 30*time.Second)
	})
}

// Decisions 1, 6, 46: `--scale worker=3` is one flag; three workers claim
// three builds at once, each exactly once; scaling down drops the pool.
func TestComposeScaleToThreeWorkers(t *testing.T) {
	requireStack(t)
	t.Cleanup(func() { _ = composeCmd("up", "-d", "--no-recreate", "--scale", "worker=2").Run() })
	compose(t, "up", "-d", "--wait", "--no-recreate", "--scale", "worker=3")
	w := waitLiveWorkers(t, 3, 30*time.Second)
	var ids []string
	for _, x := range w.Workers {
		ids = append(ids, x.WorkerID)
	}
	t.Logf("[api] GET /workers: live_workers=%d registered=%v; docker: %s", w.Pool.LiveWorkers, ids, psSummary(t))

	round := newRound(t, 3)
	var bids []string
	for sh := 0; sh < 3; sh++ {
		bids = append(bids, mustSubmitNew(t, round, sh, xcBigN).BuildID)
	}
	deadline := time.Now().Add(10 * time.Second)
	var owners map[string]string
	for {
		var rs xRoundStatus
		callAPI(t, "GET", "/rounds/"+strconv.Itoa(int(round)), nil, &rs)
		owners = map[string]string{}
		leased := 0
		for _, b := range rs.Builds {
			if b.State == "leased" {
				leased++
				owners[b.owner()] = b.BuildID
			}
		}
		if leased == 3 {
			for _, b := range rs.Builds {
				t.Logf("[api] simultaneously: %s started_at=%s", b, b.StartedAt.Format("15:04:05.000"))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("three builds were never leased at the same time by three workers; last: %+v", rs.Builds)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(owners) != 3 {
		t.Errorf("invariant 2 / decision 46: three leased builds must have three distinct owners; got %v", owners)
	}
	for _, id := range bids {
		d := waitBuild(t, id, 20*time.Second, "done", terminal)
		if d.State != "done" || d.Attempt != 1 {
			t.Errorf("expected %s done with attempt 1, got %s", id, d)
		}
		assertSingleCompletion(t, id, 1, "")
	}

	t0 := time.Now()
	compose(t, "up", "-d", "--no-recreate", "--scale", "worker=2")
	w = waitLiveWorkers(t, 2, 30*time.Second)
	t.Logf("[api] pool back to %d live workers %.1fs after scaling down (WORKER_STALE_AFTER=5s); docker: %s", w.Pool.LiveWorkers, time.Since(t0).Seconds(), psSummary(t))
}

// Decision 47 (named volume, restart policy) and invariant 1: a Postgres
// restart mid-build; services recover and nothing completes twice.
func TestComposePostgresRestartMidBuild(t *testing.T) {
	requireStack(t)
	round := newRound(t, 1)
	s := mustSubmitNew(t, round, 0, xcBigN)
	b := waitBuild(t, s.BuildID, 15*time.Second, "first claim", isLeased1)
	before := map[string]string{}
	for _, c := range append(serviceContainers(t, "worker", false), serviceContainers(t, "scheduler", false)...) {
		before[c.Name] = inspect(t, c.Name, "{{.RestartCount}}")
	}
	time.Sleep(1 * time.Second)
	t.Logf("restarting postgres while %s is leased by %s", s.BuildID, b.owner())
	compose(t, "restart", "postgres")
	t.Logf("[api] /healthz 200 %.1fs after postgres restarted", waitHealthy(t, 90*time.Second).Seconds())

	done := waitBuild(t, s.BuildID, 90*time.Second, "terminal", terminal)
	if done.State != "done" {
		t.Errorf("a transient Postgres restart should not lose the build: expected done, got %s", done)
	}
	t.Logf("in-flight build ended as %s (attempt >1 means its lease was lost during the outage and it was reaped)", done)
	assertSingleCompletion(t, s.BuildID, int(done.Attempt), "")
	for name, rc := range before {
		t.Logf("[docker] %s restart_count %s -> %s", name, rc, inspect(t, name, "{{.RestartCount}}"))
	}
	for name, logs := range logsByContainer(t) {
		inTB := 0
		for _, l := range strings.Split(logs, "\n") {
			if strings.Contains(l, "Traceback") {
				inTB = 40
			} else if inTB > 0 && len(l) > 2 && l[0] >= '0' && l[0] <= '9' && l[2] == ':' {
				inTB = 0 // next timestamped log line ends the traceback
			}
			if inTB > 0 || strings.Contains(l, "ERROR") || strings.Contains(l, "WARN") || strings.Contains(l, "starting") {
				t.Logf("[log %s] %s", name, strings.TrimRight(l, " "))
			}
			if inTB > 0 {
				inTB--
			}
		}
		if strings.Contains(logs, "Traceback") && strings.Contains(name, "worker") {
			t.Errorf("Step 3 behaviour paragraph: a crash traceback \"should never happen after decisions 39 and 41\"; %s crashed (traceback above) during the Postgres restart", name)
		}
	}
	w := waitLiveWorkers(t, 2, 30*time.Second)
	t.Logf("[api] pool: live_workers=%d", w.Pool.LiveWorkers)

	var rs xRoundStatus
	if code, raw := callAPI(t, "GET", "/rounds/"+strconv.Itoa(int(round)), nil, &rs); code != http.StatusOK || len(rs.Builds) != 1 {
		t.Errorf("the round and its build should survive the restart (named volume); got %d %s", code, raw)
	}
	r2 := newRound(t, 1)
	s2 := mustSubmitNew(t, r2, 0, xcSmallN)
	d2 := waitBuild(t, s2.BuildID, 20*time.Second, "fresh build done", terminal)
	if d2.State != "done" || d2.Attempt != 1 {
		t.Errorf("after the restart a fresh build should complete normally; got %s", d2)
	}
	assertSingleCompletion(t, s2.BuildID, 1, "")
}

// concurrentSubmits fires n identical submits at once and tallies them.
func concurrentSubmits(t *testing.T, round int64, shard, n int) (id string, created, dup int, other []string) {
	t.Helper()
	type res struct {
		code int
		raw  string
		s    xSubmit
		err  error
	}
	results := make([]res, n)
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			var x xSubmit
			c, raw, err := apiTry("POST", "/builds", map[string]any{"round_id": round, "shard_id": shard, "n_vectors": xcSmallN, "dim": 128}, &x)
			results[i] = res{c, raw, x, err}
		}(i)
	}
	close(gate)
	wg.Wait()
	for _, r := range results {
		switch {
		case r.err == nil && r.code == http.StatusCreated && r.s.Created:
			created++
		case r.err == nil && r.code == http.StatusOK && !r.s.Created:
			dup++
		default:
			other = append(other, strconv.Itoa(r.code)+" "+strings.TrimSpace(r.raw)+errString(r.err))
		}
		if r.s.BuildID != "" {
			id = r.s.BuildID
		}
	}
	return id, created, dup, other
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return " err=" + err.Error()
}

// crashWorkerProcess SIGKILLs the python process inside a worker container.
// PID 1 is docker-init (whose cmdline also mentions kgpu_worker), so match
// on the program name.
func crashWorkerProcess(t *testing.T, container string) {
	t.Helper()
	dockerCmd(t, "exec", container, "sh", "-c",
		`for p in /proc/[0-9]*; do pid=${p#/proc/}; [ "$pid" = 1 ] && continue; case "$(tr "\0" " " < $p/cmdline 2>/dev/null)" in python*) kill -9 $pid;; esac; done`)
}
