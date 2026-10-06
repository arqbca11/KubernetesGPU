package crosscheck

// Phase 1 step 6 cross-check: the roadmap's four failure tests, plus three
// the implementer did not run, against the whole Compose stack while the
// shard simulator drives continuous rounds (skewed, 6 shards, 2x time scale,
// 5 s leases). Every test injects its failure into a live round and then
// asserts verifyRound: the round still completes correctly.
//
// Black box: the scheduler's HTTP API, the docker CLI, container logs.
// Spec: CLAUDE.md invariants; docs/design/phase1-scheduler.md decisions 1-5,
// 24, 36, 37, 42, 47, 48, 49, 53-57, 61; roadmap Phase 1 failure tests.

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const ldRoundWait = 120 * time.Second

// Roadmap failure test 1 under load: SIGKILL the leaseholder mid-build.
// Spec: decision 2 (lease + attempt), invariant 5 (the reaper requeues an
// expired lease once), decision 47 (the restart policy brings the crashed
// worker back), roadmap "the lease expires, another worker picks the job up,
// and it completes once".
func TestLoadCrashLeaseholderMidBuild(t *testing.T) {
	requireLoadStack(t)
	r := waitFreshRound(t, 60*time.Second)
	b, _ := waitRoundBuild(t, r.Round.RoundID, 30*time.Second, "a build of >=100k vectors freshly leased at attempt 1",
		freshlyLeased(false, 100000, 700*time.Millisecond))
	victim := nameOf(t, b.owner())
	t.Logf("[inject] round %d: %s (n_vectors=%d) leased to %s; crashing it mid-build", r.Round.RoundID, b, b.NVectors, victim)
	killedAt := time.Now()
	ldCrashWorker(t, victim)

	after := waitBuild(t, b.BuildID, 60*time.Second, "the build done after recovery", func(x xBuild) bool { return terminal(x) })
	t.Logf("[api] build %s ended %s, %.1fs after the kill (lease 5 s + rebuild)", b.BuildID, after, time.Since(killedAt).Seconds())
	if after.State != "done" || after.Attempt < 2 {
		t.Errorf("want done at attempt >= 2 after the crash (the claim bumps attempt, invariant 4), got %s", after)
	}
	if ls := linesWith(containerLogs(t, victim), b.BuildID, 1, "completed"); len(ls) != 0 {
		t.Fatalf("the crash landed too late: %s completed attempt 1 before it died: %v", victim, ls)
	}
	reaps := reapLines(t, b.BuildID, 1)
	logAll(t, "scheduler", reaps)
	if len(reaps) != 1 {
		t.Errorf("want exactly one 'lease expired' line for %s attempt 1 (reaper idempotent, invariant 5), got %d", b.BuildID, len(reaps))
	}

	v := verifyRound(t, r.Round.RoundID, ldRoundWait)
	final, _ := v.Round.build(b.BuildID)
	sl := shardLines(t, b.BuildID)
	logAll(t, "shards", sl)
	if len(linesWith(strings.Join(sl, "\n"), b.BuildID, int(final.Attempt), "index ready")) != 1 {
		t.Errorf("the shard's log should show 'index ready' for %s at attempt %d exactly once", b.BuildID, final.Attempt)
	}
	if len(linesWith(strings.Join(sl, "\n"), b.BuildID, int(final.Attempt), "state=leased")) == 0 {
		t.Logf("[note] the shard's 500 ms report did not catch %s leased at attempt %d (the window can be short)", b.BuildID, final.Attempt)
	}
	waitLiveWorkers(t, 2, 30*time.Second)
	t.Logf("[verify] pool back to 2 live workers after the crash")
}

// Roadmap failure test 2 under load: SIGSTOP the leaseholder past its lease,
// then SIGCONT. Spec: decision 2 (fencing), decision 36 (a rejected renew
// cancels the build; the worker logs that it lost ownership and writes
// nothing more for that attempt), design flow "its renew and complete carry
// attempt = 1, match zero rows".
func TestLoadPauseLeaseholderPastLease(t *testing.T) {
	requireLoadStack(t)
	r := waitFreshRound(t, 60*time.Second)
	b, _ := waitRoundBuild(t, r.Round.RoundID, 30*time.Second, "a build of >=100k vectors freshly leased at attempt 1",
		freshlyLeased(false, 100000, 700*time.Millisecond))
	victim := nameOf(t, b.owner())
	t.Cleanup(func() { _ = execQuiet("docker", "unpause", victim) })
	t.Logf("[inject] round %d: %s leased to %s; docker pause for 8 s against a 5 s lease", r.Round.RoundID, b, victim)
	dockerCmd(t, "pause", victim)
	pausedAt := time.Now()

	reaped := waitBuild(t, b.BuildID, 15*time.Second, "the paused worker's lease reaped (attempt moves past 1 or state leaves leased)",
		func(x xBuild) bool { return x.Attempt > 1 || x.State == "queued" })
	t.Logf("[api] while %s is paused: %s", victim, reaped)
	if wait := 8*time.Second - time.Since(pausedAt); wait > 0 {
		time.Sleep(wait)
	}
	resumedAt := time.Now()
	dockerCmd(t, "unpause", victim)
	t.Logf("[inject] unpaused %s after %.1fs", victim, time.Since(pausedAt).Seconds())

	// The woken worker must notice it lost the lease and write nothing for attempt 1.
	deadline := time.Now().Add(10 * time.Second)
	var lost []string
	for time.Now().Before(deadline) {
		logs := logsSince(t, victim, resumedAt.Add(-time.Second))
		lost = append(linesWith(logs, b.BuildID, 1, "lost"), linesWith(logs, b.BuildID, 1, "rejected")...)
		if len(lost) > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	logAll(t, "log "+victim, lost)
	if len(lost) == 0 {
		t.Errorf("after SIGCONT, %s logged no 'lost'/'rejected' line for %s attempt 1 (decision 36)", victim, b.BuildID)
	}

	after := waitBuild(t, b.BuildID, 60*time.Second, "the build done", terminal)
	if after.State != "done" || after.Attempt < 2 {
		t.Errorf("want done at attempt >= 2, got %s", after)
	}
	v := verifyRound(t, r.Round.RoundID, ldRoundWait)
	for _, c := range allCompletions(t)[b.BuildID] {
		if c.Attempt == 1 {
			t.Errorf("the paused worker's attempt 1 completed after waking: %s", c.Line)
		}
	}
	final, _ := v.Round.build(b.BuildID)
	t.Logf("[verify] %s finished once at attempt %d; the woken worker was fenced out", b.BuildID, final.Attempt)
	logAll(t, "shards", shardLines(t, b.BuildID))
	waitLiveWorkers(t, 2, 30*time.Second)
}

// Roadmap failure test 3 under load: `docker compose restart scheduler`
// mid-round. Spec: decision 3 (all state in Postgres, a restart costs
// nothing), decision 61 (shard calls retry transient failures; a scheduler
// restart is invisible to the round), decision 4 (workers talk to Postgres,
// so builds carry on at attempt 1).
func TestLoadSchedulerRestartMidRound(t *testing.T) {
	requireLoadStack(t)
	r := waitFreshRound(t, 60*time.Second)
	b, _ := waitRoundBuild(t, r.Round.RoundID, 30*time.Second, "any build leased at attempt 1",
		freshlyLeased(false, 0, 2*time.Second))
	startedBefore := ldInspect(t, ldSchedulerName, "{{.State.StartedAt}}")
	t.Logf("[inject] round %d mid-flight (%s); restarting the scheduler", r.Round.RoundID, b)
	restartAt := time.Now()
	ldCompose(t, "restart", "scheduler")
	down := waitHealthy(t, 60*time.Second)
	t.Logf("[docker] scheduler healthy again %.1fs after the restart command returned; StartedAt %s -> %s",
		down.Seconds(), startedBefore, ldInspect(t, ldSchedulerName, "{{.State.StartedAt}}"))
	if ldInspect(t, ldSchedulerName, "{{.State.StartedAt}}") == startedBefore {
		t.Fatalf("scheduler StartedAt did not change: the restart did not happen")
	}

	v := verifyRound(t, r.Round.RoundID, ldRoundWait)
	for _, x := range v.Round.Builds {
		if x.Attempt != 1 {
			t.Errorf("build %s at attempt %d: a scheduler restart should not cost a lease (workers renew through Postgres, decision 4)", x.BuildID, x.Attempt)
		}
	}
	sl := logsSince(t, ldShardsName, restartAt.Add(-time.Second))
	var retrying, recovered []string
	for _, l := range strings.Split(sl, "\n") {
		if strings.Contains(l, "scheduler call failed, retrying") {
			retrying = append(retrying, strings.TrimSpace(l))
		}
		if strings.Contains(l, "succeeded after retries") {
			recovered = append(recovered, strings.TrimSpace(l))
		}
	}
	t.Logf("[shards] %d 'scheduler call failed, retrying' lines, %d 'succeeded after retries' lines since the restart", len(retrying), len(recovered))
	logAll(t, "shards", retrying[:min(len(retrying), 5)])
	logAll(t, "shards", recovered[:min(len(recovered), 5)])
	if len(retrying) == 0 {
		t.Errorf("no shard call failed across the restart: the simulator's log does not show the outage it should have ridden out (decision 61)")
	}
	if len(recovered) == 0 {
		t.Errorf("no 'succeeded after retries' line: the shard log does not show recovery from the restart")
	}
}

// Roadmap failure test 4 under load: duplicate submits into a live, full
// round. Spec: invariant 6 (build_id is the idempotency key; resubmitting is a
// no-op), decision 26/27 and bug log 6 (a repeat into a full round is 200
// created:false, not 409; a new shard into a full round is 409), API table
// ("every field but reason comes from the stored row").
func TestLoadDuplicateSubmitDuringRound(t *testing.T) {
	requireLoadStack(t)
	r := waitFreshRound(t, 60*time.Second)
	b, _ := waitRoundBuild(t, r.Round.RoundID, 30*time.Second, "any build leased at attempt 1", freshlyLeased(false, 0, 3*time.Second))
	t.Logf("[inject] round %d is full (%d of %d builds); resubmitting %s 8 times concurrently with n_vectors=%d",
		r.Round.RoundID, len(r.Builds), r.Round.NShards, b.BuildID, b.NVectors*3)

	var wg sync.WaitGroup
	var mu sync.Mutex
	type res struct {
		code int
		s    xSubmit
		raw  string
		err  error
	}
	var results []res
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var s xSubmit
			code, raw, err := apiTry("POST", "/builds", map[string]any{"round_id": r.Round.RoundID, "shard_id": b.ShardID, "n_vectors": b.NVectors * 3, "dim": 128}, &s)
			mu.Lock()
			results = append(results, res{code, s, raw, err})
			mu.Unlock()
		}()
	}
	wg.Wait()
	for _, x := range results {
		if x.err != nil || x.code != http.StatusOK || x.s.Created || x.s.BuildID != b.BuildID || x.s.MemBytes != b.MemBytes || x.s.Placement != b.Placement {
			t.Errorf("duplicate submit: want 200 created:false with the stored decision (mem_bytes=%d placement=%s), got %d err=%v %s",
				b.MemBytes, b.Placement, x.code, x.err, strings.TrimSpace(x.raw))
		}
	}
	t.Logf("[api] 8 concurrent duplicates -> %d answered; first: %s", len(results), strings.TrimSpace(results[0].raw))

	now := getBuild(t, b.BuildID)
	t.Logf("[api] after the duplicates: %s n_vectors=%d", now, now.NVectors)
	if now.NVectors != b.NVectors || now.Attempt < b.Attempt || now.State == "queued" && b.State != "queued" {
		t.Errorf("the duplicate changed the row: before %s n=%d, after %s n=%d", b, b.NVectors, now, now.NVectors)
	}

	code, raw := callAPI(t, "POST", "/builds", map[string]any{"round_id": r.Round.RoundID, "shard_id": 99, "n_vectors": 1000, "dim": 128}, nil)
	t.Logf("[api] a NEW shard 99 into the full round -> %d %s", code, strings.TrimSpace(raw))
	if code != http.StatusConflict {
		t.Errorf("new build into a full round: want 409 (decision 27), got %d", code)
	}

	v := verifyRound(t, r.Round.RoundID, ldRoundWait)
	final, _ := v.Round.build(b.BuildID)
	if final.NVectors != b.NVectors {
		t.Errorf("stored n_vectors changed from %d to %d", b.NVectors, final.NVectors)
	}
	if final.Attempt != 1 {
		t.Errorf("the duplicates cost %s a lease: attempt %d", b.BuildID, final.Attempt)
	}

	// After the round is stamped: a repeat is still a no-op (not "round finished").
	var s xSubmit
	code, raw = callAPI(t, "POST", "/builds", map[string]any{"round_id": r.Round.RoundID, "shard_id": b.ShardID, "n_vectors": 7, "dim": 64}, &s)
	t.Logf("[api] duplicate after the round finished -> %d %s", code, strings.TrimSpace(raw))
	if code != http.StatusOK || s.Created {
		t.Errorf("duplicate into a finished round: want 200 created:false (only a NEW build gets 409), got %d %s", code, raw)
	}
	if got := getBuild(t, b.BuildID); got.State != "done" || got.Attempt != final.Attempt {
		t.Errorf("a duplicate after completion disturbed the build: %s", got)
	}
}

// Not run by the implementer: crash the leaseholder of the round's LARGEST
// build, then crash whoever holds attempt 2, so the build reaches attempt 3.
// Spec: invariant 4 (every claim bumps attempt), invariant 5 and decision 42
// (each expiry is reaped once; MAX_ATTEMPTS=5 so attempt 3 is still
// requeued), CLAUDE.md "one slow shard holds up the whole round": the shard
// whose build was delayed must be the round's straggler in the timeline.
func TestLoadLargestBuildKilledTwiceReachesAttempt3(t *testing.T) {
	requireLoadStack(t)
	r := waitFreshRound(t, 60*time.Second)
	big, _ := r.largest()
	t.Logf("[setup] round %d: largest build %s n_vectors=%d (shard %d)", r.Round.RoundID, big.BuildID, big.NVectors, big.ShardID)

	b1, _ := waitRoundBuild(t, r.Round.RoundID, 30*time.Second, "the largest build freshly leased at attempt 1", freshlyLeased(true, 0, time.Second))
	v1 := nameOf(t, b1.owner())
	t.Logf("[inject] crash 1: %s held by %s", b1, v1)
	ldCrashWorker(t, v1)

	b2 := waitBuild(t, big.BuildID, 30*time.Second, "attempt 2 leased", func(x xBuild) bool {
		return x.State == "leased" && x.Attempt == 2 && x.StartedAt != nil && time.Since(*x.StartedAt) < 1500*time.Millisecond
	})
	v2 := nameOf(t, b2.owner())
	t.Logf("[inject] crash 2: %s held by %s (%s)", b2, v2, map[bool]string{true: "the same container, restarted", false: "the other worker"}[v1 == v2])
	ldCrashWorker(t, v2)

	b3 := waitBuild(t, big.BuildID, 60*time.Second, "the build terminal", terminal)
	if b3.State != "done" || b3.Attempt != 3 {
		t.Errorf("want done at attempt 3, got %s", b3)
	}
	for a := 1; a <= 2; a++ {
		rl := reapLines(t, big.BuildID, a)
		logAll(t, "scheduler", rl)
		if len(rl) != 1 {
			t.Errorf("want exactly one 'lease expired' line for attempt %d, got %d", a, len(rl))
		}
	}
	if rl := reapLines(t, big.BuildID, 3); len(rl) != 0 {
		t.Errorf("attempt 3 was reaped too: %v", rl)
	}

	v := verifyRound(t, r.Round.RoundID, ldRoundWait)
	sl := shardLines(t, big.BuildID)
	logAll(t, "shards", sl)
	joined := strings.Join(sl, "\n")
	if len(linesWith(joined, big.BuildID, 3, "index ready")) != 1 {
		t.Errorf("the shard's log should show 'index ready' at attempt 3 exactly once")
	}
	if len(linesWith(joined, big.BuildID, 2, "state=leased")) == 0 {
		// Spec gap, not asserted: nothing promises a shard log line per
		// attempt change; the shard logs "build state" on state changes, and
		// a requeue shorter than its 500 ms report interval looks like
		// leased -> leased. The timeline's att column and "index ready"
		// carry the attempt.
		t.Logf("[note] the shard's log has no line for attempt 2 although it was leased for >5 s (ten report replies carried attempt=2)")
	}

	// The straggler effect in the simulator's own timeline.
	row := v.Timeline.Rows[int(big.ShardID)]
	t.Logf("[timeline] %s", v.Timeline.Summary)
	if row.Att != 3 {
		t.Errorf("timeline att column for shard %d: %d, want 3", big.ShardID, row.Att)
	}
	if row.BDone < 10 {
		t.Errorf("shard %d's b.done %.1fs: two 5 s leases should delay it past 10 s", big.ShardID, row.BDone)
	}
	for sh, o := range v.Timeline.Rows {
		if sh == int(big.ShardID) {
			continue
		}
		if o.Finish >= row.Finish {
			t.Errorf("straggler not visible: shard %d finished at %.1fs, not before the twice-crashed shard %d (%.1fs)", sh, o.Finish, big.ShardID, row.Finish)
		}
		if o.BDone >= row.BDone {
			t.Errorf("shard %d's build finished at %.1fs, not before the twice-crashed one (%.1fs)", sh, o.BDone, row.BDone)
		}
	}
	dur := v.Round.Round.FinishedAt.Sub(v.Round.Round.StartedAt).Seconds()
	t.Logf("[verify] round lasted %.1fs real; the twice-crashed shard %d finished at %.1fs and set it", dur, big.ShardID, row.Finish)
	if d := dur - row.Finish; d > 0.2 || d < -0.2 {
		t.Errorf("round duration %.1fs is not the straggler shard's finish %.1fs", dur, row.Finish)
	}
	waitLiveWorkers(t, 2, 30*time.Second)
}

// Not run by the implementer under load: `docker compose restart postgres`
// mid-round. Spec: decision 48 (workers survive lost connections: reconnect,
// retry guarded writes until the lease deadline; no crash), decision 61
// (shard calls retry 5xx and connection errors for up to 60 s), decision 3
// (Postgres on a named volume is the only state; the round survives).
func TestLoadPostgresRestartMidRound(t *testing.T) {
	requireLoadStack(t)
	r := waitFreshRound(t, 60*time.Second)
	b, _ := waitRoundBuild(t, r.Round.RoundID, 30*time.Second, "any build freshly leased", freshlyLeased(false, 100000, time.Second))
	wrc := map[string]int{}
	for _, c := range serviceContainers(t, "worker", false) {
		wrc[c.Name] = ldRestartCount(t, c.Name)
	}
	src := ldRestartCount(t, ldSchedulerName)
	t.Logf("[inject] round %d mid-flight (%s); restarting postgres", r.Round.RoundID, b)
	ldCompose(t, "restart", "postgres")
	waitHealthy(t, 60*time.Second)

	v := verifyRound(t, r.Round.RoundID, ldRoundWait)
	for name, n := range wrc {
		if got := ldRestartCount(t, name); got != n {
			t.Errorf("%s restarted (RestartCount %d -> %d) across a Postgres restart; decision 48 says it reconnects instead:\n%s",
				name, n, got, tail(containerLogs(t, name), 15))
		}
	}
	t.Logf("[docker] scheduler RestartCount %d -> %d across the Postgres restart (not a spec promise in Phase 1)", src, ldRestartCount(t, ldSchedulerName))
	for _, x := range v.Round.Builds {
		if x.Attempt != 1 {
			t.Logf("[note] %s needed attempt %d (a lease can legitimately expire if Postgres was away longer than the lease)", x.BuildID, x.Attempt)
		}
	}
	waitLiveWorkers(t, 2, 30*time.Second)
}

// Not run by the implementer under load: `docker stop` the leaseholder of the
// round's largest build. At these settings the largest build is ~3.8 s, under
// SHUTDOWN_FINISH_BUDGET_SECONDS=5, so the finish branch applies. Spec:
// decision 37 (SIGTERM: stop claiming; finish within budget), decision 49
// (deregister on clean exit), decision 47 (docker stop is a manual stop; no
// restart).
func TestLoadDockerStopLeaseholderMidRound(t *testing.T) {
	requireLoadStack(t)
	r := waitFreshRound(t, 60*time.Second)
	b, _ := waitRoundBuild(t, r.Round.RoundID, 30*time.Second, "the largest build freshly leased at attempt 1", freshlyLeased(true, 0, 700*time.Millisecond))
	victim := nameOf(t, b.owner())
	vid := b.owner()
	t.Cleanup(func() { _ = execQuiet("docker", "start", victim) })
	t.Logf("[inject] round %d: %s held by %s; docker stop (SIGTERM, 15 s grace)", r.Round.RoundID, b, victim)
	stopAt := time.Now()
	dockerCmd(t, "stop", victim)
	t.Logf("[docker] %s stopped in %.1fs, exit code %s", victim, time.Since(stopAt).Seconds(), ldInspect(t, victim, "{{.State.ExitCode}}"))

	ids := workerIDs(t)
	t.Logf("[api] /workers right after the stop: %v", ids)
	for _, id := range ids {
		if id == vid {
			t.Errorf("stopped worker %s is still registered; decision 49 says a clean exit deregisters", vid)
		}
	}
	got := getBuild(t, b.BuildID)
	t.Logf("[api] %s after the stop", got)
	if got.State != "done" || got.Attempt != 1 {
		t.Errorf("remaining time was under the 5 s budget, so the stopping worker should have finished attempt 1 (decision 37); got %s", got)
	}
	vlogs := logsSince(t, victim, stopAt.Add(-200*time.Millisecond))
	for _, l := range strings.Split(vlogs, "\n") {
		if strings.Contains(l, " claimed build_id=") && !strings.Contains(l, " claimed build_id="+b.BuildID+" ") {
			t.Errorf("the stopping worker claimed new work after SIGTERM: %s", strings.TrimSpace(l))
		}
	}
	logAll(t, "log "+victim, linesWith(vlogs, b.BuildID, -1))

	time.Sleep(3 * time.Second)
	if st := ldInspect(t, victim, "{{.State.Status}}"); st != "exited" {
		t.Errorf("%s is %s 3 s after docker stop; a manual stop should not be restarted", victim, st)
	}

	v := verifyRound(t, r.Round.RoundID, ldRoundWait)
	_ = v
	dockerCmd(t, "start", victim)
	waitLiveWorkers(t, 2, 30*time.Second)
	t.Logf("[cleanup] %s started again; pool back to 2", victim)
}

// Audit of every round this run produced (failure-injected or not): the
// shard's LAST stored report. The happy-path flow in the design doc ends with
// "stream exhausted and backlog empty: report stream_done=true,
// queue_depth=0"; shard_status is also the policy's input (decision 29), so
// a stamped round whose shards still read as backlogged is stale state in the
// one source of truth (invariant 1). Observed (2026-10-05): at the moment a
// round is stamped, some shards' rows still say queue_depth>0 (logged as
// [finding] by verifyRound), because completion is computed from shard_jobs,
// not from the report; by the time this audit runs, the shard's next report
// has zeroed them. This test asserts the settled state.
func TestLoadFinalReportShowsEmptyBacklog(t *testing.T) {
	requireLoadStack(t)
	last := latestRound(t)
	rounds, stale := 0, 0
	for id := int64(1); id <= last; id++ {
		r, code, err := getRoundTry(id)
		if err != nil || code != http.StatusOK || r.Round.FinishedAt == nil {
			continue
		}
		rounds++
		for _, s := range r.ShardStatus {
			if s.QueueDepth != 0 || s.WaitingIdx != 0 {
				stale++
				var lastFin time.Time
				for _, j := range r.Jobs {
					if j.ShardID == s.ShardID && j.FinishedAt != nil && j.FinishedAt.After(lastFin) {
						lastFin = *j.FinishedAt
					}
				}
				t.Errorf("round %d (stamped %s) shard %d: last report (updated_at %s, %+.3fs vs the shard's last query finish %s) says stream_done=%v queue_depth=%d waiting_needs_index=%d, yet all its queries finished",
					id, r.Round.FinishedAt.Format("15:04:05.000"), s.ShardID, s.UpdatedAt.Format("15:04:05.000"), s.UpdatedAt.Sub(lastFin).Seconds(),
					lastFin.Format("15:04:05.000"), s.StreamDone, s.QueueDepth, s.WaitingIdx)
			}
		}
	}
	t.Logf("[verify] %d stamped rounds audited, %d shard rows left with a non-empty backlog", rounds, stale)
	if rounds == 0 {
		t.Fatalf("no stamped rounds to audit")
	}
}
