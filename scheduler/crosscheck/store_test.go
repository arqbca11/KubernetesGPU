package crosscheck

import (
	"sync"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

// Spec: state diagram "queued --> leased: claim (attempt + 1)"; invariant 4;
// CHECK "leased requires lease_owner and lease_until"; roadmap Claim SQL sets
// started_at = now(); go doc Claim: "ok is false when the queue has nothing claimable".
func TestClaimLeasesAndBumpsAttempt(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 1)
	b := gpuBuild(r, 1, 0, 100)
	mustSubmit(t, ctx, st, b, nil)
	before := mustGet(t, ctx, st, b.BuildID)
	t.Logf("submitted gpu build; Postgres holds: %s", fmtRow(before))
	if before.State != "queued" || before.Attempt != 0 {
		t.Errorf("design state diagram: gpu submit starts queued with attempt 0; got state=%s attempt=%d", before.State, before.Attempt)
	}

	c, ok, err := st.Claim(ctx, "w1", 1<<30, lease30)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	after := mustGet(t, ctx, st, b.BuildID)
	t.Logf("claimed by w1: returned build_id=%s attempt=%d; Postgres holds: %s", c.BuildID, c.Attempt, fmtRow(after))
	if c.BuildID != b.BuildID || c.Attempt != 1 {
		t.Errorf("invariant 4: first claim returns attempt 1; got %s attempt=%d", c.BuildID, c.Attempt)
	}
	if c.NVectors != b.NVectors || c.Dim != b.Dim || c.MemBytes != b.MemBytes {
		t.Errorf("Claimed should carry n_vectors/dim/mem_bytes of the row; got %+v", c)
	}
	if after.State != "leased" || after.Attempt != 1 {
		t.Errorf("state diagram: claim moves queued->leased with attempt+1; got state=%s attempt=%d", after.State, after.Attempt)
	}
	if after.LeaseOwner == nil || *after.LeaseOwner != "w1" {
		t.Errorf("roadmap Claim SQL: lease_owner = worker id; got %v", after.LeaseOwner)
	}
	if after.LeaseUntil == nil {
		t.Errorf("leased row must have lease_until")
	} else {
		d := time.Until(*after.LeaseUntil)
		t.Logf("lease_until is %v from now (lease requested: 30s)", d.Round(time.Millisecond))
		if d < 20*time.Second || d > 40*time.Second {
			t.Errorf("lease_until should be about now()+30s; got %v from now", d)
		}
	}
	if after.StartedAt == nil {
		t.Errorf("roadmap Claim SQL sets started_at = now(); got NULL")
	}

	_, ok, err = st.Claim(ctx, "w2", 1<<30, lease30)
	t.Logf("second claim on a queue with no queued builds: ok=%v err=%v", ok, err)
	if err != nil || ok {
		t.Errorf("go doc Claim: ok=false when nothing is claimable; got ok=%v err=%v", ok, err)
	}
}

// Spec: design "ConcurrentClaimsNeverShareABuild"; invariant 2 (FOR UPDATE SKIP LOCKED).
func TestConcurrentClaimsNeverShareABuild(t *testing.T) {
	ctx, _, st := setup(t)
	const nBuilds, nWorkers = 12, 4
	r := mustRound(t, ctx, st, nBuilds)
	for i := 1; i <= nBuilds; i++ {
		mustSubmit(t, ctx, st, gpuBuild(r, int32(i), 0, 100), nil)
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	errs := make(chan error, nWorkers)
	for w := 0; w < nWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				c, ok, err := st.Claim(ctx, "w"+itoa(int64(w)), 1<<30, lease30)
				if err != nil {
					errs <- err
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				seen[c.BuildID]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Claim error under concurrency: %v", err)
	}
	t.Logf("%d workers drained %d queued builds; distinct builds claimed: %d", nWorkers, nBuilds, len(seen))
	if len(seen) != nBuilds {
		t.Errorf("every queued build should be claimed exactly once; got %d distinct", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("invariant 2: build %s claimed %d times", id, n)
		}
		row := mustGet(t, ctx, st, id)
		if row.Attempt != 1 || row.State != "leased" {
			t.Errorf("%s: want leased attempt 1, got %s", id, fmtRow(row))
		}
	}
}

// Spec: invariant 3 + design "Failure: worker killed mid-build" / SIGSTOP variant:
// after reap and re-claim, the old owner's renew, complete and fail match zero rows.
func TestStaleOwnerFencedAfterReap(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 1)
	b := gpuBuild(r, 1, 0, 100)
	mustSubmit(t, ctx, st, b, nil)

	c1, ok, err := st.Claim(ctx, "w-old", 1<<30, time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	time.Sleep(100 * time.Millisecond)
	reaped, err := st.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	t.Logf("w-old claimed with a 1ms lease (attempt %d), slept 100ms, reaped: %+v; row: %s", c1.Attempt, reaped, fmtRow(mustGet(t, ctx, st, b.BuildID)))
	if len(reaped) != 1 || reaped[0].BuildID != b.BuildID || reaped[0].Attempt != 1 || reaped[0].LeaseOwner != "w-old" {
		t.Errorf("Reap should report exactly the expired lease (build, attempt 1, owner w-old); got %+v", reaped)
	}

	c2, ok, err := st.Claim(ctx, "w-new", 1<<30, lease30)
	if err != nil || !ok {
		t.Fatalf("re-Claim: ok=%v err=%v", ok, err)
	}
	if c2.Attempt != 2 {
		t.Errorf("invariant 4: re-claim after reap should get attempt 2; got %d", c2.Attempt)
	}
	snap := mustGet(t, ctx, st, b.BuildID)
	t.Logf("w-new re-claimed: %s", fmtRow(snap))

	okRenew, err1 := st.Renew(ctx, b.BuildID, c1.Attempt, lease30)
	okComplete, err2 := st.Complete(ctx, b.BuildID, c1.Attempt)
	okFail, err3 := st.Fail(ctx, b.BuildID, c1.Attempt, "late")
	okRelease, err4 := st.Release(ctx, b.BuildID, c1.Attempt)
	for _, e := range []error{err1, err2, err3, err4} {
		if e != nil {
			t.Fatalf("stale write returned error: %v", e)
		}
	}
	after := mustGet(t, ctx, st, b.BuildID)
	t.Logf("w-old (attempt 1) wakes: renew=%v complete=%v fail=%v release=%v; row now: %s", okRenew, okComplete, okFail, okRelease, fmtRow(after))
	if okRenew || okComplete || okFail || okRelease {
		t.Errorf("invariant 3: every stale-attempt write must report false")
	}
	if after.State != "leased" || after.Attempt != 2 || after.LeaseOwner == nil || *after.LeaseOwner != "w-new" ||
		!after.LeaseUntil.Equal(*snap.LeaseUntil) || after.FailReason != nil {
		t.Errorf("invariant 3: stale writes must leave the row untouched; before %s / after %s", fmtRow(snap), fmtRow(after))
	}

	okc, err := st.Complete(ctx, b.BuildID, c2.Attempt)
	t.Logf("w-new completes with attempt 2: %v (err %v); row: %s", okc, err, fmtRow(mustGet(t, ctx, st, b.BuildID)))
	if err != nil || !okc {
		t.Errorf("current owner's complete should succeed")
	}
}

// Spec: invariant 5 ("only moves expired leases back to queued, and running it
// twice changes nothing"); design "a done row is never reaped".
func TestReapOnlyExpiredAndIdempotent(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 3)
	live, expired, done := gpuBuild(r, 1, 3, 100), gpuBuild(r, 2, 2, 100), gpuBuild(r, 3, 1, 100)
	for _, b := range []store.Build{live, expired, done} {
		mustSubmit(t, ctx, st, b, nil)
	}
	// Priority order makes the claim order deterministic: live, expired, done.
	cLive, _, _ := st.Claim(ctx, "wl", 1<<30, lease30)
	cExp, _, _ := st.Claim(ctx, "we", 1<<30, time.Millisecond)
	cDone, _, _ := st.Claim(ctx, "wd", 1<<30, time.Millisecond)
	if cLive.BuildID != live.BuildID || cExp.BuildID != expired.BuildID || cDone.BuildID != done.BuildID {
		t.Fatalf("unexpected claim order: %s %s %s", cLive.BuildID, cExp.BuildID, cDone.BuildID)
	}
	if ok, err := st.Complete(ctx, done.BuildID, cDone.Attempt); !ok || err != nil {
		t.Fatalf("Complete: %v %v", ok, err)
	}
	time.Sleep(100 * time.Millisecond)

	first, err := st.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []store.Build{live, expired, done} {
		t.Logf("after two reaps: %s", fmtRow(mustGet(t, ctx, st, b.BuildID)))
	}
	t.Logf("first reap reported %+v; second reap reported %+v", first, second)
	if len(first) != 1 || first[0].BuildID != expired.BuildID {
		t.Errorf("invariant 5: first reap should touch only the expired lease; got %+v", first)
	}
	if len(second) != 0 {
		t.Errorf("invariant 5: second reap must change nothing; got %+v", second)
	}
	if g := mustGet(t, ctx, st, live.BuildID); g.State != "leased" || g.Attempt != 1 {
		t.Errorf("live lease must not be reaped: %s", fmtRow(g))
	}
	if g := mustGet(t, ctx, st, expired.BuildID); g.State != "queued" || g.Attempt != 1 {
		t.Errorf("expired lease returns to queued with attempt unchanged (attempt only changes on claim): %s", fmtRow(g))
	}
	if g := mustGet(t, ctx, st, done.BuildID); g.State != "done" {
		t.Errorf("done row must never be reaped: %s", fmtRow(g))
	}
}

// Spec: state diagram "leased --> done" / "done --> [*]"; implementation note
// "every post-claim write is guarded by build_id AND attempt AND state = 'leased'".
// A terminal row cannot be re-completed, failed, renewed or released by its own
// (still current) attempt.
func TestDoneIsTerminalEvenForCurrentAttempt(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 1)
	b := gpuBuild(r, 1, 0, 100)
	mustSubmit(t, ctx, st, b, nil)
	c, _, _ := st.Claim(ctx, "w1", 1<<30, lease30)
	ok1, _ := st.Complete(ctx, b.BuildID, c.Attempt)
	snap := mustGet(t, ctx, st, b.BuildID)
	t.Logf("first complete: %v; row: %s", ok1, fmtRow(snap))
	if !ok1 || snap.State != "done" || snap.FinishedAt == nil {
		t.Fatalf("complete should set done and finished_at")
	}
	ok2, e1 := st.Complete(ctx, b.BuildID, c.Attempt)
	ok3, e2 := st.Fail(ctx, b.BuildID, c.Attempt, "after done")
	ok4, e3 := st.Renew(ctx, b.BuildID, c.Attempt, lease30)
	ok5, e4 := st.Release(ctx, b.BuildID, c.Attempt)
	after := mustGet(t, ctx, st, b.BuildID)
	t.Logf("same attempt again: complete=%v fail=%v renew=%v release=%v (errs %v %v %v %v); row: %s",
		ok2, ok3, ok4, ok5, e1, e2, e3, e4, fmtRow(after))
	if ok2 || ok3 || ok4 || ok5 {
		t.Errorf("state guard: writes to a done row must report false")
	}
	if after.State != "done" || after.FailReason != nil || !after.FinishedAt.Equal(*snap.FinishedAt) {
		t.Errorf("done row must be unchanged; before %s after %s", fmtRow(snap), fmtRow(after))
	}
}

// Spec: decision 15 / design test "ReleaseReturnsToQueueAndFences".
func TestReleaseRequeuesAndFences(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 1)
	b := gpuBuild(r, 1, 0, 100)
	mustSubmit(t, ctx, st, b, nil)
	c1, _, _ := st.Claim(ctx, "w1", 1<<30, lease30)
	ok, err := st.Release(ctx, b.BuildID, c1.Attempt)
	row := mustGet(t, ctx, st, b.BuildID)
	t.Logf("w1 releases attempt %d: %v (err %v); row: %s", c1.Attempt, ok, err, fmtRow(row))
	if !ok || row.State != "queued" {
		t.Errorf("Release should move leased -> queued")
	}
	c2, ok2, _ := st.Claim(ctx, "w2", 1<<30, lease30)
	okLate, _ := st.Complete(ctx, b.BuildID, c1.Attempt)
	row = mustGet(t, ctx, st, b.BuildID)
	t.Logf("w2 claims (ok=%v attempt=%d); w1 tries to complete attempt 1: %v; row: %s", ok2, c2.Attempt, okLate, fmtRow(row))
	if c2.Attempt != 2 {
		t.Errorf("invariant 4: claim after release should give attempt 2; got %d", c2.Attempt)
	}
	if okLate || row.State != "leased" || row.Attempt != 2 {
		t.Errorf("releasing owner must be fenced out")
	}
}

// Spec: invariant 6 / design test "DuplicateSubmitIsNoop" / go doc SubmitBuild:
// "A build_id that already exists leaves everything untouched", including jobs.
func TestDuplicateSubmitLeavesBuildAndJobsUntouched(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 1)
	b := gpuBuild(r, 1, 5, 100)
	jobs := []store.JobSpec{{Seq: 0, DurationMs: 100, NeedsIndex: false}, {Seq: 1, DurationMs: 200, NeedsIndex: true}}
	mustSubmit(t, ctx, st, b, jobs)
	c, _, _ := st.Claim(ctx, "w1", 1<<30, lease30)
	before := mustGet(t, ctx, st, b.BuildID)

	dup := b
	dup.Placement = "local"
	dup.Priority = 99
	dup.NVectors = 7
	ins, err := st.SubmitBuild(ctx, dup, []store.JobSpec{{Seq: 0, DurationMs: 9999, NeedsIndex: true}, {Seq: 5, DurationMs: 1, NeedsIndex: false}})
	after := mustGet(t, ctx, st, b.BuildID)
	gotJobs, jerr := st.ListJobs(ctx, r)
	t.Logf("resubmitted mid-lease with placement=local priority=99 n_vectors=7 and different jobs: inserted=%v err=%v", ins, err)
	t.Logf("row before: %s", fmtRow(before))
	t.Logf("row after:  %s", fmtRow(after))
	t.Logf("jobs after: %+v (err %v)", gotJobs, jerr)
	if err != nil || ins {
		t.Errorf("invariant 6: duplicate submit returns inserted=false, no error")
	}
	if after.Placement != "gpu" || after.Priority != 5 || after.NVectors != 1000 || after.State != "leased" || after.Attempt != c.Attempt {
		t.Errorf("invariant 6: build row must be unchanged")
	}
	if len(gotJobs) != 2 || gotJobs[0].DurationMs != 100 || gotJobs[1].DurationMs != 200 || !gotJobs[1].NeedsIndex {
		t.Errorf("go doc SubmitBuild: duplicate leaves the follow-up jobs untouched; got %+v", gotJobs)
	}
}

// Spec: decision 7, memory constraint enforced in the claim's WHERE.
func TestClaimSkipsBuildsThatDoNotFit(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 2)
	big, small := gpuBuild(r, 1, 10, 1000), gpuBuild(r, 2, 1, 100)
	mustSubmit(t, ctx, st, big, nil)
	mustSubmit(t, ctx, st, small, nil)
	c, ok, _ := st.Claim(ctx, "small-w", 500, lease30)
	t.Logf("worker with 500 bytes claims: ok=%v got %s (queue: big=1000B prio 10, small=100B prio 1)", ok, c.BuildID)
	if !ok || c.BuildID != small.BuildID {
		t.Errorf("decision 7: small worker should skip the higher-priority build that does not fit and take %s", small.BuildID)
	}
	_, ok, _ = st.Claim(ctx, "small-w2", 500, lease30)
	t.Logf("another 500-byte worker: ok=%v; big row: %s", ok, fmtRow(mustGet(t, ctx, st, big.BuildID)))
	if ok {
		t.Errorf("decision 7: nothing left that fits in 500 bytes")
	}
	c, ok, _ = st.Claim(ctx, "big-w", 1000, lease30)
	t.Logf("worker with exactly 1000 bytes claims: ok=%v got %s", ok, c.BuildID)
	if !ok || c.BuildID != big.BuildID {
		t.Errorf("a worker whose capacity equals mem_bytes should be able to claim it (fits)")
	}
}

// Spec: decision 5 (local rows are never claimed or reaped); go doc CompleteLocal
// ("the guard is the placement and state").
func TestLocalBuildLifecycle(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 2)
	loc := gpuBuild(r, 1, 100, 1)
	loc.Placement = "local"
	gpu := gpuBuild(r, 2, 0, 1)
	mustSubmit(t, ctx, st, loc, nil)
	mustSubmit(t, ctx, st, gpu, nil)
	t.Logf("local submit: %s", fmtRow(mustGet(t, ctx, st, loc.BuildID)))
	if g := mustGet(t, ctx, st, loc.BuildID); g.State != "running" {
		t.Errorf("go doc SubmitBuild: local builds start running")
	}
	c, ok, _ := st.Claim(ctx, "w1", 1<<30, lease30)
	_, ok2, _ := st.Claim(ctx, "w2", 1<<30, lease30)
	reaped, _ := st.Reap(ctx)
	t.Logf("claims: first ok=%v got %q, second ok=%v; reap: %+v", ok, c.BuildID, ok2, reaped)
	if c.BuildID == loc.BuildID || ok2 || len(reaped) != 0 {
		t.Errorf("decision 5: a running local build is never claimed or reaped")
	}
	okGPU, _ := st.CompleteLocal(ctx, gpu.BuildID)
	t.Logf("CompleteLocal on the (leased) gpu build: %v; row %s", okGPU, fmtRow(mustGet(t, ctx, st, gpu.BuildID)))
	if okGPU {
		t.Errorf("CompleteLocal must refuse a gpu build")
	}
	ok1, _ := st.CompleteLocal(ctx, loc.BuildID)
	snap := mustGet(t, ctx, st, loc.BuildID)
	ok3, _ := st.CompleteLocal(ctx, loc.BuildID)
	after := mustGet(t, ctx, st, loc.BuildID)
	t.Logf("CompleteLocal twice: %v then %v; row %s", ok1, ok3, fmtRow(after))
	if !ok1 || ok3 || after.State != "done" || after.FinishedAt == nil || !after.FinishedAt.Equal(*snap.FinishedAt) {
		t.Errorf("local build completes exactly once; second call is false and leaves finished_at alone")
	}
}

// Spec: decision 16 + decision 18 + go doc FinishCompleteRounds: a round is
// stamped only when all n_shards builds are done/failed and all jobs finished;
// finished_at = latest child finish; idempotent.
func TestRoundFinishesOnlyWhenAllExpectedShardsDone(t *testing.T) {
	ctx, _, st := setup(t)
	r := mustRound(t, ctx, st, 2)
	b1 := gpuBuild(r, 1, 0, 1)
	mustSubmit(t, ctx, st, b1, []store.JobSpec{{Seq: 0, DurationMs: 1, NeedsIndex: false}})
	c, _, _ := st.Claim(ctx, "w1", 1<<30, lease30)
	st.Complete(ctx, b1.BuildID, c.Attempt)
	st.StartJob(ctx, r, 1, 0)
	st.FinishJob(ctx, r, 1, 0)

	n, err := st.FinishCompleteRounds(ctx)
	rd, _, _ := st.GetRound(ctx, r)
	t.Logf("shard 1 of 2 fully done, shard 2 not yet submitted: FinishCompleteRounds=%d err=%v finished_at=%v", n, err, rd.FinishedAt)
	if n != 0 || rd.FinishedAt != nil {
		t.Errorf("decision 16: a round with n_shards=2 must not finish when only 1 shard has shown up")
	}

	b2 := gpuBuild(r, 2, 0, 1)
	b2.Placement = "local"
	mustSubmit(t, ctx, st, b2, []store.JobSpec{{Seq: 0, DurationMs: 1, NeedsIndex: true}})
	st.CompleteLocal(ctx, b2.BuildID)
	n, _ = st.FinishCompleteRounds(ctx)
	rd, _, _ = st.GetRound(ctx, r)
	t.Logf("shard 2 build done but its job not run: FinishCompleteRounds=%d finished_at=%v", n, rd.FinishedAt)
	if n != 0 || rd.FinishedAt != nil {
		t.Errorf("go doc FinishCompleteRounds: unfinished follow-up job must keep the round open")
	}

	st.StartJob(ctx, r, 2, 0)
	time.Sleep(20 * time.Millisecond)
	st.FinishJob(ctx, r, 2, 0)
	time.Sleep(50 * time.Millisecond)
	n, _ = st.FinishCompleteRounds(ctx)
	rd, _, _ = st.GetRound(ctx, r)
	n2, _ := st.FinishCompleteRounds(ctx)
	rd2, _, _ := st.GetRound(ctx, r)

	var latest time.Time
	builds, _ := st.ListBuilds(ctx, r)
	jobs, _ := st.ListJobs(ctx, r)
	for _, b := range builds {
		if b.FinishedAt != nil && b.FinishedAt.After(latest) {
			latest = *b.FinishedAt
		}
	}
	for _, j := range jobs {
		if j.FinishedAt != nil && j.FinishedAt.After(latest) {
			latest = *j.FinishedAt
		}
	}
	t.Logf("everything done: FinishCompleteRounds=%d finished_at=%v; latest child finish=%v; second call=%d finished_at=%v",
		n, rd.FinishedAt, latest, n2, rd2.FinishedAt)
	if n != 1 || rd.FinishedAt == nil {
		t.Fatalf("round should be stamped once every shard is done")
	}
	if !rd.FinishedAt.Equal(latest) {
		t.Errorf("decision 18: finished_at must equal the latest build/job finish (%v), got %v", latest, rd.FinishedAt)
	}
	if n2 != 0 || !rd2.FinishedAt.Equal(*rd.FinishedAt) {
		t.Errorf("FinishCompleteRounds is idempotent: second call should stamp 0 rounds and keep finished_at")
	}
}
