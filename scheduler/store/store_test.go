package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arqbca11/KubernetesGPU/db"
)

// Integration tests. Skipped unless TEST_DATABASE_URL points at a Postgres
// the test may wipe. scripts/test-db.sh starts one in a container.

const lease = 30 * time.Second

func newStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; run scripts/test-db.sh")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE shard_status, shard_jobs, builds, rounds, workers`); err != nil {
		t.Fatal(err)
	}
	return New(pool), pool
}

func submit(t *testing.T, s *Store, round int64, shard int32, placement string, priority float64, mem int64) string {
	t.Helper()
	id := BuildID(shard, round)
	ins, err := s.SubmitBuild(context.Background(), Build{
		BuildID: id, RoundID: round, ShardID: shard, NVectors: 1000, Dim: 128,
		MemBytes: mem, Placement: placement, Priority: priority,
	})
	if err != nil || !ins {
		t.Fatalf("submit %s: inserted=%v err=%v", id, ins, err)
	}
	t.Logf("submitted build %s: placement=%s priority=%v mem=%d", id, placement, priority, mem)
	return id
}

func state(t *testing.T, pool *pgxpool.Pool, id string) (st string, attempt int32) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		`SELECT state, attempt FROM builds WHERE build_id = $1`, id).Scan(&st, &attempt); err != nil {
		t.Fatal(err)
	}
	return
}

// reap runs the reaper and returns how many leases it returned to the queue.
func reap(t *testing.T, s *Store) int {
	t.Helper()
	r, err := s.Reap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range r {
		t.Logf("  [reaper] build %s attempt %d (was leased to %s) -> queued", x.BuildID, x.Attempt, x.LeaseOwner)
	}
	return len(r)
}

func expireLease(t *testing.T, pool *pgxpool.Pool, id string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE builds SET lease_until = now() - interval '1 second' WHERE build_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	t.Logf("  (test) forced lease of %s into the past, as if the worker had stopped renewing", id)
}

// row logs the build row as Postgres has it right now.
func row(t *testing.T, pool *pgxpool.Pool, id string) {
	t.Helper()
	var st, placement string
	var attempt int32
	var owner *string
	var until *time.Time
	if err := pool.QueryRow(context.Background(),
		`SELECT state, placement, attempt, lease_owner, lease_until FROM builds WHERE build_id = $1`, id).
		Scan(&st, &placement, &attempt, &owner, &until); err != nil {
		t.Fatal(err)
	}
	o, u := "-", "-"
	if owner != nil {
		o = *owner
	}
	if until != nil {
		u = time.Until(*until).Round(time.Second).String()
	}
	t.Logf("  [db] build %s: placement=%s state=%-7s attempt=%d lease_owner=%s lease_expires_in=%s",
		id, placement, st, attempt, o, u)
}

func TestClaimOrdersByPriorityThenAge(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	low := submit(t, s, r, 1, "gpu", 1, 1<<20)
	high := submit(t, s, r, 2, "gpu", 5, 1<<20)
	mid := submit(t, s, r, 3, "gpu", 3, 1<<20)

	var got []string
	for i := range 3 {
		c, ok, err := s.Claim(ctx, "w", 1<<30, lease)
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		if c.Attempt != 1 {
			t.Fatalf("first claim of %s: attempt=%d", c.BuildID, c.Attempt)
		}
		t.Logf("claim #%d by worker w -> %s (attempt %d)", i+1, c.BuildID, c.Attempt)
		got = append(got, c.BuildID)
	}
	want := []string{high, mid, low}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("claim order: got %v want %v", got, want)
		}
	}
	t.Logf("order was %v: priority 5, then 3, then 1, as expected", got)
	if _, ok, _ := s.Claim(ctx, "w", 1<<30, lease); ok {
		t.Fatal("claim on empty queue returned a build")
	}
	t.Log("claim #4 on the empty queue -> nothing, as expected")
}

func TestConcurrentClaimsNeverShareABuild(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	const n = 20
	for i := range int32(n) {
		if _, err := s.SubmitBuild(ctx, Build{BuildID: BuildID(i, r), RoundID: r, ShardID: i,
			NVectors: 1000, Dim: 128, MemBytes: 1 << 20, Placement: "gpu"}); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("submitted %d gpu builds, all priority 0", n)
	t.Logf("starting 8 workers that claim in a loop until the queue is empty")

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c, ok, err := s.Claim(ctx, "w"+string(rune('a'+w)), 1<<30, lease)
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				seen[c.BuildID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("claimed %d distinct builds, want %d", len(seen), n)
	}
	for id, k := range seen {
		if k != 1 {
			t.Fatalf("build %s claimed %d times", id, k)
		}
	}
	t.Logf("all %d builds claimed, each exactly once: FOR UPDATE SKIP LOCKED kept the workers apart", n)
}

// The core of Phase 1: a paused or dead worker's late writes are rejected.
func TestFencingAfterReap(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	id := submit(t, s, r, 1, "gpu", 0, 1<<20)

	c1, ok, _ := s.Claim(ctx, "worker-1", 1<<30, lease)
	if !ok || c1.Attempt != 1 {
		t.Fatalf("first claim: ok=%v attempt=%d", ok, c1.Attempt)
	}
	t.Logf("worker-1 claims -> attempt %d, lease %s", c1.Attempt, lease)
	row(t, pool, id)

	// Lease still live: nothing to reap.
	n := reap(t, s)
	if n != 0 {
		t.Fatalf("reaped %d live leases", n)
	}
	t.Logf("reaper runs while the lease is live -> %d rows changed", n)

	// worker-1 goes silent (kill -9 or SIGSTOP). Lease expires.
	t.Log("worker-1 stops renewing (kill -9, or SIGSTOP)")
	expireLease(t, pool, id)
	row(t, pool, id)
	n = reap(t, s)
	if n != 1 {
		t.Fatalf("reap: got %d want 1", n)
	}
	t.Logf("reaper runs after expiry -> %d row back to queued", n)
	if st, _ := state(t, pool, id); st != "queued" {
		t.Fatalf("after reap: state=%s", st)
	}
	row(t, pool, id)
	// Reaper is idempotent.
	n = reap(t, s)
	if n != 0 {
		t.Fatalf("second reap changed %d rows", n)
	}
	t.Logf("reaper runs again -> %d rows changed (idempotent)", n)

	c2, ok, _ := s.Claim(ctx, "worker-2", 1<<30, lease)
	if !ok || c2.Attempt != 2 || c2.BuildID != id {
		t.Fatalf("second claim: ok=%v attempt=%d id=%s", ok, c2.Attempt, c2.BuildID)
	}
	t.Logf("worker-2 claims -> attempt %d (the fencing token moved on)", c2.Attempt)
	row(t, pool, id)

	// worker-1 wakes up (SIGCONT) and tries to carry on with attempt 1.
	t.Logf("worker-1 wakes up (SIGCONT) still holding attempt %d and tries to carry on:", c1.Attempt)
	ok, _ = s.Renew(ctx, id, c1.Attempt, lease)
	if ok {
		t.Fatal("stale renew succeeded")
	}
	t.Logf("  renew(attempt %d)    -> rejected (0 rows)", c1.Attempt)
	ok, _ = s.Complete(ctx, id, c1.Attempt)
	if ok {
		t.Fatal("stale complete succeeded")
	}
	t.Logf("  complete(attempt %d) -> rejected (0 rows)", c1.Attempt)
	ok, _ = s.Fail(ctx, id, c1.Attempt, "late")
	if ok {
		t.Fatal("stale fail succeeded")
	}
	t.Logf("  fail(attempt %d)     -> rejected (0 rows)", c1.Attempt)
	if st, a := state(t, pool, id); st != "leased" || a != 2 {
		t.Fatalf("stale writes changed the row: state=%s attempt=%d", st, a)
	}
	row(t, pool, id)

	// The real owner proceeds.
	ok, _ = s.Renew(ctx, id, c2.Attempt, lease)
	if !ok {
		t.Fatal("live renew rejected")
	}
	t.Logf("worker-2 renew(attempt %d)    -> accepted", c2.Attempt)
	ok, _ = s.Complete(ctx, id, c2.Attempt)
	if !ok {
		t.Fatal("live complete rejected")
	}
	t.Logf("worker-2 complete(attempt %d) -> accepted", c2.Attempt)
	if st, _ := state(t, pool, id); st != "done" {
		t.Fatalf("after complete: state=%s", st)
	}
	row(t, pool, id)
	// Completing twice is a no-op, and the done row is never reaped.
	ok, _ = s.Complete(ctx, id, c2.Attempt)
	if ok {
		t.Fatal("second complete succeeded")
	}
	t.Log("worker-2 completes again       -> rejected (already done)")
	n = reap(t, s)
	if n != 0 {
		t.Fatalf("reap touched a done build")
	}
	t.Logf("reaper runs on the done build -> %d rows changed", n)
	t.Log("result: the build finished exactly once, by worker-2")
}

func TestReleaseReturnsToQueueAndFences(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	id := submit(t, s, r, 1, "gpu", 0, 1<<20)

	c1, _, _ := s.Claim(ctx, "w1", 1<<30, lease)
	t.Logf("w1 claims -> attempt %d", c1.Attempt)
	if ok, _ := s.Release(ctx, id, c1.Attempt); !ok {
		t.Fatal("release rejected")
	}
	t.Log("w1 gets SIGTERM and releases the build instead of finishing")
	if st, _ := state(t, pool, id); st != "queued" {
		t.Fatalf("after release: state=%s", st)
	}
	row(t, pool, id)
	c2, _, _ := s.Claim(ctx, "w2", 1<<30, lease)
	if c2.Attempt != 2 {
		t.Fatalf("attempt after release+claim = %d", c2.Attempt)
	}
	t.Logf("w2 claims -> attempt %d", c2.Attempt)
	if ok, _ := s.Complete(ctx, id, c1.Attempt); ok {
		t.Fatal("old owner completed after release")
	}
	t.Logf("w1 complete(attempt %d) -> rejected", c1.Attempt)
}

func TestClaimRespectsWorkerMemory(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	big := submit(t, s, r, 1, "gpu", 10, 16<<30) // 16 GiB, high priority
	small := submit(t, s, r, 2, "gpu", 0, 1<<30)

	// An 8 GiB worker skips the big build even though it is first in line.
	c, ok, _ := s.Claim(ctx, "small-gpu", 8<<30, lease)
	if !ok || c.BuildID != small {
		t.Fatalf("8 GiB worker claimed %q ok=%v, want %s", c.BuildID, ok, small)
	}
	t.Logf("8 GiB worker claims -> %s (skipped the 16 GiB build that was first in line)", c.BuildID)
	c, ok, _ = s.Claim(ctx, "big-gpu", 24<<30, lease)
	if !ok || c.BuildID != big {
		t.Fatalf("24 GiB worker claimed %q ok=%v, want %s", c.BuildID, ok, big)
	}
	t.Logf("24 GiB worker claims -> %s", c.BuildID)
}

func TestDuplicateSubmitIsNoop(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	b := Build{BuildID: BuildID(1, r), RoundID: r, ShardID: 1, NVectors: 10, Dim: 4, MemBytes: 1, Placement: "gpu", Priority: 7}
	if ins, err := s.SubmitBuild(ctx, b); err != nil || !ins {
		t.Fatalf("first submit: %v %v", ins, err)
	}
	t.Logf("submitted %s as gpu, priority 7", b.BuildID)
	c, _, _ := s.Claim(ctx, "w", 1<<30, lease)
	t.Logf("worker claims it -> attempt %d", c.Attempt)
	row(t, pool, b.BuildID)

	b.Priority = 99
	b.Placement = "local"
	ins, err := s.SubmitBuild(ctx, b)
	if err != nil || ins {
		t.Fatalf("second submit: inserted=%v err=%v", ins, err)
	}
	t.Logf("shard resubmits the same build_id as local, priority 99 -> inserted=%v", ins)
	var prio float64
	var placement string
	st, a := state(t, pool, b.BuildID)
	pool.QueryRow(ctx, `SELECT priority, placement FROM builds WHERE build_id = $1`, b.BuildID).Scan(&prio, &placement) //nolint:errcheck
	if st != "leased" || a != c.Attempt || prio != 7 || placement != "gpu" {
		t.Fatalf("resubmit changed the row: state=%s attempt=%d priority=%v placement=%s", st, a, prio, placement)
	}
	row(t, pool, b.BuildID)
	t.Log("row unchanged: still gpu, priority 7, leased to the worker")
}

func TestLocalBuildsAreNeverReaped(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	id := submit(t, s, r, 1, "local", 0, 1<<20)
	if st, _ := state(t, pool, id); st != "running" {
		t.Fatalf("local build state=%s, want running", st)
	}
	row(t, pool, id)
	if _, ok, _ := s.Claim(ctx, "w", 1<<40, lease); ok {
		t.Fatal("a worker claimed a local build")
	}
	t.Log("a worker tries to claim -> nothing (local builds are not in the queue)")
	n := reap(t, s)
	if n != 0 {
		t.Fatalf("reap touched a local build")
	}
	t.Logf("reaper runs -> %d rows changed (local builds have no lease)", n)
	if ok, _ := s.CompleteLocal(ctx, id); !ok {
		t.Fatal("complete local rejected")
	}
	t.Log("shard reports the local build done -> accepted")
	if ok, _ := s.CompleteLocal(ctx, id); ok {
		t.Fatal("second complete local succeeded")
	}
	t.Log("shard reports it again -> rejected")
	row(t, pool, id)
}

func TestHeartbeatAndPoolState(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	p, _ := s.Pool(ctx, time.Minute)
	if p.LiveWorkers != 0 || p.LargestMem != 0 {
		t.Fatalf("no workers, got %+v", p)
	}
	t.Log("no workers registered -> pool is empty")
	s.Heartbeat(ctx, "a", 8<<30)  //nolint:errcheck
	s.Heartbeat(ctx, "b", 24<<30) //nolint:errcheck
	t.Log("workers a (8 GiB) and b (24 GiB) heartbeat")
	p, _ = s.Pool(ctx, time.Minute)
	if p.LiveWorkers != 2 || p.LargestMem != 24<<30 {
		t.Fatalf("pool = %+v", p)
	}
	t.Logf("pool within 1 minute -> %d live, largest 24 GiB", p.LiveWorkers)
	// b goes stale.
	pool.Exec(ctx, `UPDATE workers SET last_seen = now() - interval '10 minutes' WHERE worker_id = 'b'`) //nolint:errcheck
	t.Log("  (test) b's last_seen pushed 10 minutes into the past, as if it had died")
	p, _ = s.Pool(ctx, time.Minute)
	if p.LiveWorkers != 1 || p.LargestMem != 8<<30 {
		t.Fatalf("pool after b stale = %+v", p)
	}
	t.Logf("pool within 1 minute -> %d live, largest 8 GiB (b no longer counts)", p.LiveWorkers)
	// Heartbeat is an upsert.
	if err := s.Heartbeat(ctx, "a", 8<<30); err != nil {
		t.Fatal(err)
	}
	ws, _ := s.ListWorkers(ctx)
	if len(ws) != 2 {
		t.Fatalf("ListWorkers = %d rows", len(ws))
	}
}

func TestRoundFinishesOnlyWhenEveryShardIsDone(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 2)
	t.Log("round expects 2 shards (decision 30: done = all builds terminal, all shards stream_done, all queries finished)")
	if _, err := s.SubmitBuild(ctx, Build{BuildID: BuildID(1, r), RoundID: r, ShardID: 1, NVectors: 10, Dim: 4, MemBytes: 1, Placement: "gpu"}); err != nil {
		t.Fatal(err)
	}
	t.Log("shard 1 submits a gpu build; two queries arrive at it over time")
	s.RecordArrival(ctx, r, 1, 0, 100, false) //nolint:errcheck
	s.RecordArrival(ctx, r, 1, 1, 100, true)  //nolint:errcheck
	report := func(shard int32, depth int32, done bool) {
		if err := s.ReportStatus(ctx, ShardStatus{RoundID: r, ShardID: shard, QueueDepth: depth, StreamDone: done}); err != nil {
			t.Fatal(err)
		}
	}

	finish := func(label string) int64 {
		n, err := s.FinishCompleteRounds(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rd, _, _ := s.GetRound(ctx, r)
		done := "open"
		if rd.FinishedAt != nil {
			done = "finished"
		}
		t.Logf("%-62s -> rounds stamped: %d, round is %s", label, n, done)
		return n
	}
	finish("nothing done yet")
	c, _, _ := s.Claim(ctx, "w", 1<<30, lease)
	s.Complete(ctx, c.BuildID, c.Attempt) //nolint:errcheck
	s.StartJob(ctx, r, 1, 0)              //nolint:errcheck
	s.FinishJob(ctx, r, 1, 0)             //nolint:errcheck
	s.StartJob(ctx, r, 1, 1)              //nolint:errcheck
	s.FinishJob(ctx, r, 1, 1)             //nolint:errcheck
	report(1, 0, true)
	if n := finish("shard 1 build done, queries finished, stream_done; shard 2 absent"); n != 0 {
		t.Fatal("round finished before shard 2 even submitted")
	}
	if _, err := s.SubmitBuild(ctx, Build{BuildID: BuildID(2, r), RoundID: r, ShardID: 2, NVectors: 10, Dim: 4, MemBytes: 1, Placement: "local"}); err != nil {
		t.Fatal(err)
	}
	s.RecordArrival(ctx, r, 2, 0, 100, false) //nolint:errcheck
	report(2, 1, false)
	if n := finish("shard 2 submits a local build, one query waiting, stream not done"); n != 0 {
		t.Fatal("round finished with a running build")
	}
	s.CompleteLocal(ctx, BuildID(2, r)) //nolint:errcheck
	s.StartJob(ctx, r, 2, 0)            //nolint:errcheck
	s.FinishJob(ctx, r, 2, 0)           //nolint:errcheck
	report(2, 0, false)
	if n := finish("shard 2 build done, query finished, but stream NOT reported done"); n != 0 {
		t.Fatal("round finished before shard 2 said its stream was exhausted")
	}
	report(2, 0, true)
	if n := finish("shard 2 reports stream_done with an empty queue"); n != 1 {
		t.Fatal("round did not finish")
	}
	if n := finish("run again"); n != 0 {
		t.Fatal("finish is not idempotent")
	}
	var ok bool
	pool.QueryRow(ctx, `
		SELECT r.finished_at = (SELECT MAX(finished_at) FROM shard_jobs WHERE round_id = r.round_id)
		FROM rounds r WHERE round_id = $1`, r).Scan(&ok) //nolint:errcheck
	if !ok {
		t.Fatal("round finished_at is not the last query's finished_at")
	}
	t.Log("round finished_at equals the last query's finished_at, not the time of the stream_done report")
	if ok, _ := s.FinishJob(ctx, r, 1, 1); ok {
		t.Fatal("finished an already-finished job")
	}
}

func TestPreemptAndRenegeTransitions(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1, 100)
	id := submit(t, s, r, 1, "local", 0, 1<<20)
	row(t, pool, id)

	t.Log("decision 31: preempt a running local build to the gpu queue")
	if ok, _ := s.PreemptToGPU(ctx, id, 42); !ok {
		t.Fatal("preempt rejected")
	}
	row(t, pool, id)
	var prio float64
	pool.QueryRow(ctx, `SELECT priority FROM builds WHERE build_id = $1`, id).Scan(&prio) //nolint:errcheck
	if st, a := state(t, pool, id); st != "queued" || a != 0 || prio != 42 {
		t.Fatalf("after preempt: state=%s attempt=%d priority=%v", st, a, prio)
	}
	if ok, _ := s.PreemptToGPU(ctx, id, 1); ok {
		t.Fatal("preempted a build that is no longer a running local build")
	}
	t.Log("preempting again -> rejected (not running/local any more)")
	if ok, _ := s.CompleteLocal(ctx, id); ok {
		t.Fatal("the shard completed a build that was taken away from it")
	}
	t.Log("the shard, unaware, reports its local build done -> rejected: that is how it learns")

	t.Log("renege the queued gpu build back to local")
	if ok, _ := s.RenegeToLocal(ctx, id); !ok {
		t.Fatal("renege rejected")
	}
	row(t, pool, id)
	if _, ok, _ := s.Claim(ctx, "w", 1<<40, lease); ok {
		t.Fatal("a worker claimed a build that was moved back to local")
	}
	t.Log("a worker tries to claim -> nothing; the build is local again")

	t.Log("a leased build is never moved")
	s.PreemptToGPU(ctx, id, 99) //nolint:errcheck
	c, ok, _ := s.Claim(ctx, "w", 1<<40, lease)
	if !ok || c.Attempt != 1 {
		t.Fatalf("claim after preempt: ok=%v attempt=%d", ok, c.Attempt)
	}
	row(t, pool, id)
	if ok, _ := s.RenegeToLocal(ctx, id); ok {
		t.Fatal("reneged a leased build")
	}
	t.Logf("renege on a leased build -> rejected; attempt is %d, bumped by the claim, untouched by the moves", c.Attempt)
}
