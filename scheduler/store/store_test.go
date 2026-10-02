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
	if _, err := pool.Exec(ctx, `TRUNCATE builds, shard_jobs, rounds, workers`); err != nil {
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

func expireLease(t *testing.T, pool *pgxpool.Pool, id string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE builds SET lease_until = now() - interval '1 second' WHERE build_id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestClaimOrdersByPriorityThenAge(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1)
	low := submit(t, s, r, 1, "gpu", 1, 1<<20)
	high := submit(t, s, r, 2, "gpu", 5, 1<<20)
	mid := submit(t, s, r, 3, "gpu", 3, 1<<20)

	var got []string
	for range 3 {
		c, ok, err := s.Claim(ctx, "w", 1<<30, lease)
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		if c.Attempt != 1 {
			t.Fatalf("first claim of %s: attempt=%d", c.BuildID, c.Attempt)
		}
		got = append(got, c.BuildID)
	}
	want := []string{high, mid, low}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("claim order: got %v want %v", got, want)
		}
	}
	if _, ok, _ := s.Claim(ctx, "w", 1<<30, lease); ok {
		t.Fatal("claim on empty queue returned a build")
	}
}

func TestConcurrentClaimsNeverShareABuild(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1)
	const n = 20
	for i := range int32(n) {
		submit(t, s, r, i, "gpu", 0, 1<<20)
	}

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
}

// The core of Phase 1: a paused or dead worker's late writes are rejected.
func TestFencingAfterReap(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1)
	id := submit(t, s, r, 1, "gpu", 0, 1<<20)

	c1, ok, _ := s.Claim(ctx, "worker-1", 1<<30, lease)
	if !ok || c1.Attempt != 1 {
		t.Fatalf("first claim: ok=%v attempt=%d", ok, c1.Attempt)
	}

	// Lease still live: nothing to reap.
	if n, _ := s.Reap(ctx); n != 0 {
		t.Fatalf("reaped %d live leases", n)
	}

	// worker-1 goes silent (kill -9 or SIGSTOP). Lease expires.
	expireLease(t, pool, id)
	if n, _ := s.Reap(ctx); n != 1 {
		t.Fatalf("reap: got %d want 1", n)
	}
	if st, _ := state(t, pool, id); st != "queued" {
		t.Fatalf("after reap: state=%s", st)
	}
	// Reaper is idempotent.
	if n, _ := s.Reap(ctx); n != 0 {
		t.Fatalf("second reap changed %d rows", n)
	}

	c2, ok, _ := s.Claim(ctx, "worker-2", 1<<30, lease)
	if !ok || c2.Attempt != 2 || c2.BuildID != id {
		t.Fatalf("second claim: ok=%v attempt=%d id=%s", ok, c2.Attempt, c2.BuildID)
	}

	// worker-1 wakes up (SIGCONT) and tries to carry on with attempt 1.
	if ok, _ := s.Renew(ctx, id, c1.Attempt, lease); ok {
		t.Fatal("stale renew succeeded")
	}
	if ok, _ := s.Complete(ctx, id, c1.Attempt); ok {
		t.Fatal("stale complete succeeded")
	}
	if ok, _ := s.Fail(ctx, id, c1.Attempt, "late"); ok {
		t.Fatal("stale fail succeeded")
	}
	if st, a := state(t, pool, id); st != "leased" || a != 2 {
		t.Fatalf("stale writes changed the row: state=%s attempt=%d", st, a)
	}

	// The real owner proceeds.
	if ok, _ := s.Renew(ctx, id, c2.Attempt, lease); !ok {
		t.Fatal("live renew rejected")
	}
	if ok, _ := s.Complete(ctx, id, c2.Attempt); !ok {
		t.Fatal("live complete rejected")
	}
	if st, _ := state(t, pool, id); st != "done" {
		t.Fatalf("after complete: state=%s", st)
	}
	// Completing twice is a no-op, and the done row is never reaped.
	if ok, _ := s.Complete(ctx, id, c2.Attempt); ok {
		t.Fatal("second complete succeeded")
	}
	if n, _ := s.Reap(ctx); n != 0 {
		t.Fatalf("reap touched a done build")
	}
}

func TestReleaseReturnsToQueueAndFences(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1)
	id := submit(t, s, r, 1, "gpu", 0, 1<<20)

	c1, _, _ := s.Claim(ctx, "w1", 1<<30, lease)
	if ok, _ := s.Release(ctx, id, c1.Attempt); !ok {
		t.Fatal("release rejected")
	}
	if st, _ := state(t, pool, id); st != "queued" {
		t.Fatalf("after release: state=%s", st)
	}
	c2, _, _ := s.Claim(ctx, "w2", 1<<30, lease)
	if c2.Attempt != 2 {
		t.Fatalf("attempt after release+claim = %d", c2.Attempt)
	}
	if ok, _ := s.Complete(ctx, id, c1.Attempt); ok {
		t.Fatal("old owner completed after release")
	}
}

func TestClaimRespectsWorkerMemory(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1)
	big := submit(t, s, r, 1, "gpu", 10, 16<<30) // 16 GiB, high priority
	small := submit(t, s, r, 2, "gpu", 0, 1<<30)

	// An 8 GiB worker skips the big build even though it is first in line.
	c, ok, _ := s.Claim(ctx, "small-gpu", 8<<30, lease)
	if !ok || c.BuildID != small {
		t.Fatalf("8 GiB worker claimed %q ok=%v, want %s", c.BuildID, ok, small)
	}
	c, ok, _ = s.Claim(ctx, "big-gpu", 24<<30, lease)
	if !ok || c.BuildID != big {
		t.Fatalf("24 GiB worker claimed %q ok=%v, want %s", c.BuildID, ok, big)
	}
}

func TestDuplicateSubmitIsNoop(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1)
	b := Build{BuildID: BuildID(1, r), RoundID: r, ShardID: 1, NVectors: 10, Dim: 4, MemBytes: 1, Placement: "gpu", Priority: 7}
	if ins, err := s.SubmitBuild(ctx, b); err != nil || !ins {
		t.Fatalf("first submit: %v %v", ins, err)
	}
	c, _, _ := s.Claim(ctx, "w", 1<<30, lease)

	b.Priority = 99
	b.Placement = "local"
	if ins, err := s.SubmitBuild(ctx, b); err != nil || ins {
		t.Fatalf("second submit: inserted=%v err=%v", ins, err)
	}
	var prio float64
	var placement string
	st, a := state(t, pool, b.BuildID)
	pool.QueryRow(ctx, `SELECT priority, placement FROM builds WHERE build_id = $1`, b.BuildID).Scan(&prio, &placement) //nolint:errcheck
	if st != "leased" || a != c.Attempt || prio != 7 || placement != "gpu" {
		t.Fatalf("resubmit changed the row: state=%s attempt=%d priority=%v placement=%s", st, a, prio, placement)
	}
}

func TestLocalBuildsAreNeverReaped(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	r, _ := s.CreateRound(ctx, "test", 1)
	id := submit(t, s, r, 1, "local", 0, 1<<20)
	if st, _ := state(t, pool, id); st != "running" {
		t.Fatalf("local build state=%s, want running", st)
	}
	if _, ok, _ := s.Claim(ctx, "w", 1<<40, lease); ok {
		t.Fatal("a worker claimed a local build")
	}
	if n, _ := s.Reap(ctx); n != 0 {
		t.Fatalf("reap touched a local build")
	}
	if ok, _ := s.CompleteLocal(ctx, id); !ok {
		t.Fatal("complete local rejected")
	}
	if ok, _ := s.CompleteLocal(ctx, id); ok {
		t.Fatal("second complete local succeeded")
	}
}

func TestHeartbeatAndLargestLiveWorker(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	if m, _ := s.LargestLiveWorkerMem(ctx, time.Minute); m != 0 {
		t.Fatalf("no workers, got %d", m)
	}
	s.Heartbeat(ctx, "a", 8<<30)  //nolint:errcheck
	s.Heartbeat(ctx, "b", 24<<30) //nolint:errcheck
	if m, _ := s.LargestLiveWorkerMem(ctx, time.Minute); m != 24<<30 {
		t.Fatalf("largest live = %d", m)
	}
	// b goes stale.
	pool.Exec(ctx, `UPDATE workers SET last_seen = now() - interval '10 minutes' WHERE worker_id = 'b'`) //nolint:errcheck
	if m, _ := s.LargestLiveWorkerMem(ctx, time.Minute); m != 8<<30 {
		t.Fatalf("largest live after b stale = %d", m)
	}
	// Heartbeat is an upsert.
	if err := s.Heartbeat(ctx, "a", 8<<30); err != nil {
		t.Fatal(err)
	}
}
