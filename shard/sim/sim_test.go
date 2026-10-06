package sim

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arqbca11/KubernetesGPU/db"
	"github.com/arqbca11/KubernetesGPU/experiments/workload"
	"github.com/arqbca11/KubernetesGPU/scheduler/api"
	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/policy"
	"github.com/arqbca11/KubernetesGPU/scheduler/reaper"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
	"github.com/arqbca11/KubernetesGPU/shard/client"
)

// Integration tests: real scheduler API (httptest) and real Postgres, with an
// in-test fake worker completing GPU builds. Skipped without TEST_DATABASE_URL.

type harness struct {
	t    *testing.T
	pool *pgxpool.Pool
	st   *store.Store
	api  *client.Client
	log  *slog.Logger
	stop chan struct{}
	wg   sync.WaitGroup
}

func newHarness(t *testing.T) *harness {
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
	st := store.New(pool)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(api.New(st, policy.AlwaysGPU{}, costmodel.Default(), api.Config{WorkerStaleAfter: 30 * time.Second}, log))
	t.Cleanup(srv.Close)
	h := &harness{t: t, pool: pool, st: st, api: client.New(srv.URL), log: log, stop: make(chan struct{})}
	// A reaper tick loop, as the scheduler binary would run.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-tk.C:
				reaper.Tick(ctx, st, 5, log)
			}
		}
	}()
	t.Cleanup(func() { close(h.stop); h.wg.Wait() })
	return h
}

// fakeWorker claims and completes GPU builds after a scaled fake build time,
// like the Python worker would. memCap limits what it takes.
func (h *harness) fakeWorker(id string, memCap int64, timeScale float64) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ctx := context.Background()
		cm := costmodel.Default()
		for {
			select {
			case <-h.stop:
				return
			default:
			}
			h.st.Heartbeat(ctx, id, memCap) //nolint:errcheck
			c, ok, err := h.st.Claim(ctx, id, memCap, 10*time.Second)
			if err != nil || !ok {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			d := time.Duration(float64(cm.GPUTotal(c.NVectors, c.Dim)) / timeScale)
			h.t.Logf("  [worker %s] claimed %s attempt %d, building %s", id, c.BuildID, c.Attempt, d.Round(time.Millisecond))
			time.Sleep(d)
			h.st.Complete(ctx, c.BuildID, c.Attempt) //nolint:errcheck
		}
	}()
}

const scale = 20.0

func q(at, dur time.Duration, idx bool, kind workload.QueryKind, cq int64) workload.Query {
	return workload.Query{Arrival: at, Duration: dur, NeedsIndex: idx, Kind: kind, ClusterQueryID: cq}
}

// twoShards: shard 0 small build (gpu), shard 1 huge build that fits no worker (local).
// Durations are modeled seconds; at scale 20 the round takes a few real seconds.
func twoShards() workload.Workload {
	w := workload.Workload{
		Scenario: workload.Scenario{Name: "test-two", NShards: 2, Dim: 128,
			Stream: workload.StreamProfile{Horizon: 20 * time.Second}},
		Seed: 1,
		Shards: []workload.ShardWorkload{
			{ShardID: 0, NVectors: 100_000, Queries: []workload.Query{ // gpu: ~4.6 s modeled
				q(1*time.Second, 2*time.Second, false, workload.KindFanOut, 1),
				q(2*time.Second, 2*time.Second, true, workload.KindFanOut, 2),
				q(6*time.Second, 1*time.Second, false, workload.KindSingleShard, 3),
			}},
			{ShardID: 1, NVectors: 20_000_000, Queries: []workload.Query{ // 19 GiB: local (~520 s modeled cpu... too long)
				q(1*time.Second, 2*time.Second, false, workload.KindFanOut, 1),
				q(2*time.Second, 2*time.Second, true, workload.KindFanOut, 2),
			}},
		},
	}
	return w
}

func TestTwoShardRoundEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.fakeWorker("gpu-1", 8<<30, scale)
	time.Sleep(100 * time.Millisecond) // let the worker heartbeat so the pool is non-empty

	// Shard 1's 20M build is 520 s modeled on CPU: too slow for a test. Use a
	// 4M build instead (~ 1,230 s cpu? no). Pick sizes against the cost model:
	// 300k x 128: cpu ~ 67 s modeled -> 3.4 s at 20x; gpu would be 8.6 s modeled.
	w := twoShards()
	w.Shards[1].NVectors = 300_000
	w.Shards[1].Queries = append(w.Shards[1].Queries, q(3*time.Second, 1*time.Second, false, workload.KindSingleShard, 4))
	// Force shard 1 local: a 1-byte worker is the only live one? No: policy v0
	// places local only when the build fits no worker. 300k x 128 needs ~300 MB,
	// which fits 8 GiB. So both go to the GPU in this test; locality is tested
	// in TestLocalBuildBlocksThenDrains.
	cfg := RoundConfig{Shard: Config{TimeScale: scale, PollInterval: 100 * time.Millisecond, Slice: 20 * time.Millisecond},
		MinWorkers: 1, WaitWorkers: 5 * time.Second, RoundTimeout: 60 * time.Second}
	tl, err := RunRound(context.Background(), h.api, w, cfg, h.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", tl.Text())
	for _, s := range tl.Shards {
		for _, e := range s.Events {
			t.Logf("  shard %d +%5.2fs %s", s.ShardID, e.At.Seconds(), e.Msg)
		}
	}
	if tl.Duration <= 0 {
		t.Fatal("round not finished")
	}
	if tl.BuildsGPU != 2 {
		t.Fatalf("both builds should be gpu with an 8 GiB worker; got gpu=%d local=%d", tl.BuildsGPU, tl.BuildsLocal)
	}
	for _, s := range tl.Shards {
		if s.BuildState != "done" || s.Attempt != 1 {
			t.Fatalf("shard %d build %s attempt %d", s.ShardID, s.BuildState, s.Attempt)
		}
		for _, ql := range s.Queries {
			if ql.Finished < 0 {
				t.Fatalf("shard %d query %d never finished", s.ShardID, ql.Seq)
			}
			if ql.NeedsIndex && ql.Started < s.BuildDone {
				t.Fatalf("shard %d query %d needs the index but started at %s before the build finished at %s", s.ShardID, ql.Seq, ql.Started, s.BuildDone)
			}
			if !ql.NeedsIndex && ql.Arrived < s.BuildDone && ql.Wait > 500*time.Millisecond && ql.Seq == 0 {
				t.Fatalf("shard %d query %d does not need the index yet waited %s with a free CPU (offloaded build)", s.ShardID, ql.Seq, ql.Wait)
			}
		}
	}
	// Cluster query 2 (fan-out, needs index) spans both shards: its latency is the slowest piece's.
	var cl *ClusterLine
	for i := range tl.ClusterQueries {
		if tl.ClusterQueries[i].ClusterQueryID == 2 {
			cl = &tl.ClusterQueries[i]
		}
	}
	if cl == nil || cl.Pieces != 2 {
		t.Fatalf("cluster query 2 should have 2 pieces: %+v", cl)
	}
	t.Logf("cluster query 2 (fan-out, needs_index) latency = slowest piece = %s", cl.Latency.Round(time.Millisecond))
	if tl.Latency.FanOutN != 2 || tl.Latency.SingleN != 2 {
		t.Fatalf("latency counts: fanout=%d single=%d", tl.Latency.FanOutN, tl.Latency.SingleN)
	}
}

// With a tiny worker nothing fits, so every build is local; queries that need
// the index wait for the local build, the others also wait because the CPU is
// busy (decision 32: local build occupies the CPU).
func TestLocalBuildBlocksThenDrains(t *testing.T) {
	h := newHarness(t)
	h.fakeWorker("tiny", 1, scale)
	time.Sleep(100 * time.Millisecond)
	w := workload.Workload{
		Scenario: workload.Scenario{Name: "test-local", NShards: 1, Dim: 128, Stream: workload.StreamProfile{Horizon: 20 * time.Second}},
		Seed:     1,
		Shards: []workload.ShardWorkload{{ShardID: 0, NVectors: 100_000, Queries: []workload.Query{ // cpu 20.6 s modeled -> ~1 s at 20x
			q(1*time.Second, 2*time.Second, false, workload.KindSingleShard, 1),
			q(2*time.Second, 2*time.Second, true, workload.KindSingleShard, 2),
		}}},
	}
	cfg := RoundConfig{Shard: Config{TimeScale: scale, PollInterval: 100 * time.Millisecond, Slice: 20 * time.Millisecond},
		MinWorkers: 1, WaitWorkers: 5 * time.Second, RoundTimeout: 60 * time.Second}
	tl, err := RunRound(context.Background(), h.api, w, cfg, h.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", tl.Text())
	for _, e := range tl.Shards[0].Events {
		t.Logf("  +%5.2fs %s", e.At.Seconds(), e.Msg)
	}
	s := tl.Shards[0]
	if s.Placement != "local" || s.BuildState != "done" {
		t.Fatalf("expected a done local build, got %s/%s", s.Placement, s.BuildState)
	}
	for _, ql := range s.Queries {
		if ql.Started < s.BuildDone {
			t.Fatalf("query %d started at %s while the local build ran until %s: the CPU was busy", ql.Seq, ql.Started, s.BuildDone)
		}
	}
	t.Log("both queries waited for the local build: the CPU was occupied (modeling assumption)")
}

// A local build is preempted to the GPU queue mid-way (as Phase 3's reconsider
// would do). The shard learns from its report reply, aborts the local build,
// and the worker builds it; the needs_index query runs after that.
func TestPreemptionAbortsLocalBuild(t *testing.T) {
	h := newHarness(t)
	h.fakeWorker("tiny", 1, scale) // nothing fits: builds go local
	time.Sleep(100 * time.Millisecond)
	w := workload.Workload{
		Scenario: workload.Scenario{Name: "test-preempt", NShards: 1, Dim: 128, Stream: workload.StreamProfile{Horizon: 30 * time.Second}},
		Seed:     1,
		Shards: []workload.ShardWorkload{{ShardID: 0, NVectors: 400_000, Queries: []workload.Query{ // cpu ~92 s modeled -> 4.6 s at 20x
			q(500*time.Millisecond, 1*time.Second, false, workload.KindSingleShard, 1),
			q(1*time.Second, 1*time.Second, true, workload.KindSingleShard, 2),
		}}},
	}
	cfg := RoundConfig{Shard: Config{TimeScale: scale, PollInterval: 100 * time.Millisecond, Slice: 20 * time.Millisecond},
		MinWorkers: 1, WaitWorkers: 5 * time.Second, RoundTimeout: 60 * time.Second}

	// Preempt once the local build is under way, then start a capable worker.
	go func() {
		ctx := context.Background()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			// Find the running local build (round ids keep counting across tests).
			var id string
			var progress float32
			err := h.pool.QueryRow(ctx, `SELECT b.build_id, COALESCE(s.build_progress, 0) FROM builds b
				LEFT JOIN shard_status s ON s.round_id = b.round_id AND s.shard_id = b.shard_id
				WHERE b.placement = 'local' AND b.state = 'running' LIMIT 1`).Scan(&id, &progress)
			if err == nil && progress > 0.2 {
				ok, _ := h.st.PreemptToGPU(ctx, id, 50)
				h.t.Logf("  [reconsider] preempt %s at progress %.0f%% -> %v", id, 100*progress, ok)
				h.fakeWorker("big", 8<<30, scale)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	tl, err := RunRound(context.Background(), h.api, w, cfg, h.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", tl.Text())
	var aborted, ready bool
	for _, e := range tl.Shards[0].Events {
		t.Logf("  +%5.2fs %s", e.At.Seconds(), e.Msg)
		aborted = aborted || strings.Contains(e.Msg, "aborted")
		ready = ready || strings.Contains(e.Msg, "index ready")
	}
	s := tl.Shards[0]
	if !aborted || !ready || s.Placement != "gpu" || s.BuildState != "done" || s.Attempt != 1 {
		t.Fatalf("expected an aborted local build then a gpu completion at attempt 1; got aborted=%v ready=%v %s/%s attempt %d",
			aborted, ready, s.Placement, s.BuildState, s.Attempt)
	}
	q0, q1 := s.Queries[0], s.Queries[1]
	if q0.Started >= s.BuildDone {
		t.Fatalf("query 0 (no index needed) should have run once the CPU was freed by the preemption, before the gpu build finished at %s; started %s", s.BuildDone, q0.Started)
	}
	if q1.Started < s.BuildDone {
		t.Fatalf("query 1 (needs index) started %s before the gpu build finished %s", q1.Started, s.BuildDone)
	}
	t.Log("preemption freed the CPU: the independent query ran during the gpu build; the index query waited for it")
}

// Decisions 59 and 60: with a staggered DDL, queries that arrive before the
// shard's DDL run on the CPU as normal and are backfilled, with their own
// timestamps, once the build exists. Nothing about a query crosses HTTP while
// it runs, so recorded durations are exact.
func TestPreDDLQueriesRunAndAreBackfilled(t *testing.T) {
	h := newHarness(t)
	h.fakeWorker("gpu-1", 8<<30, scale)
	time.Sleep(100 * time.Millisecond)
	w := workload.Workload{
		Scenario: workload.Scenario{Name: "test-staggered", NShards: 1, Dim: 128,
			Stream: workload.StreamProfile{Horizon: 60 * time.Second}, DDL: workload.ArrivalPattern{Kind: "staggered", Spread: 20 * time.Second}},
		Seed: 1,
		Shards: []workload.ShardWorkload{{ShardID: 0, NVectors: 100_000, DDLOffset: 20 * time.Second, Queries: []workload.Query{ // DDL at 1 s real
			q(4*time.Second, 6*time.Second, false, workload.KindSingleShard, 1),  // arrives 0.2 s, runs 0.3 s: before the DDL
			q(10*time.Second, 6*time.Second, true, workload.KindSingleShard, 2),  // arrives 0.5 s, needs the index: waits for the gpu build
			q(30*time.Second, 6*time.Second, false, workload.KindSingleShard, 3), // after the DDL
		}}},
	}
	cfg := RoundConfig{Shard: Config{TimeScale: scale, PollInterval: 100 * time.Millisecond, Slice: 20 * time.Millisecond},
		MinWorkers: 1, WaitWorkers: 5 * time.Second, RoundTimeout: 60 * time.Second}
	tl, err := RunRound(context.Background(), h.api, w, cfg, h.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", tl.Text())
	s := tl.Shards[0]
	for _, e := range s.Events {
		t.Logf("  +%5.2fs %s", e.At.Seconds(), e.Msg)
	}
	q0, q1, q2 := s.Queries[0], s.Queries[1], s.Queries[2]
	if q0.Arrived >= s.Submitted || q0.Finished >= s.Submitted {
		t.Fatalf("query 0 should have arrived and finished before the DDL (submitted at %s): arrived %s finished %s", s.Submitted, q0.Arrived, q0.Finished)
	}
	if q1.Arrived >= s.Submitted || q1.Started < s.BuildDone {
		t.Fatalf("query 1 arrived before the DDL (%s < %s) but needs the index, so must start after the build finished (%s); started %s", q1.Arrived, s.Submitted, s.BuildDone, q1.Started)
	}
	if q2.Arrived < s.Submitted {
		t.Fatalf("query 2 should arrive after the DDL")
	}
	// Recorded run time is exact at this time scale: 6 s modeled / 20 = 300 ms.
	for _, ql := range s.Queries {
		run := ql.Finished - ql.Started
		if run < 290*time.Millisecond || run > 340*time.Millisecond {
			t.Fatalf("query %d recorded run time %s, want ~300 ms (no HTTP inside the measured interval)", ql.Seq, run)
		}
	}
	t.Logf("pre-DDL queries ran before the submit and were backfilled with their true times; run times %s, %s, %s (modeled 6 s / %g)",
		(q0.Finished - q0.Started).Round(time.Millisecond), (q1.Finished - q1.Started).Round(time.Millisecond), (q2.Finished - q2.Started).Round(time.Millisecond), scale)
}
