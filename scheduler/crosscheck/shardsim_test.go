package crosscheck

// Step 5 cross-check: the shard simulator as a black-box process.
//
// Scheduler: option (a), in-process. The real scheduler API (policy AlwaysGPU,
// costmodel.Default) runs in an httptest server, a reaper loop calls
// reaper.Tick every 200 ms, and the GPU worker is played by fakeGPU through
// store.Heartbeat/Claim/Complete. The shardsim binary is built from
// ./shard/cmd/shardsim and pointed at the server with SCHEDULER_URL.
// Observation is through Postgres (store methods and SQL), the binary's
// stdout, and its TIMELINE_DIR JSON. Spec: docs/design/phase1-scheduler.md
// (decisions 28-33, 51-56, the API table, Step 5), CLAUDE.md, roadmap Phase 1.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/experiments/workload"
	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

const bigMem = int64(1) << 40 // a worker every build fits on

// ---- workload helpers ----

func genWorkload(t *testing.T, preset string, n int, seed int64) workload.Workload {
	t.Helper()
	sc, err := workload.PresetWithShards(preset, n)
	if err != nil {
		t.Fatalf("PresetWithShards(%s, %d): %v", preset, n, err)
	}
	w := workload.Generate(sc, seed)
	if err := w.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return w
}

// postDDL returns the queries the shard can record: Step 5 open question 4
// says queries arriving before the shard's DDL are dropped in Phase 1.
func postDDL(s workload.ShardWorkload) []workload.Query {
	var out []workload.Query
	for _, q := range s.Queries {
		if q.Arrival >= s.DDLOffset {
			out = append(out, q)
		}
	}
	return out
}

// jobsByShard returns the round's shard_jobs grouped by shard, sorted by seq.
func jobsByShard(t *testing.T, e *simEnv, round int64) map[int32][]store.JobRow {
	t.Helper()
	jobs, err := e.st.ListJobs(e.ctx, round)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	m := map[int32][]store.JobRow{}
	for _, j := range jobs {
		m[j.ShardID] = append(m[j.ShardID], j)
	}
	for k := range m {
		sort.Slice(m[k], func(a, b int) bool { return m[k][a].Seq < m[k][b].Seq })
	}
	return m
}

func buildsByShard(t *testing.T, e *simEnv, round int64) map[int32]store.BuildRow {
	t.Helper()
	bs, err := e.st.ListBuilds(e.ctx, round)
	if err != nil {
		t.Fatalf("ListBuilds: %v", err)
	}
	m := map[int32]store.BuildRow{}
	for _, b := range bs {
		m[b.ShardID] = b
	}
	return m
}

// checkRecordedMatchesWorkload: every post-DDL query of the workload is in
// shard_jobs exactly once (the PK makes duplicates impossible, so "once" is a
// count check), in arrival order, with the workload's needs_index and modeled
// duration, and was started and finished.
func checkRecordedMatchesWorkload(t *testing.T, w workload.Workload, jobs map[int32][]store.JobRow) {
	t.Helper()
	for _, s := range w.Shards {
		want := postDDL(s)
		got := jobs[s.ShardID]
		dropped := len(s.Queries) - len(want)
		t.Logf("shard %d: workload has %d queries (%d before DDL at %v, dropped per open question 4), shard_jobs has %d",
			s.ShardID, len(s.Queries), dropped, s.DDLOffset, len(got))
		if len(got) != len(want) {
			t.Errorf("shard %d: %d queries recorded, workload says %d after the DDL", s.ShardID, len(got), len(want))
			continue
		}
		mism := 0
		for i, j := range got {
			q := want[i]
			dm := q.Duration.Milliseconds()
			if j.NeedsIndex != q.NeedsIndex || j.DurationMs < dm-1 || j.DurationMs > dm+1 {
				if mism < 3 {
					t.Errorf("shard %d seq %d (arrival #%d): recorded needs_index=%v duration_ms=%d, workload needs_index=%v duration=%v (schema: duration_ms is the modeled CPU time)",
						s.ShardID, j.Seq, i, j.NeedsIndex, j.DurationMs, q.NeedsIndex, q.Duration)
				}
				mism++
			}
			if j.StartedAt == nil || j.FinishedAt == nil {
				t.Errorf("shard %d seq %d never ran: started=%v finished=%v", s.ShardID, j.Seq, tptr(j.StartedAt), tptr(j.FinishedAt))
				continue
			}
			if j.StartedAt.Before(j.ArrivedAt) || j.FinishedAt.Before(*j.StartedAt) {
				t.Errorf("shard %d seq %d timestamps out of order: arrived=%v started=%v finished=%v", s.ShardID, j.Seq, j.ArrivedAt, *j.StartedAt, *j.FinishedAt)
			}
		}
		if mism > 0 {
			t.Errorf("shard %d: %d of %d recorded queries disagree with the workload in arrival order", s.ShardID, mism, len(got))
		}
	}
}

func sumRun(jobs []store.JobRow) (run time.Duration, modeled time.Duration) {
	for _, j := range jobs {
		if j.StartedAt != nil && j.FinishedAt != nil {
			run += j.FinishedAt.Sub(*j.StartedAt)
		}
		modeled += time.Duration(j.DurationMs) * time.Millisecond
	}
	return
}

// ---- 1. determinism ----

var (
	pwHeader = regexp.MustCompile(`^(\S+), (\d+) shards, seed (-?\d+): horizon=(\S+) `)
	pwShard  = regexp.MustCompile(`^\s*shard\s+(\d+): n_vectors=\s*(\d+) ddl_offset=(\S+)\s+queries=\s*(\d+) \(fanout\s+(\d+), single\s+(\d+), local_ddl\s+(\d+); needs_index\s+(\d+)\)`)
	pwTotal  = regexp.MustCompile(`total shard-level queries: (\d+)`)
)

func TestShardsimPrintWorkloadMatchesGenerate(t *testing.T) {
	// Invariant 10 and decision 51: the workload is seeded and Generate is
	// pure, so the binary's PRINT_WORKLOAD summary in a separate process must
	// equal what Generate gives this process for the same preset/seed/shards.
	// No scheduler is needed: SCHEDULER_URL points at a closed port.
	cases := []struct {
		preset string
		seed   int64
		n      int
	}{{"skewed", 1, 6}, {"bimodal", 42, 8}, {"staggered", 7, 5}, {"downstream_heavy", 3, 50}}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s-seed%d-n%d", c.preset, c.seed, c.n), func(t *testing.T) {
			vars := map[string]string{"PRINT_WORKLOAD": "1", "SCENARIO": c.preset, "SEED": strconv.FormatInt(c.seed, 10),
				"N_SHARDS": strconv.Itoa(c.n), "SCHEDULER_URL": "http://127.0.0.1:1"}
			p1 := startShardsim(t, vars)
			if code := p1.wait(t, 30*time.Second); code != 0 {
				t.Fatalf("PRINT_WORKLOAD exit %d: %s", code, p1.stderr.String())
			}
			p2 := startShardsim(t, vars)
			if code := p2.wait(t, 30*time.Second); code != 0 {
				t.Fatalf("second run exit %d", code)
			}
			out := p1.stdout.String()
			if out != p2.stdout.String() {
				t.Errorf("two runs with the same preset/seed/shards printed different output (invariant 10)")
			}
			w := genWorkload(t, c.preset, c.n, c.seed)
			sc := bufio.NewScanner(strings.NewReader(out))
			seen := map[int]bool{}
			header, total := false, -1
			for sc.Scan() {
				line := sc.Text()
				if m := pwHeader.FindStringSubmatch(line); m != nil {
					header = true
					if m[1] != w.Scenario.Name || m[2] != strconv.Itoa(c.n) || m[3] != strconv.FormatInt(c.seed, 10) {
						t.Errorf("header %q disagrees with scenario %s, %d shards, seed %d", line, w.Scenario.Name, c.n, c.seed)
					}
					if h, err := time.ParseDuration(m[4]); err != nil || h != w.Scenario.Stream.Horizon {
						t.Errorf("header horizon %s, Generate says %v", m[4], w.Scenario.Stream.Horizon)
					}
					continue
				}
				if m := pwTotal.FindStringSubmatch(line); m != nil {
					total, _ = strconv.Atoi(m[1])
					continue
				}
				m := pwShard.FindStringSubmatch(line)
				if m == nil {
					continue
				}
				id, _ := strconv.Atoi(m[1])
				seen[id] = true
				if id >= len(w.Shards) {
					t.Errorf("printed shard %d, workload has %d shards", id, len(w.Shards))
					continue
				}
				s := w.Shards[id]
				nv, _ := strconv.ParseInt(m[2], 10, 64)
				off, err := time.ParseDuration(m[3])
				if err != nil {
					t.Errorf("shard %d ddl_offset %q: %v", id, m[3], err)
				}
				var fan, single, ddl, ni int
				for _, q := range s.Queries {
					switch q.Kind {
					case workload.KindFanOut:
						fan++
					case workload.KindSingleShard:
						single++
					case workload.KindLocalDDL:
						ddl++
					}
					if q.NeedsIndex {
						ni++
					}
				}
				got := [5]string{m[4], m[5], m[6], m[7], m[8]}
				want := [5]string{strconv.Itoa(len(s.Queries)), strconv.Itoa(fan), strconv.Itoa(single), strconv.Itoa(ddl), strconv.Itoa(ni)}
				if nv != s.NVectors || off != s.DDLOffset || got != want {
					t.Errorf("shard %d printed n_vectors=%d ddl=%v counts(q,fan,single,ddl,ni)=%v; Generate gives %d %v %v",
						id, nv, off, got, s.NVectors, s.DDLOffset, want)
				}
			}
			if !header {
				t.Errorf("no header line in output:\n%s", out)
			}
			if len(seen) != c.n {
				t.Errorf("printed %d shard lines, want %d", len(seen), c.n)
			}
			n := 0
			for _, s := range w.Shards {
				n += len(s.Queries)
			}
			if total != n {
				t.Errorf("printed total %d, Generate gives %d", total, n)
			}
			t.Logf("%s seed %d, %d shards: printed summary equals Generate (%d shard-level queries), byte-identical across two processes", c.preset, c.seed, c.n, n)
		})
	}
}

// ---- 2. a full GPU round ----

func TestShardsimGPURoundRecordsEveryQueryOnce(t *testing.T) {
	// Decisions 28, 30, 53, 55; roadmap step "Shard simulator". A default
	// (skewed, 6 shards, seed 1) round with one live big worker: every build
	// goes to the GPU and ends done at attempt 1; every workload query is
	// recorded once and finished; the round is stamped; one CPU per shard;
	// query durations shrink by TIME_SCALE.
	e := setupSim(t)
	const scale = 20
	g := startFakeGPU(t, e, "xc-gpu-1", bigMem, bigMem, scale, true)
	g2 := startFakeGPU(t, e, "xc-gpu-2", bigMem, bigMem, scale, true)
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": e.srv.URL, "TIME_SCALE": strconv.Itoa(scale), "POLL_INTERVAL": "100ms", "LOG_FORMAT": "text"})
	if code := p.wait(t, 3*time.Minute); code != 0 {
		t.Fatalf("shardsim exit %d\nstdout tail:\n%s\nstderr tail:\n%s", code, tail(p.stdout.String(), 20), tail(p.stderr.String(), 20))
	}
	t.Logf("shardsim exited 0 after %v", p.exitAt.Sub(p.start).Round(time.Millisecond))
	round := e.waitFirstRound(t, time.Second)
	r := e.mustRound(t, round)
	if r.FinishedAt == nil {
		t.Errorf("round %d not stamped finished after shardsim exited (decision 30)", round)
	} else {
		t.Logf("[db] round %d scenario=%s seed=%d n_shards=%d duration=%v", round, r.Scenario, r.Seed, r.NShards, r.FinishedAt.Sub(r.StartedAt))
	}
	if r.Scenario != "skewed" || r.Seed != 1 || r.NShards != 6 {
		t.Errorf("round row scenario=%s seed=%d n_shards=%d; want skewed/1/6 (shardsim defaults)", r.Scenario, r.Seed, r.NShards)
	}
	w := genWorkload(t, "skewed", 6, 1)
	bs := buildsByShard(t, e, round)
	if len(bs) != 6 {
		t.Errorf("%d builds, want 6", len(bs))
	}
	for _, s := range w.Shards {
		b, ok := bs[s.ShardID]
		if !ok {
			t.Errorf("shard %d has no build", s.ShardID)
			continue
		}
		logBuild(t, fmt.Sprintf("shard %d", s.ShardID), b)
		if b.Placement != "gpu" || b.State != "done" || b.Attempt != 1 || b.NVectors != s.NVectors || b.Dim != w.Scenario.Dim {
			t.Errorf("shard %d build: placement=%s state=%s attempt=%d n=%d dim=%d; want gpu/done/1/%d/%d",
				s.ShardID, b.Placement, b.State, b.Attempt, b.NVectors, b.Dim, s.NVectors, w.Scenario.Dim)
		}
		if b.BuildID != store.BuildID(s.ShardID, round) {
			t.Errorf("build id %s, want %s (invariant 6)", b.BuildID, store.BuildID(s.ShardID, round))
		}
	}
	jobs := jobsByShard(t, e, round)
	checkRecordedMatchesWorkload(t, w, jobs)

	// One CPU per shard (decision 32): query run intervals do not overlap.
	// Timestamps are the scheduler's now() at the start/done calls, so allow
	// a few ms for HTTP ordering.
	const slack = 5 * time.Millisecond
	for sid, js := range jobs {
		ivs := append([]store.JobRow(nil), js...)
		sort.Slice(ivs, func(a, b int) bool {
			if ivs[a].StartedAt == nil || ivs[b].StartedAt == nil {
				return false
			}
			return ivs[a].StartedAt.Before(*ivs[b].StartedAt)
		})
		over := 0
		for i := 1; i < len(ivs); i++ {
			if ivs[i].StartedAt == nil || ivs[i-1].FinishedAt == nil {
				continue
			}
			if ivs[i].StartedAt.Add(slack).Before(*ivs[i-1].FinishedAt) {
				if over < 2 {
					t.Errorf("shard %d: seq %d started %v before seq %d finished (one CPU, decision 32)",
						sid, ivs[i].Seq, ivs[i-1].FinishedAt.Sub(*ivs[i].StartedAt), ivs[i-1].Seq)
				}
				over++
			}
		}
		run, modeled := sumRun(js)
		ratio := float64(run) / (float64(modeled) / scale)
		t.Logf("shard %d: %d queries; total run time %v vs modeled/%d %v (ratio %.2f); overlaps %d",
			sid, len(js), run.Round(time.Millisecond), scale, (modeled / scale).Round(time.Millisecond), ratio, over)
		if ratio < 0.9 {
			t.Errorf("shard %d: queries ran for %.2f of modeled/TIME_SCALE (decision 55 says every duration is divided by TIME_SCALE)", sid, ratio)
		}
	}
	sts, _ := e.st.ListShardStatus(e.ctx, round)
	for _, s := range sts {
		if !s.StreamDone {
			t.Errorf("shard %d stream_done=false at end", s.ShardID)
		}
	}
	claims := map[string]int{}
	var ids []string
	for _, gg := range []*fakeGPU{g, g2} {
		for _, c := range gg.claimed() {
			claims[c.BuildID]++
			ids = append(ids, c.BuildID)
		}
	}
	t.Logf("fake workers claimed %v", ids)
	for id, c := range claims {
		if c != 1 {
			t.Errorf("build %s claimed %d times", id, c)
		}
	}
	if len(claims) != 6 {
		t.Errorf("%d distinct builds claimed, want 6", len(claims))
	}
	if !strings.Contains(p.stdout.String(), "round "+strconv.FormatInt(round, 10)) {
		t.Errorf("stdout has no timeline for round %d", round)
	}
}

// ---- 3. needs_index waits for the GPU build; the rest do not ----

func TestShardsimNeedsIndexWaitsForGPUBuild(t *testing.T) {
	// Decisions 23/29, 32, 54. The GPU is held back for 1 s real (10 s
	// modeled at TIME_SCALE=10) so a backlog forms. No needs_index query may
	// start before its shard's build finished_at; queries that do not need the
	// index must not wait for it (offloading frees the CPU).
	e := setupSim(t)
	const scale = 10
	g := startFakeGPU(t, e, "xc-gpu-1", bigMem, bigMem, scale, false)
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": e.srv.URL, "N_SHARDS": "3", "SEED": "11",
		"TIME_SCALE": strconv.Itoa(scale), "POLL_INTERVAL": "100ms", "LOG_FORMAT": "text"})
	round := e.waitFirstRound(t, 10*time.Second)
	time.Sleep(time.Second)
	t.Logf("opening the fake GPU 1 s after the round started")
	g.open()
	if code := p.wait(t, 3*time.Minute); code != 0 {
		t.Fatalf("shardsim exit %d\n%s", code, tail(p.stdout.String(), 20))
	}
	e.waitRoundFinished(t, round, 5*time.Second)
	w := genWorkload(t, "skewed", 3, 11)
	jobs := jobsByShard(t, e, round)
	checkRecordedMatchesWorkload(t, w, jobs)
	bs := buildsByShard(t, e, round)
	for sid, js := range jobs {
		b := bs[sid]
		if b.FinishedAt == nil {
			t.Errorf("shard %d build not finished: %+v", sid, b)
			continue
		}
		bf := *b.FinishedAt
		early, indepBefore, niAfter := 0, 0, 0
		var worst time.Duration
		for _, j := range js {
			if j.StartedAt == nil {
				continue
			}
			if j.NeedsIndex {
				if j.StartedAt.Before(bf) {
					early++
					if d := bf.Sub(*j.StartedAt); d > worst {
						worst = d
					}
				} else {
					niAfter++
				}
			} else if j.StartedAt.Before(bf) {
				indepBefore++
			}
		}
		t.Logf("shard %d: build %s %s/%s attempt %d finished %v after round start; needs_index started after it: %d, before it: %d (worst %v); independent queries started before it: %d",
			sid, b.BuildID, b.Placement, b.State, b.Attempt, bf.Sub(e.mustRound(t, round).StartedAt).Round(time.Millisecond), niAfter, early, worst, indepBefore)
		if early > 0 {
			t.Errorf("shard %d: %d needs_index queries started before the build finished (worst by %v); decision 32: not ready until the index is built", sid, early, worst)
		}
		if indepBefore == 0 {
			t.Errorf("shard %d: no index-independent query started before the GPU build finished; decision 32 says they do not wait", sid)
		}
	}
}

// ---- 4. local builds occupy the CPU ----

func TestShardsimLocalBuildOccupiesCPU(t *testing.T) {
	// Policy v0 places a build locally when it fits no live worker (decision
	// 7). The only live worker advertises 1 byte, so every build is local.
	// Decision 32 / modeling assumptions: a local build occupies the shard's
	// CPU, so no query on that shard starts before the build is done; the
	// build runs for cpu_build_ms / TIME_SCALE (decision 55); attempt stays 0
	// because attempt only moves on a claim (invariant 4).
	e := setupSim(t)
	const scale = 20
	startFakeGPU(t, e, "xc-tiny", 1, 1, scale, true)
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": e.srv.URL, "N_SHARDS": "3", "SEED": "2",
		"TIME_SCALE": strconv.Itoa(scale), "POLL_INTERVAL": "100ms", "LOG_FORMAT": "text"})
	if code := p.wait(t, 3*time.Minute); code != 0 {
		t.Fatalf("shardsim exit %d\n%s", code, tail(p.stdout.String(), 20))
	}
	round := e.waitFirstRound(t, time.Second)
	e.waitRoundFinished(t, round, 5*time.Second)
	w := genWorkload(t, "skewed", 3, 2)
	jobs := jobsByShard(t, e, round)
	checkRecordedMatchesWorkload(t, w, jobs)
	bs := buildsByShard(t, e, round)
	cm := costmodel.Default()
	for _, s := range w.Shards {
		b := bs[s.ShardID]
		logBuild(t, fmt.Sprintf("shard %d", s.ShardID), b)
		if b.Placement != "local" || b.State != "done" || b.Attempt != 0 {
			t.Errorf("shard %d build placement=%s state=%s attempt=%d; want local/done/0", s.ShardID, b.Placement, b.State, b.Attempt)
		}
		if b.StartedAt == nil || b.FinishedAt == nil {
			t.Errorf("shard %d build has no start/finish", s.ShardID)
			continue
		}
		want := time.Duration(float64(cm.CPUBuild(b.NVectors, b.Dim)) / scale)
		got := b.FinishedAt.Sub(*b.StartedAt)
		t.Logf("shard %d: local build ran %v, cpu_build/TIME_SCALE = %v", s.ShardID, got.Round(time.Millisecond), want.Round(time.Millisecond))
		if got < want*9/10 || got > want*3/2+time.Second {
			t.Errorf("shard %d: local build took %v, expected about %v (decision 55)", s.ShardID, got, want)
		}
		before, beforeStart := 0, 0
		var worst time.Duration
		for _, j := range jobs[s.ShardID] {
			if j.StartedAt != nil && j.StartedAt.Before(*b.FinishedAt) {
				before++
				if j.StartedAt.Before(*b.StartedAt) {
					beforeStart++
				}
				if d := b.FinishedAt.Sub(*j.StartedAt); d > worst {
					worst = d
				}
			}
		}
		t.Logf("shard %d: %d queries; started before the local build finished: %d (of which before it started: %d)", s.ShardID, len(jobs[s.ShardID]), before, beforeStart)
		if before > 0 {
			t.Errorf("shard %d: %d queries started while the local build held the CPU (worst %v before its finish); decision 32", s.ShardID, before, worst)
		}
	}
}

// ---- 5. reports: cadence, contents, sticky stream_done ----

func TestShardsimReportsCadenceAndStickyStreamDone(t *testing.T) {
	// Decisions 29, 30, 33. Each shard reports every POLL_INTERVAL; reports
	// carry the backlog (waiting_needs_index rises while the GPU is held
	// back); stream_done is set only after the shard's last arrival and,
	// once true, stays true.
	e := setupSim(t)
	const scale = 10
	const poll = 200 * time.Millisecond
	g := startFakeGPU(t, e, "xc-gpu-1", bigMem, bigMem, scale, false)
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": e.srv.URL, "N_SHARDS": "3", "SEED": "4",
		"TIME_SCALE": strconv.Itoa(scale), "POLL_INTERVAL": poll.String(), "LOG_FORMAT": "text"})
	round := e.waitFirstRound(t, 10*time.Second)
	type sample struct {
		at time.Time
		st store.ShardStatus
	}
	var mu sync.Mutex
	samples := map[int32][]sample{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			sts, err := e.st.ListShardStatus(e.ctx, round)
			if err != nil {
				continue
			}
			now := time.Now()
			mu.Lock()
			for _, s := range sts {
				samples[s.ShardID] = append(samples[s.ShardID], sample{now, s})
			}
			mu.Unlock()
		}
	}()
	time.Sleep(1500 * time.Millisecond)
	g.open()
	code := p.wait(t, 3*time.Minute)
	time.Sleep(3 * poll)
	close(stop)
	wg.Wait()
	if code != 0 {
		t.Fatalf("shardsim exit %d", code)
	}
	e.waitRoundFinished(t, round, 5*time.Second)
	jobs := jobsByShard(t, e, round)
	for sid, ss := range samples {
		var gaps []time.Duration
		var last time.Time
		maxNI, maxDepth := int32(0), int32(0)
		doneSeen := false
		var firstDone store.ShardStatus
		flipBack := 0
		for _, s := range ss {
			if !s.st.UpdatedAt.Equal(last) {
				if !last.IsZero() {
					gaps = append(gaps, s.st.UpdatedAt.Sub(last))
				}
				last = s.st.UpdatedAt
			}
			if s.st.WaitingNeedsIndex > maxNI {
				maxNI = s.st.WaitingNeedsIndex
			}
			if s.st.QueueDepth > maxDepth {
				maxDepth = s.st.QueueDepth
			}
			if s.st.StreamDone && !doneSeen {
				doneSeen, firstDone = true, s.st
			}
			if doneSeen && !s.st.StreamDone {
				flipBack++
			}
		}
		sort.Slice(gaps, func(a, b int) bool { return gaps[a] < gaps[b] })
		var med time.Duration
		if len(gaps) > 0 {
			med = gaps[len(gaps)/2]
		}
		var lastArr time.Time
		for _, j := range jobs[sid] {
			if j.ArrivedAt.After(lastArr) {
				lastArr = j.ArrivedAt
			}
		}
		t.Logf("shard %d: %d samples, %d distinct reports, median gap %v (POLL_INTERVAL %v); max queue_depth %d, max waiting_needs_index %d; stream_done first at %v (last arrival %v)",
			sid, len(ss), len(gaps)+1, med, poll, maxDepth, maxNI, firstDone.UpdatedAt.Format("15:04:05.000"), lastArr.Format("15:04:05.000"))
		if len(gaps) < 5 {
			t.Errorf("shard %d: only %d distinct reports seen", sid, len(gaps)+1)
		} else if med < poll/2 || med > 2*poll {
			t.Errorf("shard %d: median report gap %v, POLL_INTERVAL %v", sid, med, poll)
		}
		if maxNI == 0 {
			t.Errorf("shard %d: waiting_needs_index never above 0 although the GPU was held back 1.5 s (decision 29: the report carries the backlog)", sid)
		}
		if !doneSeen {
			t.Errorf("shard %d: stream_done never seen true", sid)
		} else if firstDone.UpdatedAt.Before(lastArr) {
			t.Errorf("shard %d: stream_done reported at %v, before its last arrival at %v (decision 30)", sid, firstDone.UpdatedAt, lastArr)
		}
		if flipBack > 0 {
			t.Errorf("shard %d: stream_done went back to false in %d samples (decision 33: sticky)", sid, flipBack)
		}
	}
	if len(samples) != 3 {
		t.Errorf("reports seen from %d shards, want 3", len(samples))
	}
}

// ---- 6. preemption ----

func TestShardsimHonoursPreemption(t *testing.T) {
	// Decisions 31, 54 and the "local build, and its preemption" flow. Every
	// build starts local (the only live worker advertises 1 byte). While the
	// biggest shard's local build is running, the test calls PreemptToGPU.
	// The shard must notice from its report reply, abort the local build and
	// free the CPU: independent queries then start well before the GPU
	// finishes, needs_index queries wait for the GPU, the shard's done call
	// never succeeds, and the build ends done on the GPU at attempt 1 after
	// the fake worker (which claims with a big capacity) takes it.
	e := setupSim(t)
	const scale = 10
	g := startFakeGPU(t, e, "xc-gpu-1", 1, bigMem, scale, false)
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": e.srv.URL, "N_SHARDS": "2", "SEED": "1",
		"TIME_SCALE": strconv.Itoa(scale), "POLL_INTERVAL": "100ms", "LOG_FORMAT": "text"})
	round := e.waitFirstRound(t, 10*time.Second)
	w := genWorkload(t, "skewed", 2, 1)
	big := w.Shards[0]
	for _, s := range w.Shards {
		if s.NVectors > big.NVectors {
			big = s
		}
	}
	id := store.BuildID(big.ShardID, round)
	var b store.BuildRow
	for dl := time.Now().Add(10 * time.Second); ; {
		row, ok, err := e.st.GetBuild(e.ctx, id)
		if err == nil && ok && row.Placement == "local" && row.State == "running" {
			b = row
			break
		}
		if time.Now().After(dl) {
			t.Fatalf("build %s never local/running: ok=%v err=%v row=%+v", id, ok, err, row)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cpu := time.Duration(float64(costmodel.Default().CPUBuild(b.NVectors, b.Dim)) / scale)
	t.Logf("shard %d build %s is local/running; local build would take %v real", big.ShardID, id, cpu.Round(time.Millisecond))
	time.Sleep(400 * time.Millisecond)
	var before, after time.Time
	_ = e.pool.QueryRow(e.ctx, `SELECT clock_timestamp()`).Scan(&before)
	ok, err := e.st.PreemptToGPU(e.ctx, id, 1)
	_ = e.pool.QueryRow(e.ctx, `SELECT clock_timestamp()`).Scan(&after)
	if err != nil || !ok {
		t.Fatalf("PreemptToGPU: ok=%v err=%v", ok, err)
	}
	logBuild(t, "after preempt", e.mustBuild(t, id))
	time.Sleep(1500 * time.Millisecond)
	t.Logf("opening the fake GPU 1.5 s after the preemption")
	g.open()
	if code := p.wait(t, 3*time.Minute); code != 0 {
		t.Fatalf("shardsim exit %d\n%s", code, tail(p.stdout.String(), 30))
	}
	e.waitRoundFinished(t, round, 5*time.Second)
	fb := e.mustBuild(t, id)
	logBuild(t, "final", fb)
	if fb.Placement != "gpu" || fb.State != "done" || fb.Attempt != 1 {
		t.Errorf("preempted build ended placement=%s state=%s attempt=%d; want gpu/done/1", fb.Placement, fb.State, fb.Attempt)
	}
	cl := g.claimed()
	g.mu.Lock()
	completedOK := g.completes[id]
	g.mu.Unlock()
	t.Logf("fake GPU claims: %+v; its Complete(%s) succeeded: %v", cl, id, completedOK)
	if !completedOK {
		t.Errorf("fake GPU's guarded Complete of %s did not succeed: something else finished the build (a shard done must be refused, decision 31)", id)
	}
	if fb.FinishedAt == nil {
		t.Fatalf("no finished_at")
	}
	gpuDone := *fb.FinishedAt
	jobs := jobsByShard(t, e, round)
	checkRecordedMatchesWorkload(t, w, jobs)
	preStart, indepWindow, niEarly := 0, 0, 0
	var firstIndep time.Time
	for _, j := range jobs[big.ShardID] {
		if j.StartedAt == nil {
			continue
		}
		s := *j.StartedAt
		if s.Before(before) {
			preStart++
		}
		if !j.NeedsIndex && s.After(before) && s.Before(gpuDone) {
			indepWindow++
			if firstIndep.IsZero() || s.Before(firstIndep) {
				firstIndep = s
			}
		}
		if j.NeedsIndex && s.Before(gpuDone) {
			niEarly++
		}
	}
	t.Logf("shard %d: preempted at %v; GPU done %v later; queries started before preemption: %d; independent queries started between preemption and GPU done: %d (first %v after preemption); needs_index started before GPU done: %d",
		big.ShardID, before.Format("15:04:05.000"), gpuDone.Sub(before).Round(time.Millisecond), preStart, indepWindow, firstIndep.Sub(after).Round(time.Millisecond), niEarly)
	if preStart > 0 {
		t.Errorf("%d queries started on shard %d while its local build held the CPU", preStart, big.ShardID)
	}
	if indepWindow == 0 {
		t.Errorf("no independent query started between the preemption and the GPU completion: the shard did not free its CPU (decision 54)")
	} else if firstIndep.Sub(after) > time.Second {
		t.Errorf("first independent query started %v after the preemption; the shard should notice within about one POLL_INTERVAL (100ms)", firstIndep.Sub(after))
	}
	if niEarly > 0 {
		t.Errorf("%d needs_index queries started before the GPU build was done (decision 32)", niEarly)
	}
	var lines []string
	for _, l := range strings.Split(p.stdout.String(), "\n") {
		if strings.Contains(l, "build_id="+id+" ") && len(lines) < 12 {
			lines = append(lines, l)
		}
	}
	t.Logf("shardsim log lines for the preempted build:\n%s", strings.Join(lines, "\n"))
}

// ---- 7. SIGTERM ----

func TestShardsimSIGTERMStopsPromptly(t *testing.T) {
	// Operational: a simulator mid-round (TIME_SCALE=1, a 30 s horizon) must
	// stop promptly on SIGTERM and stop driving the scheduler.
	e := setupSim(t)
	startFakeGPU(t, e, "xc-gpu-1", bigMem, bigMem, 1, true)
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": e.srv.URL, "LOG_FORMAT": "text", "POLL_INTERVAL": "200ms"})
	round := e.waitFirstRound(t, 10*time.Second)
	deadline := time.Now().Add(10 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		_ = e.pool.QueryRow(e.ctx, `SELECT count(*) FROM shard_jobs WHERE round_id=$1`, round).Scan(&n)
		if n > 50 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("round %d running with %d arrivals recorded; sending SIGTERM", round, n)
	sent := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	code := p.wait(t, 5*time.Second)
	t.Logf("shardsim exited %v after SIGTERM with code %d; stdout tail:\n%s", p.exitAt.Sub(sent).Round(time.Millisecond), code, tail(p.stdout.String(), 4))
	if p.exitAt.Sub(sent) > 2*time.Second {
		t.Errorf("exit took %v after SIGTERM", p.exitAt.Sub(sent))
	}
	var n1, n2 int
	_ = e.pool.QueryRow(e.ctx, `SELECT count(*) FROM shard_jobs WHERE round_id=$1`, round).Scan(&n1)
	time.Sleep(time.Second)
	_ = e.pool.QueryRow(e.ctx, `SELECT count(*) FROM shard_jobs WHERE round_id=$1`, round).Scan(&n2)
	if n2 != n1 {
		t.Errorf("arrivals kept being recorded after exit: %d then %d", n1, n2)
	}
	if r := e.mustRound(t, round); r.FinishedAt != nil {
		t.Errorf("round %d was stamped finished although the simulator was stopped at 30 s horizon", round)
	}
}

// ---- 8. bad configuration ----

func TestShardsimBadScenarioExitsBeforeScheduler(t *testing.T) {
	// A bad SCENARIO must exit non-zero before touching the scheduler: no
	// HTTP request, no round row.
	e := setupSim(t)
	var hits atomic.Int64
	counter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		e.srv.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(counter.Close)
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": counter.URL, "SCENARIO": "no_such_preset", "MIN_WORKERS": "0"})
	code := p.wait(t, 10*time.Second)
	out := p.stdout.String() + p.stderr.String()
	t.Logf("exit %d after %v; output: %s", code, p.exitAt.Sub(p.start).Round(time.Millisecond), strings.TrimSpace(out))
	if code == 0 {
		t.Errorf("bad SCENARIO exited 0")
	}
	if hits.Load() != 0 {
		t.Errorf("scheduler received %d requests from a misconfigured simulator", hits.Load())
	}
	var n int
	_ = e.pool.QueryRow(e.ctx, `SELECT count(*) FROM rounds`).Scan(&n)
	if n != 0 {
		t.Errorf("%d rounds created", n)
	}
	// Observations only (the env header promises no validation for these).
	for _, kv := range [][2]string{{"TIME_SCALE", "0"}, {"TIME_SCALE", "-1"}, {"N_SHARDS", "0"}, {"POLL_INTERVAL", "0s"}, {"SEED", "abc"}} {
		before := hits.Load()
		q := startShardsim(t, map[string]string{"SCHEDULER_URL": counter.URL, "MIN_WORKERS": "0", "ROUND_TIMEOUT": "3s", kv[0]: kv[1]})
		select {
		case <-q.done:
			t.Logf("observation: %s=%s exited %d after %v with %d scheduler requests: %s", kv[0], kv[1], q.cmd.ProcessState.ExitCode(),
				q.exitAt.Sub(q.start).Round(time.Millisecond), hits.Load()-before, strings.TrimSpace(tail(q.stdout.String()+q.stderr.String(), 1)))
		case <-time.After(5 * time.Second):
			_ = q.cmd.Process.Signal(syscall.SIGKILL)
			<-q.done
			t.Logf("observation: %s=%s still running after 5 s (%d scheduler requests); killed", kv[0], kv[1], hits.Load()-before)
		}
	}
	_, _ = e.pool.Exec(e.ctx, `SELECT 1`)
}

// ---- 9. timeline JSON against Postgres ----

type tlQuery struct {
	Seq            int32  `json:"seq"`
	Kind           string `json:"kind"`
	ClusterQueryID int64  `json:"cluster_query_id"`
	NeedsIndex     bool   `json:"needs_index"`
	Arrived        int64  `json:"arrived"`
	Started        int64  `json:"started"`
	Finished       int64  `json:"finished"`
}

type tlShard struct {
	ShardID    int32     `json:"shard_id"`
	NVectors   int64     `json:"n_vectors"`
	Placement  string    `json:"placement"`
	Attempt    int32     `json:"attempt"`
	BuildState string    `json:"build_state"`
	BuildStart int64     `json:"build_start"`
	BuildDone  int64     `json:"build_done"`
	Finished   int64     `json:"finished"`
	Queries    []tlQuery `json:"queries"`
}

type timelineJSON struct {
	RoundID         int64     `json:"round_id"`
	Scenario        string    `json:"scenario"`
	Seed            int64     `json:"seed"`
	TimeScale       float64   `json:"time_scale"`
	StartedAt       time.Time `json:"started_at"`
	Duration        int64     `json:"duration"`
	DurationModeled int64     `json:"duration_modeled"`
	StragglerLag    int64     `json:"straggler_lag"`
	BuildsGPU       int       `json:"builds_gpu"`
	BuildsLocal     int       `json:"builds_local"`
	Queries         int       `json:"queries"`
	Shards          []tlShard `json:"shards"`
}

func TestShardsimTimelineJSONAgreesWithPostgres(t *testing.T) {
	// Decision 56 and the roadmap's per-round timeline step; ROUNDS/SEED from
	// the env header ("each further round uses SEED+1"). Bimodal preset with
	// the preset's advised worker memory, so heavy builds that fit no worker go
	// local and the rest go to the GPU: both placements appear. Two rounds.
	e := setupSim(t)
	const scale = 20
	const n = 4
	sc, err := workload.PresetWithShards("bimodal", n)
	if err != nil {
		t.Fatal(err)
	}
	mem := sc.Pool.WorkerMemBytes
	startFakeGPU(t, e, "xc-gpu-1", mem, mem, scale, true)
	startFakeGPU(t, e, "xc-gpu-2", mem, mem, scale, true)
	dir := t.TempDir()
	p := startShardsim(t, map[string]string{"SCHEDULER_URL": e.srv.URL, "SCENARIO": "bimodal", "N_SHARDS": strconv.Itoa(n), "SEED": "5",
		"ROUNDS": "2", "ROUND_GAP": "200ms", "TIME_SCALE": strconv.Itoa(scale), "POLL_INTERVAL": "100ms", "TIMELINE_DIR": dir, "LOG_FORMAT": "text"})
	if code := p.wait(t, 4*time.Minute); code != 0 {
		t.Fatalf("shardsim exit %d\n%s", code, tail(p.stdout.String(), 20))
	}
	rows, err := e.pool.Query(e.ctx, `SELECT round_id FROM rounds ORDER BY round_id`)
	if err != nil {
		t.Fatal(err)
	}
	var rounds []int64
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		rounds = append(rounds, id)
	}
	rows.Close()
	if len(rounds) != 2 {
		t.Fatalf("%d rounds in Postgres, want 2 (ROUNDS=2)", len(rounds))
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	t.Logf("TIMELINE_DIR holds %v", files)
	const tol = 5 * time.Millisecond
	for i, round := range rounds {
		seed := int64(5 + i)
		r := e.mustRound(t, round)
		if r.Seed != seed || r.Scenario != "bimodal" || r.NShards != n || r.FinishedAt == nil {
			t.Errorf("round %d: seed=%d scenario=%s n=%d finished=%v; want seed %d bimodal %d finished", round, r.Seed, r.Scenario, r.NShards, tptr(r.FinishedAt), seed, n)
		}
		path := filepath.Join(dir, fmt.Sprintf("round-%d.json", round))
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("round %d: %v", round, err)
			continue
		}
		var tl timelineJSON
		if err := json.Unmarshal(raw, &tl); err != nil {
			t.Errorf("round %d JSON: %v", round, err)
			continue
		}
		w := genWorkload(t, "bimodal", n, seed)
		jobs := jobsByShard(t, e, round)
		checkRecordedMatchesWorkload(t, w, jobs)
		bs := buildsByShard(t, e, round)
		dbQueries, gpu, local := 0, 0, 0
		for _, b := range bs {
			if b.Placement == "gpu" {
				gpu++
			} else {
				local++
			}
		}
		for _, js := range jobs {
			dbQueries += len(js)
		}
		t.Logf("round %d (seed %d): JSON queries=%d builds gpu=%d local=%d duration=%v lag=%v; DB queries=%d gpu=%d local=%d duration=%v",
			round, seed, tl.Queries, tl.BuildsGPU, tl.BuildsLocal, time.Duration(tl.Duration), time.Duration(tl.StragglerLag), dbQueries, gpu, local, r.FinishedAt.Sub(r.StartedAt))
		if tl.RoundID != round || tl.Seed != seed || tl.Scenario != "bimodal" || tl.TimeScale != scale {
			t.Errorf("JSON header round=%d seed=%d scenario=%s scale=%v", tl.RoundID, tl.Seed, tl.Scenario, tl.TimeScale)
		}
		if tl.Queries != dbQueries || tl.BuildsGPU != gpu || tl.BuildsLocal != local {
			t.Errorf("JSON counts queries=%d gpu=%d local=%d; Postgres %d/%d/%d", tl.Queries, tl.BuildsGPU, tl.BuildsLocal, dbQueries, gpu, local)
		}
		if gpu == 0 || local == 0 {
			t.Errorf("expected both placements with the bimodal advised memory; got gpu=%d local=%d", gpu, local)
		}
		if !tl.StartedAt.Equal(r.StartedAt) && absDur(tl.StartedAt.Sub(r.StartedAt)) > tol {
			t.Errorf("JSON started_at %v, Postgres %v", tl.StartedAt, r.StartedAt)
		}
		if d := absDur(time.Duration(tl.Duration) - r.FinishedAt.Sub(r.StartedAt)); d > 20*time.Millisecond {
			t.Errorf("JSON duration %v, Postgres finished-started %v", time.Duration(tl.Duration), r.FinishedAt.Sub(r.StartedAt))
		}
		if d := math.Abs(float64(tl.DurationModeled) - float64(tl.Duration)*scale); d > float64(50*time.Millisecond)*scale {
			t.Errorf("duration_modeled %v is not duration x TIME_SCALE (%v)", time.Duration(tl.DurationModeled), time.Duration(tl.Duration*scale))
		}
		var finishes []time.Duration
		if len(tl.Shards) != n {
			t.Errorf("JSON has %d shards", len(tl.Shards))
		}
		for _, s := range tl.Shards {
			b := bs[s.ShardID]
			if s.Placement != b.Placement || s.Attempt != b.Attempt || s.BuildState != b.State || s.NVectors != b.NVectors {
				t.Errorf("shard %d JSON build %s/%s attempt %d n=%d; Postgres %s/%s attempt %d n=%d", s.ShardID, s.Placement, s.BuildState, s.Attempt, s.NVectors, b.Placement, b.State, b.Attempt, b.NVectors)
			}
			if b.FinishedAt != nil {
				if d := absDur(time.Duration(s.BuildDone) - b.FinishedAt.Sub(r.StartedAt)); d > tol {
					t.Errorf("shard %d JSON build_done %v, Postgres %v", s.ShardID, time.Duration(s.BuildDone), b.FinishedAt.Sub(r.StartedAt))
				}
			}
			finishes = append(finishes, time.Duration(s.Finished))
			js := jobs[s.ShardID]
			if len(s.Queries) != len(js) {
				t.Errorf("shard %d JSON %d queries, Postgres %d", s.ShardID, len(s.Queries), len(js))
				continue
			}
			bySeq := map[int32]store.JobRow{}
			for _, j := range js {
				bySeq[j.Seq] = j
			}
			post := postDDL(w.Shards[s.ShardID])
			bad := 0
			var lastFin time.Duration
			if b.FinishedAt != nil {
				lastFin = b.FinishedAt.Sub(r.StartedAt)
			}
			for k, q := range s.Queries {
				j, ok := bySeq[q.Seq]
				if !ok || j.StartedAt == nil || j.FinishedAt == nil {
					bad++
					continue
				}
				if q.NeedsIndex != j.NeedsIndex ||
					absDur(time.Duration(q.Started)-j.StartedAt.Sub(r.StartedAt)) > tol ||
					absDur(time.Duration(q.Finished)-j.FinishedAt.Sub(r.StartedAt)) > tol ||
					absDur(time.Duration(q.Arrived)-j.ArrivedAt.Sub(r.StartedAt)) > tol {
					if bad < 2 {
						t.Errorf("shard %d seq %d: JSON ni=%v arr=%v start=%v fin=%v; Postgres ni=%v arr=%v start=%v fin=%v", s.ShardID, q.Seq, q.NeedsIndex,
							time.Duration(q.Arrived), time.Duration(q.Started), time.Duration(q.Finished), j.NeedsIndex,
							j.ArrivedAt.Sub(r.StartedAt), j.StartedAt.Sub(r.StartedAt), j.FinishedAt.Sub(r.StartedAt))
					}
					bad++
				}
				// kind and cluster id come from the workload (decision 56);
				// match by arrival order.
				if k < len(post) && (string(post[k].Kind) != q.Kind || post[k].ClusterQueryID != q.ClusterQueryID) {
					if bad < 2 {
						t.Errorf("shard %d query #%d: JSON kind=%s cluster=%d; workload %s/%d", s.ShardID, k, q.Kind, q.ClusterQueryID, post[k].Kind, post[k].ClusterQueryID)
					}
					bad++
				}
				if f := j.FinishedAt.Sub(r.StartedAt); f > lastFin {
					lastFin = f
				}
			}
			if bad > 0 {
				t.Errorf("shard %d: %d JSON query entries disagree with Postgres/workload", s.ShardID, bad)
			}
			if d := absDur(time.Duration(s.Finished) - lastFin); d > tol {
				t.Errorf("shard %d JSON finished %v; latest build/query finish in Postgres %v", s.ShardID, time.Duration(s.Finished), lastFin)
			}
		}
		if len(finishes) == n {
			sort.Slice(finishes, func(a, b int) bool { return finishes[a] < finishes[b] })
			maxF := finishes[n-1]
			lo, hi := maxF-finishes[(n-1)/2], maxF-finishes[n/2]
			mid := maxF - (finishes[(n-1)/2]+finishes[n/2])/2
			lag := time.Duration(tl.StragglerLag)
			if absDur(lag-lo) > time.Millisecond && absDur(lag-hi) > time.Millisecond && absDur(lag-mid) > time.Millisecond {
				t.Errorf("straggler_lag %v is not last finish minus median finish (%v / %v / %v)", lag, lo, mid, hi)
			}
		}
	}
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
