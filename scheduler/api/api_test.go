package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arqbca11/KubernetesGPU/db"
	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/policy"
	"github.com/arqbca11/KubernetesGPU/scheduler/reaper"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

// Integration test of the HTTP surface against a real Postgres. Skipped
// unless TEST_DATABASE_URL is set; scripts/test-db.sh provides one.

type harness struct {
	t   *testing.T
	st  *store.Store
	srv *httptest.Server
	log *slog.Logger
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
	srv := httptest.NewServer(New(st, policy.AlwaysGPU{}, costmodel.Default(), Config{WorkerStaleAfter: 30 * time.Second}, log))
	t.Cleanup(srv.Close)
	return &harness{t: t, st: st, srv: srv, log: log}
}

// call does one request, logs it, decodes the JSON body into out (if any),
// and returns the status code.
func (h *harness) call(method, path string, body any, out any) int {
	h.t.Helper()
	var rd io.Reader
	shown := ""
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
		shown = " " + string(b)
	}
	req, _ := http.NewRequest(method, h.srv.URL+path, rd)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	line := string(bytes.TrimSpace(raw)) // a copy: truncating must not touch raw
	if len(line) > 160 {
		line = line[:160] + "..."
	}
	h.t.Logf("%s %s%s\n      -> %d %s", method, path, shown, resp.StatusCode, line)
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			h.t.Fatalf("decode %s %s: %v: %s", method, path, err, raw)
		}
	}
	return resp.StatusCode
}

func TestEndToEndRoundOverHTTP(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	t.Log("--- a worker with 8 GiB registers by heartbeat (workers use SQL, not HTTP)")
	if err := h.st.Heartbeat(ctx, "gpu-1", 8<<30); err != nil {
		t.Fatal(err)
	}
	var ws WorkersResponse
	h.call("GET", "/workers", nil, &ws)
	if ws.Pool.LiveWorkers != 1 {
		t.Fatalf("pool = %+v", ws.Pool)
	}

	t.Log("--- shard simulator starts a round of 2 shards")
	var cr CreateRoundResponse
	if code := h.call("POST", "/rounds", CreateRoundRequest{Scenario: "test", Seed: 42, NShards: 2}, &cr); code != 201 {
		t.Fatalf("create round: %d", code)
	}
	r := cr.RoundID
	report := func(shard int32, req ReportRequest) ReportResponse {
		var rr ReportResponse
		if code := h.call("POST", fmt.Sprintf("/shards/%d/%d/report", r, shard), req, &rr); code != 200 {
			t.Fatalf("report: %d", code)
		}
		return rr
	}

	t.Log("--- shard 1 submits a 100k x 128 build; it knows nothing about future queries")
	var b1 SubmitBuildResponse
	code := h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 1, NVectors: 100_000, Dim: 128}, &b1)
	if code != 201 || b1.Placement != "gpu" || !b1.Created {
		t.Fatalf("submit 1: code=%d resp=%+v", code, b1)
	}
	t.Logf("    placed on gpu; estimates: cpu %d ms, gpu %d ms, mem %d MiB", b1.CPUBuildMs, b1.GPUTotalMs, b1.MemBytes>>20)

	t.Log("--- shard 1 resubmits the same build (retry after a timeout, say)")
	var dup SubmitBuildResponse
	code = h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 1, NVectors: 100_000, Dim: 128}, &dup)
	if code != 200 || dup.Created || dup.Placement != "gpu" || dup.BuildID != b1.BuildID {
		t.Fatalf("duplicate submit: code=%d resp=%+v", code, dup)
	}

	t.Log("--- shard 2 submits a 20M x 128 build: 19 GiB modeled memory, more than any live worker")
	var b2 SubmitBuildResponse
	code = h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 2, NVectors: 20_000_000, Dim: 128}, &b2)
	if code != 201 || b2.Placement != "local" {
		t.Fatalf("submit 2: code=%d resp=%+v", code, b2)
	}
	t.Logf("    placed local: %s", b2.Reason)

	t.Log("--- queries arrive at shard 1 while its build is queued on the gpu")
	if code := h.call("POST", fmt.Sprintf("/jobs/%d/1", r), ArrivalRequest{Seq: 0, DurationMs: 50, NeedsIndex: false}, nil); code != 201 {
		t.Fatalf("arrival: %d", code)
	}
	if code := h.call("POST", fmt.Sprintf("/jobs/%d/1", r), ArrivalRequest{Seq: 0, DurationMs: 50, NeedsIndex: false}, nil); code != 200 {
		t.Fatalf("repeat arrival should be 200, got %d", code)
	}
	h.call("POST", fmt.Sprintf("/jobs/%d/1", r), ArrivalRequest{Seq: 1, DurationMs: 50, NeedsIndex: true}, nil)

	t.Log("--- shard 1 reports: 2 waiting, 1 of them needs the index; the reply tells it the build is still queued")
	rr := report(1, ReportRequest{QueueDepth: 2, WaitingNeedsIndex: 1, OldestWaitMs: 120})
	if rr.Build.Placement != "gpu" || rr.Build.State != "queued" {
		t.Fatalf("report reply: %+v", rr.Build)
	}

	t.Log("--- shard 1's cpu is free, so query 0 (needs_index=false) runs now")
	if code := h.call("POST", fmt.Sprintf("/jobs/%d/1/0/start", r), nil, nil); code != 200 {
		t.Fatalf("job start: %d", code)
	}
	if code := h.call("POST", fmt.Sprintf("/jobs/%d/1/0/done", r), nil, nil); code != 200 {
		t.Fatalf("job done: %d", code)
	}
	if code := h.call("POST", fmt.Sprintf("/jobs/%d/1/0/done", r), nil, nil); code != 409 {
		t.Fatalf("finishing a finished job should be 409, got %d", code)
	}

	t.Log("--- the gpu worker claims and completes shard 1's build (SQL, as the Python worker will)")
	c, ok, err := h.st.Claim(ctx, "gpu-1", 8<<30, 30*time.Second)
	if err != nil || !ok || c.BuildID != b1.BuildID {
		t.Fatalf("claim: ok=%v id=%s err=%v", ok, c.BuildID, err)
	}
	if ok, _ := h.st.Complete(ctx, c.BuildID, c.Attempt); !ok {
		t.Fatal("complete rejected")
	}

	t.Log("--- shard 1's next report learns the build is done; query 1 (needs_index) runs; stream exhausted")
	rr = report(1, ReportRequest{QueueDepth: 1, WaitingNeedsIndex: 1, OldestWaitMs: 300})
	if rr.Build.State != "done" || rr.Build.Attempt != 1 {
		t.Fatalf("report reply after completion: %+v", rr.Build)
	}
	h.call("POST", fmt.Sprintf("/jobs/%d/1/1/start", r), nil, nil)
	h.call("POST", fmt.Sprintf("/jobs/%d/1/1/done", r), nil, nil)
	report(1, ReportRequest{QueueDepth: 0, StreamDone: true})

	t.Log("--- shard 2: one query arrives during its local build, then the build finishes, the query runs")
	h.call("POST", fmt.Sprintf("/jobs/%d/2", r), ArrivalRequest{Seq: 0, DurationMs: 50, NeedsIndex: true}, nil)
	report(2, ReportRequest{QueueDepth: 1, WaitingNeedsIndex: 1, OldestWaitMs: 900, BuildProgress: 0.6})
	if code := h.call("POST", "/builds/"+b2.BuildID+"/done", nil, nil); code != 200 {
		t.Fatalf("local done: %d", code)
	}
	if code := h.call("POST", "/builds/"+b2.BuildID+"/done", nil, nil); code != 409 {
		t.Fatalf("second local done should be 409, got %d", code)
	}
	if code := h.call("POST", "/builds/"+b1.BuildID+"/done", nil, nil); code != 409 {
		t.Fatalf("local-done on a gpu build should be 409, got %d", code)
	}
	h.call("POST", fmt.Sprintf("/jobs/%d/2/0/start", r), nil, nil)
	h.call("POST", fmt.Sprintf("/jobs/%d/2/0/done", r), nil, nil)

	t.Log("--- the round is still open: shard 2 has not said its stream is done")
	var rs RoundStatus
	h.call("GET", fmt.Sprintf("/rounds/%d", r), nil, &rs)
	if rs.Round.FinishedAt != nil {
		t.Fatal("round finished before shard 2 reported stream_done")
	}
	report(2, ReportRequest{QueueDepth: 0, StreamDone: true})

	t.Log("--- the reaper tick (or a GET) stamps the round finished")
	reaper.Tick(ctx, h.st, 5, h.log)
	h.call("GET", fmt.Sprintf("/rounds/%d", r), nil, &rs)
	if rs.Round.FinishedAt == nil {
		t.Fatal("round not finished")
	}
	if len(rs.Builds) != 2 || len(rs.Jobs) != 3 || len(rs.Statuses) != 2 {
		t.Fatalf("status: builds=%d jobs=%d statuses=%d", len(rs.Builds), len(rs.Jobs), len(rs.Statuses))
	}
	for _, b := range rs.Builds {
		t.Logf("    build %s: %s/%s attempt %d", b.BuildID, b.Placement, b.State, b.Attempt)
		if b.State != "done" {
			t.Fatalf("build %s state %s", b.BuildID, b.State)
		}
	}
	t.Logf("    round %d finished; duration %s", r, rs.Round.FinishedAt.Sub(rs.Round.StartedAt).Round(time.Millisecond))
}

func TestReportReflectsPreemption(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	var cr CreateRoundResponse
	h.call("POST", "/rounds", CreateRoundRequest{Scenario: "test", Seed: 1, NShards: 1}, &cr)
	r := cr.RoundID
	h.st.Heartbeat(ctx, "small", 1) //nolint:errcheck
	var b SubmitBuildResponse
	h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 0, NVectors: 100_000, Dim: 128}, &b)
	if b.Placement != "local" {
		t.Fatalf("expected local (1-byte worker), got %s", b.Placement)
	}
	t.Log("--- shard 0 builds locally and reports a growing queue")
	var rr ReportResponse
	h.call("POST", fmt.Sprintf("/shards/%d/0/report", r), ReportRequest{QueueDepth: 5, WaitingNeedsIndex: 1, OldestWaitMs: 4000, BuildProgress: 0.2}, &rr)
	if rr.Build.Placement != "local" || rr.Build.State != "running" {
		t.Fatalf("reply: %+v", rr.Build)
	}
	t.Log("--- (Phase 3 reconsider) preempts the build to the gpu queue")
	if ok, _ := h.st.PreemptToGPU(ctx, b.BuildID, 5); !ok {
		t.Fatal("preempt failed")
	}
	t.Log("--- the shard's next report tells it: abort the local build")
	h.call("POST", fmt.Sprintf("/shards/%d/0/report", r), ReportRequest{QueueDepth: 6, WaitingNeedsIndex: 1, OldestWaitMs: 4500, BuildProgress: 0.3}, &rr)
	if rr.Build.Placement != "gpu" || rr.Build.State != "queued" {
		t.Fatalf("reply after preempt: %+v", rr.Build)
	}
	if code := h.call("POST", "/builds/"+b.BuildID+"/done", nil, nil); code != 409 {
		t.Fatalf("a preempted shard's done must be refused, got %d", code)
	}
	t.Log("--- a report for a shard with no build is 404; out-of-range fields are 400")
	if code := h.call("POST", fmt.Sprintf("/shards/%d/7/report", r), ReportRequest{}, nil); code != 404 {
		t.Fatalf("report without build: %d", code)
	}
	if code := h.call("POST", fmt.Sprintf("/shards/%d/0/report", r), ReportRequest{QueueDepth: 1, WaitingNeedsIndex: 2}, nil); code != 400 {
		t.Fatalf("bad report: %d", code)
	}
}

func TestValidationAndNotFound(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		method, path string
		body         any
		want         int
	}{
		{"POST", "/rounds", CreateRoundRequest{NShards: 0}, 400},
		{"POST", "/rounds", `{"n_shards": 1, "bogus": 1}`, 400},
		{"GET", "/rounds/999999", nil, 404},
		{"GET", "/rounds/abc", nil, 400},
		{"POST", "/builds", SubmitBuildRequest{RoundID: 999999, ShardID: 1, NVectors: 10, Dim: 4}, 404},
		{"POST", "/builds", SubmitBuildRequest{RoundID: 1, ShardID: 1, NVectors: 0, Dim: 4}, 400},
		{"POST", "/builds/nope/done", nil, 404},
		{"POST", "/jobs/999999/1/0/start", nil, 404},
		{"GET", "/builds/nope", nil, 404},
		{"POST", "/jobs/999999/1", ArrivalRequest{Seq: -1}, 400},
		{"GET", "/healthz", nil, 200},
	}
	for _, c := range cases {
		var body any = c.body
		if s, isStr := c.body.(string); isStr {
			body = json.RawMessage(s)
		}
		if got := h.call(c.method, c.path, body, nil); got != c.want {
			t.Fatalf("%s %s: got %d want %d", c.method, c.path, got, c.want)
		}
	}
}

func TestSubmitValidationAgainstRound(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	var cr CreateRoundResponse
	h.call("POST", "/rounds", CreateRoundRequest{Scenario: "test", Seed: 1, NShards: 2}, &cr)
	r := cr.RoundID

	t.Log("--- shard ids are the shard's own numbering; 7 and 42 are fine in a 2-shard round")
	var first, dup SubmitBuildResponse
	if code := h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 7, NVectors: 100_000, Dim: 128}, &first); code != 201 {
		t.Fatalf("shard 7: got %d", code)
	}
	if code := h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 42, NVectors: 10, Dim: 4}, nil); code != 201 {
		t.Fatalf("shard 42: got %d", code)
	}

	t.Log("--- a third distinct shard would let the round finish early: refused")
	if code := h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 99, NVectors: 10, Dim: 4}, nil); code != 409 {
		t.Fatalf("third shard in a 2-shard round: got %d want 409", code)
	}

	t.Log("--- a duplicate with a different body returns estimates for the STORED row, and is fine in a full round")
	h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 7, NVectors: 5, Dim: 128}, &dup)
	if dup.Created || dup.CPUBuildMs != first.CPUBuildMs || dup.GPUTotalMs != first.GPUTotalMs || dup.MemBytes != first.MemBytes {
		t.Fatalf("duplicate estimates differ: first %+v dup %+v", first, dup)
	}

	t.Log("--- finish the round; a duplicate is still 200, a new build would be 409")
	for range 2 {
		c, ok, _ := h.st.Claim(ctx, "w", 1<<40, 30*time.Second)
		if !ok {
			t.Fatal("expected a claimable build")
		}
		h.st.Complete(ctx, c.BuildID, c.Attempt) //nolint:errcheck
	}
	for _, shard := range []int32{7, 42} {
		h.call("POST", fmt.Sprintf("/shards/%d/%d/report", r, shard), ReportRequest{StreamDone: true}, nil)
	}
	var rs RoundStatus
	h.call("GET", fmt.Sprintf("/rounds/%d", r), nil, &rs)
	if rs.Round.FinishedAt == nil {
		t.Fatal("round should be finished")
	}
	if code := h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 42, NVectors: 10, Dim: 4}, nil); code != 200 {
		t.Fatalf("duplicate into a finished round: got %d want 200", code)
	}
}

// Decision 60 over HTTP: the report carries a batch of client-timestamped
// query records; bad batches are 400; a new arrival after stream_done is 409.
func TestReportWithJobBatch(t *testing.T) {
	h := newHarness(t)
	var cr CreateRoundResponse
	h.call("POST", "/rounds", CreateRoundRequest{Scenario: "test", Seed: 1, NShards: 1}, &cr)
	r := cr.RoundID
	h.call("POST", "/builds", SubmitBuildRequest{RoundID: r, ShardID: 0, NVectors: 1000, Dim: 4}, nil)
	t0 := time.Now().Add(-5 * time.Second)
	at := func(ms int) *time.Time { x := t0.Add(time.Duration(ms) * time.Millisecond); return &x }
	path := fmt.Sprintf("/shards/%d/0/report", r)

	t.Log("--- a report with two records, one finished")
	var rr ReportResponse
	code := h.call("POST", path, ReportRequest{QueueDepth: 1, Jobs: []store.JobRecord{
		{Seq: 0, DurationMs: 10, ArrivedAt: *at(0), StartedAt: at(1), FinishedAt: at(11)},
		{Seq: 1, DurationMs: 10, ArrivedAt: *at(2)},
	}}, &rr)
	if code != 200 || rr.Build.State != "queued" {
		t.Fatalf("batch report: %d %+v", code, rr)
	}
	var rs RoundStatus
	h.call("GET", fmt.Sprintf("/rounds/%d", r), nil, &rs)
	if len(rs.Jobs) != 2 || rs.Jobs[0].FinishedAt == nil || rs.Jobs[0].ArrivedAt.Sub(t0) != 0 {
		t.Fatalf("jobs: %+v", rs.Jobs)
	}
	t.Log("    both recorded with the shard's timestamps; the reply still carries the build")

	t.Log("--- bad batches are 400")
	for _, bad := range []store.JobRecord{
		{Seq: 5, DurationMs: 10}, // no arrived_at
		{Seq: 5, DurationMs: 10, ArrivedAt: *at(10), StartedAt: at(5)},   // started before arrived
		{Seq: 5, DurationMs: 10, ArrivedAt: *at(10), FinishedAt: at(20)}, // finished without started
	} {
		if code := h.call("POST", path, ReportRequest{Jobs: []store.JobRecord{bad}}, nil); code != 400 {
			t.Fatalf("bad record %+v: got %d", bad, code)
		}
	}
	t.Log("--- stream_done with the last record; then a new arrival is 409")
	if code := h.call("POST", path, ReportRequest{StreamDone: true, Jobs: []store.JobRecord{{Seq: 1, DurationMs: 10, ArrivedAt: *at(2), StartedAt: at(12), FinishedAt: at(22)}}}, nil); code != 200 {
		t.Fatalf("final batch: %d", code)
	}
	if code := h.call("POST", path, ReportRequest{StreamDone: true, Jobs: []store.JobRecord{{Seq: 2, DurationMs: 10, ArrivedAt: *at(30)}}}, nil); code != 409 {
		t.Fatalf("arrival after stream_done: got %d want 409", code)
	}
}
