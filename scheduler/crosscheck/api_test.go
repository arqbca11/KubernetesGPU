package crosscheck

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/arqbca11/KubernetesGPU/scheduler/api"
)

func createRound(t *testing.T, base string, n int32) int64 {
	t.Helper()
	code, body := do(t, "POST", base+"/rounds", api.CreateRoundRequest{Scenario: "crosscheck", Seed: 1, NShards: n})
	if code != http.StatusCreated {
		t.Fatalf("POST /rounds: %d %s", code, body)
	}
	var cr api.CreateRoundResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		t.Fatalf("decode: %v %s", err, body)
	}
	return cr.RoundID
}

func submit(t *testing.T, base string, req api.SubmitBuildRequest) (int, api.SubmitBuildResponse, []byte) {
	t.Helper()
	code, body := do(t, "POST", base+"/builds", req)
	var resp api.SubmitBuildResponse
	_ = json.Unmarshal(body, &resp)
	return code, resp, body
}

// Spec: API table POST /builds ("201 ... created:true; a repeat returns 200 with
// created:false and the existing decision", invariant 6) and decision 19 (zero
// live workers still places on the GPU queue).
func TestAPISubmitIsIdempotentAndQueuesWithEmptyPool(t *testing.T) {
	ctx, _, st := setup(t)
	srv := newServer(t, st)
	r := createRound(t, srv.URL, 1)
	req := api.SubmitBuildRequest{RoundID: r, ShardID: 3, NVectors: 100000, Dim: 128,
		Jobs: []api.JobRequest{{DurationMs: 10, NeedsIndex: false}}}
	code1, resp1, body1 := submit(t, srv.URL, req)
	t.Logf("first POST /builds with zero workers: %d %s", code1, body1)
	if code1 != http.StatusCreated || !resp1.Created {
		t.Errorf("API table: first submit is 201 with created:true")
	}
	if resp1.BuildID != fmt.Sprintf("3:%d", r) {
		t.Errorf("invariant 6: build_id is shard_id:round_id = 3:%d; got %q", r, resp1.BuildID)
	}
	if resp1.Placement != "gpu" {
		t.Errorf("decision 19: zero live workers still places on the GPU queue; got %q", resp1.Placement)
	}
	if resp1.CPUBuildMs <= 0 || resp1.GPUTotalMs <= 0 || resp1.MemBytes <= 0 {
		t.Errorf("decision 20: response carries cost estimates; got cpu=%d gpu=%d mem=%d", resp1.CPUBuildMs, resp1.GPUTotalMs, resp1.MemBytes)
	}

	// Now a tiny worker is live: a fresh decision would be local. The repeat must
	// still return the existing gpu decision.
	if err := st.Heartbeat(ctx, "tiny", 1); err != nil {
		t.Fatal(err)
	}
	req2 := req
	req2.NVectors = 5
	req2.Jobs = []api.JobRequest{{DurationMs: 999, NeedsIndex: true}, {DurationMs: 1}}
	code2, resp2, body2 := submit(t, srv.URL, req2)
	row := mustGet(t, ctx, st, resp1.BuildID)
	jobs, _ := st.ListJobs(ctx, r)
	t.Logf("repeat POST /builds (different n_vectors/jobs, and a 1-byte worker now live): %d %s", code2, body2)
	t.Logf("Postgres row: %s n_vectors=%d; jobs: %+v", fmtRow(row), row.NVectors, jobs)
	if code2 != http.StatusOK || resp2.Created {
		t.Errorf("API table: repeat submit is 200 with created:false")
	}
	if resp2.BuildID != resp1.BuildID || resp2.Placement != resp1.Placement || resp2.Priority != resp1.Priority || resp2.MemBytes != resp1.MemBytes {
		t.Errorf("API table: repeat returns the existing decision; first %+v repeat %+v", resp1, resp2)
	}
	if resp2.CPUBuildMs != resp1.CPUBuildMs || resp2.GPUTotalMs != resp1.GPUTotalMs {
		t.Errorf("go doc SubmitBuildResponse.Created: on a repeat the fields describe the existing row, and decision 20 says the shard sleeps for cpu_build_ms; "+
			"existing row (n_vectors=100000, dim=128) was estimated cpu=%dms gpu=%dms, repeat returned cpu=%dms gpu=%dms",
			resp1.CPUBuildMs, resp1.GPUTotalMs, resp2.CPUBuildMs, resp2.GPUTotalMs)
	}
	if row.Placement != "gpu" || row.State != "queued" || row.NVectors != 100000 || len(jobs) != 1 || jobs[0].DurationMs != 10 {
		t.Errorf("invariant 6: duplicate submit must not change the row or jobs")
	}
}

// Spec: API table POST /builds/{id}/done ("409 unless the build is a running local
// build") and POST /jobs/.../start, /done ("409 if the job is not in the right state").
func TestAPIConflictsOnWrongState(t *testing.T) {
	ctx, _, st := setup(t)
	srv := newServer(t, st)
	r := createRound(t, srv.URL, 2)

	_, gpuResp, body := submit(t, srv.URL, api.SubmitBuildRequest{RoundID: r, ShardID: 1, NVectors: 1000, Dim: 16})
	t.Logf("shard 1 submit (no workers): %s", body)
	code, b := do(t, "POST", srv.URL+"/builds/"+gpuResp.BuildID+"/done", nil)
	t.Logf("POST /builds/%s/done on a queued gpu build: %d %s; row %s", gpuResp.BuildID, code, b, fmtRow(mustGet(t, ctx, st, gpuResp.BuildID)))
	if code != http.StatusConflict {
		t.Errorf("API table: /builds/{id}/done on a gpu build must be 409; got %d", code)
	}

	if err := st.Heartbeat(ctx, "tiny", 1); err != nil {
		t.Fatal(err)
	}
	_, locResp, body := submit(t, srv.URL, api.SubmitBuildRequest{RoundID: r, ShardID: 2, NVectors: 1000, Dim: 16,
		Jobs: []api.JobRequest{{DurationMs: 5, NeedsIndex: true}}})
	t.Logf("shard 2 submit with a 1-byte live worker: %s", body)
	if locResp.Placement != "local" {
		t.Fatalf("policy v0: a build that fits no live worker goes local; got %q", locResp.Placement)
	}
	c1, _ := do(t, "POST", srv.URL+"/builds/"+locResp.BuildID+"/done", nil)
	c2, _ := do(t, "POST", srv.URL+"/builds/"+locResp.BuildID+"/done", nil)
	t.Logf("local done twice: %d then %d; row %s", c1, c2, fmtRow(mustGet(t, ctx, st, locResp.BuildID)))
	if c1 < 200 || c1 > 299 || c2 != http.StatusConflict {
		t.Errorf("API table: first local done is 2xx, second is 409 (no longer running)")
	}

	job := fmt.Sprintf("%s/jobs/%d/2/0", srv.URL, r)
	cDoneEarly, _ := do(t, "POST", job+"/done", nil)
	cStart, _ := do(t, "POST", job+"/start", nil)
	cStart2, _ := do(t, "POST", job+"/start", nil)
	cDone, _ := do(t, "POST", job+"/done", nil)
	cDone2, _ := do(t, "POST", job+"/done", nil)
	cMissing, _ := do(t, "POST", fmt.Sprintf("%s/jobs/%d/2/7/start", srv.URL, r), nil)
	jobs, _ := st.ListJobs(ctx, r)
	t.Logf("job events: done-before-start=%d start=%d start-again=%d done=%d done-again=%d start-nonexistent=%d; jobs %+v",
		cDoneEarly, cStart, cStart2, cDone, cDone2, cMissing, jobs)
	if cDoneEarly != http.StatusConflict || cStart2 != http.StatusConflict || cDone2 != http.StatusConflict {
		t.Errorf("API table: job events out of order must be 409")
	}
	if cStart < 200 || cStart > 299 || cDone < 200 || cDone > 299 {
		t.Errorf("in-order job start/done should succeed")
	}
	if cMissing != http.StatusConflict && cMissing != http.StatusNotFound {
		t.Errorf("a job that does not exist should be 404 or 409; got %d", cMissing)
	}
}

// Spec: API table GET /rounds/{id} ("Finishes complete rounds, then returns the
// round, every build row and every job row"); decision 18.
func TestAPIGetRoundFinishesCompleteRound(t *testing.T) {
	ctx, _, st := setup(t)
	srv := newServer(t, st)
	r := createRound(t, srv.URL, 1)
	if err := st.Heartbeat(ctx, "tiny", 1); err != nil {
		t.Fatal(err)
	}
	_, resp, _ := submit(t, srv.URL, api.SubmitBuildRequest{RoundID: r, ShardID: 1, NVectors: 1000, Dim: 16,
		Jobs: []api.JobRequest{{DurationMs: 1}}})
	do(t, "POST", srv.URL+"/builds/"+resp.BuildID+"/done", nil)
	do(t, "POST", fmt.Sprintf("%s/jobs/%d/1/0/start", srv.URL, r), nil)
	do(t, "POST", fmt.Sprintf("%s/jobs/%d/1/0/done", srv.URL, r), nil)

	before, _, _ := st.GetRound(ctx, r)
	code, body := do(t, "GET", fmt.Sprintf("%s/rounds/%d", srv.URL, r), nil)
	var rs api.RoundStatus
	_ = json.Unmarshal(body, &rs)
	after, _, _ := st.GetRound(ctx, r)
	t.Logf("before GET (no reaper running) finished_at=%v; GET /rounds/%d -> %d %s; after finished_at=%v", before.FinishedAt, r, code, body, after.FinishedAt)
	if code != http.StatusOK {
		t.Fatalf("GET /rounds: %d", code)
	}
	if rs.Round.FinishedAt == nil || after.FinishedAt == nil {
		t.Errorf("API table: GET /rounds/{id} finishes complete rounds before answering")
	}
	if rs.Round.NShards != 1 || len(rs.Builds) != 1 || len(rs.Jobs) != 1 {
		t.Errorf("GET /rounds returns the round (n_shards=1), every build and every job; got %+v", rs)
	}
	code404, _ := do(t, "GET", srv.URL+"/rounds/987654321", nil)
	t.Logf("GET unknown round: %d", code404)
	if code404 != http.StatusNotFound {
		t.Errorf("unknown round should be 404 (design test ValidationAndNotFound); got %d", code404)
	}
}

// Spec: decision 21 ("DisallowUnknownFields on request bodies catches client typos").
func TestAPIRejectsUnknownFields(t *testing.T) {
	ctx, _, st := setup(t)
	srv := newServer(t, st)
	r := createRound(t, srv.URL, 1)
	body := fmt.Sprintf(`{"round_id":%d,"shard_id":1,"n_vectors":1000,"dim":16,"n_vectros":5}`, r)
	code, out := do(t, "POST", srv.URL+"/builds", body)
	rows, _ := st.ListBuilds(ctx, r)
	t.Logf("POST /builds with typo field n_vectros: %d %s; builds in round: %d", code, out, len(rows))
	if code != http.StatusBadRequest {
		t.Errorf("decision 21: unknown field must be rejected with 400; got %d", code)
	}
	if len(rows) != 0 {
		t.Errorf("a rejected request must not insert a build")
	}
}
