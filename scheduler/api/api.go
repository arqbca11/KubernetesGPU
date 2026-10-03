// Package api is the scheduler's HTTP surface. Shards talk to it; workers
// never do (they use SQL directly, see design decision 4).
//
//	POST /rounds                                 start a round
//	GET  /rounds/{id}                            round status with every build and job
//	POST /builds                                 submit a build (+ follow-up jobs); returns placement
//	POST /builds/{id}/done                       a shard finished a local build
//	POST /jobs/{round}/{shard}/{seq}/start       a follow-up job began
//	POST /jobs/{round}/{shard}/{seq}/done        a follow-up job ended
//	GET  /workers                                registered workers and the live pool state
//	GET  /healthz                                200 when Postgres answers
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/policy"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

type Config struct {
	WorkerStaleAfter time.Duration // a worker not seen for this long is not in the pool
}

type Server struct {
	st  *store.Store
	pol policy.Policy
	cm  costmodel.Model
	cfg Config
	log *slog.Logger
	mux *http.ServeMux
}

func New(st *store.Store, pol policy.Policy, cm costmodel.Model, cfg Config, log *slog.Logger) *Server {
	s := &Server{st: st, pol: pol, cm: cm, cfg: cfg, log: log, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /rounds", s.createRound)
	s.mux.HandleFunc("GET /rounds/{id}", s.getRound)
	s.mux.HandleFunc("POST /builds", s.submitBuild)
	s.mux.HandleFunc("POST /builds/{id}/done", s.localBuildDone)
	s.mux.HandleFunc("POST /jobs/{round}/{shard}/{seq}/start", s.jobStart)
	s.mux.HandleFunc("POST /jobs/{round}/{shard}/{seq}/done", s.jobDone)
	s.mux.HandleFunc("GET /workers", s.workers)
	s.mux.HandleFunc("GET /healthz", s.healthz)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// ---- rounds ---------------------------------------------------------------

type CreateRoundRequest struct {
	Scenario string `json:"scenario"`
	Seed     int64  `json:"seed"`
	NShards  int32  `json:"n_shards"`
}

type CreateRoundResponse struct {
	RoundID int64 `json:"round_id"`
}

func (s *Server) createRound(w http.ResponseWriter, r *http.Request) {
	var req CreateRoundRequest
	if !decode(w, r, &req) {
		return
	}
	if req.NShards <= 0 {
		writeError(w, http.StatusBadRequest, "n_shards must be positive")
		return
	}
	if req.Scenario == "" {
		req.Scenario = "default"
	}
	id, err := s.st.CreateRound(r.Context(), req.Scenario, req.Seed, req.NShards)
	if err != nil {
		s.internal(w, "create round", err)
		return
	}
	s.log.Info("round started", "round_id", id, "scenario", req.Scenario, "seed", req.Seed, "n_shards", req.NShards)
	writeJSON(w, http.StatusCreated, CreateRoundResponse{RoundID: id})
}

type RoundStatus struct {
	Round  store.Round      `json:"round"`
	Builds []store.BuildRow `json:"builds"`
	Jobs   []store.JobRow   `json:"jobs"`
}

func (s *Server) getRound(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "id")
	if !ok {
		return
	}
	ctx := r.Context()
	// Make the answer fresh: GPU completions happen via SQL and the reaper
	// tick may not have run since.
	if _, err := s.st.FinishCompleteRounds(ctx); err != nil {
		s.internal(w, "finish rounds", err)
		return
	}
	rd, found, err := s.st.GetRound(ctx, id)
	if err != nil {
		s.internal(w, "get round", err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "no such round")
		return
	}
	builds, err := s.st.ListBuilds(ctx, id)
	if err != nil {
		s.internal(w, "list builds", err)
		return
	}
	jobs, err := s.st.ListJobs(ctx, id)
	if err != nil {
		s.internal(w, "list jobs", err)
		return
	}
	if builds == nil {
		builds = []store.BuildRow{}
	}
	if jobs == nil {
		jobs = []store.JobRow{}
	}
	writeJSON(w, http.StatusOK, RoundStatus{Round: rd, Builds: builds, Jobs: jobs})
}

// ---- builds ---------------------------------------------------------------

type JobRequest struct {
	DurationMs int64 `json:"duration_ms"`
	NeedsIndex bool  `json:"needs_index"`
}

type SubmitBuildRequest struct {
	RoundID  int64        `json:"round_id"`
	ShardID  int32        `json:"shard_id"`
	NVectors int64        `json:"n_vectors"`
	Dim      int32        `json:"dim"`
	Jobs     []JobRequest `json:"jobs"` // in execution order; seq is the index
}

type SubmitBuildResponse struct {
	BuildID   string  `json:"build_id"`
	Placement string  `json:"placement"`
	Priority  float64 `json:"priority"`
	Reason    string  `json:"reason"`
	MemBytes  int64   `json:"mem_bytes"`
	Created   bool    `json:"created"` // false: this build_id already existed; fields describe the existing row
	// Cost model estimates, so the shard can sleep for the right time on a local build.
	CPUBuildMs int64 `json:"cpu_build_ms"`
	GPUTotalMs int64 `json:"gpu_total_ms"`
}

func (s *Server) submitBuild(w http.ResponseWriter, r *http.Request) {
	var req SubmitBuildRequest
	if !decode(w, r, &req) {
		return
	}
	if req.RoundID <= 0 || req.ShardID < 0 || req.NVectors <= 0 || req.Dim <= 0 {
		writeError(w, http.StatusBadRequest, "round_id, n_vectors and dim must be positive; shard_id non-negative")
		return
	}
	for i, j := range req.Jobs {
		if j.DurationMs < 0 {
			writeError(w, http.StatusBadRequest, "jobs["+strconv.Itoa(i)+"].duration_ms must be non-negative")
			return
		}
	}
	ctx := r.Context()
	if _, found, err := s.st.GetRound(ctx, req.RoundID); err != nil {
		s.internal(w, "get round", err)
		return
	} else if !found {
		writeError(w, http.StatusNotFound, "no such round")
		return
	}

	buildID := store.BuildID(req.ShardID, req.RoundID)
	mem := s.cm.GPUMemBytes(req.NVectors, req.Dim)
	resp := SubmitBuildResponse{
		BuildID:    buildID,
		MemBytes:   mem,
		CPUBuildMs: s.cm.CPUBuild(req.NVectors, req.Dim).Milliseconds(),
		GPUTotalMs: s.cm.GPUTotal(req.NVectors, req.Dim).Milliseconds(),
	}

	pool, err := s.st.Pool(ctx, s.cfg.WorkerStaleAfter)
	if err != nil {
		s.internal(w, "pool state", err)
		return
	}
	spec := policy.BuildSpec{ShardID: req.ShardID, RoundID: req.RoundID, NVectors: req.NVectors, Dim: req.Dim, MemBytes: mem}
	jobs := make([]store.JobSpec, len(req.Jobs))
	for i, j := range req.Jobs {
		spec.Jobs = append(spec.Jobs, policy.Job{DurationMs: j.DurationMs, NeedsIndex: j.NeedsIndex})
		jobs[i] = store.JobSpec{Seq: int32(i), DurationMs: j.DurationMs, NeedsIndex: j.NeedsIndex}
	}
	dec := s.pol.Decide(policy.Input{Build: spec, Pool: policy.PoolState{LiveWorkers: pool.LiveWorkers, LargestMem: pool.LargestMem}})

	inserted, err := s.st.SubmitBuild(ctx, store.Build{
		BuildID: buildID, RoundID: req.RoundID, ShardID: req.ShardID, NVectors: req.NVectors,
		Dim: req.Dim, MemBytes: mem, Placement: dec.Placement, Priority: dec.Priority,
	}, jobs)
	if err != nil {
		s.internal(w, "submit build", err)
		return
	}
	if !inserted {
		// Invariant 6: the row already exists; describe it and change nothing.
		existing, _, err := s.st.GetBuild(ctx, buildID)
		if err != nil {
			s.internal(w, "get build", err)
			return
		}
		resp.Placement, resp.Priority, resp.MemBytes = existing.Placement, existing.Priority, existing.MemBytes
		resp.Reason = "duplicate submit; existing decision returned"
		s.log.Info("duplicate submit ignored", "build_id", buildID, "round_id", req.RoundID, "attempt", existing.Attempt, "placement", existing.Placement)
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Placement, resp.Priority, resp.Reason, resp.Created = dec.Placement, dec.Priority, dec.Reason, true
	s.log.Info("build placed", "build_id", buildID, "round_id", req.RoundID, "attempt", 0,
		"shard_id", req.ShardID, "placement", dec.Placement, "priority", dec.Priority, "policy", s.pol.Name(),
		"reason", dec.Reason, "n_vectors", req.NVectors, "dim", req.Dim, "mem_bytes", mem,
		"live_workers", pool.LiveWorkers, "jobs", len(jobs))
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) localBuildDone(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	ok, err := s.st.CompleteLocal(ctx, id)
	if err != nil {
		s.internal(w, "complete local", err)
		return
	}
	if !ok {
		b, found, err := s.st.GetBuild(ctx, id)
		if err != nil {
			s.internal(w, "get build", err)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "no such build")
			return
		}
		writeError(w, http.StatusConflict, "build is "+b.Placement+"/"+b.State+", not a running local build")
		return
	}
	b, _, _ := s.st.GetBuild(ctx, id)
	s.log.Info("local build done", "build_id", id, "round_id", b.RoundID, "attempt", b.Attempt, "shard_id", b.ShardID)
	writeJSON(w, http.StatusOK, map[string]string{"build_id": id, "state": "done"})
}

// ---- follow-up jobs -------------------------------------------------------

func (s *Server) jobStart(w http.ResponseWriter, r *http.Request) {
	s.jobEvent(w, r, s.st.StartJob, "started")
}
func (s *Server) jobDone(w http.ResponseWriter, r *http.Request) {
	s.jobEvent(w, r, s.st.FinishJob, "finished")
}

func (s *Server) jobEvent(w http.ResponseWriter, r *http.Request,
	op func(context.Context, int64, int32, int32) (bool, error), what string) {
	round, ok := pathInt64(w, r, "round")
	if !ok {
		return
	}
	shard, ok := pathInt32(w, r, "shard")
	if !ok {
		return
	}
	seq, ok := pathInt32(w, r, "seq")
	if !ok {
		return
	}
	done, err := op(r.Context(), round, shard, seq)
	if err != nil {
		s.internal(w, "job "+what, err)
		return
	}
	if !done {
		writeError(w, http.StatusConflict, "no such job, or it is not in the right state to be "+what)
		return
	}
	s.log.Info("follow-up job "+what, "round_id", round, "shard_id", shard, "seq", seq,
		"build_id", store.BuildID(shard, round))
	writeJSON(w, http.StatusOK, map[string]any{"round_id": round, "shard_id": shard, "seq": seq, "event": what})
}

// ---- workers, health ------------------------------------------------------

type WorkersResponse struct {
	Pool    store.PoolState `json:"pool"`
	Workers []store.Worker  `json:"workers"`
}

func (s *Server) workers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pool, err := s.st.Pool(ctx, s.cfg.WorkerStaleAfter)
	if err != nil {
		s.internal(w, "pool", err)
		return
	}
	ws, err := s.st.ListWorkers(ctx)
	if err != nil {
		s.internal(w, "list workers", err)
		return
	}
	if ws == nil {
		ws = []store.Worker{}
	}
	writeJSON(w, http.StatusOK, WorkersResponse{Pool: pool, Workers: ws})
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.st.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "postgres: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- helpers --------------------------------------------------------------

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
		return false
	}
	return true
}

func pathInt64(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	v, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, name+" must be an integer")
		return 0, false
	}
	return v, true
}

func pathInt32(w http.ResponseWriter, r *http.Request, name string) (int32, bool) {
	v, err := strconv.ParseInt(r.PathValue(name), 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest, name+" must be a 32-bit integer")
		return 0, false
	}
	return int32(v), true
}

func (s *Server) internal(w http.ResponseWriter, what string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	s.log.Error(what+" failed", "err", err)
	writeError(w, http.StatusInternalServerError, what+" failed")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
