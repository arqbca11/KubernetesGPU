// Package crosscheck holds black-box tests written from the spec alone
// (docs/design/phase1-scheduler.md, CLAUDE.md, db/migrations), exercising the
// store and the HTTP API through their exported surfaces only.
package crosscheck

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/db"
	"github.com/arqbca11/KubernetesGPU/scheduler/api"
	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/policy"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

type env struct {
	ctx  context.Context
	pool *pgxpool.Pool
	st   *store.Store
	srv  *httptest.Server
}

func setup(t *testing.T) *env {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE shard_status, shard_jobs, builds, rounds, workers`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	st := store.New(pool)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(api.New(st, policy.AlwaysGPU{}, costmodel.Default(), api.Config{WorkerStaleAfter: time.Minute}, logger))
	t.Cleanup(srv.Close)
	return &env{ctx: ctx, pool: pool, st: st, srv: srv}
}

// do sends a JSON request and decodes the response into out (if non-nil).
// It returns the status code and the raw body.
func (e *env) do(t *testing.T, method, path string, body any, out any) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rd = bytes.NewBufferString(b)
		default:
			buf, err := json.Marshal(b)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			rd = bytes.NewReader(buf)
		}
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return resp.StatusCode, string(raw)
}

func (e *env) createRound(t *testing.T, nShards int32) int64 {
	t.Helper()
	var r api.CreateRoundResponse
	code, body := e.do(t, "POST", "/rounds", api.CreateRoundRequest{Scenario: "crosscheck", Seed: 7, NShards: nShards}, &r)
	if code != http.StatusCreated {
		t.Fatalf("POST /rounds: %d %s", code, body)
	}
	return r.RoundID
}

func (e *env) submit(t *testing.T, round int64, shard int32) api.SubmitBuildResponse {
	t.Helper()
	var r api.SubmitBuildResponse
	code, body := e.do(t, "POST", "/builds", api.SubmitBuildRequest{RoundID: round, ShardID: shard, NVectors: 10000, Dim: 64}, &r)
	if code != http.StatusCreated {
		t.Fatalf("POST /builds shard %d: %d %s", shard, code, body)
	}
	return r
}

// submitLocal inserts a local build directly through the store (placement is
// the caller's choice there), so a test can get a running local build without
// depending on the policy.
func (e *env) submitLocal(t *testing.T, round int64, shard int32) string {
	t.Helper()
	id := store.BuildID(shard, round)
	ok, err := e.st.SubmitBuild(e.ctx, store.Build{BuildID: id, RoundID: round, ShardID: shard, NVectors: 10000, Dim: 64, MemBytes: 1, Placement: "local"})
	if err != nil || !ok {
		t.Fatalf("SubmitBuild local %s: ok=%v err=%v", id, ok, err)
	}
	return id
}

func (e *env) mustBuild(t *testing.T, id string) store.BuildRow {
	t.Helper()
	b, ok, err := e.st.GetBuild(e.ctx, id)
	if err != nil || !ok {
		t.Fatalf("GetBuild %s: ok=%v err=%v", id, ok, err)
	}
	return b
}

func (e *env) mustRound(t *testing.T, id int64) store.Round {
	t.Helper()
	r, ok, err := e.st.GetRound(e.ctx, id)
	if err != nil || !ok {
		t.Fatalf("GetRound %d: ok=%v err=%v", id, ok, err)
	}
	return r
}

func logBuild(t *testing.T, label string, b store.BuildRow) {
	t.Helper()
	t.Logf("[db] %s: build_id=%s placement=%s state=%s attempt=%d priority=%v lease_owner=%v started_at=%v finished_at=%v",
		label, b.BuildID, b.Placement, b.State, b.Attempt, b.Priority, deref(b.LeaseOwner), tptr(b.StartedAt), tptr(b.FinishedAt))
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func tptr(p *time.Time) string {
	if p == nil {
		return "<nil>"
	}
	return p.Format(time.RFC3339Nano)
}

func report(stream bool) api.ReportRequest {
	return api.ReportRequest{StreamDone: stream}
}

func runJob(t *testing.T, e *env, round int64, shard, seq int32) {
	t.Helper()
	if ok, err := e.st.StartJob(e.ctx, round, shard, seq); err != nil || !ok {
		t.Fatalf("StartJob %d/%d/%d: ok=%v err=%v", round, shard, seq, ok, err)
	}
	if ok, err := e.st.FinishJob(e.ctx, round, shard, seq); err != nil || !ok {
		t.Fatalf("FinishJob %d/%d/%d: ok=%v err=%v", round, shard, seq, ok, err)
	}
}
