// Package crosscheck holds independent black-box tests of the Phase 1
// scheduler, written only from docs/design/phase1-scheduler.md, CLAUDE.md,
// the roadmap, the SQL migrations and `go doc` output.
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

const lease30 = 30 * time.Second

// setup connects, migrates and empties every table.
func setup(t *testing.T) (context.Context, *pgxpool.Pool, *store.Store) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE shard_jobs, builds, rounds, workers"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return ctx, pool, store.New(pool)
}

func mustRound(t *testing.T, ctx context.Context, st *store.Store, nShards int32) int64 {
	t.Helper()
	id, err := st.CreateRound(ctx, "crosscheck", 42, nShards)
	if err != nil {
		t.Fatalf("CreateRound: %v", err)
	}
	return id
}

func gpuBuild(roundID int64, shard int32, prio float64, mem int64) store.Build {
	return store.Build{
		BuildID:   store.BuildID(shard, roundID),
		RoundID:   roundID,
		ShardID:   shard,
		NVectors:  1000,
		Dim:       16,
		MemBytes:  mem,
		Placement: "gpu",
		Priority:  prio,
	}
}

func mustSubmit(t *testing.T, ctx context.Context, st *store.Store, b store.Build, jobs []store.JobSpec) {
	t.Helper()
	ins, err := st.SubmitBuild(ctx, b, jobs)
	if err != nil {
		t.Fatalf("SubmitBuild %s: %v", b.BuildID, err)
	}
	if !ins {
		t.Fatalf("SubmitBuild %s: inserted=false on first submit", b.BuildID)
	}
}

func mustGet(t *testing.T, ctx context.Context, st *store.Store, id string) store.BuildRow {
	t.Helper()
	r, ok, err := st.GetBuild(ctx, id)
	if err != nil || !ok {
		t.Fatalf("GetBuild %s: ok=%v err=%v", id, ok, err)
	}
	return r
}

func fmtRow(r store.BuildRow) string {
	s := func(p *string) string {
		if p == nil {
			return "NULL"
		}
		return *p
	}
	tm := func(p *time.Time) string {
		if p == nil {
			return "NULL"
		}
		return p.Format(time.RFC3339Nano)
	}
	return "build_id=" + r.BuildID + " placement=" + r.Placement + " state=" + r.State +
		" attempt=" + itoa(int64(r.Attempt)) + " lease_owner=" + s(r.LeaseOwner) +
		" lease_until=" + tm(r.LeaseUntil) + " started_at=" + tm(r.StartedAt) +
		" finished_at=" + tm(r.FinishedAt) + " fail_reason=" + s(r.FailReason)
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func newServer(t *testing.T, st *store.Store) *httptest.Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(api.New(st, policy.AlwaysGPU{}, costmodel.Default(),
		api.Config{WorkerStaleAfter: 30 * time.Second}, logger))
	t.Cleanup(srv.Close)
	return srv
}

// do sends a request and returns the status and raw body.
func do(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = bytes.NewBufferString(b)
	default:
		buf, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}
