// Package store holds every statement that touches the builds queue and
// leases. The Python worker runs the same claim, renew and complete SQL
// (see worker/); this package is the Go side and the reference the
// integration test checks.
//
// Rules (CLAUDE.md invariants 2–5):
//   - Claim is one statement with FOR UPDATE SKIP LOCKED and bumps attempt.
//   - Every later write to a build is guarded by build_id AND attempt.
//     A zero-row update means the caller lost ownership and must stop.
//   - Reap only moves expired leases back to queued and is idempotent.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Build is a row of the builds table as the scheduler submits it.
type Build struct {
	BuildID   string
	RoundID   int64
	ShardID   int32
	NVectors  int64
	Dim       int32
	MemBytes  int64
	Placement string // "local" or "gpu"
	Priority  float64
}

// Claimed is what a worker gets back from a successful claim.
type Claimed struct {
	BuildID  string
	Attempt  int32
	NVectors int64
	Dim      int32
	MemBytes int64
}

// BuildID is the idempotency key: shard_id:round_id.
func BuildID(shardID int32, roundID int64) string {
	return fmt.Sprintf("%d:%d", shardID, roundID)
}

// CreateRound inserts a round and returns its id.
func (s *Store) CreateRound(ctx context.Context, scenario string, seed int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO rounds (scenario, seed) VALUES ($1, $2) RETURNING round_id`,
		scenario, seed).Scan(&id)
	return id, err
}

// SubmitBuild inserts a build. GPU builds start queued; local builds start
// running. A build_id that already exists is left untouched and inserted is
// false (invariant 6: resubmitting is a no-op).
func (s *Store) SubmitBuild(ctx context.Context, b Build) (inserted bool, err error) {
	var state string
	switch b.Placement {
	case "gpu":
		state = "queued"
	case "local":
		state = "running"
	default:
		return false, fmt.Errorf("submit %s: bad placement %q", b.BuildID, b.Placement)
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO builds
		  (build_id, round_id, shard_id, n_vectors, dim, mem_bytes, placement, state, priority, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
		        CASE WHEN $8 = 'running' THEN now() END)
		ON CONFLICT (build_id) DO NOTHING`,
		b.BuildID, b.RoundID, b.ShardID, b.NVectors, b.Dim, b.MemBytes, b.Placement, state, b.Priority)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Claim takes the highest-priority queued GPU build that fits in memCap and
// leases it to workerID for lease. ok is false when the queue has nothing
// claimable. attempt is incremented here and nowhere else.
func (s *Store) Claim(ctx context.Context, workerID string, memCap int64, lease time.Duration) (c Claimed, ok bool, err error) {
	err = s.pool.QueryRow(ctx, `
		UPDATE builds
		SET state = 'leased',
		    attempt = attempt + 1,
		    lease_owner = $1,
		    lease_until = now() + make_interval(secs => $3),
		    started_at = now()
		WHERE build_id = (
		  SELECT build_id FROM builds
		  WHERE state = 'queued' AND placement = 'gpu' AND mem_bytes <= $2
		  ORDER BY priority DESC, enqueued_at
		  LIMIT 1
		  FOR UPDATE SKIP LOCKED)
		RETURNING build_id, attempt, n_vectors, dim, mem_bytes`,
		workerID, memCap, lease.Seconds()).
		Scan(&c.BuildID, &c.Attempt, &c.NVectors, &c.Dim, &c.MemBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claimed{}, false, nil
	}
	if err != nil {
		return Claimed{}, false, err
	}
	return c, true, nil
}

// Renew extends the lease. false means the lease is lost: stop working.
func (s *Store) Renew(ctx context.Context, buildID string, attempt int32, lease time.Duration) (bool, error) {
	return s.guarded(ctx, `
		UPDATE builds
		SET lease_until = now() + make_interval(secs => $3)
		WHERE build_id = $1 AND attempt = $2 AND state = 'leased'`,
		buildID, attempt, lease.Seconds())
}

// Complete marks a leased build done. false means a stale attempt.
func (s *Store) Complete(ctx context.Context, buildID string, attempt int32) (bool, error) {
	return s.guarded(ctx, `
		UPDATE builds
		SET state = 'done', finished_at = now(), lease_owner = NULL, lease_until = NULL
		WHERE build_id = $1 AND attempt = $2 AND state = 'leased'`,
		buildID, attempt)
}

// Fail marks a leased build failed with a reason. false means a stale attempt.
func (s *Store) Fail(ctx context.Context, buildID string, attempt int32, reason string) (bool, error) {
	return s.guarded(ctx, `
		UPDATE builds
		SET state = 'failed', fail_reason = $3, finished_at = now(), lease_owner = NULL, lease_until = NULL
		WHERE build_id = $1 AND attempt = $2 AND state = 'leased'`,
		buildID, attempt, reason)
}

// Release hands a leased build back to the queue (graceful shutdown).
// Faster than waiting for the reaper. false means a stale attempt.
func (s *Store) Release(ctx context.Context, buildID string, attempt int32) (bool, error) {
	return s.guarded(ctx, `
		UPDATE builds
		SET state = 'queued', lease_owner = NULL, lease_until = NULL, started_at = NULL
		WHERE build_id = $1 AND attempt = $2 AND state = 'leased'`,
		buildID, attempt)
}

// CompleteLocal marks a local build done. Local builds have no lease, so the
// guard is the placement and state, not an attempt.
func (s *Store) CompleteLocal(ctx context.Context, buildID string) (bool, error) {
	return s.guarded(ctx, `
		UPDATE builds
		SET state = 'done', finished_at = now()
		WHERE build_id = $1 AND placement = 'local' AND state = 'running'`,
		buildID)
}

// Reap returns every expired lease to the queue and reports how many.
// Idempotent: a second call right after the first touches nothing.
func (s *Store) Reap(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE builds
		SET state = 'queued', lease_owner = NULL, lease_until = NULL, started_at = NULL
		WHERE state = 'leased' AND lease_until < now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Heartbeat registers a worker or refreshes its last_seen.
func (s *Store) Heartbeat(ctx context.Context, workerID string, memBytes int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (worker_id, mem_bytes, last_seen)
		VALUES ($1, $2, now())
		ON CONFLICT (worker_id) DO UPDATE
		SET mem_bytes = EXCLUDED.mem_bytes, last_seen = now()`,
		workerID, memBytes)
	return err
}

// LargestLiveWorkerMem is the biggest memory capacity among workers seen
// within maxAge. Zero when there are none. Used at submit time: a build that
// fits nowhere must build locally.
func (s *Store) LargestLiveWorkerMem(ctx context.Context, maxAge time.Duration) (int64, error) {
	var mem int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(mem_bytes), 0) FROM workers
		WHERE last_seen > now() - make_interval(secs => $1)`,
		maxAge.Seconds()).Scan(&mem)
	return mem, err
}

func (s *Store) guarded(ctx context.Context, sql string, args ...any) (bool, error) {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
