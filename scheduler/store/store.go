// Package store holds every statement that touches rounds, builds, follow-up
// jobs and workers. The Python worker runs the same claim, renew, complete,
// fail, release and heartbeat SQL (see worker/); this package is the Go side
// and the reference the integration test checks.
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

// Ping checks the database is reachable (health endpoint).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// ---- rounds -------------------------------------------------------------

type Round struct {
	RoundID    int64      `json:"round_id"`
	Scenario   string     `json:"scenario"`
	Seed       int64      `json:"seed"`
	NShards    int32      `json:"n_shards"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

// CreateRound inserts a round expecting nShards shards and returns its id.
func (s *Store) CreateRound(ctx context.Context, scenario string, seed int64, nShards int32) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO rounds (scenario, seed, n_shards) VALUES ($1, $2, $3) RETURNING round_id`,
		scenario, seed, nShards).Scan(&id)
	return id, err
}

func (s *Store) GetRound(ctx context.Context, id int64) (Round, bool, error) {
	var r Round
	err := s.pool.QueryRow(ctx,
		`SELECT round_id, scenario, seed, n_shards, started_at, finished_at FROM rounds WHERE round_id = $1`, id).
		Scan(&r.RoundID, &r.Scenario, &r.Seed, &r.NShards, &r.StartedAt, &r.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Round{}, false, nil
	}
	return r, err == nil, err
}

// FinishCompleteRounds stamps finished_at on every round whose n_shards
// builds have all reached done or failed and whose follow-up jobs have all
// finished. finished_at is the latest build or job finish, not now(), so the
// round duration does not depend on how often this runs. Idempotent.
func (s *Store) FinishCompleteRounds(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE rounds r
		SET finished_at = GREATEST(
		      (SELECT MAX(finished_at) FROM builds     WHERE round_id = r.round_id),
		      (SELECT MAX(finished_at) FROM shard_jobs WHERE round_id = r.round_id))
		WHERE r.finished_at IS NULL
		  AND (SELECT COUNT(*) FROM builds b WHERE b.round_id = r.round_id) = r.n_shards
		  AND NOT EXISTS (SELECT 1 FROM builds b
		                  WHERE b.round_id = r.round_id AND b.state NOT IN ('done', 'failed'))
		  AND NOT EXISTS (SELECT 1 FROM shard_jobs j
		                  WHERE j.round_id = r.round_id AND j.finished_at IS NULL)`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ---- builds -------------------------------------------------------------

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

// BuildRow is a full row as read back.
type BuildRow struct {
	BuildID    string     `json:"build_id"`
	RoundID    int64      `json:"round_id"`
	ShardID    int32      `json:"shard_id"`
	NVectors   int64      `json:"n_vectors"`
	Dim        int32      `json:"dim"`
	MemBytes   int64      `json:"mem_bytes"`
	Placement  string     `json:"placement"`
	State      string     `json:"state"`
	Priority   float64    `json:"priority"`
	Attempt    int32      `json:"attempt"`
	LeaseOwner *string    `json:"lease_owner"`
	LeaseUntil *time.Time `json:"lease_until"`
	FailReason *string    `json:"fail_reason"`
	EnqueuedAt time.Time  `json:"enqueued_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

// JobSpec is a follow-up job as the shard declares it at submit time.
type JobSpec struct {
	Seq        int32
	DurationMs int64
	NeedsIndex bool
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

const selectBuild = `
	SELECT build_id, round_id, shard_id, n_vectors, dim, mem_bytes, placement, state,
	       priority, attempt, lease_owner, lease_until, fail_reason,
	       enqueued_at, started_at, finished_at
	FROM builds`

func scanBuild(row pgx.Row) (BuildRow, error) {
	var b BuildRow
	err := row.Scan(&b.BuildID, &b.RoundID, &b.ShardID, &b.NVectors, &b.Dim, &b.MemBytes,
		&b.Placement, &b.State, &b.Priority, &b.Attempt, &b.LeaseOwner, &b.LeaseUntil,
		&b.FailReason, &b.EnqueuedAt, &b.StartedAt, &b.FinishedAt)
	return b, err
}

func (s *Store) GetBuild(ctx context.Context, id string) (BuildRow, bool, error) {
	b, err := scanBuild(s.pool.QueryRow(ctx, selectBuild+` WHERE build_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return BuildRow{}, false, nil
	}
	return b, err == nil, err
}

func (s *Store) ListBuilds(ctx context.Context, roundID int64) ([]BuildRow, error) {
	rows, err := s.pool.Query(ctx, selectBuild+` WHERE round_id = $1 ORDER BY shard_id`, roundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BuildRow
	for rows.Next() {
		b, err := scanBuild(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// SubmitBuild inserts a build and its follow-up jobs in one transaction. GPU
// builds start queued; local builds start running. A build_id that already
// exists leaves everything untouched and returns inserted=false (invariant
// 6: resubmitting is a no-op).
func (s *Store) SubmitBuild(ctx context.Context, b Build, jobs []JobSpec) (inserted bool, err error) {
	var state string
	switch b.Placement {
	case "gpu":
		state = "queued"
	case "local":
		state = "running"
	default:
		return false, fmt.Errorf("submit %s: bad placement %q", b.BuildID, b.Placement)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		INSERT INTO builds
		  (build_id, round_id, shard_id, n_vectors, dim, mem_bytes, placement, state, priority, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
		        CASE WHEN $8 = 'running' THEN now() END)
		ON CONFLICT (build_id) DO NOTHING`,
		b.BuildID, b.RoundID, b.ShardID, b.NVectors, b.Dim, b.MemBytes, b.Placement, state, b.Priority)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	for _, j := range jobs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO shard_jobs (round_id, shard_id, seq, duration_ms, needs_index)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING`,
			b.RoundID, b.ShardID, j.Seq, j.DurationMs, j.NeedsIndex); err != nil {
			return false, err
		}
	}
	return true, tx.Commit(ctx)
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

// Reaped identifies one lease the reaper returned to the queue.
type Reaped struct {
	BuildID    string
	RoundID    int64
	Attempt    int32
	LeaseOwner string
}

// Reap returns every expired lease to the queue and reports which ones.
// Idempotent: a second call right after the first touches nothing.
func (s *Store) Reap(ctx context.Context) ([]Reaped, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE builds
		SET state = 'queued', lease_owner = NULL, lease_until = NULL, started_at = NULL
		WHERE state = 'leased' AND lease_until < now()
		RETURNING build_id, round_id, attempt, (SELECT lease_owner FROM builds b2 WHERE b2.build_id = builds.build_id)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Reaped
	for rows.Next() {
		var r Reaped
		var owner *string
		if err := rows.Scan(&r.BuildID, &r.RoundID, &r.Attempt, &owner); err != nil {
			return nil, err
		}
		if owner != nil {
			r.LeaseOwner = *owner
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- follow-up jobs -----------------------------------------------------

type JobRow struct {
	RoundID    int64      `json:"round_id"`
	ShardID    int32      `json:"shard_id"`
	Seq        int32      `json:"seq"`
	DurationMs int64      `json:"duration_ms"`
	NeedsIndex bool       `json:"needs_index"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

// StartJob records that a follow-up job began. false if no such job or
// already started.
func (s *Store) StartJob(ctx context.Context, roundID int64, shardID, seq int32) (bool, error) {
	return s.guarded(ctx, `
		UPDATE shard_jobs SET started_at = now()
		WHERE round_id = $1 AND shard_id = $2 AND seq = $3 AND started_at IS NULL`,
		roundID, shardID, seq)
}

// FinishJob records that a started follow-up job ended. false if no such
// job, not started, or already finished.
func (s *Store) FinishJob(ctx context.Context, roundID int64, shardID, seq int32) (bool, error) {
	return s.guarded(ctx, `
		UPDATE shard_jobs SET finished_at = now()
		WHERE round_id = $1 AND shard_id = $2 AND seq = $3
		  AND started_at IS NOT NULL AND finished_at IS NULL`,
		roundID, shardID, seq)
}

func (s *Store) ListJobs(ctx context.Context, roundID int64) ([]JobRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT round_id, shard_id, seq, duration_ms, needs_index, started_at, finished_at
		FROM shard_jobs WHERE round_id = $1 ORDER BY shard_id, seq`, roundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobRow
	for rows.Next() {
		var j JobRow
		if err := rows.Scan(&j.RoundID, &j.ShardID, &j.Seq, &j.DurationMs, &j.NeedsIndex, &j.StartedAt, &j.FinishedAt); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ---- workers ------------------------------------------------------------

type Worker struct {
	WorkerID     string    `json:"worker_id"`
	MemBytes     int64     `json:"mem_bytes"`
	LastSeen     time.Time `json:"last_seen"`
	RegisteredAt time.Time `json:"registered_at"`
}

// PoolState summarises the live workers for the policy.
type PoolState struct {
	LiveWorkers int   `json:"live_workers"`
	LargestMem  int64 `json:"largest_mem_bytes"`
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

// Pool counts workers seen within maxAge and the largest memory among them.
func (s *Store) Pool(ctx context.Context, maxAge time.Duration) (PoolState, error) {
	var p PoolState
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(MAX(mem_bytes), 0) FROM workers
		WHERE last_seen > now() - make_interval(secs => $1)`,
		maxAge.Seconds()).Scan(&p.LiveWorkers, &p.LargestMem)
	return p, err
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT worker_id, mem_bytes, last_seen, registered_at FROM workers ORDER BY worker_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.WorkerID, &w.MemBytes, &w.LastSeen, &w.RegisteredAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) guarded(ctx context.Context, sql string, args ...any) (bool, error) {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
