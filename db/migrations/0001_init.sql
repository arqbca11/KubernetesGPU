-- Phase 1 schema. See docs/design/phase1-scheduler.md.
-- Postgres is the only source of truth; every other process is stateless.

CREATE TABLE rounds (
  round_id      bigserial PRIMARY KEY,
  scenario      text NOT NULL,
  seed          bigint NOT NULL,
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz
);

CREATE TABLE builds (
  build_id      text PRIMARY KEY,              -- shard_id:round_id, the idempotency key
  round_id      bigint NOT NULL REFERENCES rounds,
  shard_id      int NOT NULL,
  n_vectors     bigint NOT NULL CHECK (n_vectors > 0),
  dim           int NOT NULL CHECK (dim > 0),
  mem_bytes     bigint NOT NULL CHECK (mem_bytes >= 0),   -- modeled GPU memory need
  placement     text NOT NULL CHECK (placement IN ('local', 'gpu')),
  state         text NOT NULL CHECK (state IN ('queued', 'leased', 'running', 'done', 'failed')),
  priority      double precision NOT NULL DEFAULT 0,
  attempt       int NOT NULL DEFAULT 0,         -- fencing token, +1 on every claim
  lease_owner   text,
  lease_until   timestamptz,
  fail_reason   text,
  enqueued_at   timestamptz NOT NULL DEFAULT now(),
  started_at    timestamptz,
  finished_at   timestamptz,
  -- 'queued' and 'leased' are GPU-only states; 'running' is local-only.
  CHECK (placement = 'gpu' OR state <> 'queued'),
  CHECK (placement = 'gpu' OR state <> 'leased'),
  CHECK (placement = 'local' OR state <> 'running'),
  CHECK (state <> 'leased' OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL))
);

-- The claim's scan: highest priority first, then oldest.
CREATE INDEX builds_queue ON builds (priority DESC, enqueued_at)
  WHERE state = 'queued' AND placement = 'gpu';

-- The reaper's scan.
CREATE INDEX builds_leases ON builds (lease_until)
  WHERE state = 'leased';

CREATE INDEX builds_round ON builds (round_id);

CREATE TABLE shard_jobs (
  round_id      bigint NOT NULL REFERENCES rounds,
  shard_id      int NOT NULL,
  seq           int NOT NULL,                  -- order within the shard's CPU queue
  duration_ms   bigint NOT NULL CHECK (duration_ms >= 0),
  needs_index   boolean NOT NULL,
  started_at    timestamptz,
  finished_at   timestamptz,
  PRIMARY KEY (round_id, shard_id, seq)
);

CREATE TABLE workers (
  worker_id     text PRIMARY KEY,
  mem_bytes     bigint NOT NULL CHECK (mem_bytes > 0), -- modeled GPU memory capacity
  last_seen     timestamptz NOT NULL,          -- heartbeat; stale rows are not counted in pool state
  registered_at timestamptz NOT NULL DEFAULT now()
);
