-- Model revision (design decisions 28-32): queries arrive over the round and
-- are recorded on arrival; shards report their load; placement may change in
-- flight. See docs/design/phase1-scheduler.md.

ALTER TABLE shard_jobs ADD COLUMN arrived_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE shard_status (
  round_id             bigint NOT NULL REFERENCES rounds,
  shard_id             int NOT NULL,
  queue_depth          int NOT NULL CHECK (queue_depth >= 0),
  waiting_needs_index  int NOT NULL CHECK (waiting_needs_index >= 0 AND waiting_needs_index <= queue_depth),
  oldest_wait_ms       bigint NOT NULL CHECK (oldest_wait_ms >= 0),
  build_progress       real NOT NULL CHECK (build_progress >= 0 AND build_progress <= 1),
  stream_done          boolean NOT NULL,
  updated_at           timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (round_id, shard_id)
);
