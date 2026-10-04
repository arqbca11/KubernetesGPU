-- Only shards that submitted take part in a round (design decision 33). The
-- store enforced this in Go; this makes Postgres hold the rule for every
-- caller. See docs/bugs.md entry 3.

-- (round_id, shard_id) has been one-build-per-shard-per-round by convention
-- (build_id is the string "shard_id:round_id"). Make it a key.
ALTER TABLE builds
  ADD CONSTRAINT builds_round_shard_unique UNIQUE (round_id, shard_id);

-- A load report must come from a shard with a build in that round.
ALTER TABLE shard_status
  ADD CONSTRAINT shard_status_build_fk
  FOREIGN KEY (round_id, shard_id) REFERENCES builds (round_id, shard_id);

-- A recorded query must belong to a shard with a build in that round.
ALTER TABLE shard_jobs
  ADD CONSTRAINT shard_jobs_build_fk
  FOREIGN KEY (round_id, shard_id) REFERENCES builds (round_id, shard_id);
