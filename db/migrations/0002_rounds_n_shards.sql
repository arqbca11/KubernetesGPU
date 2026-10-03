-- A round must know how many shards it expects, or it cannot tell "every
-- shard is done" from "the shards that have shown up so far are done".
ALTER TABLE rounds ADD COLUMN n_shards int NOT NULL DEFAULT 1;
ALTER TABLE rounds ALTER COLUMN n_shards DROP DEFAULT;
ALTER TABLE rounds ADD CONSTRAINT rounds_n_shards_positive CHECK (n_shards > 0);
