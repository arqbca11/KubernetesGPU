# worker

Python. Pulls GPU jobs from Postgres, renews its lease, runs the build behind `Builder.build(job) -> Artifact`, publishes the result.

- `kgpu_worker/store.py` mirrors the Go store's SQL statement for statement (design decision 13).
- `kgpu_worker/builder.py` is the one interface; `FakeBuilder` sleeps for the cost model's time.
- `kgpu_worker/worker.py` is the loop: heartbeat thread, claim, renew thread, build, complete; release on SIGTERM.
- Config is by environment variable; see `kgpu_worker/config.py`.

Run: `uv sync && DATABASE_URL=... uv run python -m kgpu_worker`. Tests: `scripts/test-db.sh --py` from the repo root.
