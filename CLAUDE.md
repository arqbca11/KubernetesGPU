# Shard-to-GPU index build scheduler

A coordinator that schedules HNSW vector index builds from ~50 database shards onto a small shared pool of GPU workers, with the option to build locally on the shard's CPU instead. The goal is to minimize **round completion time**: how long until every shard has finished its build and its OLTP job. One slow shard holds up the whole round.

Full roadmap, schema, policies and experiment plan: `docs/roadmap.md`. Read the section for the current phase before starting work.

## Current phase

**Phase 1: coordinator with fake jobs, Docker Compose, no Kubernetes.**
Update this line when a phase is done. Don't start work that belongs to a later phase unless asked.

## Architecture

| Component | Role | Language |
| --- | --- | --- |
| `coordinator/` | Decides local vs GPU per build, owns the queue, reaps expired leases, tracks rounds | Go |
| `shard/` | Simulates N shards (goroutines), each with a sequential CPU queue for local builds and OLTP jobs | Go |
| `worker/` | Pulls GPU jobs, renews its lease, runs the build, publishes the result | Python |
| `db/migrations/` | Postgres schema; Postgres is the only source of truth | SQL |
| `experiments/` | Seeded workloads, discrete-event simulator, runner, results | Go / Python |
| `deploy/` | Docker Compose now; kind + manifests from Phase 2 | YAML |

## Invariants (never break these)

1. **All shared state lives in Postgres.** Coordinator, shards and workers keep no state that must survive a restart.
2. **Workers pull; the coordinator never pushes jobs to workers.** A worker claims with `FOR UPDATE SKIP LOCKED`.
3. **Every write to a `builds` row after the claim is a conditional `UPDATE` guarded by `build_id` and `attempt`.** No unconditional writes. Zero rows updated means the caller lost ownership and must stop.
4. **`attempt` increments on every claim.** It is the fencing token.
5. **The reaper is idempotent:** it only moves expired leases back to `queued`, and running it twice changes nothing.
6. **`build_id` is the idempotency key** (`shard_id:round_id`). Resubmitting a build is a no-op.
7. **Artifacts are stored under `build_id/attempt/`; the manifest is written last**, and completion is recorded only after it. (From Phase 4.)
8. **Placement policies are pure functions** of the state passed in: no I/O, no reading the clock, no randomness except through an injected seeded source. The simulator reuses them unchanged.
9. **The build step sits behind one interface**, `build(job) -> artifact`. Fake, hnswlib and cuVS are implementations; nothing else changes when one is added.
10. **Workloads are seeded and reproducible.**

## Modeling assumptions (all config flags)

- A round gives every shard one build and one OLTP job; it ends when every shard has finished both.
- A local build occupies the shard's CPU; OLTP can't start until it finishes.
- An offloaded build frees the CPU, so OLTP runs in parallel. A per-job flag can instead make OLTP wait for the new index.
- A GPU worker runs one build at a time.
- Index sizes are Zipf-distributed across shards.

## Conventions

- Go: standard library first; `pgx` for Postgres; `log/slog` for structured logs.
- Python: type hints, a pinned lockfile, `psycopg` for Postgres.
- Every log line about a build carries `build_id`, `attempt` and `round_id`.
- Prometheus metric names follow `docs/roadmap.md` (Phase 2, Metrics).
- Correctness tests come before features: the failure tests in the roadmap (kill -9, SIGSTOP past lease expiry, coordinator restart, duplicate submit) must pass before a phase is done.

## Working agreement

- Ask before adding a dependency or changing the schema.
- When a design question isn't answered here or in the roadmap, ask instead of picking silently.
- Keep a **Commands** section below up to date as build, test and run commands are added.

## Commands

_(none yet)_
