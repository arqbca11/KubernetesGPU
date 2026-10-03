# Shard-to-GPU index build scheduler

A scheduler that schedules HNSW vector index builds from ~50 database shards onto a small shared pool of GPU workers, with the option to build locally on the shard's CPU instead. The goal is to minimize **round completion time**: how long until every shard has finished its build and all of its follow-up jobs. One slow shard holds up the whole round.

Start small: the default configuration is **6 shards and 2 GPU workers**. Scaling to 50 shards is a config change, done once the small setup runs end to end.

Full roadmap, schema, policies and experiment plan: `docs/roadmap.md`. That is the plan written before building. The design as built, with diagrams and a decisions log, is one doc per phase under `docs/design/`. Read both for the current phase before starting work.

## Current phase

**Phase 1: scheduler with fake jobs, Docker Compose, no Kubernetes.**
Update this line when a phase is done. Don't start work that belongs to a later phase unless asked.

## Architecture

| Component | Role | Language |
| --- | --- | --- |
| `scheduler/` | Decides local vs GPU per build, owns the queue, reaps expired leases, tracks rounds | Go |
| `shard/` | Simulates N shards (goroutines), each with a sequential CPU queue for local builds and follow-up jobs | Go |
| `worker/` | Pulls GPU jobs, renews its lease, runs the build, publishes the result | Python |
| `db/migrations/` | Postgres schema; Postgres is the only source of truth | SQL |
| `experiments/` | Seeded workloads, discrete-event simulator, runner, results | Go / Python |
| `deploy/` | Docker Compose now; kind + manifests from Phase 2 | YAML |

## Invariants (never break these)

1. **All shared state lives in Postgres.** Scheduler, shards and workers keep no state that must survive a restart.
2. **Workers pull; the scheduler never pushes jobs to workers.** A worker claims with `FOR UPDATE SKIP LOCKED`.
3. **Every write to a `builds` row after the claim is a conditional `UPDATE` guarded by `build_id` and `attempt`.** No unconditional writes. Zero rows updated means the caller lost ownership and must stop.
4. **`attempt` increments on every claim.** It is the fencing token.
5. **The reaper is idempotent:** it only moves expired leases back to `queued`, and running it twice changes nothing.
6. **`build_id` is the idempotency key** (`shard_id:round_id`). Resubmitting a build is a no-op.
7. **Artifacts are stored under `build_id/attempt/`; the manifest is written last**, and completion is recorded only after it. (From Phase 4.)
8. **Placement policies are pure functions** of the state passed in: no I/O, no reading the clock, no randomness except through an injected seeded source. The simulator reuses them unchanged.
9. **The build step sits behind one interface**, `build(job) -> artifact`. Fake, hnswlib and cuVS are implementations; nothing else changes when one is added.
10. **Workloads are seeded and reproducible.**

## Modeling assumptions (all config flags)

- A **round** gives every shard one index build and a list of **follow-up jobs**. It ends when every shard has finished its build and all of its follow-up jobs.
- A **follow-up job** is any CPU work the shard does after submitting its build: an OLTP batch, a query workload, anything. Each has a CPU duration and a flag `needs_index`. The old "one OLTP job" model is the special case of a one-job list. **Follow-up jobs are never scheduled or placed.** They run on the shard regardless. They are in the model because they are the cost of a local build made measurable: without them, placement would depend only on build speed and the straggler effect would be invisible. The scheduler records them for round completion and the timeline, and (from Phase 3) reads them as policy input: a shard's projected finish is its build plus its jobs, and that projection sets its queue priority. A real shard would not declare a job list; the scheduler would estimate remaining work from its backlog. The declared list is the simulation's stand-in for that. A round with empty job lists is the narrower "builds only" case.
- A **local build** occupies the shard's CPU; no follow-up job runs until it finishes.
- An **offloaded build** frees the CPU. Follow-up jobs with `needs_index = false` run immediately; those with `needs_index = true` wait for the build to complete wherever it ran.
- Follow-up jobs on a shard run sequentially, in order. They are CPU-only and never go to the GPU pool.
- A **GPU worker** runs one build at a time and has a modeled memory capacity. A build whose modeled memory need exceeds every worker's capacity must build locally.
- **Workloads are scenarios**: a seeded combination of a size distribution (uniform, Zipf, bimodal), a follow-up profile, an arrival pattern (all at once or staggered), and a pool configuration. Zipf with all-at-once arrival is the default. The scenario table is in `docs/roadmap.md` (Phase 3).

## Conventions

- Go: standard library first; `pgx` for Postgres; `log/slog` for structured logs.
- Python: type hints, a pinned lockfile, `psycopg` for Postgres.
- Every log line about a build carries `build_id`, `attempt` and `round_id`.
- Prometheus metric names follow `docs/roadmap.md` (Phase 2, Metrics).
- Correctness tests come before features: the failure tests in the roadmap (kill -9, SIGSTOP past lease expiry, scheduler restart, duplicate submit) must pass before a phase is done.

## Working agreement

- Ask before adding a dependency or changing the schema.
- When a design question isn't answered here or in the roadmap, ask instead of picking silently.
- Keep a **Commands** section below up to date as build, test and run commands are added.
- Tests narrate what they do with `t.Logf` (what was done, what Postgres now holds, what was expected), so `test-logs/latest.log` reads as a record of what happened. Commit the refreshed log with the change that produced it.
- After each step that adds behavior, run the `crosscheck-tester` agent (`.claude/agents/crosscheck-tester.md`): it writes black-box tests in `scheduler/crosscheck/` from the spec without reading the implementation. Triage its findings as bug, spec gap or test error; fix bugs, turn spec gaps into design-doc rows, and record the outcome in the design doc. Its tests are committed and run with the rest.
- Keep the current phase's doc in `docs/design/` current: every design decision goes in its decisions table (append-only; a reversal is a new row pointing at the old one), and the implementation notes section is updated as components land. Diagrams are Mermaid.
- **Explain Docker, Kubernetes and Go concepts as you go.** The owner has school-level knowledge of these and deep database knowledge. When you introduce a concept from any of the three (an image vs a container, a Compose service, a goroutine, a Go module, a Deployment, a probe, a taint) explain it in a sentence or two in your reply the first time it comes up. This applies to every agent and subagent working in this repo.
- **Keep `docs/study-notes.md` up to date.** It is gitignored. Add a short entry for each concept you explained, grouped by topic, so the owner has one place to review. Don't repeat Postgres concepts; those are known.

## Commands

Toolchain (macOS, Homebrew): `brew install go python@3.12 uv libpq && brew install --cask docker`.
`psql` lives in `/opt/homebrew/opt/libpq/bin` (added to `~/.zshrc`).

| Task | Command |
| --- | --- |
| Check toolchain | `go version && python3.12 --version && uv --version && psql --version && docker compose version` |
| Compile and vet Go | `go build ./... && go vet ./...` |
| Go tests (needs Docker; starts a throwaway Postgres) | `scripts/test-db.sh` (default `-v ./...`; extra args replace that, e.g. `scripts/test-db.sh -v -run Fencing ./scheduler/...`). Runs packages one at a time (`-p 1`) because they share the database. Writes a narrated log to `test-logs/latest.log` (tracked) and `test-logs/history/` (local). |
| Go unit tests only, no database | `go test ./...` (database tests skip themselves when `TEST_DATABASE_URL` is unset) |

| Run the scheduler locally | `DATABASE_URL=postgres://postgres:test@127.0.0.1:5432/kgpu?sslmode=disable LOG_FORMAT=text go run ./scheduler/cmd/scheduler` (env vars documented at the top of `scheduler/cmd/scheduler/main.go`) |
| Build the scheduler image | `docker build -f scheduler/Dockerfile -t kgpu-scheduler .` (context is the repo root) |

The Go module is `github.com/arqbca11/KubernetesGPU`, one module at the repo root covering `db/`, `scheduler/`, `shard/` and the Go parts of `experiments/`.
