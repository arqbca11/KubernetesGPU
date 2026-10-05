# Shard-to-GPU index build scheduler

A scheduler that schedules HNSW vector index builds from ~50 database shards onto a small shared pool of GPU workers, with the option to build locally on the shard's CPU instead. The goal is to minimize **round completion time**: how long until every shard has built its index and drained the queries the build held up. One slow shard holds up the whole round.

Start small: the default configuration is **6 shards and 2 GPU workers**. Scaling to 50 shards is a config change, done once the small setup runs end to end.

Full roadmap, schema, policies and experiment plan: `docs/roadmap.md`. That is the plan written before building. The design as built, with diagrams and a decisions log, is one doc per phase under `docs/design/`. Significant bugs and their causes are in `docs/bugs.md`. Read the roadmap and design doc for the current phase before starting work.

## Current phase

**Phase 1: scheduler with fake jobs, Docker Compose, no Kubernetes.**
Update this line when a phase is done. Don't start work that belongs to a later phase unless asked.

## Architecture

| Component | Role | Language |
| --- | --- | --- |
| `scheduler/` | Decides local vs GPU per build, owns the queue, reaps expired leases, tracks rounds | Go |
| `shard/` | Simulates N shards (goroutines), each with a CPU that runs its local build and its arriving queries, and reports its load to the scheduler | Go |
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

- A **round** begins when every shard receives its index build (the DDL). Each shard also has a **query stream**: a seeded, finite sequence of queries arriving over time during the round, each with a CPU duration and a `needs_index` flag. A shard is done when its build is terminal, its stream is exhausted, and its query backlog has drained. The round ends when every shard is done. Round completion time is therefore the cluster's **recovery time from the DDL**: how long the slowest shard holds everyone up, measured by the work it blocked.
- **Nobody declares queries in advance.** A real shard receiving a DDL knows the build's size and nothing about the queries to come. The scheduler learns a shard's load only from what the shard **reports** periodically: queue depth, how many waiting queries need the index, oldest wait, build progress. A real database exposes exactly these. The simulator knows the future and may hand it to a *clairvoyant* oracle policy for comparison, never to a deployable one.
- **A local build occupies the shard's CPU.** Queries queue behind it; the backlog grows at the arrival rate for as long as the build runs.
- **An offloaded build frees the CPU.** Queries that don't need the index run as they arrive; those that do wait until the build completes wherever it ran. The shard's CPU runs ready queries one at a time in arrival order, skipping queries whose index isn't ready yet (they wait, they don't block others).
- **Placement can change while a build is in flight.** A local build can be **preempted**: it is killed, its CPU progress is lost, the CPU is freed, and the build joins the GPU queue with a priority that grows with the blocked work behind it. A queued GPU build can be moved back to local. A build leased to a GPU worker is never preempted (Phase 3). The shard learns of any change through the reply to its own periodic report, never by push.
- **Queries are never scheduled or placed.** They run on their shard regardless. They are in the model because they are the cost of a slow or badly placed build made measurable: without them, placement would depend only on build speed and the straggler effect would be invisible. A round with empty streams is the narrower "builds only" case.
- A **GPU worker** runs one build at a time and has a modeled memory capacity. A build whose modeled memory need exceeds every live worker's capacity must build locally.
- **Workloads are scenarios**: a seeded combination of a size distribution (uniform, Zipf, bimodal), a query stream profile per shard (arrival rate, duration, `needs_index` fraction, horizon), a DDL arrival pattern (all at once or staggered), and a pool configuration. Zipf with all-at-once arrival is the default. The scenario table is in `docs/roadmap.md` (Phase 3).

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
- Tests narrate what they do with `t.Logf` (what was done, what Postgres now holds, what was expected), so the logs read as a record of what happened. Every step keeps a saved log of its own tests and a separate one of its cross-check run under `test-logs/phase<N>/` (`scripts/test-db.sh --save ...`), regenerated when the step's code changes and committed with it. The index is `test-logs/README.md`.
- After each step that adds behavior, run the `crosscheck-tester` agent (`.claude/agents/crosscheck-tester.md`): it writes black-box tests in `scheduler/crosscheck/` from the spec without reading the implementation. Triage its findings as bug, spec gap or test error; fix bugs, turn spec gaps into design-doc rows, and record the outcome in the design doc. Its tests are committed and run with the rest.
- Before working on a bug, read `docs/bugs.md` and look for connected ones: the same mechanism elsewhere, the same class with a different trigger, or a fix that should have been wider. Say what you found. Significant bugs go in `docs/bugs.md`: symptom, cause, fix, what it would look like in production, lesson, related entries, commit. A logged bug gets its own commit, separate from tests, docs and other changes, so the fix can be read on its own; the commit message names the bug log entry and the entry links the commit. Log a bug when it changes how we think about the system, is subtle enough to be made again, or was found by a method worth repeating. The log is public.
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
| All tests, Go then Python (needs Docker; starts a throwaway Postgres) | `scripts/test-db.sh`. `--go` or `--py` for one side; explicit `go test` args (e.g. `scripts/test-db.sh -v -run Fencing ./scheduler/...`) run Go only. Go packages run one at a time (`-p 1`) because they share the database. Writes a narrated log to `test-logs/latest.log` (tracked) and `test-logs/history/` (local). `--save phase1/<name>` also keeps a tracked copy at `test-logs/phase1/<name>.log`; see `test-logs/README.md`. |
| Go unit tests only, no database | `go test ./...` (database tests skip themselves when `TEST_DATABASE_URL` is unset) |
| Python environment | `cd worker && uv sync` (creates `.venv` from `uv.lock`; `uv lock` after changing `pyproject.toml`) |
| Python unit tests only, no database | `cd worker && uv run pytest` (database tests skip without `TEST_DATABASE_URL`) |
| Run the worker locally | `cd worker && DATABASE_URL=... LOG_FORMAT=text FAKE_TIME_SCALE=10 uv run python -m kgpu_worker` (env vars in `worker/kgpu_worker/config.py`) |
| Build the worker image | `docker build -t kgpu-worker worker/` |

| Run the scheduler locally | `DATABASE_URL=postgres://postgres:test@127.0.0.1:5432/kgpu?sslmode=disable LOG_FORMAT=text go run ./scheduler/cmd/scheduler` (env vars documented at the top of `scheduler/cmd/scheduler/main.go`) |
| Build the scheduler image | `docker build -f scheduler/Dockerfile -t kgpu-scheduler .` (context is the repo root) |

The Go module is `github.com/arqbca11/KubernetesGPU`, one module at the repo root covering `db/`, `scheduler/`, `shard/` and the Go parts of `experiments/`.
