# Phase 1: scheduler with fake jobs

## Status and scope

**In progress.** Started 2026-10-01.

Build a scheduler that stays correct under failures, using builds that only sleep. Everything runs locally under Docker Compose. Default configuration: 6 shards, 2 GPU workers. The policy is the trivial one: every build goes to the GPU queue unless it does not fit in any worker's memory.

**Model revision, 2026-10-02.** The first version of this doc had each shard declare its follow-up jobs at submit time. The owner pointed out that a real shard receiving a DDL knows nothing about the queries to come, so a policy fed that list would be using information no production scheduler has. The model is now: queries arrive over the round as a seeded stream; the shard records each one as it arrives and reports its load periodically; placement can change in flight (preemption of a local build, reneging of a queued GPU build), and the shard learns of changes in the reply to its own report. Decisions 28 onward record the revision; decision 17 is superseded.

**Done when** a worker killed mid-build still results in that build finishing exactly once, at both 6 and 50 shards, and the other three failure tests pass (SIGSTOP past lease expiry, scheduler restart mid-round, duplicate submit).

## System diagram

Four kinds of process on one Docker network. Postgres is the only stateful one. The scheduler is the control plane; shards and workers are the data plane.

```mermaid
flowchart LR
    subgraph shardsim["shard simulator (1 process)"]
        S1["shard 1<br/>CPU: build + queries<br/>query stream"]
        S2["shard 2"]
        SN["shard N"]
    end

    subgraph sched["scheduler (Go)"]
        API["HTTP API<br/>rounds, builds, queries,<br/>shard reports"]
        POL["policy v0<br/>always GPU if it fits"]
        REAP["reaper<br/>every few seconds"]
    end

    PG[("Postgres<br/>rounds, builds, shard_jobs,<br/>shard_status, workers")]

    subgraph workers["GPU workers (Python)"]
        W1["worker 1<br/>claim, renew, sleep, complete"]
        W2["worker 2"]
    end

    S1 & S2 & SN -- "submit build, record queries,<br/>report load (reply: placement),<br/>poll round" --> API
    API --> POL
    API -- "INSERT / SELECT" --> PG
    REAP -- "expired leases to queued" --> PG
    W1 & W2 -- "heartbeat, claim,<br/>renew, complete<br/>(SQL, no scheduler)" --> PG
```

Two things to notice:

- **Shards talk to the scheduler. Workers talk to Postgres.** The scheduler must see every submission, because that is where the placement decision runs. Workers bypass it because the claim has to be one atomic SQL statement, and routing it through the scheduler would add a hop and a single point of failure to the hottest path for no benefit.
- **No arrows between the scheduler and the workers.** The scheduler never contacts a worker. Everything it wants a worker to do, it expresses by changing rows.

## Flows

### A GPU build, happy path

```mermaid
sequenceDiagram
    participant S as shard k
    participant A as scheduler
    participant P as Postgres
    participant W as worker

    S->>A: POST /builds {shard k, round r, n_vectors, dim}
    A->>A: policy.decide → gpu, priority
    A->>P: INSERT builds (build_id k:r, placement gpu, state queued)
    A-->>S: {placement: gpu, estimates}
    Note over S: CPU is free. Queries arrive. Those with<br/>needs_index=false run as they come.

    loop every poll interval
        S->>A: POST /shards/r/k/report {queue_depth, waiting_needs_index, oldest_wait_ms, build_progress, stream_done}
        A->>P: UPSERT shard_status, SELECT build
        A-->>S: {build: placement, state, attempt}
    end
    S->>A: POST /jobs/r/k (each query as it arrives)

    loop until a row is returned
        W->>P: claim: UPDATE … state=leased, attempt+1 … FOR UPDATE SKIP LOCKED
    end
    P-->>W: build_id, attempt, n_vectors, dim

    par renew every ~10 s
        W->>P: UPDATE lease_until WHERE build_id AND attempt AND state=leased
    and build
        W->>W: sleep(modeled GPU time + transfer time)
    end

    W->>P: complete: UPDATE state=done WHERE build_id AND attempt
    Note over S: next report reply says state=done:<br/>queries with needs_index=true become runnable.
    S->>A: POST /jobs/r/k/{seq}/start and /done as queries run
    Note over S: stream exhausted and backlog empty:<br/>report stream_done=true, queue_depth=0
```

### A local build, and its preemption

```mermaid
sequenceDiagram
    participant S as shard k
    participant A as scheduler
    participant P as Postgres

    S->>A: POST /builds
    A->>A: policy.decide → local
    A->>P: INSERT builds (placement local, state running)
    A-->>S: {placement: local, cpu_build_ms}
    Note over S: CPU busy with the build. Queries arrive and queue.
    loop every poll interval
        S->>A: POST /shards/r/k/report {queue_depth grows, build_progress}
        A-->>S: {build: local, running}
    end
    Note over A: Phase 3 reconsider: migrating would recover sooner
    A->>P: UPDATE builds SET placement=gpu, state=queued, priority=p<br/>WHERE build_id AND placement=local AND state=running
    S->>A: POST /shards/r/k/report
    A-->>S: {build: gpu, queued}
    Note over S: abort the local build, free the CPU.<br/>Queries with needs_index=false start draining now.
    S->>A: POST /builds/k:r/done (if the shard had finished anyway)
    A-->>S: 409: not a running local build
```

In Phase 1 the policy never preempts, but the transition, the report path and the shard's reaction exist so the simulator is built right.

### Build row state machine

```mermaid
stateDiagram-v2
    [*] --> queued: submit (placement gpu)
    [*] --> running: submit (placement local)
    queued --> leased: claim (attempt + 1)
    queued --> running: renege to local (Phase 3)
    running --> queued: preempt to gpu (Phase 3)
    leased --> done: complete (guarded by build_id + attempt)
    leased --> queued: reap (lease_until < now)
    leased --> failed: worker reports error (guarded)
    running --> done: shard reports done
    done --> [*]
    failed --> [*]
```

`attempt` only changes on the `queued → leased` edge. Every edge out of `leased` is a conditional update on `build_id` and `attempt`. A worker whose update touches zero rows has lost the lease and stops. The two Phase 3 edges flip `placement` as well as `state`; a shard whose local build was preempted finds out because its `done` call is refused, or earlier from its report reply.

### Failure: worker killed mid-build

```mermaid
sequenceDiagram
    participant P as Postgres
    participant W1 as worker 1
    participant R as reaper
    participant W2 as worker 2

    W1->>P: claim → attempt 1, lease_until = t+30s
    Note over W1: kill -9 at t+12s. No more renews.
    R->>P: t+32s: UPDATE state=queued WHERE state=leased AND lease_until < now()
    W2->>P: claim → attempt 2
    W2->>P: complete WHERE build_id AND attempt=2 → 1 row
    Note over P: Build finished exactly once.
```

The SIGSTOP variant is the same picture except worker 1 comes back: its renew and complete carry `attempt = 1`, match zero rows, and it abandons the build. That is the paused-process case fencing tokens exist for.

### Shard CPU, and how the queue grows

Each shard is a goroutine with one CPU. Queries arrive from its seeded stream at rate λ. The CPU runs one thing at a time: the local build if there is one, otherwise the oldest ready query. A query is ready if it doesn't need the index, or the index is built. Drawn for one shard with a 10 s build and four queries, two of which need the index:

```mermaid
gantt
    dateFormat X
    axisFormat %s
    section local build
    build on CPU            :a1, 0, 10
    q1 (arrived 1)          :a2, after a1, 2
    q2 needs idx (arr 3)    :a3, after a2, 2
    q3 (arrived 5)          :a4, after a3, 2
    q4 needs idx (arr 7)    :a5, after a4, 2
    section offloaded build
    build on GPU (queue + build) :b1, 0, 8
    q1 (arrived 1)          :b2, 1, 2
    q3 (arrived 5)          :b3, 5, 2
    q2 needs idx (arr 3)    :b4, after b1, 2
    q4 needs idx (arr 7)    :b5, after b4, 2
```

Local: the backlog grows at λ for the whole build, then drains at (service rate − λ). Everything waits, including queries that never needed the index. Offloaded: q1 and q3 run on arrival; q2 and q4 wait for the GPU. The shard recovers at 12 instead of 18. If the GPU queue were long enough that the build finished after 14 or so, local would win. The report carries exactly what this picture needs: queue depth, how many waiting queries need the index, how long the oldest has waited, and how far the local build has got.

## Design decisions

| # | Decision | Alternatives | Why |
| --- | --- | --- | --- |
| 1 | Workers pull from a queue in Postgres | Scheduler pushes to an idle worker | Push needs a live view of every worker's state, which is exactly what was missing. A pull makes the request the idleness signal, and a dead worker's lease expires on its own. |
| 2 | Lease with a 30 s expiry plus a per-claim `attempt` counter as a fencing token | Lease only; or a distributed lock service | A lease alone cannot stop a paused worker from writing after it wakes. The counter makes every late write a no-op. Kleppmann's fencing-token argument. |
| 3 | All shared state in Postgres; every other process stateless | State in the scheduler's memory with periodic snapshots | A restart then costs nothing, and the scheduler-restart failure test is trivial. Postgres gives `SKIP LOCKED`, transactions and a partial index for the queue. |
| 4 | Shards call an HTTP API on the scheduler; workers use SQL directly | Both via API; or both via SQL | Placement must run in one place that sees every submission. The claim must be one atomic statement on the hottest path. See the note under the system diagram. |
| 5 | Local builds also get a `builds` row, in state `running` | Only GPU builds in the table | Three reasons. Round completion is computed from Postgres, so every shard's build must be there (invariant 1). The timeline and the per-placement metrics need every build's start and end in one place. Phase 3's reconsider and speculation read local builds in progress to pick one to copy to a GPU. The row is bookkeeping, not a queue entry: `running` is matched by neither the claim nor the reaper predicate, so workers and the reaper never see it. |
| 6 | A `workers` table with a heartbeat and a memory capacity | Derive pool size from leased rows | Idle workers are invisible in `builds`. Phase 3 policies need the live worker count, and it is one of the six missing pieces from the motivation. |
| 7 | Memory constraint enforced in the claim's `WHERE`, and at submit time against the largest live worker | A placement step that assigns a specific worker | Pull cannot target a GPU. Filtering in the claim is the only place the worker's own capacity is known. Submit-time check sends builds that fit nowhere to local. |
| 8 | Fake build = sleep for the cost model v0 time | Burn CPU for that long | Sleeping lets 50 shards and several workers run on one laptop. Phase 4 switches to real work. |
| 9 | The shard simulator drives rounds: it starts a round, submits every build, runs follow-up jobs, polls for completion | A separate experiment runner drives rounds | One fewer process in Phase 1. Phase 3 adds the runner and the simulator becomes a client of it. |
| 10 | Default 6 shards and 2 workers; 50 and 3 by config | Start at 50 | Small enough to read the full timeline by eye and debug. Nothing in the design depends on N. |
| 11 | Migrations are plain SQL files embedded in the Go binary and applied in filename order on process start, under an advisory lock, with applied versions recorded in `schema_migrations` | A migration library; a separate migrate command | One table set, few migrations, no dependency. Embedding means the scheduler image carries its own schema. The lock makes two replicas starting together safe. |
| 12 | One Go module at the repo root | One module per component | `scheduler/`, `shard/` and the simulator share the store and policy packages. Separate modules would need replace directives for no gain. |
| 13 | The worker's SQL (claim, renew, complete, fail, release, heartbeat) lives twice: in `scheduler/store` for Go and verbatim in the Python worker | One SQL file loaded by both | pgx and psycopg use different placeholder syntax (`$1` vs `%s`). The Go integration test is the reference; the Python copy must match it statement for statement, and a comment in each file points at the other. |
| 14 | State and placement rules are `CHECK` constraints in the schema, not only application logic | Trust the application | A row can never be `queued` with `placement = 'local'`, or `leased` without a lease. Bugs in any client fail loudly at the database. |
| 15 | `Release` (guarded move from `leased` back to `queued`) is in the store from day one | Add in Phase 2 with SIGTERM handling | It is one more guarded update and is tested alongside the others; Phase 2 only wires it to a signal. |
| 16 | `rounds.n_shards`: a round declares how many shards it expects (migration 0002) | The client closes the round explicitly | The scheduler can then tell "every shard is done" from "the shards seen so far are done", which matters once arrivals are staggered (Phase 3). Completion logic stays in Postgres. Chosen by the owner on 2026-10-02. |
| 17 | ~~A shard declares its follow-up jobs in the same submit call as the build~~ **Superseded by 28.** | | Declared future work is information a real shard does not have. |
| 18 | Round completion is computed by one idempotent `UPDATE` run on every reaper tick and before every `GET /rounds/{id}`; `finished_at` is the latest build or job finish, not the time of the check | Stamp the round in the handler that records the last completion | GPU completions happen over SQL from workers, so no handler sees them. Using the latest child finish makes the round duration independent of the tick interval. |
| 19 | With zero live workers, policy v0 still places on the GPU queue | Fall back to local when the pool is empty | "Nothing fits" is a statement about workers we can see. An empty pool may be cold-starting (Phase 5), and Compose start order should not turn a whole round local. The memory rule applies only when at least one worker is live. |
| 20 | Cost model v0 lives in `scheduler/costmodel` as pure functions of `n_vectors` and `dim`, with every constant overridable by env; the submit response returns the CPU and GPU estimates | Hard-code sleep times in the worker and shard | One source for the numbers the scheduler, shard and worker all need. The shard sleeps for `cpu_build_ms` on a local build; the Python worker reimplements the same formulas and must stay in step. |
| 21 | Standard-library HTTP only: `net/http` with Go 1.22 method-and-pattern routing, `encoding/json`, `log/slog` | A router or web framework | Eight routes do not justify a dependency. `DisallowUnknownFields` on request bodies catches client typos early. |
| 22 | Scheduler image is a two-stage build onto `distroless/static`, running as non-root | Alpine or Debian runtime image | The binary is static; distroless has no shell or package manager, so the attack surface and image size are minimal. Health checks therefore go through the HTTP endpoint, not a shell command. |
| 23 | A shard learns that its GPU build is done by polling `GET /builds/{id}` at a configurable interval, only while it has a `needs_index` job waiting and nothing else to run | Worker notifies the shard; scheduler pushes to the shard; long polling backed by `LISTEN/NOTIFY` | Workers talk only to Postgres, and the scheduler learns of GPU completions only from Postgres, so the shard asking is the only path that adds no new state or address book. The shard is blocked anyway, so polling costs it nothing. Added latency is at most one interval per shard and is recorded in the timeline so it is visible. OpenSearch's data nodes poll their remote build service the same way. Long polling is the upgrade if Phase 3 shows the artifact matters; the endpoint is shaped so the shard logic does not change. Chosen by the owner on 2026-10-02. |
| 24 | `started_at` on a build means "when the current attempt started"; reap and release clear it along with the lease columns, and `enqueued_at` keeps the submit time | Keep the first attempt's start | The timeline wants the winning attempt's duration, and work lost to abandoned attempts is `finished_at - enqueued_at` minus the last attempt. One column cannot hold both; this reading is simpler. (Cross-check gap 1.) |
| 25 | `Renew` has no `lease_until > now()` check: a worker that wakes after expiry but before the reaper acts may renew and continue | Reject renew once the deadline has passed | No one else owns the row until the reaper or a claimer acts, so continuing is safe and keeps the work. The lease is lost exactly when `attempt` stops matching. (Cross-check gap 2.) |
| 26 | "Fits" means `mem_bytes <= capacity`; a heartbeat with a new `mem_bytes` updates the worker's capacity; two simultaneous submits of one `build_id` yield exactly one `created:true` because the insert is `ON CONFLICT DO NOTHING` | | Stated so the spec says it. (Cross-check gaps 6, 8, 9.) |
| 27 | Submit validation: the round must exist (404); a new build is refused with 409 when the round already holds `n_shards` builds or has finished, enforced inside `SubmitBuild` under `SELECT … FOR UPDATE` on the round row; `n_shards` must be positive (400); a lease duration must be positive (store returns an error). `shard_id` is any non-negative integer. | Require `shard_id` in `0..n_shards-1` (the first version of this row) | Completion counts builds against `n_shards`, so the cap is what prevents an early finish. A dense id range was tried first and rejected the same day: both the implementer's and the cross-checker's tests had independently numbered shards 1-based or sparsely, which showed the range rule encoded an assumption the spec never made. The row lock serialises submits per round so the cap holds under concurrency. (Cross-check gaps 4, 8.) |
| 28 | **Queries are an arriving stream, not a declared list.** The shard records each query when it arrives (`POST /jobs/{round}/{shard}`), the submit call carries no jobs, and the policy sees only the shard's reports | Declare jobs at submit (decision 17) | A shard receiving a DDL knows the build's size and nothing about the queries to come. A policy fed the future would be an oracle, not a scheduler. The simulator still knows the future and gives it only to the clairvoyant policy, to measure what not knowing costs. Owner's call, 2026-10-02. |
| 29 | **The shard's periodic report is its sync point.** `POST /shards/{round}/{shard}/report` upserts `shard_status` and replies with the build's current placement, state and attempt. The shard acts on the reply: abort a preempted local build, start a build moved to local, run `needs_index` queries once the build is done | Separate polling (decision 23) plus a push channel for placement changes | One call, one direction, no push, no address book. The scheduler learns the shard's load and the shard learns the scheduler's decision in the same round trip. Decision 23's `GET /builds/{id}` remains for debugging and tests. |
| 30 | **Finite query stream per round.** The generator produces a seeded, finite sequence per shard; the shard reports `stream_done` when exhausted. Round completion = `n_shards` shards with `stream_done`, all their queries finished, all builds terminal | Continuous stream with a "recovered to steady state" criterion | Deterministic and simple to end. Realism beyond this needs real load data, which does not exist yet; the point is to have thought through how the queue grows, not to model production traffic. Owner's call, 2026-10-02. |
| 31 | **In-flight placement changes are two more guarded transitions.** Preempt: `running` → `queued` with `placement` flipped to `gpu`, `started_at` cleared, `priority` set, guarded by `placement = 'local' AND state = 'running'`. Renege: `queued` → `running` with `placement` flipped to `local` and `started_at` set to now, guarded by `placement = 'gpu' AND state = 'queued'`. A `leased` build is never moved. `attempt` and `enqueued_at` are untouched by both: the first claim bumps `attempt`, and `enqueued_at` keeps the submit time for the timeline (decision 24), so a preempted build sorts among equal priorities by its original submit time | Cancel and resubmit under a new id | Same row, same idempotency key, same timeline. The guards make a change race-free against a concurrent claim or completion, and a shard that didn't notice a preemption is refused at `done` by the existing guard. Phase 1 ships the transitions and tests them; Phase 3 ships the policy that uses them. |
| 32 | **Queue semantics on the shard:** one CPU, runs the local build if any, else the oldest ready query; a query is ready if it doesn't need the index or the index is built; queries that aren't ready wait without blocking others | Strict FIFO where a `needs_index` query at the head blocks everything behind it | Sessions in a real database are independent; a blocked one doesn't block the rest. This also makes "freeing the CPU" worth something even when some queries need the index. |
| 33 | **Only shards that submitted take part in a round.** A report or an arrival from a shard with no build in the round is refused (`ErrNoBuild`, 404 over HTTP); `FinishCompleteRounds` counts `stream_done` only for shards that have a build; `stream_done` is sticky (a later report cannot withdraw it); an arrival after `stream_done` is refused (`ErrStreamDone`, 409) | Accept and ignore; or a foreign key from `shard_status` and `shard_jobs` to `builds` | Found by the cross-check: a `stream_done` row from a shard with no build could complete a round while a real shard was still streaming. Refusing at the store makes the rule hold for every caller, not only HTTP. Migration 0004 (owner's yes, 2026-10-03) goes further: `UNIQUE (round_id, shard_id)` on `builds` and foreign keys from `shard_status` and `shard_jobs` to it, so the rule is Postgres's, and the store maps SQLSTATE 23503 to `ErrNoBuild`. |
| 34 | **Worker: one Python package, `worker/kgpu_worker`, with `store.py` (SQL), `builder.py` (the interface), `worker.py` (the loop), `config.py` (env), run as `python -m kgpu_worker`** | A single script | The build interface must be a seam from day one (invariant 9); Phase 4 adds `hnswlib.py`, Phase 5 `cuvs.py`, and nothing else moves. |
| 35 | **Three threads, one connection each: main (claim, build, complete), renewer (per build), heartbeat.** All run with autocommit, one statement per call | asyncio; a shared connection | The build blocks for seconds to minutes, so the renewer must be a separate thread anyway. One connection per thread avoids sharing a psycopg connection across threads. Autocommit matches the Go store: every guarded update is its own transaction. |
| 36 | **Lease lost means cancel.** The renewer's rejected renew sets `lost` and `cancel`; the fake builder checks `cancel` every 100 ms and raises; the main loop then neither completes nor releases, it only logs `lost ownership` | Keep building and let `complete` fail at the end | Stops wasting the GPU the moment ownership is gone, and makes the SIGSTOP test observable in the worker's own log, not only in Postgres. |
| 37 | **SIGTERM: stop claiming; finish the current build if its remaining time (from reported progress) is within `SHUTDOWN_FINISH_BUDGET_SECONDS`, else cancel and `release` it** | Always finish; always release | A nearly done build is worth the few seconds. A long one is handed back now so another worker starts it before the lease would have expired. The budget is config so Phase 2 can tie it to `terminationGracePeriodSeconds`. |
| 38 | **`FAKE_TIME_SCALE` divides every fake build time** | Separate tiny cost constants for tests | Tests and demos run at 10x to 50x against the same cost model the scheduler uses, so estimates and sleeps stay consistent; `estimate_s` is scaled the same way, so the shutdown budget decision stays right. |
| 39 | **The worker validates every configuration value before connecting and exits with status 2 and one line naming the variable.** Includes `RENEW_INTERVAL_SECONDS < LEASE_SECONDS` and positive `COST_*` values | Fail at first use | A worker must never claim a build it cannot handle. `COST_BANDWIDTH=0` once got as far as a claim and then crashed with the lease held (cross-check, bug log 5). |
| 40 | **`FAKE_FAIL_BUILD_IDS`: the fake builder fails the listed builds at 50%** | No way to fail a fake build | The `fail` path, and later the Phase 4 question of a failed build and a waiting `needs_index` query, need a documented trigger at the process level. |
| 41 | **After the claim, every exception that is not a cancellation ends in `fail` with `TypeName: message` as the reason, and the worker keeps claiming.** This covers the builder, the estimate, and the worker's own code. If even `fail` cannot be written, the worker logs it and leaves the lease to the reaper | Let the process crash | A crash leaves the row `leased`; the reaper requeues it, the next worker claims it and crashes too: a poison pill looping through the pool. Bug log 5. |
| 42 | **Retry budget in the reaper.** When a lease expires on a build whose `attempt` has reached `MAX_ATTEMPTS` (scheduler env, default 5), the reaper marks it `failed` with the reason recorded instead of requeueing it. `0` disables | Requeue forever; cap in the worker | A build that kills every worker that claims it, by exception, by the kernel's out-of-memory killer, by a GPU fault, would otherwise cycle through the pool indefinitely (bug log 5). The reaper is the one place that sees every expiry, and it already runs on every scheduler. Decision 36's exception path handles the half it can see; this handles the rest. A legitimate transient outage that costs several leases in a row would also park the build, which is the right trade: the owner sees a `failed` row with a clear reason instead of a silent loop. |
| 43 | **One Compose file, `deploy/compose.yaml`, project name `kgpu`, every knob an environment variable with a default** (`${LEASE_SECONDS:-30}` style) | One file per environment; hard-coded values | The same file runs the slow realistic defaults and the fast failure-test settings; `scripts/compose-failures.sh` just exports variables. Phase 2's manifests will carry the same variable names. |
| 44 | **The scheduler image health-checks itself:** `scheduler healthcheck` GETs its own `/healthz`; Compose runs that as the container health probe | `curl` in the image; no health check | The image is distroless, with no shell or curl. A subcommand on the binary is the standard answer and is what the Kubernetes probe will call too. |
| 45 | **Start order by health, not by start:** workers `depends_on` the scheduler being *healthy*, the scheduler `depends_on` Postgres being *healthy* | Plain `depends_on`; retry loops everywhere | The scheduler applies the schema on start; a worker that starts before the tables exist would fail its first heartbeat. `condition: service_healthy` encodes "the schema exists" as "the scheduler answered `/healthz`". Phase 2 replaces this with readiness probes. |
| 46 | **Workers are one service with `replicas: 2`; `WORKER_ID` defaults to the container hostname** (the short container id) | Two named worker services; a fixed id per replica | `--scale worker=3` is one flag, which step 7 needs. The id is opaque but unique, and `docker ps --filter id=` maps it back to a container name, which the failure script does to pick its victim. |
| 47 | **`stop_grace_period: 15s` on workers; `restart: unless-stopped` and `init: true` on scheduler and workers; Postgres on a named volume** | Defaults (10 s grace, no restart, no init, anonymous volume) | The grace period gives decision 37 room to finish or release. The restart policy brings a *crashed* worker back, as Kubernetes will. `init: true` puts tini at PID 1 so SIGTERM is forwarded and, for the crash test, so the worker process is not PID 1: `docker kill` counts as a manual stop (no restart), and the kernel ignores SIGKILL sent to a namespace's init from inside, so the only faithful crash is killing the worker's own pid from inside a container that has an init. The named volume survives `down` without `-v`, so the Phase 2 Postgres-restart test has something to restart. |
| 48 | **The worker survives lost database connections.** Idle: reconnect with backoff and continue. Holding a lease: `complete`, `fail` and `release` retry on connection errors until the lease deadline (last successful claim or renew plus `LEASE_SECONDS`), then stop touching the row and log it as lost. A renew that *errors* keeps retrying until that deadline, then the lease is assumed lost and the build cancelled; a renew that is *rejected* (zero rows) is a lost lease at once (decision 36). Heartbeat reconnects | Crash and rely on the restart policy (what happened); retry forever | Bug log 7. An error is not a rejection: after a rejection someone else owns the build; after an error you don't know, and the lease deadline is the only honest bound on "don't know". Crashing cost a lease and a restart per blip and, under Compose, could leave the pool short for good. |
| 49 | **A worker deregisters on a clean exit** (`DELETE FROM workers`), so pool state stops counting it at once instead of after `WORKER_STALE_AFTER` | Rely on staleness | Scale-down and `docker stop` otherwise overcount the pool for up to the stale window; the policy would place on capacity that has left. A crash still relies on staleness, which is what the window is for. |
| 50 | **Operational clarifications from the Compose cross-check:** `SHUTDOWN_FINISH_BUDGET_SECONDS` must be comfortably under the container's `stop_grace_period` (Compose: 5 s against 15 s), or the finish branch gets SIGKILLed; a clean release is counted as `builds_released`, never as lost; `WORKER_ID` names the worker *slot* (a restarted container reuses its hostname), so `lease_owner` cannot distinguish a dead process from its replacement, which is fine for fencing (attempts do that) and for pool state; a reap happens within one `REAP_INTERVAL` of the lease deadline | | Stated so the spec says it. |
| 51 | **The workload is a contract, generated by someone who has not seen the scheduler or shard code.** `experiments/workload` defines the types (`Workload`, `ShardWorkload`, `Query` with `Kind` and `ClusterQueryID`, `Scenario` with its four knobs) and `Validate`; the generator (`Generate`, presets, its own `DESIGN.md`) is written by a separate agent from the roadmap and the modeling assumptions alone | One author for everything | The workload is the one component that could bias the Phase 3 comparison if it were tuned, even unconsciously, to a policy. Owner's call, 2026-10-05. The same `Workload` type feeds the Phase 3 simulator and the clairvoyant oracle. |
| 52 | **Shard-level queries are correlated through cluster-level queries.** A `Query` carries a `Kind` (`fanout`: one piece of a coordinator query split across several shards, all pieces sharing `ClusterQueryID` and `Arrival`; `single`: routed to one shard by key; `local_ddl`: one shard's own maintenance) | Independent per-shard streams | In a distributed database most shard work is a piece of a larger query, and a fan-out query is complete only when its slowest piece is: the straggler effect at the query level. The timeline reports cluster-query latency as the slowest piece's. Owner's guidance, 2026-10-05. |
| 53 | **One shard is three goroutines sharing one mutex-guarded state:** arrivals (replays the stream, records each arrival with the scheduler *before* making it runnable), CPU (one thing at a time: the local build if the scheduler wants one, else the oldest ready query; a query is ready if it does not need the index or the index is built), reports (every poll interval, sends the load report and acts on the reply) | One goroutine with a select loop; a channel per concern | Three concerns with three different clocks (arrival schedule, CPU work, report cadence) read naturally as three loops over one small state. Recording before enqueueing was a bug the integration test caught: the CPU goroutine started a query the scheduler had not heard of (404). |
| 54 | **The shard's reaction to the report reply:** `done` → index ready, `needs_index` queries become runnable; `failed` → logged as an error, those queries stay stuck (open question); `gpu`/`queued` or `leased` while building locally → abort the local build at the next slice (preempt); `local`/`running` while not building → start the local build (renege, or the initial decision). A local build that finishes but is refused at `done` (409) was preempted in the gap and waits for the GPU | Poll `GET /builds/{id}` separately | The report reply is the one sync point (decision 29). The 409 case closes the race between the last slice and the done call. |
| 55 | **Everything modeled is divided by one `TIME_SCALE`:** arrival offsets, query durations, the local build time (from the scheduler's `cpu_build_ms`). It is the same number as the worker's `FAKE_TIME_SCALE`, set once for the whole stack | Separate knobs | A round at 10x is a faithful compression of a round at 1x only if every duration shrinks together. |
| 56 | **The timeline is built client-side** from the scheduler's round status (timestamps), the workload (what each query was: kind, cluster id, needs_index) and the shard's own events (submit decision, local build start/abort/done, preemption noticed, index ready). Reported: per-shard build and queries, shard finish, straggler lag (last finish minus median), cluster-query latency p50/p99 by kind | Store kind and cluster id in `shard_jobs` | The scheduler does not need to know what a query is for; the shard does. Keeping the schema free of workload concepts keeps the scheduler generic. |
| 57 | **The simulator validates its configuration before touching the scheduler** (positive shard count, poll interval, time scale and round timeout; non-negative rounds, gap and minimum workers; a known scenario) and exits 2 naming the variable; on SIGTERM mid-round it logs that the round is left open and exits 0 | Fail at first use | A zero poll interval was accepted and produced a report storm until the round timed out (cross-check). An abandoned round has no state of its own; the log line is the record. |
| 58 | **`NeedsIndex` stays independent of fan-out** in the generator. A later preset may couple them (every fan-out needs the index, the unfiltered-vector-search regime) via a new contract field | Tie the flag to fan-out now | The roadmap sweeps the needs-index fraction and the fan-out mix separately; coupling them would make a result impossible to attribute. A tenant-filtered vector search is a real single-shard index query, so the independent model is simpler, not wrong. Owner's call, 2026-10-05 (generator open question 1). |
| 59 | **Staggered DDL: pre-DDL queries are real load and are backfilled.** A shard runs queries that arrive before its DDL on its CPU as normal, keeps their arrival, start and finish times on its own clock, and sends them to the scheduler once its build exists. Phase 1 drops them (step 5); Phase 3 implements the backfill on top of decision 60 | Per-shard measurement window starting at the DDL | A shard that was already busy when its DDL arrived starts its build with a backlog, which is exactly the effect the staggered scenario exists to show; a per-shard window loses it and gives fan-out queries straddling two windows a partial latency. Owner's call, 2026-10-05 (generator open question 4). |
| 60 | **Query records are batched and client-timestamped.** Each shard keeps every query's arrival, start and finish on its own clock and sends them to the scheduler once per poll interval, in one call alongside its load report; the scheduler writes the batch in one transaction. Build timestamps stay server-assigned (workers write them through SQL). Replaces the three HTTP calls per query from step 5 | Keep per-event calls; scale Postgres (read replicas, partitioning, a separate telemetry store) | Two reasons and one principle. Capacity: at 50 shards a round is 30k to 40k shard-level queries, so per-event calls at a 10x time scale would be tens of thousands of writes per second against one scheduler and one Postgres; batching makes it about 100 calls per second. Fidelity: the bracketing calls inflated each query's recorded run time by 1.6 to 1.8x at a 20x scale (step 5 cross-check); with client timestamps the CPU loop sleeps exactly the scaled duration. **The principle (owner, 2026-10-05): the scheduler's Postgres is a control-plane store and must stay simple and unscaled. It holds decisions and their inputs at control-plane rates: builds, leases, rounds, per-shard load summaries. Shard telemetry exists to inform GPU placement, so it is aggregated at the shard and reported at the report cadence, never streamed per event. If the information the policy needs cannot be carried that way, the design is wrong, not the database too small.** Cost: query timestamps now depend on shard clocks (zero skew on one machine, NTP-level on Kubernetes). Implemented in step 7. |
| 61 | **The shard's client retries transient failures** (connection errors, 5xx) with backoff for up to `RetryFor` (60 s); a 409 on a job start or done after a retry counts as recorded, since the first attempt landed before its reply was lost | Fail the shard on the first error | Every shard call is idempotent on the scheduler side (`build_id`, `seq` with `ON CONFLICT`, guarded updates, upserts), so retrying is safe, and a scheduler restart mid-round (roadmap failure test 3) is then invisible to the round: seven retried calls, all succeeded, no shard failed. The Phase 2 "retries with backoff on every call" item, done early for the shard side. |

## Implementation notes

### Step 1: schema and the four operations (done 2026-10-02)

| Path | What |
| --- | --- |
| `db/migrations/0001_init.sql` | The four tables plus `CHECK` constraints, the queue index (partial, on `priority DESC, enqueued_at`) and the reaper index (partial, on `lease_until`). This file is now the schema reference; the roadmap's block is the original plan. |
| `db/migrate.go` | `db.Migrate(ctx, pool)`: embeds `migrations/*.sql`, applies unapplied files in order inside transactions, records them in `schema_migrations`, holds advisory lock `0x4B475055` for the run. |
| `scheduler/store/store.go` | `Store` with `CreateRound`, `SubmitBuild`, `Claim`, `Renew`, `Complete`, `Fail`, `Release`, `CompleteLocal`, `Reap`, `Heartbeat`, `LargestLiveWorkerMem`. Every post-claim write is guarded by `build_id AND attempt AND state = 'leased'` and returns a bool: false means the caller lost ownership. |
| `scheduler/store/store_test.go` | Integration tests, skipped without `TEST_DATABASE_URL`. |
| `scripts/test-db.sh` | Starts `postgres:17` in a container on a random port, runs `go test`, removes the container. Writes `test-logs/latest.log` (tracked, so the last run is visible on GitHub) and a timestamped copy in `test-logs/history/` (gitignored). The log has a header (date, commit, versions), the narrated test output, and the container's Docker events. |

What the tests prove, and which invariant each covers:

| Test | Shows |
| --- | --- |
| `ClaimOrdersByPriorityThenAge` | Queue order is priority desc, then enqueue time; attempt is 1 on first claim; empty queue returns nothing. |
| `ConcurrentClaimsNeverShareABuild` | 8 goroutines draining 20 builds: each claimed exactly once (`SKIP LOCKED`, invariant 2). |
| `FencingAfterReap` | Live lease is not reaped; expired lease is reaped once and a second reap changes nothing (invariant 5); the re-claim gets attempt 2 (invariant 4); the old owner's renew, complete and fail all match zero rows and leave the row untouched (invariant 3); a done row is never reaped. This is the SIGSTOP/SIGCONT scenario at the SQL level. |
| `ReleaseReturnsToQueueAndFences` | Release puts the build back; the next claim bumps attempt; the releasing owner cannot complete afterwards. |
| `ClaimRespectsWorkerMemory` | A small worker skips a high-priority build that does not fit and takes the next one; a big worker takes it (decision 7). |
| `DuplicateSubmitIsNoop` | Resubmitting with different priority and placement changes nothing, even mid-lease (invariant 6). |
| `LocalBuildsAreNeverReaped` | Local builds start `running`, cannot be claimed, are not reaped, and complete once (decision 5). |
| `HeartbeatAndLargestLiveWorker` | Upsert heartbeat; stale workers drop out of pool state. |

Lease length (30 s) and renew interval (10 s) are parameters of the calls, not constants in the store; the scheduler and worker configs will own them.

### Step 2: scheduler binary (done 2026-10-02)

| Path | What |
| --- | --- |
| `db/migrations/0002_rounds_n_shards.sql` | Adds `rounds.n_shards` (decision 16). |
| `scheduler/costmodel/` | Cost model v0 (decision 20). Defaults: 100k x 128 is about 20 s on CPU, 4 s on GPU plus 0.6 s transfer, 100 MiB of GPU memory. |
| `scheduler/policy/` | `Policy` interface and `AlwaysGPU` (decision 19). Pure; unit-tested without a database. |
| `scheduler/store/` | Added rounds, follow-up jobs, pool state, `FinishCompleteRounds`, `Reap` now reports which builds it touched (for the log). |
| `scheduler/api/` | The HTTP surface, table below. |
| `scheduler/reaper/` | `Run` ticks `Tick`: reap expired leases (one warning log line per build, with `build_id`, `attempt`, `round_id`, `lease_owner`; an error line and `failed` state when the retry budget is exhausted, decision 42), then finish complete rounds. |
| `scheduler/cmd/scheduler/` | `main`: env config, connect to Postgres with backoff retry, migrate, start reaper, serve HTTP, graceful shutdown on SIGTERM. |
| `scheduler/Dockerfile`, `.dockerignore` | Image build (decision 22). |

The API as built (revised 2026-10-02 for the query-stream model):

| Method | Path | Caller | Effect |
| --- | --- | --- | --- |
| POST | `/rounds` | shard simulator | `{scenario, seed, n_shards}` → 201 `{round_id}` |
| GET | `/rounds/{id}` | shard simulator | Finishes complete rounds, then returns the round, every build, every query and every shard status |
| POST | `/builds` | shard | `{round_id, shard_id, n_vectors, dim}` → 201 with `{build_id, placement, priority, reason, mem_bytes, cpu_build_ms, gpu_total_ms, created:true}`. A repeat returns 200 with `created:false`; every field but `reason` comes from the stored row. 404 for an unknown round; 409 for a new build when the round already holds `n_shards` builds or has finished. |
| GET | `/builds/{id}` | shard, tests | The build row |
| POST | `/builds/{id}/done` | shard | Local build finished. 409 unless the build is a running local build (so a preempted shard finds out here at the latest). |
| POST | `/jobs/{round}/{shard}` | shard | `{seq, duration_ms, needs_index}`: a query arrived → 201. Repeat of the same `seq` → 200, unchanged. 400 for negative fields; 404 if the shard has no build in the round; 409 if the shard already reported `stream_done`. |
| POST | `/jobs/{round}/{shard}/{seq}/start`, `/done` | shard | Record when a query ran → 200. 404 if no such query, 409 if wrong state. |
| POST | `/shards/{round}/{shard}/report` | shard | `{queue_depth, waiting_needs_index, oldest_wait_ms, build_progress, stream_done}` → 200 `{build: {placement, state, attempt}}`. Upserts `shard_status`; `stream_done` is sticky. 400 for out-of-range fields (`waiting_needs_index > queue_depth`, progress outside 0..1); 404 if the shard has no build in the round (the same 404 covers an unknown round). |
| GET | `/workers` | anyone | Live pool state and every registered worker |
| GET | `/healthz` | Compose, Kubernetes | 200 when Postgres answers |

Tests added: `costmodel` (range and monotonicity), `policy` (the four placement cases and determinism), `store` (`RoundFinishesOnlyWhenEveryShardIsDone`, which walks a two-shard round through every partial state and checks the round is stamped only at the end, with `finished_at` equal to the last job's finish), `api` (`EndToEndRoundOverHTTP`: a two-shard round over HTTP with a GPU build claimed and completed through the store, a local build forced by memory, duplicate submit, job events, 409s, and round completion; `ValidationAndNotFound`).

### Model revision applied to steps 1 and 2 (2026-10-02, later the same day)

| Path | What |
| --- | --- |
| `db/migrations/0003_query_stream.sql` | `shard_jobs.arrived_at`; new `shard_status` table (decisions 28 to 30). |
| `db/migrations/0004_shard_fks.sql` | `UNIQUE (round_id, shard_id)` on `builds`; foreign keys from `shard_status` and `shard_jobs` to it (decision 33). The store's `ReportStatus` and `RecordArrival` now rely on the constraint and translate the violation to `ErrNoBuild`. Test `SchemaRefusesOrphanShardRows` inserts orphans straight at Postgres and checks the refusal. |
| `scheduler/store/` | `SubmitBuild` no longer takes jobs. New: `RecordArrival`, `ReportStatus`, `ListShardStatus`, `PreemptToGPU`, `RenegeToLocal`. `FinishCompleteRounds` now also requires `n_shards` shards with `stream_done`. |
| `scheduler/api/` | Submit carries no jobs. New: `POST /jobs/{round}/{shard}` (arrival), `POST /shards/{round}/{shard}/report`, `GET /builds/{id}`. `GET /rounds/{id}` includes shard statuses. |
| `scheduler/policy/` | `BuildSpec` has no job list. |

Tests revised or added: store `RoundFinishesOnlyWhenEveryShardIsDone` (now walks arrivals, reports and the `stream_done` gate) and `PreemptAndRenegeTransitions` (decision 31, including that a leased build is never moved and a preempted shard's `done` is refused); api `EndToEndRoundOverHTTP` (arrivals, reports, the reply carrying the build's state, the round staying open until `stream_done`) and `ReportReflectsPreemption` (the shard learns of a preemption from its report reply). The old-spec cross-check tests were removed and the cross-check rerun against the revised spec; see below.

### Step 3: Python worker (done 2026-10-04)

| Path | What |
| --- | --- |
| `worker/pyproject.toml`, `worker/uv.lock` | Project and pinned lockfile. Runtime dependency: `psycopg[binary]`. Dev: `pytest`. |
| `worker/kgpu_worker/store.py` | The worker's SQL: heartbeat, claim, renew, complete, fail, release. Each statement mirrors `scheduler/store/store.go` (decision 13); `claim` returns `round_id` too, so every log line can carry it. |
| `worker/kgpu_worker/costmodel.py` | Cost model v0 mirror; `test_costmodel.py` pins the Go test's exact numbers. |
| `worker/kgpu_worker/builder.py` | `Builder` protocol: `build(job, progress, cancel) -> Artifact`, `estimate_s(job)`. `FakeBuilder` sleeps for `gpu_total_s / FAKE_TIME_SCALE` in 100 ms slices, reporting progress and honouring `cancel`. |
| `worker/kgpu_worker/worker.py` | The loop (decisions 35 to 37). |
| `worker/kgpu_worker/config.py` | Environment variables, table below. |
| `worker/kgpu_worker/logging_setup.py` | JSON lines (default) or text; every build line carries `build_id`, `attempt`, `round_id`. |
| `worker/Dockerfile` | `python:3.12-slim`, `uv` copied from its image, two-layer `uv sync`, non-root, exec-form entrypoint so SIGTERM reaches Python. About 360 MB. |
| `worker/tests/` | `conftest.py` applies `db/migrations/*.sql` the same way the Go runner does. `test_store.py`: claim order, the full fencing walk from the Python side, memory filter, release, heartbeat. `test_worker.py`: a worker completes builds; a worker whose lease is reaped and re-claimed mid-build abandons without completing; SIGTERM releases a long build and finishes a short one; two workers drain twelve builds with every attempt equal to 1. |
| `scripts/test-db.sh` | Now runs Go then Python; `--go`, `--py`. |

Worker configuration:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres |
| `WORKER_ID` | hostname | Appears in `workers` and `lease_owner` |
| `WORKER_MEM_BYTES` | 8 GiB | Modeled capacity; the claim filters `mem_bytes <=` this |
| `LEASE_SECONDS` | 30 | Lease length on claim and renew |
| `RENEW_INTERVAL_SECONDS` | 10 | Renewer period |
| `HEARTBEAT_INTERVAL_SECONDS` | 5 | Heartbeat period |
| `POLL_INTERVAL_SECONDS` | 0.5 | Wait when the queue is empty |
| `BUILDER` | `fake` | Builder implementation |
| `FAKE_TIME_SCALE` | 1 | Divide fake build times (decision 38) |
| `SHUTDOWN_FINISH_BUDGET_SECONDS` | 5 | Decision 37; compared with remaining time in scaled seconds (after `FAKE_TIME_SCALE`) |
| `FAKE_FAIL_BUILD_IDS` | none | Comma-separated build ids the fake builder fails at 50% (decision 40) |
| `LOG_FORMAT` | `json` | or `text` |
| `COST_*` | | Same overrides as the scheduler |

Behaviour the worker promises, in the order the loop does it: validate config (decision 39); connect; a synchronous heartbeat, so the worker is in `workers` before it can hold a lease, then heartbeats every interval; claim with its own `WORKER_ID` and `WORKER_MEM_BYTES`; renew every interval for exactly the claimed `attempt`; on a rejected renew, cancel the build and write nothing more to that row, then keep claiming; on any exception after the claim, `fail` with `TypeName: message` as the reason and keep claiming (decision 41); on success, `complete`; on SIGTERM, decision 37, whether the signal arrives mid-build, between the claim and the build, or while idle (then exit within one poll interval); exit code 0 after SIGTERM, promptly after a release, after the build when finishing it. Logs go to stdout; a crash traceback, which should never happen after decisions 39 and 41, would go to stderr.

Log vocabulary (the `msg` field; tests may match on these):

| `msg` | When |
| --- | --- |
| `worker starting`, `registered`, `worker stopped` | Lifecycle; `worker stopped` carries `builds_done`, `builds_lost` (ownership lost to fencing) and `builds_released` (handed back on shutdown) |
| `claimed` | After a successful claim; carries `estimate_s` |
| `renew rejected: lease lost, cancelling build` | Renewer saw zero rows (WARN) |
| `lost ownership` | Main thread gave up on the build; `why` says which write was rejected (WARN) |
| `completed`, `marked failed` | The guarded write succeeded; `marked failed` carries `reason` |
| `postgres unavailable while idle, reconnecting`, `postgres unavailable during renew/complete/fail/release/heartbeat, reconnecting` | A connection error; the worker reconnects (WARN, decision 48) |
| `renew failing past the lease deadline: assuming the lease is lost, cancelling build` | Connection errors outlasted the lease (WARN) |
| `deregistered` | Clean exit removed the `workers` row (decision 49) |
| `build failed` | The exception, with traceback (ERROR) |
| `shutdown requested` | SIGTERM/SIGINT; `why` |
| `finishing current build before exit`, `cancelling current build to release it` | Decision 37; both carry `remaining_s` and `budget_s` |
| `released back to queue` | The guarded release succeeded |

**Smoke test with real processes (2026-10-04).** Postgres in a container, the Go scheduler binary with a 1 s reap interval, two Python worker processes with a 3 s lease, 1 s renew, 10x time scale. A 2M-vector build (65 s modeled, 6.5 s scaled) was submitted. Worker A claimed it (attempt 1) and was `kill -9`'d after 2 s. The scheduler logged `lease expired, build back to queued … attempt=1 lease_owner=gpu-A`; worker B claimed attempt 2 and completed it; the final row was `done`, attempt 2, 9.8 s after the kill (3 s lease plus 6.5 s build). Then a second build: worker B claimed it, received SIGTERM 1 s in with 5.4 s remaining against a 5 s budget, cancelled at 19%, released it (`queued`, attempt 1, no owner), and exited 0. This is the phase's done-when condition, shown by hand at the process level; step 4 repeats it under Compose and step 6 under the shard simulator.

### Step 4: Docker Compose (done 2026-10-05)

| Path | What |
| --- | --- |
| `deploy/compose.yaml` | Postgres 17 on a named volume; the scheduler built from `scheduler/Dockerfile` with a self health check; two worker replicas built from `worker/`; start order by health; every knob an env var with a default (decisions 43 to 47). |
| `scheduler/cmd/scheduler/main.go` | `scheduler healthcheck` subcommand (decision 44). |
| `scripts/compose-failures.sh` | Brings the stack up with 5 s leases, 1 s renews and 10x fake builds, then runs the failure tests an operator would run by hand, narrating, and saves `test-logs/phase1/step4-compose-failures.log`. |

What the script does and what it showed (2026-10-05 run):

| Step | Scenario | Observed |
| --- | --- | --- |
| 1 | `up -d --build`; wait for health | Postgres healthy, then scheduler healthy, then two workers; pool reports 2 live workers. |
| 2 | Smoke: one round, one 100k build | Claimed and done in about half a second, attempt 1. |
| 3 | SIGKILL the worker's python process from inside its container (a crash; the container has tini as PID 1) 2 s into a 6.5 s build | Scheduler logged `lease expired … attempt=1`; the other worker claimed attempt 2 and completed; done 12 s after the kill (5 s lease plus 6.5 s build). The crashed container's log ends at `claimed`. The restart policy brought it back and the pool returned to 2. It took three versions of this step to get a real crash: `docker kill` is a manual stop (no restart), and `kill -9 1` from inside is ignored by the kernel; see the study notes and decision 47. Roadmap failure test 1. |
| 4 | `docker pause` (SIGSTOP) the leaseholder 1 s in, for 8 s against a 5 s lease, then `docker unpause` | While paused the row was reaped to `queued`. On resume the paused worker logged `renew rejected: lease lost, cancelling build` and `lost ownership` for attempt 1 and wrote nothing more for that attempt. Attempt 2 was claimed and completed exactly once, by whichever worker got to it first (it can be the woken worker itself, re-claiming, which is correct: fencing is about attempts, not identities). Roadmap failure test 2. |
| 5 | `docker compose restart scheduler` while a build is leased | The worker never noticed: the build completed as attempt 1. The restarted scheduler answered `/healthz` and stamped the round finished. Roadmap failure test 3. |
| 6 | Resubmit the finished build with a different body | 200, `created:false`, the stored decision. Roadmap failure test 4. |

All four roadmap failure tests pass on the Compose stack. Step 6 repeats them with the shard simulator generating the load, and step 7 at 50 shards.

### Step 5: shard simulator and workload generator (done 2026-10-05)

| Path | What |
| --- | --- |
| `experiments/workload/workload.go` | The contract: `Workload`, `ShardWorkload`, `Query` (`Kind`, `ClusterQueryID`, `NeedsIndex`), `Scenario` and its four knobs, `Validate` (decisions 51, 52). Project-owned. |
| `experiments/workload/generate.go`, `presets.go`, `DESIGN.md` | The generator, written by an independent agent on Opus from the roadmap and the modeling assumptions alone. Poisson cluster-level arrivals split into fan-out pieces (uniform width in [2, n], correlated log-normal durations) or routed single-shard queries; rare long local DDL; `NeedsIndex` drawn per cluster query; Zipf sizes by rank; bimodal with a fit boundary at 0.9 Max; staggered DDL spread evenly; per-shard utilisation held constant across 6 and 50 shards. Six presets. Choices and references (TPC-C, YCSB, Citus, Elasticsearch kNN) in its `DESIGN.md`. 13 tests. |
| `shard/client` | The shard's HTTP client. |
| `shard/sim` | `Shard` (decisions 53 to 55), `RunRound`, `Timeline` (decision 56). |
| `shard/cmd/shardsim` | The binary: env config, `PRINT_WORKLOAD=1` to inspect a workload, rounds loop, JSON timelines to `TIMELINE_DIR`. |
| `shard/Dockerfile`, `deploy/compose.yaml` (`shards` service) | Distroless image; the `/timelines` directory is created nonroot-owned so the named volume inherits it. |
| `scripts/compose-round.sh` | Runs one or more rounds on the stack and saves the timeline and service logs as a step log. |

**First real round on the stack (2026-10-05, `test-logs/phase1/step5-compose-round.log`).** Skewed preset, 6 shards (sizes 33k to 200k), 2 workers, 10x time scale: 3,412 shard-level queries (1,792 cluster queries, 30% fan-out) over a 30 s modeled horizon. All six builds went to the GPU under policy v0 and finished at attempt 1 within 1.7 s real (17 s modeled); the round finished at 32.6 s modeled, set by the query horizon, with a straggler lag of 0.25 s. Fan-out latency p50 0.4 s, p99 1.9 s modeled; single-shard p50 0.1 s, p99 1.7 s. At 10x the scheduler took roughly 1,000 HTTP calls per second (three per query) without complaint; step 7's 50-shard round will multiply that by eight, which is the capacity question the generator agent raised.

Details the cross-check asked to have stated: `seq` is 0-based and counts recorded arrivals (so it diverges from the workload index once pre-DDL queries are dropped); local builds stay at `attempt 0`, since only a claim increments it; a shard reacts to a preemption within one poll interval plus one slice (measured 189 ms at a 100 ms poll). **Time-scale fidelity:** each query's recorded run time includes the two HTTP calls that bracket it (`start`, `done`), so at a time scale of 20 a 20 ms query, which should take 1 ms, is recorded at 1.6 to 1.8 ms, and `duration_modeled` inflates that by the scale. Keep the scale such that scaled query durations are well above a few milliseconds, or move to batched, client-timestamped job reporting, which is the same change step 7 needs for capacity.

**Open questions from the generator agent**, for the owner (recorded here, not decided):

1. ~~Should `NeedsIndex` be tied to fan-out?~~ Decided: no, decision 58.
2. Should `downstream_heavy`'s "mostly needs the index" apply only to the hot shard? That needs a contract field.
3. Should query load grow with shard size? Today it is independent.
4. ~~Staggered DDL~~ Decided: backfill, decision 59.
5. The bimodal preset's advised worker memory assumes 640 bytes per vector; the scheduler's cost model says 1,024 (`MemFactor` 2 times 4-byte floats times 128 dims). The fit boundary moves; align the two before Phase 3.
6. Should index-needing queries cost more CPU than others?
7. `math/rand/v2`'s derived methods are not promised stable across Go releases; store generated workloads as JSON alongside results, not only the seed.
8. ~~Capacity at 50 shards~~ Decided: batched, client-timestamped reporting, decision 60, implemented in step 7.

### Step 6: the failure tests under simulator load (done 2026-10-05)

| Path | What |
| --- | --- |
| `shard/client` | Retry with backoff; 409 on start/done as recorded (decision 61). Unit tests with a fake scheduler. |
| `shard/cmd/shardsim` | Logs a stop between rounds. |
| `scripts/compose-failures-load.sh` | The stack with the simulator running continuous rounds (6 shards, skewed, 2x time scale, 5 s leases); each test waits for a round with a leased build, injects the failure, and checks the round still completes: every build done once, every query finished, the round stamped. Saves `test-logs/phase1/step6-compose-failures-load.log`. |
| `scripts/jget.py` | JSON expression helper for the scripts. |

Results (2026-10-05 run), each on a live round of about 3,500 queries:

| Test | Observed |
| --- | --- |
| Baseline round | 3,412 queries, 6 builds done at attempt 1, round stamped at 15 s real. |
| Crash the leaseholder mid-build | The shard's own log shows the build go leased → queued (reaped) → leased at attempt 2 → done; completed exactly once; the shard's index-ready came 8.9 s in instead of about 7; the round finished with every query done; the pool returned to 2. |
| Pause the leaseholder past its lease | Attempt 2 completed once; the paused worker logged `renew rejected` and `lost ownership` for attempt 1; the round finished. |
| Restart the scheduler mid-round | Seven shard calls failed and were retried, all succeeded; no shard failed; every build still at attempt 1 (workers never noticed); the round finished. |
| Duplicate submit during a round | 200 `created:false`, `n_vectors` unchanged, round finished. |
| `docker stop` the simulator | It logs `stopped by signal` and exits 0. |

### Cross-check of step 5, the shard simulator (2026-10-05)

A fresh agent ran the real `shardsim` binary against an in-process scheduler (`httptest`), a reaper loop and a fake GPU worker it played itself, observing only through Postgres, the binary's stdout and its timeline JSON. 9 tests, all passed: determinism of the replayed workload across processes; every query recorded once, in order, and finished, with one CPU per shard and builds done at attempt 1; `needs_index` queries never start before the build finishes while independent ones run; a local build occupies the CPU for exactly its scaled time; reports every poll interval with sticky `stream_done`; a preemption honoured within 100 ms with the independent queries draining on the freed CPU; SIGTERM exits in milliseconds; a bad scenario exits 2 with no traffic; the timeline JSON matches Postgres field for field across two mixed-placement rounds.

| Finding | Resolution |
| --- | --- |
| `POLL_INTERVAL=0s` accepted: 746 requests in 3 s, then the round timed out. | Decision 57: validation at startup. |
| SIGTERM mid-round exits 0 silently and leaves the round open with `finished_at` NULL. | Decision 57: a warning names the open round. No "abandoned" state exists; recorded as an open question. |
| Query run times inflated 1.6 to 1.8x at a time scale of 20 by the bracketing HTTP calls. | Documented above as a fidelity bound; batched client-timestamped reporting (step 7) removes it. |
| `seq` semantics, local `attempt 0`, preemption reaction time unstated. | Stated above. |
| Pre-DDL dropping never exercised in a live round. | True; staggered is Phase 3's scenario (open question 4). |

### Cross-check of step 4, the Compose stack (2026-10-05)

A fresh agent wrote 9 Go tests (10 cases) that bring up their own copy of the stack under project `kgpu-xcheck` and drive it only through the API and the `docker` CLI. 7 cases passed, 3 failed. Passing cases of note: `docker stop` on a leaseholder with a long remainder released the build and another worker re-claimed it within 0.4 s, before the lease would have expired; with a short remainder the build was finished before exit; a lease that expired while the scheduler was *stopped* was reaped exactly once by the next scheduler; `--scale worker=3` gave three simultaneous leases by three distinct owners.

| Finding | Resolution |
| --- | --- |
| **Bug.** Eight simultaneous submits of one build into a round's last slot: one 201, seven 409 "round full" instead of 200 `created:false`. | Bug log 6, commit `422d722`: the duplicate check moved after the round lock, like the count. Store test with five trials. |
| **Bug.** `docker compose restart postgres` crashed both workers with tracebacks; the build was still recovered by the reaper as attempt 2. | Bug log 7, decision 48: reconnect while idle, retry guarded writes until the lease deadline, errored renews are not rejections. Python test kills every backend connection mid-build with `pg_terminate_backend`; the worker keeps its lease and completes attempt 1. |
| **Spec was wrong.** The agent's kill test asserted that `docker kill` is followed by a restart, as decision 47 and the step 4 table then claimed. It is not: Docker treats `docker kill` as a manual stop. | The implementer had found the same thing hours earlier (see decision 47); the agent's test was updated to crash the worker's own process from inside the container, which the restart policy does recover. Both findings agree. |
| Budget vs grace period; releases counted as lost; no deregistration on clean exit; `WORKER_ID` reused across restarts; errored vs rejected renew; reap timing bound. | Decisions 48 to 50. |

### Cross-check of step 3, the worker (2026-10-04)

A fresh agent wrote 11 Go tests that run the real worker process (`python -m kgpu_worker`) as a subprocess, configure it only through its documented environment variables, and observe Postgres and the worker's log. 9 passed, 2 failed. Among the passes: `kill -9` mid-build with a second worker completing attempt 2 exactly once; SIGSTOP past the lease, SIGCONT, and the row byte-for-byte unchanged a full build time later; a reclaimed build cancelled within one renew interval; both SIGTERM branches.

| Finding | Resolution |
| --- | --- |
| **Bug.** The first heartbeat raced the first claim: the claim's `started_at` was 3 to 6 ms before the worker's `registered_at`, so pool state could miss a worker that was already busy. | The first heartbeat is synchronous, before the loop starts. Python test `registers_before_first_claim`. |
| **Bug, serious.** `COST_BANDWIDTH=0`, a documented override, made the builder's arithmetic raise after the claim; the process died with exit 1 and the row stayed `leased`. The reaper would requeue it and the next worker would crash the same way: a poison pill. | Decisions 39 and 41. Bug log 5. The agent's test became two: bad config is refused at startup with exit 2 and no claim; a build error via `FAKE_FAIL_BUILD_IDS` ends in `failed` with the reason and the worker lives on. |
| No way to fail a fake build. | Decision 40, `FAKE_FAIL_BUILD_IDS`. |
| After a lost lease, does the worker keep claiming? Budget units? SIGTERM while idle or during a claim? Which stream? Log message names? `RENEW >= LEASE`? | All stated in the behaviour paragraph, the config table, the log vocabulary table, and decision 39. |

### Cross-check of steps 1 and 2, second run (2026-10-02, against the revised spec)

A fresh agent wrote 16 tests from the revised spec. 12 passed, 4 failed. Findings and what was done:

| Finding | Resolution |
| --- | --- |
| **Bug, serious.** Twelve concurrent submits into a round with `n_shards = 3` all succeeded, over HTTP and through the store. The cap held only for sequential submits. Cause: the build count was a subquery inside the `SELECT … FOR UPDATE` statement. Under READ COMMITTED a statement's snapshot is taken when it starts, before it blocks on the lock, so the waiter counted zero builds even after the winner committed. | Fixed: lock in one statement, count in the next (fresh snapshot). The agent's two concurrency tests now pass. |
| **Bug.** A `stream_done` report from a shard with no build (reachable through the store, not HTTP) completed a round while a real shard was still streaming. | Fixed; decision 33. The agent's test was updated to the stricter resolution (the report is refused, not ignored). |
| **Spec disagreement.** Preempt reset `enqueued_at` to now; decisions 24 and 31 say it keeps the submit time. | Fixed: preempt no longer touches `enqueued_at`; decision 31 reworded. |
| Arrival validation unspecified (unknown round, no build, after `stream_done`, finished round). | 404 / 404 / 409; decision 33; API table. A finished round has every shard `stream_done`, so the last case is covered by the 409. |
| `stream_done` could be withdrawn by a later report. | Sticky; decision 33. |
| Renege sets `started_at`; 400 for an invalid report and 200 for start/done not in the API table; unknown round on report shares the 404. | Documented in decision 31 and the API table. |
| `ErrRoundFinished` is unreachable on its own. | True; kept as a defensive guard and noted in the store doc. |
| Stale "follow-up job" wording in the store package. | Fixed. |

### Cross-check of steps 1 and 2, first run (2026-10-02, before the model revision)

An independent agent (`.claude/agents/crosscheck-tester.md`, Opus, fresh context) wrote 14 black-box tests in `scheduler/crosscheck/` from the spec alone, without reading the implementation. 13 passed. The one failure and the gaps it reported, with what was done:

| Finding | Resolution |
| --- | --- |
| **Bug.** A duplicate `POST /builds` returned `cpu_build_ms` and `gpu_total_ms` computed from the repeat's request body while `mem_bytes` came from the stored row. A shard retrying with a different body would sleep for the wrong time. | Fixed: all estimates on a duplicate come from the stored row. The cross-check test now passes. |
| Which columns reap and release clear was unspecified; clearing `started_at` loses the first attempt's start. | Decision 24. |
| `Renew` succeeds on an expired-but-unreaped lease. | Intended; decision 25. |
| A failed GPU build leaves a `needs_index` job that can never start, so the round never finishes. | Open question below; fake builds cannot fail, so this is decided when real failures arrive (Phase 4). |
| `shard_id` not validated against `n_shards`; submits to a finished round accepted. | Fixed with a per-round cap rather than an id range; decision 27. |
| Job endpoints returned 409 for a nonexistent job. | Now 404; API table updated. |
| "Fits" boundary, heartbeat capacity change, concurrent duplicate submits unspecified. | Decision 26. |
| `reason` on a duplicate is not the original reason. | Not stored; the doc comment and API table now say so. |
| Stale duplicate API table in this doc. | Removed. |

Smoke test of the real binary: started before Postgres was ready (connect retry observed), migrated both files, answered `/healthz`, created a round, placed a build on the GPU queue with zero live workers (decision 19), and shut down cleanly on SIGTERM.

## Open questions

- **A failed GPU build and a waiting `needs_index` job.** `FinishCompleteRounds` treats `failed` as terminal, but the shard's job that needs the index can never start, so the round stays open forever. Resubmitting is a no-op by design. Candidates: the scheduler re-places a failed GPU build as local; or the shard marks dependent jobs skipped; or failed builds are retried once on the GPU. Decide in Phase 4, when the recall gate makes failure real. Raised by the cross-check.
- **An abandoned round.** If the simulator is stopped mid-round, the round stays open forever (`finished_at` NULL, builds queued or done). There is no round state to mark it. Candidates: a `cancelled_at` column set by a `DELETE`-like API call, or a reaper rule that closes rounds with no report for a long time. Decide when rounds are driven by the experiment runner (Phase 3).
- Polling for build completion (decision 23) adds up to one poll interval per shard to the round. Revisit with long polling if Phase 3 measurements show it matters.

- Should `failed` builds be retried automatically, or left for the shard to resubmit? Phase 1 leaves them; nothing fails in a fake build except a worker crash, which goes through reaping, not `failed`.
- Lease length and renew interval (30 s / 10 s) are placeholders. Phase 2's SIGTERM handling and Phase 5's preemption measurements will inform them.
