# Shard-to-GPU index build scheduler: roadmap

## The question and the metric

The project answers one question: when up to 50 shards compete for a few GPU workers, and each shard can also build on its own CPU, which placement policy minimizes the time until every shard has reported its result?

Two pressures pull against each other:

- **Building locally blocks the shard.** DML is blocked while the CPU builds the HNSW graph, so the queries arriving at the shard queue up behind it. Because results are gathered from all shards, one slow shard holds up the whole cluster.
- **Offloading to a saturated pool doesn't help.** If every GPU is busy, a new job just waits in a queue, and a local CPU build might have finished sooner.

**Primary metric:** round completion time, from the DDL until every shard has built its index and drained the queries the build held up: the cluster's recovery time. Report p50 and p99 over many rounds.

**Secondary metrics:** GPU busy fraction, queue wait, wasted work from speculative builds, and (from Phase 4) recall@10 of the built indexes.

### Modeling assumptions

Each of these is a config flag, so results can show which ones they depend on.

1. A **round** begins when every shard receives its index build (the DDL). Each shard also has a **query stream**: a seeded, finite sequence of queries arriving over time, each with a CPU duration and a `needs_index` flag. A shard is done when its build is terminal, its stream is exhausted and its backlog has drained; the round ends when every shard is done.
2. **Nobody declares queries in advance.** The scheduler learns a shard's load only from the shard's periodic **report**: queue depth, waiting queries that need the index, oldest wait, build progress. The simulator knows the future and may give it to a clairvoyant oracle policy for comparison, never to a deployable one.
3. A **local build** occupies the shard's CPU; the backlog grows at the arrival rate while it runs.
4. An **offloaded build** frees the CPU. Queries that don't need the index run as they arrive; those that do wait for the build wherever it ran. The CPU runs ready queries one at a time in arrival order, skipping queries whose index isn't ready.
5. **Placement can change in flight.** A local build can be preempted to the GPU queue (progress lost, CPU freed, priority grows with blocked work). A queued GPU build can be moved back to local. A leased build is never preempted. The shard learns of changes in the reply to its own report.
6. A **GPU worker** runs one build at a time and has a modeled memory capacity. A build that fits no live worker must build locally. Batching several small builds onto one worker comes later.
7. **Distribution:** index sizes follow a configurable distribution; Zipf is the default. Query stream profiles (rate, duration, `needs_index` fraction, horizon) are a second scenario dimension.
8. **Scale:** the default configuration is 6 shards and 2 GPU workers. 50 shards and 3 workers is the target, reached by changing config once the small setup runs.

## Why this is a runtime coordination problem

Sharded databases mostly avoid runtime coordination by planning ahead. This problem can't be planned away, which is why it needs a scheduler.

| | Sharded database queries | GPU index builds across shards |
| --- | --- | --- |
| Where coordination happens | Mostly at compile time: rewrite the query, send per-shard pieces, merge results | At runtime: jobs arrive unpredictably, GPUs go busy and idle, workers die |
| What makes it tractable | Data placement decided in advance: shard keys, co-located tables, duplicated reference tables | Shared state, leases, a global queue, and a placement policy |
| Where the hard runtime cases live | A few rare paths: distributed transactions, rebalancing, failover | Every job |

## System overview

Shards ask the scheduler where each build should run. The scheduler either tells the shard to build locally or puts the job in a GPU queue that workers pull from. Postgres holds all shared state, so every component except the job store is stateless and can be restarted. Local builds never leave the shard; only GPU builds go through the queue and the object store.

```
 Shards x50 ──submit build──▶ Scheduler
     ▲  │     ◀──build locally──  │
     │  │                         │ enqueue GPU jobs
     │  │ vectors for GPU builds  ▼
     │  ▼                      Postgres ◀──claim / renew / complete── GPU workers x2–4
   MinIO ◀────────────── vectors in, index files out ──────────────────────┘
```

| Component | What it does | Language | First appears |
| --- | --- | --- | --- |
| Shard simulator (×6, then ×50) | Per round: submits its build, generates its query stream, runs local builds and queries on its CPU, reports its load, obeys placement changes | Go | Phase 1 |
| Scheduler | Decides CPU or GPU per build, owns the queue, grants and reaps leases, tracks rounds | Go | Phase 1 |
| Job store | Postgres tables for jobs, leases and rounds; the single source of truth | SQL | Phase 1 |
| GPU worker (×2–4) | Pulls jobs, renews its lease, runs the build, publishes the result | Python | Phase 1 (fake), 4 (hnswlib), 5 (cuVS) |
| Object store | MinIO: vectors in, index files and manifests out | off the shelf | Phase 4 |
| Experiment runner | Replays seeded workloads under each policy and records results | Go or Python | Phase 3 |
| Observability | Prometheus and Grafana | off the shelf | Phase 2 |

### Repo layout

```
scheduler/     Go: API, placement policies, lease reaper
shard/           Go: shard simulator
worker/          Python: fake, hnswlib and cuVS builders behind one interface
db/migrations/   Postgres schema
deploy/          kind config, manifests or Kustomize, Helm values
experiments/     workload configs, runner, result CSVs, plots
docs/            design notes and failure write-ups
```

Keep the worker's build step behind one interface (`build(job) -> artifact`) from the first day. Phases 4 and 5 then only add implementations, and the scheduler never changes because of them.

## Phase 1: Scheduler with fake jobs

Build a scheduler that stays correct under failures, using jobs that only sleep. Run everything locally with Docker Compose; no Kubernetes yet. Start at 6 shards and 2 workers. **Done when** a worker killed mid-job still results in that job finishing exactly once, at both 6 and 50 shards.

### Fake jobs and cost model v0

A job carries `n_vectors` and `dim`. The worker sleeps for the modeled GPU time plus transfer time. All constants live in config, since Phase 4 replaces them with measurements.

- CPU build time = a × n × log n × dim
- GPU build time = CPU build time ÷ speedup + fixed overhead
- Transfer time = (vector bytes out + index bytes back) ÷ bandwidth
- GPU memory need = b × n × dim (vectors plus graph), compared against each worker's configured capacity

### Schema

Five tables. `builds` is the queue and the lease; `shard_jobs` records each query as it arrives and runs, so round completion and the timeline come from Postgres alone; `shard_status` holds each shard's latest load report; `workers` is membership and health; `rounds` is bookkeeping. The migrations in `db/migrations/` are authoritative; this is the shape.

```sql
CREATE TABLE rounds (
  round_id     bigserial PRIMARY KEY,
  scenario     text NOT NULL,
  seed         bigint NOT NULL,
  n_shards     int NOT NULL,            -- how many shards must report done before the round is complete
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz
);

CREATE TABLE builds (
  build_id     text PRIMARY KEY,        -- shard_id:round_id, the idempotency key
  round_id     bigint NOT NULL REFERENCES rounds,
  shard_id     int NOT NULL,
  n_vectors    bigint NOT NULL,
  dim          int NOT NULL,
  mem_bytes    bigint NOT NULL,         -- modeled GPU memory need
  placement    text NOT NULL,           -- 'local' or 'gpu'; may change while queued or running
  state        text NOT NULL,           -- gpu: queued, leased, done, failed; local: running, done, failed
  priority     double precision NOT NULL DEFAULT 0,
  attempt      int NOT NULL DEFAULT 0,  -- fencing token, +1 on every claim
  lease_owner  text,
  lease_until  timestamptz,
  fail_reason  text,
  enqueued_at  timestamptz NOT NULL DEFAULT now(),
  started_at   timestamptz,             -- start of the current attempt
  finished_at  timestamptz
);
CREATE INDEX builds_queue ON builds (priority DESC, enqueued_at)
  WHERE state = 'queued' AND placement = 'gpu';

CREATE TABLE shard_jobs (                -- one row per query, written on arrival
  round_id     bigint NOT NULL REFERENCES rounds,
  shard_id     int NOT NULL,
  seq          int NOT NULL,            -- arrival order within the shard
  duration_ms  bigint NOT NULL,         -- modeled CPU time
  needs_index  boolean NOT NULL,
  arrived_at   timestamptz NOT NULL DEFAULT now(),
  started_at   timestamptz,
  finished_at  timestamptz,
  PRIMARY KEY (round_id, shard_id, seq)
);

CREATE TABLE shard_status (              -- the shard's latest report; the scheduler's only view of its load
  round_id             bigint NOT NULL REFERENCES rounds,
  shard_id             int NOT NULL,
  queue_depth          int NOT NULL,     -- queries waiting (any kind)
  waiting_needs_index  int NOT NULL,     -- of those, how many need the new index
  oldest_wait_ms       bigint NOT NULL,  -- how long the oldest waiting query has waited
  build_progress       real NOT NULL,    -- 0..1 for a local build in progress, else 0
  stream_done          boolean NOT NULL, -- the shard's query stream is exhausted
  updated_at           timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (round_id, shard_id)
);

CREATE TABLE workers (
  worker_id    text PRIMARY KEY,
  mem_bytes    bigint NOT NULL,         -- modeled GPU memory capacity
  last_seen    timestamptz NOT NULL,    -- heartbeat; stale rows are not counted in pool state
  registered_at timestamptz NOT NULL DEFAULT now()
);
```

Local builds also get a `builds` row (`placement = 'local'`, state `running` then `done`), so every build in a round is visible in one place. A round is complete when `n_shards` shards have `stream_done`, every one of their queries has finished, and every build is terminal.

### The four operations

**Claim** (worker, when idle): workers pull, so no one has to track which GPU is idle.

```sql
UPDATE builds
SET state = 'leased', attempt = attempt + 1, lease_owner = $1,
    lease_until = now() + interval '30 seconds', started_at = now()
WHERE build_id = (
  SELECT build_id FROM builds
  WHERE state = 'queued' AND placement = 'gpu'
  ORDER BY priority DESC, enqueued_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED)
RETURNING build_id, attempt, n_vectors, dim;
```

**Renew** (worker, every ~10 s while building): extend `lease_until` `WHERE build_id = $1 AND attempt = $2 AND state = 'leased'`. Zero rows updated means the lease was lost, so the worker abandons the job.

**Complete** (worker): set `state = 'done'` under the same `build_id` + `attempt` guard. A worker holding a stale attempt number can't complete a job someone else now owns.

**Reap** (scheduler, every few seconds): set expired leases back to `queued`. The next claim bumps `attempt`, which fences out the old owner.

### Steps

- [x] Postgres schema and migrations, with an integration test of claim, renew, complete and reap against a throwaway Postgres container
- [x] Scheduler API: start a round, submit a build (v0 policy: always GPU unless it doesn't fit in memory), record query arrivals and completions, accept shard load reports and answer with the build's current placement, read round status
- [ ] Worker loop: register and heartbeat, claim, renew on a background thread, sleep, complete
- [x] Reaper in the scheduler
- [ ] Shard simulator: one binary running N shards as goroutines. Each shard submits its build, generates its query stream, runs the local build and ready queries on its single CPU under the rules above, reports its load every poll interval, and obeys placement changes in the reply (abort a preempted local build; start a build moved to local)
- [ ] Seeded workload generator with the scenario knobs: size distribution, query stream profile, DDL arrival pattern, pool size. Only the default scenario needs to run in Phase 1.
- [ ] Docker Compose: Postgres, scheduler, shard simulator, 2 workers. 6 shards by default.
- [ ] Per-round timeline output: for each shard, the build (with any placement change) and each query's arrival, start and end
- [ ] Scale to 50 shards and 3 workers by config, and rerun the failure tests

### Failure tests

- [ ] `kill -9` a worker mid-job: the lease expires, another worker picks the job up, and it completes once
- [ ] `SIGSTOP` a worker until its lease expires, then `SIGCONT` it: its renew and complete are rejected. This is the paused-process case that fencing tokens exist for.
- [ ] Restart the scheduler mid-round: the round still completes, because all state is in Postgres
- [ ] A shard resubmits the same build: the primary key makes it a no-op

## Phase 2: Kubernetes on kind

Run the same system on a local kind cluster, with Prometheus and Grafana watching it. **Done when** the dashboard shows a worker pod deleted mid-round and the round still completing.

### Cluster layout

- [ ] kind cluster with one control-plane node and three worker nodes
- [ ] Label and taint one node as the stand-in GPU pool. Workers get a `nodeSelector` and a toleration; nothing else can land there. This rehearses real GPU scheduling before there is a GPU.
- [ ] Postgres as a StatefulSet with a PersistentVolumeClaim
- [ ] Scheduler as a Deployment with one replica. Running two replicas is also safe for the reaper, because reaping is a conditional `UPDATE` and doing it twice changes nothing.
- [ ] Workers as a Deployment of long-running pullers. One Kubernetes Job per build is a different design; Phase 6 compares the two.
- [ ] Shards as a StatefulSet, e.g. 5 pods running 10 simulated shards each. Set CPU requests and limits now, so local builds compete for real CPU in Phase 4.

### Lifecycle

- [ ] Readiness probe: a worker is ready only once it can reach Postgres
- [ ] Liveness probe on every service
- [ ] Graceful shutdown: on SIGTERM a worker stops claiming, then either finishes its job within `terminationGracePeriodSeconds` or releases it with a guarded `UPDATE` back to `queued`. Releasing explicitly is faster than waiting for the lease to expire.
- [ ] Retries with backoff on every Postgres call, so a database restart doesn't crash anything

### Metrics

| Metric | Type | Emitted by |
| --- | --- | --- |
| `round_duration_seconds` | histogram | scheduler |
| `shard_lag_seconds` (last shard minus median shard, per round) | histogram | scheduler |
| `build_queue_wait_seconds` | histogram | scheduler |
| `build_duration_seconds{placement}` | histogram | worker, shard |
| `placement_decisions_total{placement, policy}` | counter | scheduler |
| `lease_expirations_total` | counter | scheduler |
| `gpu_queue_depth` | gauge | scheduler |
| `worker_busy` | gauge | worker |

Install Prometheus and Grafana with the `kube-prometheus-stack` Helm chart and scrape each service with a ServiceMonitor.

### Failure injection

- [ ] Delete a worker pod mid-build
- [ ] Delete the scheduler pod mid-round
- [ ] Drain the stand-in GPU node: every worker is evicted and the queue grows. This is the case the CPU-fallback policies in Phase 3 should handle.
- [ ] Restart Postgres: services retry, and nothing completes twice

## Phase 3: Placement policies and experiments

This phase is the core of the project: four policies behind one interface, compared on identical seeded workloads. Jobs are still fake. **Done when** the policy comparison exists for the baseline workload plus the sweeps below.

### Policy interface

A policy is a pure function of the state it's given, which lets the same code run in the real scheduler and in a simulator.

- `decide(build, shard_status, pool_state) -> (placement, priority)` when a shard submits
- `reconsider(pool_state, all_builds, all_shard_status) -> actions` on a timer: preempt a local build to the GPU queue, move a queued GPU build back to local, start a speculative copy, change a priority

What a deployable policy may know:

- **From the shard at submit:** `n_vectors`, `dim`, `mem_bytes`, and its current load report. Not its future queries.
- **From the shard while building:** its periodic report: queue depth, waiting queries that need the index, oldest wait, build progress. This is the signal that a local build is hurting.
- **From the pool:** live workers and their memory capacity, the remaining time of each in-flight build, and the estimated GPU time of each queued build

What only the **clairvoyant oracle** may know: each shard's full future query stream. It exists to measure how much the unknowable future costs the deployable policies.

**GPU finish estimate:** replay the queue. Given each worker's remaining time and the jobs ahead in priority order, assign each job to the earliest-free worker, then add this job's GPU and transfer time. This is list scheduling, and it only works because the scheduler can see the whole queue.

### The policies

1. **Always GPU.** Every build goes to the queue, first come first served. The baseline.
2. **Static threshold.** Builds with `n_vectors` ≥ T go to the GPU queue, the rest build locally. Sweep T. The OpenSearch-style approach.
3. **Reactive migration.** The core policy. At submit it knows only the build size, the pool, and the shard's current load, so it places like a threshold policy (or always local). Then, on every `reconsider`, for each shard building locally it compares two futures from the shard's report:
    - **Stay local:** remaining local time (from `build_progress`) plus draining the backlog afterwards.
    - **Migrate:** kill the local build and queue it on the GPU. Queries that don't need the index start draining the moment the CPU is freed; those that do wait for GPU queue wait plus GPU build plus transfer. Priority in the GPU queue grows with the blocked work behind the build, so the shard that is hurting most is served first.
    - If migrating recovers sooner, preempt. Sunk cost is in the comparison through `build_progress`: a build at ninety percent is rarely worth killing.
    - Also move a queued GPU build back to local when its GPU estimate has grown past its local estimate (reneging).
4. **Reactive migration plus speculation.** When a worker is idle and the queue is empty, take the local build whose shard reports the worst backlog. Start a GPU copy without killing the local build: the first to finish wins and the other is cancelled. Never loses CPU progress; burns a GPU. Record the wasted work.
5. **Clairvoyant cost-based.** The oracle. Knows every shard's future stream, so it computes exact local and GPU finish times at submit, chooses the sooner, and sets priority to the shard's would-be local finish (longest-processing-time-first). Not deployable; the gap between it and policy 3 or 4 is how much the unknowable future costs.

### Simulator

Fake jobs sleep in real time, so a 200-round sweep on the live system would take days. A small discrete-event simulator calls the same policy code with modeled time. Use it for the sweeps, and run a few configurations on the live cluster to check that the two agree. Where they disagree is a finding worth writing up.

### Scenarios

A workload is a seeded combination of four knobs: size distribution, query stream profile, DDL arrival pattern, and pool configuration. Each named scenario is chosen to stress one GPU resource management case. Every policy runs on every scenario.

| Scenario | Shape | GPU management case it exercises |
| --- | --- | --- |
| Uniform | Equal sizes, light query load, all at once | Pure queueing. Does CPU fallback relieve an undersized pool? |
| Skewed (default) | Zipf sizes, moderate query load with a mixed needs_index fraction, all at once | Critical-path priority. Longest-build-first should beat FCFS. |
| Bimodal | A few huge builds, many tiny; some huge builds exceed one worker's memory | Memory bin-packing, and stragglers set by the huge builds. |
| Downstream-heavy | Medium builds; one shard has a much higher query rate, mostly needing the index | The critical path is set by the queries behind the build, not by build size. Reactive migration should notice from the report alone. |
| Staggered | Shards submit at different times within the round | Speculation and reneging. GPUs sit idle while CPU builds are mid-flight. |
| Shrinking pool | A worker dies or is drained mid-round | Reneging queued jobs back to local. |

### Sweeps

| Variable | Values | What it shows |
| --- | --- | --- |
| GPU workers | 1, 2, 4 | Where GPUs stop being the bottleneck |
| Size skew (Zipf exponent) | 0.5, 1.0, 1.5 | How much critical-path priority matters |
| Estimate error (log-normal σ on every estimate) | 0, 0.3, 0.6 | How fragile the cost model is, and whether speculation covers for it |
| Query load relative to build work (arrival rate × duration) | 0.5×, 1×, 2× | When blocking the shard costs the most |
| `needs_index` fraction | 0, 0.5, 1 | When freeing the CPU early helps, and when only a finished index does |
| Shard count | 6, 50 | Whether conclusions from the small setup hold at scale |

### Steps

- [ ] Policy interface and the five implementations (clairvoyant runs only in the simulator)
- [ ] GPU finish estimator (queue replay)
- [ ] Preemption, reneging and speculation in `reconsider`, including cancelling the losing copy
- [ ] Discrete-event simulator using the same policy code
- [ ] Seeded runner writing one CSV row per round
- [ ] Plots: p50 and p99 round time per policy; p99 against estimate error per policy
- [ ] Validate three or four configurations on the live kind cluster

## Phase 4: Real CPU builds with hnswlib

Replace the sleeps with real hnswlib builds on real vectors, and replace the cost model's constants with measurements. **Done when** the fitted model's error distribution is known and the Phase 3 comparison has been rerun on real builds.

### Keeping the tradeoff real without a GPU

Workers are still CPU-only here, so give them more cores than a shard gets (e.g. 8 threads against a shard's 2). The shape of the tradeoff stays intact: remote builds are faster, but they wait in a queue and pay transfer costs.

### Data

- [ ] Use an ann-benchmarks dataset. SIFT-128 (1M vectors) or GloVe-100 fits on a laptop.
- [ ] Split it across 50 shards with Zipf-distributed sizes, capped so the total fits in memory
- [ ] Store each shard's vectors in MinIO as `.npy` files
- [ ] Compute exact ground truth per shard once with brute force, for the recall gate

### Builder

- [ ] Package the builder as a CLI, so the Python worker and the Go shard (via exec) run the same code
- [ ] Build: download vectors, `init_index(M=16, ef_construction=200)`, `add_items` with a set thread count, save the index
- [ ] Store artifacts under `build_id/attempt/`, so a stale attempt can never overwrite the winning one
- [ ] Publish order: upload the index, then write the manifest (build ID, attempt, checksum, parameters, recall), then mark the job complete under the fencing guard
- [ ] Recall gate: run held-out queries and compare recall@10 with ground truth. Below the threshold, mark the build failed with a reason.
- [ ] Keep queries simulated, but as a CPU-burning loop instead of a sleep, so they compete with local builds for real

### Calibration

- [ ] Measure build time over a grid of sizes (10k to 500k vectors) and thread counts, separately for worker and shard CPU limits
- [ ] Fit a × n × log n per configuration and check the residuals rather than assuming the curve
- [ ] Measure MinIO throughput inside the cluster for the transfer term
- [ ] Record the estimate error distribution and feed it back into the simulator's σ, so the Phase 3 sweeps use a realistic value

### Optional

- [ ] A query service that loads the newest manifest-verified index and swaps it in without dropping requests

## Phase 5: GPUs and cuVS

Add a real GPU build path, run it on a small cloud GPU node pool for a few hours, and rerun the comparison with a measured GPU cost model. **Done when** the policy comparison includes real GPU numbers and a spot preemption has been observed and recovered from.

### GPU builder

- [ ] A separate builder image based on a RAPIDS or cuVS container. Keep the CPU image as it is.
- [ ] Build path: build a CAGRA graph with cuVS, convert it to HNSW, save. Check the cuVS docs for the current API, and confirm the saved file loads in the query path (hnswlib) before trusting the pipeline.
- [ ] Alternative path if the direct API causes trouble: Faiss with its cuVS-backed CAGRA index, which is what OpenSearch's GPU workers use
- [ ] Measure on the GPU: build time across the size grid, conversion time, and peak GPU memory
- [ ] Add a memory constraint to placement: a build whose peak memory exceeds a worker's GPU can't be placed there

### Cluster

- [ ] A managed cluster (e.g. GKE) with one single-GPU node pool (e.g. one L4) on spot, autoscaling from zero
- [ ] Drivers and device plugin per the provider's docs; outside a managed provider, the NVIDIA GPU Operator
- [ ] Workers request `nvidia.com/gpu: 1`, with the same taint and `nodeSelector` rehearsed in Phase 2
- [ ] NVIDIA's DCGM exporter for GPU utilization and memory, on the same Grafana dashboard

### What to measure

- [ ] **Cold start:** with the pool at zero, the first job waits for a node to be provisioned. Add that delay to the GPU finish estimate. Expect CPU fallback to win more often while the pool is cold.
- [ ] **Preemption:** when a spot node is reclaimed mid-build, recovery should go through the lease-expiry path. Record how much work is lost, and whether shorter renew intervals are worth it.
- [ ] Rerun the Phase 3 sweeps in the simulator with the measured GPU model, then validate a few rounds live

### Cost control

- [ ] Set a billing alert before creating anything
- [ ] Tear the cluster down after each session; kind remains the everyday environment

## Phase 6 (optional): Kubernetes-native dispatch

Rebuild the dispatch side with a custom resource, an operator and Kueue, then write down what each hand-built piece became. The placement policy stays on top: Kueue handles admission, quota and priority, but it has no idea what queries are waiting on a shard, so it can't make the CPU-versus-GPU decision.

| Hand-built (Phases 1–5) | Kubernetes-native | Note |
| --- | --- | --- |
| `builds` table in Postgres | `IndexBuild` custom resources in etcd | State visible through kubectl; controllers watch instead of polling |
| Claim with `SKIP LOCKED` | Kueue admits a Job, the scheduler binds its pod | Push instead of pull |
| Lease expiry and the reaper | Job retries (`backoffLimit`) and node lifecycle handling | Same idea, different trigger |
| Reaper loop | Controller reconcile loop | Same pattern |
| Queue priority | Kueue workload priority | The policy still computes it |
| Worker count | ClusterQueue quota on `nvidia.com/gpu` | Fair sharing across LocalQueues |
| `attempt` fencing | Nothing built in | Still needs to be built |

Kubernetes can start a replacement pod while the original is still running on a partitioned node, and its docs say Job pods should tolerate running more than once. The artifact fencing from Phase 4 is still required.

### Steps

- [ ] `IndexBuild` CRD and a kubebuilder operator that creates one Job per GPU build and updates its status
- [ ] Kueue: a ResourceFlavor for the GPU nodes, a ClusterQueue with a GPU quota, LocalQueues per group of shards
- [ ] Map the policy's priority onto Kueue's workload priority
- [ ] Measure dispatch latency and GPU idle time for long-running workers against one Job per build. Per-build pods pay pod startup and possibly a large image pull; long-running workers hold GPUs while idle.
- [ ] Put the mapping table in `docs/`, noting what still had to be built by hand

## Open questions

- [ ] Is batching several small builds onto one GPU worker in scope?
- [ ] What should the project be called?

## Reading list

- Dean and Barroso, *The Tail at Scale* (CACM, 2013): stragglers, hedged and backup requests
- Martin Kleppmann, *How to do distributed locking* (2016): leases and fencing tokens
- Graham (1969) on multiprocessor scheduling bounds: the longest-processing-time-first rule
- PostgreSQL docs on `SELECT … FOR UPDATE SKIP LOCKED`
- Ootomo et al., *CAGRA* (ICDE 2024): the GPU graph index cuVS builds
- [OpenSearch GPU-accelerated vector search](https://opensearch.org/blog/gpu-accelerated-vector-search-opensearch-new-frontier/) and its RFCs for [GPU acceleration](https://github.com/opensearch-project/k-NN/issues/2293) and [remote index build](https://github.com/opensearch-project/k-NN/issues/2294)
- [OpenSearch remote index build docs](https://docs.opensearch.org/latest/vector-search/remote-index-build): its size threshold is the static policy 2 compares against
- [NVIDIA cuVS integration patterns](https://docs.nvidia.com/cuvs/user-guide/integration-patterns): offloaded builds and CAGRA-to-HNSW
- Kueue documentation: ClusterQueues, ResourceFlavors, workload priority
