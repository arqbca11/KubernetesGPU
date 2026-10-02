# Shard-to-GPU index build scheduler: roadmap

## The question and the metric

The project answers one question: when up to 50 shards compete for a few GPU workers, and each shard can also build on its own CPU, which placement policy minimizes the time until the coordinator has every shard's result?

Two pressures pull against each other:

- **Building locally blocks the shard.** DML is blocked while the CPU builds the HNSW graph, so the shard's next OLTP job waits. Because results are gathered from all shards, one slow shard holds up the whole cluster.
- **Offloading to a saturated pool doesn't help.** If every GPU is busy, a new job just waits in a queue, and a local CPU build might have finished sooner.

**Primary metric:** round completion time, from the start of a round until the coordinator has all shard results. Report p50 and p99 over many rounds.

**Secondary metrics:** GPU busy fraction, queue wait, wasted work from speculative builds, and (from Phase 4) recall@10 of the built indexes.

### Modeling assumptions

Each of these is a config flag, so results can show which ones they depend on.

1. A **round** gives every shard one index build and one OLTP job. The round ends when every shard has finished both.
2. A **local build** occupies the shard's CPU, and its OLTP job can't start until the build finishes.
3. An **offloaded build** frees the shard's CPU, so its OLTP job runs while the GPU builds. The shard is done when both finish. A per-job flag can instead make the OLTP job wait for the new index; the default workload mixes both kinds.
4. A **GPU worker** runs one build at a time to start with. Batching several small builds onto one worker comes later.
5. **Skew:** index sizes follow a Zipf-like distribution, so a few shards are heavy and most are light.

## Why this is a runtime coordination problem

Sharded databases mostly avoid runtime coordination by planning ahead. This problem can't be planned away, which is why it needs a coordinator.

| | Sharded database queries | GPU index builds across shards |
| --- | --- | --- |
| Where coordination happens | Mostly at compile time: rewrite the query, send per-shard pieces, merge results | At runtime: jobs arrive unpredictably, GPUs go busy and idle, workers die |
| What makes it tractable | Data placement decided in advance: shard keys, co-located tables, duplicated reference tables | Shared state, leases, a global queue, and a placement policy |
| Where the hard runtime cases live | A few rare paths: distributed transactions, rebalancing, failover | Every job |

## System overview

Shards ask the coordinator where each build should run. The coordinator either tells the shard to build locally or puts the job in a GPU queue that workers pull from. Postgres holds all shared state, so every component except the job store is stateless and can be restarted. Local builds never leave the shard; only GPU builds go through the queue and the object store.

```
 Shards x50 ──submit build──▶ Coordinator
     ▲  │     ◀──build locally──  │
     │  │                         │ enqueue GPU jobs
     │  │ vectors for GPU builds  ▼
     │  ▼                      Postgres ◀──claim / renew / complete── GPU workers x2–4
   MinIO ◀────────────── vectors in, index files out ──────────────────────┘
```

| Component | What it does | Language | First appears |
| --- | --- | --- | --- |
| Shard simulator (×50) | Creates one build and one OLTP job per round, runs local builds when told to, reports completion | Go | Phase 1 |
| Coordinator | Decides CPU or GPU per build, owns the queue, grants and reaps leases, tracks rounds | Go | Phase 1 |
| Job store | Postgres tables for jobs, leases and rounds; the single source of truth | SQL | Phase 1 |
| GPU worker (×2–4) | Pulls jobs, renews its lease, runs the build, publishes the result | Python | Phase 1 (fake), 4 (hnswlib), 5 (cuVS) |
| Object store | MinIO: vectors in, index files and manifests out | off the shelf | Phase 4 |
| Experiment runner | Replays seeded workloads under each policy and records results | Go or Python | Phase 3 |
| Observability | Prometheus and Grafana | off the shelf | Phase 2 |

### Repo layout

```
coordinator/     Go: API, placement policies, lease reaper
shard/           Go: shard simulator
worker/          Python: fake, hnswlib and cuVS builders behind one interface
db/migrations/   Postgres schema
deploy/          kind config, manifests or Kustomize, Helm values
experiments/     workload configs, runner, result CSVs, plots
docs/            design notes and failure write-ups
```

Keep the worker's build step behind one interface (`build(job) -> artifact`) from the first day. Phases 4 and 5 then only add implementations, and the coordinator never changes because of them.

## Phase 1: Coordinator with fake jobs

Build a coordinator that stays correct under failures, using jobs that only sleep. Run everything locally with Docker Compose; no Kubernetes yet. **Done when** a worker killed mid-job still results in that job finishing exactly once.

### Fake jobs and cost model v0

A job carries `n_vectors` and `dim`. The worker sleeps for the modeled GPU time plus transfer time. All constants live in config, since Phase 4 replaces them with measurements.

- CPU build time = a × n × log n × dim
- GPU build time = CPU build time ÷ speedup + fixed overhead
- Transfer time = (vector bytes out + index bytes back) ÷ bandwidth

### Schema

```sql
CREATE TABLE builds (
  build_id     text PRIMARY KEY,        -- shard_id:round_id, the idempotency key
  round_id     bigint NOT NULL,
  shard_id     int NOT NULL,
  n_vectors    bigint NOT NULL,
  dim          int NOT NULL,
  placement    text NOT NULL,           -- 'local' or 'gpu'
  state        text NOT NULL,           -- queued, leased, done, failed
  priority     double precision NOT NULL DEFAULT 0,
  attempt      int NOT NULL DEFAULT 0,  -- fencing token, +1 on every claim
  lease_owner  text,
  lease_until  timestamptz,
  enqueued_at  timestamptz NOT NULL DEFAULT now(),
  started_at   timestamptz,
  finished_at  timestamptz
);
CREATE INDEX builds_queue ON builds (priority DESC, enqueued_at)
  WHERE state = 'queued' AND placement = 'gpu';
```

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

**Reap** (coordinator, every few seconds): set expired leases back to `queued`. The next claim bumps `attempt`, which fences out the old owner.

### Steps

- [ ] Postgres schema and migrations
- [ ] Coordinator API: start a round, submit a build (v0 policy: always GPU), read round status
- [ ] Worker loop: claim, renew on a background thread, sleep, complete
- [ ] Reaper in the coordinator
- [ ] Shard simulator: one binary running N shards as goroutines, each with a sequential CPU queue that applies the blocking rules from the assumptions above
- [ ] Seeded workload generator: 50 shards, Zipf-distributed index sizes, OLTP durations
- [ ] Docker Compose: Postgres, coordinator, shard simulator, 3 workers
- [ ] Per-round timeline output: for each shard, when its build and its OLTP job started and ended

### Failure tests

- [ ] `kill -9` a worker mid-job: the lease expires, another worker picks the job up, and it completes once
- [ ] `SIGSTOP` a worker until its lease expires, then `SIGCONT` it: its renew and complete are rejected. This is the paused-process case that fencing tokens exist for.
- [ ] Restart the coordinator mid-round: the round still completes, because all state is in Postgres
- [ ] A shard resubmits the same build: the primary key makes it a no-op

## Phase 2: Kubernetes on kind

Run the same system on a local kind cluster, with Prometheus and Grafana watching it. **Done when** the dashboard shows a worker pod deleted mid-round and the round still completing.

### Cluster layout

- [ ] kind cluster with one control-plane node and three worker nodes
- [ ] Label and taint one node as the stand-in GPU pool. Workers get a `nodeSelector` and a toleration; nothing else can land there. This rehearses real GPU scheduling before there is a GPU.
- [ ] Postgres as a StatefulSet with a PersistentVolumeClaim
- [ ] Coordinator as a Deployment with one replica. Running two replicas is also safe for the reaper, because reaping is a conditional `UPDATE` and doing it twice changes nothing.
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
| `round_duration_seconds` | histogram | coordinator |
| `shard_lag_seconds` (last shard minus median shard, per round) | histogram | coordinator |
| `build_queue_wait_seconds` | histogram | coordinator |
| `build_duration_seconds{placement}` | histogram | worker, shard |
| `placement_decisions_total{placement, policy}` | counter | coordinator |
| `lease_expirations_total` | counter | coordinator |
| `gpu_queue_depth` | gauge | coordinator |
| `worker_busy` | gauge | worker |

Install Prometheus and Grafana with the `kube-prometheus-stack` Helm chart and scrape each service with a ServiceMonitor.

### Failure injection

- [ ] Delete a worker pod mid-build
- [ ] Delete the coordinator pod mid-round
- [ ] Drain the stand-in GPU node: every worker is evicted and the queue grows. This is the case the CPU-fallback policies in Phase 3 should handle.
- [ ] Restart Postgres: services retry, and nothing completes twice

## Phase 3: Placement policies and experiments

This phase is the core of the project: four policies behind one interface, compared on identical seeded workloads. Jobs are still fake. **Done when** the policy comparison exists for the baseline workload plus the sweeps below.

### Policy interface

A policy is a pure function of the state it's given, which lets the same code run in the real coordinator and in a simulator.

- `decide(build, shard_state, pool_state) -> (placement, priority)` when a shard submits
- `reconsider(pool_state, all_builds) -> actions` on a timer, for moving queued jobs back to local or starting speculative copies

What the coordinator needs to know:

- **From the shard:** `n_vectors`, `dim`, remaining OLTP work in seconds, and its local build estimate
- **From the pool:** worker count, the remaining time of each in-flight job, and the estimated GPU time of each queued job

**GPU finish estimate:** replay the queue. Given each worker's remaining time and the jobs ahead in priority order, assign each job to the earliest-free worker, then add this job's GPU and transfer time. This is list scheduling, and it only works because the coordinator can see the whole queue.

### The four policies

1. **Always GPU.** Every build goes to the queue, first come first served. This is the baseline.
2. **Static threshold.** Builds with `n_vectors` ≥ T go to the GPU queue, the rest build locally. Sweep T. This is the OpenSearch-style approach.
3. **Cost-based with critical-path priority.**
    - Local finish = CPU build + OLTP, run one after the other.
    - GPU finish = the later of the GPU finish estimate and the OLTP job when the two are independent, or the GPU finish estimate plus the OLTP job when the OLTP job needs the index.
    - Choose whichever finishes sooner.
    - Queue priority = the shard's finish time if it built locally, largest first, so the shards that would be the worst stragglers get GPUs first. This is the longest-processing-time-first rule for minimizing makespan.
    - On each `reconsider`, move any queued job whose GPU estimate now exceeds its local finish back to local.
4. **Cost-based plus speculation.** When a worker is idle and the queue is empty, take the local build with the latest projected finish. If a GPU copy would finish sooner than the remaining local work, start one. The first to finish wins and the other is cancelled. Record the wasted work.

### Simulator

Fake jobs sleep in real time, so a 200-round sweep on the live system would take days. A small discrete-event simulator calls the same policy code with modeled time. Use it for the sweeps, and run a few configurations on the live cluster to check that the two agree. Where they disagree is a finding worth writing up.

### Sweeps

| Variable | Values | What it shows |
| --- | --- | --- |
| GPU workers | 1, 2, 4 | Where GPUs stop being the bottleneck |
| Size skew (Zipf exponent) | 0.5, 1.0, 1.5 | How much critical-path priority matters |
| Estimate error (log-normal σ on every estimate) | 0, 0.3, 0.6 | How fragile the cost model is, and whether speculation covers for it |
| OLTP work relative to build work | 0.5×, 1×, 2× | When blocking the shard costs the most |

### Steps

- [ ] Policy interface and the four implementations
- [ ] GPU finish estimator (queue replay)
- [ ] Reneging and speculation in `reconsider`, including cancelling the losing copy
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
- [ ] Keep OLTP simulated, but as a CPU-burning loop instead of a sleep, so it competes with local builds for real

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

Rebuild the dispatch side with a custom resource, an operator and Kueue, then write down what each hand-built piece became. The placement policy stays on top: Kueue handles admission, quota and priority, but it has no idea that a shard's OLTP job is waiting, so it can't make the CPU-versus-GPU decision.

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
