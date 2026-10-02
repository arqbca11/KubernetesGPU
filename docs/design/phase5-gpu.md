# Phase 5: GPUs and cuVS

## Status and scope

**Planned.** Add a real GPU build implementation, run it for a few hours on a small cloud GPU node pool, and rerun the comparison with a measured GPU cost model.

**Done when** the policy comparison includes real GPU numbers and a spot preemption has been observed and recovered from.

## System diagram

The same manifests as Phase 2, applied to a managed cluster with a GPU node pool that autoscales from zero. The worker image changes; nothing else does.

```mermaid
flowchart TB
    subgraph cloud["managed cluster (e.g. GKE)"]
        subgraph std["standard node pool"]
            SD["scheduler"]
            PG["postgres"]
            MO["minio"]
            SH["shards"]
            MON["prometheus, grafana,<br/>DCGM exporter"]
        end
        subgraph gpu["GPU node pool: spot, 0…N nodes, one L4 each<br/>taint gpu-only, device plugin"]
            W["worker (cuVS image)<br/>requests nvidia.com/gpu: 1"]
        end
    end
    AS["cluster autoscaler"] -- "pending worker pod → add node<br/>idle node → remove" --> gpu
    SP["spot reclaim"] -. "node disappears mid-build" .-> gpu
```

## Flows

### Cold start

With the pool at zero, the first GPU job waits for a node. That delay is part of the GPU finish estimate, so the cost-based policy sends more work to CPUs while the pool is cold.

```mermaid
sequenceDiagram
    participant A as scheduler
    participant P as Postgres
    participant K as autoscaler
    participant N as new GPU node
    participant W as worker pod

    A->>P: first GPU build queued, workers table empty
    Note over A: estimate = node provision time + GPU time
    W->>K: pod pending (no node satisfies nvidia.com/gpu)
    K->>N: provision node (minutes)
    N->>W: pod starts, image pulled
    W->>P: register, heartbeat, claim
```

### Spot preemption

Recovery goes through the lease path; Kubernetes replaces the pod when a node is available again. What is measured: work lost per preemption, and whether a shorter renew interval would reduce it.

```mermaid
sequenceDiagram
    participant W1 as worker on node A
    participant P as Postgres
    participant R as reaper
    participant W2 as worker on node B

    W1->>P: claim, attempt 1
    Note over W1: node A reclaimed (30 s notice on most clouds)
    W1->>P: release if the notice is caught: state=queued (guarded)
    R->>P: otherwise lease expires → queued
    W2->>P: claim, attempt 2, rebuild from scratch
```

## Design decisions

| # | Decision | Alternatives | Why |
| --- | --- | --- | --- |
| 1 | Separate GPU worker image on a RAPIDS/cuVS base; CPU image unchanged | One image with both | The GPU image is large and needs CUDA. Keeping the CPU image lets the kind cluster stay the everyday environment. |
| 2 | Build CAGRA with cuVS, convert to HNSW, save; verify it loads in hnswlib | Serve CAGRA directly | The query path stays CPU hnswlib, matching the industry pattern (build on GPU, search on CPU). |
| 3 | Memory constraint in placement uses measured peak GPU memory per size | The modeled `mem_bytes` from Phase 1 | Phase 1's model is a placeholder; this phase measures it. |
| 4 | Spot nodes, autoscaling from zero, billing alert first, tear down after each session | On-demand nodes kept up | Cost. Preemption is also a feature here: it is the failure this phase wants to observe. |
| 5 | Cold-start delay added to the GPU finish estimate | Ignore it | It is real, minutes long, and the cost-based policy should visibly prefer CPUs while the pool is cold. |

## Open questions

- Faiss with cuVS-backed CAGRA as the fallback build path if the direct cuVS API causes trouble.
- Which cloud. GKE is the roadmap's example because its GPU node pools and autoscaler are well documented.
