# Phase 4: real CPU builds with hnswlib

## Status and scope

**Planned.** Replace sleeps with real hnswlib builds on real vectors, add the object store, and replace the cost model's constants with measurements.

**Done when** the fitted cost model's error distribution is known and the Phase 3 comparison has been rerun on real builds.

## System diagram

MinIO joins as the second stateful service. Vectors flow out to workers; index files and manifests flow back. The scheduler and Postgres are unchanged; only the build implementation and the shard's local build change.

```mermaid
flowchart LR
    DS["ann-benchmarks dataset<br/>(SIFT-128 or GloVe-100)"] -- "split once, Zipf sizes" --> MINIO

    subgraph MINIO["MinIO (S3 API)"]
        V["vectors/shard_k.npy"]
        GT["ground_truth/shard_k.npy"]
        ART["artifacts/build_id/attempt/<br/>index.bin, manifest.json"]
    end

    subgraph shard["shard k (2 CPU threads)"]
        LB["local build:<br/>builder CLI via exec"]
    end

    subgraph worker["worker (8 CPU threads, no GPU yet)"]
        RB["build(job) → artifact:<br/>hnswlib implementation"]
    end

    V --> LB & RB
    RB --> ART
    GT --> RB
    ART -- "manifest-verified" --> Q["query service (optional)"]
```

Workers get more threads than a shard, so the tradeoff keeps its shape without a GPU: remote builds are faster but wait in a queue and pay transfer.

## Flows

### Publish order and fencing on the artifact store

```mermaid
sequenceDiagram
    participant W as worker (attempt a)
    participant M as MinIO
    participant P as Postgres

    W->>M: GET vectors/shard_k.npy
    W->>W: init_index(M=16, ef_construction=200), add_items, save
    W->>W: recall@10 against ground truth
    alt recall below threshold
        W->>P: UPDATE state=failed, reason WHERE build_id AND attempt
    else ok
        W->>M: PUT artifacts/k:r/a/index.bin
        W->>M: PUT artifacts/k:r/a/manifest.json (written last)
        W->>P: UPDATE state=done WHERE build_id AND attempt
    end
```

A stale attempt writes under its own `attempt` prefix and can never overwrite the winner. A reader that sees a manifest knows the index beside it is complete. Completion in Postgres is recorded only after the manifest exists, so `state = done` implies a readable artifact.

### Calibration

```mermaid
flowchart LR
    G["grid: n in 10k…500k,<br/>threads in {2, 8}"] --> M["measure build time<br/>per config"]
    M --> F["fit a × n log n × dim<br/>per config; inspect residuals"]
    F --> C["constants into config"]
    M --> E["estimate error distribution"]
    E --> S["simulator σ for Phase 3 sweeps"]
    T["measure MinIO throughput<br/>inside the cluster"] --> C
```

## Design decisions

| # | Decision | Alternatives | Why |
| --- | --- | --- | --- |
| 1 | Object store (MinIO) for vectors and artifacts | Blobs in Postgres; shared filesystem | Hundreds of MB per shard. Postgres is a poor file server; a shared filesystem is painful across machines. S3 API means the cloud phase changes a URL. |
| 2 | Artifacts under `build_id/attempt/`, manifest written last, completion recorded after | Overwrite a single path per build | The fencing token extends to the artifact store. Without it a paused worker could overwrite a newer index after waking. |
| 3 | The builder is a CLI invoked by both the Python worker and the Go shard (via exec) | Reimplement the build in Go for the shard | One implementation, one set of parameters, one recall gate. Shard-side build cost is measured on the same code. |
| 4 | Recall gate before publishing | Trust the build | Catches a broken build before it is marked done. Also the secondary metric from the roadmap. |
| 5 | Follow-up jobs become CPU-burning loops, not sleeps | Keep sleeping | They must compete with local builds for the shard's real CPU limit, or the blocking rule is not being tested. |
| 6 | Fit the cost model from measurements and keep the residuals | Assume n log n | The estimate error distribution is an input to Phase 3's σ sweep; assuming the curve would hide the project's main uncertainty. |

## Calibration targets from the published figures (2026-10-06)

The measurements this phase makes have published reference points (Phase 1 doc, "Cost model v0 against published numbers"):

- **CPU build time.** 1M x 128 in 490 s and 10M x 128 in 9,489 s on 8 vCPUs (OpenSearch RFC); our model is 2 to 3x optimistic. The grid should reach at least 1M vectors at 128 dimensions on the worker's thread count, and the fitted constant should be compared with those two points. Whether `n log n` holds across the grid, rather than being assumed, is the residual check already planned.
- **Dimensions.** SIFT-128 and GloVe-100 calibrate the low-dimension regime only. Add at least one high-dimension dataset subset (768 or 1536, e.g. a Cohere or OpenAI embedding set, a few hundred thousand vectors) so the dimension term of the fit is measured, since transfer and memory scale with it and Phase 3's `high_dim` scenario depends on it.
- **Index size.** Oracle's rule, 1.3 x vectors x dims x 4 bytes, and our 640 bytes per 128-dim vector agree within a few percent; measure the actual file sizes from hnswlib and record bytes per vector against dimension and M.
- **Transfer.** Measure MinIO throughput inside the cluster for objects from 100 MB to several GB; at production sizes a single shard's vectors are 6 to 61 GB, so throughput and object-store parallelism decide whether the GPU path pays at all.
- **Thread scaling.** The published figures are 8 vCPUs (RFC), 32 cores (NVIDIA), 64 vCPUs (pgvector, 30x with parallel workers). Fit build time against thread count for both the shard's limit and the worker's, because the local-vs-GPU tradeoff is really "2 threads here vs 8 there vs a GPU".

## Open questions

- Dataset: SIFT-128 (1M) or GloVe-100? Decide by what fits in laptop memory after the Zipf split with 50 shards.
- Is the optional query service worth building? Only if there is time; it is the thing that would make "swap in a new index without dropping requests" concrete.
