# Phase 3: placement policies and experiments

## Status and scope

**Planned.** This is the core of the project: four policies behind one interface, compared on identical seeded workloads across the six scenarios. Builds are still fake.

**Done when** the policy comparison exists for every scenario plus the sweeps in the roadmap, with the simulator validated against a few live runs.

## System diagram

The policy package is pure and is linked into two hosts: the live scheduler and a discrete-event simulator. Both feed it the same inputs; only the clock differs.

```mermaid
flowchart LR
    subgraph policy["policy package (pure functions)"]
        D["decide(build, shard_state, pool_state)<br/>→ placement, priority"]
        RC["reconsider(pool_state, all_builds)<br/>→ actions"]
        EST["gpu_finish_estimate<br/>(queue replay)"]
        D --> EST
        RC --> EST
    end

    subgraph live["live scheduler"]
        API["HTTP API"] --> D
        T["timer"] --> RC
        D & RC --> PG[("Postgres")]
    end

    subgraph sim["discrete-event simulator"]
        EV["event loop<br/>(modeled time)"] --> D
        EV --> RC
        D & RC --> ST["in-memory state"]
    end

    RUN["experiment runner<br/>seeded scenarios → CSV"] --> sim
    RUN -. "a few configs" .-> live
```

## Flows

### Queue replay: the GPU finish estimate

List scheduling over the current queue. Possible only because the scheduler sees every queued job and every live worker.

```mermaid
flowchart TB
    A["workers with remaining time<br/>w1: 4 s, w2: 0 s"] --> B["queued jobs ahead, by priority<br/>j1: 6 s, j2: 3 s"]
    B --> C["assign each to earliest-free worker<br/>j1 → w2 (free at 0, done 6)<br/>j2 → w1 (free at 4, done 7)"]
    C --> D["this build → earliest free = w2 at 6<br/>+ GPU time + transfer = estimate"]
```

### Cost-based decision for one shard

```mermaid
flowchart LR
    IN["shard: n_vectors, dim, mem_bytes,<br/>follow-up jobs, local build estimate"] --> L["local finish =<br/>CPU build, then jobs in order"]
    IN --> G["GPU finish =<br/>simulate the job queue:<br/>needs_index=false jobs run now,<br/>first needs_index=true job waits for the GPU estimate"]
    L & G --> CMP{"earlier?"}
    CMP -- local --> OUT1["placement local"]
    CMP -- gpu --> OUT2["placement gpu<br/>priority = local finish time<br/>(largest first)"]
```

Priority equals the shard's would-be local finish time, so the worst would-be straggler gets a GPU first. This is the longest-processing-time-first rule for minimizing makespan.

### Speculation

```mermaid
sequenceDiagram
    participant RC as reconsider (timer)
    participant P as Postgres
    participant W as idle worker
    participant S as shard k (building locally)

    RC->>P: queue empty and a worker idle?
    RC->>P: pick the local build with the latest projected finish
    RC->>P: INSERT speculative GPU copy (build_id k:r:spec, state queued)
    W->>P: claim the copy
    alt GPU copy completes first
        W->>P: complete (guarded)
        RC->>P: mark local copy cancelled, shard stops its build
    else local build completes first
        S->>P: done
        RC->>P: mark GPU copy cancelled, worker stops at its next failed renew
    end
    RC->>P: record wasted work = time spent on the loser
```

## Design decisions

| # | Decision | Alternatives | Why |
| --- | --- | --- | --- |
| 1 | Policies are pure functions of passed-in state; no I/O, no clock, seeded randomness only | Policies query Postgres directly | The simulator can then reuse them byte for byte, and the sweep comparison measures the policy, not two implementations of it. |
| 2 | Priority is a number computed by the policy and stored on the row; the claim query orders by it | Separate queues per priority class | One queue, one index, one claim statement. The policy owns the meaning of the number. |
| 3 | `reconsider` runs on a timer in the scheduler and expresses every action as a row change | A long-lived planner that holds the queue in memory | Keeps invariant 1 (all state in Postgres) and makes reconsider restart-safe. |
| 4 | Reneging moves a queued GPU job back to local by a guarded `UPDATE … WHERE state = 'queued'` | Cancel and resubmit | A leased job is never reneged; only queued ones. The guard makes it race-free against a concurrent claim. |
| 5 | A speculative copy is a second `builds` row with its own `build_id` suffix | A flag on the original row | Two rows, two leases, two attempt counters. The original invariants hold unchanged. |
| 6 | Simulator for sweeps, live cluster for validation only | Sweeps on the live cluster | Fake jobs sleep in real time; a 200-round sweep would take days. Disagreement between the two is itself a result to write up. |
| 7 | Estimate error injected as log-normal noise on every estimate the policy sees | Only on build times | The policy never sees truth; the sweep over σ measures its fragility end to end. |

## Open questions

- Should `reconsider` be able to pre-empt a leased job (cancel it mid-build to hand the GPU to a worse straggler)? Not in Phase 3. It adds wasted work and the speculation policy already covers the idle-GPU case.
- Where does the shard's local build estimate come from? In Phase 3 it is the same cost model the scheduler uses. In Phase 4 the shard measures.
