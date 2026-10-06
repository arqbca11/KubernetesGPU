# Bug log

Significant bugs found in this project, written up so the cause is understood, not just the fix. One entry per bug: what was observed, what was actually wrong, how it was fixed, and what it teaches. Newest first.

Entries are added when a bug changes how we think about the system, is subtle enough that it would be made again, or was found by a method worth repeating (an independent cross-check, a failure test). Typos and one-line slips are not logged.

---

## 8. Every simulated query ran 0.6 ms longer than modeled: a fixed overhead that scales with the time scale

**Found:** 2026-10-06, by the step 7 cross-check's run-time fidelity test at 50 shards (and confirmed at 6). Fixed in [`2e601f3`](https://github.com/arqbca11/KubernetesGPU/commit/2e601f3), `shard/sim/shard.go` (`runQuery`). **Related:** the step 5 cross-check's time-scale finding (the HTTP calls inside the measured interval), which decision 60 fixed; this is the residue underneath it.

**Symptom.** Across 31,087 queries in one round, every recorded run time (`finished_at - started_at`) was the scaled modeled duration plus about 0.56 ms (p50), never less. Aggregate run time was 105.5% of modeled. By size: queries under 1 ms ran at 1.77x, 1 to 5 ms at 1.19x, over 100 ms at 1.004x. The same 0.6 ms appeared at 6 shards, so it was not load.

**Why it matters.** The overhead is fixed per query, so it grows with the time scale: at 10x a median 12 ms query becomes 1.2 ms real plus 0.6, about 1.5x. The shard's CPU is then busier than the model says by tens of percent, queues grow faster than they should, and queue growth is exactly what the Phase 3 experiments measure. Every 10x number in the step 7 notes was taken with this inflation.

**Cause.** `runQuery` took `started = now`, slept the scaled duration with a relative timer, then took `finished = now`. A timer never wakes early and usually wakes a little late (Go timer granularity plus goroutine scheduling, about half a millisecond here), so the recorded run time was the sleep plus the slop, and the next query started only after that, so the slop accumulated into throughput.

**Fix (decision 65).** The CPU is a virtual clock. A query starts at the later of "the CPU is free" and "the query arrived", finishes exactly its scaled duration later, and both timestamps are recorded from that clock; the goroutine sleeps to the absolute finish deadline. A late wake-up no longer stretches the recorded run time, and it does not accumulate, because the next sleep is to an absolute time and is shorter by the same amount. The shard runs at most one slop behind real time.

**What it would have looked like in production.** Nothing: this is a simulator defect. What it would have looked like in the results: every policy comparison at high time scale would have run on shards 20 to 50 percent busier than the scenario said, and the conclusions about when local builds hurt would have been drawn from the wrong load.

**Lesson.** A relative sleep measures "at least this long". For anything whose duration is the measurement, keep a virtual clock, record from it, and use real sleeps only to pace it. And a fidelity claim ("sleeps exactly the scaled duration") needs a test that checks it against every record, not a lower bound.

---

## 7. Workers crashed when Postgres restarted

**Found:** 2026-10-05, by the Compose cross-check agent's `docker compose restart postgres` test. Fixed in [`51716f8`](https://github.com/arqbca11/KubernetesGPU/commit/51716f8), `worker/kgpu_worker/worker.py` and `store.py`. **Related:** entry 5 (same class: an exit path while holding a lease that nothing guarded).

**Symptom.** Postgres was restarted while one worker held a lease and the other was idle. Both worker processes died with a traceback and were restarted by the Docker restart policy. The idle one crashed inside `claim` with `psycopg.errors.AdminShutdown: terminating connection due to administrator command`. The leaseholder logged `heartbeat errored`, `renew errored`, then crashed after its build finished, in the `complete` call. The build was still recovered, by the reaper and the other worker as attempt 2, 17 s after the restart, and nothing completed twice.

**Why it matters.** Correctness held, but every database blip cost a full lease plus a process restart per worker, and under Compose a container stopped the wrong way is not restarted at all (see the study notes on restart policy), so one blip could leave the pool permanently short. In production the symptom is a fleet of worker pods restarting in unison every time the database fails over, with the builds table showing a burst of attempt increments.

**Cause.** Two unguarded paths. The main loop's `claim` had no error handling at all: it was outside any `try`, so an idle worker died on the first failed statement. The leaseholder's `complete` ran in `_publish`, after the `try` from bug 5 had ended, so a connection error there escaped too. And the renewer treated an *errored* renew as a transient to retry forever, with no notion that past the lease deadline ownership can no longer be assumed.

**Fix (decision 48).** A connection error while idle logs, reconnects with backoff, and continues. `complete`, `fail` and `release` run through one helper that retries on connection errors until the lease deadline (the last successful claim or renew plus the lease length) and then stops touching the row, logging it as lost, because by then the reaper may have requeued it. A renew that errors is not a rejection: it keeps retrying until the same deadline, after which the lease is assumed lost and the build is cancelled. The heartbeat reconnects. psycopg marks a broken connection `closed`, so each thread's store simply reconnects.

**Lesson.** "No exit while holding a lease" has to cover connection loss, not just exceptions from the build. And an error is not a rejection: a rejected renew means someone else owns the build; an errored renew means you don't know, and the only honest deadline for "I don't know" is the lease itself.

---

## 6. Concurrent duplicates of a round's last build were refused as "round full"

**Found:** 2026-10-05, by the Compose cross-check agent. Fixed in [`422d722`](https://github.com/arqbca11/KubernetesGPU/commit/422d722), the `SubmitBuild` hunk of `scheduler/store/store.go`. **Related:** entry 4 (the same snapshot-before-lock mechanism, one statement earlier).

**Symptom.** A round with two slots, one taken. Eight identical submits of the second shard's build fired at once. One got 201. The other seven got `409 round already has n_shards builds` instead of `200 created:false`. In a round with spare capacity the same burst was handled correctly every time. A shard retrying its submit after a client timeout, as the round's last shard, would have been told the round was full rather than given its stored decision.

**Cause.** After bug 4, `SubmitBuild` locked the round row and then counted builds in a fresh statement. But the duplicate check, `SELECT EXISTS … WHERE build_id = $1`, still ran *before* the lock. The seven losers evaluated it from a snapshot taken before the winner committed, saw no build, waited for the lock, and then counted one build in a one-slot round. Full. The fix for bug 4 had moved the count after the lock and left the existence check where it was.

**Fix.** Lock first, then every check in its own statement: existence, then finished, then count. A losing twin now sees the winner's row and returns "not inserted," which the API turns into the idempotent 200.

**Lesson.** When a fix moves one check behind a lock, look at every other check in the same transaction. Bug 4's fix was correct and incomplete, and the same cross-check method that found 4 found 6 three days later by testing a different race.

---

## 5. A builder exception crashed the worker with the lease held: a poison-pill build

**Found:** 2026-10-04, by the cross-check agent running the worker as a black-box process. Fixed the same day in [`9f206d0`](https://github.com/arqbca11/KubernetesGPU/commit/9f206d0); the fix is the `_work` hunk of `worker/kgpu_worker/worker.py` plus `validate` in `config.py` and the exit-2 handling in `__main__.py`. (Bundled with the rest of that cross-check round; later bug fixes get their own commit.)

**Symptom.** With `COST_BANDWIDTH=0`, a documented configuration override, the worker claimed a build and then the whole process died with exit status 1 and a `ZeroDivisionError` traceback. No `failed` or `released` line was logged. The row stayed `state=leased, attempt=1, lease_owner=gpu-err`.

**Why it is worse than one crash.** The reaper would return the lease to the queue after it expired. A restarted worker, or any other worker with the same configuration, would claim the same build and crash the same way. Each cycle costs a lease length and a worker restart, and nothing ever marks the build failed. The build is a poison pill circulating through the pool forever, and under Kubernetes the symptom would be a worker Deployment in a crash loop with no obvious cause in the builds table.

**Cause.** The exception came from `estimate_s`, which the loop called to write the `claimed` log line, outside the `try` that guarded `build()`. The guard covered the step the implementer thought of as "the build" rather than everything that happens while the lease is held.

**Fix.** Two layers. Configuration is validated before the worker connects, so a value that would make arithmetic fail is refused with exit status 2 and one line naming the variable (decision 39). And the `try` now begins the instant the claim returns: any exception that is not a cancellation ends in the guarded `fail` with `TypeName: message` as the reason, the worker logs it, and the loop continues (decision 41). If even `fail` cannot be written, the worker says so and leaves the lease to the reaper.

**What it would have looked like in production.** On Kubernetes the worker pod exits 1 and is restarted with exponential backoff, so it shows `CrashLoopBackOff`. The pod log ends in a `ZeroDivisionError` traceback inside cost-model code, which reads as a code bug rather than a configuration one. On a different screen, the `builds` table shows one build whose `attempt` climbs by one every lease length, and the scheduler's reaper logs `lease expired` for the same `build_id` over and over. If the workers share a ConfigMap they all crash on the same build in turn, the GPU pool looks dead, and every other shard's build waits in a queue nobody drains. None of the three symptoms names the cause, and they live in three places: pod status, pod log, database.

What would make it visible: the `attempt` counter is the tell, since a healthy system loses a lease only when a worker dies, so `attempt` above 2 or 3 on any build is abnormal and worth an alert (Phase 2 metrics: `lease_expirations_total` by `build_id`, or a gauge of builds by attempt). The reaper's log line already carries `build_id`, `attempt` and `lease_owner`, so "same build, new attempt, freshly restarted owner, every thirty seconds" is a diagnosable pattern once you know to look. Startup validation is the cheapest layer: one clear line at deploy time, before any shared state is touched.

**The wider class, and the third fix.** Exception handling closes only half of this. A worker killed by the kernel for running out of memory on one oversized build raises nothing, so `fail` never runs, and that build cycles through the pool the same way, killing each worker that claims it. The defence that covers both halves is a retry budget: the reaper marks a build `failed` when its lease expires on attempt `MAX_ATTEMPTS` (default 5) instead of requeueing it, with the reason recorded (decision 42, commit [`7b13c7e`](https://github.com/arqbca11/KubernetesGPU/commit/7b13c7e)). It is a dead-letter rule, independent of why the attempts failed. Added 2026-10-05.

**Lesson.** In a lease-based worker, the invariant is not "the build is guarded" but "while I hold a lease, no code path may exit without resolving it." The guard belongs at the claim, not at the step you happen to call the build. A process that can crash on a configured value should refuse the configuration at startup, before it can touch shared state. And any retry loop that can be entered by a failure you did not anticipate needs a budget.

**Related:** entry 3 (a rule enforced in one caller is not an invariant; here the guard was in one place too).

---

## 4. Lock-then-count race: a per-round cap that held for sequential submits and failed completely under concurrency

**Found:** 2026-10-02, by the independent cross-check agent's concurrency test. Fixed in [`c3e7178`](https://github.com/arqbca11/KubernetesGPU/commit/c3e7178), the `SubmitBuild` hunk of `scheduler/store/store.go`.

**Symptom.** Twelve concurrent `POST /builds` into a round created with `n_shards = 3` all returned 201. Postgres held twelve builds in the round. The same cap worked perfectly when submits arrived one at a time, which is why the implementer's tests, all sequential, passed.

**The code as it was.** `SubmitBuild` ran this inside a transaction, then inserted if the count was below the cap:

```sql
SELECT r.n_shards, r.finished_at,
       (SELECT COUNT(*) FROM builds WHERE round_id = r.round_id)   -- the count
FROM rounds r
WHERE r.round_id = $1
FOR UPDATE;                                                        -- the lock
```

The intent: lock the round's row so submits into one round serialise, count the builds already present, insert only if under the cap. It reads as atomic. It is not.

**What READ COMMITTED does.** Each statement takes its own snapshot at the moment it starts and sees exactly the rows committed before that moment. `FOR UPDATE` adds one refinement: if the row to be locked is held by another transaction, the statement blocks until that transaction ends, then re-fetches the latest version of *that one row* and rechecks the `WHERE` clause against it (EvalPlanQual). Nothing else in the statement is re-evaluated. A subquery against another table still answers from the snapshot taken at the start.

**The interleaving.** Two shards, cap 3, round 7 empty.

| Step | Transaction A | Transaction B |
| --- | --- | --- |
| 1 | Statement starts, snapshot taken: 0 builds in round 7 | |
| 2 | Locks round 7's row. Count = 0 | |
| 3 | | Statement starts, snapshot taken: still 0 builds (A has not committed) |
| 4 | | Tries to lock round 7's row, blocks behind A |
| 5 | Inserts the build for shard 1 | blocked |
| 6 | COMMIT, lock released | blocked |
| 7 | | Acquires the lock. Rechecks round 7's row: unchanged, A never modified it. Returns `n_shards = 3`, count = 0, from the step-3 snapshot |
| 8 | | 0 < 3, inserts the build for shard 2 |

B's count was correct at step 3 and stale by step 7, and nothing in the statement's semantics ever refreshed it. With twelve transactions all starting before the first commit, every one snapshots zero builds, they serialise politely on the lock one after another, and all twelve insert. The lock did its job. The count was answered against the wrong moment.

**The fix.** Same transaction, same lock, but the count is its own statement, so it gets a fresh snapshot taken after the lock was acquired:

```sql
SELECT n_shards, finished_at FROM rounds WHERE round_id = $1 FOR UPDATE;  -- statement 1: lock
SELECT COUNT(*) FROM builds WHERE round_id = $1;                        -- statement 2: count
```

Replaying the table: B blocks at step 4 inside statement 1, acquires the lock at step 7, and only then starts statement 2, whose snapshot includes A's committed insert. Count = 1. The fourth transaction in a row of twelve sees 3 and is refused. After the fix the same test produced 3 inserts and 9 refusals, over HTTP and through the store.

Both halves are necessary. Lock without a fresh count is this bug. A fresh count without the lock lets two transactions both read 2, both insert, and end at 4.

**Alternatives not taken.** `SERIALIZABLE` isolation would have detected the read-write conflict and aborted one side with a serialization failure, correct but turning a rare condition into a retry loop. A database constraint cannot express a count cap directly without a trigger.

**Lesson.** Under READ COMMITTED, "lock, then read, then act" must be three statements, or at least the read must come after the lock in a statement of its own. A check folded into the locking statement is answered before the lock is granted. More generally: a test that exercises a concurrency promise must actually be concurrent. The promise in the design doc said "the row lock serialises submits per round so the cap holds"; the implementer tested the cap, the cross-checker tested the sentence.

---

## 3. A `stream_done` report from a shard with no build could complete a round

**Found:** 2026-10-02, by the cross-check agent. Fixed in [`c3e7178`](https://github.com/arqbca11/KubernetesGPU/commit/c3e7178) and [`412c189`](https://github.com/arqbca11/KubernetesGPU/commit/412c189) (the foreign keys).

**Symptom.** Round of two shards. Both builds finished. Shard 0 reported its stream done, shard 1 was still streaming. A status row written for a shard 99 that had never submitted, with `stream_done = true`, made `FinishCompleteRounds` stamp the round finished.

**Cause.** The completion rule counted `shard_status` rows with `stream_done` and compared the count to `n_shards`. Nothing tied those rows to shards that had actually submitted builds. The HTTP report handler refused a report for a shard with no build, so no real client could produce the state, but the store method underneath did no such check, and the store is a public surface used by tests, the reaper, and the future reconsider loop. A guard that exists in one caller is not an invariant.

**Fix.** Three layers. The completion query joins `shard_status` to `builds` on `(round_id, shard_id)`, so only shards with a build count. `ReportStatus` and `RecordArrival` return `ErrNoBuild` (404 over HTTP) for a shard with no build. And, the next day, migration 0004 added `UNIQUE (round_id, shard_id)` on `builds` with foreign keys from `shard_status` and `shard_jobs` to it, so the orphan row cannot exist no matter who writes it; the store now gets that error from Postgres (SQLSTATE 23503) rather than checking first.

**Lesson.** Enforce a rule where every caller passes through, or in the schema, not in the one handler that happens to exist today.

---

## 2. Test packages sharing one database truncated each other mid-test

**Found:** 2026-10-02, while triaging the first cross-check run. Fixed in [`5fdc4eb`](https://github.com/arqbca11/KubernetesGPU/commit/5fdc4eb), `scripts/test-db.sh`.

**Symptom.** A test created a round, got `{"round_id": 19}` back, submitted a build into round 19, and received 404 "no such round." Another package's test reported a build it had just inserted as missing.

**Cause.** `go test ./...` runs different packages' test binaries in parallel. Every package's database tests begin with `TRUNCATE` on the shared throwaway Postgres. The api and crosscheck packages ran at the same time, and one package's `TRUNCATE` landed between another's two requests.

**Fix.** `scripts/test-db.sh` runs `go test -p 1`, one package at a time. Within a package, tests were already sequential.

**Lesson.** Package-level parallelism is a default worth knowing before pointing several packages at one external resource. The phantom 404 was easy to misread as an API bug.

---

## 1. Duplicate submit mixed the stored row with the repeat's request body

**Found:** 2026-10-02, by the cross-check agent's first run. Fixed in [`5fdc4eb`](https://github.com/arqbca11/KubernetesGPU/commit/5fdc4eb), `scheduler/api/api.go`.

**Symptom.** A shard retried `POST /builds` with a body that differed from the original. The response said `created: false` and returned the stored placement and memory, but the CPU and GPU time estimates were computed from the repeat's `n_vectors`. A shard retrying a local build would have slept for the wrong time.

**Cause.** The handler computed estimates from the request before discovering the build existed, then overwrote only some fields from the stored row.

**Fix.** On a duplicate, every field in the response is derived from the stored row. The doc comment on `Created` had already promised this; the code hadn't kept the promise.

**Lesson.** When an endpoint is idempotent, build the duplicate response from storage alone. Mixing request and storage is how two retries disagree with each other.
