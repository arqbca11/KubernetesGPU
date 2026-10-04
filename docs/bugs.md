# Bug log

Significant bugs found in this project, written up so the cause is understood, not just the fix. One entry per bug: what was observed, what was actually wrong, how it was fixed, and what it teaches. Newest first.

Entries are added when a bug changes how we think about the system, is subtle enough that it would be made again, or was found by a method worth repeating (an independent cross-check, a failure test). Typos and one-line slips are not logged.

---

## 4. Lock-then-count race: a per-round cap that held for sequential submits and failed completely under concurrency

**Found:** 2026-10-02, by the independent cross-check agent's concurrency test. Fixed in `c3e7178`.

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

**Found:** 2026-10-02, by the cross-check agent. Fixed in `c3e7178`.

**Symptom.** Round of two shards. Both builds finished. Shard 0 reported its stream done, shard 1 was still streaming. A status row written for a shard 99 that had never submitted, with `stream_done = true`, made `FinishCompleteRounds` stamp the round finished.

**Cause.** The completion rule counted `shard_status` rows with `stream_done` and compared the count to `n_shards`. Nothing tied those rows to shards that had actually submitted builds. The HTTP report handler refused a report for a shard with no build, so no real client could produce the state, but the store method underneath did no such check, and the store is a public surface used by tests, the reaper, and the future reconsider loop. A guard that exists in one caller is not an invariant.

**Fix.** Two layers. The completion query joins `shard_status` to `builds` on `(round_id, shard_id)`, so only shards with a build count. And `ReportStatus` and `RecordArrival` refuse writes for a shard with no build (`ErrNoBuild`, 404 over HTTP). A third layer, foreign keys from `shard_status` and `shard_jobs` to a new `UNIQUE (round_id, shard_id)` on `builds`, is proposed as migration 0004.

**Lesson.** Enforce a rule where every caller passes through, or in the schema, not in the one handler that happens to exist today.

---

## 2. Test packages sharing one database truncated each other mid-test

**Found:** 2026-10-02, while triaging the first cross-check run. Fixed in `5fdc4eb`.

**Symptom.** A test created a round, got `{"round_id": 19}` back, submitted a build into round 19, and received 404 "no such round." Another package's test reported a build it had just inserted as missing.

**Cause.** `go test ./...` runs different packages' test binaries in parallel. Every package's database tests begin with `TRUNCATE` on the shared throwaway Postgres. The api and crosscheck packages ran at the same time, and one package's `TRUNCATE` landed between another's two requests.

**Fix.** `scripts/test-db.sh` runs `go test -p 1`, one package at a time. Within a package, tests were already sequential.

**Lesson.** Package-level parallelism is a default worth knowing before pointing several packages at one external resource. The phantom 404 was easy to misread as an API bug.

---

## 1. Duplicate submit mixed the stored row with the repeat's request body

**Found:** 2026-10-02, by the cross-check agent's first run. Fixed in `5fdc4eb`.

**Symptom.** A shard retried `POST /builds` with a body that differed from the original. The response said `created: false` and returned the stored placement and memory, but the CPU and GPU time estimates were computed from the repeat's `n_vectors`. A shard retrying a local build would have slept for the wrong time.

**Cause.** The handler computed estimates from the request before discovering the build existed, then overwrote only some fields from the stored row.

**Fix.** On a duplicate, every field in the response is derived from the stored row. The doc comment on `Created` had already promised this; the code hadn't kept the promise.

**Lesson.** When an endpoint is idempotent, build the duplicate response from storage alone. Mixing request and storage is how two retries disagree with each other.
