# Test catalog

Every test in the repo, in plain words: the scenario it sets up and what it proves. Tests marked **cross-check** were written by the independent agent from the spec alone, without reading the implementation. The narrated output of each group is in `test-logs/phase1/` (see the log column).

Legend for "proves": the invariants are numbered in `CLAUDE.md`; decisions are numbered in `docs/design/phase1-scheduler.md`.

## Step 1: the store (Go, `scheduler/store`). Log: `step1-store.log`

| Test | Scenario | Proves |
| --- | --- | --- |
| ClaimOrdersByPriorityThenAge | Three builds queued with priorities 1, 5, 3. One worker claims three times, then once more on the empty queue. | Claims come out highest priority first (5, 3, 1), each with attempt 1; an empty queue returns nothing. |
| ConcurrentClaimsNeverShareABuild | Twenty builds; eight goroutines claim in a loop at the same time until the queue is empty. | Every build is claimed exactly once. `FOR UPDATE SKIP LOCKED` keeps concurrent workers off the same row (invariant 2). |
| FencingAfterReap | Worker 1 claims. Its lease is forced into the past (as if it stopped renewing). The reaper runs. Worker 2 claims. Worker 1 "wakes up" and tries renew, complete and fail with its old attempt number. | The reaper requeues only expired leases and is idempotent (invariant 5). The re-claim bumps attempt (invariant 4). Every write from the old owner matches zero rows and the row is untouched (invariant 3). Worker 2 completes once; a done row is never reaped. This is the SIGSTOP scenario at the SQL level. |
| ReleaseReturnsToQueueAndFences | Worker 1 claims then releases (graceful shutdown). Worker 2 claims. Worker 1 tries to complete. | Release puts the build back; the next claim gets attempt 2; the releasing owner's later write is refused (decision 15). |
| ClaimRespectsWorkerMemory | A 16 GiB build at high priority and a 1 GiB build. An 8 GiB worker claims, then a 24 GiB worker. | The small worker skips the build that does not fit even though it is first in line; the big worker takes it. "Fits" is `mem_bytes <= capacity` (decisions 7, 26). |
| DuplicateSubmitIsNoop | Submit a build, let a worker claim it, then submit the same build id again with a different priority and placement. | The second submit inserts nothing and changes nothing, even mid-lease (invariant 6). |
| LocalBuildsAreNeverReaped | Submit a local build. A worker tries to claim. The reaper runs. The shard reports it done, twice. | Local builds sit in state `running`, invisible to claims and the reaper; the first done is accepted, the second refused (decision 5). |
| HeartbeatAndPoolState | No workers, then two heartbeats, then one worker's last_seen pushed ten minutes into the past. | Pool state counts only recently seen workers and reports the largest memory among them; heartbeat is an upsert. |
| RoundFinishesOnlyWhenEveryShardIsDone | A two-shard round. Shard 1 submits, two queries arrive, its build completes, both queries finish, it reports stream done. Then shard 2 goes through the same, one step at a time. | The round is stamped finished only when both shards have terminal builds, finished queries and `stream_done`; the stamp is the last query's finish time, not the time of the check; running the check again changes nothing (decisions 18, 30). |
| PreemptAndRenegeTransitions | A running local build is preempted to the GPU queue with a priority, then reneged back to local, then preempted again and claimed by a worker, then renege is attempted on the leased build. | Preempt and renege flip placement and state under their guards; attempt is untouched by both; a shard that did not notice its build was taken away is refused at done; a leased build is never moved (decision 31). |
| SchemaRefusesOrphanShardRows | Insert a status row and a query row for a shard with no build, and a second build for the same shard in the same round, straight at Postgres. | The foreign keys and the unique constraint from migration 0004 refuse all three; through the store the same facts come back as `ErrNoBuild` (decision 33). |
| ConcurrentDuplicateIntoLastSlotIsNoop | Eight goroutines submit the same build id at once into a round with one slot, five times over. | Exactly one insert and seven no-ops each time, never "round full": the duplicate check runs after the round lock (bug log 6, invariant 6). |
| ReapFailsBuildAfterRetryBudget | A "poison" build. Three workers in turn claim it and die (lease forced to expire). The reaper runs each time with a budget of 3. A fourth worker claims. | The first two expiries requeue; the third marks the build failed with a reason; the queue is clear and the next worker gets the healthy build; with the budget disabled an expiry always requeues (decision 42, bug log 5). |

## Step 2: scheduler API, policy, cost model (Go). Log: `step2-scheduler.log`

| Test | Scenario | Proves |
| --- | --- | --- |
| api: EndToEndRoundOverHTTP | A full two-shard round through HTTP: a worker heartbeats; a round is created; shard 1's 100k build goes to the GPU and a retry of the same submit returns the original decision; shard 2's 20M build is placed local because it fits no worker; queries arrive at shard 1; shard 1 reports and sees its build still queued; a query that does not need the index runs; the worker claims and completes via SQL; shard 1's next report sees `done` and runs its index-dependent query; shard 2 finishes locally and its query runs; the round stays open until shard 2 reports stream done; a reaper tick stamps it. | The whole protocol a shard will follow, end to end, with the status codes the API table promises. |
| api: ReportReflectsPreemption | Shard 0 builds locally and reports a growing queue. The build is preempted to the GPU queue (as Phase 3's reconsider would). The shard reports again, then tries to report its local build done. | The report reply is how a shard learns of a preemption; its late done is refused with 409; a report for a shard with no build is 404; out-of-range report fields are 400 (decision 29). |
| api: ValidationAndNotFound | A table of bad requests: zero shards, an unknown JSON field, a bogus round id, a non-integer path, an unknown round, zero vectors, unknown build and job, and the health endpoint. | Each gets the documented status code; unknown fields are rejected, not ignored (decision 21). |
| api: SubmitValidationAgainstRound | Shards numbered 7 and 42 submit into a two-shard round; a third shard tries; shard 7 resubmits with a different body; the round is finished; shard 42 resubmits. | Shard ids need not be dense; the third distinct shard is refused with 409 (the cap); a duplicate's estimates come from the stored row; a duplicate into a finished round is still the idempotent 200 (decision 27). |
| policy: AlwaysGPU | Four inputs: fits, exactly fits, too big for every live worker, no live workers at all. | GPU, GPU, local, GPU. The memory rule applies only when a worker is live, since an empty pool may be cold-starting (decision 19). The same input twice gives the same answer (invariant 8). |
| costmodel: DefaultsAreInTheIntendedRange | A 100k x 128 build and a 1k x 128 build. | The big build takes about 20 s on CPU and less on the GPU; the tiny one is faster on CPU because of the fixed GPU overhead and transfer. The defaults produce the tradeoff the project studies. |
| costmodel: MonotonicInSize | Builds of 1k, 10k, 100k, 1M vectors. | CPU time strictly increases with size; degenerate sizes cost nothing. |

## Steps 1 and 2, cross-check (Go, `scheduler/crosscheck`, non-worker tests). Log: `step2-crosscheck.log`

| Test | Scenario | Proves |
| --- | --- | --- |
| ReportUpsertsStatusAndRepliesWithBuild | A shard reports twice. | One status row per shard, updated in place; the reply carries the build's placement, state and attempt (decision 29). |
| ReportReplyTracksWorkerProgress | A shard reports before a claim, after a claim, after completion. | The reply says queued, then leased with attempt 1, then done. |
| ReportFromShardWithoutBuildIs404 | A report for a shard that never submitted. | 404, and nothing is written. |
| InvalidReportRejectedAndPreviousKept | A valid report, then one with more index-waiting queries than queue depth. | 400, and the earlier report is still what is stored. |
| ArrivalRecordedOnceAndRepeatUnchanged | A query arrival, then the same seq again with different fields. | 201 then 200; the row is unchanged (decision 28). |
| JobStartDoneStatusCodes | Start and done on a nonexistent query, done before start, start twice, done twice. | 404 for no such query, 409 for the wrong state. |
| PreemptMovesRunningLocalToGPUQueue | A running local build is preempted. The shard reports and tries done. A worker claims. | Exactly the columns decision 31 names change, including that `enqueued_at` keeps the submit time (this test caught the implementation resetting it); the first claim gives attempt 1. |
| RenegeMovesQueuedGPUToLocal | A queued GPU build is reneged to local. A worker tries to claim. The shard reports and completes it. | Placement and state flip, attempt untouched, no claim possible, done succeeds. |
| TransitionsRefusedOutsideTheirGuards | Preempt and renege attempted on a leased build and on builds in the wrong state; the lease holder renews. | Neither transition touches a leased build or a build in the wrong state; the holder's renew still works. |
| RoundWaitsForStreamDoneThenStampsLatestFinish | All builds and queries done but one shard has not said stream done; then it does. | The round waits for `stream_done`; the stamp equals the latest child finish. |
| RoundStaysOpenWhileAQueryIsUnfinished | Everything done except one query that has started but not finished. | The round stays open. |
| StreamDoneFromBuildlessShardDoesNotCount | Two real shards, one still streaming; a `stream_done` report written for a shard 99 with no build. | The report is refused at the store (decision 33); the round does not finish. This test caught the original completion bug (bug log 3). |
| SubmitValidationAndCap | Zero and negative shard counts; an unknown round; a third shard into a two-shard round; a duplicate in a full round; sparse shard ids. | 400, 404, 409, 200, accepted, in that order (decision 27). |
| ConcurrentSubmitsRespectCap | Twelve shards submit at the same moment over HTTP into a round capped at 3. | Exactly 3 get 201 and 9 get 409. This test caught the lock-then-count race (bug log 4). |
| ConcurrentStoreSubmitsRespectCap | The same, through the store directly. | Exactly 3 inserted, 9 `ErrRoundFull`. |
| SubmitWithJobsFieldRejected | A submit carrying the old spec's `jobs` field. | 400, nothing written: a shard cannot declare its future (decisions 21, 28). |

## Step 3: the Python worker (`worker/tests`). Log: `step3-worker.log`

| Test | Scenario | Proves |
| --- | --- | --- |
| test_matches_go_defaults | The Python cost model computes the 100k x 128 and 1k x 128 numbers. | They equal the Go test's output to the microsecond. If either side's formula drifts, this fails. |
| test_env_overrides | `COST_SPEEDUP` and `COST_GPU_OVERHEAD` set in the environment. | The overrides take effect, same variable names as the scheduler. |
| test_claim_order_and_attempt | Two builds at priorities 1 and 5; the Python claim runs three times. | Priority order, attempt 1, round id returned, nothing on the empty queue: the Python SQL behaves like the Go SQL. |
| test_fencing_after_reap | The full SIGSTOP walk from the Python side: claim, expire, reap, re-claim, stale renew/complete/fail/release, live renew/complete, second complete. | Each stale write is refused; each live write is accepted once (decision 13: the two SQL mirrors agree). |
| test_memory_filter_and_release | A 16 GiB and a 1 GiB build; an 8 GiB worker claims, releases, re-claims; a 24 GiB worker claims. | Memory filter and release behave as in Go. |
| test_heartbeat_upsert | Two heartbeats with different capacities. | One row, capacity updated. |
| test_worker_claims_builds_completes | Two queued builds; a real `Worker` object runs in a thread with fake builds sped up 50x. | Both builds reach done with attempt 1; the worker is registered; it exits cleanly with `builds_done=2`. |
| test_lease_lost_mid_build_abandons_without_completing | A worker claims a long build. The test plays reaper and a second worker: requeues the row and leases it to `gpu-other` with attempt 2. | The worker's renew is rejected, it cancels its build, logs `lost ownership`, and never writes to the row again; the row stays with `gpu-other` (decision 36). |
| test_sigterm_releases_long_build_and_finishes_short_one | SIGTERM with 0.8 s of build left and a 0.2 s budget; then SIGTERM on a short build with a generous budget. | The long build is released at once (queued, attempt unchanged, no owner); the short one is finished before exit (decision 37). |
| test_two_workers_never_share_a_build | Twelve builds, two workers at once. | All twelve done, every attempt 1, no lost ownership on either side. |
| test_registers_before_first_claim | A worker starts with a build waiting. | Its `workers` row exists before its first claim's `started_at` (cross-check finding). |
| test_build_error_marks_failed_with_reason_and_worker_continues | One build is listed in `FAKE_FAIL_BUILD_IDS`, another is healthy. | The failing build ends `failed` with `RuntimeError: injected failure…` as the reason and no lease; the worker completes the healthy one and does not crash (decisions 40, 41). |
| test_bad_config_is_refused_before_connecting | Zero bandwidth, renew interval equal to the lease, negative time scale, a non-numeric cost constant, an unknown log format. | Each is refused with a message naming the variable, before any connection (decision 39). |
| test_survives_lost_database_connections_mid_build | Mid-build, every one of the worker's backend connections is terminated server-side with `pg_terminate_backend` (what a Postgres restart does to clients). | All three threads reconnect; the lease is renewed in time (no reap, attempt stays 1); the build completes; the worker goes on to claim and complete the next one (bug log 7, decision 48). |
| test_renew_outage_past_lease_deadline_assumes_lost | Every renew raises a connection error for longer than the lease. | The worker assumes the lease is lost, cancels the build, counts it lost and writes nothing (decision 48). |

## Step 5: shard simulator (Go, `shard/sim`). Log: `step5-shardsim.log`

Integration tests: the real scheduler API over HTTP, real Postgres, and an in-test fake worker that claims and completes GPU builds. Workloads are hand-built, so these do not depend on the generator.

| Test | Scenario | Proves |
| --- | --- | --- |
| TwoShardRoundEndToEnd | Two shards, an 8 GiB worker, a fan-out query that spans both shards and needs the index, independent queries, a single-shard query; the round runs at 20x. | Both builds go to the GPU and finish at attempt 1; every query finishes; a query that needs the index never starts before its shard's build is done; an independent query runs at once on a free CPU; the fan-out query's latency is its slowest piece's; the round is stamped and the timeline has every shard, query and cluster query (decisions 32, 52, 56). |
| LocalBuildBlocksThenDrains | A worker with 1 byte of capacity, so the build is placed local; two queries arrive during it, one needing the index. | Both queries start only after the local build finishes: the CPU was occupied (modeling assumption; decision 32). |
| PreemptionAbortsLocalBuild | A local build is preempted to the GPU queue at about 20% progress (as Phase 3's reconsider would), and a capable worker appears. | The shard aborts the local build at its next slice, the independent query runs on the freed CPU while the GPU builds, the index query waits for the GPU completion, and the build ends done at attempt 1 (decisions 31, 54). |

## Step 4: Compose failure tests (`scripts/compose-failures.sh`). Log: `step4-compose-failures.log`

Not a test suite but an operator's script: it brings the real stack up and does to it what the roadmap's failure tests describe, narrating what Postgres and the container logs show.

| Step | Scenario | Proves |
| --- | --- | --- |
| 1 | `docker compose up --build`, wait for health | Start order by health works: Postgres, then scheduler (which applies the schema), then two workers register. |
| 2 | One round, one small build | The stack does the normal thing end to end. |
| 3 | SIGKILL the worker's python process from inside its container (tini is PID 1), mid-build | A crash leaves no chance to release. The lease expires, the reaper requeues, the other worker completes attempt 2, exactly once. The restart policy brings the crashed worker back. (An external `docker kill` is a manual stop and is not restarted; `kill -9 1` from inside is ignored by the kernel.) Roadmap failure test 1. |
| 4 | `docker pause` the leaseholder past its lease, then `docker unpause` | The paused process wakes holding attempt 1, its renew is rejected, it logs `lost ownership` and writes nothing more for that attempt; attempt 2 is completed exactly once, by whichever worker claimed it (possibly the woken one, re-claiming). Roadmap failure test 2. |
| 5 | `docker compose restart scheduler` while a build is leased | The worker is unaffected (talks to Postgres, not the scheduler); the build completes as attempt 1; the restarted scheduler stamps the round. Roadmap failure test 3. |
| 6 | Resubmit a finished build with a different body | 200 `created:false`, the stored decision. Roadmap failure test 4. |

## Step 4, cross-check of the Compose stack (Go, `scheduler/crosscheck`, `Compose*` tests; need `KGPU_COMPOSE=1`). Log: `step4-crosscheck-compose.log`

These bring up their own copy of the stack (project `kgpu-xcheck`) and drive it only through the API and the `docker` CLI.

| Test | Scenario | Proves |
| --- | --- | --- |
| ComposeKill9LeaseholderMidBuild | Crash the worker process holding a 6.5 s build, 2 s in (SIGKILL to its pid from inside the container); watch the row, the scheduler log, every worker log and the crashed container. | The reaper requeues after the 5 s lease, the other worker completes attempt 2, exactly one `completed` line exists, and the restart policy brings the crashed container back (roadmap failure test 1, decision 47). The agent's first version used `docker kill`, which Docker treats as a manual stop; see decision 47. |
| ComposePauseLeaseholderPastLease | `docker pause` the leaseholder 1 s in, for 8 s against a 5 s lease; unpause while attempt 2 is still building elsewhere. | Reaped and re-claimed while paused; on resume the worker logs `renew rejected` and `lost ownership` for attempt 1 and writes nothing; one completion at attempt 2; the done row is unchanged afterwards (roadmap failure test 2, decisions 25 and 36, invariant 3). |
| ComposeSchedulerRestartMidRound | `docker compose restart scheduler` while a build is leased, then resubmit, add the second shard, try a third, report stream done, read the round. | The worker never notices (done at attempt 1, no reap); state survives the restart (duplicate is 200 `created:false`, cap still 409); the round is stamped with the latest build finish (roadmap failure test 3, decisions 3, 18, 27). |
| ComposeReaperResumesAfterSchedulerOutage | Stop the scheduler, SIGKILL the leaseholder, wait past the lease, start the scheduler. | The new scheduler's reaper requeues the expired lease exactly once and attempt 2 completes once; the reaper keeps no state (decision 3, invariant 5). |
| ComposeDuplicateSubmit | Resubmit a finished build with a different body; then, repeatedly, 8 simultaneous submits of one new build, into a round's last slot and into a round with spare capacity. | A sequential duplicate is a no-op returning stored estimates (roadmap failure test 4, invariant 6); simultaneous duplicates give one 201 and seven 200 `created:false` even into the last slot (decision 26). Caught bug log 6. |
| ComposeDockerStopLeaseholder/LongRemainderIsReleased | `docker stop` the leaseholder 0.5 s into a 6.5 s build (budget 5 s). | The worker cancels, releases attempt 1, and exits 0 well inside `stop_grace_period`; another worker claims attempt 2 within 0.4 s, without a reap; one completion; the stopped container stays stopped (decisions 15, 37, 47). |
| ComposeDockerStopLeaseholder/ShortRemainderIsFinished | `docker stop` the leaseholder 3 s into a 6.5 s build. | The worker logs `finishing current build before exit`, completes attempt 1, then exits 0 inside the grace period (decision 37). |
| ComposeScaleToThreeWorkers | `--scale worker=3`, submit three 6.5 s builds, then scale back to 2. | The pool reports 3 live workers; three builds are leased at once by three distinct owners, each done once with attempt 1; the pool drops back to 2 after scale-down (decisions 6, 46, invariant 2). |
| ComposePostgresRestartMidBuild | `docker compose restart postgres` 1 s into a leased build, then check the build, the pool, the round and a fresh build. | The stack recovers, data survives on the named volume, the in-flight build completes exactly once, and no worker crashes (decision 48). Caught bug log 7. |

## Step 3, cross-check of the worker as a black-box process (Go, `scheduler/crosscheck`, `Worker*` tests). Log: `step3-crosscheck-worker.log`

These start the real `python -m kgpu_worker` as a subprocess, configure it only through its documented environment variables, and watch Postgres and its log.

| Test | Scenario | Proves |
| --- | --- | --- |
| WorkerHeartbeatsBeforeFirstClaimAndEveryInterval | A worker starts with a build waiting; the test compares its registration time with the claim time and watches `last_seen` move. | Registered before the first claim; heartbeats keep arriving at the configured interval. This test caught the heartbeat race. |
| WorkerCompletesBuildForScaledModelTime | One build; measure how long the worker takes. | Claim then complete with attempt 1; the build lasts the cost model's time divided by `FAKE_TIME_SCALE` (1.63 s measured vs 1.63 s modeled); every build log line carries build_id, attempt and round_id. |
| WorkerTextLogFormatCarriesBuildFields | Same with `LOG_FORMAT=text`. | The three fields are present in text logs too. |
| WorkerClaimsOnlyWhatFitsItsMemory | A worker with 5000 bytes of capacity; builds of 5001 (higher priority) and 5000 bytes. | The 5001-byte build is never claimed; the 5000-byte one is (fits means `<=`). |
| WorkerRenewKeepsLeaseAliveUnderReaper | A 3.3 s build with a 1 s lease while the test runs the reaper every 100 ms. | The lease is never reaped; `lease_until` advanced 15 times. The renewer does its job. |
| WorkerKill9MidBuildAnotherWorkerCompletesOnce | Worker A claims; the test sends SIGKILL mid-build; the reaper runs; worker B is started. | Reap reports attempt 1 owned by A; B completes attempt 2; exactly one completion. Roadmap failure test 1. |
| WorkerSIGSTOPPastLeaseLosesOwnershipAndWritesNothing | Worker A is paused with SIGSTOP past its lease; the reaper and a second claim move on; A gets SIGCONT. | A's renew is rejected, it logs `lost ownership` for attempt 1, and the row is byte-for-byte unchanged a full build time later; A exits 0 on SIGTERM. Roadmap failure test 2. |
| WorkerReclaimedMidBuildCancelsWithinARenewInterval | Mid-build, the test requeues and re-leases the row to another owner. | `lost ownership` appears within one renew interval, about 5 s before the build would have ended; no complete, fail or release follows (decision 36). |
| WorkerSIGTERMReleasesBuildOverBudget | SIGTERM with more remaining time than the budget. | Row back to queued with attempt 1 and lease columns NULL; the next claim gets attempt 2; exit 0 (decisions 24, 37). |
| WorkerSIGTERMFinishesBuildWithinBudgetAndStopsClaiming | SIGTERM with 2.75 s remaining and the default 5 s budget; a second build is queued. | The current build reaches done; the second stays untouched at attempt 0; exit 0. |
| WorkerBadConfigIsRefusedBeforeAnyClaim | `COST_BANDWIDTH=0` with a build waiting. | Exit 2 with the variable named on stderr; the build is still queued at attempt 0; no worker row (decision 39). This scenario was the original poison-pill crash (bug log 5). |
| WorkerBuildErrorRecordsFailWithReason | One build in `FAKE_FAIL_BUILD_IDS`, one healthy. | The failing build ends `failed` with the exception text and no lease; the worker completes the other and is still running (decision 41). |
