# Test logs

Narrated test output, kept so what was checked and what happened can be read without rerunning anything. **For a plain-language description of every test and the scenario it checks, read [`docs/tests.md`](../docs/tests.md) first**; the logs are the evidence, the catalog is the explanation. Each log has a header (date, commit, versions), one line per test step as the test narrates it, `[db]` lines showing rows as Postgres held them, the Docker events for the throwaway Postgres container, and the result.

| File | What |
| --- | --- |
| `latest.log` | The most recent run of `scripts/test-db.sh`, whatever it covered. Overwritten every run. |
| `phase1/step1-store.log` | Step 1: the store's lease operations and fencing (Go, `scheduler/store`). |
| `phase1/step2-scheduler.log` | Step 2: the HTTP API, policy v0 and cost model (Go, `scheduler/api`, `scheduler/policy`, `scheduler/costmodel`). |
| `phase1/step2-crosscheck.log` | The independent cross-check agent's tests for steps 1 and 2, written from the spec without reading the implementation (Go, `scheduler/crosscheck`, non-worker tests). |
| `phase1/step3-worker.log` | Step 3: the Python worker (`worker/tests`). |
| `phase1/step3-crosscheck-worker.log` | The cross-check agent's tests for the worker, run as a black-box process (Go, `scheduler/crosscheck`, `Worker*` tests). |
| `phase1/step4-compose-failures.log` | Step 4: the operator's failure script on the Compose stack (crash, pause, scheduler restart, duplicate submit). |
| `phase1/step4-crosscheck-compose.log` | The cross-check agent's nine Compose scenarios (`Compose*` tests, need `KGPU_COMPOSE=1`). |
| `phase1/step5-workload.log` | Step 5: the workload generator's 13 tests (written by an independent agent). |
| `phase1/step5-shardsim.log` | Step 5: the shard simulator against the real API and Postgres (end-to-end round, local build blocking, preemption). |
| `phase1/step5-compose-round.log` | Step 5: the first real six-shard round on the Compose stack, with the timeline. |
| `phase1/step5-crosscheck-shardsim.log` | The cross-check agent's nine black-box tests of the `shardsim` binary. |
| `phase1/step6-compose-failures-load.log` | Step 6: the four failure tests injected into live rounds on the Compose stack. |
| `phase1/step6-crosscheck-load.log` | The cross-check agent's eight under-load scenarios (`Load*` tests, need `KGPU_COMPOSE=1`). |
| `phase1/step7-compose-round-50.log` | Step 7: one 50-shard, 3-worker round with the timeline. |
| `phase1/step7-failures-load-50.log` | Step 7: the four failure tests injected into live 50-shard rounds. |
| `phase1/step7-crosscheck-load-50.log` | The cross-check agent's suite at 50 shards and 3 workers, with the batching, fidelity and GPU-queue scale tests. |

Per-step logs are the state of the tests at the end of that step, including any fixes from the step's cross-check round, regenerated whenever the step's code changes. `history/` (local only, not committed) keeps every timestamped run.

Produce a saved log with `scripts/test-db.sh --save phase1/<name> <args>`.
