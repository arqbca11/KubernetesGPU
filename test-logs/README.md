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

Per-step logs are the state of the tests at the end of that step, including any fixes from the step's cross-check round, regenerated whenever the step's code changes. `history/` (local only, not committed) keeps every timestamped run.

Produce a saved log with `scripts/test-db.sh --save phase1/<name> <args>`.
