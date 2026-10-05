# workload

The contract between the workload generator and everything that replays a workload: the shard simulator (Phase 1), the discrete-event simulator and the clairvoyant oracle (Phase 3).

- `workload.go` (project-owned): the types and `Validate`. Do not change without updating consumers.
- `generate.go`, `presets.go`, tests, `DESIGN.md` (generator-owned): `Generate(Scenario, seed) Workload`, the six roadmap scenarios as presets, and the modeling choices behind them, written independently of the scheduler and shard code.
