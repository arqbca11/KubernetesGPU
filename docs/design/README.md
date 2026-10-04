# Design docs

One document per phase. The [roadmap](../roadmap.md) is the plan written before building; these are the designs as built, kept current while the phase is in progress and frozen when it is done. Significant bugs are written up in the [bug log](../bugs.md).

| Doc | Status |
| --- | --- |
| [Phase 1: scheduler with fake jobs](phase1-scheduler.md) | In progress |
| [Phase 2: Kubernetes on kind](phase2-kubernetes.md) | Planned |
| [Phase 3: placement policies and experiments](phase3-policies.md) | Planned |
| [Phase 4: real CPU builds with hnswlib](phase4-hnswlib.md) | Planned |
| [Phase 5: GPUs and cuVS](phase5-gpu.md) | Planned |
| [Phase 6: Kubernetes-native dispatch](phase6-kubernetes-native.md) | Optional, planned |

## Template

Every doc has the same sections, so a reader knows where to look.

1. **Status and scope.** What this phase builds and the done-when condition.
2. **System diagram.** Components, where they run, what talks to what.
3. **Flows.** Sequence or state diagrams for the paths that matter, including failure paths.
4. **Design decisions.** A table: decision, alternatives considered, why. Append-only; a reversed decision gets a new row that points at the old one.
5. **Implementation notes.** Filled in as components land: file layout, commands, anything a reader needs to run or modify it.
6. **Open questions.** Things decided later or deferred to a later phase.

Diagrams are Mermaid in fenced code blocks, which GitHub and Obsidian render inline.
