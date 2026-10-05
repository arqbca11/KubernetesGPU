"""The build step behind one interface (invariant 9).

    Builder.build(job, progress, cancel) -> Artifact

Fake (this file), hnswlib (Phase 4) and cuVS (Phase 5) are implementations.
Nothing else in the worker changes when one is added.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Protocol

from .costmodel import Model


@dataclass(frozen=True)
class Job:
    build_id: str
    round_id: int
    attempt: int
    n_vectors: int
    dim: int
    mem_bytes: int


@dataclass(frozen=True)
class Artifact:
    build_id: str
    attempt: int
    kind: str          # "fake", later "hnswlib", "cagra-hnsw"
    index_bytes: int   # modeled or real size of what was produced
    elapsed_s: float


class Cancelled(Exception):
    """The build was abandoned: lease lost, or shutdown."""


class Builder(Protocol):
    name: str

    def build(self, job: Job, progress: Callable[[float], None], cancel: threading.Event) -> Artifact: ...

    def estimate_s(self, job: Job) -> float:
        """How long build() is expected to take; used for the shutdown decision."""
        ...


class FakeBuilder:
    """Sleeps for the cost model's GPU time plus transfer time, in slices, so it
    can report progress and notice cancellation. time_scale > 1 speeds up
    everything (tests, demos) without touching the model."""

    name = "fake"

    def __init__(self, model: Model, time_scale: float = 1.0, slice_s: float = 0.1) -> None:
        if time_scale <= 0:
            raise ValueError("time_scale must be positive")
        self.model = model
        self.time_scale = time_scale
        self.slice_s = slice_s

    def estimate_s(self, job: Job) -> float:
        return self.model.gpu_total_s(job.n_vectors, job.dim) / self.time_scale

    def build(self, job: Job, progress: Callable[[float], None], cancel: threading.Event) -> Artifact:
        total = self.estimate_s(job)
        start = time.monotonic()
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= total:
                break
            if cancel.is_set():
                raise Cancelled(f"build {job.build_id} attempt {job.attempt} cancelled at {elapsed / total:.0%}")
            progress(min(elapsed / total, 1.0))
            time.sleep(min(self.slice_s, total - elapsed))
        progress(1.0)
        return Artifact(
            build_id=job.build_id,
            attempt=job.attempt,
            kind=self.name,
            index_bytes=self.model.index_bytes(job.n_vectors, job.dim),
            elapsed_s=time.monotonic() - start,
        )


def by_name(name: str, model: Model, time_scale: float) -> Builder:
    if name in ("", "fake"):
        return FakeBuilder(model, time_scale=time_scale)
    raise ValueError(f"unknown builder {name!r}")
