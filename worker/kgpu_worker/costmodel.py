"""Cost model v0, mirroring scheduler/costmodel/costmodel.go.

The worker only needs GPU total time (what the fake build sleeps for). The
formulas and defaults must stay identical to the Go package; the test
test_costmodel.py pins the Go test's numbers.

    CPU build time  = A * n * ln(n) * dim
    GPU build time  = CPU build time / Speedup + GPUOverhead
    transfer time   = (vector bytes out + index bytes back) / Bandwidth
    vector bytes    = n * dim * 4
    index bytes     = n * (dim*4 + 2*M*4)
    GPU memory      = MemFactor * vector bytes
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Model:
    a: float = 1.4e-7
    speedup: float = 10.0
    gpu_overhead: float = 2.0
    bandwidth: float = 200e6
    m: int = 16
    mem_factor: float = 2.0
    bytes_per_dim: int = 4

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Model":
        """Same COST_* variables the scheduler reads, so one Compose env block serves both."""
        env = os.environ if env is None else env
        d = cls()
        f = lambda key, default: float(env[key]) if env.get(key) else default  # noqa: E731
        return cls(
            a=f("COST_A", d.a),
            speedup=f("COST_SPEEDUP", d.speedup),
            gpu_overhead=f("COST_GPU_OVERHEAD", d.gpu_overhead),
            bandwidth=f("COST_BANDWIDTH", d.bandwidth),
            mem_factor=f("COST_MEM_FACTOR", d.mem_factor),
        )

    def vector_bytes(self, n: int, dim: int) -> int:
        return n * dim * self.bytes_per_dim

    def index_bytes(self, n: int, dim: int) -> int:
        return n * (dim * self.bytes_per_dim + 2 * self.m * 4)

    def gpu_mem_bytes(self, n: int, dim: int) -> int:
        return math.ceil(self.mem_factor * self.vector_bytes(n, dim))

    def cpu_build_s(self, n: int, dim: int) -> float:
        if n < 2:
            return 0.0
        return self.a * n * math.log(n) * dim

    def gpu_build_s(self, n: int, dim: int) -> float:
        return self.cpu_build_s(n, dim) / self.speedup + self.gpu_overhead

    def transfer_s(self, n: int, dim: int) -> float:
        return (self.vector_bytes(n, dim) + self.index_bytes(n, dim)) / self.bandwidth

    def gpu_total_s(self, n: int, dim: int) -> float:
        """What a worker sleeps for: transfer plus build."""
        return self.gpu_build_s(n, dim) + self.transfer_s(n, dim)
