"""The Python cost model must agree with the Go one. These numbers are the Go
test's output (scheduler/costmodel/costmodel_test.go, defaults)."""

from kgpu_worker.costmodel import Model


def test_matches_go_defaults():
    m = Model()
    cpu = m.cpu_build_s(100_000, 128)
    gpu = m.gpu_total_s(100_000, 128)
    mem = m.gpu_mem_bytes(100_000, 128)
    print(f"100k x 128: cpu={cpu:.3f}s gpu_total={gpu:.3f}s mem={mem}")
    # Go: cpu=20.631162433s, gpu(total incl. transfer)=4.639116243s, mem_bytes=102400000
    assert abs(cpu - 20.631162433) < 1e-6
    assert abs(gpu - 4.639116243) < 1e-6
    assert mem == 102_400_000
    # Go: 1k x 128: cpu=123.786974ms gpu=2.018138697s
    assert abs(m.cpu_build_s(1_000, 128) - 0.123786974) < 1e-6
    assert abs(m.gpu_total_s(1_000, 128) - 2.018138697) < 1e-6


def test_env_overrides():
    m = Model.from_env({"COST_SPEEDUP": "20", "COST_GPU_OVERHEAD": "0"})
    assert m.speedup == 20 and m.gpu_overhead == 0
    assert m.gpu_build_s(100_000, 128) == m.cpu_build_s(100_000, 128) / 20
