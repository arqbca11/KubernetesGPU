package costmodel

import (
	"testing"
	"time"
)

func TestDefaultsAreInTheIntendedRange(t *testing.T) {
	m := Default()
	cpu := m.CPUBuild(100_000, 128)
	gpu := m.GPUTotal(100_000, 128)
	t.Logf("100k x 128: cpu=%s gpu(total incl. transfer)=%s mem=%d MiB", cpu, gpu, m.GPUMemBytes(100_000, 128)>>20)
	if cpu < 15*time.Second || cpu > 30*time.Second {
		t.Fatalf("cpu build for 100k x 128 = %s, want roughly 20 s", cpu)
	}
	if gpu >= cpu {
		t.Fatalf("gpu total %s should beat cpu %s at this size", gpu, cpu)
	}
	// Tiny builds: the fixed GPU overhead and transfer make the CPU win.
	cpu, gpu = m.CPUBuild(1_000, 128), m.GPUTotal(1_000, 128)
	t.Logf("1k x 128: cpu=%s gpu=%s", cpu, gpu)
	if gpu <= cpu {
		t.Fatalf("for a tiny build gpu %s should lose to cpu %s", gpu, cpu)
	}
}

func TestMonotonicInSize(t *testing.T) {
	m := Default()
	prev := time.Duration(0)
	for _, n := range []int64{1_000, 10_000, 100_000, 1_000_000} {
		d := m.CPUBuild(n, 128)
		if d <= prev {
			t.Fatalf("cpu build not increasing at n=%d: %s <= %s", n, d, prev)
		}
		prev = d
	}
	if m.CPUBuild(0, 128) != 0 || m.CPUBuild(1, 128) != 0 {
		t.Fatal("degenerate sizes should cost nothing")
	}
}
