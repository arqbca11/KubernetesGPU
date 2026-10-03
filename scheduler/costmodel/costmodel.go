// Package costmodel is cost model v0: how long a fake build takes and how
// much GPU memory it needs, as pure functions of n_vectors and dim.
//
// Every constant is a field so config can override it, and so Phase 4 can
// replace the lot with fitted values. The Python worker reimplements the
// same formulas to decide how long to sleep; keep the two in step.
//
//	CPU build time  = A * n * ln(n) * dim
//	GPU build time  = CPU build time / Speedup + GPUOverhead
//	transfer time   = (vector bytes out + index bytes back) / Bandwidth
//	vector bytes    = n * dim * 4                 (float32)
//	index bytes     = n * (dim*4 + 2*M*4)         (vectors + M links each way)
//	GPU memory      = MemFactor * vector bytes
package costmodel

import (
	"math"
	"time"
)

type Model struct {
	A           float64 // seconds per (n * ln n * dim) on a shard CPU
	Speedup     float64 // GPU build time = CPU time / Speedup ...
	GPUOverhead float64 // ... + this many seconds
	Bandwidth   float64 // bytes per second between shard and worker
	M           int     // HNSW links per node, for the index size
	MemFactor   float64 // GPU memory need as a multiple of the vector bytes
	BytesPerDim int     // 4 for float32
}

// Default is tuned so that a 100k x 128 build takes about 20 s on a CPU and
// about 4 s on a GPU, which keeps a 6-shard round under a minute.
func Default() Model {
	return Model{
		A:           1.4e-7,
		Speedup:     10,
		GPUOverhead: 2,
		Bandwidth:   200e6,
		M:           16,
		MemFactor:   2,
		BytesPerDim: 4,
	}
}

func (m Model) VectorBytes(n int64, dim int32) int64 {
	return n * int64(dim) * int64(m.BytesPerDim)
}

func (m Model) IndexBytes(n int64, dim int32) int64 {
	return n * (int64(dim)*int64(m.BytesPerDim) + int64(2*m.M*4))
}

func (m Model) GPUMemBytes(n int64, dim int32) int64 {
	return int64(math.Ceil(m.MemFactor * float64(m.VectorBytes(n, dim))))
}

func (m Model) CPUBuild(n int64, dim int32) time.Duration {
	if n < 2 {
		return 0
	}
	s := m.A * float64(n) * math.Log(float64(n)) * float64(dim)
	return secs(s)
}

func (m Model) GPUBuild(n int64, dim int32) time.Duration {
	return secs(m.CPUBuild(n, dim).Seconds()/m.Speedup + m.GPUOverhead)
}

func (m Model) Transfer(n int64, dim int32) time.Duration {
	return secs(float64(m.VectorBytes(n, dim)+m.IndexBytes(n, dim)) / m.Bandwidth)
}

// GPUTotal is what a worker sleeps for: transfer plus build.
func (m Model) GPUTotal(n int64, dim int32) time.Duration {
	return m.GPUBuild(n, dim) + m.Transfer(n, dim)
}

func secs(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }
