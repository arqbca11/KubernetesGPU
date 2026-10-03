// Package policy decides where a build runs. A Policy is a pure function of
// the Input it is handed: no I/O, no clock, no randomness (invariant 8). The
// scheduler and the Phase 3 simulator call the same code.
package policy

import "fmt"

// BuildSpec is what the shard tells us about the build. Nothing about the
// queries to come: a real shard does not know them (decision 28).
type BuildSpec struct {
	ShardID  int32
	RoundID  int64
	NVectors int64
	Dim      int32
	MemBytes int64 // modeled GPU memory need, from the cost model
}

// PoolState is what the scheduler knows about live GPU workers.
type PoolState struct {
	LiveWorkers int
	LargestMem  int64 // biggest memory capacity among live workers; 0 if none
}

type Input struct {
	Build BuildSpec
	Pool  PoolState
}

type Decision struct {
	Placement string  // "gpu" or "local"
	Priority  float64 // higher claims first; only meaningful for gpu
	Reason    string  // one line for the log and the API response
}

type Policy interface {
	Name() string
	Decide(Input) Decision
}

// AlwaysGPU is policy v0: every build goes to the GPU queue, first come
// first served, unless a live worker exists and none can hold it.
//
// With zero live workers it still queues. An empty pool may be cold-starting
// (Phase 5), and "nothing fits" is a statement about workers we can see, not
// about workers we cannot.
type AlwaysGPU struct{}

func (AlwaysGPU) Name() string { return "always_gpu" }

func (AlwaysGPU) Decide(in Input) Decision {
	if in.Pool.LiveWorkers > 0 && in.Build.MemBytes > in.Pool.LargestMem {
		return Decision{
			Placement: "local",
			Reason: fmt.Sprintf("needs %d bytes, largest live worker has %d",
				in.Build.MemBytes, in.Pool.LargestMem),
		}
	}
	return Decision{Placement: "gpu", Priority: 0, Reason: "always_gpu"}
}

// ByName returns the policy for a config value.
func ByName(name string) (Policy, error) {
	switch name {
	case "", "always_gpu":
		return AlwaysGPU{}, nil
	}
	return nil, fmt.Errorf("unknown policy %q", name)
}
