package workload

import (
	"fmt"
	"time"
)

// DefaultPreset is the roadmap's default scenario.
const DefaultPreset = "skewed"

// DefaultShards is the project's starting scale (roadmap, Modeling
// assumptions 8). Phase 1 step 7 reruns the same scenarios at 50.
const DefaultShards = 6

// Shared preset constants; DESIGN.md explains each.
const (
	presetDim             = 128
	presetMeanQuery       = 20 * time.Millisecond
	presetFanOutFraction  = 0.30
	presetFanOutMin       = 2
	presetNeedsIndex      = 0.5
	presetLocalDDLRate    = 0.005 // per shard per second: ~one per round across 6 shards
	presetLocalDDLMean    = 2 * time.Second
	presetHorizon         = 30 * time.Second
	presetStaggerHorizon  = 40 * time.Second
	presetStaggerSpread   = 15 * time.Second
	presetBigWorkerMem    = 16 << 30 // effectively unbounded for these sizes
	presetHotShard        = 0
	presetHotLoadRatio    = 4.0  // hot shard's offered load / a typical shard's
	presetHotFanOut       = 0.15 // tenant-routed workload: fewer scatter-gathers
	presetHotNeedsIndex   = 0.8
	presetBimodalHeavy    = 0.25
	presetBimodalMemPoint = 0.9 // worker memory fits a heavy build up to 0.9*Max
	// approxGraphBytesPerVector is the HNSW layer-0 graph per vector at M=16
	// (2*M links of 4 bytes). With float32 vectors that is 128*4 + 128 = 640 B
	// per vector at dim 128.
	approxGraphBytesPerVector = 2 * 16 * 4
)

// PresetNames lists the six roadmap scenarios, in roadmap order.
func PresetNames() []string {
	return []string{"uniform", "skewed", "bimodal", "downstream_heavy", "staggered", "shrinking_pool"}
}

// Preset returns a named scenario at DefaultShards shards.
func Preset(name string) (Scenario, error) {
	return PresetWithShards(name, DefaultShards)
}

// PresetWithShards returns a named scenario at n shards. Query rates are
// derived from a target per-shard CPU utilisation, so per-shard load stays
// the same when n changes; the fan-out width upper bound is n (a full
// scatter-gather).
func PresetWithShards(name string, n int) (Scenario, error) {
	if n < 1 {
		return Scenario{}, fmt.Errorf("workload: preset %q needs at least 1 shard, got %d", name, n)
	}
	sc := Scenario{
		Name:    name,
		NShards: n,
		Dim:     presetDim,
		DDL:     ArrivalPattern{Kind: "all_at_once"},
		Pool:    PoolConfig{Workers: defaultWorkers(n), WorkerMemBytes: presetBigWorkerMem},
		Stream: StreamProfile{
			Horizon:              presetHorizon,
			FanOutFraction:       presetFanOutFraction,
			FanOutShardsMin:      min(presetFanOutMin, n),
			FanOutShardsMax:      n,
			NeedsIndexFraction:   presetNeedsIndex,
			MeanQueryDuration:    presetMeanQuery,
			LocalDDLRatePerShard: presetLocalDDLRate,
			LocalDDLDuration:     presetLocalDDLMean,
			HotShard:             -1,
		},
	}
	switch name {
	case "uniform":
		// Equal sizes, light load: pure queueing on an undersized pool.
		sc.Sizes = SizeDist{Kind: "uniform", Min: 100_000, Max: 100_000}
		sc.Stream.ClusterQueryRate = rateForUtil(0.15, sc.Stream, n)
	case "skewed", "shrinking_pool":
		// Zipf sizes, moderate load, mixed needs_index. shrinking_pool uses
		// the same workload: the worker loss is injected by the runner at
		// runtime, not described by the workload.
		sc.Sizes = SizeDist{Kind: "zipf", Min: 10_000, Max: 200_000, ZipfExponent: 1.0}
		sc.Stream.ClusterQueryRate = rateForUtil(0.4, sc.Stream, n)
	case "bimodal":
		// A quarter of shards huge, the rest tiny; worker memory set so the
		// upper half of the heavy cluster does not fit one worker.
		sc.Sizes = SizeDist{Kind: "bimodal", Min: 10_000, Max: 400_000, BimodalHeavyFraction: presetBimodalHeavy}
		sc.Stream.ClusterQueryRate = rateForUtil(0.4, sc.Stream, n)
		sc.Pool.WorkerMemBytes = int64(presetBimodalMemPoint * float64(sc.Sizes.Max) * float64(ApproxBuildMemBytesPerVector(presetDim)))
	case "downstream_heavy":
		// Medium builds; one hot shard (a hot tenant under tenant-key routing)
		// carries presetHotLoadRatio times a typical shard's load, and most
		// queries need the index.
		sc.Sizes = SizeDist{Kind: "uniform", Min: 80_000, Max: 120_000}
		sc.Stream.FanOutFraction = presetHotFanOut
		sc.Stream.NeedsIndexFraction = presetHotNeedsIndex
		sc.Stream.HotShard = presetHotShard
		sc.Stream.HotShardRateMultiplier = hotMultiplier(presetHotLoadRatio, sc.Stream, n)
		sc.Stream.ClusterQueryRate = rateForUtilHot(0.15, sc.Stream, n)
	case "staggered":
		// Skewed workload, shards receive the DDL spread over 15 s.
		sc.Sizes = SizeDist{Kind: "zipf", Min: 10_000, Max: 200_000, ZipfExponent: 1.0}
		sc.Stream.Horizon = presetStaggerHorizon
		sc.Stream.ClusterQueryRate = rateForUtil(0.4, sc.Stream, n)
		sc.DDL = ArrivalPattern{Kind: "staggered", Spread: presetStaggerSpread}
	default:
		return Scenario{}, fmt.Errorf("workload: unknown preset %q (known: %v)", name, PresetNames())
	}
	return sc, nil
}

// ApproxBuildMemBytesPerVector is the generator's own memory estimate used
// only to place the bimodal preset's WorkerMemBytes: float32 vectors plus an
// HNSW layer-0 graph at M=16. The scheduler's cost model (b x n x dim) is
// configured elsewhere; see DESIGN.md, open questions.
func ApproxBuildMemBytesPerVector(dim int32) int64 {
	return int64(dim)*4 + approxGraphBytesPerVector
}

func defaultWorkers(n int) int {
	if n >= 50 {
		return 3 // roadmap target: 50 shards, 3 workers
	}
	return 2 // roadmap default: 6 shards, 2 workers
}

// meanWidth is the expected fan-out width under the generator's uniform
// width distribution, after clamping to [1, n].
func meanWidth(p StreamProfile, n int) float64 {
	kmin, kmax := max(1, min(p.FanOutShardsMin, n)), min(p.FanOutShardsMax, n)
	if kmax < kmin {
		kmax = kmin
	}
	return float64(kmin+kmax) / 2
}

// rateForUtil returns the cluster query rate that gives each shard a mean
// CPU utilisation of util from queries (local DDL excluded): every cluster
// query yields (1-f) + f*E[k] pieces, spread evenly over n shards.
func rateForUtil(util float64, p StreamProfile, n int) float64 {
	piecesPerQuery := (1 - p.FanOutFraction) + p.FanOutFraction*meanWidth(p, n)
	return util * float64(n) / (p.MeanQueryDuration.Seconds() * piecesPerQuery)
}

// hotMultiplier solves for the single-shard weight M that makes the hot
// shard's piece rate ratio times a typical shard's. With a = 1-f and
// c = f*E[k]/n (fan-out pieces per shard per cluster query):
// hot = a*M/(n-1+M) + c, typical = a/(n-1+M) + c; hot = ratio*typical gives
// M = (ratio + K(n-1)) / (1-K) with K = (ratio-1)c/a.
func hotMultiplier(ratio float64, p StreamProfile, n int) float64 {
	if n == 1 {
		return 1
	}
	a := 1 - p.FanOutFraction
	c := p.FanOutFraction * meanWidth(p, n) / float64(n)
	k := (ratio - 1) * c / a
	if k >= 0.95 {
		k = 0.95 // ratio unreachable with this fan-out mix; cap rather than go negative
	}
	m := (ratio + k*float64(n-1)) / (1 - k)
	return float64(int(m*10+0.5)) / 10
}

// rateForUtilHot is rateForUtil for a typical (non-hot) shard when one shard
// has single-shard weight M.
func rateForUtilHot(util float64, p StreamProfile, n int) float64 {
	m := p.HotShardRateMultiplier
	a := 1 - p.FanOutFraction
	perShardPerQuery := a/(float64(n-1)+m) + p.FanOutFraction*meanWidth(p, n)/float64(n)
	return util / (p.MeanQueryDuration.Seconds() * perShardPerQuery)
}
