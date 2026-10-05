// Package workload is the contract between the workload generator and its
// consumers: the shard simulator (Phase 1), the discrete-event simulator and
// the clairvoyant oracle policy (Phase 3).
//
// A Workload is the complete, seeded, finite description of one round: when
// each shard receives its build (the DDL), how big that build is, and every
// query that will arrive at each shard, with its arrival offset, CPU cost,
// whether it needs the new index, and which cluster-level query it belongs to.
//
// Generate is pure: the same Scenario and seed give byte-identical output.
// Consumers never generate anything themselves; they only replay.
//
// The types in this file are the contract and are owned by the project. The
// generator (generate.go, presets.go) is written separately, from the roadmap
// and the modeling assumptions, so that nothing about the policies can leak
// into the workload.
package workload

import (
	"fmt"
	"sort"
	"time"
)

// QueryKind says where a shard-level query came from. In a distributed
// database most work at a shard is a piece of a larger query that the query
// coordinator split across shards; the pieces of one such query arrive at
// their shards at the same moment and the query is complete only when its
// slowest piece is. Some queries are routed to a single shard by shard key.
// Some are DDL that touches only one shard (a local index, say).
type QueryKind string

const (
	// KindFanOut is one piece of a coordinator query that was split across
	// several shards (scatter-gather read, cross-shard aggregate or join leg).
	// All pieces share a ClusterQueryID and an Arrival.
	KindFanOut QueryKind = "fanout"
	// KindSingleShard is a query routed to exactly this shard by its shard key
	// (point lookup, single-row DML). Its ClusterQueryID is unique to it.
	KindSingleShard QueryKind = "single"
	// KindLocalDDL is a schema or maintenance operation on this shard alone
	// (a local index, statistics, a partition operation). Typically long, rare.
	KindLocalDDL QueryKind = "local_ddl"
)

// Query is one unit of CPU work arriving at one shard.
type Query struct {
	// Arrival is the offset from round start at which the query reaches the shard.
	Arrival time.Duration
	// Duration is the modeled CPU time the shard spends on it once it runs.
	Duration time.Duration
	// NeedsIndex is true if the query cannot run until the new index exists
	// on this shard (a vector search); false if it is independent of the build.
	NeedsIndex bool
	// Kind is where the query came from (see QueryKind).
	Kind QueryKind
	// ClusterQueryID groups the pieces of one coordinator-level query. Pieces
	// of the same fan-out query on different shards share it; single-shard
	// queries and local DDL have their own. Unique within a Workload.
	ClusterQueryID int64
}

// ShardWorkload is everything one shard will do in the round.
type ShardWorkload struct {
	ShardID int32
	// DDLOffset is when this shard receives the index build, from round start.
	// Zero for the all-at-once arrival pattern; staggered patterns spread it.
	DDLOffset time.Duration
	// NVectors is the size of this shard's index build. Dim is per Scenario.
	NVectors int64
	// Queries is the shard's finite query stream, sorted by Arrival.
	Queries []Query
}

// Workload is one round's complete, seeded description.
type Workload struct {
	Scenario Scenario
	Seed     int64
	Shards   []ShardWorkload // one per shard, ShardID 0..NShards-1, in order
}

// Scenario is a named combination of the four knobs from the roadmap: size
// distribution, query stream profile, DDL arrival pattern, pool configuration.
// Presets (presets.go) populate it; callers may also build one by hand.
type Scenario struct {
	Name    string
	NShards int
	Dim     int32 // vector dimension, same for every shard in the round

	Sizes  SizeDist
	Stream StreamProfile
	DDL    ArrivalPattern

	// Pool is advisory for the experiment runner (how many workers to run and
	// their memory); the generator does not use it.
	Pool PoolConfig
}

// SizeDist describes how NVectors is distributed across shards.
type SizeDist struct {
	Kind string // "uniform", "zipf", "bimodal"
	// Min and Max bound NVectors for every shard.
	Min, Max int64
	// ZipfExponent applies to "zipf" (roadmap sweep: 0.5, 1.0, 1.5).
	ZipfExponent float64
	// BimodalHeavyFraction applies to "bimodal": the fraction of shards that
	// get a build near Max; the rest get builds near Min.
	BimodalHeavyFraction float64
}

// StreamProfile describes the queries arriving at the shards during the round.
// The generator decides the processes behind these numbers and documents them.
type StreamProfile struct {
	// Horizon is how long queries keep arriving after round start. The stream
	// is finite: nothing arrives after Horizon (decision 30).
	Horizon time.Duration
	// ClusterQueryRate is the mean rate of coordinator-level queries per second
	// across the whole cluster. Each becomes one or more shard-level pieces.
	ClusterQueryRate float64
	// FanOutFraction is the fraction of coordinator queries that are split
	// across several shards; the rest are routed to a single shard.
	FanOutFraction float64
	// FanOutShardsMin/Max bound how many shards a fan-out query touches.
	FanOutShardsMin, FanOutShardsMax int
	// NeedsIndexFraction is the fraction of queries (by cluster query) that
	// need the new index (roadmap sweep: 0, 0.5, 1).
	NeedsIndexFraction float64
	// MeanQueryDuration is the mean CPU time of one shard-level piece.
	MeanQueryDuration time.Duration
	// LocalDDLRatePerShard is the mean rate of single-shard DDL per shard per
	// second; LocalDDLDuration its mean duration. Zero disables.
	LocalDDLRatePerShard float64
	LocalDDLDuration     time.Duration
	// HotShard, if >= 0, is a shard that receives HotShardRateMultiplier times
	// its share of single-shard queries (the "downstream-heavy" scenario).
	HotShard               int
	HotShardRateMultiplier float64
}

// ArrivalPattern describes when shards receive the DDL.
type ArrivalPattern struct {
	Kind string // "all_at_once", "staggered"
	// Spread applies to "staggered": DDLOffsets are spread over [0, Spread].
	Spread time.Duration
}

// PoolConfig is what the experiment runner should run; advisory.
type PoolConfig struct {
	Workers        int
	WorkerMemBytes int64
}

// Validate checks the structural promises consumers rely on: one shard per
// id in order, every stream sorted by arrival and within the horizon,
// non-negative durations, consistent fan-out groups.
func (w Workload) Validate() error {
	if len(w.Shards) != w.Scenario.NShards {
		return fmt.Errorf("workload has %d shards, scenario says %d", len(w.Shards), w.Scenario.NShards)
	}
	groups := map[int64]time.Duration{}
	for i, s := range w.Shards {
		if int(s.ShardID) != i {
			return fmt.Errorf("shard %d has id %d; ids must be 0..n-1 in order", i, s.ShardID)
		}
		if s.NVectors <= 0 {
			return fmt.Errorf("shard %d: NVectors %d must be positive", i, s.NVectors)
		}
		if s.DDLOffset < 0 {
			return fmt.Errorf("shard %d: negative DDLOffset", i)
		}
		if !sort.SliceIsSorted(s.Queries, func(a, b int) bool { return s.Queries[a].Arrival < s.Queries[b].Arrival }) {
			return fmt.Errorf("shard %d: queries not sorted by arrival", i)
		}
		for j, q := range s.Queries {
			if q.Arrival < 0 || q.Arrival > w.Scenario.Stream.Horizon {
				return fmt.Errorf("shard %d query %d: arrival %s outside [0, %s]", i, j, q.Arrival, w.Scenario.Stream.Horizon)
			}
			if q.Duration < 0 {
				return fmt.Errorf("shard %d query %d: negative duration", i, j)
			}
			switch q.Kind {
			case KindFanOut, KindSingleShard, KindLocalDDL:
			default:
				return fmt.Errorf("shard %d query %d: unknown kind %q", i, j, q.Kind)
			}
			if a, seen := groups[q.ClusterQueryID]; seen {
				if q.Kind != KindFanOut {
					return fmt.Errorf("shard %d query %d: ClusterQueryID %d reused by a non-fanout query", i, j, q.ClusterQueryID)
				}
				if a != q.Arrival {
					return fmt.Errorf("cluster query %d: pieces arrive at different times (%s vs %s)", q.ClusterQueryID, a, q.Arrival)
				}
			} else {
				groups[q.ClusterQueryID] = q.Arrival
			}
		}
	}
	return nil
}
