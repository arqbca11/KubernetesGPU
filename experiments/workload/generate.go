package workload

import (
	"math"
	"math/rand/v2"
	"sort"
	"time"
)

// Modeling constants that the Scenario contract does not expose. Each is
// justified in DESIGN.md; changing one changes every workload.
const (
	// pieceSigma is the log-space standard deviation of a shard-level piece's
	// CPU duration (log-normal; coefficient of variation about 1.3).
	pieceSigma = 1.0
	// sharedSigma is the part of pieceSigma shared by all pieces of one
	// fan-out query (same plan, same predicate); the rest is per piece (data
	// on that shard). sharedSigma^2 + perPieceSigma^2 = pieceSigma^2, so a
	// piece's marginal distribution is the same whether it is a fan-out piece
	// or a single-shard query.
	sharedSigma   = 0.8
	perPieceSigma = 0.6
	// pieceCapFactor truncates the log-normal tail at this multiple of the
	// mean (probability of hitting it is about 2e-4 at sigma 1).
	pieceCapFactor = 20.0
	// localDDLSigma and localDDLCapFactor shape local DDL durations: long but
	// less variable than queries, since they scan the whole local table.
	localDDLSigma     = 0.5
	localDDLCapFactor = 5.0
	// minDuration keeps every modeled duration strictly positive.
	minDuration = time.Microsecond
	// Bimodal clusters: heavy builds in [bimodalHeavyLow*Max, Max], light
	// builds in [Min, bimodalLightHigh*Min] (clamped to Max).
	bimodalHeavyLow  = 0.8
	bimodalLightHigh = 2.0
)

// Generate builds one round's workload from a scenario and a seed. It is pure
// and deterministic: no clock, no global random source, no I/O. The same
// (sc, seed) always gives byte-identical output.
//
// Draw order (fixed, so that determinism survives refactoring as long as it
// is kept): sizes, DDL offsets, the cluster query stream, then local DDL shard
// by shard. ClusterQueryIDs are assigned 1, 2, 3, ... in generation order.
func Generate(sc Scenario, seed int64) Workload {
	rng := rand.New(rand.NewPCG(uint64(seed), 0x6b7a_6770_7573_6875))
	n := sc.NShards
	if n < 0 {
		n = 0
	}
	w := Workload{Scenario: sc, Seed: seed, Shards: make([]ShardWorkload, n)}
	if n == 0 {
		return w
	}

	sizes := genSizes(sc.Sizes, n, rng)
	offsets := genDDLOffsets(sc.DDL, n, rng)
	for i := range w.Shards {
		w.Shards[i] = ShardWorkload{ShardID: int32(i), DDLOffset: offsets[i], NVectors: sizes[i]}
	}

	nextID := int64(1)
	genClusterStream(sc.Stream, n, rng, w.Shards, &nextID)
	genLocalDDL(sc.Stream, rng, w.Shards, &nextID)

	for i := range w.Shards {
		q := w.Shards[i].Queries
		sort.SliceStable(q, func(a, b int) bool { return q[a].Arrival < q[b].Arrival })
	}
	return w
}

// genSizes returns NVectors per shard, always within [Min, Max] and >= 1.
func genSizes(d SizeDist, n int, rng *rand.Rand) []int64 {
	lo, hi := d.Min, d.Max
	if lo < 1 {
		lo = 1
	}
	if hi < lo {
		hi = lo
	}
	out := make([]int64, n)
	switch d.Kind {
	case "zipf":
		// Rank-size Zipf: the shard of rank r gets Max / r^s, clamped at Min.
		// Ranks are assigned to shards by a seeded permutation, so the largest
		// shard is not always shard 0.
		s := d.ZipfExponent
		perm := rng.Perm(n)
		for i := 0; i < n; i++ {
			r := float64(perm[i] + 1)
			out[i] = clampSize(int64(math.Round(float64(hi)/math.Pow(r, s))), lo, hi)
		}
	case "bimodal":
		heavy := int(math.Round(d.BimodalHeavyFraction * float64(n)))
		if d.BimodalHeavyFraction > 0 && heavy == 0 {
			heavy = 1
		}
		if heavy > n {
			heavy = n
		}
		heavyLo := int64(math.Round(bimodalHeavyLow * float64(hi)))
		if heavyLo < lo {
			heavyLo = lo
		}
		lightHi := int64(math.Round(bimodalLightHigh * float64(lo)))
		if lightHi > hi {
			lightHi = hi
		}
		// perm[i] < heavy marks shard i heavy. Heavy sizes are stratified:
		// the j-th heavy shard draws from the j-th of `heavy` equal slots of
		// [heavyLo, hi], so the heavy cluster always spans its range.
		perm := rng.Perm(n)
		for i := 0; i < n; i++ {
			if j := perm[i]; j < heavy {
				span := float64(hi - heavyLo)
				v := float64(heavyLo) + span*(float64(j)+rng.Float64())/float64(heavy)
				out[i] = clampSize(int64(math.Round(v)), heavyLo, hi)
			} else {
				out[i] = lo + rng.Int64N(lightHi-lo+1)
			}
		}
	default: // "uniform" and anything unrecognised
		for i := 0; i < n; i++ {
			out[i] = lo + rng.Int64N(hi-lo+1)
		}
	}
	return out
}

func clampSize(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// genDDLOffsets: all zero for "all_at_once"; for "staggered", stratified
// uniform over [0, Spread]: shard order is a seeded permutation, and the
// shard in slot j draws its offset uniformly from [j*Spread/n, (j+1)*Spread/n].
func genDDLOffsets(p ArrivalPattern, n int, rng *rand.Rand) []time.Duration {
	out := make([]time.Duration, n)
	if p.Kind != "staggered" || p.Spread <= 0 {
		return out
	}
	perm := rng.Perm(n)
	for i := 0; i < n; i++ {
		f := (float64(perm[i]) + rng.Float64()) / float64(n)
		out[i] = time.Duration(f * float64(p.Spread))
		if out[i] > p.Spread {
			out[i] = p.Spread
		}
	}
	return out
}

// genClusterStream draws coordinator-level queries as a Poisson process at
// ClusterQueryRate over [0, Horizon] and splits each into shard pieces.
func genClusterStream(p StreamProfile, n int, rng *rand.Rand, shards []ShardWorkload, nextID *int64) {
	if p.ClusterQueryRate <= 0 || p.Horizon <= 0 {
		return
	}
	kmin, kmax := p.FanOutShardsMin, p.FanOutShardsMax
	if kmin < 1 {
		kmin = 1
	}
	if kmin > n {
		kmin = n
	}
	if kmax > n {
		kmax = n
	}
	if kmax < kmin {
		kmax = kmin
	}
	hot := p.HotShard
	mult := p.HotShardRateMultiplier
	if hot < 0 || hot >= n || mult <= 0 {
		hot = -1
	}
	// scratch is a permutation of shard ids; a partial Fisher-Yates over it
	// picks k distinct shards per fan-out query in O(k).
	scratch := make([]int, n)
	for i := range scratch {
		scratch[i] = i
	}
	mean := p.MeanQueryDuration
	mu := logMu(mean, pieceSigma)
	horizon := p.Horizon.Seconds()

	t := 0.0
	for {
		t += rng.ExpFloat64() / p.ClusterQueryRate
		if t > horizon {
			return
		}
		arrival := time.Duration(t * 1e9)
		if arrival > p.Horizon {
			return
		}
		id := *nextID
		*nextID++
		needs := rng.Float64() < p.NeedsIndexFraction
		shared := rng.NormFloat64()
		if rng.Float64() < p.FanOutFraction {
			k := kmin + rng.IntN(kmax-kmin+1)
			for j := 0; j < k; j++ {
				x := j + rng.IntN(n-j)
				scratch[j], scratch[x] = scratch[x], scratch[j]
				d := logNormalDur(mu, sharedSigma*shared+perPieceSigma*rng.NormFloat64(), mean, pieceCapFactor)
				s := scratch[j]
				shards[s].Queries = append(shards[s].Queries, Query{
					Arrival: arrival, Duration: d, NeedsIndex: needs, Kind: KindFanOut, ClusterQueryID: id,
				})
			}
		} else {
			s := pickSingleShard(n, hot, mult, rng)
			d := logNormalDur(mu, sharedSigma*shared+perPieceSigma*rng.NormFloat64(), mean, pieceCapFactor)
			shards[s].Queries = append(shards[s].Queries, Query{
				Arrival: arrival, Duration: d, NeedsIndex: needs, Kind: KindSingleShard, ClusterQueryID: id,
			})
		}
	}
}

// pickSingleShard routes a single-shard query: every shard has weight 1,
// the hot shard (if any) has weight mult. Hash sharding spreads keys evenly,
// so without a hot shard the routing is uniform.
func pickSingleShard(n, hot int, mult float64, rng *rand.Rand) int {
	if hot < 0 {
		return rng.IntN(n)
	}
	u := rng.Float64() * (float64(n-1) + mult)
	if u < mult || n == 1 {
		return hot
	}
	s := int(u - mult)
	if s >= n-1 {
		s = n - 2
	}
	if s >= hot {
		s++
	}
	return s
}

// genLocalDDL draws each shard's local DDL as an independent Poisson process.
func genLocalDDL(p StreamProfile, rng *rand.Rand, shards []ShardWorkload, nextID *int64) {
	if p.LocalDDLRatePerShard <= 0 || p.LocalDDLDuration <= 0 || p.Horizon <= 0 {
		return
	}
	mu := logMu(p.LocalDDLDuration, localDDLSigma)
	horizon := p.Horizon.Seconds()
	for i := range shards {
		t := 0.0
		for {
			t += rng.ExpFloat64() / p.LocalDDLRatePerShard
			if t > horizon {
				break
			}
			arrival := time.Duration(t * 1e9)
			if arrival > p.Horizon {
				break
			}
			d := logNormalDur(mu, localDDLSigma*rng.NormFloat64(), p.LocalDDLDuration, localDDLCapFactor)
			shards[i].Queries = append(shards[i].Queries, Query{
				Arrival: arrival, Duration: d, NeedsIndex: false, Kind: KindLocalDDL, ClusterQueryID: *nextID,
			})
			*nextID++
		}
	}
}

// logMu is the log-space location giving a log-normal with the given mean.
func logMu(mean time.Duration, sigma float64) float64 {
	m := float64(mean)
	if m <= 0 {
		m = float64(minDuration)
	}
	return math.Log(m) - sigma*sigma/2
}

// logNormalDur returns exp(mu + z) as a Duration, capped at capFactor*mean
// and floored at minDuration. z already carries the sigma.
func logNormalDur(mu, z float64, mean time.Duration, capFactor float64) time.Duration {
	v := math.Exp(mu + z)
	if c := capFactor * float64(mean); c > 0 && v > c {
		v = c
	}
	d := time.Duration(v)
	if d < minDuration {
		d = minDuration
	}
	return d
}
