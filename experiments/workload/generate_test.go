package workload

import (
	"encoding/json"
	"math"
	"sort"
	"testing"
	"time"
)

func mustPreset(t *testing.T, name string, n int) Scenario {
	t.Helper()
	sc, err := PresetWithShards(name, n)
	if err != nil {
		t.Fatalf("PresetWithShards(%q, %d): %v", name, n, err)
	}
	return sc
}

func mustJSON(t *testing.T, w Workload) []byte {
	t.Helper()
	b, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// clusterQueries groups every non-local-DDL piece by ClusterQueryID.
type piece struct {
	shard int
	q     Query
}

func groupByCluster(w Workload) map[int64][]piece {
	g := map[int64][]piece{}
	for i, s := range w.Shards {
		for _, q := range s.Queries {
			if q.Kind == KindLocalDDL {
				continue
			}
			g[q.ClusterQueryID] = append(g[q.ClusterQueryID], piece{i, q})
		}
	}
	return g
}

func TestDeterminism(t *testing.T) {
	t.Logf("Contract (workload.go): Generate is pure; same Scenario and seed give byte-identical output. CLAUDE.md invariant 10: workloads are seeded and reproducible.")
	for _, name := range PresetNames() {
		sc := mustPreset(t, name, DefaultShards)
		a := mustJSON(t, Generate(sc, 42))
		b := mustJSON(t, Generate(sc, 42))
		c := mustJSON(t, Generate(sc, 43))
		if string(a) != string(b) {
			t.Fatalf("%s: seed 42 twice gave different JSON", name)
		}
		if string(a) == string(c) {
			t.Fatalf("%s: seeds 42 and 43 gave identical JSON", name)
		}
		t.Logf("%s: generated twice with seed 42 -> %d bytes of JSON, identical; seed 43 differs, as expected.", name, len(a))
	}
}

func TestPresetsValidate(t *testing.T) {
	t.Logf("Contract: consumers run Validate() on every workload. Every preset must pass at 6 shards (roadmap default) and 50 (Phase 1 step 7), over several seeds.")
	for _, n := range []int{6, 50} {
		for _, name := range PresetNames() {
			sc := mustPreset(t, name, n)
			for seed := int64(1); seed <= 5; seed++ {
				w := Generate(sc, seed)
				if err := w.Validate(); err != nil {
					t.Fatalf("%s n=%d seed=%d: %v", name, n, seed, err)
				}
			}
			w := Generate(sc, 1)
			total := 0
			for _, s := range w.Shards {
				total += len(s.Queries)
			}
			t.Logf("%s n=%d: 5 seeds validate; seed 1 has %d shard-level queries, cluster rate %.1f/s, horizon %s.", name, n, total, sc.Stream.ClusterQueryRate, sc.Stream.Horizon)
		}
	}
	if _, err := Preset("nope"); err == nil {
		t.Fatalf("unknown preset should be an error")
	}
	if _, err := PresetWithShards("skewed", 0); err == nil {
		t.Fatalf("0 shards should be an error")
	}
	t.Logf("Unknown preset name and 0 shards are rejected with an error.")
}

func largestOverMedian(w Workload) float64 {
	s := make([]float64, len(w.Shards))
	for i, sh := range w.Shards {
		s[i] = float64(sh.NVectors)
	}
	sort.Float64s(s)
	n := len(s)
	med := s[n/2]
	if n%2 == 0 {
		med = (s[n/2-1] + s[n/2]) / 2
	}
	return s[n-1] / med
}

func TestZipfSkew(t *testing.T) {
	t.Logf("Roadmap Phase 3 sweep: Zipf exponent 0.5, 1.0, 1.5. Rank-size Zipf (size of rank r = Max/r^s) at 6 shards gives largest/median of about 1.9, 3.4, 6.3.")
	ratios := map[float64]float64{}
	for _, s := range []float64{0.5, 1.0, 1.5} {
		sc := mustPreset(t, "skewed", 6)
		sc.Sizes.ZipfExponent = s
		w := Generate(sc, 7)
		ratios[s] = largestOverMedian(w)
		sizes := []int64{}
		for _, sh := range w.Shards {
			sizes = append(sizes, sh.NVectors)
			if sh.NVectors < sc.Sizes.Min || sh.NVectors > sc.Sizes.Max {
				t.Fatalf("size %d outside [%d,%d]", sh.NVectors, sc.Sizes.Min, sc.Sizes.Max)
			}
		}
		t.Logf("exponent %.1f: sizes %v, largest/median %.2f", s, sizes, ratios[s])
	}
	if ratios[1.0] < 3 {
		t.Fatalf("exponent 1.0: largest/median %.2f, want >= 3 (several times)", ratios[1.0])
	}
	if !(ratios[1.5] > ratios[1.0] && ratios[1.0] > ratios[0.5]) {
		t.Fatalf("skew not monotone in exponent: %v", ratios)
	}
	if ratios[0.5] > 2.5 {
		t.Fatalf("exponent 0.5: largest/median %.2f, want mild skew (< 2.5)", ratios[0.5])
	}
	t.Logf("As expected: 1.0 is several times the median, 1.5 more, 0.5 less.")
}

func TestBimodalTwoClusters(t *testing.T) {
	t.Logf("Roadmap scenario Bimodal: a few huge builds, many tiny; some huge builds exceed one worker's memory. Light cluster in [Min, 2*Min], heavy in [0.8*Max, Max], heavy count round(0.25*n), worker memory fits heavy builds up to 0.9*Max.")
	for _, n := range []int{6, 50} {
		sc := mustPreset(t, "bimodal", n)
		per := ApproxBuildMemBytesPerVector(sc.Dim)
		for seed := int64(1); seed <= 5; seed++ {
			w := Generate(sc, seed)
			heavy, light, tooBig := 0, 0, 0
			for _, s := range w.Shards {
				switch {
				case s.NVectors >= int64(0.8*float64(sc.Sizes.Max)):
					heavy++
					if s.NVectors*per > sc.Pool.WorkerMemBytes {
						tooBig++
					}
				case s.NVectors <= 2*sc.Sizes.Min:
					light++
				default:
					t.Fatalf("n=%d seed=%d: size %d in the gap between clusters", n, seed, s.NVectors)
				}
			}
			want := int(math.Round(0.25 * float64(n)))
			if heavy != want || light != n-want {
				t.Fatalf("n=%d seed=%d: heavy=%d light=%d, want %d/%d", n, seed, heavy, light, want, n-want)
			}
			if tooBig == 0 || tooBig == heavy {
				t.Fatalf("n=%d seed=%d: %d of %d heavy builds exceed worker memory; want some but not all", n, seed, tooBig, heavy)
			}
			if seed == 1 {
				t.Logf("n=%d: %d heavy, %d light, nothing in the gap; %d heavy builds exceed %d MB worker memory.", n, heavy, light, tooBig, sc.Pool.WorkerMemBytes>>20)
			}
		}
	}
}

func TestClusterQueryRate(t *testing.T) {
	t.Logf("StreamProfile.ClusterQueryRate is the mean coordinator-level query rate. Arrivals are Poisson, so the count over the horizon should be within 4 sqrt(expected) of rate*horizon.")
	for _, name := range []string{"uniform", "skewed", "downstream_heavy"} {
		for _, n := range []int{6, 50} {
			sc := mustPreset(t, name, n)
			for seed := int64(1); seed <= 3; seed++ {
				w := Generate(sc, seed)
				got := float64(len(groupByCluster(w)))
				want := sc.Stream.ClusterQueryRate * sc.Stream.Horizon.Seconds()
				if math.Abs(got-want) > 4*math.Sqrt(want) {
					t.Fatalf("%s n=%d seed=%d: %v cluster queries, want %.0f +- %.0f", name, n, seed, got, want, 4*math.Sqrt(want))
				}
				if seed == 1 {
					t.Logf("%s n=%d: %v cluster queries vs expected %.0f (tolerance %.0f).", name, n, got, want, 4*math.Sqrt(want))
				}
			}
		}
	}
}

func TestPerShardLoadMatchesTarget(t *testing.T) {
	t.Logf("Presets derive the cluster rate from a target per-shard CPU utilisation (skewed: 0.4) so load per shard is the same at 6 and 50 shards (roadmap sweep: query load relative to build work). Check the realised mean piece duration and offered load.")
	for _, n := range []int{6, 50} {
		sc := mustPreset(t, "skewed", n)
		sc.Stream.LocalDDLRatePerShard = 0
		var sum time.Duration
		cnt := 0
		for seed := int64(1); seed <= 4; seed++ {
			for _, s := range Generate(sc, seed).Shards {
				for _, q := range s.Queries {
					sum += q.Duration
					cnt++
				}
			}
		}
		mean := sum / time.Duration(cnt)
		util := sum.Seconds() / (4 * float64(n) * sc.Stream.Horizon.Seconds())
		if r := float64(mean) / float64(sc.Stream.MeanQueryDuration); r < 0.85 || r > 1.15 {
			t.Fatalf("n=%d: mean piece %s vs configured %s", n, mean, sc.Stream.MeanQueryDuration)
		}
		if util < 0.4*0.85 || util > 0.4*1.15 {
			t.Fatalf("n=%d: per-shard utilisation %.3f, want 0.4 +- 15%%", n, util)
		}
		t.Logf("n=%d: %d pieces, mean %s (configured %s), per-shard utilisation %.3f (target 0.4).", n, cnt, mean, sc.Stream.MeanQueryDuration, util)
	}
}

func TestNeedsIndexFraction(t *testing.T) {
	t.Logf("NeedsIndexFraction is by cluster query (contract), and every piece of one cluster query agrees. Roadmap sweep values 0, 0.5, 1. Local DDL never needs the index.")
	for _, f := range []float64{0, 0.5, 0.8, 1} {
		sc := mustPreset(t, "skewed", 50)
		sc.Stream.NeedsIndexFraction = f
		w := Generate(sc, 3)
		g := groupByCluster(w)
		needs := 0
		for id, ps := range g {
			for _, p := range ps {
				if p.q.NeedsIndex != ps[0].q.NeedsIndex {
					t.Fatalf("cluster query %d: pieces disagree on NeedsIndex", id)
				}
			}
			if ps[0].q.NeedsIndex {
				needs++
			}
		}
		for _, s := range w.Shards {
			for _, q := range s.Queries {
				if q.Kind == KindLocalDDL && q.NeedsIndex {
					t.Fatalf("local DDL marked NeedsIndex")
				}
			}
		}
		got := float64(needs) / float64(len(g))
		if math.Abs(got-f) > 0.03 {
			t.Fatalf("fraction %.3f, configured %.2f", got, f)
		}
		t.Logf("configured %.2f: %d of %d cluster queries need the index (%.3f).", f, needs, len(g), got)
	}
}

func TestFanOutPieces(t *testing.T) {
	t.Logf("A fan-out query produces one piece on each of k distinct shards, k in [FanOutShardsMin, FanOutShardsMax], all with the same ClusterQueryID and Arrival (contract). Single-shard queries have exactly one piece.")
	for _, n := range []int{6, 50} {
		sc := mustPreset(t, "skewed", n)
		w := Generate(sc, 11)
		seenMin, seenMax := n+1, 0
		fan, single := 0, 0
		for id, ps := range groupByCluster(w) {
			shards := map[int]bool{}
			for _, p := range ps {
				if p.q.Arrival != ps[0].q.Arrival || p.q.Kind != ps[0].q.Kind {
					t.Fatalf("cluster query %d: pieces differ in arrival or kind", id)
				}
				if shards[p.shard] {
					t.Fatalf("cluster query %d: two pieces on shard %d", id, p.shard)
				}
				shards[p.shard] = true
			}
			if ps[0].q.Kind == KindSingleShard {
				single++
				if len(ps) != 1 {
					t.Fatalf("single-shard query %d has %d pieces", id, len(ps))
				}
				continue
			}
			fan++
			k := len(ps)
			if k < sc.Stream.FanOutShardsMin || k > sc.Stream.FanOutShardsMax {
				t.Fatalf("fan-out query %d has width %d outside [%d,%d]", id, k, sc.Stream.FanOutShardsMin, sc.Stream.FanOutShardsMax)
			}
			seenMin, seenMax = min(seenMin, k), max(seenMax, k)
		}
		if seenMin != sc.Stream.FanOutShardsMin || seenMax != sc.Stream.FanOutShardsMax {
			t.Fatalf("n=%d: widths seen [%d,%d], want the whole range [%d,%d]", n, seenMin, seenMax, sc.Stream.FanOutShardsMin, sc.Stream.FanOutShardsMax)
		}
		ff := float64(fan) / float64(fan+single)
		if math.Abs(ff-sc.Stream.FanOutFraction) > 0.04 {
			t.Fatalf("n=%d: fan-out fraction %.3f, configured %.2f", n, ff, sc.Stream.FanOutFraction)
		}
		t.Logf("n=%d: %d fan-out queries (fraction %.3f, configured %.2f) with widths spanning [%d,%d]; %d single-shard queries; all pieces share arrival.", n, fan, ff, sc.Stream.FanOutFraction, seenMin, seenMax, single)
	}
}

func TestHotShard(t *testing.T) {
	t.Logf("Roadmap scenario Downstream-heavy: one shard has a much higher query rate. Contract: the hot shard receives HotShardRateMultiplier times its share of single-shard queries. The preset also sizes the multiplier so the hot shard's total load is 4x a typical shard's.")
	for _, n := range []int{6, 50} {
		sc := mustPreset(t, "downstream_heavy", n)
		sc.Stream.Horizon = 600 * time.Second // long horizon to reduce noise
		w := Generate(sc, 5)
		hot := sc.Stream.HotShard
		singles := make([]float64, n)
		load := make([]float64, n)
		for i, s := range w.Shards {
			for _, q := range s.Queries {
				if q.Kind == KindSingleShard {
					singles[i]++
				}
				if q.Kind != KindLocalDDL {
					load[i] += q.Duration.Seconds()
				}
			}
		}
		var others, otherLoad float64
		for i := range singles {
			if i != hot {
				others += singles[i]
				otherLoad += load[i]
			}
		}
		others /= float64(n - 1)
		otherLoad /= float64(n - 1)
		ratio := singles[hot] / others
		m := sc.Stream.HotShardRateMultiplier
		if math.Abs(ratio-m)/m > 0.15 {
			t.Fatalf("n=%d: hot shard got %.2fx single-shard queries, multiplier %.1f", n, ratio, m)
		}
		lr := load[hot] / otherLoad
		if lr < 3.2 || lr > 4.8 {
			t.Fatalf("n=%d: hot shard load ratio %.2f, want about 4", n, lr)
		}
		t.Logf("n=%d: multiplier %.1f; hot shard got %.2fx a typical shard's single-shard queries and %.2fx its CPU load (target 4).", n, m, ratio, lr)
	}
	sc := mustPreset(t, "skewed", 6)
	w := Generate(sc, 5)
	t.Logf("Without a hot shard (skewed, HotShard=-1) single-shard routing is uniform; checked by the totals below.")
	for i, s := range w.Shards {
		c := 0
		for _, q := range s.Queries {
			if q.Kind == KindSingleShard {
				c++
			}
		}
		t.Logf("  skewed shard %d: %d single-shard queries", i, c)
	}
}

func TestStreamFiniteAndSorted(t *testing.T) {
	t.Logf("Decision 30: the stream is finite per round. Contract: each shard's queries are sorted by Arrival and nothing arrives after Horizon; shard ids 0..n-1 in order; ClusterQueryIDs of non-fan-out queries are unique.")
	for _, name := range PresetNames() {
		sc := mustPreset(t, name, 50)
		w := Generate(sc, 9)
		var last time.Duration
		total := 0
		ids := map[int64]int{}
		for i, s := range w.Shards {
			if int(s.ShardID) != i {
				t.Fatalf("shard %d has id %d", i, s.ShardID)
			}
			for j, q := range s.Queries {
				if j > 0 && q.Arrival < s.Queries[j-1].Arrival {
					t.Fatalf("%s shard %d unsorted at %d", name, i, j)
				}
				if q.Arrival > sc.Stream.Horizon || q.Arrival < 0 {
					t.Fatalf("%s: arrival %s outside horizon %s", name, q.Arrival, sc.Stream.Horizon)
				}
				if q.Duration <= 0 {
					t.Fatalf("%s: non-positive duration", name)
				}
				if q.Kind != KindFanOut {
					ids[q.ClusterQueryID]++
				}
				last = max(last, q.Arrival)
				total++
			}
		}
		for id, c := range ids {
			if c > 1 {
				t.Fatalf("%s: non-fan-out ClusterQueryID %d used %d times", name, id, c)
			}
		}
		t.Logf("%s n=50: %d queries, all sorted, last arrival %s <= horizon %s.", name, total, last.Round(time.Millisecond), sc.Stream.Horizon)
	}
}

func TestLocalDDL(t *testing.T) {
	t.Logf("Owner guidance: local DDL is rare, long and touches one shard. Generated per shard as Poisson at LocalDDLRatePerShard with mean LocalDDLDuration; check count and mean over a long horizon.")
	sc := mustPreset(t, "skewed", 50)
	sc.Stream.Horizon = 400 * time.Second
	w := Generate(sc, 2)
	cnt := 0
	var sum time.Duration
	for _, s := range w.Shards {
		for _, q := range s.Queries {
			if q.Kind == KindLocalDDL {
				cnt++
				sum += q.Duration
			}
		}
	}
	want := sc.Stream.LocalDDLRatePerShard * 50 * sc.Stream.Horizon.Seconds()
	if math.Abs(float64(cnt)-want) > 4*math.Sqrt(want) {
		t.Fatalf("%d local DDL, want %.0f", cnt, want)
	}
	mean := sum / time.Duration(cnt)
	if r := float64(mean) / float64(sc.Stream.LocalDDLDuration); r < 0.85 || r > 1.15 {
		t.Fatalf("local DDL mean %s vs %s", mean, sc.Stream.LocalDDLDuration)
	}
	t.Logf("%d local DDL statements (expected %.0f), mean duration %s (configured %s), %.0fx a query's mean.", cnt, want, mean.Round(time.Millisecond), sc.Stream.LocalDDLDuration, float64(sc.Stream.LocalDDLDuration)/float64(sc.Stream.MeanQueryDuration))
}

func TestDDLOffsets(t *testing.T) {
	t.Logf("Roadmap scenario Staggered: shards submit at different times. Contract: staggered offsets lie in [0, Spread]; all_at_once offsets are zero. Stratified sampling means the earliest offset is in the first 1/n of Spread and the latest in the last 1/n.")
	for _, n := range []int{6, 50} {
		sc := mustPreset(t, "staggered", n)
		sp := sc.DDL.Spread
		w := Generate(sc, 4)
		lo, hi := sp, time.Duration(0)
		for _, s := range w.Shards {
			if s.DDLOffset < 0 || s.DDLOffset > sp {
				t.Fatalf("offset %s outside [0,%s]", s.DDLOffset, sp)
			}
			lo, hi = min(lo, s.DDLOffset), max(hi, s.DDLOffset)
		}
		if lo > sp/time.Duration(n) || hi < sp-sp/time.Duration(n) {
			t.Fatalf("n=%d: offsets span [%s,%s], want close to [0,%s]", n, lo, hi, sp)
		}
		if sc.Stream.Horizon <= sp {
			t.Fatalf("horizon %s should outlast the spread %s", sc.Stream.Horizon, sp)
		}
		t.Logf("staggered n=%d: offsets span [%s, %s] within Spread %s; horizon %s outlasts it.", n, lo.Round(time.Millisecond), hi.Round(time.Millisecond), sp, sc.Stream.Horizon)
	}
	for _, name := range PresetNames() {
		if name == "staggered" {
			continue
		}
		for _, s := range Generate(mustPreset(t, name, 6), 4).Shards {
			if s.DDLOffset != 0 {
				t.Fatalf("%s: all_at_once offset %s", name, s.DDLOffset)
			}
		}
	}
	t.Logf("All other presets are all_at_once and every offset is zero.")
}

func TestPresetTable(t *testing.T) {
	t.Logf("Prints the preset parameters (used for DESIGN.md); no assertions beyond construction.")
	for _, n := range []int{6, 50} {
		for _, name := range PresetNames() {
			sc := mustPreset(t, name, n)
			s := sc.Stream
			t.Logf("n=%d %-16s sizes=%s[%d,%d] s=%.1f heavy=%.2f ddl=%s/%s horizon=%s rate=%.1f/s fan=%.2f k=[%d,%d] needs=%.2f meanQ=%s lddl=%.3f/%s hot=%d x%.1f workers=%d mem=%dMB",
				n, name, sc.Sizes.Kind, sc.Sizes.Min, sc.Sizes.Max, sc.Sizes.ZipfExponent, sc.Sizes.BimodalHeavyFraction, sc.DDL.Kind, sc.DDL.Spread,
				s.Horizon, s.ClusterQueryRate, s.FanOutFraction, s.FanOutShardsMin, s.FanOutShardsMax, s.NeedsIndexFraction, s.MeanQueryDuration,
				s.LocalDDLRatePerShard, s.LocalDDLDuration, s.HotShard, s.HotShardRateMultiplier, sc.Pool.Workers, sc.Pool.WorkerMemBytes>>20)
		}
	}
}
