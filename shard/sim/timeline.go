package sim

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/arqbca11/KubernetesGPU/experiments/workload"
	"github.com/arqbca11/KubernetesGPU/shard/client"
)

// Timeline is the per-round record the roadmap asks for: for each shard, its
// build (placement, attempts, start, end) and every query's arrival, start
// and end; plus the cluster-level view, where a fan-out query's latency is
// that of its slowest piece.
type Timeline struct {
	RoundID         int64         `json:"round_id"`
	Scenario        string        `json:"scenario"`
	Seed            int64         `json:"seed"`
	TimeScale       float64       `json:"time_scale"`
	StartedAt       time.Time     `json:"started_at"`
	Duration        time.Duration `json:"duration"`         // real
	DurationModeled time.Duration `json:"duration_modeled"` // real x time scale
	StragglerLag    time.Duration `json:"straggler_lag"`    // last shard finish minus median shard finish (real)
	BuildsGPU       int           `json:"builds_gpu"`
	BuildsLocal     int           `json:"builds_local"`
	Queries         int           `json:"queries"`
	Shards          []ShardLine   `json:"shards"`
	ClusterQueries  []ClusterLine `json:"cluster_queries"`
	Latency         LatencyStats  `json:"latency"`
}

type ShardLine struct {
	ShardID      int32         `json:"shard_id"`
	NVectors     int64         `json:"n_vectors"`
	Placement    string        `json:"placement"`
	Attempt      int32         `json:"attempt"`
	BuildState   string        `json:"build_state"`
	Submitted    time.Duration `json:"submitted"`   // offsets from round start, real
	BuildStarted time.Duration `json:"build_start"` // last attempt's start (-1 if unknown)
	BuildDone    time.Duration `json:"build_done"`
	Finished     time.Duration `json:"finished"` // when this shard was fully done
	Queries      []QueryLine   `json:"queries"`
	Events       []Event       `json:"events"`
}

type QueryLine struct {
	Seq            int32              `json:"seq"`
	Kind           workload.QueryKind `json:"kind"`
	ClusterQueryID int64              `json:"cluster_query_id"`
	NeedsIndex     bool               `json:"needs_index"`
	Arrived        time.Duration      `json:"arrived"`
	Started        time.Duration      `json:"started"`
	Finished       time.Duration      `json:"finished"`
	Wait           time.Duration      `json:"wait"` // started - arrived
}

type ClusterLine struct {
	ClusterQueryID int64              `json:"cluster_query_id"`
	Kind           workload.QueryKind `json:"kind"`
	NeedsIndex     bool               `json:"needs_index"`
	Pieces         int                `json:"pieces"`
	Arrived        time.Duration      `json:"arrived"`
	Latency        time.Duration      `json:"latency"` // slowest piece's finish - arrival
}

type LatencyStats struct {
	FanOutP50, FanOutP99 time.Duration
	SingleP50, SingleP99 time.Duration
	FanOutN, SingleN     int
}

// BuildTimeline joins the scheduler's view of the round (timestamps) with the
// workload (what each query was) and the shards' own events.
func BuildTimeline(w workload.Workload, st client.RoundStatus, events map[int32][]Event, timeScale float64) *Timeline {
	t0 := st.Round.StartedAt
	off := func(t *time.Time) time.Duration {
		if t == nil {
			return -1
		}
		return t.Sub(t0)
	}
	tl := &Timeline{RoundID: st.Round.RoundID, Scenario: w.Scenario.Name, Seed: w.Seed, TimeScale: timeScale, StartedAt: t0}
	if st.Round.FinishedAt != nil {
		tl.Duration = st.Round.FinishedAt.Sub(t0)
		tl.DurationModeled = time.Duration(float64(tl.Duration) * timeScale)
	}
	builds := map[int32]client.BuildRow{}
	for _, b := range st.Builds {
		builds[b.ShardID] = b
	}
	jobs := map[int32]map[int32]client.JobRow{}
	for _, j := range st.Jobs {
		if jobs[j.ShardID] == nil {
			jobs[j.ShardID] = map[int32]client.JobRow{}
		}
		jobs[j.ShardID][j.Seq] = j
	}
	type piece struct {
		q   workload.Query
		fin time.Duration
	}
	cluster := map[int64][]piece{}
	var finishes []time.Duration
	for _, sw := range w.Shards {
		b := builds[sw.ShardID]
		line := ShardLine{ShardID: sw.ShardID, NVectors: sw.NVectors, Placement: b.Placement, Attempt: b.Attempt, BuildState: b.State,
			Submitted: b.EnqueuedAt.Sub(t0), BuildStarted: off(b.StartedAt), BuildDone: off(b.FinishedAt), Events: events[sw.ShardID]}
		if b.Placement == "gpu" {
			tl.BuildsGPU++
		} else {
			tl.BuildsLocal++
		}
		finished := line.BuildDone
		for seq, q := range sw.Queries {
			j, ok := jobs[sw.ShardID][int32(seq)]
			ql := QueryLine{Seq: int32(seq), Kind: q.Kind, ClusterQueryID: q.ClusterQueryID, NeedsIndex: q.NeedsIndex, Started: -1, Finished: -1, Wait: -1}
			if ok {
				ql.Arrived = j.ArrivedAt.Sub(t0)
				ql.Started, ql.Finished = off(j.StartedAt), off(j.FinishedAt)
				if j.StartedAt != nil {
					ql.Wait = j.StartedAt.Sub(j.ArrivedAt)
				}
				if ql.Finished > finished {
					finished = ql.Finished
				}
				cluster[q.ClusterQueryID] = append(cluster[q.ClusterQueryID], piece{q: q, fin: ql.Finished - ql.Arrived})
			}
			line.Queries = append(line.Queries, ql)
			tl.Queries++
		}
		line.Finished = finished
		finishes = append(finishes, finished)
		tl.Shards = append(tl.Shards, line)
	}
	sort.Slice(finishes, func(i, j int) bool { return finishes[i] < finishes[j] })
	if n := len(finishes); n > 0 {
		tl.StragglerLag = finishes[n-1] - finishes[n/2]
	}
	var fan, single []time.Duration
	for id, ps := range cluster {
		cl := ClusterLine{ClusterQueryID: id, Kind: ps[0].q.Kind, NeedsIndex: ps[0].q.NeedsIndex, Pieces: len(ps), Arrived: ps[0].q.Arrival}
		for _, p := range ps {
			if p.fin > cl.Latency {
				cl.Latency = p.fin
			}
		}
		tl.ClusterQueries = append(tl.ClusterQueries, cl)
		switch cl.Kind {
		case workload.KindFanOut:
			fan = append(fan, cl.Latency)
		case workload.KindSingleShard:
			single = append(single, cl.Latency)
		}
	}
	sort.Slice(tl.ClusterQueries, func(i, j int) bool { return tl.ClusterQueries[i].ClusterQueryID < tl.ClusterQueries[j].ClusterQueryID })
	tl.Latency.FanOutP50, tl.Latency.FanOutP99, tl.Latency.FanOutN = pct(fan, 0.5), pct(fan, 0.99), len(fan)
	tl.Latency.SingleP50, tl.Latency.SingleP99, tl.Latency.SingleN = pct(single, 0.5), pct(single, 0.99), len(single)
	return tl
}

func pct(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	i := int(p * float64(len(xs)-1))
	return xs[i]
}

// Text renders the timeline for a terminal: a summary, one line per shard,
// and a bar chart of shard finish times.
func (tl *Timeline) Text() string {
	var b strings.Builder
	r := func(d time.Duration) string {
		if d < 0 {
			return "-"
		}
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	fmt.Fprintf(&b, "round %d  scenario=%s seed=%d  time_scale=%g\n", tl.RoundID, tl.Scenario, tl.Seed, tl.TimeScale)
	fmt.Fprintf(&b, "duration %s real (%s modeled)   straggler lag %s   builds: %d gpu, %d local   queries: %d\n",
		r(tl.Duration), r(tl.DurationModeled), r(tl.StragglerLag), tl.BuildsGPU, tl.BuildsLocal, tl.Queries)
	fmt.Fprintf(&b, "query latency (slowest piece): fan-out p50 %s p99 %s (n=%d)   single-shard p50 %s p99 %s (n=%d)\n",
		r(tl.Latency.FanOutP50), r(tl.Latency.FanOutP99), tl.Latency.FanOutN, r(tl.Latency.SingleP50), r(tl.Latency.SingleP99), tl.Latency.SingleN)
	fmt.Fprintf(&b, "%-6s %-9s %-6s %-4s %-8s %-8s %-8s %-8s %-7s %s\n", "shard", "n_vectors", "place", "att", "submit", "b.start", "b.done", "finish", "queries", "max wait")
	maxFin := time.Duration(0)
	for _, s := range tl.Shards {
		if s.Finished > maxFin {
			maxFin = s.Finished
		}
	}
	for _, s := range tl.Shards {
		maxWait := time.Duration(0)
		for _, q := range s.Queries {
			if q.Wait > maxWait {
				maxWait = q.Wait
			}
		}
		bar := ""
		if maxFin > 0 && s.Finished > 0 {
			bar = strings.Repeat("#", int(30*float64(s.Finished)/float64(maxFin)))
		}
		fmt.Fprintf(&b, "%-6d %-9d %-6s %-4d %-8s %-8s %-8s %-8s %-7d %-8s %s\n", s.ShardID, s.NVectors, s.Placement, s.Attempt,
			r(s.Submitted), r(s.BuildStarted), r(s.BuildDone), r(s.Finished), len(s.Queries), r(maxWait), bar)
	}
	return b.String()
}
