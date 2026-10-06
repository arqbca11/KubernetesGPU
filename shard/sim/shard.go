// Package sim runs simulated shards against the real scheduler.
//
// One Shard is one goroutine group: an arrivals goroutine that replays the
// shard's query stream, a CPU goroutine that runs the local build or the
// oldest ready query (one thing at a time), and a report goroutine that
// tells the scheduler the shard's load every poll interval and acts on the
// reply (decisions 29, 31, 32 in docs/design/phase1-scheduler.md).
//
// Every modeled duration (build time, query duration, arrival offset) is
// divided by Config.TimeScale, the same knob as the worker's FAKE_TIME_SCALE.
package sim

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/arqbca11/KubernetesGPU/experiments/workload"
	"github.com/arqbca11/KubernetesGPU/shard/client"
)

type Config struct {
	TimeScale    float64       // divide every modeled duration by this (1 = real time)
	PollInterval time.Duration // how often the shard reports and syncs with the scheduler
	Slice        time.Duration // granularity at which a local build checks for cancellation
}

func (c Config) scale(d time.Duration) time.Duration {
	if c.TimeScale <= 0 {
		return d
	}
	return time.Duration(float64(d) / c.TimeScale)
}

// Event is one thing that happened on the shard, for the timeline.
type Event struct {
	At  time.Duration `json:"at"` // since round start, real time
	Msg string        `json:"msg"`
}

type queued struct {
	seq     int32
	q       workload.Query
	arrived time.Time
}

type buildState struct {
	id         string
	placement  string
	state      string
	attempt    int32
	cpuBuild   time.Duration // scaled local build duration
	localWant  bool          // the scheduler says: build locally, and we have not started
	localBusy  bool          // the CPU goroutine is building right now
	cancel     bool          // abort the local build at the next slice
	progress   float32
	indexReady bool
	failed     bool
}

type Shard struct {
	w     workload.ShardWorkload
	dim   int32
	round int64
	api   *client.Client
	cfg   Config
	log   *slog.Logger
	start time.Time

	mu         sync.Mutex
	b          buildState
	queue      []*queued
	nextSeq    int32
	streamDone bool
	events     []Event
	finished   bool
	err        error
}

func NewShard(w workload.ShardWorkload, dim int32, round int64, api *client.Client, cfg Config, log *slog.Logger) *Shard {
	return &Shard{w: w, dim: dim, round: round, api: api, cfg: cfg,
		log: log.With("shard_id", w.ShardID, "build_id", fmt.Sprintf("%d:%d", w.ShardID, round))} // round_id comes from the caller's logger
}

// Events returns the shard's timeline events (after Run).
func (s *Shard) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

func (s *Shard) event(f string, a ...any) {
	s.events = append(s.events, Event{At: time.Since(s.start), Msg: fmt.Sprintf(f, a...)})
}

// Run plays the shard's round: wait for its DDL, submit, then run arrivals,
// CPU and reports until the build is terminal, the stream is exhausted and
// the queue is empty. Returns when the final stream_done report is accepted.
func (s *Shard) Run(ctx context.Context, start time.Time) error {
	s.start = start
	if d := s.cfg.scale(s.w.DDLOffset); d > 0 {
		select {
		case <-time.After(time.Until(start.Add(d))):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	resp, err := s.api.Submit(ctx, client.SubmitBuildRequest{RoundID: s.round, ShardID: s.w.ShardID, NVectors: s.w.NVectors, Dim: s.dim})
	if err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	s.mu.Lock()
	s.b = buildState{id: resp.BuildID, placement: resp.Placement, cpuBuild: s.cfg.scale(time.Duration(resp.CPUBuildMs) * time.Millisecond)}
	if resp.Placement == "local" {
		s.b.state, s.b.localWant = "running", true
	} else {
		s.b.state = "queued"
	}
	s.event("submitted: placement=%s (%s); cpu_build=%s gpu_total=%s (modeled)", resp.Placement, resp.Reason,
		time.Duration(resp.CPUBuildMs)*time.Millisecond, time.Duration(resp.GPUTotalMs)*time.Millisecond)
	s.mu.Unlock()
	s.log.Info("submitted", "placement", resp.Placement, "reason", resp.Reason, "n_vectors", s.w.NVectors, "attempt", 0)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); s.arrivals(ctx) }()
	go func() { defer wg.Done(); s.cpu(ctx) }()
	go func() { defer wg.Done(); s.reports(ctx) }()
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// ---- arrivals ------------------------------------------------------------------

func (s *Shard) arrivals(ctx context.Context) {
	// Queries that arrive before this shard's DDL belong to the pre-DDL steady
	// state. Phase 1 drops them (the scheduler does not know the shard yet, so
	// it would refuse the arrival); the staggered scenario in Phase 3 needs a
	// better answer (open question in the design doc).
	skipped := 0
	for _, q := range s.w.Queries {
		if q.Arrival < s.w.DDLOffset {
			skipped++
			continue
		}
		at := s.start.Add(s.cfg.scale(q.Arrival))
		select {
		case <-time.After(time.Until(at)):
		case <-ctx.Done():
			return
		}
		// Record the arrival with the scheduler BEFORE making the query runnable:
		// otherwise the CPU goroutine can start it and hit "no such job".
		s.mu.Lock()
		seq := s.nextSeq
		s.nextSeq++
		s.mu.Unlock()
		arrived := time.Now()
		if err := s.api.Arrival(ctx, s.round, s.w.ShardID, client.ArrivalRequest{
			Seq: seq, DurationMs: q.Duration.Milliseconds(), NeedsIndex: q.NeedsIndex}); err != nil {
			s.fail(fmt.Errorf("record arrival seq %d: %w", seq, err))
			return
		}
		s.mu.Lock()
		s.queue = append(s.queue, &queued{seq: seq, q: q, arrived: arrived})
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.streamDone = true
	s.event("stream exhausted: %d queries arrived (%d before the DDL were not replayed)", len(s.w.Queries)-skipped, skipped)
	s.mu.Unlock()
}

// ---- CPU -------------------------------------------------------------------------

// cpu runs one thing at a time: the local build if the scheduler wants one,
// else the oldest ready query. A query is ready if it does not need the
// index or the index is built (decision 32).
func (s *Shard) cpu(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		s.mu.Lock()
		if s.finished || s.err != nil {
			s.mu.Unlock()
			return
		}
		if s.b.localWant && !s.b.localBusy {
			s.b.localWant, s.b.localBusy, s.b.cancel, s.b.progress = false, true, false, 0
			dur := s.b.cpuBuild
			s.event("local build started (%s scaled)", dur.Round(time.Millisecond))
			s.mu.Unlock()
			s.runLocalBuild(ctx, dur)
			continue
		}
		idx := s.nextReady()
		if idx < 0 {
			done := s.streamDone && len(s.queue) == 0 && (s.b.indexReady || s.b.failed || s.b.state == "done")
			if done && !s.b.localBusy {
				s.finished = true
				s.event("shard done: build %s, stream exhausted, queue empty", s.b.state)
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		item := s.queue[idx]
		s.queue = append(s.queue[:idx], s.queue[idx+1:]...)
		s.mu.Unlock()
		s.runQuery(ctx, item)
	}
}

// nextReady returns the index of the oldest ready query, or -1. Caller holds mu.
func (s *Shard) nextReady() int {
	for i, item := range s.queue {
		if !item.q.NeedsIndex || s.b.indexReady {
			return i
		}
	}
	return -1
}

func (s *Shard) runQuery(ctx context.Context, item *queued) {
	if err := s.api.JobStart(ctx, s.round, s.w.ShardID, item.seq); err != nil {
		s.fail(fmt.Errorf("job %d start: %w", item.seq, err))
		return
	}
	select {
	case <-time.After(s.cfg.scale(item.q.Duration)):
	case <-ctx.Done():
		return
	}
	if err := s.api.JobDone(ctx, s.round, s.w.ShardID, item.seq); err != nil {
		s.fail(fmt.Errorf("job %d done: %w", item.seq, err))
		return
	}
	s.mu.Lock()
	s.event("query %d done: %s %s waited %s", item.seq, item.q.Kind, map[bool]string{true: "needs_index", false: ""}[item.q.NeedsIndex],
		time.Since(item.arrived).Round(time.Millisecond))
	s.mu.Unlock()
}

func (s *Shard) runLocalBuild(ctx context.Context, dur time.Duration) {
	started := time.Now()
	for {
		elapsed := time.Since(started)
		s.mu.Lock()
		if s.b.cancel {
			s.b.localBusy = false
			s.b.progress = 0
			s.event("local build aborted at %.0f%%: preempted to the gpu queue", 100*float64(elapsed)/float64(dur))
			s.mu.Unlock()
			s.log.Warn("local build aborted: preempted", "progress", float64(elapsed)/float64(dur))
			return
		}
		if elapsed >= dur {
			s.mu.Unlock()
			break
		}
		if dur > 0 {
			s.b.progress = float32(elapsed) / float32(dur)
		}
		s.mu.Unlock()
		slice := s.cfg.Slice
		if slice <= 0 {
			slice = 100 * time.Millisecond
		}
		if rem := dur - elapsed; rem < slice {
			slice = rem
		}
		select {
		case <-time.After(slice):
		case <-ctx.Done():
			return
		}
	}
	accepted, err := s.api.LocalDone(ctx, s.b.id)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.localBusy = false
	if err != nil {
		s.err = fmt.Errorf("local done: %w", err)
		return
	}
	if !accepted {
		// Preempted between our last slice and the done call: the build is the
		// GPU's now. The next report reply tells us its state.
		s.event("local build finished but refused (409): it had been preempted; waiting for the gpu")
		s.log.Warn("local done refused: build was preempted")
		return
	}
	s.b.state, s.b.indexReady, s.b.progress = "done", true, 1
	s.event("local build done after %s", time.Since(started).Round(time.Millisecond))
	s.log.Info("local build done", "attempt", 0)
}

// ---- reports -------------------------------------------------------------------------

func (s *Shard) reports(ctx context.Context) {
	interval := s.cfg.PollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	sentFinal := false
	for {
		s.mu.Lock()
		if s.err != nil {
			s.mu.Unlock()
			return
		}
		fin := s.finished
		req := s.reportLocked()
		s.mu.Unlock()
		if fin && sentFinal {
			return
		}
		summary, err := s.api.Report(ctx, s.round, s.w.ShardID, req)
		if err != nil {
			s.fail(fmt.Errorf("report: %w", err))
			return
		}
		if fin && req.StreamDone && req.QueueDepth == 0 {
			sentFinal = true
		}
		s.apply(summary)
		if fin && sentFinal {
			return
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

// reportLocked builds the load report from the current state. Caller holds mu.
func (s *Shard) reportLocked() client.ReportRequest {
	var waitingIdx int32
	var oldest time.Duration
	for _, it := range s.queue {
		if it.q.NeedsIndex {
			waitingIdx++
		}
		if w := time.Since(it.arrived); w > oldest {
			oldest = w
		}
	}
	progress := float32(0)
	if s.b.localBusy {
		progress = s.b.progress
	}
	return client.ReportRequest{
		QueueDepth: int32(len(s.queue)), WaitingNeedsIndex: waitingIdx, OldestWaitMs: oldest.Milliseconds(),
		BuildProgress: progress, StreamDone: s.streamDone,
	}
}

// apply acts on the scheduler's view of our build (decision 29): a placement
// change is how we learn we were preempted or reneged; done unblocks the
// queries that need the index.
func (s *Shard) apply(b client.BuildSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prevPlacement, prevState, prevAttempt := s.b.placement, s.b.state, s.b.attempt
	s.b.placement, s.b.state, s.b.attempt = b.Placement, b.State, b.Attempt
	if b.Attempt != prevAttempt && prevAttempt > 0 {
		// A requeue and re-claim can both happen between two reports, so the
		// attempt counter is the only sure sign of a recovery (cross-check gap).
		s.event("build attempt %d -> %d: the previous attempt was lost and the build re-claimed", prevAttempt, b.Attempt)
	}
	switch {
	case b.State == "done" && !s.b.indexReady:
		s.b.indexReady = true
		s.event("index ready (attempt %d, %s)", b.Attempt, b.Placement)
		s.log.Info("index ready", "attempt", b.Attempt, "placement", b.Placement)
	case b.State == "failed" && !s.b.failed:
		s.b.failed = true
		s.event("build FAILED (attempt %d): queries that need the index can never run on this shard", b.Attempt)
		s.log.Error("build failed; needs_index queries are stuck (open question in the design doc)", "attempt", b.Attempt)
	case b.Placement == "gpu" && (b.State == "queued" || b.State == "leased") && s.b.localBusy:
		s.b.cancel = true
		s.event("scheduler moved the build to the gpu (%s): aborting the local build", b.State)
	case b.Placement == "local" && b.State == "running" && !s.b.localBusy && !s.b.indexReady && !s.b.localWant:
		if prevPlacement == "gpu" {
			s.event("scheduler moved the build back to local (renege): starting the local build")
		}
		s.b.localWant = true
	}
	if prevPlacement != b.Placement || prevState != b.State || prevAttempt != b.Attempt {
		s.log.Info("build state", "placement", b.Placement, "state", b.State, "attempt", b.Attempt)
	}
}

func (s *Shard) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
		s.event("ERROR: %v", err)
		s.log.Error("shard failed", "err", err)
	}
}
