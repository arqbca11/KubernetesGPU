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
//
// Query records are batched (decision 60): the shard keeps each query's
// arrival, start and finish on its own clock and sends the records that
// changed since the last report inside the report, once per poll interval.
// Nothing about a query goes over HTTP while it runs, so recorded durations
// are exact at any time scale, and queries that arrive before the shard's DDL
// are run and backfilled once the build exists (decision 59).
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
	seq      int32
	q        workload.Query
	arrived  time.Time
	started  *time.Time
	finished *time.Time
}

// record is the batch form of a queued item (a snapshot of its timestamps).
func (it *queued) record() client.JobRecord {
	return client.JobRecord{Seq: it.seq, DurationMs: it.q.Duration.Milliseconds(), NeedsIndex: it.q.NeedsIndex,
		ArrivedAt: it.arrived, StartedAt: it.started, FinishedAt: it.finished}
}

type buildState struct {
	id         string
	placement  string
	state      string
	attempt    int32
	deciding   bool          // the DDL has arrived and the placement is not known yet: the CPU runs no queries (decision 64)
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
	cpuFreeAt  time.Time // virtual CPU clock: when the current query's modeled time ends (decision 65)
	nextSeq    int32
	dirty      []*queued // records changed since the last report (arrived, started or finished)
	submitted  bool      // the build exists at the scheduler; reports may begin
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
	s.b.id = fmt.Sprintf("%d:%d", s.w.ShardID, s.round)
	// Queries start arriving and running at round start; the build is submitted
	// at the shard's DDL offset (decision 59), and reports begin after that.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); s.arrivals(ctx) }()
	go func() { defer wg.Done(); s.cpu(ctx) }()
	go func() {
		defer wg.Done()
		if err := s.submitAtDDL(ctx); err != nil {
			s.fail(err)
			return
		}
		s.reports(ctx)
	}()
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Shard) submitAtDDL(ctx context.Context) error {
	if d := s.cfg.scale(s.w.DDLOffset); d > 0 {
		select {
		case <-time.After(time.Until(s.start.Add(d))):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// The DDL has arrived: the CPU is held until the placement is known, as
	// DML is blocked while a DDL is processed (decision 64).
	s.mu.Lock()
	s.b.deciding = true
	s.event("DDL received: submitting; the CPU runs no queries until the placement is known")
	s.mu.Unlock()
	resp, err := s.api.Submit(ctx, client.SubmitBuildRequest{RoundID: s.round, ShardID: s.w.ShardID, NVectors: s.w.NVectors, Dim: s.dim})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.deciding = false
	if err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	s.b.placement = resp.Placement
	s.b.cpuBuild = s.cfg.scale(time.Duration(resp.CPUBuildMs) * time.Millisecond)
	if resp.Placement == "local" {
		s.b.state, s.b.localWant = "running", true
	} else {
		s.b.state = "queued"
	}
	s.submitted = true
	pre := len(s.dirty)
	s.event("submitted: placement=%s (%s); cpu_build=%s gpu_total=%s (modeled); %d query records from before the DDL to backfill",
		resp.Placement, resp.Reason, time.Duration(resp.CPUBuildMs)*time.Millisecond, time.Duration(resp.GPUTotalMs)*time.Millisecond, pre)
	s.log.Info("submitted", "placement", resp.Placement, "reason", resp.Reason, "n_vectors", s.w.NVectors, "attempt", 0, "pre_ddl_records", pre)
	return nil
}

// ---- arrivals ------------------------------------------------------------------

func (s *Shard) arrivals(ctx context.Context) {
	for _, q := range s.w.Queries {
		at := s.start.Add(s.cfg.scale(q.Arrival))
		select {
		case <-time.After(time.Until(at)):
		case <-ctx.Done():
			return
		}
		s.mu.Lock()
		item := &queued{seq: s.nextSeq, q: q, arrived: time.Now()}
		s.nextSeq++
		s.queue = append(s.queue, item)
		s.dirty = append(s.dirty, item) // reported in the next batch
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.streamDone = true
	s.event("stream exhausted: %d queries arrived", len(s.w.Queries))
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
		idx := -1
		if !s.b.deciding {
			idx = s.nextReady()
		}
		if idx < 0 {
			done := s.submitted && s.streamDone && len(s.queue) == 0 && (s.b.indexReady || s.b.failed || s.b.state == "done")
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

// runQuery occupies the CPU for the query's scaled duration. Timestamps are
// taken locally and reported in the next batch; no HTTP on this path.
//
// The CPU is a virtual clock (decision 65): a query starts when the CPU is
// free or when it arrived, whichever is later, and finishes exactly its
// scaled duration after that. The goroutine sleeps to that absolute
// deadline. A late wake-up (timer slop, scheduling) therefore does not
// stretch the recorded run time and does not accumulate: the next sleep is
// shorter by the same amount. Measured before this: +0.6 ms per query.
func (s *Shard) runQuery(ctx context.Context, item *queued) {
	s.mu.Lock()
	start := time.Now()
	if s.cpuFreeAt.After(start) {
		start = s.cpuFreeAt
	}
	if item.arrived.After(start) {
		start = item.arrived
	}
	end := start.Add(s.cfg.scale(item.q.Duration))
	s.cpuFreeAt = end
	item.started = &start
	s.dirty = append(s.dirty, item)
	s.mu.Unlock()
	select {
	case <-time.After(time.Until(end)):
	case <-ctx.Done():
		return
	}
	s.mu.Lock()
	item.finished = &end
	s.dirty = append(s.dirty, item)
	s.event("query %d done: %s %s waited %s", item.seq, item.q.Kind, map[bool]string{true: "needs_index", false: ""}[item.q.NeedsIndex],
		start.Sub(item.arrived).Round(time.Millisecond))
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
	s.cpuFreeAt = time.Now()
	s.event("local build done after %s", time.Since(started).Round(time.Millisecond))
	s.log.Info("local build done", "attempt", 0)
}

// ---- reports -------------------------------------------------------------------------

func (s *Shard) reports(ctx context.Context) {
	interval := s.cfg.PollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	// Spread the shards' report beats across the interval (decision 66): shard
	// k starts its cadence at (k mod 10)/10 of an interval, deterministically,
	// so 50 shards do not all report in the same few milliseconds.
	select {
	case <-time.After(interval * time.Duration(s.w.ShardID%10) / 10):
	case <-ctx.Done():
		return
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
		req.Jobs = s.drainDirtyLocked()
		s.mu.Unlock()
		if fin && sentFinal {
			return
		}
		summary, err := s.api.Report(ctx, s.round, s.w.ShardID, req)
		if err != nil {
			s.fail(fmt.Errorf("report: %w", err))
			return
		}
		if fin && req.StreamDone && req.QueueDepth == 0 && len(req.Jobs) == 0 {
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

// drainDirtyLocked snapshots the records that changed since the last report,
// deduplicated by seq (an item may have arrived, started and finished since).
// Caller holds mu. The snapshot copies timestamps, so later changes are
// reported in a later batch and a resend is always safe (COALESCE upsert).
func (s *Shard) drainDirtyLocked() []client.JobRecord {
	if len(s.dirty) == 0 {
		return nil
	}
	seen := map[int32]bool{}
	var out []client.JobRecord
	for _, it := range s.dirty {
		if seen[it.seq] {
			continue
		}
		seen[it.seq] = true
		out = append(out, it.record())
	}
	s.dirty = s.dirty[:0]
	return out
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
