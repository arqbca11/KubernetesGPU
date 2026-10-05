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

// RoundConfig drives one round.
type RoundConfig struct {
	Shard        Config
	MinWorkers   int           // wait until this many workers are live before starting (0 = don't wait)
	WaitWorkers  time.Duration // how long to wait for MinWorkers
	RoundTimeout time.Duration // give up on the round after this (real time)
}

// RunRound creates a round, plays every shard's workload against the
// scheduler, waits for the scheduler to stamp the round finished, and
// returns the timeline. The shards run as goroutines in this process.
func RunRound(ctx context.Context, api *client.Client, w workload.Workload, cfg RoundConfig, log *slog.Logger) (*Timeline, error) {
	if err := w.Validate(); err != nil {
		return nil, fmt.Errorf("workload: %w", err)
	}
	if cfg.MinWorkers > 0 {
		deadline := time.Now().Add(cfg.WaitWorkers)
		for {
			n, err := api.LiveWorkers(ctx)
			if err == nil && n >= cfg.MinWorkers {
				break
			}
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("only %d live workers after %s, want %d", n, cfg.WaitWorkers, cfg.MinWorkers)
			}
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	roundID, err := api.CreateRound(ctx, w.Scenario.Name, w.Seed, int32(w.Scenario.NShards))
	if err != nil {
		return nil, fmt.Errorf("create round: %w", err)
	}
	log = log.With("round_id", roundID)
	log.Info("round started", "scenario", w.Scenario.Name, "seed", w.Seed, "n_shards", w.Scenario.NShards, "time_scale", cfg.Shard.TimeScale)
	start := time.Now()

	rctx := ctx
	var cancel context.CancelFunc
	if cfg.RoundTimeout > 0 {
		rctx, cancel = context.WithTimeout(ctx, cfg.RoundTimeout)
		defer cancel()
	}
	shards := make([]*Shard, len(w.Shards))
	errs := make([]error, len(w.Shards))
	var wg sync.WaitGroup
	for i, sw := range w.Shards {
		shards[i] = NewShard(sw, w.Scenario.Dim, roundID, api, cfg.Shard, log)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = shards[i].Run(rctx, start)
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			return nil, fmt.Errorf("shard %d: %w", i, e)
		}
	}

	// Every shard has sent its final report; the scheduler stamps the round on
	// the next GET (or reaper tick).
	var st client.RoundStatus
	for {
		st, err = api.GetRound(rctx, roundID)
		if err != nil {
			return nil, fmt.Errorf("get round: %w", err)
		}
		if st.Round.FinishedAt != nil {
			break
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-rctx.Done():
			return nil, fmt.Errorf("round %d not stamped finished: %w", roundID, rctx.Err())
		}
	}
	events := make(map[int32][]Event, len(shards))
	for _, s := range shards {
		events[s.w.ShardID] = s.Events()
	}
	tl := BuildTimeline(w, st, events, cfg.Shard.TimeScale)
	log.Info("round finished", "duration", tl.Duration.Round(time.Millisecond), "duration_modeled", tl.DurationModeled.Round(time.Millisecond),
		"straggler_lag", tl.StragglerLag.Round(time.Millisecond), "builds_gpu", tl.BuildsGPU, "builds_local", tl.BuildsLocal, "queries", tl.Queries)
	return tl, nil
}
