// Package reaper is the scheduler's periodic sweep: expired leases go back
// to the queue, and rounds whose work is all done get their finished_at.
// Both statements are idempotent, so two schedulers running this is safe.
package reaper

import (
	"context"
	"log/slog"
	"time"

	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

// Config for the sweep.
type Config struct {
	Interval    time.Duration
	MaxAttempts int32 // a build whose lease expires on this attempt is failed, not requeued; <= 0 disables
}

// Run ticks until ctx is cancelled. It never returns an error: a failed tick
// is logged and the next tick tries again.
func Run(ctx context.Context, st *store.Store, cfg Config, log *slog.Logger) {
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			Tick(ctx, st, cfg.MaxAttempts, log)
		}
	}
}

// Tick runs one sweep. Exposed so tests and the API can run it on demand.
func Tick(ctx context.Context, st *store.Store, maxAttempts int32, log *slog.Logger) {
	reaped, err := st.Reap(ctx, maxAttempts)
	if err != nil {
		log.Error("reap failed", "err", err)
	}
	for _, r := range reaped {
		if r.Outcome == "failed" {
			log.Error("lease expired, retry budget exhausted, build failed",
				"build_id", r.BuildID, "round_id", r.RoundID, "attempt", r.Attempt, "lease_owner", r.LeaseOwner,
				"max_attempts", maxAttempts)
			continue
		}
		log.Warn("lease expired, build back to queued",
			"build_id", r.BuildID, "round_id", r.RoundID, "attempt", r.Attempt, "lease_owner", r.LeaseOwner)
	}
	n, err := st.FinishCompleteRounds(ctx)
	if err != nil {
		log.Error("finish rounds failed", "err", err)
	}
	if n > 0 {
		log.Info("rounds finished", "count", n)
	}
}
