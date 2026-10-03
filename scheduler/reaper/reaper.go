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

// Run ticks until ctx is cancelled. It never returns an error: a failed tick
// is logged and the next tick tries again.
func Run(ctx context.Context, st *store.Store, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			Tick(ctx, st, log)
		}
	}
}

// Tick runs one sweep. Exposed so tests and the API can run it on demand.
func Tick(ctx context.Context, st *store.Store, log *slog.Logger) {
	reaped, err := st.Reap(ctx)
	if err != nil {
		log.Error("reap failed", "err", err)
	}
	for _, r := range reaped {
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
