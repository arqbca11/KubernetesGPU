// shardsim: the shard simulator. Generates a seeded workload for a scenario,
// plays it as N shard goroutines against the scheduler, and prints the
// per-round timeline. Configuration is by environment variable.
//
//	SCHEDULER_URL     default http://127.0.0.1:8080
//	SCENARIO          preset name; default skewed (see experiments/workload)
//	N_SHARDS          default 6
//	SEED              default 1; each further round uses SEED+1, SEED+2, ...
//	ROUNDS            default 1; 0 = run rounds forever
//	ROUND_GAP         pause between rounds; default 2s
//	TIME_SCALE        divide every modeled duration (same number as the worker's FAKE_TIME_SCALE); default 1
//	POLL_INTERVAL     shard report cadence; default 500ms
//	MIN_WORKERS       wait for this many live workers before the first round; default 1 (0 = don't wait)
//	ROUND_TIMEOUT     default 30m
//	TIMELINE_DIR      if set, write <dir>/round-<id>.json per round
//	LOG_FORMAT        json (default) or text
//	PRINT_WORKLOAD    if 1, print the generated workload summary and exit (no scheduler needed)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/arqbca11/KubernetesGPU/experiments/workload"
	"github.com/arqbca11/KubernetesGPU/shard/client"
	"github.com/arqbca11/KubernetesGPU/shard/sim"
)

func main() {
	log := newLogger(env("LOG_FORMAT", "json"))
	scenario := env("SCENARIO", workload.DefaultPreset)
	nShards := envInt(log, "N_SHARDS", workload.DefaultShards)
	seed := int64(envInt(log, "SEED", 1))
	rounds := envInt(log, "ROUNDS", 1)
	gap := envDuration(log, "ROUND_GAP", 2*time.Second)
	timeScale := envFloat(log, "TIME_SCALE", 1)
	cfg := sim.RoundConfig{
		Shard:        sim.Config{TimeScale: timeScale, PollInterval: envDuration(log, "POLL_INTERVAL", 500*time.Millisecond), Slice: 100 * time.Millisecond},
		MinWorkers:   envInt(log, "MIN_WORKERS", 1),
		WaitWorkers:  2 * time.Minute,
		RoundTimeout: envDuration(log, "ROUND_TIMEOUT", 30*time.Minute),
	}
	timelineDir := os.Getenv("TIMELINE_DIR")
	// Validate before touching anything (the simulator's version of decision 39).
	for _, c := range []struct {
		ok  bool
		msg string
	}{
		{nShards > 0, "N_SHARDS must be positive"},
		{rounds >= 0, "ROUNDS must be non-negative (0 = forever)"},
		{gap >= 0, "ROUND_GAP must be non-negative"},
		{cfg.Shard.PollInterval > 0, "POLL_INTERVAL must be positive"},
		{cfg.MinWorkers >= 0, "MIN_WORKERS must be non-negative"},
		{cfg.RoundTimeout > 0, "ROUND_TIMEOUT must be positive"},
	} {
		if !c.ok {
			fmt.Fprintln(os.Stderr, "bad config:", c.msg)
			os.Exit(2)
		}
	}

	sc, err := workload.PresetWithShards(scenario, nShards)
	if err != nil {
		log.Error("bad scenario", "err", err, "known", workload.PresetNames())
		os.Exit(2)
	}
	if os.Getenv("PRINT_WORKLOAD") == "1" {
		printWorkload(workload.Generate(sc, seed))
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	api := client.New(env("SCHEDULER_URL", "http://127.0.0.1:8080"))
	for !api.Healthy(ctx) {
		log.Warn("scheduler not ready, retrying")
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
	log.Info("shardsim starting", "scenario", sc.Name, "n_shards", nShards, "seed", seed, "rounds", rounds, "time_scale", timeScale,
		"query_rate", sc.Stream.ClusterQueryRate, "horizon", sc.Stream.Horizon, "pool_workers_advised", sc.Pool.Workers)

	for i := 0; rounds == 0 || i < rounds; i++ {
		if ctx.Err() != nil {
			log.Info("stopped by signal between rounds")
			return
		}
		w := workload.Generate(sc, seed+int64(i))
		tl, err := sim.RunRound(ctx, api, w, cfg, log)
		if err != nil {
			if ctx.Err() != nil {
				// Interrupted mid-round. The round stays open in Postgres (no
				// "abandoned" state exists); say so, so an operator knows why a
				// round never finished.
				log.Warn("stopped by signal mid-round; the round is left open in Postgres", "err", err)
				return
			}
			log.Error("round failed", "err", err)
			os.Exit(1)
		}
		fmt.Println(tl.Text())
		if timelineDir != "" {
			if err := writeTimeline(timelineDir, tl); err != nil {
				log.Error("write timeline", "err", err)
			}
		}
		if rounds == 0 || i < rounds-1 {
			select {
			case <-time.After(gap):
			case <-ctx.Done():
				log.Info("stopped by signal between rounds")
				return
			}
		}
	}
	log.Info("all rounds done", "rounds", rounds)
}

func printWorkload(w workload.Workload) {
	sc := w.Scenario
	fmt.Printf("%s, %d shards, seed %d: horizon=%s cluster_rate=%.1f q/s fanout=%.2f needs_index=%.2f advised pool=%d workers x %d MiB\n",
		sc.Name, sc.NShards, w.Seed, sc.Stream.Horizon, sc.Stream.ClusterQueryRate, sc.Stream.FanOutFraction, sc.Stream.NeedsIndexFraction,
		sc.Pool.Workers, sc.Pool.WorkerMemBytes>>20)
	total := 0
	for _, s := range w.Shards {
		var fan, single, ddl, idx int
		for _, q := range s.Queries {
			switch q.Kind {
			case workload.KindFanOut:
				fan++
			case workload.KindSingleShard:
				single++
			case workload.KindLocalDDL:
				ddl++
			}
			if q.NeedsIndex {
				idx++
			}
		}
		total += len(s.Queries)
		fmt.Printf("  shard %2d: n_vectors=%7d ddl_offset=%-6s queries=%4d (fanout %3d, single %3d, local_ddl %d; needs_index %3d)\n",
			s.ShardID, s.NVectors, s.DDLOffset, len(s.Queries), fan, single, ddl, idx)
	}
	fmt.Printf("  total shard-level queries: %d\n", total)
}

func writeTimeline(dir string, tl *sim.Timeline) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(tl, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, fmt.Sprintf("round-%d.json", tl.RoundID)), b, 0o644)
}

func newLogger(format string) *slog.Logger {
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(log *slog.Logger, key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Error("bad integer", "var", key, "value", v)
		os.Exit(2)
	}
	return n
}

func envFloat(log *slog.Logger, key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		log.Error("bad positive number", "var", key, "value", v)
		os.Exit(2)
	}
	return f
}

func envDuration(log *slog.Logger, key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Error("bad duration", "var", key, "value", v)
		os.Exit(2)
	}
	return d
}
