// The scheduler: HTTP API for shards, placement policy, reaper. Stateless;
// everything lives in Postgres. Configuration is by environment variable.
//
//	DATABASE_URL         required, e.g. postgres://user:pw@host:5432/db
//	LISTEN_ADDR          default :8080
//	POLICY               default always_gpu
//	REAP_INTERVAL        default 2s
//	MAX_ATTEMPTS         default 5; a build whose lease expires on this attempt is marked failed
//	                     instead of requeued (decision 42). 0 disables.
//	WORKER_STALE_AFTER   default 30s; a worker unseen for longer is not in the pool
//	LOG_FORMAT           json (default) or text
//	COST_A, COST_SPEEDUP, COST_GPU_OVERHEAD, COST_BANDWIDTH, COST_MEM_FACTOR
//	                     override cost model v0 constants (see scheduler/costmodel)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arqbca11/KubernetesGPU/db"
	"github.com/arqbca11/KubernetesGPU/scheduler/api"
	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/policy"
	"github.com/arqbca11/KubernetesGPU/scheduler/reaper"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

func main() {
	// `scheduler healthcheck` is the container health probe: the image is
	// distroless (no shell, no curl), so the binary checks itself.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(env("LISTEN_ADDR", ":8080")))
	}
	log := newLogger(env("LOG_FORMAT", "json"))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(2)
	}
	pol, err := policy.ByName(env("POLICY", "always_gpu"))
	if err != nil {
		log.Error("bad POLICY", "err", err)
		os.Exit(2)
	}
	cm := costModelFromEnv(log)
	reapInterval := envDuration(log, "REAP_INTERVAL", 2*time.Second)
	maxAttempts := envInt(log, "MAX_ATTEMPTS", 5)
	staleAfter := envDuration(log, "WORKER_STALE_AFTER", 30*time.Second)
	addr := env("LISTEN_ADDR", ":8080")

	// SIGTERM (Kubernetes, docker stop) or Ctrl-C cancels ctx.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := connectWithRetry(ctx, dbURL, log)
	if pool == nil {
		os.Exit(1)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		log.Error("migrate failed", "err", err)
		os.Exit(1)
	}
	st := store.New(pool)

	go reaper.Run(ctx, st, reaper.Config{Interval: reapInterval, MaxAttempts: int32(maxAttempts)}, log)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(st, pol, cm, api.Config{WorkerStaleAfter: staleAfter}, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("scheduler listening", "addr", addr, "policy", pol.Name(),
			"reap_interval", reapInterval, "max_attempts", maxAttempts, "worker_stale_after", staleAfter)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
}

// healthcheck GETs /healthz on the local server; 0 if it answers 200.
func healthcheck(addr string) int {
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

// connectWithRetry keeps trying until Postgres answers or ctx is cancelled,
// so starting before the database (Compose, Kubernetes) is not fatal.
func connectWithRetry(ctx context.Context, url string, log *slog.Logger) *pgxpool.Pool {
	wait := 500 * time.Millisecond
	for {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				log.Info("connected to postgres")
				return pool
			}
			pool.Close()
		}
		log.Warn("postgres not ready, retrying", "err", err, "in", wait)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		if wait < 5*time.Second {
			wait *= 2
		}
	}
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
		log.Error("bad integer", "var", key, "value", v, "err", err)
		os.Exit(2)
	}
	return n
}

func envDuration(log *slog.Logger, key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Error("bad duration", "var", key, "value", v, "err", err)
		os.Exit(2)
	}
	return d
}

func costModelFromEnv(log *slog.Logger) costmodel.Model {
	m := costmodel.Default()
	f := func(key string, dst *float64) {
		v := os.Getenv(key)
		if v == "" {
			return
		}
		x, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Error("bad float", "var", key, "value", v, "err", err)
			os.Exit(2)
		}
		*dst = x
	}
	f("COST_A", &m.A)
	f("COST_SPEEDUP", &m.Speedup)
	f("COST_GPU_OVERHEAD", &m.GPUOverhead)
	f("COST_BANDWIDTH", &m.Bandwidth)
	f("COST_MEM_FACTOR", &m.MemFactor)
	return m
}
