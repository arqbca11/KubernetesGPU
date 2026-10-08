// Package db applies the SQL migrations in migrations/ in filename order.
//
// Applied versions are recorded in schema_migrations, and the whole run holds
// a session-level advisory lock, so two processes starting at once (two
// scheduler replicas, or the scheduler and a test) cannot both apply a file.
package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Arbitrary constant; any process migrating this database takes the same lock.
const advisoryLockKey = 0x4B47_5055 // "KGPU"

// Migrate applies every migration file not yet recorded in schema_migrations.
// It is safe to call on every process start.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := MigrateReport(ctx, pool)
	return err
}

// Report says what a Migrate call did, so a process can log it: with two
// scheduler replicas starting together, the logs otherwise cannot show which
// one applied the schema and which one waited on the lock.
type Report struct {
	WaitedForLock time.Duration // how long the advisory lock took to acquire
	Applied       []string      // migration files applied by this call
	AlreadyThere  int           // migrations that were already recorded
}

// MigrateReport is Migrate with a Report.
func MigrateReport(ctx context.Context, pool *pgxpool.Pool) (Report, error) {
	var rep Report
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return rep, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	t0 := time.Now()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return rep, fmt.Errorf("advisory lock: %w", err)
	}
	rep.WaitedForLock = time.Since(t0)
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockKey) //nolint:errcheck

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return rep, fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return rep, fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return rep, err
		}
		applied[v] = true
	}
	rows.Close()
	rep.AlreadyThere = len(applied)

	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return rep, err
	}
	sort.Strings(files)

	for _, f := range files {
		version := f[len("migrations/"):]
		if applied[version] {
			continue
		}
		sql, err := migrations.ReadFile(f)
		if err != nil {
			return rep, err
		}
		if err := applyOne(ctx, conn.Conn(), version, string(sql)); err != nil {
			return rep, fmt.Errorf("migration %s: %w", version, err)
		}
		rep.Applied = append(rep.Applied, version)
	}
	return rep, nil
}

func applyOne(ctx context.Context, conn *pgx.Conn, version, sql string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
