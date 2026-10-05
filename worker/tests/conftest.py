"""Shared fixtures. Database tests skip unless TEST_DATABASE_URL is set
(scripts/test-db.sh sets it). The schema comes from ../db/migrations, applied
the same way the Go runner does: in filename order, recorded in
schema_migrations."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

MIGRATIONS = Path(__file__).resolve().parents[2] / "db" / "migrations"


def apply_migrations(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (0x4B475055,))
        try:
            cur.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())""")
            cur.execute("SELECT version FROM schema_migrations")
            applied = {r[0] for r in cur.fetchall()}
            for f in sorted(MIGRATIONS.glob("*.sql")):
                if f.name in applied:
                    continue
                with conn.transaction():
                    cur.execute(f.read_text())
                    cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (f.name,))
        finally:
            cur.execute("SELECT pg_advisory_unlock(%s)", (0x4B475055,))


@pytest.fixture
def dsn() -> str:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set; run scripts/test-db.sh")
    return url


@pytest.fixture
def db(dsn: str):
    """A clean database with the schema applied. Yields an autocommit connection."""
    conn = psycopg.connect(dsn, autocommit=True)
    apply_migrations(conn)
    conn.execute("TRUNCATE shard_status, shard_jobs, builds, rounds, workers")
    yield conn
    conn.close()


def new_round(conn: psycopg.Connection, n_shards: int = 100) -> int:
    row = conn.execute(
        "INSERT INTO rounds (scenario, seed, n_shards) VALUES ('pytest', 1, %s) RETURNING round_id", (n_shards,)
    ).fetchone()
    assert row
    return row[0]


def new_build(conn: psycopg.Connection, round_id: int, shard: int, n: int = 100_000, dim: int = 128,
              mem: int = 1 << 20, priority: float = 0.0) -> str:
    """Insert a queued GPU build the way the scheduler would."""
    build_id = f"{shard}:{round_id}"
    conn.execute(
        """INSERT INTO builds (build_id, round_id, shard_id, n_vectors, dim, mem_bytes, placement, state, priority)
           VALUES (%s, %s, %s, %s, %s, %s, 'gpu', 'queued', %s)""",
        (build_id, round_id, shard, n, dim, mem, priority),
    )
    return build_id


def row(conn: psycopg.Connection, build_id: str) -> dict:
    r = conn.execute(
        """SELECT state, attempt, lease_owner, placement,
                  CASE WHEN lease_until IS NULL THEN NULL
                       ELSE round(extract(epoch FROM lease_until - now()))::int END AS lease_left_s
           FROM builds WHERE build_id = %s""",
        (build_id,),
    ).fetchone()
    assert r
    d = dict(zip(["state", "attempt", "lease_owner", "placement", "lease_left_s"], r))
    print(f"  [db] build {build_id}: {d}")
    return d
