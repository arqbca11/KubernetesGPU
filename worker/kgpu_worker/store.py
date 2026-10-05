"""The worker's SQL. Each statement mirrors scheduler/store/store.go; the Go
integration tests are the reference (design decision 13). Keep the two in
step: if a statement changes there, change it here.

Placeholders are psycopg's %s where Go has $1, $2. Everything runs with
autocommit, one statement per call, exactly as the Go side does.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import psycopg

log = logging.getLogger("kgpu.store")


@dataclass(frozen=True)
class Claimed:
    build_id: str
    round_id: int
    attempt: int
    n_vectors: int
    dim: int
    mem_bytes: int


class Store:
    """One autocommit connection. Not thread-safe: each thread gets its own Store."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    # ---- connection ----------------------------------------------------------

    def connect(self, retry_until: float | None = None) -> None:
        """Connect, retrying with backoff until success or retry_until (monotonic seconds)."""
        wait = 0.5
        while True:
            try:
                self._conn = psycopg.connect(self._dsn, autocommit=True, connect_timeout=5)
                return
            except psycopg.OperationalError as e:
                if retry_until is not None and time.monotonic() >= retry_until:
                    raise
                log.warning("postgres not ready, retrying", extra={"err": str(e).strip(), "in_s": wait})
                time.sleep(wait)
                wait = min(wait * 2, 5.0)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self.connect()
        assert self._conn is not None
        return self._conn

    def _exec(self, sql: str, params: tuple) -> int:
        """Run one statement; return rows affected."""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    # ---- workers --------------------------------------------------------------

    def heartbeat(self, worker_id: str, mem_bytes: int) -> None:
        self._exec(
            """
            INSERT INTO workers (worker_id, mem_bytes, last_seen)
            VALUES (%s, %s, now())
            ON CONFLICT (worker_id) DO UPDATE
            SET mem_bytes = EXCLUDED.mem_bytes, last_seen = now()""",
            (worker_id, mem_bytes),
        )

    # ---- the four operations ----------------------------------------------------

    def claim(self, worker_id: str, mem_cap: int, lease_s: float) -> Claimed | None:
        """Take the highest-priority queued GPU build that fits. None if nothing is claimable.
        attempt is incremented here and nowhere else (invariant 4)."""
        if lease_s <= 0:
            raise ValueError(f"lease must be positive, got {lease_s}")
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE builds
                SET state = 'leased',
                    attempt = attempt + 1,
                    lease_owner = %s,
                    lease_until = now() + make_interval(secs => %s),
                    started_at = now()
                WHERE build_id = (
                  SELECT build_id FROM builds
                  WHERE state = 'queued' AND placement = 'gpu' AND mem_bytes <= %s
                  ORDER BY priority DESC, enqueued_at
                  LIMIT 1
                  FOR UPDATE SKIP LOCKED)
                RETURNING build_id, round_id, attempt, n_vectors, dim, mem_bytes""",
                (worker_id, lease_s, mem_cap),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return Claimed(*row)

    def renew(self, build_id: str, attempt: int, lease_s: float) -> bool:
        """Extend the lease. False means the lease is lost: stop working."""
        if lease_s <= 0:
            raise ValueError(f"lease must be positive, got {lease_s}")
        return 1 == self._exec(
            """
            UPDATE builds
            SET lease_until = now() + make_interval(secs => %s)
            WHERE build_id = %s AND attempt = %s AND state = 'leased'""",
            (lease_s, build_id, attempt),
        )

    def complete(self, build_id: str, attempt: int) -> bool:
        """Mark done. False means a stale attempt: someone else owns the build now."""
        return 1 == self._exec(
            """
            UPDATE builds
            SET state = 'done', finished_at = now(), lease_owner = NULL, lease_until = NULL
            WHERE build_id = %s AND attempt = %s AND state = 'leased'""",
            (build_id, attempt),
        )

    def fail(self, build_id: str, attempt: int, reason: str) -> bool:
        return 1 == self._exec(
            """
            UPDATE builds
            SET state = 'failed', fail_reason = %s, finished_at = now(), lease_owner = NULL, lease_until = NULL
            WHERE build_id = %s AND attempt = %s AND state = 'leased'""",
            (reason, build_id, attempt),
        )

    def release(self, build_id: str, attempt: int) -> bool:
        """Hand a leased build back to the queue (graceful shutdown). Faster than the reaper."""
        return 1 == self._exec(
            """
            UPDATE builds
            SET state = 'queued', lease_owner = NULL, lease_until = NULL, started_at = NULL
            WHERE build_id = %s AND attempt = %s AND state = 'leased'""",
            (build_id, attempt),
        )
