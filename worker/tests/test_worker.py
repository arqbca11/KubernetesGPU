"""The worker loop against a real Postgres with the fake builder sped up.
Each test runs a Worker in a thread, drives the database the way the
scheduler, the reaper and a second worker would, and reads the outcome back.
"""

from __future__ import annotations

import threading
import time

from conftest import new_build, new_round, row
from kgpu_worker.builder import FakeBuilder
from kgpu_worker.config import Config
from kgpu_worker.costmodel import Model
from kgpu_worker.worker import Worker


def make_worker(dsn: str, worker_id: str, *, time_scale: float = 50.0, lease_s: float = 2.0,
                renew_s: float = 0.2, budget_s: float = 5.0, fail_ids: frozenset[str] = frozenset()) -> Worker:
    cfg = Config(
        database_url=dsn, worker_id=worker_id, mem_bytes=8 << 30, lease_s=lease_s,
        renew_interval_s=renew_s, heartbeat_interval_s=0.5, poll_interval_s=0.05,
        builder="fake", fake_time_scale=time_scale, shutdown_finish_budget_s=budget_s,
        fake_fail_build_ids=fail_ids, log_format="text", model=Model(),
    )
    cfg.validate()
    return Worker(cfg, FakeBuilder(cfg.model, time_scale=time_scale, fail_build_ids=fail_ids))


def run_in_thread(w: Worker) -> threading.Thread:
    t = threading.Thread(target=w.run, name=w.cfg.worker_id, daemon=True)
    t.start()
    return t


def wait_for(pred, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def test_worker_claims_builds_completes(db, dsn):
    r = new_round(db)
    a, b = new_build(db, r, 1), new_build(db, r, 2)
    print("two queued builds; one worker; fake builds take ~0.09 s each at 50x")
    w = make_worker(dsn, "gpu-1")
    t = run_in_thread(w)
    wait_for(lambda: db.execute("SELECT count(*) FROM builds WHERE state = 'done'").fetchone()[0] == 2, 10, "both builds done")
    for bid in (a, b):
        d = row(db, bid)
        assert d["state"] == "done" and d["attempt"] == 1
    hb = db.execute("SELECT worker_id, mem_bytes FROM workers").fetchall()
    print(f"  [db] workers: {hb}")
    assert hb == [("gpu-1", 8 << 30)]
    w.shutdown("test over")
    t.join(5)
    assert w.builds_done == 2 and w.builds_lost == 0
    print("worker exited cleanly; builds_done=2 builds_lost=0")


def test_lease_lost_mid_build_abandons_without_completing(db, dsn):
    """A paused worker's lease is reaped and re-claimed; when it resumes, its
    renew is rejected, it cancels the build and does NOT complete."""
    r = new_round(db)
    bid = new_build(db, r, 1, n=5_000_000)   # ~0.9 s at 50x: long enough to interfere with
    w = make_worker(dsn, "gpu-slow", renew_s=0.1)
    t = run_in_thread(w)
    wait_for(lambda: row(db, bid)["state"] == "leased", 5, "claim")
    print("worker claimed (attempt 1) and is building")

    print("the scheduler's reaper decides the lease expired, and another worker claims attempt 2")
    db.execute("""UPDATE builds SET state = 'queued', lease_owner = NULL, lease_until = NULL, started_at = NULL
                  WHERE build_id = %s""", (bid,))
    db.execute("""UPDATE builds SET state = 'leased', attempt = attempt + 1, lease_owner = 'gpu-other',
                  lease_until = now() + interval '30 seconds', started_at = now() WHERE build_id = %s""", (bid,))
    row(db, bid)

    wait_for(lambda: w.builds_lost == 1, 5, "worker to notice the lost lease")
    d = row(db, bid)
    assert d["state"] == "leased" and d["attempt"] == 2 and d["lease_owner"] == "gpu-other"
    print("worker's renew was rejected; it cancelled its build and left the row to gpu-other")
    w.shutdown("test over")
    t.join(5)
    assert w.builds_done == 0


def test_sigterm_releases_long_build_and_finishes_short_one(db, dsn):
    r = new_round(db)
    long_ = new_build(db, r, 1, n=5_000_000)   # ~0.9 s at 50x
    w = make_worker(dsn, "gpu-term", budget_s=0.2)
    t = run_in_thread(w)
    wait_for(lambda: row(db, long_)["state"] == "leased", 5, "claim of the long build")
    print("SIGTERM arrives with ~0.8 s of build left and a 0.2 s finish budget")
    w.shutdown("SIGTERM")
    t.join(5)
    d = row(db, long_)
    assert d["state"] == "queued" and d["attempt"] == 1 and d["lease_owner"] is None
    assert w.builds_released == 1 and w.builds_lost == 0
    print("the build went back to the queue immediately (no waiting for the reaper), attempt unchanged; counted as released, not lost")
    assert db.execute("SELECT count(*) FROM workers WHERE worker_id = 'gpu-term'").fetchone()[0] == 0
    print("the worker deregistered itself on the clean exit: pool state no longer counts it")
    # Park it so the next worker does not pick the long build up again.
    db.execute("UPDATE builds SET state = 'done', finished_at = now() WHERE build_id = %s", (long_,))

    short = new_build(db, r, 2, n=1_000)        # ~0.04 s at 50x
    w2 = make_worker(dsn, "gpu-term2", budget_s=5.0, time_scale=5.0)   # ~0.4 s build, 5 s budget
    t2 = run_in_thread(w2)
    wait_for(lambda: row(db, short)["state"] == "leased", 5, "claim of the short build")
    print("SIGTERM arrives with the short build mid-flight and a generous budget")
    w2.shutdown("SIGTERM")
    t2.join(10)
    d = row(db, short)
    assert d["state"] == "done"
    print("the short build was finished before exit")


def test_two_workers_never_share_a_build(db, dsn):
    r = new_round(db)
    ids = [new_build(db, r, i, n=20_000) for i in range(12)]
    w1, w2 = make_worker(dsn, "gpu-a"), make_worker(dsn, "gpu-b")
    t1, t2 = run_in_thread(w1), run_in_thread(w2)
    wait_for(lambda: w1.builds_done + w2.builds_done == 12, 20, "both workers to count 12 completions")
    assert db.execute("SELECT count(*) FROM builds WHERE state = 'done'").fetchone()[0] == 12
    attempts = db.execute("SELECT attempt FROM builds WHERE round_id = %s", (r,)).fetchall()
    assert all(a == (1,) for a in attempts)
    w1.shutdown("done"); w2.shutdown("done")
    t1.join(5); t2.join(5)
    print(f"12 builds, 2 workers: gpu-a did {w1.builds_done}, gpu-b did {w2.builds_done}, every attempt is 1")
    assert w1.builds_done + w2.builds_done == 12 and w1.builds_lost == 0 and w2.builds_lost == 0


def test_registers_before_first_claim(db, dsn):
    """A worker must be in `workers` before it can hold a lease, so pool state
    never misses a busy worker (cross-check finding)."""
    r = new_round(db)
    bid = new_build(db, r, 1, n=2_000_000)
    w = make_worker(dsn, "gpu-first")
    t = run_in_thread(w)
    wait_for(lambda: row(db, bid)["state"] == "leased", 5, "claim")
    reg, started = db.execute(
        """SELECT w.registered_at, b.started_at FROM workers w, builds b
           WHERE w.worker_id = 'gpu-first' AND b.build_id = %s""", (bid,)).fetchone()
    print(f"  [db] registered_at={reg.time()} claim started_at={started.time()}")
    assert reg <= started
    w.shutdown("test over"); t.join(5)


def test_build_error_marks_failed_with_reason_and_worker_continues(db, dsn):
    r = new_round(db)
    bad = new_build(db, r, 1, n=20_000)
    good = new_build(db, r, 2, n=20_000)
    w = make_worker(dsn, "gpu-fail", fail_ids=frozenset({bad}))
    t = run_in_thread(w)
    wait_for(lambda: row(db, bad)["state"] == "failed" and row(db, good)["state"] == "done", 10, "failed + done")
    reason = db.execute("SELECT fail_reason FROM builds WHERE build_id = %s", (bad,)).fetchone()[0]
    print(f"  [db] {bad}: failed, reason={reason!r}")
    assert "injected failure" in reason and "RuntimeError" in reason
    assert row(db, bad)["lease_owner"] is None
    print(f"  the worker kept going and completed {good}; the process did not crash")
    w.shutdown("test over"); t.join(5)
    assert w.builds_done == 1 and w.builds_lost == 0


def test_bad_config_is_refused_before_connecting():
    import pytest
    from kgpu_worker.config import ConfigError
    base = {"DATABASE_URL": "postgres://nowhere/x"}
    for env, msg in [
        ({"COST_BANDWIDTH": "0"}, "COST_BANDWIDTH"),
        ({"LEASE_SECONDS": "5", "RENEW_INTERVAL_SECONDS": "5"}, "RENEW_INTERVAL_SECONDS must be less than"),
        ({"FAKE_TIME_SCALE": "-1"}, "FAKE_TIME_SCALE"),
        ({"COST_A": "fast"}, "not a number"),
        ({"LOG_FORMAT": "yaml"}, "LOG_FORMAT"),
    ]:
        with pytest.raises(ConfigError) as e:
            Config.from_env(base | env)
        print(f"  {env} -> ConfigError({e.value})")
        assert msg in str(e.value)


def test_survives_lost_database_connections_mid_build(db, dsn):
    """Postgres restarts (or the network blinks) while a build is in flight.
    The worker must not crash: it reconnects, keeps renewing, completes the
    build, and goes on claiming (bug log 7, decision 48)."""
    r = new_round(db)
    bid = new_build(db, r, 1, n=5_000_000)      # ~3.4 s at 50x
    nxt = new_build(db, r, 2, n=20_000)
    w = make_worker(dsn, "gpu-outage", lease_s=3.0, renew_s=0.3)
    t = run_in_thread(w)
    wait_for(lambda: row(db, bid)["state"] == "leased", 5, "claim")
    print("worker claimed and is building; now every one of its connections is killed server-side")
    killed = db.execute("""SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
                           WHERE datname = current_database() AND pid <> pg_backend_pid()""").fetchone()[0]
    print(f"  terminated {killed} backend connections (main, renewer, heartbeat)")
    wait_for(lambda: row(db, bid)["state"] == "done", 15, "the in-flight build to complete")
    d = row(db, bid)
    assert d["attempt"] == 1, "the worker kept its lease: no reap, no re-claim"
    wait_for(lambda: row(db, nxt)["state"] == "done", 10, "the next build")
    assert t.is_alive(), "the worker process survived"
    w.shutdown("test over"); t.join(5)
    assert w.builds_done == 2 and w.builds_lost == 0
    print("reconnected on every thread, renewed in time, completed attempt 1, then claimed and completed the next build")


def test_renew_outage_past_lease_deadline_assumes_lost(db, dsn):
    """If the database stays unreachable past the lease deadline, the worker
    must assume the reaper has requeued the build and stop touching it."""
    import kgpu_worker.worker as wk
    r = new_round(db)
    bid = new_build(db, r, 1, n=5_000_000)
    w = make_worker(dsn, "gpu-blackout", lease_s=1.0, renew_s=0.2)
    t = run_in_thread(w)
    wait_for(lambda: row(db, bid)["state"] == "leased", 5, "claim")
    # Make every renew fail with a connection error for longer than the lease.
    import psycopg
    real_renew = wk.Store.renew
    def failing_renew(self, *a, **k):
        raise psycopg.OperationalError("simulated: server closed the connection unexpectedly")
    wk.Store.renew = failing_renew
    try:
        wait_for(lambda: w.builds_lost == 1, 6, "the worker to give up past the lease deadline")
    finally:
        wk.Store.renew = real_renew
    print("renews failed for longer than the lease: the worker assumed the lease lost and cancelled the build")
    d = row(db, bid)
    assert d["state"] == "leased" and d["attempt"] == 1, "it wrote nothing (the real reaper would requeue it)"
    w.shutdown("test over"); t.join(5)
    assert w.builds_done == 0
