"""The Python statements must behave exactly like the Go store's: same claim
order, same fencing. This is the Python side of design decision 13."""

from conftest import new_build, new_round, row
from kgpu_worker.store import Store

LEASE = 30.0


def test_claim_order_and_attempt(db, dsn):
    r = new_round(db)
    low = new_build(db, r, 1, priority=1)
    high = new_build(db, r, 2, priority=5)
    s = Store(dsn)
    c = s.claim("w", 1 << 30, LEASE)
    print(f"claim #1 -> {c}")
    assert c and c.build_id == high and c.attempt == 1 and c.round_id == r
    c = s.claim("w", 1 << 30, LEASE)
    print(f"claim #2 -> {c}")
    assert c and c.build_id == low
    assert s.claim("w", 1 << 30, LEASE) is None
    print("claim #3 on the empty queue -> None")


def test_fencing_after_reap(db, dsn):
    """The SIGSTOP scenario at the SQL level, from the Python side."""
    r = new_round(db)
    bid = new_build(db, r, 1)
    w1, w2 = Store(dsn), Store(dsn)

    c1 = w1.claim("worker-1", 1 << 30, LEASE)
    assert c1 and c1.attempt == 1
    print("worker-1 claims -> attempt 1")
    row(db, bid)

    print("worker-1 goes silent; the lease expires; the scheduler's reaper runs")
    db.execute("UPDATE builds SET lease_until = now() - interval '1 second' WHERE build_id = %s", (bid,))
    n = db.execute("""UPDATE builds SET state = 'queued', lease_owner = NULL, lease_until = NULL, started_at = NULL
                      WHERE state = 'leased' AND lease_until < now()""").rowcount
    assert n == 1
    row(db, bid)

    c2 = w2.claim("worker-2", 1 << 30, LEASE)
    assert c2 and c2.attempt == 2 and c2.build_id == bid
    print("worker-2 claims -> attempt 2")
    row(db, bid)

    print("worker-1 wakes up holding attempt 1:")
    assert w1.renew(bid, c1.attempt, LEASE) is False
    print("  renew(1)    -> rejected")
    assert w1.complete(bid, c1.attempt) is False
    print("  complete(1) -> rejected")
    assert w1.fail(bid, c1.attempt, "late") is False
    print("  fail(1)     -> rejected")
    assert w1.release(bid, c1.attempt) is False
    print("  release(1)  -> rejected")
    d = row(db, bid)
    assert d["state"] == "leased" and d["attempt"] == 2 and d["lease_owner"] == "worker-2"

    assert w2.renew(bid, c2.attempt, LEASE) is True
    assert w2.complete(bid, c2.attempt) is True
    print("worker-2 renew(2), complete(2) -> accepted")
    d = row(db, bid)
    assert d["state"] == "done"
    assert w2.complete(bid, c2.attempt) is False
    print("worker-2 complete(2) again -> rejected (already done)")


def test_memory_filter_and_release(db, dsn):
    r = new_round(db)
    big = new_build(db, r, 1, mem=16 << 30, priority=10)
    small = new_build(db, r, 2, mem=1 << 30)
    s = Store(dsn)
    c = s.claim("small-gpu", 8 << 30, LEASE)
    assert c and c.build_id == small
    print(f"8 GiB worker skipped the 16 GiB build and took {c.build_id}")
    assert s.release(c.build_id, c.attempt) is True
    print("released it")
    c2 = s.claim("small-gpu", 8 << 30, LEASE)
    assert c2 and c2.build_id == small and c2.attempt == 2
    print(f"re-claimed -> attempt {c2.attempt}")
    c3 = s.claim("big-gpu", 24 << 30, LEASE)
    assert c3 and c3.build_id == big


def test_heartbeat_upsert(db, dsn):
    s = Store(dsn)
    s.heartbeat("gpu-a", 8 << 30)
    s.heartbeat("gpu-a", 16 << 30)
    r = db.execute("SELECT mem_bytes FROM workers WHERE worker_id = 'gpu-a'").fetchone()
    assert r and r[0] == 16 << 30
    print("two heartbeats, one row, capacity updated")
