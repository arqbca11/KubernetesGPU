"""The worker loop.

    register + heartbeat (background thread)
    loop:
        claim                        -> nothing? sleep poll interval
        start renew thread           every renew interval; a failed renew means
                                     the lease is lost -> cancel the build
        builder.build(job)           sleeps (fake) / builds (Phase 4+)
        complete                     guarded by build_id + attempt
    on SIGTERM: stop claiming; finish the current build if its remaining time
    fits the budget, else cancel it and release it back to the queue.

Every write after the claim is guarded; a False from the store means this
worker lost ownership and must stop touching the build (invariant 3).
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass

from .builder import Artifact, Builder, Cancelled, Job
from .config import Config
from .store import Claimed, Store

log = logging.getLogger("kgpu.worker")


@dataclass
class Current:
    """The build this worker is working on right now, if any."""
    job: Job
    cancel: threading.Event
    lost: threading.Event        # set by the renew thread when a renew is rejected
    progress: float = 0.0
    started: float = 0.0


class Worker:
    def __init__(self, cfg: Config, builder: Builder) -> None:
        self.cfg = cfg
        self.builder = builder
        self.store = Store(cfg.database_url)
        self.stopping = threading.Event()   # SIGTERM received: no more claims
        self.current: Current | None = None
        self._lock = threading.Lock()
        self.builds_done = 0                 # for tests and logs
        self.builds_lost = 0

    # ---- lifecycle ---------------------------------------------------------------

    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda signum, frame: self.shutdown(f"signal {signum}"))

    def shutdown(self, why: str) -> None:
        """Stop claiming. Decide what to do with the current build: finish it
        if the remaining time fits the budget, otherwise cancel it (the
        main loop then releases it)."""
        if self.stopping.is_set():
            return
        log.info("shutdown requested", extra={"why": why})
        self.stopping.set()
        with self._lock:
            cur = self.current
        if cur is not None:
            self._apply_shutdown_policy(cur)

    def _apply_shutdown_policy(self, cur: Current) -> None:
        """Decision 37. remaining is in scaled seconds, the same units as the budget."""
        remaining = self.builder.estimate_s(cur.job) * (1.0 - cur.progress)
        tags = self._tags(cur.job) | {"remaining_s": round(remaining, 2), "budget_s": self.cfg.shutdown_finish_budget_s}
        if remaining <= self.cfg.shutdown_finish_budget_s:
            log.info("finishing current build before exit", extra=tags)
        else:
            log.info("cancelling current build to release it", extra=tags)
            cur.cancel.set()

    def run(self) -> None:
        self.store.connect()
        log.info("worker starting", extra={"worker_id": self.cfg.worker_id, "mem_bytes": self.cfg.mem_bytes,
                                          "builder": self.builder.name, "lease_s": self.cfg.lease_s,
                                          "renew_interval_s": self.cfg.renew_interval_s})
        # The first heartbeat is synchronous: a worker must be in `workers`
        # before it can hold a lease, so pool state never misses a busy worker.
        self.store.heartbeat(self.cfg.worker_id, self.cfg.mem_bytes)
        log.info("registered", extra={"worker_id": self.cfg.worker_id})
        hb = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        hb.start()
        try:
            while not self.stopping.is_set():
                claimed = self.store.claim(self.cfg.worker_id, self.cfg.mem_bytes, self.cfg.lease_s)
                if claimed is None:
                    self.stopping.wait(self.cfg.poll_interval_s)
                    continue
                self._work(claimed)
        finally:
            log.info("worker stopped", extra={"worker_id": self.cfg.worker_id, "builds_done": self.builds_done,
                                             "builds_lost": self.builds_lost})
            self.store.close()

    # ---- one build ---------------------------------------------------------------

    def _work(self, c: Claimed) -> None:
        job = Job(build_id=c.build_id, round_id=c.round_id, attempt=c.attempt,
                  n_vectors=c.n_vectors, dim=c.dim, mem_bytes=c.mem_bytes)
        cur = Current(job=job, cancel=threading.Event(), lost=threading.Event(), started=time.monotonic())
        with self._lock:
            self.current = cur
        tags = self._tags(job)
        renewer = threading.Thread(target=self._renew_loop, args=(cur,), name=f"renew-{job.build_id}", daemon=True)
        renewer.start()

        def progress(p: float) -> None:
            cur.progress = p

        # From here on the lease is ours, so every exception that is not a
        # cancellation ends in `fail` with the reason. A crash would leave
        # the row leased and the build would poison the next worker too.
        try:
            log.info("claimed", extra=tags | {"n_vectors": job.n_vectors, "dim": job.dim,
                                              "estimate_s": round(self.builder.estimate_s(job), 2)})
            if self.stopping.is_set():
                # SIGTERM landed between the claim and here: same decision as mid-build.
                self._apply_shutdown_policy(cur)
            artifact = self.builder.build(job, progress, cur.cancel)
        except Cancelled as e:
            self._after_cancel(cur, str(e))
            return
        except Exception as e:  # noqa: BLE001  the build (or anything after the claim) failed
            log.exception("build failed", extra=tags)
            reason = f"{type(e).__name__}: {e}"[:500]
            try:
                ok = self.store.fail(job.build_id, job.attempt, reason)
            except Exception:  # noqa: BLE001  database gone too: the reaper will recover the lease
                log.exception("could not mark failed; leaving it to the reaper", extra=tags)
                return
            if ok:
                log.info("marked failed", extra=tags | {"reason": reason})
            else:
                self._lost(tags, "fail rejected: stale attempt")
            return
        finally:
            cur.cancel.set()          # stops the renew loop
            renewer.join(timeout=5)
            with self._lock:
                self.current = None

        self._publish(cur, artifact)

    def _publish(self, cur: Current, artifact: Artifact) -> None:
        tags = self._tags(cur.job)
        if cur.lost.is_set():
            self._lost(tags, "lease was lost during the build; not completing")
            return
        if self.store.complete(cur.job.build_id, cur.job.attempt):
            self.builds_done += 1
            log.info("completed", extra=tags | {"kind": artifact.kind, "index_bytes": artifact.index_bytes,
                                                "elapsed_s": round(artifact.elapsed_s, 2)})
        else:
            self._lost(tags, "complete rejected: stale attempt")

    def _after_cancel(self, cur: Current, why: str) -> None:
        tags = self._tags(cur.job)
        if cur.lost.is_set():
            self._lost(tags, "lease lost; build abandoned")
            return
        # Cancelled by shutdown while we still own it: give it back now rather
        # than making the reaper wait for the lease to expire.
        if self.store.release(cur.job.build_id, cur.job.attempt):
            log.info("released back to queue", extra=tags | {"why": why})
        else:
            self._lost(tags, "release rejected: stale attempt")

    def _lost(self, tags: dict, why: str) -> None:
        self.builds_lost += 1
        log.warning("lost ownership", extra=tags | {"why": why})

    # ---- background loops --------------------------------------------------------

    def _renew_loop(self, cur: Current) -> None:
        store = Store(self.cfg.database_url)   # own connection: the main thread is busy
        store.connect()
        tags = self._tags(cur.job)
        try:
            while not cur.cancel.wait(self.cfg.renew_interval_s):
                try:
                    ok = store.renew(cur.job.build_id, cur.job.attempt, self.cfg.lease_s)
                except Exception:  # noqa: BLE001  transient db error: try again next tick
                    log.exception("renew errored", extra=tags)
                    continue
                if ok:
                    log.debug("renewed", extra=tags)
                else:
                    log.warning("renew rejected: lease lost, cancelling build", extra=tags)
                    cur.lost.set()
                    cur.cancel.set()
                    return
        finally:
            store.close()

    def _heartbeat_loop(self) -> None:
        store = Store(self.cfg.database_url)
        store.connect()
        try:
            while True:
                try:
                    store.heartbeat(self.cfg.worker_id, self.cfg.mem_bytes)
                except Exception:  # noqa: BLE001
                    log.exception("heartbeat errored")
                if self.stopping.wait(self.cfg.heartbeat_interval_s):
                    return
        finally:
            store.close()

    @staticmethod
    def _tags(job: Job) -> dict:
        return {"build_id": job.build_id, "attempt": job.attempt, "round_id": job.round_id}
