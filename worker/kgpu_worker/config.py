"""Configuration by environment variable, like the scheduler.

    DATABASE_URL              required
    WORKER_ID                 default: hostname
    WORKER_MEM_BYTES          modeled GPU memory capacity; default 8 GiB
    LEASE_SECONDS             default 30
    RENEW_INTERVAL_SECONDS    default 10
    HEARTBEAT_INTERVAL_SECONDS default 5
    POLL_INTERVAL_SECONDS     how long to wait when the queue is empty; default 0.5
    BUILDER                   fake (default)
    FAKE_TIME_SCALE           divide fake build times by this; default 1
    SHUTDOWN_FINISH_BUDGET_SECONDS  on SIGTERM, finish the build if its remaining
                              time is under this, else release it; default 5
    LOG_FORMAT                json (default) or text
    COST_*                    cost model overrides, same names as the scheduler
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass

from .costmodel import Model


@dataclass(frozen=True)
class Config:
    database_url: str
    worker_id: str
    mem_bytes: int
    lease_s: float
    renew_interval_s: float
    heartbeat_interval_s: float
    poll_interval_s: float
    builder: str
    fake_time_scale: float
    shutdown_finish_budget_s: float
    log_format: str
    model: Model

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        env = dict(os.environ) if env is None else env
        url = env.get("DATABASE_URL", "")
        if not url:
            raise SystemExit("DATABASE_URL is required")

        def f(key: str, default: float) -> float:
            v = env.get(key)
            return float(v) if v else default

        return cls(
            database_url=url,
            worker_id=env.get("WORKER_ID") or socket.gethostname(),
            mem_bytes=int(env.get("WORKER_MEM_BYTES") or 8 << 30),
            lease_s=f("LEASE_SECONDS", 30),
            renew_interval_s=f("RENEW_INTERVAL_SECONDS", 10),
            heartbeat_interval_s=f("HEARTBEAT_INTERVAL_SECONDS", 5),
            poll_interval_s=f("POLL_INTERVAL_SECONDS", 0.5),
            builder=env.get("BUILDER") or "fake",
            fake_time_scale=f("FAKE_TIME_SCALE", 1.0),
            shutdown_finish_budget_s=f("SHUTDOWN_FINISH_BUDGET_SECONDS", 5),
            log_format=env.get("LOG_FORMAT") or "json",
            model=Model.from_env(env),
        )
