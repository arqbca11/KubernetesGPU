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
                              (scaled) time is under this, else release it; default 5
    FAKE_FAIL_BUILD_IDS       comma-separated build ids the fake builder fails at 50%
                              (test and demo knob for the fail path); default none
    HEALTH_ADDR               where the probe server listens; default :8081; empty disables
    LOG_FORMAT                json (default) or text
    COST_*                    cost model overrides, same names as the scheduler

Every value is validated before the worker connects to anything; a bad value
exits with status 2 and one line saying which variable. A worker must never
claim a build it cannot handle.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass

from .costmodel import Model


class ConfigError(Exception):
    """A configuration value is missing or invalid. __main__ prints it and exits 2."""


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
    fake_fail_build_ids: frozenset[str]
    health_addr: str
    log_format: str
    model: Model

    def validate(self) -> None:
        """Raise ConfigError with one clear line on the first bad value."""
        checks = [
            (self.mem_bytes > 0, "WORKER_MEM_BYTES must be positive"),
            (self.lease_s > 0, "LEASE_SECONDS must be positive"),
            (self.renew_interval_s > 0, "RENEW_INTERVAL_SECONDS must be positive"),
            (self.renew_interval_s < self.lease_s, "RENEW_INTERVAL_SECONDS must be less than LEASE_SECONDS"),
            (self.heartbeat_interval_s > 0, "HEARTBEAT_INTERVAL_SECONDS must be positive"),
            (self.poll_interval_s > 0, "POLL_INTERVAL_SECONDS must be positive"),
            (self.fake_time_scale > 0, "FAKE_TIME_SCALE must be positive"),
            (self.shutdown_finish_budget_s >= 0, "SHUTDOWN_FINISH_BUDGET_SECONDS must be non-negative"),
            (self.log_format in ("json", "text"), "LOG_FORMAT must be json or text"),
            (self.model.a > 0, "COST_A must be positive"),
            (self.model.speedup > 0, "COST_SPEEDUP must be positive"),
            (self.model.gpu_overhead >= 0, "COST_GPU_OVERHEAD must be non-negative"),
            (self.model.bandwidth > 0, "COST_BANDWIDTH must be positive"),
            (self.model.mem_factor > 0, "COST_MEM_FACTOR must be positive"),
        ]
        for ok, msg in checks:
            if not ok:
                raise ConfigError(msg)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        env = dict(os.environ) if env is None else env
        url = env.get("DATABASE_URL", "")
        if not url:
            raise ConfigError("DATABASE_URL is required")

        def f(key: str, default: float) -> float:
            v = env.get(key)
            if not v:
                return default
            try:
                return float(v)
            except ValueError:
                raise ConfigError(f"{key}={v!r} is not a number") from None

        try:
            model = Model.from_env(env)
        except ValueError as e:
            raise ConfigError(f"COST_* value is not a number ({e})") from None

        cfg = cls(
            database_url=url,
            worker_id=env.get("WORKER_ID") or socket.gethostname(),
            mem_bytes=int(f("WORKER_MEM_BYTES", 8 << 30)),
            lease_s=f("LEASE_SECONDS", 30),
            renew_interval_s=f("RENEW_INTERVAL_SECONDS", 10),
            heartbeat_interval_s=f("HEARTBEAT_INTERVAL_SECONDS", 5),
            poll_interval_s=f("POLL_INTERVAL_SECONDS", 0.5),
            builder=env.get("BUILDER") or "fake",
            fake_time_scale=f("FAKE_TIME_SCALE", 1.0),
            shutdown_finish_budget_s=f("SHUTDOWN_FINISH_BUDGET_SECONDS", 5),
            fake_fail_build_ids=frozenset(x.strip() for x in (env.get("FAKE_FAIL_BUILD_IDS") or "").split(",") if x.strip()),
            health_addr=env.get("HEALTH_ADDR", ":8081"),
            log_format=env.get("LOG_FORMAT") or "json",
            model=model,
        )
        cfg.validate()
        return cfg
