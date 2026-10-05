"""python -m kgpu_worker"""

from __future__ import annotations

import sys

from . import builder as builders
from .config import Config, ConfigError
from .logging_setup import setup
from .worker import Worker


def main() -> None:
    try:
        cfg = Config.from_env()
    except ConfigError as e:
        # Decision 39: refuse before touching anything; exit 2, one line, the variable named.
        print(f"bad config: {e}", file=sys.stderr)
        sys.exit(2)
    setup(cfg.log_format)
    b = builders.by_name(cfg.builder, cfg.model, cfg.fake_time_scale, cfg.fake_fail_build_ids)
    w = Worker(cfg, b)
    w.install_signal_handlers()
    w.run()


if __name__ == "__main__":
    main()
