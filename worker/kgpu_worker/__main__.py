"""python -m kgpu_worker"""

from __future__ import annotations

from . import builder as builders
from .config import Config
from .logging_setup import setup
from .worker import Worker


def main() -> None:
    cfg = Config.from_env()
    setup(cfg.log_format)
    b = builders.by_name(cfg.builder, cfg.model, cfg.fake_time_scale)
    w = Worker(cfg, b)
    w.install_signal_handlers()
    w.run()


if __name__ == "__main__":
    main()
