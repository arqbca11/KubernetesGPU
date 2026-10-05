"""Structured logs. Every line about a build carries build_id, attempt and
round_id (CLAUDE.md convention), passed via the `extra=` dict."""

from __future__ import annotations

import json
import logging
import sys
import time

_STD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in _STD and not k.startswith("_"):
                out[k] = v
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extras = " ".join(f"{k}={v}" for k, v in record.__dict__.items() if k not in _STD and not k.startswith("_"))
        ts = time.strftime("%H:%M:%S", time.localtime(record.created)) + f".{int(record.msecs):03d}"
        line = f"{ts} {record.levelname:<5} {record.getMessage()}"
        return f"{line} {extras}" if extras else line


def setup(fmt: str) -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(TextFormatter() if fmt == "text" else JSONFormatter())
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(logging.INFO)
