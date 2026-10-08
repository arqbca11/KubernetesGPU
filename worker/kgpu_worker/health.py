"""A tiny HTTP server on its own thread for the Kubernetes probes.

    GET /livez     200 always: the process is alive and this thread runs
    GET /healthz   200 if the last heartbeat to Postgres succeeded within
                   `stale_after` seconds, else 503: readiness

It must never depend on the main thread, which may be inside a build for
minutes (Phase 2 design doc, "probes must not depend on the build thread").
"""

from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("kgpu.health")


class Health:
    def __init__(self, stale_after_s: float) -> None:
        self.stale_after_s = stale_after_s
        self._last_ok = 0.0          # monotonic time of the last successful heartbeat
        self._lock = threading.Lock()

    def heartbeat_ok(self) -> None:
        with self._lock:
            self._last_ok = time.monotonic()

    def ready(self) -> tuple[bool, float]:
        with self._lock:
            age = time.monotonic() - self._last_ok if self._last_ok else float("inf")
        return age <= self.stale_after_s, age

    def serve(self, addr: str) -> ThreadingHTTPServer:
        """Start serving on addr ("host:port" or ":port") in a daemon thread."""
        host, _, port = addr.rpartition(":")
        health = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/livez":
                    self._reply(200, {"status": "alive"})
                elif self.path == "/healthz":
                    ok, age = health.ready()
                    self._reply(200 if ok else 503, {"status": "ok" if ok else "postgres unreachable",
                                                     "last_heartbeat_age_s": None if age == float("inf") else round(age, 1)})
                else:
                    self._reply(404, {"error": "not found"})

            def _reply(self, code: int, body: dict) -> None:
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args: object) -> None:  # probes every few seconds: keep the log clean
                pass

        srv = ThreadingHTTPServer((host or "0.0.0.0", int(port)), Handler)
        threading.Thread(target=srv.serve_forever, name="health", daemon=True).start()
        log.info("health server listening", extra={"addr": addr})
        return srv
