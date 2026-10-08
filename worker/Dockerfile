# Build context is worker/:
#   docker build -t kgpu-worker worker/
FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

# Dependencies first, so they are cached across code edits.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY kgpu_worker ./kgpu_worker
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
# tini as PID 1: forwards SIGTERM to python (so the release-on-shutdown path
# runs) and reaps zombies. It also means python is NOT PID 1, so a SIGKILL
# sent to it from inside the container is a real crash: the kernel ignores
# SIGKILL aimed at a PID namespace's init from within. Compose had init: true
# for this; Kubernetes has no such flag, so the image carries its own init.
RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*
RUN useradd --no-create-home --uid 10001 worker
USER worker
ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "kgpu_worker"]
