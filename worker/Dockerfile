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
RUN useradd --no-create-home --uid 10001 worker
USER worker
# SIGTERM goes to python directly (exec form, no shell), so the release-on-shutdown path runs.
ENTRYPOINT ["python", "-m", "kgpu_worker"]
