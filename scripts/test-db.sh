#!/usr/bin/env bash
# Run the Go tests against a throwaway Postgres container.
# Usage: scripts/test-db.sh [go test args...]   (default: ./...)
set -euo pipefail

name="kgpu-test-pg-$$"
port=$(( 20000 + RANDOM % 20000 ))
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --rm --name "$name" -p "127.0.0.1:${port}:5432" \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=kgpu postgres:17 >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$name" pg_isready -U postgres -q 2>/dev/null; then break; fi
  sleep 0.5
done

export TEST_DATABASE_URL="postgres://postgres:test@127.0.0.1:${port}/kgpu?sslmode=disable"
cd "$(dirname "$0")/.."
go test "${@:-./...}"
