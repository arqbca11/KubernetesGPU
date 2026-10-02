#!/usr/bin/env bash
# Run the Go tests against a throwaway Postgres container and write a
# readable log to test-logs/latest.log (tracked) and test-logs/history/
# (local only).
#
# Usage: scripts/test-db.sh [go test args...]   (default: -v ./...)
set -euo pipefail

cd "$(dirname "$0")/.."
if [ $# -eq 0 ]; then set -- -v ./...; fi
mkdir -p test-logs/history
stamp=$(date +%Y%m%d-%H%M%S)
log="test-logs/history/${stamp}.log"

name="kgpu-test-pg-$$"
port=$(( 20000 + RANDOM % 20000 ))
image="postgres:17"
started=$(date +%s)

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  sleep 1  # let the daemon flush the die/destroy events before we read them
  {
    echo
    echo "== Docker events for container $name"
    docker events --since "$started" --until 0s \
      --filter "container=$name" \
      --format '{{.Time}} {{.Action}}' 2>/dev/null \
      | grep -E '^[0-9]+ (create|start|kill|die|destroy)$' \
      | while read -r t a; do printf '  %s  %s\n' "$(date -r "$t" +%H:%M:%S)" "$a"; done
    echo
    echo "== Result: ${result:-interrupted}"
  } >> "$log"
  cp "$log" test-logs/latest.log
  echo "log: test-logs/latest.log (copy: $log)"
}
trap cleanup EXIT

docker run -d --rm --name "$name" -p "127.0.0.1:${port}:5432" \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=kgpu "$image" >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$name" pg_isready -U postgres -q 2>/dev/null; then break; fi
  sleep 0.5
done
pgver=$(docker exec "$name" postgres --version)

{
  echo "== Go tests against a throwaway Postgres"
  echo "date:      $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "commit:    $(git rev-parse --short HEAD) $(git diff --quiet || echo '(uncommitted changes)')"
  echo "go:        $(go version | cut -d' ' -f3)"
  echo "postgres:  $pgver (image $image, container $name, port $port)"
  echo "args:      go test $*"
  echo
  echo "Lines starting with 'store_test.go:N:' are the test narrating what it did."
  echo "Lines tagged [db] are the build row as Postgres had it at that moment."
  echo
} > "$log"

export TEST_DATABASE_URL="postgres://postgres:test@127.0.0.1:${port}/kgpu?sslmode=disable"
set +e
go test "$@" 2>&1 | tee -a "$log"
rc=${PIPESTATUS[0]}
set -e
if [ "$rc" -eq 0 ]; then result="PASS"; else result="FAIL (exit $rc)"; fi
exit "$rc"
