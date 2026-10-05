#!/usr/bin/env bash
# Run the Go and Python tests against a throwaway Postgres container and
# write a readable log to test-logs/latest.log (tracked) and
# test-logs/history/ (local only).
#
# Usage:
#   scripts/test-db.sh                 Go (-v ./...) then Python (worker/)
#   scripts/test-db.sh --go            Go only
#   scripts/test-db.sh --py            Python only
#   scripts/test-db.sh <go test args>  Go only, with these args
#   scripts/test-db.sh --save NAME ... also keep a tracked copy at test-logs/NAME.log
#                                      (e.g. --save phase1/step3-worker). Every
#                                      step's final run and every cross-check run
#                                      is saved this way, so the record survives.
set -euo pipefail

cd "$(dirname "$0")/.."
run_go=1; run_py=1; save=""
while [ $# -gt 0 ]; do
  case "$1" in
    --go) run_py=0; shift ;;
    --py) run_go=0; shift ;;
    --save) save="$2"; shift 2 ;;
    *) break ;;
  esac
done
if [ $# -gt 0 ]; then run_py=0; fi   # explicit go test args: Go only
if [ "$run_go" = 1 ] && [ $# -eq 0 ]; then set -- -v ./...; fi
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
  if [ -n "$save" ]; then
    mkdir -p "test-logs/$(dirname "$save")"
    cp "$log" "test-logs/${save}.log"
    echo "log: test-logs/${save}.log (also test-logs/latest.log, copy: $log)"
  else
    echo "log: test-logs/latest.log (copy: $log)"
  fi
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
  echo "go:        $([ "$run_go" = 1 ] && echo "go test -p 1 $*" || echo skipped)"
  echo "python:    $([ "$run_py" = 1 ] && echo "uv run pytest -v -rA (worker/)" || echo skipped)"
  echo
  echo "Lines starting with 'store_test.go:N:' are the test narrating what it did."
  echo "Lines tagged [db] are the build row as Postgres had it at that moment."
  echo
} > "$log"

export TEST_DATABASE_URL="postgres://postgres:test@127.0.0.1:${port}/kgpu?sslmode=disable"
rc=0
if [ "$run_go" = 1 ]; then
  echo "== go test" | tee -a "$log"
  set +e
  # -p 1: run one package at a time. Every package's tests share this one
  # database and TRUNCATE it, so packages must not overlap.
  go test -p 1 "$@" 2>&1 | tee -a "$log"
  rc=${PIPESTATUS[0]}
  set -e
fi
if [ "$run_py" = 1 ]; then
  echo "== pytest (worker/)" | tee -a "$log"
  set +e
  # -rA prints each test's captured output (the narration) even on PASS.
  (cd worker && uv run --quiet pytest -v -rA 2>&1) | tee -a "$log"
  prc=${PIPESTATUS[0]}
  set -e
  if [ "$prc" -ne 0 ]; then rc=$prc; fi
fi
if [ "$rc" -eq 0 ]; then result="PASS"; else result="FAIL (exit $rc)"; fi
exit "$rc"
