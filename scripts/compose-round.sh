#!/usr/bin/env bash
# Run one (or more) shard-simulator rounds on the Compose stack and save the
# timeline and the relevant service logs as a step log.
#
#   scripts/compose-round.sh                       # 6 shards, skewed, 10x, 1 round -> test-logs/phase1/step5-compose-round.log
#   N_SHARDS=50 LOG=phase1/step7-compose-round-50 scripts/compose-round.sh
#   SCENARIO=bimodal ROUNDS=3 scripts/compose-round.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export FAKE_TIME_SCALE="${FAKE_TIME_SCALE:-10}" SCENARIO="${SCENARIO:-skewed}" N_SHARDS="${N_SHARDS:-6}" SEED="${SEED:-1}" \
       ROUNDS="${ROUNDS:-1}" LOG_FORMAT=text API_PORT="${API_PORT:-18080}" PG_PORT="${PG_PORT:-15433}" \
       LEASE_SECONDS="${LEASE_SECONDS:-10}" RENEW_INTERVAL_SECONDS="${RENEW_INTERVAL_SECONDS:-3}" REAP_INTERVAL=1s \
       WORKER_STALE_AFTER=15s MIN_WORKERS="${MIN_WORKERS:-2}"
SCALE_WORKERS="${WORKERS:-2}"
C="docker compose -f deploy/compose.yaml"
log="test-logs/${LOG:-phase1/step5-compose-round}.log"
mkdir -p "$(dirname "$log")"
exec > >(tee "$log") 2>&1

cleanup() {
  if [ "${KEEP:-0}" != 1 ]; then $C down -v --remove-orphans >/dev/null 2>&1 || true; echo "   stack removed (KEEP=1 keeps it)"; fi
  echo "log: $log"
}
trap cleanup EXIT

echo "== Compose round: scenario=$SCENARIO shards=$N_SHARDS workers=$SCALE_WORKERS seed=$SEED rounds=$ROUNDS time_scale=$FAKE_TIME_SCALE ($(date '+%Y-%m-%d %H:%M:%S %Z'), commit $(git rev-parse --short HEAD))"
echo; echo "== workload"
PRINT_WORKLOAD=1 go run ./shard/cmd/shardsim 2>&1 | sed 's/^/   /'

echo; echo "== up"
$C down -v --remove-orphans >/dev/null 2>&1 || true
$C up -d --build --quiet-pull --scale worker="$SCALE_WORKERS" 2>&1 | grep -E "Started|Healthy|Error|error" | sed 's/^/   /' | tail -6
T0=$(date +%s)
# The shards container exits when its rounds are done (restart: "no").
for _ in $(seq 1 1800); do
  st=$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "$($C ps -aq shards)" 2>/dev/null || echo "missing")
  case "$st" in exited*) break;; esac
  sleep 1
done
echo "   shards container: $st after $(( $(date +%s) - T0 )) s"

echo; echo "== shardsim output (timeline)"
$C logs --no-log-prefix shards 2>/dev/null | grep -vE '^time=' | sed 's/^/   /'
echo; echo "== shardsim log (round events)"
$C logs --no-log-prefix shards 2>/dev/null | grep -E 'round started|round finished|submitted|index ready|aborted|build state|ERROR|failed' | sed 's/^time=[^ ]* //' | sed 's/^/   /' | head -60
echo; echo "== scheduler log (placements, reaper, rounds)"
$C logs --no-log-prefix scheduler 2>/dev/null | grep -E 'build placed|lease expired|rounds finished|round started|level=ERROR' | sed 's/^time=[^ ]* //' | sed 's/^/   /' | head -40
echo; echo "== workers (claims and completions)"
for w in $($C ps -q worker); do docker logs "$w" 2>&1 | grep -E 'claimed|completed|lost|failed' | sed 's/^/   /'; done
echo; echo "== round rows (from the scheduler)"
curl -s "http://127.0.0.1:$API_PORT/rounds/1" > /tmp/kgpu-round.json
python3 - <<'PY'
import json
r = json.load(open("/tmp/kgpu-round.json")); rd = r["round"]
print(f"   round {rd['round_id']}: n_shards={rd['n_shards']} started={rd['started_at']} finished={rd['finished_at']}")
print("   builds: " + ", ".join(f"{b['build_id']}={b['placement']}/{b['state']}@{b['attempt']}" for b in r["builds"]))
jobs = r["jobs"]; done_ = sum(1 for j in jobs if j["finished_at"])
print(f"   queries recorded: {len(jobs)}, finished: {done_}")
print("   shard_status: " + ", ".join(f"{s['shard_id']}:q={s['queue_depth']} done={s['stream_done']}" for s in r["shard_status"]))
PY
echo; echo "== timeline JSON written by the simulator"
docker run --rm -v kgpu_timelines:/t alpine sh -c 'ls -la /t; python3 -c 1 2>/dev/null; head -c 400 /t/round-1.json' 2>/dev/null | sed 's/^/   /' || true
case "$st" in "exited 0") echo; echo "== Result: PASS";; *) echo; echo "== Result: FAIL ($st)";; esac
