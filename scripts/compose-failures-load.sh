#!/usr/bin/env bash
# Phase 1 step 6: the four failure tests while the shard simulator generates
# real load (continuous rounds, 6 shards, skewed preset). Each test waits for a
# round with a leased build, injects the failure, and checks the round still
# completes correctly: every build done once, every query finished, the round
# stamped. Saves test-logs/phase1/step6-compose-failures-load.log.
#
#   scripts/compose-failures-load.sh
#   N_SHARDS=50 WORKERS=3 LOG=phase1/step7-failures-load-50 scripts/compose-failures-load.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export FAKE_TIME_SCALE="${FAKE_TIME_SCALE:-2}" SCENARIO="${SCENARIO:-skewed}" N_SHARDS="${N_SHARDS:-6}" SEED="${SEED:-1}" \
       ROUNDS=0 ROUND_GAP=1s LOG_FORMAT=text API_PORT="${API_PORT:-18080}" PG_PORT="${PG_PORT:-15433}" \
       LEASE_SECONDS=5 RENEW_INTERVAL_SECONDS=1 HEARTBEAT_INTERVAL_SECONDS=1 REAP_INTERVAL=1s WORKER_STALE_AFTER=5s \
       MIN_WORKERS="${WORKERS:-2}"
SCALE_WORKERS="${WORKERS:-2}"
C="docker compose -f deploy/compose.yaml"
API="http://127.0.0.1:${API_PORT}"
log="test-logs/${LOG:-phase1/step6-compose-failures-load}.log"
mkdir -p "$(dirname "$log")"
exec > >(tee "$log") 2>&1

say()  { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\n!! FAIL: %s\n' "$*"; result=FAIL; }
result=PASS; reached_end=0
jget() { python3 scripts/jget.py "$1" 2>/dev/null || echo ""; }
round_json() { curl -s "$API/rounds/$1"; }
latest_round() { # highest round id that exists
  local n=${1:-1}; while curl -sf "$API/rounds/$((n+1))" >/dev/null 2>&1; do n=$((n+1)); done; echo "$n"; }
wait_leased() { # round -> prints "build_id owner" of the LARGEST leased build (longest remaining), waiting up to $2 s
  for _ in $(seq 1 $(( ${2:-60} * 10 ))); do
    local r; r=$(round_json "$1" | jget "' '.join(next(((b['build_id'],b['lease_owner']) for b in sorted(d['builds'], key=lambda b: -b['n_vectors']) if b['state']=='leased' and b['lease_owner']), ('','')))")
    [ -n "${r// /}" ] && { echo "$r"; return 0; }; sleep 0.1
  done; return 1
}
wait_round_finished() { for _ in $(seq 1 $(( ${2:-180} * 2 ))); do [ -n "$(round_json "$1" | jget "d['round']['finished_at'] or ''")" ] && return 0; sleep 0.5; done; return 1; }
wait_new_round() { # wait until a round newer than $1 exists
  for _ in $(seq 1 240); do local n; n=$(latest_round "$1"); [ "$n" -gt "$1" ] && { echo "$n"; return 0; }; sleep 0.5; done; return 1; }
check_round() { # round: every build done once, every query finished, round stamped
  local r=$1; local j; j=$(round_json "$r")
  local fin; fin=$(echo "$j" | jget "d['round']['finished_at'] or ''")
  local builds; builds=$(echo "$j" | jget "', '.join(b['build_id']+'='+b['placement']+'/'+b['state']+'@'+str(b['attempt']) for b in d['builds'])")
  local notdone; notdone=$(echo "$j" | jget "sum(1 for b in d['builds'] if b['state']!='done')")
  local jobs; jobs=$(echo "$j" | jget "len(d['jobs'])"); local unfinished; unfinished=$(echo "$j" | jget "sum(1 for x in d['jobs'] if not x['finished_at'])")
  note "round $r: finished_at=${fin:-NULL}; builds: $builds"
  note "round $r: queries recorded=$jobs unfinished=$unfinished"
  [ -n "$fin" ] || fail "round $r never finished"
  [ "$notdone" = 0 ] || fail "round $r has $notdone builds not done"
  [ "$unfinished" = 0 ] || fail "round $r has $unfinished unfinished queries"
}
completions_of() { # build_id -> number of 'completed' log lines across all worker containers (incl. stopped)
  local n=0; for w in $($C ps -aq worker); do n=$(( n + $(docker logs "$w" 2>&1 | grep -c "completed build_id=$1 " || true) )); done; echo "$n"; }
container_of_owner() { docker ps --filter "id=$1" --format '{{.Names}}'; }
timeline_line() { # round shard -> the shard's line from the simulator's text timeline
  $C logs --no-log-prefix shards 2>/dev/null | awk -v r="round $1 " -v s="^$2 " 'index($0, r)==1{f=1} f && $0 ~ s {print; exit}' | sed 's/^/   | timeline: /'; }
shard_events() { $C logs --no-log-prefix shards 2>/dev/null | grep -E "round_id=$1 .*shard_id=$2 " | grep -E "$3" | sed 's/^time=[^ ]* //' | sed 's/^/   | shard: /' | head -${4:-4}; }

cleanup() {
  [ "$reached_end" = 1 ] || result="ABORTED (script error before the end)"
  say "Result: $result"
  if [ "${KEEP:-0}" != 1 ]; then $C down -v --remove-orphans >/dev/null 2>&1 || true; note "stack removed (KEEP=1 keeps it)"; fi
  echo "log: $log"; [ "$result" = PASS ]
}
trap cleanup EXIT

say "Phase 1 step 6: failure tests under simulator load  ($(date '+%Y-%m-%d %H:%M:%S %Z'), commit $(git rev-parse --short HEAD))"
note "scenario=$SCENARIO shards=$N_SHARDS workers=$SCALE_WORKERS time_scale=$FAKE_TIME_SCALE lease=${LEASE_SECONDS}s renew=${RENEW_INTERVAL_SECONDS}s; the simulator runs rounds continuously"

say "0. Bring the stack up; the simulator starts its first round"
$C down -v --remove-orphans >/dev/null 2>&1 || true
$C up -d --build --quiet-pull --scale worker="$SCALE_WORKERS" 2>&1 | grep -E "Started" | sed 's/^/   /' | tail -5
for _ in $(seq 1 120); do curl -sf "$API/rounds/1" >/dev/null 2>&1 && break; sleep 1; done
curl -sf "$API/rounds/1" >/dev/null || { fail "no round started"; exit 1; }
note "round 1 exists; letting it run to completion as a baseline"
wait_round_finished 1 || fail "baseline round did not finish"
check_round 1
timeline_line 1 0

say "1. Crash the worker holding a lease, mid-build, during a round"
R=$(wait_new_round 1); note "round $R started"
read -r BID OWNER <<<"$(wait_leased "$R" 60)" || { fail "no leased build in round $R"; }
VICTIM=$(container_of_owner "$OWNER"); note "build $BID leased to $OWNER ($VICTIM); crashing its process now (0.3 s in)"; sleep 0.3
docker exec "$VICTIM" sh -c 'for p in /proc/[0-9]*; do pid=${p#/proc/}; [ "$pid" = 1 ] && continue; case "$(tr "\0" " " < $p/cmdline 2>/dev/null)" in python*) kill -9 $pid;; esac; done'
wait_round_finished "$R" || fail "round $R did not finish after the crash"
check_round "$R"
ATT=$(round_json "$R" | jget "next(b['attempt'] for b in d['builds'] if b['build_id']=='$BID')")
[ "$ATT" = 2 ] || fail "$BID should have finished at attempt 2 after the crash, got $ATT"
[ "$(completions_of "$BID")" = 1 ] || fail "$BID completed $(completions_of "$BID") times"
note "$BID: attempt $ATT, completed exactly once; the shard's view:"
shard_events "$R" "${BID%%:*}" "submitted|build state|index ready" 6
timeline_line "$R" "${BID%%:*}"
for _ in $(seq 1 30); do [ "$(curl -s $API/workers | jget "d['pool']['live_workers']")" = "$SCALE_WORKERS" ] && break; sleep 1; done
note "pool back to $(curl -s $API/workers | jget "d['pool']['live_workers']") live workers"

say "2. Pause the worker holding a lease past its lease, then unpause, during a round"
R=$(wait_new_round "$R"); note "round $R started"
read -r BID OWNER <<<"$(wait_leased "$R" 60)" || { fail "no leased build in round $R"; }
VICTIM=$(container_of_owner "$OWNER"); note "build $BID leased to $OWNER ($VICTIM); pausing for $(( LEASE_SECONDS + 3 )) s"
docker pause "$VICTIM" >/dev/null; sleep $(( LEASE_SECONDS + 3 )); docker unpause "$VICTIM" >/dev/null; note "unpaused"
wait_round_finished "$R" || fail "round $R did not finish after the pause"
check_round "$R"
ATT=$(round_json "$R" | jget "next(b['attempt'] for b in d['builds'] if b['build_id']=='$BID')")
[ "$ATT" = 2 ] || fail "$BID should have finished at attempt 2 after the pause, got $ATT"
[ "$(completions_of "$BID")" = 1 ] || fail "$BID completed $(completions_of "$BID") times"
[ "$(docker logs "$VICTIM" 2>&1 | grep -c "lost ownership build_id=$BID attempt=1")" -gt 0 ] || fail "paused worker did not log lost ownership for attempt 1"
note "$BID: attempt $ATT, completed exactly once; the paused worker logged lost ownership for attempt 1:"
docker logs "$VICTIM" 2>&1 | grep -E "build_id=$BID " | grep -E "renew rejected|lost ownership" | sed 's/^/   | worker: /' | head -2
timeline_line "$R" "${BID%%:*}"

say "3. Restart the scheduler mid-round; shards retry their calls (decision 61)"
R=$(wait_new_round "$R"); note "round $R started"
wait_leased "$R" 60 >/dev/null || fail "no leased build in round $R"
sleep 1; note "restarting the scheduler now"; $C restart scheduler >/dev/null 2>&1
for _ in $(seq 1 60); do curl -sf "$API/healthz" >/dev/null 2>&1 && break; sleep 0.5; done; note "scheduler back"
wait_round_finished "$R" || fail "round $R did not finish across the scheduler restart"
check_round "$R"
RETRIES=$($C logs --no-log-prefix shards 2>/dev/null | grep -c "scheduler call failed, retrying" || true)
SUCC=$($C logs --no-log-prefix shards 2>/dev/null | grep -c "succeeded after retries" || true)
note "shard client: $RETRIES retried calls, $SUCC succeeded after retries, no shard failed"
[ "$RETRIES" -gt 0 ] || fail "expected the shards to have retried at least one call during the restart"
[ "$($C logs --no-log-prefix shards 2>/dev/null | grep -c "round failed")" = 0 ] || fail "a round failed"
NOT1=$(round_json "$R" | jget "sum(1 for b in d['builds'] if b['attempt']!=1)")
[ "$NOT1" = 0 ] || fail "workers should be unaffected by a scheduler restart; $NOT1 builds not at attempt 1"
note "all builds at attempt 1 (workers never noticed)"

say "4. Duplicate submit through the real API during a round"
R=$(wait_new_round "$R"); note "round $R started"; sleep 0.5
DUP=$(curl -s -X POST "$API/builds" -d "{\"round_id\":$R,\"shard_id\":0,\"n_vectors\":5,\"dim\":4}")
echo "$DUP" | grep -q '"created":false' && note "resubmit of 0:$R with a different body -> $(echo "$DUP" | cut -c1-100)" || fail "duplicate was not a no-op: $DUP"
NV=$(round_json "$R" | jget "next(b['n_vectors'] for b in d['builds'] if b['build_id']=='0:$R')")
[ "$NV" != 5 ] || fail "the duplicate changed n_vectors"
wait_round_finished "$R" || fail "round $R did not finish"
check_round "$R"

say "5. Stop the simulator cleanly (SIGTERM via docker stop)"
$C stop shards >/dev/null 2>&1 || true
{ $C logs --no-log-prefix shards 2>/dev/null | grep -E "stopped by signal" | tail -1 | sed 's/^time=[^ ]* //' | sed 's/^/   | shards: /'; } || true
[ "$($C logs --no-log-prefix shards 2>/dev/null | grep -c "stopped by signal")" -gt 0 ] || fail "the simulator did not log that it was stopped by a signal"
note "rounds run: $(latest_round 1)"
reached_end=1
