#!/usr/bin/env bash
# Phase 1 step 4: bring the Compose stack up and run the failure tests by hand,
# the way an operator would, narrating as it goes. Saves a tracked log.
#
#   scripts/compose-failures.sh                  # writes test-logs/phase1/step4-compose-failures.log
#   KEEP=1 scripts/compose-failures.sh           # leave the stack running afterwards
#
# Settings are tuned for speed: 5 s leases renewed every second, fake builds at
# 10x. A 2M-vector build is 65 s modeled, 6.5 s here: long enough to kill or
# pause a worker in the middle of it.
set -euo pipefail
cd "$(dirname "$0")/.."

export LEASE_SECONDS=5 RENEW_INTERVAL_SECONDS=1 HEARTBEAT_INTERVAL_SECONDS=1 FAKE_TIME_SCALE=10 \
       REAP_INTERVAL=1s WORKER_STALE_AFTER=5s LOG_FORMAT=text API_PORT=18080 PG_PORT=15433
C="docker compose -f deploy/compose.yaml"
API="http://127.0.0.1:${API_PORT}"
log="test-logs/phase1/step4-compose-failures.log"
mkdir -p test-logs/phase1
exec > >(tee "$log") 2>&1

say()  { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }
fail() { printf '\n!! FAIL: %s\n' "$*"; result=FAIL; }
result=PASS; reached_end=0
build_row() { curl -s "$API/builds/$1" | python3 -c 'import sys,json; b=json.load(sys.stdin); print({k:b.get(k) for k in ["build_id","state","attempt","lease_owner","placement"]})'; }
build_field() { curl -s "$API/builds/$1" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$2') or '')"; }
wait_state() { # build_id state timeout_s
  for _ in $(seq 1 $(( $3 * 4 ))); do [ "$(build_field "$1" state)" = "$2" ] && return 0; sleep 0.25; done
  return 1
}
# Per-container logs (docker compose logs wants service names; docker logs takes a container).
worker_logs() { docker logs "$1" 2>&1 | grep -vE 'heartbeat|registered|worker starting' | sed 's/^/   | /' | cut -c1-170 || true; }
container_of_owner() { # lease_owner is the worker's hostname = container short id
  docker ps --filter "id=$1" --format '{{.Names}}'
}

cleanup() {
  [ "$reached_end" = 1 ] || result="ABORTED (script error before the end; see the last lines above)"
  say "Result: $result"
  if [ "${KEEP:-0}" != 1 ]; then $C down -v --remove-orphans >/dev/null 2>&1 || true; note "stack removed (KEEP=1 keeps it)"; fi
  echo "log: $log"
  [ "$result" = PASS ]
}
trap cleanup EXIT

say "Phase 1 step 4: Compose stack failure tests  ($(date '+%Y-%m-%d %H:%M:%S %Z'), commit $(git rev-parse --short HEAD))"
note "LEASE_SECONDS=$LEASE_SECONDS RENEW_INTERVAL_SECONDS=$RENEW_INTERVAL_SECONDS FAKE_TIME_SCALE=$FAKE_TIME_SCALE REAP_INTERVAL=$REAP_INTERVAL"

say "1. Bring the stack up: postgres, scheduler, 2 workers"
$C down -v --remove-orphans >/dev/null 2>&1 || true
$C up -d --build --quiet-pull 2>&1 | grep -vE '^\s*$' | sed 's/^/   /' | tail -8
for _ in $(seq 1 60); do curl -sf "$API/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -sf "$API/healthz" >/dev/null || { fail "scheduler never became healthy"; exit 1; }
for _ in $(seq 1 30); do [ "$(curl -s $API/workers | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["live_workers"])')" = 2 ] && break; sleep 1; done
note "containers:"; $C ps --format 'table {{.Service}}\t{{.Name}}\t{{.Status}}' | sed 's/^/   /'
note "pool: $(curl -s $API/workers | python3 -c 'import sys,json; p=json.load(sys.stdin)["pool"]; print(p)')"
[ "$(curl -s $API/workers | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["live_workers"])')" = 2 ] || fail "expected 2 live workers"

say "2. Smoke: one round, one 100k build (4.6 s modeled, 0.5 s here) runs to completion"
R=$(curl -s -X POST $API/rounds -d '{"scenario":"compose-smoke","seed":1,"n_shards":1}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["round_id"])')
curl -s -X POST $API/builds -d "{\"round_id\":$R,\"shard_id\":0,\"n_vectors\":100000,\"dim\":128}" | cut -c1-120; echo
wait_state "0:$R" done 20 && note "$(build_row 0:$R)" || fail "smoke build did not complete"

say "3. kill -9 the worker process holding a lease mid-build (a crash: SIGKILL to the python process from inside its container; no shutdown path)"
R=$(curl -s -X POST $API/rounds -d '{"scenario":"compose-kill","seed":2,"n_shards":1}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["round_id"])')
curl -s -X POST $API/builds -d "{\"round_id\":$R,\"shard_id\":0,\"n_vectors\":2000000,\"dim\":128}" >/dev/null
wait_state "0:$R" leased 10 || fail "build was not claimed"
OWNER=$(build_field 0:$R lease_owner); VICTIM=$(container_of_owner "$OWNER")
note "leased: $(build_row 0:$R)"
note "owner $OWNER is container $VICTIM; building for 2 s, then kill"; sleep 2
# The container runs with init: true, so python is not PID 1 (the kernel ignores SIGKILL
# sent to a namespace's init from inside it) and docker kill is not used (Docker treats
# it as a manual stop and does not restart). Find the python pid and kill it.
T0=$(date +%s)
# PID 1 is docker-init (its cmdline also mentions kgpu_worker, so match on the program name, not the module).
docker exec "$VICTIM" sh -c 'for p in /proc/[0-9]*; do pid=${p#/proc/}; [ "$pid" = 1 ] && continue; case "$(tr "\0" " " < $p/cmdline 2>/dev/null)" in python*) kill -9 $pid; echo "   killed pid $pid ($(tr "\0" " " < $p/cmdline))";; esac; done'
note "killed the worker process in $VICTIM at +0 s"
wait_state "0:$R" done 40 && note "done at +$(( $(date +%s) - T0 )) s: $(build_row 0:$R)" || fail "build did not complete after the kill"
[ "$(build_field 0:$R attempt)" = 2 ] || fail "expected attempt 2 after one kill, got $(build_field 0:$R attempt)"
note "scheduler log:"; { $C logs --no-log-prefix scheduler 2>/dev/null | grep -E "lease expired" | tail -2 | sed 's/^/   | /' | cut -c1-170; } || true
note "the killed worker's log (nothing after 'claimed': it had no chance to say anything):"; { worker_logs "$VICTIM" | grep -E "claimed|completed|lost" | tail -3; } || true
note "the process died on its own, so restart policy (unless-stopped) brings it back; waiting for 2 live workers:"
for _ in $(seq 1 30); do [ "$(curl -s $API/workers | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["live_workers"])')" = 2 ] && break; sleep 1; done
{ $C ps --format 'table {{.Name}}\t{{.Status}}' | grep worker | sed 's/^/   /'; } || true
[ "$(curl -s $API/workers | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["live_workers"])')" = 2 ] || fail "killed worker did not come back"

say "4. SIGSTOP a worker past its lease (docker pause), then SIGCONT (docker unpause): the paused-process case fencing exists for"
R=$(curl -s -X POST $API/rounds -d '{"scenario":"compose-pause","seed":3,"n_shards":1}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["round_id"])')
curl -s -X POST $API/builds -d "{\"round_id\":$R,\"shard_id\":0,\"n_vectors\":2000000,\"dim\":128}" >/dev/null
wait_state "0:$R" leased 10 || fail "build was not claimed"
OWNER=$(build_field 0:$R lease_owner); VICTIM=$(container_of_owner "$OWNER")
note "leased: $(build_row 0:$R)"
note "owner $OWNER is container $VICTIM; building for 1 s, then pause for $(( LEASE_SECONDS + 3 )) s (lease is $LEASE_SECONDS s)"; sleep 1
docker pause "$VICTIM" >/dev/null; T0=$(date +%s)
sleep $(( LEASE_SECONDS + 3 ))
note "while paused: $(build_row 0:$R)"
docker unpause "$VICTIM" >/dev/null; note "unpaused at +$(( $(date +%s) - T0 )) s"
wait_state "0:$R" done 40 && note "done: $(build_row 0:$R)" || fail "build did not complete after the pause"
[ "$(build_field 0:$R attempt)" = 2 ] || fail "expected attempt 2, got $(build_field 0:$R attempt)"
sleep 2
note "the paused worker's log: its renew for attempt 1 is rejected when it wakes, it writes nothing more"
{ worker_logs "$VICTIM" | grep -E "attempt=1" | grep -E "claimed|renew rejected|lost ownership|cancell" | tail -4; } || true
[ "$(worker_logs "$VICTIM" | grep -c "lost ownership")" -gt 0 ] || fail "paused worker did not log 'lost ownership'"
if [ "$(worker_logs "$VICTIM" | grep -E "build_id=0:$R " | grep -E "attempt=1" | grep -cE "completed")" -gt 0 ]; then fail "paused worker completed with a stale attempt"; fi
note "exactly one completion of this build across all workers (whichever claimed attempt 2; the woken worker may itself re-claim):"
COMPLETIONS=$( { for w in $($C ps -aq worker); do worker_logs "$w" | grep -E "completed.*build_id=0:$R " || true; done; } )
echo "$COMPLETIONS" | sed 's/^/   /'
[ "$(echo "$COMPLETIONS" | grep -c completed)" = 1 ] || fail "expected exactly one completion, saw: $COMPLETIONS"

say "5. Restart the scheduler mid-round: all state is in Postgres, so the round still completes"
R=$(curl -s -X POST $API/rounds -d '{"scenario":"compose-restart","seed":4,"n_shards":1}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["round_id"])')
curl -s -X POST $API/builds -d "{\"round_id\":$R,\"shard_id\":0,\"n_vectors\":2000000,\"dim\":128}" >/dev/null
wait_state "0:$R" leased 10 || fail "build was not claimed"
note "leased: $(build_row 0:$R)"; note "restarting the scheduler container now"
$C restart scheduler >/dev/null 2>&1
for _ in $(seq 1 60); do curl -sf "$API/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
note "scheduler back: $(curl -s $API/healthz)"
wait_state "0:$R" done 40 && note "done: $(build_row 0:$R)" || fail "build did not complete across the scheduler restart"
[ "$(build_field 0:$R attempt)" = 1 ] || fail "the worker never noticed the restart; attempt should still be 1, got $(build_field 0:$R attempt)"
curl -s -X POST "$API/shards/$R/0/report" -d '{"queue_depth":0,"stream_done":true}' >/dev/null
FIN=$(curl -s $API/rounds/$R | python3 -c 'import sys,json; print(json.load(sys.stdin)["round"]["finished_at"] or "")')
[ -n "$FIN" ] && note "round $R finished_at=$FIN (stamped by the restarted scheduler)" || fail "round not finished after restart"

say "6. Duplicate submit through the real API is a no-op"
DUP=$(curl -s -X POST $API/builds -d "{\"round_id\":$R,\"shard_id\":0,\"n_vectors\":5,\"dim\":4}")
echo "$DUP" | grep -q '"created":false' && note "$(echo $DUP | cut -c1-110)" || fail "duplicate submit was not a no-op: $DUP"

reached_end=1
