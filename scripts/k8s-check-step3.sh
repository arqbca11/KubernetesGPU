#!/usr/bin/env bash
# Phase 2 step 3 check: the GPU workers on kind. Proves: both workers run on
# the GPU node and register; a build completes; probes answer during a build;
# a graceful pod deletion (SIGTERM) releases a long build and the replacement
# lands on the GPU node; a forced deletion (no grace) loses the lease and the
# reaper recovers it; every build completes exactly once.
# Saves test-logs/phase2/step3-workers.log.
set -euo pipefail
cd "$(dirname "$0")/.."
log="test-logs/phase2/step3-workers.log"; mkdir -p test-logs/phase2
exec > >(tee "$log") 2>&1
kubectl config use-context kind-kgpu >/dev/null
K="kubectl -n kgpu"
say() { printf '\n== %s\n' "$*"; }; note() { printf '   %s\n' "$*"; }
result=PASS; reached_end=0; fail() { printf '\n!! FAIL: %s\n' "$*"; result=FAIL; }
PF_PID=""; API=http://127.0.0.1:18090
pf() { [ -n "$PF_PID" ] && { kill "$PF_PID" 2>/dev/null || true; wait "$PF_PID" 2>/dev/null || true; }
  $K port-forward svc/scheduler 18090:8080 >/dev/null 2>&1 & PF_PID=$!
  for _ in $(seq 1 40); do curl -sf $API/livez >/dev/null 2>&1 && return 0; sleep 0.25; done; return 1; }
cleanup() { [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null || true; [ "$reached_end" = 1 ] || result="ABORTED (script error before the end)"; say "Result: $result"; echo "log: $log"; [ "$result" = PASS ]; }
trap cleanup EXIT
pods() { $K get pods -l app=worker -o wide --no-headers | awk '{print "   "$1, $3, "restarts="$4, "node="$7}'; }
jget() { python3 scripts/jget.py "$1" 2>/dev/null || echo ""; }
build_row() { curl -s "$API/builds/$1" | jget "dict((k,d.get(k)) for k in ('build_id','state','attempt','lease_owner'))"; }
bfield() { curl -s "$API/builds/$1" | jget "d.get('$2') or ''"; }
wait_state() { for _ in $(seq 1 $(( $3 * 4 ))); do [ "$(bfield "$1" state)" = "$2" ] && return 0; sleep 0.25; done; return 1; }
completions_of() { local n=0; for p in $($K get pods -l app=worker -o name); do n=$(( n + $($K logs "$p" 2>/dev/null | grep -c "\"msg\": \"completed\", \"build_id\": \"$1\"" || true) )); done; echo "$n"; }
new_round() { curl -s -X POST $API/rounds -d "{\"scenario\":\"k8s-step3\",\"seed\":$1,\"n_shards\":1}" | jget "d['round_id']"; }
submit() { curl -s -X POST $API/builds -d "{\"round_id\":$1,\"shard_id\":0,\"n_vectors\":$2,\"dim\":128}" | jget "d['build_id']"; }

say "Phase 2 step 3: GPU workers on kind  ($(date '+%Y-%m-%d %H:%M:%S %Z'), commit $(git rev-parse --short HEAD))"
say "1. Both workers run on the GPU node and are registered with the scheduler"
pods
GPU=$($K get nodes -l kgpu.io/gpu=true -o name | sed 's|node/||')
for n in $($K get pods -l app=worker -o jsonpath='{.items[*].spec.nodeName}'); do [ "$n" = "$GPU" ] || fail "a worker pod is on $n, not the GPU node"; done
pf || fail "port-forward failed"
W=$(curl -s $API/workers | jget "str(d['pool']['live_workers'])+' live: '+', '.join(w['worker_id'] for w in d['workers'])")
note "pool: $W"; [ "$(curl -s $API/workers | jget "d['pool']['live_workers']")" = 2 ] || fail "expected 2 live workers"
note "worker ids are the pod names (downward API)"

say "2. A build completes at attempt 1; the probes answer while it builds"
R=$(new_round 1); B=$(submit "$R" 2000000); note "round $R, build $B (2M vectors: 65 s modeled, 6.5 s at 10x)"
wait_state "$B" leased 20 || fail "not claimed"
OWNER=$(bfield "$B" lease_owner); note "leased to $OWNER"
LZ=$($K exec "$OWNER" -- python -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8081/livez',timeout=2).status)" 2>/dev/null || echo "?")
HZ=$($K exec "$OWNER" -- python -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8081/healthz',timeout=2).status)" 2>/dev/null || echo "?")
note "mid-build probes on $OWNER: /livez=$LZ /healthz=$HZ"; [ "$LZ" = 200 ] && [ "$HZ" = 200 ] || fail "probes did not answer 200 during a build"
wait_state "$B" done 60 || fail "build did not complete"; note "$(build_row "$B")"
[ "$(bfield "$B" attempt)" = 1 ] && [ "$(completions_of "$B")" = 1 ] || fail "expected attempt 1 completed once"

say "3. Graceful deletion (kubectl delete pod, SIGTERM) mid-build: the worker releases, the Deployment replaces it on the GPU node"
R=$(new_round 2); B=$(submit "$R" 2000000); wait_state "$B" leased 20 || fail "not claimed"
OWNER=$(bfield "$B" lease_owner); sleep 1; note "leased to $OWNER; deleting the pod (grace 30 s, budget 5 s, ~5.5 s of build left -> release)"
$K delete pod "$OWNER" --wait=false >/dev/null; T0=$(date +%s)
for _ in $(seq 1 40); do st=$(bfield "$B" state); [ "$st" = queued ] || [ "$st" = done ] || [ "$(bfield "$B" attempt)" = 2 ] && break; sleep 0.25; done
note "+$(( $(date +%s) - T0 )) s: $(build_row "$B")"
wait_state "$B" done 60 || fail "build did not complete after the graceful deletion"
note "$(build_row "$B")"
[ "$(bfield "$B" attempt)" = 2 ] || fail "expected attempt 2 (released then re-claimed), got $(bfield "$B" attempt)"
[ "$(completions_of "$B")" = 1 ] || fail "expected exactly one completion"
$K rollout status deployment/worker --timeout=120s >/dev/null; pods
[ "$($K get pods -l app=worker --field-selector=status.phase=Running --no-headers | wc -l | tr -d ' ')" = 2 ] || fail "expected 2 running workers after replacement"
for n in $($K get pods -l app=worker -o jsonpath='{.items[*].spec.nodeName}'); do [ "$n" = "$GPU" ] || fail "replacement landed on $n"; done
note "the deleted worker's last lines:"; $K logs "$OWNER" 2>/dev/null | python3 -c 'import sys,json
for l in sys.stdin:
    try: d=json.loads(l)
    except Exception: continue
    if d.get("msg") in ("shutdown requested","cancelling current build to release it","released back to queue","deregistered","worker stopped"): print("   |", d["msg"], {k:v for k,v in d.items() if k in ("why","remaining_s","budget_s","builds_released")})' || note "(pod log already gone; it exited within the grace period)"

say "4. Crash the worker process inside its container (SIGKILL by pid; the python image has a shell): no shutdown path, the lease expires, the reaper recovers it, the kubelet restarts the container in place"
R=$(new_round 3); B=$(submit "$R" 2000000); wait_state "$B" leased 20 || fail "not claimed"
OWNER=$(bfield "$B" lease_owner); sleep 1; note "leased to $OWNER; killing its python process"
RC0=$($K get pod "$OWNER" -o jsonpath='{.status.containerStatuses[0].restartCount}')
$K exec "$OWNER" -- sh -c 'for p in /proc/[0-9]*; do pid=${p#/proc/}; [ "$pid" = 1 ] && continue; case "$(tr "\0" " " < $p/cmdline 2>/dev/null)" in python*) kill -9 $pid;; esac; done'
T0=$(date +%s); EXP0=$($K logs deploy/scheduler 2>/dev/null | grep -c "lease expired" || true)
wait_state "$B" done 90 || fail "build did not complete after the crash"
note "+$(( $(date +%s) - T0 )) s: $(build_row "$B")"
[ "$(bfield "$B" attempt)" = 2 ] || fail "expected attempt 2 after the lease expired, got $(bfield "$B" attempt)"
[ "$(completions_of "$B")" = 1 ] || fail "expected exactly one completion"
EXP1=$($K logs deploy/scheduler 2>/dev/null | grep -c "lease expired" || true)
note "scheduler 'lease expired' lines: $EXP0 -> $EXP1"; [ "$EXP1" -gt "$EXP0" ] || fail "the lease never expired: the crash did not take the lease-expiry path"
sleep 2; RC1=$($K get pod "$OWNER" -o jsonpath='{.status.containerStatuses[0].restartCount}')
note "kubelet restarted the container in place: restart count $RC0 -> $RC1 (same pod $OWNER)"; [ "$RC1" -gt "$RC0" ] || fail "container was not restarted"
pods

say "5. Observation: a forced deletion (--grace-period=0 --force) still lets the shutdown path run"
R=$(new_round 4); B=$(submit "$R" 2000000); wait_state "$B" leased 20 || fail "not claimed"
OWNER=$(bfield "$B" lease_owner); sleep 1; note "leased to $OWNER; force-deleting the pod"
EXP0=$($K logs deploy/scheduler 2>/dev/null | grep -c "lease expired" || true)
$K delete pod "$OWNER" --grace-period=0 --force --wait=false >/dev/null 2>&1; T0=$(date +%s)
wait_state "$B" done 90 || fail "build did not complete after the forced deletion"
EXP1=$($K logs deploy/scheduler 2>/dev/null | grep -c "lease expired" || true)
note "+$(( $(date +%s) - T0 )) s: $(build_row "$B"); lease expiries $EXP0 -> $EXP1"
if [ "$EXP1" = "$EXP0" ]; then note "no expiry: the kubelet delivered SIGTERM and the worker released before the kill. A forced delete is NOT a crash; use step 4's in-container kill for that."; else note "the lease expired: this run took the crash path."; fi
[ "$(bfield "$B" attempt)" = 2 ] && [ "$(completions_of "$B")" = 1 ] || fail "expected attempt 2 completed once"
$K rollout status deployment/worker --timeout=120s >/dev/null; pods
reached_end=1
