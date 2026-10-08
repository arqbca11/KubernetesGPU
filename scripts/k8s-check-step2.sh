#!/usr/bin/env bash
# Phase 2 step 2 check: Postgres (StatefulSet + PVC) and the scheduler
# (Deployment + probes) on kind. Proves: migrations applied, the API works
# through the Service, a deleted scheduler pod is replaced and the data is
# intact, a deleted Postgres pod comes back with its data and the scheduler
# recovers without restarting. Saves test-logs/phase2/step2-postgres-scheduler.log.
set -euo pipefail
cd "$(dirname "$0")/.."
log="test-logs/phase2/step2-postgres-scheduler.log"; mkdir -p test-logs/phase2
exec > >(tee "$log") 2>&1
kubectl config use-context kind-kgpu >/dev/null
K="kubectl -n kgpu"
say() { printf '\n== %s\n' "$*"; }; note() { printf '   %s\n' "$*"; }
result=PASS; reached_end=0; fail() { printf '\n!! FAIL: %s\n' "$*"; result=FAIL; }
PF_PID=""
pf() { # (re)open a port-forward to the scheduler Service on 18090
  [ -n "$PF_PID" ] && { kill "$PF_PID" 2>/dev/null || true; wait "$PF_PID" 2>/dev/null || true; }
  $K port-forward svc/scheduler 18090:8080 >/dev/null 2>&1 & PF_PID=$!
  for _ in $(seq 1 40); do curl -sf http://127.0.0.1:18090/livez >/dev/null 2>&1 && return 0; sleep 0.25; done; return 1
}
API=http://127.0.0.1:18090
cleanup() { [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null || true; [ "$reached_end" = 1 ] || result="ABORTED (script error before the end)"; say "Result: $result"; echo "log: $log"; [ "$result" = PASS ]; }
trap cleanup EXIT
pods() { $K get pods -o wide --no-headers | awk '{print "   "$1, $3, "restarts="$4, "node="$7}'; }
restarts_of() { $K get pod "$1" -o jsonpath='{.status.containerStatuses[0].restartCount}'; }

say "Phase 2 step 2: Postgres StatefulSet and scheduler Deployment on kind  ($(date '+%Y-%m-%d %H:%M:%S %Z'), commit $(git rev-parse --short HEAD))"
say "1. Pods, and the scheduler's startup log (connect, migrate, listen)"
pods
SCHED=$($K get pods -l app=scheduler -o jsonpath='{.items[0].metadata.name}')
$K logs "$SCHED" | python3 -c 'import sys,json
for l in sys.stdin:
    try: d=json.loads(l)
    except Exception: continue
    if d.get("msg") in ("connected to postgres","scheduler listening","postgres not ready, retrying"): print("   |", d["msg"], {k:v for k,v in d.items() if k not in ("time","level","msg")})' | head -5
note "migrations in Postgres:"; $K exec postgres-0 -- psql -U postgres -d kgpu -Atc "select version from schema_migrations order by 1" | sed 's/^/   | /'
[ "$($K exec postgres-0 -- psql -U postgres -d kgpu -Atc 'select count(*) from schema_migrations')" = 4 ] || fail "expected 4 migrations"
note "PVC:"; $K get pvc --no-headers | awk '{print "   | "$1, $2, $4}'

say "2. The API through the Service (port-forward): a round and a build"
pf || fail "port-forward to the scheduler Service failed"
note "healthz: $(curl -s $API/healthz)  livez: $(curl -s $API/livez)"
R=$(curl -s -X POST $API/rounds -d '{"scenario":"k8s-step2","seed":1,"n_shards":1}' | python3 scripts/jget.py "d['round_id']")
B=$(curl -s -X POST $API/builds -d "{\"round_id\":$R,\"shard_id\":0,\"n_vectors\":100000,\"dim\":128}" | python3 scripts/jget.py "d['build_id']+' '+d['placement']+' created='+str(d['created'])")
note "round $R created; build: $B"
[ "$R" -ge 1 ] || fail "round creation failed"

say "3. Delete the scheduler pod mid-flight: the Deployment replaces it; the data was never in it"
$K delete pod "$SCHED" --wait=false >/dev/null; note "deleted $SCHED"
NEW=""
for _ in $(seq 1 60); do
  NEW=$($K get pods -l app=scheduler -o custom-columns='N:.metadata.name,R:.status.containerStatuses[0].ready' --no-headers 2>/dev/null | awk -v old="$SCHED" '$1!=old && $2=="true"{print $1; exit}' || true)
  [ -n "$NEW" ] && break; sleep 1
done
[ -n "${NEW:-}" ] || fail "no replacement scheduler pod became ready"
note "replacement pod: ${NEW:-none}"; pods
pf || fail "port-forward after replacement failed"
G=$(curl -s $API/rounds/1 | python3 scripts/jget.py "str(d['round']['round_id'])+' builds='+str(len(d['builds']))")
note "GET /rounds/1 from the new pod: $G"; [ "$G" = "1 builds=1" ] || fail "state lost across the scheduler replacement"

say "4. Delete the Postgres pod: the StatefulSet recreates postgres-0 on its PVC; the scheduler recovers without a restart"
SR0=$(restarts_of "$NEW")
$K delete pod postgres-0 --wait=false >/dev/null; note "deleted postgres-0 (restart count of scheduler before: $SR0)"
sleep 3
note "scheduler readiness while Postgres is down: ready=$($K get pod "$NEW" -o jsonpath='{.status.containerStatuses[0].ready}')"
$K rollout status statefulset/postgres --timeout=180s >/dev/null && note "postgres-0 is back"
$K wait --for=condition=Ready pod/"$NEW" --timeout=120s >/dev/null && note "scheduler ready again"
pods
SR1=$(restarts_of "$NEW"); [ "$SR0" = "$SR1" ] || fail "the scheduler restarted during the Postgres outage ($SR0 -> $SR1)"
note "scheduler restart count unchanged: $SR1"
pf || fail "port-forward after Postgres restart failed"
G=$(curl -s $API/rounds/1 | python3 scripts/jget.py "str(d['round']['round_id'])+' builds='+str(len(d['builds']))")
note "GET /rounds/1 after the Postgres restart: $G"; [ "$G" = "1 builds=1" ] || fail "data lost across the Postgres pod replacement (PVC)"
R2=$(curl -s -X POST $API/rounds -d '{"scenario":"k8s-step2","seed":2,"n_shards":1}' | python3 scripts/jget.py "d['round_id']")
note "a new round after recovery: round $R2"; [ "$R2" -gt "$R" ] || fail "new round after recovery failed"
note "scheduler log around the outage:"; $K logs "$NEW" | python3 -c 'import sys,json
for l in sys.stdin:
    try: d=json.loads(l)
    except Exception: continue
    if d.get("level") in ("ERROR","WARN"): print("   |", d["level"], d["msg"], str({k:v for k,v in d.items() if k not in ("time","level","msg")})[:120])' | head -6
reached_end=1
