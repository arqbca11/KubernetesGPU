#!/usr/bin/env bash
# Phase 2 step 1 check: the stand-in GPU node admits only pods that tolerate
# its taint and select its label, and admits those only there. Runs a few
# throwaway pause pods and reports where the Kubernetes scheduler put them.
# Saves test-logs/phase2/step1-kind-cluster.log.
set -euo pipefail
cd "$(dirname "$0")/.."
log="test-logs/phase2/step1-kind-cluster.log"; mkdir -p test-logs/phase2
exec > >(tee "$log") 2>&1
kubectl config use-context kind-kgpu >/dev/null
say() { printf '\n== %s\n' "$*"; }; note() { printf '   %s\n' "$*"; }
result=PASS; fail() { printf '\n!! FAIL: %s\n' "$*"; result=FAIL; }
cleanup() { kubectl delete pod -l kgpu.io/check=1 --ignore-not-found --wait=false >/dev/null 2>&1 || true; say "Result: $result"; echo "log: $log"; [ "$result" = PASS ]; }
trap cleanup EXIT

say "Phase 2 step 1: kind cluster with a stand-in GPU node  ($(date '+%Y-%m-%d %H:%M:%S %Z'), commit $(git rev-parse --short HEAD))"
note "kind $(kind version | cut -d' ' -f2), kubectl $(kubectl version --client -o json | python3 -c 'import sys,json; print(json.load(sys.stdin)["clientVersion"]["gitVersion"])')"
say "1. Nodes, labels and taints"
kubectl wait --for=condition=Ready node --all --timeout=180s >/dev/null
kubectl get nodes -L kgpu.io/role,kgpu.io/gpu -o wide --no-headers | awk '{print "   "$1, $2, "role="$NF}' 
kubectl get nodes -o custom-columns='NAME:.metadata.name,TAINTS:.spec.taints[*].key' --no-headers | sed 's/^/   taints: /'
GPU=$(kubectl get nodes -l kgpu.io/gpu=true -o name | sed 's|node/||'); [ -n "$GPU" ] || fail "no node carries kgpu.io/gpu=true"
note "GPU node: $GPU"

say "2. Six plain pods (no toleration): none may land on the GPU node"
for i in 1 2 3 4 5 6; do
  kubectl run plain-$i --image=registry.k8s.io/pause:3.10 --labels=kgpu.io/check=1 --restart=Never >/dev/null
done
kubectl wait --for=jsonpath='{.spec.nodeName}' pod -l kgpu.io/check=1 --timeout=120s >/dev/null 2>&1 || true
for i in 1 2 3 4 5 6; do n=$(kubectl get pod plain-$i -o jsonpath='{.spec.nodeName}'); note "plain-$i -> $n"; [ "$n" != "$GPU" ] || fail "plain-$i landed on the GPU node"; done

say "3. A worker-shaped pod (toleration + nodeSelector): must land on the GPU node"
kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata: {name: gpu-shaped, labels: {kgpu.io/check: "1"}}
spec:
  restartPolicy: Never
  nodeSelector: {kgpu.io/gpu: "true"}
  tolerations: [{key: kgpu.io/gpu, operator: Equal, value: "true", effect: NoSchedule}]
  containers: [{name: pause, image: registry.k8s.io/pause:3.10}]
YAML
kubectl wait --for=jsonpath='{.spec.nodeName}'="$GPU" pod/gpu-shaped --timeout=120s >/dev/null 2>&1 || true
n=$(kubectl get pod gpu-shaped -o jsonpath='{.spec.nodeName}'); note "gpu-shaped -> ${n:-<unscheduled>}"; [ "$n" = "$GPU" ] || fail "gpu-shaped did not land on the GPU node"

say "4. A pod with the selector but no toleration: must stay Pending (the taint repels it)"
kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata: {name: selector-only, labels: {kgpu.io/check: "1"}}
spec:
  restartPolicy: Never
  nodeSelector: {kgpu.io/gpu: "true"}
  containers: [{name: pause, image: registry.k8s.io/pause:3.10}]
YAML
sleep 5
phase=$(kubectl get pod selector-only -o jsonpath='{.status.phase}'); n=$(kubectl get pod selector-only -o jsonpath='{.spec.nodeName}')
reason=$(kubectl get events --field-selector involvedObject.name=selector-only -o jsonpath='{.items[-1:].message}' 2>/dev/null | cut -c1-160)
note "selector-only: phase=$phase node=${n:-<none>}"; note "scheduler says: $reason"
[ "$phase" = Pending ] && [ -z "$n" ] || fail "selector-only should be unschedulable"
