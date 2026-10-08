#!/usr/bin/env bash
# Create the Phase 2 kind cluster (or reuse it) and verify the stand-in GPU
# node is labelled and tainted. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
if kind get clusters 2>/dev/null | grep -qx kgpu; then
  echo "kind cluster 'kgpu' already exists"
else
  kind create cluster --config deploy/kind/cluster.yaml --wait 120s
fi
kubectl config use-context kind-kgpu >/dev/null
echo "== nodes"
kubectl get nodes -L kgpu.io/role,kgpu.io/gpu
echo "== taints"
kubectl get nodes -o custom-columns='NAME:.metadata.name,TAINTS:.spec.taints[*].key'
