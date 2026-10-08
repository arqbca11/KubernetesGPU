#!/usr/bin/env bash
# Apply the Phase 2 manifests to the kind cluster and wait for them.
set -euo pipefail
cd "$(dirname "$0")/.."
kubectl config use-context kind-kgpu >/dev/null
kubectl apply -k deploy/k8s
kubectl -n kgpu rollout status statefulset/postgres --timeout=180s
kubectl -n kgpu rollout status deployment/scheduler --timeout=180s
kubectl -n kgpu get pods -o wide
