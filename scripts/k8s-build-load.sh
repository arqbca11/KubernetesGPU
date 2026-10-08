#!/usr/bin/env bash
# Build the images and load them into the kind cluster. Nodes cannot pull from
# the laptop's Docker, so every local image must be loaded explicitly.
#   scripts/k8s-build-load.sh            all three images
#   scripts/k8s-build-load.sh scheduler  one of: scheduler worker shardsim
# (plain if/else: macOS ships bash 3.2, which lacks case fallthrough)
set -euo pipefail
cd "$(dirname "$0")/.."
want="${1:-all}"
build() { echo "== build $1"; docker build -q "${@:2}" >/dev/null; echo "== load $1 into kind"; kind load docker-image "$1" --name kgpu >/dev/null; }
if [ "$want" = all ] || [ "$want" = scheduler ]; then build kgpu-scheduler:dev -f scheduler/Dockerfile -t kgpu-scheduler:dev . ; fi
if [ "$want" = all ] || [ "$want" = worker ];    then build kgpu-worker:dev -t kgpu-worker:dev worker/ ; fi
if [ "$want" = all ] || [ "$want" = shardsim ];  then build kgpu-shardsim:dev -f shard/Dockerfile -t kgpu-shardsim:dev . ; fi
