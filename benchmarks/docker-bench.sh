#!/usr/bin/env bash
# Build and run the persistent (file-mode) benchmark for the Zhensegg broker
# inside an Ubuntu Docker container.
#
# Usage:
#   bash benchmarks/docker-bench.sh                 # defaults (30s persistent soak)
#   SECS=10 PRODUCERS=4 CONSUMERS=4 bash benchmarks/docker-bench.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "[docker-bench] building image zhensegg-bench ..."
docker build -f benchmarks/Dockerfile -t zhensegg-bench .

ARGS=()
for v in ADDR HTTP_ADDR CORES RING_MB RING_FILE TOPIC PRODUCERS CONSUMERS BATCH PAYLOAD MSGS SECS; do
    if [ -n "${!v:-}" ]; then
        ARGS+=(-e "${v}=${!v}")
    fi
done

echo "[docker-bench] running persistent file-mode benchmark ..."
if docker info --format '{{json .SecurityOptions}}' 2>/dev/null | grep -q seccomp; then
    echo "[docker-bench] seccomp enabled — adding --privileged so io_uring may work"
    docker run --rm --privileged "${ARGS[@]}" zhensegg-bench
else
    docker run --rm "${ARGS[@]}" zhensegg-bench
fi
