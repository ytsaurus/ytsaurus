#!/bin/bash
set -euo pipefail

GPU_PROVIDER="${YT_GPU_PROVIDER:-nvidia}"

start_gpuagent() {
    if ! ls /dev/nvidia* &> /dev/null
    then
        echo "No nvidia devices found"
        return
    fi
    while true; do
        echo "Launching gpuagent..."
        gpuagent --tcp 23105 --gpu-provider "${GPU_PROVIDER}" || true
        sleep 5
    done
}

start_gpuagent &
GPUAGENT_PID=$!

exec "$@"
