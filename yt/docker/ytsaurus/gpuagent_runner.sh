#!/bin/bash
set -euo pipefail

start_gpuagent() {
    if ! ls /dev/nvidia* &> /dev/null
    then
        echo "No nvidia devices found"
        return
    fi
    while true; do
        echo "Launching gpuagent..."
        gpuagent --tcp $(cat /etc/gpuagent_port.txt) || true
        sleep 5
    done
}

start_gpuagent &
GPUAGENT_PID=$!

exec "$@"
