#!/bin/sh -ex

if [ -z "$1" -o -z "$2" -o -z "$3" ]; then
    echo "Usage: run_parallel.sh {binary} {number_of_threads} {log}"
    exit 1
fi

PIDS=""
for i in `seq 1 ${2}`; do
    ${1} 1>>${3} 2>&1 &
    PIDS="$PIDS $!"
done

cleanup() {
    for pid in $PIDS; do
        if ps -p $pid > /dev/null; then
            kill -2 $pid
        fi
    done
}

trap cleanup SIGINT

set +x
while true; do
    ALIVE=0
    for pid in $PIDS; do
        if ps -p $pid > /dev/null; then
            ALIVE=1
        fi
    done
    if [ "$ALIVE" = "1" ]; then
        sleep 1
    else
        break
    fi
done
set -x
