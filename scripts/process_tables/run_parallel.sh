#!/bin/sh -ex

if [ -z "$1" -o -z "$2" -o -z "$3" ]; then
    echo "Usage: run_parallel.sh {binary} {number_of_threads} {log}"
    exit 1
fi

for i in `seq 1 ${2}`; do
    ${1} 1>>${3} 2>&1 &
done
