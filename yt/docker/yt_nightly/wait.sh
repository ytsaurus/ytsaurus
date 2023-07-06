#!/bin/bash

set -e

wait_minutes=20
attempt=0
namespace=""
name="tester"

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --namespace)
        namespace="$2"
        shift 2
        ;;
        --name)
        name="$2"
        shift 2
        ;;
        --wait-minutes)
        wait_minutes="$2"
        shift 2
        ;;
        *)
        echo "Unknown argument $1"
        exit 1
        ;;
    esac
done

if [[ $namespace != "" ]]; then
nsflags="-n ${namespace}"
fi

until [ $attempt -eq $wait_minutes ] || kubectl get pod ${nsflags} | grep $name | grep Completed; do
	echo "Waiting for test to complete, ${attempt}mins/${wait_minutes}mins"
    sleep 60
	let attempt=attempt+1
done

if [ $attempt -eq $wait_minutes ]; then
	exit 1
fi

