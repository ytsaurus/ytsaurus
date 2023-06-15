#!/bin/bash

set -e

n=40
attempt=0
namespace=""

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --namespace)
        namespace="$2"
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

until [ $attempt -eq $n ] || kubectl get pod ${nsflags} | grep tester | grep Completed; do
	echo "Waiting for tester to complete, attempt ${attempt}"
    sleep 30
	let attempt=attempt+1
done

if [ $attempt -eq $n ]; then
	exit 1
fi

