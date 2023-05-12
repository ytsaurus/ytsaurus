#!/bin/bash

set -e

n=20
attempt=0

until [ $attempt -eq $n ] || kubectl get pod | grep tester | grep Completed; do
	echo "Waiting for tester to complete, attempt ${attempt}"
    sleep 30
	let attempt=attempt+1
done

if [ $attempt -eq $n ]; then
	exit 1
fi

