#!/bin/bash

set -ex

timestamp="$(date "+%Y%m%d_%H%M%S")"

for fixture in data_*.yson
do
    ./benchmark-yson-bindings.py "${fixture}" | tee "${fixture}.log.${timestamp}"
done

