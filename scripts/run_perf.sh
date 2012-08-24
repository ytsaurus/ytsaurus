#!/bin/bash

PERF=/usr/bin/perf_2.6.38-15
DATA=/yt/disk1/profile

mkdir -p $DATA

while [[ -f $DATA/_COLLECT_PROFILE ]]; do
    $PERF -g -T -a -o $DATA/$(date +%Y%m%d_%H%M%S) sleep 5
end
