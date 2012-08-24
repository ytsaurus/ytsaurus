#!/bin/bash

if [[ "$1" == "fork" ]]; then
    $0 > /dev/null 2> /dev/null &
    disown
    exit
fi

PERF=/usr/bin/perf_2.6.38-15
DATA=/yt/disk1/profile

mkdir -p $DATA

while [[ -f $DATA/_COLLECT_PROFILE ]]; do
    _PATH=$DATA/$(date +%Y%m%d)/$(date +%H00)
    _FILE=$(date +%H%M%S)
    mkdir -p $_PATH
    $PERF record -g -T -a -o $_PATH/$_FILE sleep 5
done
