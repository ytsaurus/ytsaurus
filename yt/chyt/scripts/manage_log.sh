#!/bin/bash

if [[ "$2" == "" ]] ; then
    echo "Specify two arguments: clique alias without asterisk and 0/1 to disable/enable logging  " >&2
    exit 1
fi

if [[ "$2" == "1" ]] ; then
    bool="%false"
    cmd="mount"
else 
    bool="%true"
    cmd="unmount"
fi

set -x

yt set //sys/clickhouse/kolkhoz/$1/@disable_log "$bool"

for t in clickhouse{,.debug}.log{,.ordered_by_trace_id} ;
do
    yt ${cmd}-table //sys/clickhouse/kolkhoz/$1/${t} --sync
done
