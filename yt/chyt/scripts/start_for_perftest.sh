#!/bin/bash

if [[ "$2" == "" ]] ; then 
    echo "Specify operation alias and binary as two command-line arguments"
fi

yt clickhouse start-clique --instance-count 2 --cpu-limit 4 --operation-alias $1 --spec '{tasks={instances={set_container_cpu_limit=%true}}}' --cypress-ytserver-clickhouse-path $2 --abort-existing
