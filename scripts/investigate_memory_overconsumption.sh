#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Usage: ./investigate_memory_overconsumption.sh <node_address> <job_id>"
    exit 1
fi

scp investigate_memory_overconsumption_remote_script.sh $1:~/ 
ssh $1 "bash investigate_memory_overconsumption_remote_script.sh $2"

