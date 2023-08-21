#!/bin/bash

invocation_id=""

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --invocation_id)
        invocation_id=$2
        shift 2
        ;;
        *)
        echo "Unknown argument $1"
        exit 1
        ;;
    esac
done

namespace=$(date +"%Y%m%d-%H%M")${invocation_id:0:2}

echo $namespace
