#!/bin/sh -e

usage()
{
    echo "$0 <node name> [<core_name>]"
    exit
}

if [ -n "$1" ]; then
    NODE_NAME=$1
else
    usage
fi

if [ -n "$2" ]; then
    CORE_NAME=$2
fi

if [ -z "$CORE_NAME" ]; then
    ssh "$NODE_NAME" ls -lt /yt/disk1/core
else
    scp "$NODE_NAME:/yt/disk1/core/$CORE_NAME" .
    gdb ~yt/build/bin/ytserver $CORE_NAME
fi

