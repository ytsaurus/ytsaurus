#!/bin/sh -e

usage()
{
    echo "$0 <node name> <system_folder> [<change_log_name>]"
    exit
}

NODE_NAME=$1; shift || usage 
SYSTEM_FOLDER=$1; shift || usage 

LOG_NAME=$1

if [ -z "$LOG_NAME" ]; then
    ssh "$NODE_NAME" ls -lt /yt/disk2/$SYSTEM_FOLDER/changelogs
else
    scp "$NODE_NAME:/yt/disk2/$SYSTEM_FOLDER/changelogs/$LOG_NAME" .
    scp "$NODE_NAME:/yt/disk2/$SYSTEM_FOLDER/changelogs/$LOG_NAME.index" .
fi

