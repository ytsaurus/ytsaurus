#!/bin/bash

base=$1
if [[ "$base" == "" ]] ; then base=4200; fi

user_hash=$(echo $USER | shasum | head -c 4)
clique_id="0-0-0-$user_hash"

echo 'Removing old logs with "rm *.log"'

rm *.log
touch query.sql

if [[ "$YT_PROXY" == "" ]] ; then YT_PROXY=hahn; fi

if [ ! -e config.yson ] ;
then
    echo "Generating default config for $YT_PROXY"
    python3 generate_default_config.py > config.yson
fi

echo "Starting clique $clique_id on $YT_PROXY"

YT_JOB_COOKIE=0 /home/dakovalkov/ytarc/build/yt/chyt/server/bin/ytserver-clickhouse \
    --config config.yson \
    --instance-id "abab-cdcd-efef-$base" \
    --clique-id $clique_id \
    --rpc-port $(($base + 42)) \
    --monitoring-port $(($base + 43)) \
    --tcp-port $(($base + 44)) \
    --http-port $(($base + 45))
