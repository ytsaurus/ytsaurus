#!/bin/bash

set -x -e

cluster=$1
if [[ "$cluster" == "" ]] ; then
    echo "Specify cluster name as the only command-line argument."
    exit 1
fi

export YT_PROXY=$cluster
yt create map_node //sys/clickhouse --ignore-existing
yt create map_node //sys/clickhouse/geodata --ignore-existing
yt create document //sys/clickhouse/config --attributes "{value={}}" --ignore-existing
yt set //sys/clickhouse/config <config.yson
if [[ "$(yt exists //sys/@cluster_connection)" == "true" ]] ; then
    yt get //sys/@cluster_connection | yt set //sys/clickhouse/config/cluster_connection
else
    echo "!!! //sys/@cluster_connection is missing; make sure to specify //sys/clichouse/config/cluster_connection by yourself."
fi
yt create map_node //sys/clickhouse/bin --ignore-existing
yt create map_node //sys/clickhouse/cliques --ignore-existing

if [[ "$(yt exists //sys/users/yt-clickhouse)" == "false" ]] ; then
    yt create user --attribute "{name=yt-clickhouse}"
fi

yt set //sys/clickhouse/@acl/end '{permissions=[read;write;use;administer;remove]; action=allow; subjects=[yt-clickhouse]}'
yt set //sys/accounts/sys/@acl/end '{permissions=[use]; action=allow; subjects=[yt-clickhouse]}'

echo "!!! Clickhouse is set up. Now run deploy.sh to deploy the latest CH release binary."

