#!/bin/bash

set -x -e

cluster=$1
if [[ "$cluster" == "" ]] ; then
    echo "Specify cluster name as the only command-line argument."
    exit 1
fi

if [[ "$(yt exists //sys/users/yt-clickhouse)" == "false" ]] ; then
    yt create user --attribute "{name=yt-clickhouse}"
    yt set //sys/accounts/sys/@acl/end '{permissions=[use];action=allow;subjects=[yt-clickhouse];}'
fi

export YT_PROXY=$cluster
yt create map_node //sys/clickhouse --ignore-existing --attributes '{acl=[{permissions=[read;write;use;administer;remove];action=allow;subjects=[yt-clickhouse];}]}'

yt create map_node //sys/clickhouse/geodata --ignore-existing

yt create document //sys/clickhouse/config --attributes "{value={}}" --ignore-existing
yt --proxy hahn get //sys/clickhouse/config | yt set //sys/clickhouse/config
yt set //sys/clickhouse/config/cluster_connection '{}'
yt remove //sys/clickhouse/config/cluster_connection

yt create document //sys/clickhouse/log_tailer_config --attributes "{value={}}" --ignore-existing
yt --proxy hahn get //sys/clickhouse/log_tailer_config | yt set //sys/clickhouse/log_tailer_config
yt set //sys/clickhouse/log_tailer_config/cluster_connection '{}'
yt remove //sys/clickhouse/log_tailer_config/cluster_connection

yt create document //sys/clickhouse/defaults --attributes '{value={}}' --ignore-existing
yt get --proxy hahn //sys/clickhouse/defaults | yt set //sys/clickhouse/defaults

yt create map_node //sys/clickhouse/bin --ignore-existing
yt create map_node //sys/bin --ignore-existing

for kind in ytserver-clickhouse ytserver-log-tailer clickhouse-trampoline ytserver-rpc-proxy yt-start-clickhouse-clique ; do
    yt create map_node //sys/bin/${kind} --ignore-existing
done

for kind in ytserver-clickhouse ytserver-log-tailer clickhouse-trampoline ytserver-rpc-proxy ; do
    yt link //sys/bin/${kind}/${kind} //sys/clickhouse/bin/${kind} --ignore-existing
done

for platform in linux darwin windows ; do
    yt link //sys/bin/yt-start-clickhouse-clique/yt-start-clickhouse-clique.${platform} //sys/clickhouse/bin/yt-start-clickhouse-clique.${platform} --force
done

yt create map_node //home/clickhouse-kolkhoz --ignore-existing --attributes '{acl=[{subjects=[everyone];action=allow;permissions=[read;write;remove;create;use]}]}'
yt link --link-path //sys/clickhouse/kolkhoz --target-path //home/clickhouse-kolkhoz --ignore-existing

if [[ "$(yt exists //sys/accounts/clickhouse-kolkhoz)" == "false" ]] ; then
    yt create account --attributes '{name=clickhouse-kolkhoz; acl=[{permissions=[use];action=allow;subjects=[everyone]}]}'
    yt set //sys/accounts/clickhouse-kolkhoz/@resource_limits/tablet_count $((100 * 1000))
    yt set //sys/accounts/clickhouse-kolkhoz/@resource_limits/disk_space_per_medium/default $((50 * 1024 * 1024 * 1024 * 1024))
    yt set //sys/accounts/clickhouse-kolkhoz/@resource_limits/disk_space_per_medium/ssd_blobs $((50 * 1024 * 1024 * 1024 * 1024))
    yt set //sys/accounts/clickhouse-kolkhoz/@resource_limits/disk_space_per_medium/ssd_journals $((1 * 1024 * 1024 * 1024 * 1024))
    yt set //sys/accounts/clickhouse-kolkhoz/@resource_limits/node_count $((10 * 1000))
    yt set //sys/accounts/clickhouse-kolkhoz/@resource_limits/chunk_count $((10000 * 1000))
fi

if [[ "$(yt exists //sys/users/yt-clickhouse-cache)" == "false" ]] ; then
    yt create user --attribute "{name=yt-clickhouse-cache}"
    yt add-member --member yt-clickhouse-cache --group superusers
fi

if [[ "$(yt exists //sys/users/yt-log-tailer)" == "false" ]] ; then
    yt create user --attribute "{name=yt-log-tailer}"
fi

if [[ "$(yt exists //sys/tablet_cell_bundles/clickhouse-kolkhoz)" == "false" ]] ; then
    yt create tablet_cell_bundle --attributes "{options={changelog_account=clickhouse-kolkhoz;snapshot_account=clickhouse-kolkhoz};name=clickhouse-kolkhoz;acl=[{permissions=[use]; subjects=[everyone];action=allow}]}"
    ../dynamic_tables/manage_tablet_cells.py --config '{clickhouse-kolkhoz=8}' create
fi

yt create map_node //sys/clickhouse/cliques --ignore-existing --attributes '{account=clickhouse-kolkhoz; tablet_cell_bundle=clickhouse-kolkhoz}'

./build_geodata.sh

if [[ "$(yt get //sys/schemas/orchid/@acl)" == "[]" ]] ; then
    yt set //sys/schemas/orchid/@acl/end '{permissions=[create];action=allow;subjects=[yt-clickhouse]}';
fi

echo "!!! Clickhouse is set up. Now run deploy.py to deploy the latest CH release binary."

