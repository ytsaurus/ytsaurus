#!/bin/bash

set -e

# Matches exec node configuration in cluster.yaml
cluster_cpu=320
cluster_ram=1028966789939
cluster_disk=98956046499840
node_count=10000000

pool_name="systest"

yt list /
yt create scheduler_pool --attributes="{pool_tree=default;name=${pool_name}}"
yt set //sys/pool_trees/default/${pool_name}/@strong_guarantee_resources "{cpu=${cluster_cpu};memory=${cluster_ram}}"
yt set //sys/accounts/sys/@resource_limits/disk_space_per_medium/default $cluster_disk
yt set //sys/accounts/sys/@resource_limits/node_count $node_count
