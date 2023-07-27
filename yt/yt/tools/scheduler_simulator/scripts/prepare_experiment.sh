#!/usr/bin/env bash
set -eu

echo "CLUSTER='${CLUSTER:=hahn}'"
echo "USER_NAME='${USER_NAME:=$(whoami)}'"
echo "DATE='${DATE:=2018-09-04}'"
echo "LAST_HOUR='${LAST_HOUR:=20:00:00}'"
echo "LENGTH_HOURS='${LENGTH_HOURS:=6}'"
echo "LOCAL_DIR='${LOCAL_DIR:=/home/${USER_NAME}/scheduler_simulator/measurements/${CLUSTER}/${DATE}}'"
echo "CLUSTER_DIR='${CLUSTER_DIR:=//home/${USER_NAME}/measurements/${CLUSTER}/${DATE}}'"
echo "CLUSTER_TMP_DIR='${CLUSTER_TMP_DIR:=//tmp/${USER_NAME}/measurements/${CLUSTER}/${DATE}}'"
echo "EVENT_LOG_NAME='${EVENT_LOG_NAME:=scheduler_event_log}'"
echo "EVENT_LOG_PATH='${EVENT_LOG_PATH:=$CLUSTER_TMP_DIR/$EVENT_LOG_NAME}'"
echo ""

####################################################

export YT_PROXY="$CLUSTER"

echo "Create tmp folder $CLUSTER_TMP_DIR"
yt create -ir map_node "$CLUSTER_TMP_DIR"

echo "Extracting log entries" >&2
extract_event_log \
    --output_dir "$CLUSTER_TMP_DIR" \
    --name "$EVENT_LOG_NAME" \
    --hours "$LENGTH_HOURS" \
    --finish_time "$DATE/$LAST_HOUR" \
    --prohibit_node_tags "external" "CLOUD" "cloud" "cloud_vla" "external_vla" \
    --allow_node_regexes "n[0-9]+-sas.hahn.yt.yandex.net:9012" \
    --prohibit_node_regexes "sas2-[0-9]+-[a-z0-9]{3}-sas-yt-hahn-node-[0-9]+-[0-9]+.gencfg-c.yandex.net:9012" \
    --exclude_users "robot-scraperoveryt"

echo "Create experiment folder $CLUSTER_DIR" >&2
yt create -ir map_node "$CLUSTER_DIR"

echo "Extract operations trace" >&2
extract_operation_trace \
    --output_dir "$CLUSTER_DIR/" \
    --hours "$LENGTH_HOURS" \
    --input "${EVENT_LOG_PATH}_default" \
    --finish_time "$DATE/$LAST_HOUR"

echo "Extract operation stats" >&2
extract_operation_stats \
    --output_dir "$CLUSTER_DIR/" \
    --hours "$LENGTH_HOURS" \
    --input "${EVENT_LOG_PATH}_default" \
    --finish_time "$DATE/$LAST_HOUR"

echo "Extract fairshare info" >&2
extract_fairshare_info \
    --output_dir "$CLUSTER_DIR/" \
    --hours "$LENGTH_HOURS" \
    --input "${EVENT_LOG_PATH}_default" \
    --finish_time "$DATE/$LAST_HOUR"

echo "Extract utilization info" >&2
extract_utilization_info \
    --output_dir "$CLUSTER_DIR/" \
    --hours "$LENGTH_HOURS" \
    --input "${EVENT_LOG_PATH}_nodes_info" \
    --finish_time "$DATE/$LAST_HOUR"

echo "Extract operations prepare times" >&2
extract_operation_prepare_times \
    --output_dir "$CLUSTER_DIR/" \
    --hours "$LENGTH_HOURS" \
    --input "${EVENT_LOG_PATH}_default" \
    --finish_time "$DATE/$LAST_HOUR"

echo "Extract job preemption info" >&2
extract_preemption_info \
    --output_dir "$CLUSTER_DIR/" \
    --hours "$LENGTH_HOURS" \
    --input "${EVENT_LOG_PATH}_default" \
    --finish_time "$DATE/$LAST_HOUR"

echo "Create local experiment folder $LOCAL_DIR" >&2
mkdir -p "$LOCAL_DIR"
cd "$LOCAL_DIR"

echo "Read data" >&2
yt read "$CLUSTER_DIR/scheduler_operations" --format yson > operations.compressed.yson
yt read "$CLUSTER_DIR/scheduler_operation_prepare_times" --format yson > operation_preparation.yson

mkdir -p production
cd production
yt read "$CLUSTER_DIR/info_usage_ratio_pools" --format json > usage_pools.json
yt read "$CLUSTER_DIR/info_demand_ratio_pools" --format json > demand_pools.json
yt read "$CLUSTER_DIR/info_fair_share_ratio_pools" --format json > fair_share_pools.json
yt read "$CLUSTER_DIR/info_usage_ratio_operations" --format json > usage_operations.json
yt read "$CLUSTER_DIR/info_demand_ratio_operations" --format json > demand_operations.json
yt read "$CLUSTER_DIR/info_fair_share_ratio_operations" --format json > fair_share_operations.json
yt read "$CLUSTER_DIR/scheduler_operation_stats" --format yson > operations_stats.yson
yt read "$CLUSTER_DIR/cluster_utilization" --format json > cluster_utilization.json
yt read "$CLUSTER_DIR/preemption_events" --format yson > preemption_events.yson
yson_to_csv operations_stats.yson operations_stats.csv
yson_to_csv preemption_events.yson preemption_events.csv
cd ..

echo "Convert to binary format" >&2
convert_to_binary_format.sh operations
