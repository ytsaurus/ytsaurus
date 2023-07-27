#!/usr/bin/env bash
set -eu
source scheduler_simulator_analysis_config.sh

INPUT_DIR="${1%/}"
OUTPUT_DIR="${2%/}"
EVENT_LOG_TABLE_PATH="${3-}"
CLUSTER_TMP_DIR="//tmp/$YT_USER/simulator_analysis/$(basename "$OUTPUT_DIR")/$RANDOM"

DATA_DIR="$OUTPUT_DIR/data"

echo "Input dir: $INPUT_DIR"
echo "Output dir: $OUTPUT_DIR"
echo "Temporary dir on cluster ($YT_PROXY): $CLUSTER_TMP_DIR"

echo "Create tmp folder on cluster '$CLUSTER_TMP_DIR'"
yt create -ir map_node "$CLUSTER_TMP_DIR"

if [ -z "$EVENT_LOG_TABLE_PATH" ]; then
    echo "Upload data to YT" >&2
    pv "$INPUT_DIR/$SCHEDULER_EVENT_LOG_FILE" | yt write --proxy "$YT_PROXY" --format yson "$CLUSTER_TMP_DIR/event_log"
else
    echo "Copying event log table" >&2
    yt copy --proxy "$YT_PROXY" "$EVENT_LOG_TABLE_PATH" "$CLUSTER_TMP_DIR/event_log"
fi

echo "Extract fairshare info" >&2
extract_fairshare_info \
    --input "$CLUSTER_TMP_DIR/event_log" \
    --no_filter_by_timestamp \
    --output_dir "$CLUSTER_TMP_DIR/" &

echo "Extract utilization info" >&2
extract_utilization_info \
    --input "$CLUSTER_TMP_DIR/event_log" \
    --no_filter_by_timestamp \
    --output_dir "$CLUSTER_TMP_DIR/" &

echo "Extract job preemption info" >&2
extract_preemption_info \
    --input "$CLUSTER_TMP_DIR/event_log" \
    --no_filter_by_timestamp \
    --output_dir "$CLUSTER_TMP_DIR/" &

wait

echo "Read data" >&2
mkdir -p "$DATA_DIR"

cp "$INPUT_DIR/operations_stats.csv" "$DATA_DIR/"
yt read "$CLUSTER_TMP_DIR/info_usage_ratio_pools" --format json > "$DATA_DIR/usage_pools.json"
yt read "$CLUSTER_TMP_DIR/info_demand_ratio_pools" --format json > "$DATA_DIR/demand_pools.json"
yt read "$CLUSTER_TMP_DIR/info_fair_share_ratio_pools" --format json > "$DATA_DIR/fair_share_pools.json"
yt read "$CLUSTER_TMP_DIR/info_usage_ratio_operations" --format json > "$DATA_DIR/usage_operations.json"
yt read "$CLUSTER_TMP_DIR/info_demand_ratio_operations" --format json > "$DATA_DIR/demand_operations.json"
yt read "$CLUSTER_TMP_DIR/info_fair_share_ratio_operations" --format json > "$DATA_DIR/fair_share_operations.json"
yt read "$CLUSTER_TMP_DIR/cluster_utilization" --format json > "$DATA_DIR/cluster_utilization.json"
yt read "$CLUSTER_TMP_DIR/preemption_events" --format yson > "$DATA_DIR/preemption_events.yson"
yson_to_csv "$DATA_DIR/preemption_events.yson" "$DATA_DIR/preemption_events.csv"
