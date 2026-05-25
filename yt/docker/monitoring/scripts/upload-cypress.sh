#!/bin/bash
set -ex

DASHBOARDS_DIR="/ytsaurus/monitoring/generated/grafana"

while [[ $# -gt 0 ]]; do
    case $1 in
        --dashboards-dir)
            DASHBOARDS_DIR="$2"
            shift 2
            ;;
        --yt-dashboards-dir)
            CYPRESS_DASHBOARDS_PATH="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

if [[ -z "$CYPRESS_DASHBOARDS_PATH" ]]; then
    echo "Error: CYPRESS_DASHBOARDS_PATH is not set and --yt-dashboards-dir was not provided"
    exit 1
fi

if [[ ! -d "$DASHBOARDS_DIR" ]]; then
    echo "Error: Dashboards directory $DASHBOARDS_DIR does not exist"
    exit 1
fi

yt create map_node "$CYPRESS_DASHBOARDS_PATH" --ignore-existing --recursive

for dashboard_file in "$DASHBOARDS_DIR"/*.json; do
    if [[ -f "$dashboard_file" ]]; then
        echo "Uploading dashboard to Cypress: $(basename "$dashboard_file")"
        dashboard_name=$(basename "$dashboard_file" .json | sed 's/^ytsaurus-//')
        yt create document "$CYPRESS_DASHBOARDS_PATH/$dashboard_name" --ignore-existing
        if cat "$dashboard_file" | yt set "$CYPRESS_DASHBOARDS_PATH/$dashboard_name" --format json; then
            echo "Successfully uploaded dashboard to Cypress: $(basename "$dashboard_file")"
        else
            echo "Failed to upload dashboard to Cypress: $(basename "$dashboard_file")"
            exit 1
        fi
    fi
done

echo "All dashboards uploaded successfully to Cypress"
