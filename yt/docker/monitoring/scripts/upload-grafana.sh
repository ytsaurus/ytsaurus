#!/bin/bash
set -ex

if [[ -z "$GRAFANA_TOKEN" ]]; then
    echo "Error: GRAFANA_TOKEN is not set"
    exit 1
fi

DASHBOARDS_DIR="/ytsaurus/monitoring/generated/grafana"

while [[ $# -gt 0 ]]; do
    case $1 in
        --dashboards-dir)
            DASHBOARDS_DIR="$2"
            shift 2
            ;;
        --grafana-base-url)
            GRAFANA_BASE_URL="$2"
            shift 2
            ;;
        --grafana-datasource-uid)
            GRAFANA_DATASOURCE_UID="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

if [[ -z "$GRAFANA_BASE_URL" ]]; then
    echo "Error: GRAFANA_BASE_URL is not set and --grafana-base-url was not provided"
    exit 1
fi

if [[ -z "$GRAFANA_DATASOURCE_UID" ]]; then
    echo "Error: GRAFANA_DATASOURCE_UID is not set and --grafana-datasource-uid was not provided"
    exit 1
fi

if [[ ! -d "$DASHBOARDS_DIR" ]]; then
    echo "Error: Dashboards directory $DASHBOARDS_DIR does not exist"
    exit 1
fi

for dashboard_file in "$DASHBOARDS_DIR"/*.json; do
    if [[ -f "$dashboard_file" ]]; then
        echo "Uploading dashboard: $(basename "$dashboard_file")"
        
        temp_file=$(mktemp)
        sed "s/\${PROMETHEUS_DS_UID}/$GRAFANA_DATASOURCE_UID/g" "$dashboard_file" > "$temp_file"
        
        payload_file=$(mktemp)
        jq -n --argjson dash "$(<"$temp_file")" '{dashboard: $dash, overwrite: true}' > "$payload_file"
        
        response=$(curl -s -X POST \
            -H "Authorization: Bearer $GRAFANA_TOKEN" \
            -H "Content-Type: application/json" \
            -d @"$payload_file" \
            "$GRAFANA_BASE_URL/api/dashboards/db")
        
        if echo "$response" | grep -q '"status":"success"'; then
            echo "Successfully uploaded dashboard: $(basename "$dashboard_file")"
        else
            echo "Failed to upload dashboard: $(basename "$dashboard_file")"
            echo "Response: $response"
            rm "$temp_file"
            rm "$payload_file"
            exit 1
        fi
        
        rm "$temp_file"
        rm "$payload_file"
    fi
done

echo "All dashboards uploaded successfully to Grafana"
