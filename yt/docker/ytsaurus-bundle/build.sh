#!/usr/bin/env bash

script_name=$0

ytsaurus_source_path="."

ytsaurus_tag=""
query_tracker_tag=""
strawberry_tag=""

output_path="."

image_tag=""

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-source-path /path/to/repo (default: $ytsaurus_source_path)]
                    [--ytsaurus-tag ytsaurus/ytsaurus image tag]
                    [--query-tracker-tag ytsaurus/query-tracker tag]
                    [--strawberry-tag ytsaurus/strawberry image tag]
                    [--output-path /path/to/output (default: $output_path)]
EOF
    exit 1
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --ytsaurus-source-path)
        ytsaurus_source_path="$2"
        shift 2
        ;;
        --ytsaurus-tag)
        ytsaurus_tag="$2"
        shift 2
        ;;
        --query-tracker-tag)
        query_tracker_tag="$2"
        shift 2
        ;;
        --strawberry-tag)
        strawberry_tag="$2"
        shift 2
        ;;
        --output-path)
        output_path="$2"
        shift 2
        ;;
        -h|--help)
        print_usage
        shift
        ;;
        *)  # unknown option
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

dockerfile="${ytsaurus_source_path}/yt/docker/ytsaurus-bundle/Dockerfile"

cp ${dockerfile} ${output_path}

cd ${output_path}

docker build -t ytsaurus/ytsaurus-bundle:${ytsaurus_tag} . --build-arg YTSAURUS_TAG=${ytsaurus_tag} --build-arg QUERY_TRACKER_TAG=${query_tracker_tag} --build-arg STRAWBERRY_TAG=${strawberry_tag}
