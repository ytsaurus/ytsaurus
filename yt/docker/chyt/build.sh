#!/usr/bin/env bash

script_name=$0

image_tag="latest"
ytsaurus_source_path="."
chyt_build_path="."
output_path="."

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo (default: $ytsaurus_source_path)]
                    [--chyt-build-path /path/to/chyt.build (default: $chyt_build_path)]
                    [--output-path /path/to/output (default: $output_path)]
                    [--image-tag image_tag (default: $image_tag)]
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
        --chyt-build-path)
        chyt_build_path="$2"
        shift 2
        ;;
        --output-path)
        output_path="$2"
        shift 2
        ;;
        --image-tag)
        image_tag="$2"
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

ytserver_clickhouse="${chyt_build_path}/server/bin/ytserver-clickhouse"
clickhouse_trampoline="${ytsaurus_source_path}/yt/chyt/trampoline/clickhouse-trampoline.py"

chyt_credits="${ytsaurus_source_path}/yt/docker/chyt/credits"
dockerfile="${ytsaurus_source_path}/yt/docker/chyt/Dockerfile"
setup_script="${ytsaurus_source_path}/yt/docker/chyt/setup_cluster_for_chyt.sh"

mkdir ${output_path}/chyt

cp ${ytserver_clickhouse} ${output_path}/chyt
cp ${clickhouse_trampoline} ${output_path}/chyt

cp ${dockerfile} ${output_path}/chyt
cp ${setup_script} ${output_path}/chyt

mkdir ${output_path}/chyt/credits
cp -r ${chyt_credits}/* ${output_path}/chyt/credits

cd ${output_path}/chyt

docker build -t ytsaurus/chyt:${image_tag} .
