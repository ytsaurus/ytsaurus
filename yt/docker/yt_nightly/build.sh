#!/bin/bash

set -e

script_name=$0

ytsaurus_source_path="."
ytsaurus_build_path="."
output_path="."

registry=""
tag=""

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo]
                    [--ytsaurus-build-path /path/to/ytsaurus.build]
                    [--output-path /path/to/output]
EOF
    exit 1
}

if [[ $# -eq 0 ]]; then
    print_usage
fi

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --ytsaurus-source-path)
        ytsaurus_source_path="$2"
        shift 2
        ;;
        --ytsaurus-build-path)
        ytsaurus_build_path="$2"
        shift 2
        ;;
        --output-path)
        output_path="$2"
        shift 2
        ;;
        --registry)
        registry="$2"
        shift 2
        ;;
        --tag)
        tag="$2"
        shift 2
        ;;
        *)
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

ytserver_all_path="${ytsaurus_build_path}/yt/yt/server/all/ytserver-all"
ytserver_clickhouse="${ytsaurus_build_path}/yt/chyt/server/bin/ytserver-clickhouse"
ytserver_log_tailer="${ytsaurus_build_path}/yt/yt/server/log_tailer/bin/ytserver-log-tailer"
chyt_controller="${ytsaurus_source_path}/yt/chyt/controller/cmd/chyt-controller/chyt-controller"
init_operation_archive="${ytsaurus_source_path}/yt/python/yt/environment/init_operation_archive.py"
clickhouse_trampoline="${ytsaurus_source_path}/yt/chyt/trampoline/clickhouse-trampoline.py"
dockerfile="${ytsaurus_source_path}/yt/docker/yt_nightly/Dockerfile"

cp ${ytserver_all_path} ${output_path}
cp ${ytserver_clickhouse} ${output_path}
cp ${ytserver_log_tailer} ${output_path}
cp ${chyt_controller} ${output_path}
cp ${init_operation_archive} ${output_path}
cp ${clickhouse_trampoline} ${output_path}
cp ${dockerfile} ${output_path}

cp -r ${ytsaurus_build_path}/ytsaurus_python ${output_path}

cd ${output_path}

docker build -t ${registry}:${tag} .
