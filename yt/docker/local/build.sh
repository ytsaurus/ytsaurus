#!/usr/bin/env bash

script_name=$0

image_tag=""
ytsaurus_source_path="."
ytsaurus_build_path="."
output_path="."

image_tag=stable

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo (default: $ytsaurus_source_path)]
                    [--ytsaurus-build-path /path/to/ytsaurus.build (default: $ytsaurus_build_path)]
                    [--output-path /path/to/output (default: $output_path)]
                    [--image-tag some-tag (default: $image_tag)]

EOF
    exit 0
}

# Parse options
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

ytserver_all="${ytsaurus_build_path}/yt/yt/server/all/ytserver-all"
ytserver_all_credits="${ytsaurus_source_path}/yt/docker/ytsaurus/credits/ytserver-all.CREDITS"
dockerfile="${ytsaurus_source_path}/yt/docker/local/Dockerfile"
configure_file="${ytsaurus_source_path}/yt/docker/local/configure.sh"
start_file="${ytsaurus_source_path}/yt/docker/local/start.sh"

cp ${ytserver_all} ${output_path}
cp -r ${ytsaurus_build_path}/ytsaurus_python ${output_path}

cp ${ytserver_all_credits} ${output_path}
cp ${dockerfile} ${output_path}
cp ${configure_file} ${output_path}
cp ${start_file} ${output_path}

cd ${output_path}

docker build -t ytsaurus/local:${image_tag} .
