#!/usr/bin/env bash

# This script builds artifacts required for the yql agent. It uses docker to build system python udfs.

# Required environment variables:
# YTSAURUS_SOURCE_PATH - path to the ytsaurus source root. Needed to build yql agent.
# YDB_SOURCE_PATH - path to the ydb source root. Needed to build everything else.
# YQL_BUILD_PATH - path to the build directory. All artifacts will be placed here.
# BUILD_FLAGS - flags to pass to ya make when building.

set -eux
shopt -s expand_aliases

build_python_udfs="yes"

print_usage() {
    cat << EOF
Usage: $0 [-h|--help]
          [--build-python-udfs (default: $build_python_udfs)]
EOF
    exit 1
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --build-python-udfs)
        build_python_udfs="$2"
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

export YTSAURUS_SOURCE_PATH=$(realpath $YTSAURUS_SOURCE_PATH)
export YDB_SOURCE_PATH=$(realpath $YDB_SOURCE_PATH)
export YQL_BUILD_PATH=$(realpath $YQL_BUILD_PATH)

mkdir -p $YQL_BUILD_PATH

# Build yql agent binary.
${YTSAURUS_SOURCE_PATH}/ya make -T ${BUILD_FLAGS} --ignore-recurses --output=${YQL_BUILD_PATH} ${YTSAURUS_SOURCE_PATH}/yt/yql/agent/bin

# Build required binaries and libraries.
for path in "ydb/library/yql/tools/mrjob" \
            "ydb/library/yql/yt/dynamic" \
            "ydb/library/yql/yt/dq_vanilla_job" \
            "ydb/library/yql/yt/dq_vanilla_job.lite" \
            "ydb/library/yql/udfs/logs/dsv"
do
    ${YDB_SOURCE_PATH}/ya make -T ${BUILD_FLAGS} --ignore-recurses --output=${YQL_BUILD_PATH} ${YDB_SOURCE_PATH}/$path
done

# Build common yql udfs.
for udf_name in compress_base \
                datetime2 \
                digest \
                file \
                histogram \
                hyperloglog \
                hyperscan \
                ip_base \
                json \
                json2 \
                math \
                pire \
                protobuf \
                re2 \
                set \
                stat \
                streaming \
                string \
                top \
                topfreq \
                unicode_base \
                url_base \
                yson2
do
    ${YDB_SOURCE_PATH}/ya make -T ${BUILD_FLAGS} --ignore-recurses -DSTRIP=yes --output=${YQL_BUILD_PATH} ${YDB_SOURCE_PATH}/ydb/library/yql/udfs/common/${udf_name}
    strip --remove-section=.gnu_debuglink ${YDB_SOURCE_PATH}/ydb/library/yql/udfs/common/${udf_name}/*.so
done

if [ "$build_python_udfs" == "yes" ]; then
  # Build yql system python udfs inside a docker container.
  docker container run --rm --name yql-python-udfs-build \
    -v $YTSAURUS_SOURCE_PATH:/ytsaurus \
    -v $YDB_SOURCE_PATH:/ydb \
    -v $YQL_BUILD_PATH:/yql_build \
    --env YDB_SOURCE_PATH=/ydb \
    --env YQL_BUILD_PATH=/yql_build \
    --env "BUILD_FLAGS=$BUILD_FLAGS" \
    mirror.gcr.io/ubuntu:focal \
    /bin/bash -c \
    "apt update && \
     DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y software-properties-common build-essential && \
     DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC add-apt-repository -y ppa:deadsnakes/ppa && \
     apt update && \
     DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y python3.8-dev python3.9-dev python3.10-dev python3.11-dev python3.12-dev && \
     /ytsaurus/yt/docker/ya-build/query-tracker/build_system_python_udfs.sh"
fi

# Change ownership of files to the current user. Useful if building locally.
if [ $(id -u) -ne 0 ]; then
  sudo chown -R $(id -u):$(id -g) $YQL_BUILD_PATH
fi

# Copy all shared libraries to a single directory
mkdir -p ${YQL_BUILD_PATH}/yql_shared_libraries/yql
find ${YQL_BUILD_PATH} -name 'lib*.so' -print0 | xargs -0 -I '{}' cp -n '{}' ${YQL_BUILD_PATH}/yql_shared_libraries/yql
