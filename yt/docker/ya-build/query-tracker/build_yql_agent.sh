#!/usr/bin/env bash

# This script builds artifacts required for the yql agent. It uses docker to build system python udfs.

# Required environment variables:
# YTSAURUS_SOURCE_PATH - path to the ytsaurus source root. Needed to build yql agent.
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
export YQL_BUILD_PATH=$(realpath $YQL_BUILD_PATH)

mkdir -p $YQL_BUILD_PATH

CORE_TARGETS=(
    # Build yql agent binary.
    "${YTSAURUS_SOURCE_PATH}/yt/yql/agent/bin"
    
    # Build mrjob binary.
    "${YTSAURUS_SOURCE_PATH}/yt/yql/tools/mrjob"
    
    # Build mrjob udfs.
    "${YTSAURUS_SOURCE_PATH}/contrib/libs/libidn/dynamic"
    "${YTSAURUS_SOURCE_PATH}/contrib/libs/libiconv/dynamic"
    
    # Build required binaries and libraries
    "${YTSAURUS_SOURCE_PATH}/yt/yql/plugin/dynamic"
    "${YTSAURUS_SOURCE_PATH}/yt/yql/dq_vanilla_job"
    "${YTSAURUS_SOURCE_PATH}/yt/yql/dq_vanilla_job.lite"
    "${YTSAURUS_SOURCE_PATH}/yql/essentials/udfs/logs/dsv"
    "${YTSAURUS_SOURCE_PATH}/yt/yql/udfs/yt_clickhouse"
    "${YTSAURUS_SOURCE_PATH}/contrib/ydb/library/yql/udfs/common/clickhouse/client"
)

${YTSAURUS_SOURCE_PATH}/ya make -T ${BUILD_FLAGS} --ignore-recurses --output=${YQL_BUILD_PATH} "${CORE_TARGETS[@]}"

# Build common yql udfs.
UDF_NAMES=(
    compress_base
    datetime2
    digest
    file
    histogram
    hyperloglog
    hyperscan
    ip_base
    json
    json2
    math
    pire
    protobuf
    re2
    set
    stat
    streaming
    string
    top
    topfreq
    unicode_base
    url_base
    vector
    yson2
)

TARGETS=()
for udf in "${UDF_NAMES[@]}"; do
    TARGETS+=("${YTSAURUS_SOURCE_PATH}/yql/essentials/udfs/common/${udf}")
done

${YTSAURUS_SOURCE_PATH}/ya make -T ${BUILD_FLAGS} --ignore-recurses -DSTRIP=yes --output=${YQL_BUILD_PATH} "${TARGETS[@]}"

if [[ "$BUILD_FLAGS" != *"--bazel-remote-put"* ]]; then
    for udf_name in "${UDF_NAMES[@]}"; do
        strip --remove-section=.gnu_debuglink ${YTSAURUS_SOURCE_PATH}/yql/essentials/udfs/common/${udf_name}/*.so
    done
fi

BUILD_FLAGS_ISOLATED=${BUILD_FLAGS/ --yt-store / }

if [ "$build_python_udfs" == "yes" ]; then
  # Build yql system python udfs inside a docker container.
  docker container run --rm --name yql-python-udfs-build \
    -v $YTSAURUS_SOURCE_PATH:/ytsaurus \
    -v $YQL_BUILD_PATH:/yql_build \
    --env YTSAURUS_SOURCE_PATH=/ytsaurus \
    --env YQL_BUILD_PATH=/yql_build \
    --env "BUILD_FLAGS=$BUILD_FLAGS_ISOLATED" \
    ghcr.io/ytsaurus/build-env:latest \
    /bin/bash -c \
    "/ytsaurus/yt/docker/ya-build/query-tracker/build_system_python_udfs.sh"
fi

# Change ownership of files to the current user. Useful if building locally.
if [ $(id -u) -ne 0 ]; then
  sudo chown -R $(id -u):$(id -g) $YQL_BUILD_PATH
fi

# Copy all shared libraries to a single directory
mkdir -p ${YQL_BUILD_PATH}/yql_shared_libraries/yql
if [[ "$BUILD_FLAGS" != *"--bazel-remote-put"* ]]; then
    find ${YQL_BUILD_PATH} -name 'lib*.so' -print0 | xargs -0 -I '{}' cp -n '{}' ${YQL_BUILD_PATH}/yql_shared_libraries/yql
fi
