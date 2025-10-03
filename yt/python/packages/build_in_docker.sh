#!/bin/bash
set -eux -o pipefail

: ${YTSAURUS_SOURCES:=$(realpath "$(dirname "$0")/../../../")}

export DOCKER_BUILDKIT=1
docker build --progress plain \
    --output type=local,dest=packages \
    --build-arg YTSAURUS_PACKAGE_NAME=ytsaurus-yson \
    --build-context "ytsaurus-sources=${YTSAURUS_SOURCES}" \
    -f Dockerfile.ubuntu .
