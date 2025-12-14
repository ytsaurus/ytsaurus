#!/bin/bash -eux

# This builds python wheels inside docker container

: ${YTSAURUS_SOURCES:=$(realpath "$(dirname "$0")/../../../")}

export DOCKER_BUILDKIT=1
exec docker build --progress plain \
    --network host \
    --output type=local,dest=packages \
    --build-arg YTSAURUS_PACKAGE_NAME=ytsaurus-yson \
    --build-context "ytsaurus-sources=${YTSAURUS_SOURCES}" \
    -f ${YTSAURUS_SOURCES}/yt/docker/python/Dockerfile.ubuntu .
