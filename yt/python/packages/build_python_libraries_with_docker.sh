#!/bin/bash -eu

CONTAINER_ID=$(docker container run --name yson-bindings-build --entrypoint /bin/bash -v $SOURCE_ROOT:/ytsaurus -v $BUILD_PATH:/build -dit quay.io/pypa/manylinux_2_28_x86_64)

docker exec $CONTAINER_ID /ytsaurus/yt/python/packages/build_manylinux_bindings.sh
