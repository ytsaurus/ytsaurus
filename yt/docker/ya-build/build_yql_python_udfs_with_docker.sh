#!/usr/bin/env bash

set -eu

# Use the same base image as the one used to build query tracker image.
CONTAINER_ID=$(docker container run --name yql-python-udfs-build --entrypoint /bin/bash -v $YDB_SOURCE_ROOT:/ydb -v $YA_BUILD_PATH:/ya_build -dit mirror.gcr.io/ubuntu:focal)
docker exec $CONTAINER_ID apt-get update
docker exec --env DEBIAN_FRONTEND=noninteractive --env TZ=Etc/UTC $CONTAINER_ID apt-get install -y software-properties-common build-essential
docker exec $CONTAINER_ID add-apt-repository ppa:deadsnakes/ppa
docker exec $CONTAINER_ID apt-get update
docker exec $CONTAINER_ID apt-get install -y python3.8-dev python3.9-dev python3.10-dev python3.11-dev python3.12-dev

for version in 3.8 \
               3.9 \
               3.10 \
               3.11 \
               3.12
do
  udf_suffix=$(echo "$version" | tr . _)
  udf_name="python${udf_suffix}"
  python_name="python${version}"

  docker exec $CONTAINER_ID \
    python3 /ydb/ya make "/ydb/ydb/library/yql/udfs/common/python/system_python/${udf_name}" \
    -T --build=relwithdebinfo -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON -DOS_SDK=local -DPYTHON_CONFIG=${python_name}-config -DPYTHON_BIN=${python_name} --output /ya_build
  docker exec $CONTAINER_ID strip --remove-section=.gnu_debuglink /ya_build/ydb/library/yql/udfs/common/python/system_python/${udf_name}/libsystem${udf_name}_udf.so
done

docker stop $CONTAINER_ID
docker rm $CONTAINER_ID
