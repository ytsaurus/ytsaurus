#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$0")
YT_PYTHON_DIR=$(realpath "${SCRIPT_DIR}/../")

docker run -it -e PYTHONPATH="/source:${PYTHONPATH}" \
 --mount type=bind,source=${YT_PYTHON_DIR}/yt,target=/source/yt \
sha256:d1956f09a8e3962757be62e5b6acd61649505c0eddb41a3f81ce7d5eb078883d \
 bash -c "cp -r /workdir/python_build/yt/packages /source/yt && pytest /source/yt/wrapper/tests/$1 ${@:2}"
