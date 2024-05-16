#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$0")
YT_PYTHON_DIR=$(realpath "${SCRIPT_DIR}/../")

docker run -it -e PYTHONPATH="/source:${PYTHONPATH}" \
 --mount type=bind,source=${YT_PYTHON_DIR}/yt,target=/source/yt \
ghcr.io/dmi-feo/yt_wrapper_tests_base_image:0.0.4 \
 bash -c "cp -r /workdir/python_build/yt/packages /source/yt && pytest /source/yt/wrapper/tests/$1 ${@:2}"
