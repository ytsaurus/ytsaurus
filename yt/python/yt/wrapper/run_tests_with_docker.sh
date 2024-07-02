#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$0")
YT_PYTHON_DIR=$(realpath "${SCRIPT_DIR}/../../../")

docker run -it -e PYTHONPATH="/source:${PYTHONPATH}" \
 --mount type=bind,source=${YT_PYTHON_DIR}/yt,target=/source/yt \
sha256:0c0eb701f7cd0a61fc735c844e1b8762a63c66df8ebd03675e529be0eea6d7d1 \
 bash -c "cp -r /workdir/python_build/yt/packages /source/yt && pytest /source/yt/wrapper/tests/$1 ${@:2}"
