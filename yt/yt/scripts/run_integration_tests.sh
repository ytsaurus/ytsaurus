#!/bin/bash -eu

export SOURCE_ROOT="$(pwd)"
export BUILD_ROOT="$(realpath ../build)"
export PYTHON_ROOT="$(realpath ../python)"
export VIRTUALENV_PATH="$(realpath ../venv)"
export TESTS_SANDBOX="$(realpath ../tests_sandbox)"

# COMPAT: remove when ticket YT-19287 will be resolved.
pip3 install virtualenv

virtualenv "$VIRTUALENV_PATH" -p python3

mkdir "$PYTHON_ROOT"

pip3 install -e yt/python/packages

yt/python/packages/yt_setup/generate_python_proto.py \
    --source-root "$SOURCE_ROOT" \
    --output "$PYTHON_ROOT"

prepare_python_modules \
    --source-root "$SOURCE_ROOT" \
    --build-root "$BUILD_ROOT" \
    --output-path "$PYTHON_ROOT" \
    --prepare-bindings-libraries


mkdir -p ${BUILD_ROOT}/yt/yt/packages/tests_package/
ln -s ${BUILD_ROOT}/yt/yt/server/all/ytserver-all ${BUILD_ROOT}/yt/yt/packages/tests_package/ytserver-all

mkdir -p ${BUILD_ROOT}/yt/python/yt/environment/bin/
ln -s ${SOURCE_ROOT}/yt/python/yt/environment/bin/yt_env_watcher ${BUILD_ROOT}/yt/python/yt/environment/bin/yt_env_watcher

mkdir -p ${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/
ln -s ${SOURCE_ROOT}/yt/yt/tools/prepare_scheduling_usage/__main__.py ${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/prepare_scheduling_usage


cd yt/yt/tests/integration

pip3 install -r requirements.txt

./run_tests.sh -m opensource
