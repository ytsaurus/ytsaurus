#!/bin/bash -eu

export SCRIPT_DIR="$(dirname "$0")"
export SOURCE_ROOT="$(cd "$SCRIPT_DIR/../../../" && pwd)"
export BUILD_ROOT="$(realpath ../build)"
export PYTHON_ROOT="$(realpath ../python)"
export VIRTUALENV_PATH="$(realpath ../venv)"
export TESTS_SANDBOX="$(realpath ../tests_sandbox)"

source "$VIRTUALENV_PATH/bin/activate"

mkdir "$PYTHON_ROOT"

pip3 install -e ${SOURCE_ROOT}/yt/python/packages

generate_python_proto \
    --source-root "$SOURCE_ROOT" \
    --output "$PYTHON_ROOT"

prepare_python_modules \
    --source-root "$SOURCE_ROOT" \
    --build-root "$BUILD_ROOT" \
    --output-path "$PYTHON_ROOT" \
    --prepare-bindings-libraries


mkdir -p "${BUILD_ROOT}/yt/yt/packages/tests_package/"
ln -sf "${BUILD_ROOT}/yt/yt/server/all/ytserver-all" "${BUILD_ROOT}/yt/yt/packages/tests_package/ytserver-all"

mkdir -p "${BUILD_ROOT}/yt/python/yt/environment/bin/"
ln -sf "${SOURCE_ROOT}/yt/python/yt/environment/bin/yt_env_watcher" "${BUILD_ROOT}/yt/python/yt/environment/bin/yt_env_watcher"

mkdir -p "${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/"
ln -sf "${SOURCE_ROOT}/yt/yt/tools/prepare_scheduling_usage/__main__.py" "${BUILD_ROOT}/yt/yt/tools/prepare_scheduling_usage/prepare_scheduling_usage"

pip3 install -r "${SOURCE_ROOT}/yt/yt/scripts/pytest_requirements.txt"
