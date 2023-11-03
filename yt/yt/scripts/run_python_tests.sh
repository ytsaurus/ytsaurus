#!/bin/bash -eu

export SCRIPT_DIR="$(dirname "$0")"
export SOURCE_ROOT="$(cd "$SCRIPT_DIR/../../../" && pwd)"
export BUILD_ROOT="$(realpath ../build)"
export PYTHON_ROOT="$(realpath ../python)"
export VIRTUALENV_PATH="$(realpath ../venv)"
export TESTS_SANDBOX="$(realpath ../tests_sandbox)"

source "$VIRTUALENV_PATH/bin/activate"

cd "$PYTHON_ROOT"

export PYTHONPATH="$PYTHON_ROOT"
export YT_BUILD_ROOT="${BUILD_ROOT}"
export YT_TESTS_SANDBOX="$TESTS_SANDBOX" 

python3 -m pytest -vs "yt/local" "yt/yson" "yt/skiff"
python3 -m pytest -vs "yt/wrapper/tests" -m opensource
