#!/bin/bash -eu

export SCRIPT_DIR="$(dirname "$0")"
export SOURCE_ROOT="$(cd "$SCRIPT_DIR/../../../" && pwd)"
export BUILD_ROOT="$(realpath ../build)"
export PYTHON_ROOT="$(realpath ../python)"
export VIRTUALENV_PATH="$(realpath ../venv)"
export TESTS_SANDBOX="$(realpath ../tests_sandbox)"

source "$VIRTUALENV_PATH/bin/activate"

"$SOURCE_ROOT/yt/odin/packages/prepare_python_modules.py" --source-root "$SOURCE_ROOT" --output-path "$PYTHON_ROOT"

cd "$PYTHON_ROOT"
cp "$SOURCE_ROOT/yt/odin/packages/setup.py" .
python3 setup.py install

pip3 install -r "${SOURCE_ROOT}/yt/odin/requirements.txt"
pip3 install -r "${SOURCE_ROOT}/yt/odin/tests/requirements.txt"

export PYTHONPATH="$PYTHON_ROOT"
export YT_BUILD_ROOT="${BUILD_ROOT}"
export YT_TESTS_SANDBOX="$TESTS_SANDBOX"

cd "$SOURCE_ROOT/yt/odin/tests"

python3 -m pytest -vs
