#!/bin/bash -eu

export SOURCE_ROOT="$(pwd)"
export BUILD_ROOT="$(realpath ../build)"
export PYTHON_ROOT="$(realpath ../python)"
export VIRTUALENV_PATH="$(realpath ../venv)"
export TESTS_SANDBOX="$(realpath ../tests_sandbox)"

source "$VIRTUALENV_PATH/bin/activate"

cd yt/yt/tests/integration

./run_tests.sh -m opensource
