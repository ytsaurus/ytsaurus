#!/bin/bash -eu

ln -fs ${SOURCE_ROOT}/yt/yt/tests/conftest_lib/conftest_queries.py ${SOURCE_ROOT}/yt/yt/tests/integration/conftest.py

PYTHONPATH="${SOURCE_ROOT}/yt/yt/tests:${SOURCE_ROOT}/yt/yt/tests/library:$YTSAURUS_PYTHON:$PYTHONPATH" YT_BUILD_ROOT=${BUILD_ROOT} HDD_PATH=${TESTS_SANDBOX} YT_TESTS_SANDBOX=${TESTS_SANDBOX} python3.8 -m pytest -s -v $@
