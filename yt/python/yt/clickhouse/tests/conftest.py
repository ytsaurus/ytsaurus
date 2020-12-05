from __future__ import print_function

from yt.testlib import (YtTestEnvironment, set_testsuite_details, TEST_DIR, test_method_teardown)

from yt.packages import requests

import yt.wrapper as yt

import pytest

import os
import imp
from copy import deepcopy
import logging

set_testsuite_details(os.path.abspath(__file__), "yt/python/yt/clickhouse/tests")

def init_environment_for_test_session(**kwargs):
    config = {"backend": "http", "api_version": "v4"}

    environment = YtTestEnvironment(
        "TestClickHouse",
        config,
        **kwargs)

    yt.config.COMMANDS = None

    return environment

@pytest.fixture(scope="class")
def test_environment(request):
    environment = init_environment_for_test_session()
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="function")
def config(yt_env):
    """ Test environment startup config fixture
        Used in tests to restore config after changes.
    """
    return deepcopy(yt_env.config)

def _yt_env(request, test_environment):
    """ YT cluster fixture.
        Uses test_environment fixture.
        Starts YT cluster once per session but checks its health before each test function.
    """
    test_environment.check_liveness()
    test_environment.reload_global_configuration()
    yt.mkdir(TEST_DIR, recursive=True)
    request.addfinalizer(test_method_teardown)
    return test_environment

@pytest.fixture(scope="function")
def yt_env(request, test_environment):
    return _yt_env(request, test_environment)
