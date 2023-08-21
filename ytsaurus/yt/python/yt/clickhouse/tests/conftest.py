from __future__ import print_function

from yt.test_helpers import get_tests_sandbox as get_tests_sandbox_impl
from yt.testlib import (yatest_common, YtTestEnvironment, test_method_teardown)

import yt.wrapper as yt

import pytest

import os
from copy import deepcopy


def get_tests_location():
    if yatest_common is None:
        return os.path.dirname(os.path.abspath(__file__))
    else:
        return yatest_common.source_path("yt/python/yt/clickhouse/tests")


def get_tests_sandbox():
    return get_tests_sandbox_impl(
        os.environ.get("YT_TESTS_SANDBOX", os.path.dirname(os.path.abspath(__file__)) + ".sandbox")
    )


def init_environment_for_test_session(**kwargs):
    config = {"backend": "http", "api_version": "v4"}

    environment = YtTestEnvironment(
        get_tests_sandbox(),
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
    request.addfinalizer(test_method_teardown)
    return test_environment


@pytest.fixture(scope="function")
def yt_env(request, test_environment):
    return _yt_env(request, test_environment)
