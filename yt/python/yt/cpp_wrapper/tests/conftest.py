from yt.test_helpers import get_tests_sandbox as get_tests_sandbox_impl
from yt.testlib import (YtTestEnvironment, test_method_teardown)

import yt.wrapper as yt

import os
import pytest


TEST_DIR = "//tmp/cpp_wrapper_tests"


def get_tests_sandbox():
    return get_tests_sandbox_impl(
        os.environ.get("TESTS_SANDBOX", os.path.dirname(os.path.abspath(__file__)) + ".sandbox")
    )


def init_environment_for_test_session(**kwargs):
    config = {"backend": "http", "api_version": "v4"}

    environment = YtTestEnvironment(
        get_tests_sandbox(),
        "TestCppWrapper",
        config,
        **kwargs)

    yt.config.COMMANDS = None

    return environment


@pytest.fixture(scope="class")
def test_environment(request):
    environment = init_environment_for_test_session()
    request.addfinalizer(lambda: environment.cleanup())
    return environment


def test_function_setup():
    yt.mkdir(TEST_DIR, recursive=True)


def register_test_function_finalizer(request, remove_operations_archive=True):
    request.addfinalizer(lambda: yt.remove(TEST_DIR, recursive=True, force=True))
    request.addfinalizer(lambda: test_method_teardown(remove_operations_archive=remove_operations_archive))


def _yt_env(request, test_environment):
    """ YT cluster fixture.
        Uses test_environment fixture.
        Starts YT cluster once per session but checks its health before each test function.
    """
    test_environment.check_liveness()
    test_environment.reload_global_configuration()
    test_function_setup()
    register_test_function_finalizer(request)
    return test_environment


@pytest.fixture(scope="function", autouse=True)
def yt_env(request, test_environment):
    return _yt_env(request, test_environment)
