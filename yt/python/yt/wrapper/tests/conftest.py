from .helpers import (get_tests_location, get_tests_sandbox, get_test_file_path,
                      wait, sync_create_cell, create_job_events, TEST_DIR)

from yt.environment import arcadia_interop
from yt.common import which, makedirp
import yt.environment.init_operation_archive as init_operation_archive
import yt.subprocess_wrapper as subprocess
from yt.test_helpers.authors import pytest_configure, pytest_collection_modifyitems, pytest_itemcollected  # noqa
from yt.testlib import YtTestEnvironment, authors, test_method_teardown, ASAN_USER_JOB_MEMORY_LIMIT  # noqa

from yt.packages import requests

from yt.wrapper.common import GB

import yt.wrapper as yt

import pytest

import os
import imp
import sys
from copy import deepcopy

def pytest_ignore_collect(path, config):
    path = str(path)
    return path.startswith(get_tests_sandbox()) or \
        path.startswith(os.path.join(get_tests_location(), "__pycache__"))

def init_environment_for_test_session(mode, **kwargs):
    config = {"api_version": "v3"}
    if mode in ("native_v3", "native_v4"):
        config["backend"] = "native"
        if mode == "native_v4":
            config["api_version"] = "v4"
    elif mode == "rpc":
        config["backend"] = "rpc"
    elif mode in ("native_multicell", "yamr", "job_archive"):
        config["backend"] = "http"
        config["api_version"] = "v4"
    else:
        config["backend"] = "http"
        config["api_version"] = mode

    environment = YtTestEnvironment(
        get_tests_sandbox(),
        "TestYtWrapper" + mode.capitalize(),
        config,
        **kwargs)

    if mode.startswith("native"):
        import yt_driver_bindings
        yt_driver_bindings.configure_logging(environment.env.configs["driver_logging"])
    else:
        yt.config.COMMANDS = None

    return environment

def test_function_setup():
    yt.mkdir(TEST_DIR, recursive=True)

def register_test_function_finalizer(request):
    request.addfinalizer(lambda: yt.remove(TEST_DIR, recursive=True, force=True))
    request.addfinalizer(test_method_teardown)

@pytest.fixture(scope="class", params=["v3", "v4", "native_v3", "native_v4"])
def test_environment(request):
    environment = init_environment_for_test_session(request.param)
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="class", params=["v3", "v4"])
def test_environment_with_framing(request):
    suspending_path = "//tmp/suspending_table"
    delay_before_command = 10 * 1000
    keep_alive_period = 1 * 1000
    delta_proxy_config = {
        "api": {
            "testing": {
                "delay_before_command": {
                    "read_table": {
                        "parameter_path": "/path",
                        "substring": suspending_path,
                        "delay": delay_before_command,
                    },
                    "get_table_columnar_statistics": {
                        "parameter_path": "/paths/0",
                        "substring": suspending_path,
                        "delay": delay_before_command,
                    },
                },
            },
        },
    }
    environment = init_environment_for_test_session(request.param, delta_proxy_config=delta_proxy_config)

    # Setup framing keep-alive period through dynamic config.
    yt.set("//sys/proxies/@config", {"framing": {"keep_alive_period": keep_alive_period}})
    monitoring_port = environment.env.configs["http_proxy"][0]["monitoring_port"]
    config_url = "http://localhost:{}/orchid/coordinator/dynamic_config".format(monitoring_port)
    wait(lambda: requests.get(config_url).json()["framing"]["keep_alive_period"] == keep_alive_period)

    environment.framing_options = {
        "keep_alive_period": keep_alive_period,
        "delay_before_command": delay_before_command,
        "suspending_path": suspending_path,
    }

    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="class", params=["v3", "v4", "native_v3", "native_v4", "rpc"])
def test_environment_with_rpc(request):
    environment = init_environment_for_test_session(request.param)
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="class")
def test_environment_for_yamr(request):
    environment = init_environment_for_test_session("yamr")
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="class")
def test_environment_multicell(request):
    environment = init_environment_for_test_session(
        "native_multicell",
        env_options={"secondary_master_cell_count": 2})
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="class")
def test_environment_job_archive(request):
    if arcadia_interop.is_inside_distbuild():
        pytest.skip("porto is not available inside distbuild")

    environment = init_environment_for_test_session(
        "job_archive",
        env_options={"use_porto_for_servers": True},
        delta_node_config={
            "exec_agent": {
                "slot_manager": {
                    "enforce_job_control": True,
                    "job_environment": {
                        "type": "porto",
                    },
                },
                "statistics_reporter": {
                    "enabled": True,
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                }
            },
        },
        delta_scheduler_config={
            "scheduler": {
                "enable_job_reporter": True,
                "enable_job_spec_reporter": True,
            },
        },
        need_suid=True
    )

    sync_create_cell()
    init_operation_archive.create_tables_latest_version(yt, override_tablet_cell_bundle="default")

    request.addfinalizer(lambda: environment.cleanup())

    return environment

@pytest.fixture(scope="class", params=["v3", "v4", "native_v3", "native_v4", "rpc"])
def test_environment_with_porto(request):
    if arcadia_interop.is_inside_distbuild():
        pytest.skip("porto is not available inside distbuild")

    environment = init_environment_for_test_session(
        request.param,
        env_options={"use_porto_for_servers": True},
        delta_node_config={
            "exec_agent": {
                "slot_manager": {
                    "enforce_job_control": True,
                    "job_environment": {
                        "type": "porto",
                    },
                },
                "test_poll_job_shell": True,
            }
        },
        need_suid=True
    )

    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="class", params=["v4", "rpc"])
def test_environment_with_increased_memory(request):
    environment = init_environment_for_test_session(
        request.param,
        env_options=dict(jobs_memory_limit=8 * GB),
    )

    request.addfinalizer(lambda: environment.cleanup())
    return environment

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

@pytest.fixture(scope="function")
def yt_env(request, test_environment):
    return _yt_env(request, test_environment)

@pytest.fixture(scope="function")
def yt_env_with_framing(request, test_environment_with_framing):
    return _yt_env(request, test_environment_with_framing)

@pytest.fixture(scope="function")
def yt_env_with_rpc(request, test_environment_with_rpc):
    return _yt_env(request, test_environment_with_rpc)

@pytest.fixture(scope="function")
def test_dynamic_library(request, yt_env):
    if not which("g++"):
        raise RuntimeError("g++ not found")
    libs_dir = os.path.join(yt_env.env.path, "yt_test_dynamic_library")
    makedirp(libs_dir)

    get_number_lib = get_test_file_path("getnumber.cpp")
    subprocess.check_call(["g++", get_number_lib, "-shared", "-fPIC", "-o", os.path.join(libs_dir, "libgetnumber.so")])

    dependant_lib = get_test_file_path("yt_test_lib.cpp")
    dependant_lib_output = os.path.join(libs_dir, "yt_test_dynamic_library.so")
    subprocess.check_call(["g++", dependant_lib, "-shared", "-o", dependant_lib_output,
                           "-L", libs_dir, "-l", "getnumber", "-fPIC"])

    # Adding this pseudo-module to sys.modules and ensuring it will be collected with
    # its dependency (libgetnumber.so)
    module = imp.new_module("yt_test_dynamic_library")
    module.__file__ = dependant_lib_output
    sys.modules["yt_test_dynamic_library"] = module

    def finalizer():
        del sys.modules["yt_test_dynamic_library"]

    request.addfinalizer(finalizer)
    return libs_dir, "libgetnumber.so"

@pytest.fixture(scope="function")
def config(yt_env):
    """ Test environment startup config fixture
        Used in tests to restore config after changes.
    """
    return deepcopy(yt_env.config)

@pytest.fixture(scope="function")
def yt_env_for_yamr(request, test_environment_for_yamr):
    """ YT cluster fixture for Yamr mode tests.
        Uses test_environment_for_yamr fixture.
        Starts YT cluster once per session but checks its health
        before each test function.
    """
    test_environment_for_yamr.check_liveness()
    test_environment_for_yamr.reload_global_configuration()

    yt.set_yamr_mode()
    yt.config["yamr_mode"]["treat_unexisting_as_empty"] = False
    if not yt.exists("//sys/empty_yamr_table"):
        yt.create("table", "//sys/empty_yamr_table", recursive=True)
    if not yt.is_sorted("//sys/empty_yamr_table"):
        yt.run_sort("//sys/empty_yamr_table", "//sys/empty_yamr_table", sort_by=["key", "subkey"])
    yt.config["yamr_mode"]["treat_unexisting_as_empty"] = True
    yt.config["default_value_of_raw_option"] = True

    test_function_setup()
    register_test_function_finalizer(request)
    return test_environment_for_yamr

@pytest.fixture(scope="function")
def yt_env_multicell(request, test_environment_multicell):
    """ YT cluster fixture for tests with multiple cells.
    """
    test_environment_multicell.check_liveness()
    test_environment_multicell.reload_global_configuration()
    test_function_setup()
    register_test_function_finalizer(request)
    return test_environment_multicell

@pytest.fixture(scope="function")
def yt_env_job_archive(request, test_environment_job_archive):
    """ YT cluster fixture for tests that require job archive
    """
    test_environment_job_archive.check_liveness()
    test_environment_job_archive.reload_global_configuration()
    test_function_setup()
    register_test_function_finalizer(request)
    return test_environment_job_archive

@pytest.fixture(scope="function")
def yt_env_with_porto(request, test_environment_with_porto):
    """ YT cluster fixture for tests that require "porto" instead of "cgroups"
    """
    test_environment_with_porto.check_liveness()
    test_environment_with_porto.reload_global_configuration()
    test_function_setup()
    register_test_function_finalizer(request)
    return test_environment_with_porto

@pytest.fixture(scope="function")
def yt_env_with_increased_memory(request, test_environment_with_increased_memory):
    test_environment_with_increased_memory.check_liveness()
    test_environment_with_increased_memory.reload_global_configuration()
    test_function_setup()
    register_test_function_finalizer(request)
    return test_environment_with_increased_memory

@pytest.fixture(scope="function")
def job_events(request):
    return create_job_events()
