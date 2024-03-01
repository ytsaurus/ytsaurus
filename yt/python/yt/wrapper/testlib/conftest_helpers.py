from .helpers import (get_tests_location, get_tests_sandbox,
                      wait, sync_create_cell, create_job_events, TEST_DIR)

import yt.environment.init_operations_archive as init_operations_archive
from yt.test_helpers.authors import pytest_configure, pytest_collection_modifyitems, pytest_itemcollected  # noqa
from yt.testlib import YtTestEnvironment, authors, test_method_teardown, ASAN_USER_JOB_MEMORY_LIMIT  # noqa

from yt.packages import requests

from yt.wrapper.common import GB
from yt.wrapper.constants import UI_ADDRESS_PATTERN

import yt.wrapper as yt

try:
    from yt.packages.six import PY3
except ImportError:
    from six import PY3

import pytest

import os
import re
import socket
import sys
from copy import deepcopy


def pytest_ignore_collect(path, config):
    path = str(path)
    return path.startswith(get_tests_sandbox()) or \
        path.startswith(os.path.join(get_tests_location(), "__pycache__"))


@pytest.fixture(scope="class", autouse=True)
def active_environment():
    return set()


def init_environment_for_test_session(request, mode, **kwargs):
    active_environment = request.getfixturevalue("active_environment")
    assert not active_environment, "another test environment is already active"
    active_environment.add(request.fixturename)
    request.addfinalizer(lambda: active_environment.remove(request.fixturename))

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

    request.addfinalizer(lambda: environment.cleanup())
    return environment


def test_function_setup():
    yt.mkdir(TEST_DIR, recursive=True)


def register_test_function_finalizer(request, remove_operations_archive=True):
    request.addfinalizer(lambda: yt.remove(TEST_DIR, recursive=True, force=True))
    request.addfinalizer(lambda: test_method_teardown(remove_operations_archive=remove_operations_archive))


@pytest.fixture(scope="class", params=["v3", "v4", "native_v4"])
def test_environment(request):
    environment = init_environment_for_test_session(request, request.param)
    return environment


@pytest.fixture(scope="class", params=["v4"])
def test_environment_v4(request):
    environment = init_environment_for_test_session(request, request.param)
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
    environment = init_environment_for_test_session(request, request.param, delta_proxy_config=delta_proxy_config)

    # Setup framing keep-alive period through dynamic config.
    yt.set("//sys/http_proxies/@config", {"framing": {"keep_alive_period": keep_alive_period}})
    monitoring_port = environment.env.configs["http_proxy"][0]["monitoring_port"]
    config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)
    wait(lambda: requests.get(config_url).json()["framing"]["keep_alive_period"] == keep_alive_period)

    environment.framing_options = {
        "keep_alive_period": keep_alive_period,
        "delay_before_command": delay_before_command,
        "suspending_path": suspending_path,
    }

    return environment


@pytest.fixture(scope="class", params=["v3", "v4", "native_v4", "rpc"])
def test_environment_with_rpc(request):
    environment = init_environment_for_test_session(request, request.param)
    return environment


@pytest.fixture(scope="class", params=["v3", "v4", "native_v4", "rpc"])
def test_environment_chaos(request):
    environment = init_environment_for_test_session(
        request,
        request.param,
        env_options={"chaos_node_count": 1}
    )
    return environment


@pytest.fixture(scope="class")
def test_environment_for_yamr(request):
    environment = init_environment_for_test_session(request, "yamr")
    return environment


@pytest.fixture(scope="class")
def test_environment_multicell(request):
    environment = init_environment_for_test_session(
        request,
        "native_multicell",
        env_options={"secondary_cell_count": 2})
    return environment


@pytest.fixture(scope="class")
def test_environment_job_archive(request):
    environment = init_environment_for_test_session(
        request,
        "job_archive",
        delta_dynamic_node_config={
            "%true": {
                "exec_node": {
                    "job_reporter": {
                        "reporting_period": 10,
                        "min_repeat_delay": 10,
                        "max_repeat_delay": 10,
                    }
                },
            }
        },
        delta_scheduler_config={
            "scheduler": {
                "enable_job_reporter": True,
                "enable_job_spec_reporter": True,
            },
        },
    )

    sync_create_cell()
    init_operations_archive.create_tables_latest_version(yt, override_tablet_cell_bundle="default")
    return environment


@pytest.fixture(scope="class")
def test_environment_job_archive_porto(request):
    environment = init_environment_for_test_session(
        request,
        "job_archive",
        env_options={"use_porto_for_servers": True},
        delta_node_config={
            "exec_node": {
                "slot_manager": {
                    "enforce_job_control": True,
                    "job_environment": {
                        "type": "porto",
                    },
                },
            },
        },
        delta_dynamic_node_config={
            "%true": {
                "exec_node": {
                    "job_reporter": {
                        "reporting_period": 10,
                        "min_repeat_delay": 10,
                        "max_repeat_delay": 10,
                    }
                }
            }
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
    init_operations_archive.create_tables_latest_version(yt, override_tablet_cell_bundle="default")
    return environment


@pytest.fixture(scope="class", params=["v3", "v4", "native_v4", "rpc"])
def test_environment_with_porto(request):
    environment = init_environment_for_test_session(
        request,
        request.param,
        env_options={"use_porto_for_servers": True},
        delta_node_config={
            "exec_node": {
                "slot_manager": {
                    "enforce_job_control": True,
                    "job_environment": {
                        "type": "porto",
                    },
                },
                "job_proxy": {
                    "test_poll_job_shell": True,
                },
            }
        },
        need_suid=True
    )

    return environment


@pytest.fixture(scope="class", params=["v4", "rpc"])
def test_environment_with_increased_memory(request):
    environment = init_environment_for_test_session(
        request,
        request.param,
        env_options=dict(jobs_resource_limits={"memory": 8 * GB}),
    )

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
def yt_env_v4(request, test_environment_v4):
    return _yt_env(request, test_environment_v4)


@pytest.fixture(scope="function")
def yt_env_with_framing(request, test_environment_with_framing):
    return _yt_env(request, test_environment_with_framing)


@pytest.fixture(scope="function")
def yt_env_with_rpc(request, test_environment_with_rpc):
    return _yt_env(request, test_environment_with_rpc)


@pytest.fixture(scope="function")
def yt_env_chaos(request, test_environment_chaos):
    return _yt_env(request, test_environment_chaos)


@pytest.fixture(scope="function")
def test_dynamic_library(request, yt_env_with_increased_memory):
    if PY3:
        import types
    else:
        import imp
    libs_dir = os.path.abspath("yt_test_modules")
    dependant_lib_output = os.path.join(libs_dir, "yt_test_dynamic_library.so")

    # Adding this pseudo-module to sys.modules and ensuring it will be collected with
    # its dependency (libgetnumber.so)
    if PY3:
        module = types.ModuleType("yt_test_dynamic_library")
    else:
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
    register_test_function_finalizer(request, remove_operations_archive=False)
    return test_environment_job_archive


@pytest.fixture(scope="function")
def yt_env_job_archive_porto(request, test_environment_job_archive_porto):
    """ YT cluster fixture for tests that require job archive and porto
    """
    test_environment_job_archive_porto.check_liveness()
    test_environment_job_archive_porto.reload_global_configuration()
    test_function_setup()
    register_test_function_finalizer(request, remove_operations_archive=False)
    return test_environment_job_archive_porto


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


@pytest.hookimpl
def pytest_enter_pdb(config, pdb):

    if config and config.current_test_log_path and os.path.exists(config.current_test_log_path):
        yt_host, yt_port = None, None
        cur_host = socket.gethostname()

        for line, _ in zip(open(config.current_test_log_path, "r"), range(1000)):
            # http proxy
            res = re.search(r"Perform HTTP get request https?:\/\/(.+?):(\d+)\/api", line)
            if res:
                yt_host, yt_port = res.groups()
                break
            # backup (for rpc)
            res = re.search(r"(localhost):(\d+)", line)
            if res:
                yt_host, yt_port = res.groups()

        if yt_host:
            print("")
            print("Local YT cluster is still available for this test.")
            print("  Use: `yt --proxy \"http://{}:{}\" list /` to connect".format(yt_host, yt_port))
            print("   or: `yt --proxy \"http://{}:{}\" list /`".format(cur_host, yt_port))
            print("   or: \"{}\" (take care about network availability (f.e. port range))".format(UI_ADDRESS_PATTERN.format(cluster_name=cur_host + ":" + yt_port)))
            print("\n" + "-" * 128)
