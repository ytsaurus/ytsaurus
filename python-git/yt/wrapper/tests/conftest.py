from .helpers import TESTS_LOCATION, TEST_DIR, TESTS_SANDBOX, ENABLE_JOB_CONTROL

from yt.environment import YTInstance
from yt.wrapper.config import set_option
from yt.wrapper.default_config import get_default_config
from yt.wrapper.common import update
import yt.logger as logger
import yt.tests_runner as tests_runner

from yt.packages.six import itervalues
from yt.packages.six.moves import reload_module

import yt.wrapper as yt

import os
import re
import sys
import uuid
from copy import deepcopy
import shutil
import logging
import pytest
from collections import defaultdict

def pytest_ignore_collect(path, config):
    path = str(path)
    return path.startswith(TESTS_SANDBOX) or \
            path.startswith(os.path.join(TESTS_LOCATION, "__pycache__"))

def pytest_generate_tests(metafunc):
    metafunc.parametrize("interpreter", ["{0}.{1}".format(*sys.version_info[:2])], indirect=True)

def _pytest_finalize_func(environment, process_call_args):
    pytest.exit('Process run by command "{0}" is dead! Tests terminated.' \
                .format(" ".join(process_call_args)))
    environment.stop()

def pytest_configure(config):
    def scheduling_func(test_items, process_count):
        suites = defaultdict(list)
        for index, test in enumerate(test_items):
            match = re.search(r"\[([a-zA-Z0-9]+)\]$", test.name)
            suite_name = None
            if match and match.group(1) in ["v3", "native"]:
                suite_name = match.group(1)

            suites[suite_name].append(index)

        return tests_runner.split_test_suites(suites, process_count)

    tests_runner.set_scheduling_func(scheduling_func)

class YtTestEnvironment(object):
    def __init__(self, test_name, config=None, env_options=None):
        self.test_name = test_name

        if config is None:
            config = {}
        if env_options is None:
            env_options = {}

        has_proxy = config["backend"] != "native"

        logging.getLogger("Yt.local").setLevel(logging.INFO)
        logger.LOGGER.setLevel(logging.WARNING)

        dir = os.path.join(TESTS_SANDBOX, self.test_name, "run_" + uuid.uuid4().hex[:8])

        delta_proxy_config = {
            "proxy": {
                "driver": {
                    # Disable cache
                    "table_mount_cache": {
                        "expire_after_successful_update_time": 0,
                        "expire_after_failed_update_time": 0,
                        "expire_after_access_time": 0,
                        "refresh_time": 0
                    }
                }
            }
        }
        delta_node_config = {
            "exec_agent" : {
                "enable_cgroups" : ENABLE_JOB_CONTROL,
                "slot_manager" : {
                    "enforce_job_control" : ENABLE_JOB_CONTROL
                }
            },
            "data_node": {
                "store_locations": [
                    {
                        "max_trash_ttl": 2000
                    }
                ]
            },
            "cluster_connection": {
                # Disable cache
                "table_mount_cache": {
                    "expire_after_successful_update_time": 0,
                    "expire_after_failed_update_time": 0,
                    "expire_after_access_time": 0,
                    "refresh_time": 0
                }
            },
        }
        delta_scheduler_config = {
            "scheduler" : {
                "max_operation_count": 5,
                "operation_options": {
                    "spec_template": {
                        "max_failed_job_count": 1
                    }
                }
            }
        }

        def modify_configs(configs, abi_version):
            for config in configs["scheduler"]:
                update(config, delta_scheduler_config)
            for config in configs["node"]:
                update(config, delta_node_config)
            for config in configs["proxy"]:
                update(config, delta_proxy_config)

        self.env = YTInstance(dir,
                              master_count=1,
                              node_count=5,
                              scheduler_count=1,
                              has_proxy=has_proxy,
                              port_locks_path=os.path.join(TESTS_SANDBOX, "ports"),
                              fqdn="localhost",
                              modify_configs_func=modify_configs,
                              kill_child_processes=True,
                              **env_options)
        self.env.start(start_secondary_master_cells=True)

        self.version = "{0}.{1}".format(*self.env.abi_version)

        reload_module(yt)
        reload_module(yt.config)
        reload_module(yt.native_driver)

        yt._cleanup_http_session()

        # TODO(ignat): Remove after max_replication_factor will be implemented.
        set_option("_is_testing_mode", True, client=None)

        self.config = update(get_default_config(), config)
        self.config["enable_request_logging"] = True
        self.config["operation_tracker"]["poll_period"] = 100
        if has_proxy:
            self.config["proxy"]["url"] = "localhost:" + self.env.get_proxy_address().split(":", 1)[1]
        # NB: to decrease probability of retries test failure.
        self.config["proxy"]["retries"]["count"] = 10

        self.config["proxy"]["retries"]["backoff"]["constant_time"] = 500
        self.config["proxy"]["retries"]["backoff"]["policy"] = "constant_time"

        self.config["read_retries"]["backoff"]["constant_time"] = 500
        self.config["read_retries"]["backoff"]["policy"] = "constant_time"

        self.config["write_retries"]["backoff"]["constant_time"] = 500
        self.config["write_retries"]["backoff"]["policy"] = "constant_time"

        self.config["enable_token"] = False
        self.config["is_local_mode"] = False
        self.config["pickling"]["enable_tmpfs_archive"] = ENABLE_JOB_CONTROL
        self.config["pickling"]["module_filter"] = lambda module: hasattr(module, "__file__") and not "driver_lib" in module.__file__
        self.config["driver_config"] = self.env.configs["driver"]
        update(yt.config.config, self.config)

        os.environ["PATH"] = ".:" + os.environ["PATH"]

        # Resolve indeterminacy in sys.modules due to presence of lazy imported modules.
        for module in list(itervalues(sys.modules)):
            hasattr(module, "__file__")

    def cleanup(self):
        self.env.stop()
        for node_config in self.env.configs["node"]:
            shutil.rmtree(node_config["data_node"]["store_locations"][0]["path"])
            if "cache_locations" in node_config["data_node"]:
                shutil.rmtree(node_config["data_node"]["cache_locations"][0]["path"])
            else:
                shutil.rmtree(node_config["data_node"]["cache_location"]["path"])

    def check_liveness(self):
        self.env.check_liveness(callback_func=_pytest_finalize_func)

def init_environment_for_test_session(mode, env_options=None):
    config = {"api_version": "v3"}
    if mode == "native":
        config["backend"] = "native"
    else:
        config["backend"] = "http"

    environment = YtTestEnvironment(
        "TestYtWrapper" + mode.capitalize(),
        config,
        env_options=env_options)

    if mode == "native":
        import yt_driver_bindings
        yt_driver_bindings.configure_logging(environment.env.driver_logging_config)
    else:
        yt.config.COMMANDS = None

    return environment

@pytest.fixture(scope="session", params=["v3", "native"])
def test_environment(request):
    environment = init_environment_for_test_session(request.param)
    request.addfinalizer(lambda: environment.cleanup())
    return environment

@pytest.fixture(scope="session")
def test_environment_for_yamr(request):
    environment = init_environment_for_test_session("yamr")
    request.addfinalizer(lambda: environment.cleanup())

    yt.set_yamr_mode()
    yt.config["yamr_mode"]["treat_unexisting_as_empty"] = False
    if not yt.exists("//sys/empty_yamr_table"):
        yt.create("table", "//sys/empty_yamr_table", recursive=True)
    if not yt.is_sorted("//sys/empty_yamr_table"):
        yt.run_sort("//sys/empty_yamr_table", "//sys/empty_yamr_table", sort_by=["key", "subkey"])
    yt.config["yamr_mode"]["treat_unexisting_as_empty"] = True
    yt.config["default_value_of_raw_option"] = True

    return environment

@pytest.fixture(scope="session")
def test_environment_multicell(request):
    environment = init_environment_for_test_session(
        "native_multicell",
        env_options={"secondary_master_cell_count": 2})
    request.addfinalizer(lambda: environment.cleanup())
    return environment

def test_method_teardown():
    if yt.config["backend"] == "proxy":
        assert yt.config["proxy"]["url"].startswith("localhost")

    for tx in yt.list("//sys/transactions", attributes=["title"]):
        title = tx.attributes.get("title", "")
        if "Scheduler lock" in title or "Lease for" in title or "Prerequisite for" in title:
            continue
        try:
            yt.abort_transaction(tx)
        except:
            pass

    yt.remove(TEST_DIR, recursive=True, force=True)

@pytest.fixture(scope="function")
def yt_env(request, test_environment):
    """ YT cluster fixture.
        Uses test_environment fixture.
        Starts YT cluster once per session but checks its health before each test function.
    """
    test_environment.check_liveness()
    yt.mkdir(TEST_DIR, recursive=True)
    request.addfinalizer(test_method_teardown)
    return test_environment

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
    yt.mkdir(TEST_DIR, recursive=True)
    request.addfinalizer(test_method_teardown)
    return test_environment_for_yamr

@pytest.fixture(scope="function")
def yt_env_multicell(request, test_environment_multicell):
    """ YT cluster fixture for tests with multiple cells.
    """
    test_environment_multicell.check_liveness()
    yt.mkdir(TEST_DIR, recursive=True)
    request.addfinalizer(test_method_teardown)
    return test_environment_multicell

