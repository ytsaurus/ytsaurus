from .helpers import (get_tests_location, TEST_DIR, get_tests_sandbox, ENABLE_JOB_CONTROL,
                      sync_create_cell, get_test_file_path, get_tmpfs_path, get_port_locks_path, yatest_common)

from yt.environment import YTInstance
from yt.environment.job_events import JobEvents
from yt.wrapper.config import set_option
from yt.wrapper.default_config import get_default_config
from yt.wrapper.common import update, update_inplace
from yt.common import which, makedirp, format_error
import yt.environment.init_operation_archive as init_operation_archive
import yt.subprocess_wrapper as subprocess

from yt.packages.six import itervalues
from yt.packages.six.moves import reload_module

import yt.wrapper as yt

if yatest_common is not None:
    from yt.environment import arcadia_interop
else:
    arcadia_interop = None

import os
import imp
import sys
import uuid
from copy import deepcopy
import shutil
import tempfile
import logging
import pytest
import stat

def pytest_ignore_collect(path, config):
    path = str(path)
    return path.startswith(get_tests_sandbox()) or \
            path.startswith(os.path.join(get_tests_location(), "__pycache__"))

if yatest_common is not None:
    @pytest.fixture(scope="session", autouse=True)
    def prepare_path(request):
        destination = os.path.join(yatest_common.work_path(), "build")
        os.makedirs(destination)
        path, node_path = arcadia_interop.prepare_yt_environment(destination)
        os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])
        os.environ["NODE_PATH"] = node_path

def _pytest_finalize_func(environment, process_call_args):
    pytest.exit('Process run by command "{0}" is dead! Tests terminated.' \
                .format(" ".join(process_call_args)))
    environment.stop()

class YtTestEnvironment(object):
    def __init__(self,
                 test_name,
                 config=None,
                 env_options=None,
                 delta_scheduler_config=None,
                 delta_node_config=None,
                 delta_proxy_config=None):
        self.test_name = test_name

        if config is None:
            config = {}
        if env_options is None:
            env_options = {}

        has_proxy = config["backend"] != "native"

        logging.getLogger("Yt.local").setLevel(logging.INFO)

        run_id = uuid.uuid4().hex[:8]
        uniq_dir_name = os.path.join(self.test_name, "run_" + run_id)
        dir = os.path.join(get_tests_sandbox(), uniq_dir_name)

        common_delta_node_config = {
            "exec_agent" : {
                "enable_cgroups" : ENABLE_JOB_CONTROL,
                "slot_manager" : {
                    "enforce_job_control" : ENABLE_JOB_CONTROL
                },
                "statistics_reporter": {
                    "reporting_period": 1000
                }
            },
            "data_node": {
                "store_locations": [
                    {
                        "max_trash_ttl": 2000
                    }
                ]
            },
        }
        common_delta_scheduler_config = {
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
                update_inplace(config, common_delta_scheduler_config)
                if delta_scheduler_config:
                    update_inplace(config, delta_scheduler_config)
            for config in configs["node"]:
                update_inplace(config, common_delta_node_config)
                if delta_node_config:
                    update_inplace(config, delta_node_config)
            for config in configs["proxy"]:
                if delta_proxy_config:
                    update_inplace(config, delta_proxy_config)

        local_temp_directory = os.path.join(get_tests_sandbox(), "tmp_" + run_id)
        if not os.path.exists(local_temp_directory):
            os.mkdir(local_temp_directory)

        tmpfs_path = get_tmpfs_path()
        if tmpfs_path is not None:
            tmpfs_path = os.path.join(tmpfs_path, uniq_dir_name)
            if not os.path.exists(tmpfs_path):
                os.makedirs(tmpfs_path)

        self.env = YTInstance(dir,
                              master_count=1,
                              node_count=5,
                              scheduler_count=1,
                              has_proxy=has_proxy,
                              port_locks_path=get_port_locks_path(),
                              fqdn="localhost",
                              modify_configs_func=modify_configs,
                              kill_child_processes=True,
                              tmpfs_path=tmpfs_path,
                              allow_chunk_storage_in_tmpfs=True,
                              **env_options)
        self.env.start(start_secondary_master_cells=True)

        self.version = "{0}.{1}".format(*self.env.abi_version)

        if yatest_common is None:
            reload_module(yt)
            reload_module(yt.config)
            reload_module(yt.native_driver)

        yt._cleanup_http_session()

        # TODO(ignat): Remove after max_replication_factor will be implemented.
        set_option("_is_testing_mode", True, client=None)

        self.config = update(get_default_config(), config)
        self.config["enable_request_logging"] = True
        self.config["enable_passing_request_id_to_driver"] = True
        self.config["operation_tracker"]["poll_period"] = 100
        if has_proxy:
            self.config["proxy"]["url"] = "localhost:" + self.env.get_proxy_address().split(":", 1)[1]

        # NB: to decrease probability of retries test failure.
        self.config["proxy"]["retries"]["count"] = 10
        self.config["write_retries"]["count"] = 10

        self.config["proxy"]["retries"]["backoff"]["constant_time"] = 500
        self.config["proxy"]["retries"]["backoff"]["policy"] = "constant_time"

        self.config["read_retries"]["backoff"]["constant_time"] = 500
        self.config["read_retries"]["backoff"]["policy"] = "constant_time"

        self.config["write_retries"]["backoff"]["constant_time"] = 500
        self.config["write_retries"]["backoff"]["policy"] = "constant_time"

        self.config["batch_requests_retries"]["backoff"]["constant_time"] = 500
        self.config["batch_requests_retries"]["backoff"]["policy"] = "constant_time"

        self.config["read_parallel"]["data_size_per_thread"] = 1
        self.config["read_parallel"]["max_thread_count"] = 10

        self.config["enable_token"] = False
        self.config["is_local_mode"] = False
        self.config["pickling"]["enable_tmpfs_archive"] = ENABLE_JOB_CONTROL
        self.config["pickling"]["module_filter"] = lambda module: hasattr(module, "__file__") and not "driver_lib" in module.__file__
        self.config["driver_config"] = self.env.configs["driver"]
        self.config["local_temp_directory"] = local_temp_directory
        update_inplace(yt.config.config, self.config)

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

def init_environment_for_test_session(mode, **kwargs):
    config = {"api_version": "v3"}
    if mode == "native":
        config["backend"] = "native"
    else:
        config["backend"] = "http"

    environment = YtTestEnvironment(
        "TestYtWrapper" + mode.capitalize(),
        config,
        **kwargs)

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

@pytest.fixture(scope="session")
def test_environment_job_archive(request):
    environment = init_environment_for_test_session(
        "job_archive",
        delta_node_config={
            "exec_agent": {
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
                "enable_statistics_reporter": True,  # obsolete, need to cleanup after merging changes related to job_specs
                "enable_job_reporter": True,
                "enable_job_spec_reporter": True,
            },
        }
    )

    sync_create_cell()
    init_operation_archive.create_tables_latest_version(yt)

    request.addfinalizer(lambda: environment.cleanup())

    return environment

# TODO(ignat): fix this copypaste from yt_env_setup
def _remove_operations():
    if yt.get("//sys/scheduler/instances/@count") == 0:
        return

    operation_from_orchid = []
    try:
        operation_from_orchid = yt.list("//sys/scheduler/orchid/scheduler/operations")
    except yt.YtError as err:
        print >>sys.stderr, format_error(err)

    for operation_id in operation_from_orchid:
        try:
            yt.abort_operation(operation_id)
        except yt.YtError as err:
            print >>sys.stderr, format_error(err)

    yt.remove("//sys/operations/*")

def test_method_teardown():
    if yt.config["backend"] == "proxy":
        assert yt.config["proxy"]["url"].startswith("localhost")

    for tx in yt.list("//sys/transactions", attributes=["title"]):
        title = tx.attributes.get("title", "")
        if "Scheduler lock" in title:
            continue
        if "Controller agent incarnation" in title:
            continue
        if "Lease for" in title:
            continue
        if "Prerequisite for" in title:
            continue
        try:
            yt.abort_transaction(tx)
        except:
            pass

    yt.remove(TEST_DIR, recursive=True, force=True)

    _remove_operations()

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

@pytest.fixture(scope="function")
def yt_env_job_archive(request, test_environment_job_archive):
    """ YT cluster fixture for tests that require job archive
    """
    test_environment_job_archive.check_liveness()
    yt.mkdir(TEST_DIR, recursive=True)
    request.addfinalizer(test_method_teardown)
    return test_environment_job_archive

@pytest.fixture(scope="function")
def job_events(request):
    tmpdir = tempfile.mkdtemp(prefix="job_events", dir=get_tests_sandbox())
    os.chmod(tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    return JobEvents(tmpdir)
