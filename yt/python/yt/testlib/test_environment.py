from __future__ import print_function

from yt.test_helpers import wait
from .helpers import yatest_common

from yt.environment import YTInstance, arcadia_interop
from yt.environment.helpers import emergency_exit_within_tests
from yt.wrapper.config import set_option
from yt.wrapper.default_config import get_default_config
from yt.wrapper.common import update, update_inplace, MB
from yt.common import format_error
from yt.test_helpers.authors import pytest_configure, pytest_collection_modifyitems, pytest_itemcollected  # noqa

from yt.packages.six import iteritems, itervalues

import yt.wrapper as yt

import pytest

import os
import sys
import uuid
import shutil
import logging
import socket
import warnings

# Disables """cryptography/hazmat/primitives/constant_time.py:26: CryptographyDeprecationWarning: Support for your Python version is deprecated.
# The next version of cryptography will remove support. Please upgrade to a 2.7.x release that supports hmac.compare_digest as soon as possible."""
warnings.filterwarnings(action="ignore", module="cryptography.hazmat.primitives.*")

yt.http_helpers.RECEIVE_TOKEN_FROM_SSH_SESSION = False

ASAN_USER_JOB_MEMORY_LIMIT = 1280 * MB

def rmtree(path):
    if os.path.exists(path):
        shutil.rmtree(path)

class YtTestEnvironment(object):
    def __init__(self,
                 sandbox_path,
                 test_name,
                 config=None,
                 env_options=None,
                 delta_scheduler_config=None,
                 delta_controller_agent_config=None,
                 delta_node_config=None,
                 delta_proxy_config=None,
                 need_suid=False):
        # To use correct version of bindings we must reset it before start environment.
        yt.native_driver.driver_bindings = None

        port_locks_path = os.path.join(sandbox_path, "ports")

        self.test_name = test_name

        if config is None:
            config = {}
        if env_options is None:
            env_options = {}

        has_http_proxy = config["backend"] not in ("native",)

        logging.getLogger("Yt.local").setLevel(logging.INFO)

        run_id = uuid.uuid4().hex[:8]
        self.uniq_dir_name = os.path.join(self.test_name, "run_" + run_id)
        self.sandbox_dir = os.path.join(sandbox_path, self.uniq_dir_name)
        self.core_path = os.path.join(sandbox_path, "_cores")
        if not os.path.exists(self.core_path):
            os.makedirs(self.core_path)

        self.binaries_path = None
        if yatest_common is not None:
            if need_suid and arcadia_interop.is_inside_distbuild():
                pytest.skip()

            ytrecipe = os.environ.get("YTRECIPE") is not None

            suffix = "need_suid_" + str(int(need_suid))
            if yatest_common.get_param("ram_drive_path") is not None:
                prepare_dir = os.path.join(yatest_common.ram_drive_path(), suffix)
            else:
                prepare_dir = os.path.join(yatest_common.work_path(), suffix)

            if not os.path.exists(prepare_dir):
                os.makedirs(prepare_dir)

                self.binaries_path = arcadia_interop.prepare_yt_environment(
                    prepare_dir,
                    copy_ytserver_all=not ytrecipe,
                    need_suid=need_suid and not ytrecipe)
                os.environ["PATH"] = os.pathsep.join([self.binaries_path, os.environ.get("PATH", "")])

        common_delta_node_config = {
            "exec_agent": {
                "slot_manager": {
                    "enforce_job_control": True,
                },
                "statistics_reporter": {
                    "reporting_period": 1000,
                }
            },
        }
        common_delta_scheduler_config = {
            "scheduler": {
                "max_operation_count": 5,
            }
        }

        common_delta_controller_agent_config = {
            "controller_agent": {
                "operation_options": {
                    "spec_template": {
                        "max_failed_job_count": 1
                    }
                }
            },
            "core_dumper": {
                "path": self.core_path,
                # Pattern starts with the underscore to trick teamcity; we do not want it to
                # pay attention to the created core.
                "pattern": "_core.%CORE_PID.%CORE_SIG.%CORE_THREAD_NAME-%CORE_REASON",
            }
        }

        def modify_configs(configs, abi_version):
            for config in configs["scheduler"]:
                update_inplace(config, common_delta_scheduler_config)
                if delta_scheduler_config:
                    update_inplace(config, delta_scheduler_config)
                if configs.get("controller_agent") is None:
                    update_inplace(config["scheduler"], common_delta_controller_agent_config["controller_agent"])

            if configs.get("controller_agent") is not None:
                for config in configs["controller_agent"]:
                    update_inplace(config, common_delta_controller_agent_config)
                    if delta_controller_agent_config:
                        update_inplace(config, delta_controller_agent_config)

            for config in configs["node"]:
                update_inplace(config, common_delta_node_config)
                if delta_node_config:
                    update_inplace(config, delta_node_config)
            for config in configs["http_proxy"]:
                if delta_proxy_config:
                    update_inplace(config, delta_proxy_config)

        local_temp_directory = os.path.join(sandbox_path, "tmp_" + run_id)
        if not os.path.exists(local_temp_directory):
            os.mkdir(local_temp_directory)

        self.env = YTInstance(self.sandbox_dir,
                              master_count=1,
                              node_count=3,
                              scheduler_count=1,
                              http_proxy_count=1 if has_http_proxy else 0,
                              rpc_proxy_count=1,
                              port_locks_path=port_locks_path,
                              fqdn="localhost",
                              modify_configs_func=modify_configs,
                              kill_child_processes=True,
                              allow_chunk_storage_in_tmpfs=True,
                              **env_options)

        try:
            self.env.start(start_secondary_master_cells=True)
        except:
            self.save_sandbox()
            raise

        self.version = "{0}.{1}".format(*self.env.abi_version)

        # TODO(ignat): Remove after max_replication_factor will be implemented.
        set_option("_is_testing_mode", True, client=None)

        self.config = update(get_default_config(), config)
        self.config["enable_request_logging"] = True
        self.config["enable_passing_request_id_to_driver"] = True
        self.config["operation_tracker"]["poll_period"] = 100
        if has_http_proxy:
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
        self.config["pickling"]["module_filter"] = lambda module: hasattr(module, "__file__") and not "driver_lib" in module.__file__

        self.config["pickling"]["python_binary"] = sys.executable
        self.config["user_job_spec_defaults"] = {
            "environment": dict([(key, value) for key, value in iteritems(os.environ) if "PYTHON" in key])
        }

        if config["backend"] != "rpc":
            self.config["driver_config"] = self.env.configs["driver"]
        self.config["local_temp_directory"] = local_temp_directory
        self.config["enable_logging_for_params_changes"] = True
        self.config["allow_fallback_to_native_driver"] = False

        if yatest_common.context.sanitize == "address":
            self.config["user_job_spec_defaults"] = {"memory_limit": ASAN_USER_JOB_MEMORY_LIMIT}

        # Interrupt main in tests is unrelaible and can cause 'Test crashed' or other errors in case of flaps.
        self.config["ping_failed_mode"] = "pass"

        # NB: temporary hack
        if arcadia_interop.yatest_common is not None:
            self.config["is_local_mode"] = True
        else:
            self.config["is_local_mode"] = False

        self.reload_global_configuration()

        os.environ["PATH"] = ".:" + os.environ["PATH"]

        # To avoid using user-defined proxy in tests.
        if "YT_PROXY" in os.environ:
            del os.environ["YT_PROXY"]

        os.environ["YT_LOCAL_PORT_LOCKS_PATH"] = port_locks_path

        # NB: temporary hack
        if arcadia_interop.yatest_common is not None:
            self.env._create_cluster_client().set("//sys/@local_mode_fqdn", socket.getfqdn())

        self.env._create_cluster_client().set("//sys/@cluster_connection", self.config["driver_config"])

        # Resolve indeterminacy in sys.modules due to presence of lazy imported modules.
        for module in list(itervalues(sys.modules)):
            hasattr(module, "__file__")

    def cleanup(self):
        self.reload_global_configuration()
        self.env.stop()
        self.env.remove_runtime_data()
        self.save_sandbox()

    def save_sandbox(self):
        try:
            arcadia_interop.save_sandbox(self.sandbox_dir, self.uniq_dir_name)
        except:
            # Additional logging added due to https://github.com/pytest-dev/pytest/issues/2237
            logging.exception("YtTestEnvironment cleanup failed")
            raise

    def check_liveness(self):
        self.env.check_liveness(callback_func=emergency_exit_within_tests)

    def reload_global_configuration(self):
        yt.config._init_state()
        yt.native_driver.driver_bindings = None
        yt._cleanup_http_session()
        yt.config.config = self.config

# TODO(ignat): fix this copypaste from yt_env_setup
def _remove_operations():
    if yt.get("//sys/scheduler/instances/@count") == 0:
        return

    operation_from_orchid = []
    try:
        operation_from_orchid = [op_id for op_id in yt.list("//sys/scheduler/orchid/scheduler/operations")
                                 if not op_id.startswith("*")]
    except yt.YtError as err:
        print(format_error(err), file=sys.stderr)

    for operation_id in operation_from_orchid:
        try:
            yt.abort_operation(operation_id)
        except yt.YtError as err:
            print(format_error(err), file=sys.stderr)

    yt.remove("//sys/operations/*")

def _remove_objects():
    TYPES = [
        "accounts",
        "users",
        "groups",
        "racks",
        "data_centers",
        "tablet_cells",
        "tablet_cell_bundles",
    ]

    object_ids_to_remove = []
    object_ids_to_check = []
    for type in TYPES:

        objects = yt.list("//sys/" + ("account_tree" if type == "accounts" else type),
                          attributes=["id", "builtin", "life_stage"])
        for object in objects:
            if object.attributes["builtin"]:
                continue
            if type == "users" and str(object) == "application_operations":
                continue
            if type == "accounts" and str(object) == "operations_archive":
                continue

            id = object.attributes["id"]
            if type == "tablet_cells":
                try:
                    if any([yt.get("#" + tablet_id + "/@table_path").startswith("//sys/operations_archive")
                           for tablet_id in yt.get("#" + id + "/@tablet_ids")]):
                        continue
                except yt.YtError:
                    pass

            object_ids_to_check.append(id)
            if object.attributes["life_stage"] == "creation_committed":
                object_ids_to_remove.append(id)

    for id in object_ids_to_remove:
        yt.remove("#" + id, force=True, recursive=True)

    for id in object_ids_to_check:
        wait(lambda: not yt.exists("#" + id))

# TODO(ignat): fix copy/paste from integration tests.
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

    yt.remove("//tmp/*", recursive=True)

    _remove_operations()
    _remove_objects()
