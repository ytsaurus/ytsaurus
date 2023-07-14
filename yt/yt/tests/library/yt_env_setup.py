from __future__ import print_function

import yt_commands
import yt_scheduler_helpers

from yt.environment import YTInstance, arcadia_interop
from yt.environment.api import LocalYtConfig
from yt.environment.helpers import emergency_exit_within_tests
from yt.environment.default_config import (
    get_dynamic_master_config,
    get_dynamic_node_config,
)
from yt.environment.helpers import (  # noqa
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    CHAOS_NODES_SERVICE,
    MASTERS_SERVICE,
    QUEUE_AGENTS_SERVICE,
    RPC_PROXIES_SERVICE,
    HTTP_PROXIES_SERVICE,
)

from yt.test_helpers import wait, WaitFailed, get_work_path, get_build_root, get_tests_sandbox
import yt.test_helpers.cleanup as test_cleanup

from yt.common import YtResponseError, format_error, update_inplace
import yt.logger

from yt_driver_bindings import reopen_logs

import pytest

import gc
import os
import sys
import logging
import decorator
import functools
import shutil
import hashlib

from time import sleep, time
from threading import Thread

OUTPUT_PATH = None
SANDBOX_ROOTDIR = None

##################################################################


def prepare_yatest_environment(need_suid, artifact_components=None, force_create_environment=False):
    yt.logger.LOGGER.setLevel(logging.DEBUG)
    artifact_components = artifact_components or {}

    global OUTPUT_PATH
    global SANDBOX_ROOTDIR

    # This env var is used for determining if we are in Devtools' ytexec environment or not.
    ytrecipe = os.environ.get("YT_OUTPUT") is not None

    destination = os.path.join(get_work_path(), "build", "suid" if need_suid else "nosuid")

    if "trunk" in artifact_components:
        # Explicitly specifying trunk components is not necessary as we already assume
        # any component to be taken from trunk by default. We still allow doing that for clarity.
        # So just skip such components.
        artifact_components.pop("trunk")

    bin_path = os.path.join(destination, "bin")

    if (force_create_environment or artifact_components) and os.path.exists(destination):
        shutil.rmtree(destination)

    if not os.path.exists(destination):
        os.makedirs(destination)
        path = arcadia_interop.prepare_yt_environment(
            destination,
            binary_root=get_build_root(),
            copy_ytserver_all=not ytrecipe,
            need_suid=need_suid and not ytrecipe,
            artifact_components=artifact_components,
        )
        assert path == bin_path

    SANDBOX_ROOTDIR = get_tests_sandbox(arcadia_suffix=None)
    if arcadia_interop.yatest_common is not None:
        OUTPUT_PATH = arcadia_interop.yatest_common.output_path()
    else:
        OUTPUT_PATH = SANDBOX_ROOTDIR

    return bin_path


def search_binary_path(binary_name):
    return arcadia_interop.search_binary_path(binary_name, binary_root=get_build_root())


def _retry_with_gc_collect(func, driver=None):
    while True:
        try:
            func()
            break
        except YtResponseError:
            yt_commands.gc_collect(driver=driver)


def find_ut_file(file_name):
    import library.python.resource as rs

    with open(file_name, "wb") as bc:
        bc_content = rs.find("/llvm_bc/" + file_name.split(".")[0])
        bc.write(bc_content)
    return file_name


def skip_if_porto(func):
    def wrapper(func, self, *args, **kwargs):
        if hasattr(self, "USE_PORTO") and self.USE_PORTO:
            pytest.skip("This test does not support Porto isolation")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


def get_sanitizer_type():
    if arcadia_interop.yatest_common is None:
        return None

    try:
        return arcadia_interop.yatest_common.context.sanitize
    except NotImplementedError:
        # This method is called from is_{asan,msan}_build which may be
        # called outside of testing runtime. For example, when one test
        # imports class from another test, Arcadia binary python building
        # system collects dependencies; during that process class code is
        # interpreted, and if one of the tests imposes pytest.mark.skipif
        # depending on sanitizer type, error is raised.
        #
        # We do not want error to be raised in this case as it breaks
        # Arcadia's import_test.
        return None


def is_asan_build():
    return get_sanitizer_type() == "address"


def is_msan_build():
    return get_sanitizer_type() == "memory"


def is_debug_build():
    if arcadia_interop.yatest_common is None:
        return False

    return "debug" in arcadia_interop.yatest_common.context.build_type


def skip_if_rpc_driver_backend(func):
    def wrapper(func, self, *args, **kwargs):
        if self.DRIVER_BACKEND == "rpc":
            pytest.skip("This test is not supported with RPC proxy driver backend")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)


def parametrize_external(func):
    spec = decorator.getfullargspec(func)
    index = spec.args.index("external")

    def wrapper(func, self, *args, **kwargs):
        if self.NUM_SECONDARY_MASTER_CELLS == 0 and args[index - 1] is True:
            pytest.skip("No secondary cells")
        return func(self, *args, **kwargs)

    return pytest.mark.parametrize("external", [False, True])(decorator.decorate(func, wrapper))


class Checker(Thread):
    def __init__(self, check_function):
        super(Checker, self).__init__()
        self._check_function = check_function
        self._active = None

    def start(self):
        self._active = True
        super(Checker, self).start()

    def run(self):
        while self._active:
            now = time()
            self._check_function()
            delta = time() - now
            if delta > 0.1:
                print("Check took %lf seconds" % delta, file=sys.stderr)
            sleep(1.0)

    def stop(self):
        self._active = False
        self.join()


class YTEnvSetup(object):
    NUM_MASTERS = 3
    NUM_CLOCKS = 0
    NUM_DISCOVERY_SERVERS = 0
    NUM_TIMESTAMP_PROVIDERS = 0
    NUM_NONVOTING_MASTERS = 0
    NUM_SECONDARY_MASTER_CELLS = 0
    DEFER_SECONDARY_CELL_START = False
    ENABLE_SECONDARY_CELLS_CLEANUP = True
    MASTER_CELL_DESCRIPTORS = {}
    NUM_NODES = 5
    DEFER_NODE_START = False
    NUM_CHAOS_NODES = 0
    DEFER_CHAOS_NODE_START = False
    NUM_MASTER_CACHES = 0
    NUM_SCHEDULERS = 0
    DEFER_SCHEDULER_START = False
    NUM_CONTROLLER_AGENTS = None
    DEFER_CONTROLLER_AGENT_START = False
    ENABLE_HTTP_PROXY = False
    NUM_HTTP_PROXIES = 1
    ENABLE_RPC_PROXY = None
    NUM_RPC_PROXIES = 2
    DRIVER_BACKEND = "native"
    NODE_PORT_SET_SIZE = None
    STORE_LOCATION_COUNT = 1
    ARTIFACT_COMPONENTS = {}
    FORCE_CREATE_ENVIRONMENT = False
    NUM_CELL_BALANCERS = 0
    NUM_QUEUE_AGENTS = 0
    NUM_TABLET_BALANCERS = 0
    NUM_CYPRESS_PROXIES = 0
    ENABLE_RESOURCE_TRACKING = False
    ENABLE_TVM_ONLY_PROXIES = False
    ENABLE_DYNAMIC_TABLE_COLUMN_RENAMES = True
    ENABLE_STATIC_DROP_COLUMN = True
    ENABLE_DYNAMIC_DROP_COLUMN = True

    DELTA_DRIVER_CONFIG = {}
    DELTA_RPC_DRIVER_CONFIG = {}
    DELTA_MASTER_CONFIG = {}
    DELTA_DYNAMIC_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_DYNAMIC_NODE_CONFIG = {}
    DELTA_CHAOS_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}
    DELTA_CONTROLLER_AGENT_CONFIG = {}
    _DEFAULT_DELTA_CONTROLLER_AGENT_CONFIG = {
        "operation_options": {
            "spec_template": {
                "max_failed_job_count": 1,
            }
        },
        "controller_agent": {
            "enable_table_column_renaming": True,
        },
    }
    DELTA_PROXY_CONFIG = {}
    DELTA_RPC_PROXY_CONFIG = {}
    DELTA_CELL_BALANCER_CONFIG = {}
    DELTA_TABLET_BALANCER_CONFIG = {}
    DELTA_MASTER_CACHE_CONFIG = {}
    DELTA_QUEUE_AGENT_CONFIG = {}
    DELTA_CYPRESS_PROXY_CONFIG = {}

    USE_PORTO = False
    USE_CUSTOM_ROOTFS = False
    USE_DYNAMIC_TABLES = False
    USE_MASTER_CACHE = False
    USE_PERMISSION_CACHE = True
    USE_PRIMARY_CLOCKS = True
    USE_SEQUOIA = False
    ENABLE_BULK_INSERT = False
    ENABLE_TMP_PORTAL = False
    ENABLE_TABLET_BALANCER = False
    ENABLE_STANDALONE_TABLET_BALANCER = False

    NUM_REMOTE_CLUSTERS = 0
    NUM_TEST_PARTITIONS = 1
    NODE_IO_ENGINE_TYPE = None  # use "thread_pool" or "uring"
    NODE_USE_DIRECT_IO_FOR_READS = "never"

    # COMPAT(ignat)
    UPLOAD_DEBUG_ARTIFACT_CHUNKS = False

    # COMPAT(kvk1920)
    TEST_LOCATION_AWARE_REPLICATOR = False

    # COMPAT(kvk1920)
    TEST_MAINTENANCE_FLAGS = False

    @classmethod
    def is_multicell(cls):
        return cls.NUM_SECONDARY_MASTER_CELLS > 0

    @classmethod
    def get_num_secondary_master_cells(cls):
        return cls.NUM_SECONDARY_MASTER_CELLS

    # To be redefined in successors
    @classmethod
    def modify_master_config(cls, config, tag, index):
        pass

    @classmethod
    def modify_scheduler_config(cls, config):
        pass

    @classmethod
    def modify_queue_agent_config(cls, config):
        pass

    @classmethod
    def modify_controller_agent_config(cls, config):
        pass

    @classmethod
    def modify_node_config(cls, config):
        pass

    @classmethod
    def modify_proxy_config(cls, config):
        pass

    @classmethod
    def modify_rpc_proxy_config(cls, config):
        pass

    @classmethod
    def modify_cell_balancer_config(cls, config):
        pass

    @classmethod
    def modify_tablet_balancer_config(cls, config):
        pass

    @classmethod
    def modify_cypress_proxy_config(cls, config):
        pass

    @classmethod
    def modify_master_cache_config(cls, config):
        pass

    @classmethod
    def on_masters_started(cls):
        pass

    @classmethod
    def get_param(cls, name, cluster_index):
        value = getattr(cls, name)
        if cluster_index != 0:
            return value

        param_name = "{0}_REMOTE_{1}".format(name, cluster_index - 1)
        if hasattr(cls, param_name):
            return getattr(cls, param_name)

        return value

    @classmethod
    def partition_items(cls, items):
        if cls.NUM_TEST_PARTITIONS == 1:
            return [items]

        partitions = [[] for _ in range(cls.NUM_TEST_PARTITIONS)]
        for item in items:
            nodeid = item.nodeid.encode("utf-8")
            hexdigest = hashlib.md5(nodeid).hexdigest()
            partitions[int(hexdigest, 16) % len(partitions)].append(item)
        return partitions

    @classmethod
    def create_yt_cluster_instance(cls, index, path):
        modify_configs_func = functools.partial(cls.apply_config_patches, cluster_index=index)

        yt.logger.info("Creating cluster instance")

        if hasattr(cls, "USE_NATIVE_AUTH"):
            use_native_auth = getattr(cls, "USE_NATIVE_AUTH")
        else:
            # Use native auth by default in arcadia.
            use_native_auth = arcadia_interop.yatest_common is not None

        yt_config = LocalYtConfig(
            use_porto_for_servers=cls.USE_PORTO,
            use_native_client=True,
            master_count=cls.get_param("NUM_MASTERS", index),
            nonvoting_master_count=cls.get_param("NUM_NONVOTING_MASTERS", index),
            secondary_cell_count=cls.get_param("NUM_SECONDARY_MASTER_CELLS", index),
            defer_secondary_cell_start=cls.get_param("DEFER_SECONDARY_CELL_START", index),
            clock_count=(
                cls.get_param("NUM_CLOCKS", index)
                if not cls.get_param("USE_PRIMARY_CLOCKS", index) or index == 0
                else 0),
            timestamp_provider_count=cls.get_param("NUM_TIMESTAMP_PROVIDERS", index),
            cell_balancer_count=cls.get_param("NUM_CELL_BALANCERS", index),
            discovery_server_count=cls.get_param("NUM_DISCOVERY_SERVERS", index),
            queue_agent_count=cls.get_param("NUM_QUEUE_AGENTS", index),
            node_count=cls.get_param("NUM_NODES", index),
            defer_node_start=cls.get_param("DEFER_NODE_START", index),
            chaos_node_count=cls.get_param("NUM_CHAOS_NODES", index),
            defer_chaos_node_start=cls.get_param("DEFER_CHAOS_NODE_START", index),
            master_cache_count=cls.get_param("NUM_MASTER_CACHES", index),
            scheduler_count=cls.get_param("NUM_SCHEDULERS", index),
            defer_scheduler_start=cls.get_param("DEFER_SCHEDULER_START", index),
            controller_agent_count=(
                cls.get_param("NUM_CONTROLLER_AGENTS", index)
                if cls.get_param("NUM_CONTROLLER_AGENTS", index) is not None
                else cls.get_param("NUM_SCHEDULERS", index)),
            tablet_balancer_count=cls.get_param("NUM_TABLET_BALANCERS", index),
            defer_controller_agent_start=cls.get_param("DEFER_CONTROLLER_AGENT_START", index),
            http_proxy_count=(
                cls.get_param("NUM_HTTP_PROXIES", index) if cls.get_param("ENABLE_HTTP_PROXY", index) else 0),
            rpc_proxy_count=(
                cls.get_param("NUM_RPC_PROXIES", index) if cls.get_param("ENABLE_RPC_PROXY", index) else 0),
            cypress_proxy_count=cls.get_param("NUM_CYPRESS_PROXIES", index),
            fqdn="localhost",
            enable_master_cache=cls.get_param("USE_MASTER_CACHE", index),
            enable_permission_cache=cls.get_param("USE_PERMISSION_CACHE", index),
            primary_cell_tag=(index + 1) * 10,
            enable_structured_logging=True,
            enable_log_compression=True,
            log_compression_method="zstd",
            node_port_set_size=cls.get_param("NODE_PORT_SET_SIZE", index),
            store_location_count=cls.get_param("STORE_LOCATION_COUNT", index),
            node_io_engine_type=cls.get_param("NODE_IO_ENGINE_TYPE", index),
            node_use_direct_io_for_reads=cls.get_param("NODE_USE_DIRECT_IO_FOR_READS", index),
            cluster_name=cls.get_cluster_name(index),
            enable_resource_tracking=cls.get_param("ENABLE_RESOURCE_TRACKING", index),
            enable_tvm_only_proxies=cls.get_param("ENABLE_TVM_ONLY_PROXIES", index),
            mock_tvm_id=(1000 + index if use_native_auth else None),
        )

        instance = YTInstance(
            path,
            yt_config,
            watcher_config={"disable_logrotate": True},
            kill_child_processes=True,
            modify_configs_func=modify_configs_func,
            stderrs_path=os.path.join(
                OUTPUT_PATH,
                "yt_stderrs",
                cls.run_name,
                str(index)),
            external_bin_path=cls.bin_path
        )

        instance._cluster_name = cls.get_cluster_name(index)
        setattr(instance, "_default_driver_backend", cls.get_param("DRIVER_BACKEND", index))

        return instance

    @staticmethod
    def get_cluster_name(cluster_index):
        if cluster_index == 0:
            return "primary"
        else:
            return "remote_" + str(cluster_index - 1)

    @classmethod
    def get_cluster_names(cls):
        return [cls.get_cluster_name(cluster_index) for cluster_index in range(cls.NUM_REMOTE_CLUSTERS + 1)]

    @classmethod
    def setup_class(cls, test_name=None, run_id=None):
        logging.basicConfig(level=logging.INFO)

        cls._start_time = time()

        need_suid = False
        cls.cleanup_root_files = False
        if cls.USE_PORTO:
            need_suid = True
            cls.cleanup_root_files = True

        # Initialize `cls` fields before actual setup to make teardown correct.

        # TODO(ignat): Rename Env to env
        cls.Env = None
        cls.remote_envs = []

        if test_name is None:
            test_name = cls.__name__
        cls.test_name = test_name

        cls.liveness_checkers = []

        log_rotator = Checker(reopen_logs)
        log_rotator.daemon = True
        log_rotator.start()
        cls.liveness_checkers.append(log_rotator)

        # The following line initializes SANDBOX_ROOTDIR.
        cls.bin_path = prepare_yatest_environment(
            need_suid=need_suid,
            artifact_components=cls.ARTIFACT_COMPONENTS,
            force_create_environment=cls.FORCE_CREATE_ENVIRONMENT)
        cls.path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        cls.run_id = None
        cls.path_to_run = cls.path_to_test

        cls.run_name = os.path.basename(cls.path_to_run)

        if os.environ.get("YT_OUTPUT") is None:
            disk_path = SANDBOX_ROOTDIR
        else:
            disk_path = os.environ.get("HDD_PATH")

        cls.default_disk_path = os.path.join(disk_path, cls.run_name, "disk_default")
        cls.ssd_disk_path = os.path.join(disk_path, cls.run_name, "disk_ssd")

        cls.fake_default_disk_path = os.path.join(
            OUTPUT_PATH,
            cls.run_name,
            "disk_default")
        cls.fake_ssd_disk_path = os.path.join(
            OUTPUT_PATH,
            cls.run_name,
            "disk_ssd")

        cls.primary_cluster_path = cls.path_to_run
        if cls.NUM_REMOTE_CLUSTERS > 0:
            cls.primary_cluster_path = os.path.join(cls.path_to_run, "primary")

        try:
            cls.start_envs()
        except:  # noqa
            cls.teardown_class()
            raise

    @classmethod
    def start_envs(cls):
        cls.Env = cls.create_yt_cluster_instance(0, cls.primary_cluster_path)
        for cluster_index in range(1, cls.NUM_REMOTE_CLUSTERS + 1):
            cluster_path = os.path.join(cls.path_to_run, cls.get_cluster_name(cluster_index))
            cls.remote_envs.append(cls.create_yt_cluster_instance(cluster_index, cluster_path))

        latest_run_path = os.path.join(cls.path_to_test, "run_latest")
        if os.path.exists(latest_run_path):
            os.remove(latest_run_path)
        os.symlink(cls.path_to_run, latest_run_path)

        yt_commands.is_multicell = cls.is_multicell()
        yt_commands.path_to_run_tests = cls.path_to_run

        yt_commands.init_drivers([cls.Env] + cls.remote_envs)

        cls.Env.start(on_masters_started_func=cls.on_masters_started)
        for index, env in enumerate(cls.remote_envs):
            env.start()

        yt_commands.wait_drivers()

        for env in [cls.Env] + cls.remote_envs:
            liveness_checker = Checker(lambda: env.check_liveness(callback_func=emergency_exit_within_tests))
            liveness_checker.daemon = True
            liveness_checker.start()
            cls.liveness_checkers.append(liveness_checker)

        if len(cls.Env.configs["master"]) > 0:
            clusters = {}
            for instance in [cls.Env] + cls.remote_envs:
                clusters[instance._cluster_name] = instance.get_cluster_configuration()["cluster_connection"]

            for cluster_index in range(cls.NUM_REMOTE_CLUSTERS + 1):
                cluster_name = cls.get_cluster_name(cluster_index)
                driver = yt_commands.get_driver(cluster=cluster_name)
                if driver is None:
                    continue

                requests = [
                    yt_commands.make_batch_request("set", path="//sys/@cluster_name", input=cluster_name),
                    yt_commands.make_batch_request("set", path="//sys/clusters", input=clusters),
                    yt_commands.make_batch_request("set", path="//sys/@cluster_connection",
                                                   input=clusters[cluster_name])
                ]
                responses = yt_commands.execute_batch(requests, driver=driver)
                for response in responses:
                    yt_commands.raise_batch_error(response)

        # TODO(babenko): wait for cluster sync
        if cls.remote_envs:
            sleep(1.0)

        if yt_commands.is_multicell and not cls.DEFER_SECONDARY_CELL_START:
            yt_commands.remove("//sys/operations")
            yt_commands.create("portal_entrance", "//sys/operations", attributes={"exit_cell_tag": 11})

        if cls.USE_SEQUOIA:
            yt_commands.sync_create_cells(1, tablet_cell_bundle="sequoia")
            yt_commands.set("//sys/accounts/sequoia/@resource_limits/tablet_count", 10000)
            yt_commands.create(
                "table",
                "//sys/sequoia/chunk_meta_extensions",
                attributes={
                    "dynamic": True,
                    "schema": [
                        {"name": "id_hash", "type": "uint64", "sort_order": "ascending"},
                        {"name": "id", "type": "string", "sort_order": "ascending"},
                        {"name": "misc_ext", "type": "string"},
                        {"name": "hunk_chunk_refs_ext", "type": "string"},
                        {"name": "hunk_chunk_misc_ext", "type": "string"},
                        {"name": "boundary_keys_ext", "type": "string"},
                        {"name": "heavy_column_statistics_ext", "type": "string"},
                    ],
                    "tablet_cell_bundle": "sequoia",
                    "account": "sequoia",
                })
            yt_commands.sync_mount_table("//sys/sequoia/chunk_meta_extensions")

            yt_commands.create(
                "table",
                "//sys/sequoia/resolve_node",
                attributes={
                    "dynamic": True,
                    "schema": [
                        {"name": "path", "type": "string", "sort_order": "ascending"},
                        {"name": "node_id", "type": "string"},
                    ],
                    "tablet_cell_bundle": "sequoia",
                    "account": "sequoia",
                })
            yt_commands.sync_mount_table("//sys/sequoia/resolve_node")

        if cls.USE_DYNAMIC_TABLES:
            for cluster_index in range(cls.NUM_REMOTE_CLUSTERS + 1):
                driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
                if driver is None:
                    continue
                # Raise dynamic tables limits since they are zero by default.
                yt_commands.set(
                    "//sys/accounts/tmp/@resource_limits/tablet_count",
                    10000,
                    driver=driver,
                )
                yt_commands.set(
                    "//sys/accounts/tmp/@resource_limits/tablet_static_memory",
                    1024 * 1024 * 1024,
                    driver=driver,
                )

                if cls.ENABLE_STANDALONE_TABLET_BALANCER:
                    if not yt_commands.exists("//sys/tablet_balancer/config", driver=driver):
                        yt_commands.create(
                            "document",
                            "//sys/tablet_balancer/config",
                            attributes={"value": {}},
                            force=True,
                            driver=driver)

        if cls.USE_CUSTOM_ROOTFS:
            yt_commands.create("map_node", "//layers")

            yt_commands.create("file", "//layers/exec.tar.gz", attributes={"replication_factor": 1})
            yt_commands.write_file("//layers/exec.tar.gz", open("rootfs/exec.tar.gz", "rb").read())
            yt_commands.create("file", "//layers/rootfs.tar.gz", attributes={"replication_factor": 1})
            yt_commands.write_file("//layers/rootfs.tar.gz", open("rootfs/rootfs.tar.gz", "rb").read())

    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index):
        for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
            for index, config in enumerate(configs["master"][tag]):
                config = update_inplace(config, cls.get_param("DELTA_MASTER_CONFIG", cluster_index))
                configs["master"][tag][index] = cls.update_timestamp_provider_config(cluster_index, config)
                cls.modify_master_config(configs["master"][tag][index], tag, index)
        for index, config in enumerate(configs["scheduler"]):
            config = update_inplace(config, cls.get_param("DELTA_SCHEDULER_CONFIG", cluster_index))

            # COMPAT(pogorelov)
            if "scheduler" in cls.ARTIFACT_COMPONENTS.get("23_1", []):
                config["scheduler"]["control_unknown_operation_jobs_lifetime"] = False

            configs["scheduler"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_scheduler_config(configs["scheduler"][index])
        for index, config in enumerate(configs["queue_agent"]):
            config = update_inplace(config, cls.get_param("DELTA_QUEUE_AGENT_CONFIG", cluster_index))
            configs["queue_agent"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_queue_agent_config(configs["queue_agent"][index])
        for index, config in enumerate(configs["cell_balancer"]):
            config = update_inplace(config, cls.get_param("DELTA_CELL_BALANCER_CONFIG", cluster_index))
            configs["cell_balancer"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_cell_balancer_config(configs["cell_balancer"][index])
        for index, config in enumerate(configs["tablet_balancer"]):
            config = update_inplace(config, cls.get_param("DELTA_TABLET_BALANCER_CONFIG", cluster_index))
            configs["tablet_balancer"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_tablet_balancer_config(configs["tablet_balancer"][index])
        for index, config in enumerate(configs["master_cache"]):
            config = update_inplace(config, cls.get_param("DELTA_MASTER_CACHE_CONFIG", cluster_index))
            configs["master_cache"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_master_cache_config(configs["master_cache"][index])
        for index, config in enumerate(configs["controller_agent"]):
            delta_config = cls.get_param("DELTA_CONTROLLER_AGENT_CONFIG", cluster_index)
            config = update_inplace(
                update_inplace(config, YTEnvSetup._DEFAULT_DELTA_CONTROLLER_AGENT_CONFIG),
                delta_config,
            )

            # COMPAT(pogorelov)
            if "controller-agent" in cls.ARTIFACT_COMPONENTS.get("23_1", []):
                config["controller_agent"]["control_job_lifetime_at_scheduler"] = False
                config["controller_agent"]["job_tracker"]["abort_vanished_jobs"] = True

            # COMPAT(kvk1920)
            if "master" in cls.ARTIFACT_COMPONENTS.get("22_4", []) + cls.ARTIFACT_COMPONENTS.get("23_1", []):
                config["controller_agent"]["set_committed_attribute_via_transaction_action"] = False

            configs["controller_agent"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_controller_agent_config(configs["controller_agent"][index])
        for index, config in enumerate(configs["node"]):
            config = update_inplace(config, cls.get_param("DELTA_NODE_CONFIG", cluster_index))
            if cls.USE_PORTO:
                config = update_inplace(config, get_porto_delta_node_config())
            if cls.USE_CUSTOM_ROOTFS:
                config = update_inplace(config, get_custom_rootfs_delta_node_config())

            config["exec_node"]["job_proxy_upload_debug_artifact_chunks"] = cls.UPLOAD_DEBUG_ARTIFACT_CHUNKS

            config["ref_counted_tracker_dump_period"] = 5000

            config = cls.update_timestamp_provider_config(cluster_index, config)
            configs["node"][index] = config
            cls.modify_node_config(configs["node"][index])

        for index, config in enumerate(configs["chaos_node"]):
            config = update_inplace(config, cls.get_param("DELTA_CHAOS_NODE_CONFIG", cluster_index))
            configs["chaos_node"][index] = cls.update_timestamp_provider_config(cluster_index, config)

        for index, config in enumerate(configs["http_proxy"]):
            delta_config = cls.get_param("DELTA_PROXY_CONFIG", cluster_index)
            config = update_inplace(config, delta_config)
            configs["http_proxy"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_proxy_config(configs["http_proxy"])

        for index, config in enumerate(configs["rpc_proxy"]):
            config = update_inplace(config, cls.get_param("DELTA_RPC_PROXY_CONFIG", cluster_index))
            configs["rpc_proxy"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_rpc_proxy_config(configs["rpc_proxy"])

        for index, config in enumerate(configs["cypress_proxy"]):
            config = update_inplace(config, cls.get_param("DELTA_CYPRESS_PROXY_CONFIG", cluster_index))
            configs["cypress_proxy"][index] = cls.update_timestamp_provider_config(cluster_index, config)
            cls.modify_cypress_proxy_config(configs["cypress_proxy"][index])

        for key, config in configs["driver"].items():
            config = update_inplace(config, cls.get_param("DELTA_DRIVER_CONFIG", cluster_index))
            configs["driver"][key] = cls.update_timestamp_provider_config(cluster_index, config)

        configs["rpc_driver"] = update_inplace(
            configs["rpc_driver"],
            cls.get_param("DELTA_RPC_DRIVER_CONFIG", cluster_index),
        )

    @classmethod
    def update_timestamp_provider_config(cls, cluster_index, config):
        if cls.get_param("NUM_CLOCKS", cluster_index) == 0 or cluster_index == 0 or not cls.get_param("USE_PRIMARY_CLOCKS", cluster_index):
            return config
        primary_timestamp_provider = cls.Env.configs["chaos_node"][0]["cluster_connection"]["timestamp_provider"]
        if "timestamp_provider" in config.keys():
            config["timestamp_provider"] = primary_timestamp_provider
        if "cluster_connection" in config.keys():
            config["cluster_connection"]["timestamp_provider"] = primary_timestamp_provider
        return config

    @classmethod
    def teardown_class(cls):
        if cls.liveness_checkers:
            map(lambda c: c.stop(), cls.liveness_checkers)

        for env in [cls.Env] + cls.remote_envs:
            if env is None:
                continue
            env.stop()
            env.remove_runtime_data()

        yt_commands.terminate_drivers()
        gc.collect()

        class_duration = time() - cls._start_time
        class_limit = (16 * 60) if is_asan_build() else (8 * 60)

        if class_duration > class_limit:
            pytest.fail(
                "Execution of class {} took more than {} seconds ({} seconds).\n"
                "Check test stdout for detailed duration report.\n"
                "You can split class into smaller partitions, using NUM_TEST_PARTITIONS option (current value is {})."
                .format(cls.__name__, class_limit, class_duration, cls.NUM_TEST_PARTITIONS)
            )

    def setup_method(self, method):
        for cluster_index in range(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                continue

            master_cell_descriptors = self.get_param("MASTER_CELL_DESCRIPTORS", cluster_index)

            node_count = self.get_param("NUM_NODES", cluster_index) + self.get_param("NUM_CHAOS_NODES", cluster_index)
            if node_count > 0:
                wait(lambda: yt_commands.get("//sys/cluster_nodes/@count", driver=driver) == node_count)

            scheduler_count = self.get_param("NUM_SCHEDULERS", cluster_index)
            if scheduler_count > 0:
                scheduler_pool_trees_root = self.Env.configs["scheduler"][0]["scheduler"].get(
                    "pool_trees_root", "//sys/pool_trees"
                )
            else:
                scheduler_pool_trees_root = "//sys/pool_trees"
            self._restore_globals(
                cluster_index=cluster_index,
                master_cell_descriptors=master_cell_descriptors,
                scheduler_count=scheduler_count,
                scheduler_pool_trees_root=scheduler_pool_trees_root,
                driver=driver,
            )

            yt_commands.gc_collect(driver=driver)

            yt_commands.clear_metadata_caches(driver=driver)

            if node_count > 0:
                self._setup_nodes_dynamic_config(driver=driver, cluster_index=cluster_index)

            if self.USE_DYNAMIC_TABLES:
                self._setup_tablet_manager(driver=driver)
                self._clear_ql_pools(driver=driver)
                self._restore_default_bundle_options(driver=driver)
                self._setup_tablet_balancer_dynamic_config(driver=driver)

            if not self.get_param("DEFER_SECONDARY_CELL_START", cluster_index):
                yt_commands.wait_for_nodes(driver=driver)
                yt_commands.wait_for_chunk_replicator_enabled(driver=driver)

            if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
                for response in yt_commands.execute_batch(
                    [
                        yt_commands.make_batch_request(
                            "create",
                            path="//sys/controller_agents/config",
                            type="document",
                            attributes={
                                "value": {
                                    "enable_bulk_insert_for_everyone": self.ENABLE_BULK_INSERT,
                                    "enable_versioned_remote_copy": self.ENABLE_BULK_INSERT,
                                    "testing_options": {
                                        "rootfs_test_layers": [
                                            "//layers/exec.tar.gz",
                                            "//layers/rootfs.tar.gz",
                                        ]
                                        if self.USE_CUSTOM_ROOTFS
                                        else [],
                                    },
                                }
                            },
                            force=True,
                        ),
                        yt_commands.make_batch_request(
                            "create",
                            path="//sys/scheduler/config",
                            type="document",
                            attributes={"value": {}},
                            force=True,
                        ),
                    ],
                    driver=driver,
                ):
                    assert not yt_commands.get_batch_error(response)

                def _get_config_versions(orchids):
                    requests = [
                        yt_commands.make_batch_request(
                            "get",
                            path=orchid + "/config_revision",
                            return_only_value=True,
                        )
                        for orchid in orchids
                    ]
                    responses = yt_commands.execute_batch(requests, driver=driver)
                    return list(map(lambda r: yt_commands.get_batch_output(r), responses))

                def _wait_for_configs(orchids):
                    old_versions = _get_config_versions(orchids)

                    def _wait_func():
                        new_versions = _get_config_versions(orchids)
                        return all(new >= old + 2 for old, new in zip(old_versions, new_versions))

                    wait(_wait_func)

                orchids = []
                for instance in yt_commands.ls("//sys/controller_agents/instances", driver=driver):
                    orchids.append("//sys/controller_agents/instances/{}/orchid/controller_agent".format(instance))
                orchids.append("//sys/scheduler/orchid/scheduler")
                _wait_for_configs(orchids)

            if self.ENABLE_TMP_PORTAL and cluster_index == 0:
                yt_commands.create(
                    "portal_entrance",
                    "//tmp",
                    attributes={
                        "account": "tmp",
                        "exit_cell_tag": 11,
                        "acl": [
                            {
                                "action": "allow",
                                "permissions": ["read", "write", "remove"],
                                "subjects": ["users"],
                            },
                            {
                                "action": "allow",
                                "permissions": ["read"],
                                "subjects": ["everyone"],
                            },
                        ],
                    },
                    force=True,
                    driver=driver,
                )

                yt_commands.create(
                    "map_node",
                    "//portals",
                    attributes={
                        "account": "tmp",
                        "acl": [
                            {
                                "action": "allow",
                                "permissions": ["read", "write", "remove"],
                                "subjects": ["users"],
                            }
                        ],
                        "opaque": True,
                    },
                    force=True,
                    driver=driver,
                )
            else:
                yt_commands.create(
                    "map_node",
                    "//tmp",
                    attributes={
                        "account": "tmp",
                        "acl": [
                            {
                                "action": "allow",
                                "permissions": ["read", "write", "remove"],
                                "subjects": ["users"],
                            }
                        ],
                        "opaque": True,
                    },
                    force=True,
                    driver=driver,
                )

    def teardown_method(self, method):
        yt_commands._zombie_responses[:] = []

        for env in [self.Env] + self.remote_envs:
            env.check_liveness(callback_func=emergency_exit_within_tests)

        for cluster_index in range(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                continue

            self._reset_nodes(driver=driver)

            if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
                self._remove_operations(driver=driver)
                self._wait_for_jobs_to_vanish(driver=driver)

            self._abort_transactions(driver=driver)

            yt_commands.remove("//tmp", driver=driver)
            if self.ENABLE_TMP_PORTAL:
                yt_commands.remove("//portals", driver=driver)
                # XXX(babenko): portals
                wait(lambda: not yt_commands.exists("//tmp&", driver=driver))

            self._remove_objects(
                enable_secondary_cells_cleanup=self.get_param("ENABLE_SECONDARY_CELLS_CLEANUP", cluster_index),
                driver=driver,
            )

            yt_commands.gc_collect(driver=driver)
            yt_commands.clear_metadata_caches(driver=driver)

        yt_commands.reset_events_on_fs()

    def _abort_transactions(self, driver=None):
        abort_command = "abort_transaction" if driver.get_config()["api_version"] == 4 else "abort_tx"
        requests = []

        test_cleanup.abort_transactions(
            list_action=lambda *args, **kwargs: yt_commands.ls(*args, driver=driver, **kwargs),
            abort_action=lambda transaction_id: requests.append(
                yt_commands.make_batch_request(abort_command, transaction_id=transaction_id)),
            exists_action=lambda *args, **kwargs: yt_commands.exists(*args, driver=driver, **kwargs),
            get_action=lambda *args, **kwargs: yt_commands.get(*args, driver=driver, **kwargs))

        yt_commands.execute_batch(requests)

    def _reset_nodes(self, driver=None):
        use_maintenance_requests = yt_commands.get(
            "//sys/@config/node_tracker/forbid_maintenance_attribute_writes",
            default=False,
            driver=driver)
        if use_maintenance_requests:
            maintenance_attributes = ["maintenance_requests"]
        else:
            maintenance_attributes = ["banned", "decommissioned", "disable_write_sessions", "disable_scheduler_jobs", "disable_tablet_cells"]

        attributes = maintenance_attributes + [
            "maintenance_requests",
            "resource_limits_overrides",
            "user_tags",
        ]
        nodes = yt_commands.ls("//sys/cluster_nodes", attributes=attributes, driver=driver)

        requests = []
        for node in nodes:
            node_name = str(node)
            if node.attributes["resource_limits_overrides"] != {}:
                requests.append(
                    yt_commands.make_batch_request(
                        "set",
                        path="//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node_name),
                        input={},
                    )
                )
            if node.attributes["user_tags"] != []:
                requests.append(
                    yt_commands.make_batch_request(
                        "set",
                        path="//sys/cluster_nodes/{0}/@user_tags".format(node_name),
                        input=[],
                    )
                )
            if use_maintenance_requests:
                if node.attributes["maintenance_requests"]:
                    requests.append(
                        yt_commands.make_batch_request(
                            "remove_maintenance",
                            component="cluster_node",
                            address=node_name,
                            all=True
                        )
                    )
            else:
                for maintenance_flag in maintenance_attributes:
                    if node.attributes[maintenance_flag]:
                        requests.append(
                            yt_commands.make_batch_request(
                                "set",
                                path=f"//sys/cluster_nodes/{node}/@{maintenance_flag}",
                                input="false"
                            ))

        for response in yt_commands.execute_batch(requests, driver=driver):
            assert not yt_commands.get_batch_error(response)

    def _remove_objects(self, enable_secondary_cells_cleanup, driver=None):
        def list_multiple_action(list_kwargs):
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request("list", return_only_value=True, **kwargs)
                    for kwargs in list_kwargs
                ],
                driver=driver)
            # COMPAT(gritukan)
            object_lists = []
            for response in responses:
                objects = []
                try:
                    objects = yt_commands.get_batch_output(response)
                except YtResponseError:
                    pass
                object_lists.append(objects)
            return object_lists

        def remove_multiple_action(remove_kwargs):
            yt_commands.gc_collect(driver=driver)
            yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request("remove", **kwargs)
                    for kwargs in remove_kwargs
                ],
                driver=driver)

        def exists_multiple_action(object_ids):
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request("exists", path=object_id, return_only_value=True)
                    for object_id in object_ids
                ],
                driver=driver)
            return [yt_commands.get_batch_output(response)["value"] for response in responses]

        def get_object_ids_to_ignore():
            ids = []

            # COMPAT(gritukan, aleksandra-zh)
            if yt_commands.exists("//sys/tablet_cell_bundles/sequoia"):
                ids += yt_commands.get("//sys/tablet_cell_bundles/sequoia/@tablet_cell_ids")

            return ids

        test_cleanup.cleanup_objects(
            list_multiple_action=list_multiple_action,
            remove_multiple_action=remove_multiple_action,
            exists_multiple_action=exists_multiple_action,
            enable_secondary_cells_cleanup=enable_secondary_cells_cleanup,
            object_ids_to_ignore=get_object_ids_to_ignore(),
        )

    def _wait_for_scheduler_state_restored(self, driver=None):
        exec_node_count = len(yt_commands.get_exec_nodes())

        def check():
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "list",
                        path=yt_scheduler_helpers.scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "get",
                        path=yt_scheduler_helpers.scheduler_orchid_path() + "/scheduler/default_pool_tree",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "list",
                        path=yt_scheduler_helpers.scheduler_orchid_default_pool_tree_path() + "/pools",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "get",
                        path=yt_scheduler_helpers.scheduler_orchid_path()
                        + "/scheduler/scheduling_info_per_pool_tree/default/node_count",
                        return_only_value=True,
                    ),
                ],
                driver=driver,
            )
            return (
                all(yt_commands.get_batch_error(r) is None for r in responses)
                and yt_commands.get_batch_output(responses[0]) == ["default"]
                and yt_commands.get_batch_output(responses[1]) == "default"
                and yt_commands.get_batch_output(responses[2]) == ["<Root>"]
                and yt_commands.get_batch_output(responses[3]) == exec_node_count
            )

        wait(check)

    def _restore_globals(self,
                         cluster_index,
                         master_cell_descriptors,
                         scheduler_count,
                         scheduler_pool_trees_root,
                         driver=None):
        dynamic_master_config = get_dynamic_master_config()
        dynamic_master_config = update_inplace(
            dynamic_master_config, self.get_param("DELTA_DYNAMIC_MASTER_CONFIG", cluster_index)
        )
        dynamic_master_config["multicell_manager"]["cell_descriptors"] = master_cell_descriptors
        if self.Env.get_component_version("ytserver-master").abi >= (20, 4):
            dynamic_master_config["enable_descending_sort_order"] = True
            dynamic_master_config["enable_descending_sort_order_dynamic"] = True
        if self.Env.get_component_version("ytserver-master").abi >= (22, 1):
            dynamic_master_config["enable_table_column_renaming"] = True
        allow_dynamic_renames = \
            self.Env.get_component_version("ytserver-master").abi >= (23, 1) and \
            self.ENABLE_DYNAMIC_TABLE_COLUMN_RENAMES

        if allow_dynamic_renames:
            dynamic_master_config["enable_dynamic_table_column_renaming"] = True

        dynamic_master_config["enable_static_table_drop_column"] = self.ENABLE_STATIC_DROP_COLUMN
        dynamic_master_config["enable_dynamic_table_drop_column"] = self.ENABLE_DYNAMIC_DROP_COLUMN

        if self.USE_SEQUOIA:
            dynamic_master_config["sequoia_manager"]["enable"] = True
            dynamic_master_config["sequoia_manager"]["fetch_chunk_meta_from_sequoia"] = True

        if self.TEST_LOCATION_AWARE_REPLICATOR:
            assert dynamic_master_config["node_tracker"].pop("enable_real_chunk_locations")

        if not self.TEST_MAINTENANCE_FLAGS and self.Env.get_component_version("ytserver-master").abi >= (23, 1):
            dynamic_master_config["node_tracker"]["forbid_maintenance_attribute_writes"] = True

        default_pool_tree_config = {
            "nodes_filter": "",
            "main_resource": "cpu",
            "min_child_heap_size": 3,
            "prioritized_regular_scheduling": {
                "medium_priority_operation_count_limit": 3,
                "low_priority_fallback_min_spare_job_resources": {
                    "cpu": 1.5,
                    "user_slots": 1,
                    "memory": 512 * 1024 * 1024,
                },
            },
            # Make default settings suitable for starvation and preemption.
            "preemptive_scheduling_backoff": 500,
            "max_unpreemptible_running_job_count": 0,
            "fair_share_starvation_timeout": 1000,
            "enable_conditional_preemption": True,
            "check_operation_for_liveness_in_preschedule": False,
        }

        dynamic_master_config.setdefault("chunk_service", {})

        for response in yt_commands.execute_batch(
            [
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/tablet_cell_bundles/default/@dynamic_options",
                    input={},
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/tablet_cell_bundles/default/@tablet_balancer_config",
                    input={},
                ),
                yt_commands.make_batch_request("set", path="//sys/@config", input=dynamic_master_config),
            ],
            driver=driver,
        ):
            assert not yt_commands.get_batch_error(response)

        restore_pool_trees_requests = []
        should_set_default_config = False
        if yt_commands.exists(scheduler_pool_trees_root + "/default", driver=driver):
            restore_pool_trees_requests.append(
                yt_commands.make_batch_request("remove", path=scheduler_pool_trees_root + "/default/*"))
            should_set_default_config = True
        else:
            restore_pool_trees_requests.append(
                yt_commands.make_batch_request(
                    "create",
                    type="scheduler_pool_tree",
                    attributes={
                        "name": "default",
                        "config": default_pool_tree_config,
                    },
                )
            )
        for pool_tree in yt_commands.ls(scheduler_pool_trees_root, driver=driver):
            if pool_tree != "default":
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request("remove", path=scheduler_pool_trees_root + "/" + pool_tree)
                )
        for response in yt_commands.execute_batch(restore_pool_trees_requests, driver=driver):
            yt_commands.raise_batch_error(response)

        # Could not add this to the batch request because of possible races at scheduler.
        yt_commands.set(scheduler_pool_trees_root + "/@default_tree", "default", driver=driver)

        if not yt_commands.exists("//sys/scheduler/user_to_default_pool", driver=driver):
            yt_commands.create(
                "document",
                "//sys/scheduler/user_to_default_pool",
                attributes={"value": {}},
                force=True,
                driver=driver)

        if should_set_default_config:
            yt_commands.set(scheduler_pool_trees_root + "/default/@config", default_pool_tree_config)

        if scheduler_count > 0:
            self._wait_for_scheduler_state_restored(driver=driver)

    def _wait_for_dynamic_config(self, root_path, config, instances, driver=None):
        def check():
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "get",
                        path=f"{root_path}/{instance}/orchid/dynamic_config_manager",
                        return_only_value=True,
                    )
                    for instance in instances
                ],
                driver=driver,
                verbose=False,
            )
            for response in responses:
                output = yt_commands.get_batch_output(response)
                if config != output.get("applied_config"):
                    return False
            return True

        wait(check)

    def _setup_nodes_dynamic_config(self, cluster_index, driver=None):
        config = get_dynamic_node_config()

        config = update_inplace(
            config, self.get_param("DELTA_DYNAMIC_NODE_CONFIG", cluster_index)
        )

        # COMPAT(pogorelov)
        if "controller-agent" in self.__class__.ARTIFACT_COMPONENTS.get("23_1", []):
            config["%true"]["exec_node"]["controller_agent_connector"]["use_job_tracker_service_to_settle_jobs"] = False

        yt_commands.set("//sys/cluster_nodes/@config", config, driver=driver)

        nodes = yt_commands.ls("//sys/cluster_nodes", driver=driver)

        self._wait_for_dynamic_config("//sys/cluster_nodes", config["%true"], nodes, driver=driver)

    def _setup_tablet_manager(self, driver=None):
        for response in yt_commands.execute_batch(
            [
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/tablet_balancer/tablet_balancer_schedule",
                    input="1",
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/tablet_cell_balancer/enable_verbose_logging",
                    input=True,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer",
                    input=self.ENABLE_TABLET_BALANCER,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/enable_bulk_insert",
                    input=self.ENABLE_BULK_INSERT,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/enable_backups",
                    input=True,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/enable_tablet_resource_validation",
                    input=True,
                ),
                yt_commands.make_batch_request(
                    "set",
                    path="//sys/@config/tablet_manager/use_avenues",
                    input=True,
                ),
            ],
            driver=driver,
        ):
            assert not yt_commands.get_batch_error(response)

    def _setup_tablet_balancer_dynamic_config(self, driver=None):
        if self.ENABLE_STANDALONE_TABLET_BALANCER:
            config = {
                "enable": self.ENABLE_STANDALONE_TABLET_BALANCER,
                "enable_everywhere": self.ENABLE_STANDALONE_TABLET_BALANCER,
                "schedule": "1"
            }

            yt_commands.set(
                "//sys/tablet_balancer/config",
                config,
                driver=driver)

            instances = yt_commands.ls("//sys/tablet_balancer/instances")

            self._wait_for_dynamic_config("//sys/tablet_balancer/instances", config, instances, driver=driver)

    def _clear_ql_pools(self, driver=None):
        yt_commands.remove("//sys/ql_pools/*", driver=driver)

    def _restore_default_bundle_options(self, driver=None):
        def do():
            for response in yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "set",
                        path="//sys/tablet_cell_bundles/default/@dynamic_options",
                        input={},
                    ),
                    yt_commands.make_batch_request(
                        "set",
                        path="//sys/tablet_cell_bundles/default/@tablet_balancer_config",
                        input={},
                    ),
                    yt_commands.make_batch_request(
                        "set",
                        path="//sys/tablet_cell_bundles/default/@options",
                        input={
                            "changelog_replication_factor": 1 if self.NUM_NODES < 3 else 3,
                            "changelog_read_quorum": 1 if self.NUM_NODES < 3 else 2,
                            "changelog_write_quorum": 1 if self.NUM_NODES < 3 else 2,
                            "changelog_account": "sys",
                            "snapshot_replication_factor": 1 if self.NUM_NODES < 3 else 3,
                            "snapshot_account": "sys",
                        },
                    ),
                ],
                driver=driver,
            ):
                assert not yt_commands.get_batch_error(response)

        _retry_with_gc_collect(do, driver=driver)

    def _remove_operations(self, driver=None):
        abort_command = "abort_operation" if driver.get_config()["api_version"] == 4 else "abort_op"

        if yt_commands.get("//sys/scheduler/instances/@count", driver=driver) == 0:
            return

        abort_requests = []
        remove_requests = []

        test_cleanup.cleanup_operations(
            list_action=lambda *args, **kwargs: yt_commands.ls(*args, driver=driver, **kwargs),
            abort_action=lambda operation_id: abort_requests.append(
                yt_commands.make_batch_request(abort_command, operation_id=operation_id)),
            remove_action=lambda *args, **kwargs: remove_requests.append(
                yt_commands.make_batch_request("remove", *args, **kwargs)),
        )

        for response in yt_commands.execute_batch(abort_requests, driver=driver):
            err = yt_commands.get_batch_error(response)
            if err is not None:
                print(format_error(err), file=sys.stderr)

        # TODO(ignat): is it actually need to be called here?
        self._abort_transactions(driver=driver)

        for response in yt_commands.execute_batch(remove_requests, driver=driver):
            assert not yt_commands.get_batch_error(response)

    def _wait_for_jobs_to_vanish(self, driver=None):
        nodes = yt_commands.ls("//sys/cluster_nodes", driver=driver)

        def check_no_jobs():
            requests = [
                yt_commands.make_batch_request(
                    "get",
                    path="//sys/cluster_nodes/{0}/orchid/job_controller/active_job_count".format(node),
                    return_only_value=True,
                )
                for node in nodes
            ]
            responses = yt_commands.execute_batch(requests, driver=driver)
            return all(yt_commands.get_batch_output(response).get("scheduler", 0) == 0 for response in responses)

        try:
            wait(check_no_jobs, iter=300)
        except WaitFailed:
            requests = [
                yt_commands.make_batch_request(
                    "list",
                    path="//sys/cluster_nodes/{0}/orchid/job_controller/active_jobs/scheduler".format(node),
                    return_only_value=True,
                )
                for node in nodes
            ]
            responses = yt_commands.execute_batch(requests, driver=driver)
            print("There are remaining scheduler jobs:", file=sys.stderr)
            for node, response in zip(nodes, responses):
                print("Node {}: {}".format(node, response), file=sys.stderr)


def get_porto_delta_node_config():
    return {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            }
        }
    }


def get_custom_rootfs_delta_node_config():
    return {
        "exec_node": {
            "do_not_set_user_id": True,
            "slot_manager": {
                "job_environment": {
                    "use_exec_from_layer": True,
                },
            },
        }
    }
