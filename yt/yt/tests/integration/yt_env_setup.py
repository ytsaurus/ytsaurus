from __future__ import print_function

import yt_commands

from yt_helpers import get_current_time, parse_yt_time

from yt.environment import YTInstance, arcadia_interop
from yt.environment.api import LocalYtConfig
from yt.environment.helpers import emergency_exit_within_tests, push_front_env_path
from yt.environment.default_config import (
    get_dynamic_master_config,
    get_dynamic_node_config,
)
from yt.environment.helpers import (  # noqa
    Restarter,
    SCHEDULERS_SERVICE,
    CONTROLLER_AGENTS_SERVICE,
    NODES_SERVICE,
    MASTERS_SERVICE,
)

from yt.test_helpers import wait, WaitFailed
import yt.test_helpers.cleanup as test_cleanup
from yt.common import YtResponseError, format_error, update_inplace
import yt.logger

import pytest

import gc
import os
import sys
import logging
import decorator
import functools

from time import sleep, time
from threading import Thread

SANDBOX_ROOTDIR = None

##################################################################


def prepare_yatest_environment(need_suid, artifact_components=None):
    yt.logger.LOGGER.setLevel(logging.DEBUG)
    artifact_components = artifact_components or {}

    global SANDBOX_ROOTDIR

    # This env var is used for determining if we are in Devtools' ytexec environment or not.
    ytrecipe = os.environ.get("YT_OUTPUT") is not None

    ram_drive_path = arcadia_interop.yatest_common.get_param("ram_drive_path")
    if ram_drive_path is None:
        destination = os.path.join(arcadia_interop.yatest_common.work_path(), "build")
    else:
        destination = os.path.join(ram_drive_path, "build")

    destination = os.path.join(destination, "suid" if need_suid else "nosuid")

    if "trunk" in artifact_components:
        # Explicitly specifying trunk components is not necessary as we already assume
        # any component to be taken from trunk by default. We still allow doing that for clarity.
        # So just skip such components.
        artifact_components.pop("trunk")

    bin_path = os.path.join(destination, "bin")

    if not os.path.exists(destination):
        os.makedirs(destination)
        path = arcadia_interop.prepare_yt_environment(
            destination,
            inside_arcadia=False,
            copy_ytserver_all=not ytrecipe,
            need_suid=need_suid and not ytrecipe,
            artifact_components=artifact_components,
        )
        assert path == bin_path

    if ytrecipe:
        SANDBOX_ROOTDIR = os.environ.get("YT_OUTPUT")
    elif ram_drive_path is None:
        SANDBOX_ROOTDIR = arcadia_interop.yatest_common.output_path()
    else:
        SANDBOX_ROOTDIR = arcadia_interop.yatest_common.output_ram_drive_path()

    return bin_path


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
    MASTER_CELL_ROLES = {}
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
    ARTIFACT_COMPONENTS = {}

    DELTA_DRIVER_CONFIG = {}
    DELTA_RPC_DRIVER_CONFIG = {}
    DELTA_MASTER_CONFIG = {}
    DELTA_DYNAMIC_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}
    DELTA_CONTROLLER_AGENT_CONFIG = {}
    _DEFAULT_DELTA_CONTROLLER_AGENT_CONFIG = {
        "operation_options": {
            "spec_template": {
                "max_failed_job_count": 1,
            }
        },
    }
    DELTA_PROXY_CONFIG = {}
    DELTA_RPC_PROXY_CONFIG = {}

    USE_PORTO = False
    USE_CUSTOM_ROOTFS = False
    USE_DYNAMIC_TABLES = False
    USE_MASTER_CACHE = False
    USE_PERMISSION_CACHE = True
    ENABLE_BULK_INSERT = False
    ENABLE_TMP_PORTAL = False
    ENABLE_TABLET_BALANCER = False

    NUM_REMOTE_CLUSTERS = 0
    NUM_TEST_PARTITIONS = 1

    @classmethod
    def is_multicell(cls):
        return cls.NUM_SECONDARY_MASTER_CELLS > 0

    # To be redefined in successors
    @classmethod
    def modify_master_config(cls, config, tag, index):
        pass

    @classmethod
    def modify_scheduler_config(cls, config):
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
        items = sorted(items, key=lambda x: x.nodeid)
        for i, item in enumerate(items):
            partitions[i % len(partitions)].append(item)
        return partitions

    @classmethod
    def create_yt_cluster_instance(cls, index, path):
        modify_configs_func = functools.partial(cls.apply_config_patches, cluster_index=index)

        yt_config = LocalYtConfig(
            use_porto_for_servers=cls.USE_PORTO,
            use_native_client=True,
            master_count=cls.get_param("NUM_MASTERS", index),
            nonvoting_master_count=cls.get_param("NUM_NONVOTING_MASTERS", index),
            secondary_cell_count=cls.get_param("NUM_SECONDARY_MASTER_CELLS", index),
            defer_secondary_cell_start=cls.get_param("DEFER_SECONDARY_CELL_START", index),
            clock_count=cls.get_param("NUM_CLOCKS", index),
            timestamp_provider_count=cls.get_param("NUM_TIMESTAMP_PROVIDERS", index),
            discovery_server_count=cls.get_param("NUM_DISCOVERY_SERVERS", index),
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
            defer_controller_agent_start=cls.get_param("DEFER_CONTROLLER_AGENT_START", index),
            http_proxy_count=cls.get_param("NUM_HTTP_PROXIES", index) if cls.get_param("ENABLE_HTTP_PROXY", index) else 0,
            rpc_proxy_count=cls.get_param("NUM_RPC_PROXIES", index) if cls.get_param("ENABLE_RPC_PROXY", index) else 0,
            fqdn="localhost",
            enable_master_cache=cls.get_param("USE_MASTER_CACHE", index),
            enable_permission_cache=cls.get_param("USE_PERMISSION_CACHE", index),
            primary_cell_tag=index * 10,
            enable_structured_logging=True,
            enable_log_compression=True,
            log_compression_method="zstd",
            node_port_set_size=cls.get_param("NODE_PORT_SET_SIZE", index),
        )

        instance = YTInstance(
            path,
            yt_config,
            watcher_config={"disable_logrotate": True},
            kill_child_processes=True,
            modify_configs_func=modify_configs_func,
            stderrs_path=os.path.join(arcadia_interop.yatest_common.output_path("yt_stderrs"), cls.run_name, str(index)),
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

        log_rotator = Checker(yt_commands.reopen_logs)
        log_rotator.daemon = True
        log_rotator.start()
        cls.liveness_checkers.append(log_rotator)

        # The following line initializes SANDBOX_ROOTDIR.
        cls.bin_path = prepare_yatest_environment(need_suid=need_suid, artifact_components=cls.ARTIFACT_COMPONENTS)
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

        cls.fake_default_disk_path = os.path.join(arcadia_interop.yatest_common.output_path(), cls.run_name, "disk_default")
        cls.fake_ssd_disk_path = os.path.join(arcadia_interop.yatest_common.output_path(), cls.run_name, "disk_ssd")

        cls.primary_cluster_path = cls.path_to_run
        if cls.NUM_REMOTE_CLUSTERS > 0:
            cls.primary_cluster_path = os.path.join(cls.path_to_run, "primary")

        try:
            cls.start_envs()
        except:
            cls.teardown_class()
            raise

    @classmethod
    def start_envs(cls):
        cls.Env = cls.create_yt_cluster_instance(0, cls.primary_cluster_path)
        for cluster_index in xrange(1, cls.NUM_REMOTE_CLUSTERS + 1):
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
                clusters[instance._cluster_name] = {
                    "primary_master": instance.configs["master"][0]["primary_master"],
                    "secondary_masters": instance.configs["master"][0]["secondary_masters"],
                    "timestamp_provider": instance.configs["master"][0]["timestamp_provider"],
                    "table_mount_cache": instance.configs["driver"]["table_mount_cache"],
                    "permission_cache": instance.configs["driver"]["permission_cache"],
                    "cell_directory_synchronizer": instance.configs["driver"]["cell_directory_synchronizer"],
                    "cluster_directory_synchronizer": instance.configs["driver"]["cluster_directory_synchronizer"],
                }

            for cluster_index in xrange(cls.NUM_REMOTE_CLUSTERS + 1):
                driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
                if driver is None:
                    continue
                yt_commands.set("//sys/clusters", clusters, driver=driver)

        # TODO(babenko): wait for cluster sync
        if cls.remote_envs:
            sleep(1.0)

        if yt_commands.is_multicell and not cls.DEFER_SECONDARY_CELL_START:
            yt_commands.remove("//sys/operations")
            yt_commands.create("portal_entrance", "//sys/operations", attributes={"exit_cell_tag": 1})

        if cls.USE_DYNAMIC_TABLES:
            for cluster_index in xrange(cls.NUM_REMOTE_CLUSTERS + 1):
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

        if cls.USE_CUSTOM_ROOTFS:
            yt_commands.create("map_node", "//layers")

            yt_commands.create("file", "//layers/exec.tar.gz", attributes={"replication_factor": 1})
            yt_commands.write_file("//layers/exec.tar.gz", open("rootfs/exec.tar.gz").read())
            yt_commands.create("file", "//layers/rootfs.tar.gz", attributes={"replication_factor": 1})
            yt_commands.write_file("//layers/rootfs.tar.gz", open("rootfs/rootfs.tar.gz").read())

    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index):
        for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
            for index, config in enumerate(configs["master"][tag]):
                configs["master"][tag][index] = update_inplace(
                    config, cls.get_param("DELTA_MASTER_CONFIG", cluster_index)
                )
                cls.modify_master_config(configs["master"][tag][index], tag, index)
        for index, config in enumerate(configs["scheduler"]):
            configs["scheduler"][index] = update_inplace(config, cls.get_param("DELTA_SCHEDULER_CONFIG", cluster_index))
            cls.modify_scheduler_config(configs["scheduler"][index])
        for index, config in enumerate(configs["controller_agent"]):
            delta_config = cls.get_param("DELTA_CONTROLLER_AGENT_CONFIG", cluster_index)
            configs["controller_agent"][index] = update_inplace(
                update_inplace(config, YTEnvSetup._DEFAULT_DELTA_CONTROLLER_AGENT_CONFIG),
                delta_config,
            )

            cls.modify_controller_agent_config(configs["controller_agent"][index])
        for index, config in enumerate(configs["node"]):
            config = update_inplace(config, cls.get_param("DELTA_NODE_CONFIG", cluster_index))
            if cls.USE_PORTO:
                config = update_inplace(config, get_porto_delta_node_config())
            if cls.USE_CUSTOM_ROOTFS:
                config = update_inplace(config, get_custom_rootfs_delta_node_config())

            configs["node"][index] = config
            cls.modify_node_config(configs["node"][index])

        for index, config in enumerate(configs["http_proxy"]):
            configs["http_proxy"][index] = update_inplace(config, cls.get_param("DELTA_PROXY_CONFIG", cluster_index))
            cls.modify_proxy_config(configs["http_proxy"])

        for index, config in enumerate(configs["rpc_proxy"]):
            configs["rpc_proxy"][index] = update_inplace(config, cls.get_param("DELTA_RPC_PROXY_CONFIG", cluster_index))
            cls.modify_rpc_proxy_config(configs["rpc_proxy"])

        for key, config in configs["driver"].iteritems():
            configs["driver"][key] = update_inplace(config, cls.get_param("DELTA_DRIVER_CONFIG", cluster_index))

        configs["rpc_driver"] = update_inplace(
            configs["rpc_driver"],
            cls.get_param("DELTA_RPC_DRIVER_CONFIG", cluster_index),
        )

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
        class_limit = (10 * 60) if is_asan_build() else (5 * 60)

        if class_duration > class_limit:
            pytest.fail(
                "Test class execution took more that {} seconds ({} seconds).\n".format(class_limit, class_duration)
                + "Check test stdout for detailed duration report.\n"
                + "You can split class into smaller chunks, using NUM_TEST_PARTITIONS option.")

    def setup_method(self, method):
        for cluster_index in xrange(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                continue

            self._reset_nodes(driver=driver)

            master_cell_roles = self.get_param("MASTER_CELL_ROLES", cluster_index)

            scheduler_count = self.get_param("NUM_SCHEDULERS", cluster_index)
            if scheduler_count > 0:
                scheduler_pool_trees_root = self.Env.configs["scheduler"][0]["scheduler"].get(
                    "pool_trees_root", "//sys/pool_trees"
                )
            else:
                scheduler_pool_trees_root = "//sys/pool_trees"
            self._restore_globals(
                cluster_index=cluster_index,
                master_cell_roles=master_cell_roles,
                scheduler_count=scheduler_count,
                scheduler_pool_trees_root=scheduler_pool_trees_root,
                driver=driver,
            )

            yt_commands.gc_collect(driver=driver)

            yt_commands.clear_metadata_caches(driver=driver)

            if self.get_param("NUM_NODES", cluster_index) > 0:
                self._setup_nodes_dynamic_config(driver=driver)

            if self.USE_DYNAMIC_TABLES:
                self._setup_tablet_manager(driver=driver)
                self._clear_ql_pools(driver=driver)
                self._restore_default_bundle_options(driver=driver)

            yt_commands.wait_for_nodes(driver=driver)
            yt_commands.wait_for_chunk_replicator(driver=driver)

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
                        "exit_cell_tag": 1,
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

            # TODO(ifsmirnov): remove in a while.
            yt_commands.set(
                "//sys/@config/tablet_manager/enable_aggressive_tablet_statistics_validation",
                True,
            )

    def teardown_method(self, method):
        yt_commands._zombie_responses[:] = []

        for env in [self.Env] + self.remote_envs:
            env.check_liveness(callback_func=emergency_exit_within_tests)

        for cluster_index in xrange(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                continue

            if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
                self._remove_operations(driver=driver)
                self._wait_for_jobs_to_vanish(driver=driver)

            self._abort_transactions(driver=driver)

            yt_commands.remove("//tmp", driver=driver)
            if self.ENABLE_TMP_PORTAL:
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
                yt_commands.make_batch_request(abort_command, transaction_id=transaction_id)))

        yt_commands.execute_batch(requests)

    def _reset_nodes(self, driver=None):
        boolean_attributes = [
            "banned",
            "decommissioned",
            "disable_write_sessions",
            "disable_scheduler_jobs",
            "disable_tablet_cells",
        ]
        attributes = boolean_attributes + [
            "resource_limits_overrides",
            "user_tags",
        ]
        nodes = yt_commands.ls("//sys/cluster_nodes", attributes=attributes, driver=driver)

        requests = []
        for node in nodes:
            node_name = str(node)
            for attribute in boolean_attributes:
                if node.attributes[attribute]:
                    requests.append(
                        yt_commands.make_batch_request(
                            "set",
                            path="//sys/cluster_nodes/{0}/@{1}".format(node_name, attribute),
                            input=False,
                        )
                    )
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

        test_cleanup.cleanup_objects(
            list_multiple_action=list_multiple_action,
            remove_multiple_action=remove_multiple_action,
            exists_multiple_action=exists_multiple_action,
            enable_secondary_cells_cleanup=enable_secondary_cells_cleanup,
        )

    def _wait_for_scheduler_state_restored(self, driver=None):
        exec_node_count = len(yt_commands.get_exec_nodes())

        def check():
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "list",
                        path=yt_commands.scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "get",
                        path=yt_commands.scheduler_orchid_path() + "/scheduler/default_pool_tree",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "list",
                        path=yt_commands.scheduler_orchid_default_pool_tree_path() + "/pools",
                        return_only_value=True,
                    ),
                    yt_commands.make_batch_request(
                        "get",
                        path=yt_commands.scheduler_orchid_path()
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

    def _restore_globals(self, cluster_index, master_cell_roles, scheduler_count, scheduler_pool_trees_root, driver=None):
        dynamic_master_config = get_dynamic_master_config()
        dynamic_master_config = update_inplace(
            dynamic_master_config, self.get_param("DELTA_DYNAMIC_MASTER_CONFIG", cluster_index)
        )
        dynamic_master_config["multicell_manager"]["cell_roles"] = master_cell_roles
        if self.Env.get_component_version("ytserver-master").abi >= (20, 4):
            dynamic_master_config["enable_descending_sort_order"] = True
            dynamic_master_config["enable_descending_sort_order_dynamic"] = True

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
                    attributes={"name": "default", "config": {"main_resource": "cpu"}},
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

        if should_set_default_config:
            yt_commands.set(
                scheduler_pool_trees_root + "/default/@config",
                {
                    "nodes_filter": "",
                    "main_resource": "cpu",
                    "min_child_heap_size": 3,
                })

        if scheduler_count > 0:
            self._wait_for_scheduler_state_restored(driver=driver)

    def _setup_nodes_dynamic_config(self, driver=None):
        yt_commands.set("//sys/cluster_nodes/@config", get_dynamic_node_config(), driver=driver)

        now = get_current_time()

        nodes = yt_commands.ls("//sys/cluster_nodes", driver=driver)

        def check():
            responses = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "get",
                        path="//sys/cluster_nodes/{0}/orchid/dynamic_config_manager".format(node),
                        return_only_value=True,
                    )
                    for node in nodes
                ],
                driver=driver,
            )
            for response in responses:
                output = yt_commands.get_batch_output(response)
                if "last_config_change_time" not in output:
                    # COMPAT(babenko): YT Node is not recent enough.
                    sleep(1.0)
                    return True
                last_config_update_time = parse_yt_time(output["last_config_update_time"])
                if last_config_update_time < now:
                    return False
            return True

        wait(check)

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
                    path="//sys/@config/tablet_manager/enable_tablet_resource_validation",
                    input=True,
                ),
            ],
            driver=driver,
        ):
            assert not yt_commands.get_batch_error(response)

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
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "porto",
                },
            }
        }
    }


def get_custom_rootfs_delta_node_config():
    return {
        "exec_agent": {
            "do_not_set_user_id": True,
            "slot_manager": {
                "job_environment": {
                    "use_exec_from_layer": True,
                },
            },
        }
    }
