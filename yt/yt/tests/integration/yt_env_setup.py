from __future__ import print_function

import yt_commands

from yt.environment import YTInstance, arcadia_interop
from yt.environment.helpers import emergency_exit_within_tests
from yt.environment.porto_helpers import remove_all_volumes
from yt.environment.default_configs import (
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
from yt.common import makedirp, YtError, YtResponseError, format_error, update_inplace
import yt.logger

import pytest

import gc
import os
import sys
import logging
import shutil
import decorator
import functools
import subprocess

from time import sleep, time
from threading import Thread

SANDBOX_ROOTDIR = None
SANDBOX_STORAGE_ROOTDIR = None

##################################################################


def prepare_yatest_environment(need_suid, artifact_components=None):
    yt.logger.LOGGER.setLevel(logging.DEBUG)
    artifact_components = artifact_components or {}

    global SANDBOX_ROOTDIR
    global SANDBOX_STORAGE_ROOTDIR

    ytrecipe = os.environ.get("YTRECIPE") is not None

    ram_drive_path = arcadia_interop.yatest_common.get_param("ram_drive_path")
    if ram_drive_path is None:
        destination = os.path.join(arcadia_interop.yatest_common.work_path(), "build")
    else:
        destination = os.path.join(ram_drive_path, "build")

    destination = os.path.join(destination, "suid" if need_suid else "nosuid")

    if not os.path.exists(destination):
        os.makedirs(destination)
        path = arcadia_interop.prepare_yt_environment(
            destination,
            inside_arcadia=False,
            copy_ytserver_all=not ytrecipe,
            need_suid=need_suid and not ytrecipe,
            artifact_components=artifact_components,
        )
        os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])

    if ytrecipe:
        SANDBOX_ROOTDIR = arcadia_interop.yatest_common.work_path("ytrecipe_output")
    elif ram_drive_path is None:
        SANDBOX_ROOTDIR = arcadia_interop.yatest_common.output_path()
    else:
        SANDBOX_ROOTDIR = arcadia_interop.yatest_common.output_ram_drive_path()

    if ytrecipe:
        SANDBOX_STORAGE_ROOTDIR = None
    else:
        SANDBOX_STORAGE_ROOTDIR = arcadia_interop.yatest_common.output_path()


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


def is_asan_build():
    return arcadia_interop.yatest_common.context.sanitize == "address"


def is_msan_build():
    return arcadia_interop.yatest_common.context.sanitize == "memory"


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
        if self.NUM_SECONDARY_MASTER_CELLS == 0 and args[index - 1] == True:
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
    NUM_NONVOTING_MASTERS = 0
    NUM_SECONDARY_MASTER_CELLS = 0
    START_SECONDARY_MASTER_CELLS = True
    ENABLE_SECONDARY_CELLS_CLEANUP = True
    MASTER_CELL_ROLES = {}
    NUM_NODES = 5
    DEFER_NODE_START = False
    NUM_SCHEDULERS = 0
    DEFER_SCHEDULER_START = False
    NUM_CONTROLLER_AGENTS = None
    DEFER_CONTROLLER_AGENT_START = False
    ENABLE_HTTP_PROXY = False
    NUM_HTTP_PROXIES = 1
    HTTP_PROXY_PORTS = None
    ENABLE_RPC_PROXY = None
    NUM_RPC_PROXIES = 2
    DRIVER_BACKEND = "native"
    ENABLE_RPC_DRIVER_PROXY_DISCOVERY = None
    NODE_PORT_SET_SIZE = None
    ARTIFACT_COMPONENTS = {}

    DELTA_DRIVER_CONFIG = {}
    DELTA_RPC_DRIVER_CONFIG = {}
    DELTA_MASTER_CONFIG = {}
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
    def create_yt_cluster_instance(cls, index, path):
        modify_configs_func = functools.partial(cls.apply_config_patches, cluster_index=index)

        capture_stderr_to_file = True

        instance = YTInstance(
            path,
            master_count=cls.get_param("NUM_MASTERS", index),
            nonvoting_master_count=cls.get_param("NUM_NONVOTING_MASTERS", index),
            secondary_master_cell_count=cls.get_param("NUM_SECONDARY_MASTER_CELLS", index),
            clock_count=cls.get_param("NUM_CLOCKS", index),
            node_count=cls.get_param("NUM_NODES", index),
            defer_node_start=cls.get_param("DEFER_NODE_START", index),
            scheduler_count=cls.get_param("NUM_SCHEDULERS", index),
            defer_scheduler_start=cls.get_param("DEFER_SCHEDULER_START", index),
            controller_agent_count=cls.get_param("NUM_CONTROLLER_AGENTS", index),
            defer_controller_agent_start=cls.get_param("DEFER_CONTROLLER_AGENT_START", index),
            http_proxy_count=cls.get_param("NUM_HTTP_PROXIES", index)
            if cls.get_param("ENABLE_HTTP_PROXY", index)
            else 0,
            http_proxy_ports=cls.get_param("HTTP_PROXY_PORTS", index),
            rpc_proxy_count=cls.get_param("NUM_RPC_PROXIES", index) if cls.get_param("ENABLE_RPC_PROXY", index) else 0,
            watcher_config={"disable_logrotate": True},
            node_port_set_size=cls.get_param("NODE_PORT_SET_SIZE", index),
            kill_child_processes=True,
            use_porto_for_servers=cls.USE_PORTO,
            port_locks_path=os.path.join(SANDBOX_ROOTDIR, "ports"),
            fqdn="localhost",
            enable_master_cache=cls.get_param("USE_MASTER_CACHE", index),
            enable_permission_cache=cls.get_param("USE_PERMISSION_CACHE", index),
            modify_configs_func=modify_configs_func,
            cell_tag=index * 10,
            enable_rpc_driver_proxy_discovery=cls.get_param("ENABLE_RPC_DRIVER_PROXY_DISCOVERY", index),
            enable_structured_master_logging=True,
            enable_structured_scheduler_logging=True,
            capture_stderr_to_file=capture_stderr_to_file,
            stderrs_path=os.path.join(arcadia_interop.yatest_common.output_path("yt_stderrs"), cls.run_name, str(index)),
            validate_component_abi=False,
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

        need_suid = False
        cls.cleanup_root_files = False
        if cls.USE_PORTO:
            if arcadia_interop.is_inside_distbuild():
                pytest.skip("Porto is not available inside distbuild")

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
        prepare_yatest_environment(need_suid=need_suid, artifact_components=cls.ARTIFACT_COMPONENTS)
        cls.path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        cls.run_id = None
        cls.path_to_run = cls.path_to_test

        cls.run_name = os.path.basename(cls.path_to_run)

        if os.environ.get("YTRECIPE") is None:
            if SANDBOX_STORAGE_ROOTDIR is not None:
                disk_path = SANDBOX_STORAGE_ROOTDIR
            else:
                disk_path = SANDBOX_ROOTDIR
        else:
            disk_path = arcadia_interop.yatest_common.work_path("ytrecipe_hdd")

        cls.default_disk_path = os.path.join(disk_path, cls.run_name, "disk_default")
        cls.ssd_disk_path = os.path.join(disk_path, cls.run_name, "disk_ssd")

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

        cls.Env.start(
            start_secondary_master_cells=cls.START_SECONDARY_MASTER_CELLS,
            on_masters_started_func=cls.on_masters_started,
        )
        for index, env in enumerate(cls.remote_envs):
            env.start(start_secondary_master_cells=cls.get_param("START_SECONDARY_MASTER_CELLS", index))

        yt_commands.wait_drivers()

        for env in [cls.Env] + cls.remote_envs:
            # To avoid strange hangups.
            if env.master_count > 0:
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
                    "transaction_manager": instance.configs["master"][0]["transaction_manager"],
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

        if yt_commands.is_multicell and cls.START_SECONDARY_MASTER_CELLS:
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

        if not os.path.exists(cls.path_to_run):
            return

        if SANDBOX_STORAGE_ROOTDIR is not None:
            makedirp(SANDBOX_STORAGE_ROOTDIR)

            if cls.cleanup_root_files:
                # XXX(psushin): unlink all Porto volumes.
                remove_all_volumes(cls.path_to_run)

                # XXX(asaitgalin): Unmount everything.
                # TODO(prime@): remove this, since we are using Porto everywhere
                subprocess.check_call(
                    [
                        "sudo",
                        "find",
                        cls.path_to_run,
                        "-type",
                        "d",
                        "-exec",
                        "mountpoint",
                        "-q",
                        "{}",
                        ";",
                        "-exec",
                        "sudo",
                        "umount",
                        "{}",
                        ";",
                    ]
                )

                # XXX(asaitgalin): Ensure tests running user has enough permissions to manipulate YT sandbox.
                chown_command = [
                    "sudo",
                    "chown",
                    "-R",
                    "{0}:{1}".format(os.getuid(), os.getgid()),
                    cls.path_to_run,
                ]

                p = subprocess.Popen(chown_command, stderr=subprocess.PIPE)
                _, stderr = p.communicate()
                if p.returncode != 0:
                    print(stderr, file=sys.stderr)
                    raise subprocess.CalledProcessError(p.returncode, " ".join(chown_command))

                # XXX(psushin): Porto volume directories may have weirdest permissions ever.
                chmod_command = ["chmod", "-R", "+rw", cls.path_to_run]

                p = subprocess.Popen(chmod_command, stderr=subprocess.PIPE)
                _, stderr = p.communicate()
                if p.returncode != 0:
                    print(stderr, file=sys.stderr)
                    raise subprocess.CalledProcessError(p.returncode, " ".join(chmod_command))

                # XXX(dcherednik): Delete named pipes.
                # TODO(prime@): remove this garbage
                subprocess.check_call(["find", cls.path_to_run, "-type", "p", "-delete"])

            if SANDBOX_ROOTDIR != SANDBOX_STORAGE_ROOTDIR:
                destination_path = os.path.join(SANDBOX_STORAGE_ROOTDIR, cls.test_name)
                if cls.run_id:
                    destination_path = os.path.join(destination_path, cls.run_id)
                if os.path.exists(destination_path):
                    shutil.rmtree(destination_path, ignore_errors=True)

                print("Moving test artifacts from", cls.path_to_run, "to", destination_path, file=sys.stderr)
                shutil.move(cls.path_to_run, destination_path)
                print("Move completed", file=sys.stderr)

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
        command_name = "abort_transaction" if driver.get_config()["api_version"] == 4 else "abort_tx"
        requests = []
        for tx in yt_commands.ls("//sys/transactions", attributes=["title"], driver=driver):
            title = tx.attributes.get("title", "")
            id = str(tx)
            if "Scheduler lock" in title:
                continue
            if "Controller agent incarnation" in title:
                continue
            if "Lease for node" in title:
                continue
            if "World initialization" in title:
                continue
            requests.append(yt_commands.make_batch_request(command_name, transaction_id=id))
        yt_commands.execute_batch(requests, driver=driver)

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
        TYPES = [
            "accounts",
            "users",
            "groups",
            "racks",
            "data_centers",
            "tablet_cells",
            "tablet_cell_bundles",
            "network_projects",
        ]

        if enable_secondary_cells_cleanup:
            TYPES = TYPES + ["tablet_actions"]

        list_objects_results = yt_commands.execute_batch(
            [
                yt_commands.make_batch_request(
                    "list",
                    return_only_value=True,
                    path="//sys/" + ("account_tree" if type == "accounts" else type),
                    attributes=["id", "builtin", "life_stage"],
                )
                for type in TYPES
            ],
            driver=driver,
        )

        object_ids_to_remove = []
        object_ids_to_check = []
        for index, type in enumerate(TYPES):
            objects = yt_commands.get_batch_output(list_objects_results[index])
            for object in objects:
                if object.attributes["builtin"]:
                    continue
                if type == "users" and str(object) == "application_operations":
                    continue
                id = object.attributes["id"]
                object_ids_to_check.append(id)
                life_stage = object.attributes["life_stage"]
                if life_stage == "creation_committed" or life_stage == "creation_pre_committed":
                    object_ids_to_remove.append(id)

        def do():
            yt_commands.gc_collect(driver=driver)

            yt_commands.execute_batch(
                [yt_commands.make_batch_request("remove", path="#" + id, force=True) for id in object_ids_to_remove],
                driver=driver,
            )

            results = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request("exists", path="#" + id, return_only_value=True)
                    for id in object_ids_to_check
                ],
                driver=driver,
            )
            return all(not yt_commands.get_batch_output(result)["value"] for result in results)

        wait(do)

    def _wait_for_scheduler_state_restored(self, driver=None):
        node_count = len(yt_commands.ls("//sys/cluster_nodes", driver=driver))

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
                and yt_commands.get_batch_output(responses[3]) == node_count
            )

        wait(check)

    def _restore_globals(self, master_cell_roles, scheduler_count, scheduler_pool_trees_root, driver=None):
        dynamic_master_config = get_dynamic_master_config()
        dynamic_master_config["multicell_manager"]["cell_roles"] = master_cell_roles

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
        if yt_commands.exists(scheduler_pool_trees_root + "/default", driver=driver):

            # COMPAT(max42): drop this when 20.2 is deprecated.
            is_pool_tree_config_present = self.Env.get_component_version("ytserver-master").abi >= (20, 3)

            restore_pool_trees_requests.append(
                yt_commands.make_batch_request("remove", path=scheduler_pool_trees_root + "/default/*"))

            # TODO(eshcherbin): Clear default tree's config when it is moved to a separate attribute.
            if is_pool_tree_config_present:
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request("set", path=scheduler_pool_trees_root + "/default/@config/nodes_filter", input=""))
            else:
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request("set", path=scheduler_pool_trees_root + "/default/@nodes_filter", input=""))
        else:
            # XXX(eshcherbin, renadeen): Remove when map_node pool trees are not a thing.
            if yt_commands.get(scheduler_pool_trees_root + "/@type") == "map_node":
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request(
                        "create",
                        type="map_node",
                        path=scheduler_pool_trees_root + "/default",
                    )
                )
            else:
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request(
                        "create",
                        type="scheduler_pool_tree",
                        attributes={"name": "default"},
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

        if scheduler_count > 0:
            self._wait_for_scheduler_state_restored(driver=driver)

    def _setup_nodes_dynamic_config(self, driver=None):
        dynamic_node_config = get_dynamic_node_config()
        yt_commands.set("//sys/cluster_nodes/@config", dynamic_node_config, driver=driver)

        nodes = yt_commands.ls("//sys/cluster_nodes", driver=driver)

        def check():
            # COMPAT(max42): request only applied_config when 20.2 is no more.
            results_old = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "get",
                        path="//sys/cluster_nodes/{0}/orchid/dynamic_config_manager/config".format(node),
                        return_only_value=True,
                    )
                    for node in nodes
                ],
                driver=driver,
            )
            results_new = yt_commands.execute_batch(
                [
                    yt_commands.make_batch_request(
                        "get",
                        path="//sys/cluster_nodes/{0}/orchid/dynamic_config_manager/applied_config".format(node),
                        return_only_value=True,
                    )
                    for node in nodes
                ],
                driver=driver,
            )

            for result_old, result_new in zip(results_old, results_new):
                if "error" in result_old and "error" in result_new:
                    raise YtError("Both old and new paths for dynamic config manager returned errors:\n" +
                                  str(result_old["error"]) + "\n" + str(result_new["error"]))
                if "error" not in result_old and yt_commands.get_batch_output(result_old) != dynamic_node_config["%true"]:
                    return False
                if "error" not in result_new and yt_commands.get_batch_output(result_new) != dynamic_node_config["%true"]:
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
        command_name = "abort_operation" if driver.get_config()["api_version"] == 4 else "abort_op"

        if yt_commands.get("//sys/scheduler/instances/@count", driver=driver) == 0:
            return

        operations_from_orchid = []
        try:
            operations_from_orchid = yt_commands.ls("//sys/scheduler/orchid/scheduler/operations", driver=driver)
        except YtError as err:
            print(format_error(err), file=sys.stderr)

        requests = []
        for operation_id in operations_from_orchid:
            if not operation_id.startswith("*"):
                requests.append(yt_commands.make_batch_request(command_name, operation_id=operation_id))

        responses = yt_commands.execute_batch(requests, driver=driver)
        for response in responses:
            err = yt_commands.get_batch_error(response)
            if err is not None:
                print(format_error(err), file=sys.stderr)

        self._abort_transactions(driver=driver)

        for response in yt_commands.execute_batch(
            [
                yt_commands.make_batch_request("remove", path="//sys/operations/*"),
                yt_commands.make_batch_request("remove", path="//sys/operations_archive", force=True),
            ],
            driver=driver,
        ):
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
