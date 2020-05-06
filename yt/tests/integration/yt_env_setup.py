import yt_commands

from yt.environment import YTInstance, arcadia_interop
from yt.environment.helpers import emergency_exit_within_tests
from yt.environment.porto_helpers import porto_avaliable, remove_all_volumes
from yt.environment.default_configs import get_dynamic_master_config, get_dynamic_node_config

try:
    from yt.environment.helpers import (
        Restarter,
        SCHEDULERS_SERVICE,
        CONTROLLER_AGENTS_SERVICE,
        NODES_SERVICE,
        MASTERS_SERVICE,
    )
except ImportError:
    # Let's hope we won't need Restarter :(
    pass

from yt.test_helpers import wait, WaitFailed
from yt.common import makedirp, YtError, YtResponseError, format_error, update_inplace
import yt.logger

import pytest

import gc
import os
import sys
import glob
import logging
import resource
import shutil
import decorator
import functools
import inspect
import stat
import subprocess
import uuid
from distutils.spawn import find_executable
from time import sleep, time
from threading import Thread

if arcadia_interop.yatest_common is None:
    SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath("tests.sandbox"))
    SANDBOX_STORAGE_ROOTDIR = os.environ.get("TESTS_SANDBOX_STORAGE")
else:
    SANDBOX_ROOTDIR = None
    SANDBOX_STORAGE_ROOTDIR = None

##################################################################

def prepare_yatest_environment(need_suid):
    if arcadia_interop.yatest_common is None:
        return

    yt.logger.LOGGER.setLevel(logging.DEBUG)

    global SANDBOX_ROOTDIR
    global SANDBOX_STORAGE_ROOTDIR
    if arcadia_interop.yatest_common.get_param("teamcity"):
        SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath("tests.sandbox"))
        SANDBOX_STORAGE_ROOTDIR = os.environ.get("TESTS_SANDBOX_STORAGE")
        return

    ram_drive_path = arcadia_interop.yatest_common.get_param("ram_drive_path")
    if ram_drive_path is None:
        destination = os.path.join(arcadia_interop.yatest_common.work_path(), "build")
    else:
        destination = os.path.join(ram_drive_path, "build")
    if not os.path.exists(destination):
        os.makedirs(destination)
        path = arcadia_interop.prepare_yt_environment(destination, inside_arcadia=False, use_ytserver_all=True, copy_ytserver_all=True, need_suid=need_suid)
        os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])

    if ram_drive_path is None:
        SANDBOX_ROOTDIR = arcadia_interop.yatest_common.work_path()
    else:
        SANDBOX_ROOTDIR = arcadia_interop.yatest_common.output_ram_drive_path()

    SANDBOX_STORAGE_ROOTDIR = arcadia_interop.yatest_common.output_path()

def _abort_transactions(driver=None):
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
        requests.append(yt_commands.make_batch_request(command_name, transaction_id=id))
    yt_commands.execute_batch(requests, driver=driver)

def _reset_nodes(driver=None):
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
                requests.append(yt_commands.make_batch_request("set", path="//sys/cluster_nodes/{0}/@{1}".format(node_name, attribute), input=False))
        if node.attributes["resource_limits_overrides"] != {}:
            requests.append(yt_commands.make_batch_request("set", path="//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node_name), input={}))
        if node.attributes["user_tags"] != []:
            requests.append(yt_commands.make_batch_request("set", path="//sys/cluster_nodes/{0}/@user_tags".format(node_name), input=[]))

    responses = yt_commands.execute_batch(requests, driver=driver)
    for response in responses:
        assert not yt_commands.get_batch_output(response)

def _retry_with_gc_collect(func, driver=None):
    while True:
        try:
            func()
            break
        except YtResponseError:
            yt_commands.gc_collect(driver=driver)

def _remove_objects(enable_secondary_cells_cleanup, driver=None):
    TYPES = [
        "accounts",
        "users",
        "groups",
        "racks",
        "data_centers",
        "tablet_cells",
        "tablet_cell_bundles",
    ]

    if enable_secondary_cells_cleanup:
        TYPES = TYPES + [
            "tablet_actions"
        ]

    def do():
        list_objects_results = yt_commands.execute_batch([
            yt_commands.make_batch_request("list", return_only_value=True,
                path="//sys/" + ("account_tree" if type == "accounts" else type),
                attributes=["id", "builtin", "life_stage"]) for type in TYPES],
                driver=driver)

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
                if object.attributes["life_stage"] == "creation_committed":
                    object_ids_to_remove.append(id)

        for result in yt_commands.execute_batch([
                yt_commands.make_batch_request("remove", path="#" + id, force=True) for id in object_ids_to_remove
            ], driver=driver):
            assert not yt_commands.get_batch_output(result)

        def check_removed():
            results = yt_commands.execute_batch([
                yt_commands.make_batch_request("exists", path="#" + id) for id in object_ids_to_check
            ], driver=driver)
            return all(not yt_commands.get_batch_output(result)["value"] for result in results)
        wait(check_removed)

    _retry_with_gc_collect(do, driver=driver)

def _restore_globals(scheduler_count, scheduler_pool_trees_root, driver=None):
    def do():
        for response in yt_commands.execute_batch([
                yt_commands.make_batch_request("set", path="//sys/tablet_cell_bundles/default/@dynamic_options", input={}),
                yt_commands.make_batch_request("set", path="//sys/tablet_cell_bundles/default/@tablet_balancer_config", input={}),
                yt_commands.make_batch_request("set", path="//sys/@config", input=get_dynamic_master_config()),
            ], driver=driver):
            assert not yt_commands.get_batch_output(response)

        default_exists = yt_commands.exists(scheduler_pool_trees_root + "/default", driver=driver)
        restore_pool_trees_requests = []
        if default_exists:
            restore_pool_trees_requests.extend([
                yt_commands.make_batch_request("remove", path=scheduler_pool_trees_root + "/default/*"),
                yt_commands.make_batch_request("set", path=scheduler_pool_trees_root + "/@default_tree", input="default"),
                yt_commands.make_batch_request("set", path=scheduler_pool_trees_root + "/default/@nodes_filter", input=""),
            ])
        else:
            # XXX(eshcherbin, renadeen): Remove when map_node pool trees are not a thing.
            if yt_commands.get(scheduler_pool_trees_root + "/@type") == "map_node":
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request("create", type="map_node", path=scheduler_pool_trees_root + "/default"))
            else :
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request("create", type="scheduler_pool_tree", attributes={"name": "default"}))
        for pool_tree in yt_commands.ls(scheduler_pool_trees_root):
            if pool_tree != "default":
                restore_pool_trees_requests.append(
                    yt_commands.make_batch_request("remove", path=scheduler_pool_trees_root + "/" + pool_tree))
        for response in yt_commands.execute_batch(restore_pool_trees_requests, driver=driver):
            assert not yt_commands.get_batch_error(response)
        if default_exists:
            # Could not add this to the batch request because of possible races at scheduler.
            yt_commands.set(scheduler_pool_trees_root + "/@default_tree", "default", driver=driver)

        if scheduler_count > 0:
            wait(lambda: yt_commands.ls(yt_commands.scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree", driver=driver) == ["default"])
            wait(lambda: yt_commands.get(yt_commands.scheduler_orchid_path() + "/scheduler/default_pool_tree", driver=driver) == "default")
            wait(lambda: yt_commands.ls(yt_commands.scheduler_orchid_default_pool_tree_path() + "/pools", driver=driver) == ["<Root>"])
            node_count = len(yt_commands.ls("//sys/cluster_nodes", driver=driver))
            wait(lambda: yt_commands.get(yt_commands.scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/default/node_count", driver=driver) == node_count)

    _retry_with_gc_collect(do, driver=driver)

def _restore_default_bundle_options(driver=None):
    def do():
        for response in yt_commands.execute_batch([
                yt_commands.make_batch_request("set", path="//sys/tablet_cell_bundles/default/@options", input={
                    "changelog_account": "sys",
                    "snapshot_account": "sys"
                }),
            ], driver=driver):
            assert not yt_commands.get_batch_output(response)

    _retry_with_gc_collect(do, driver=driver)

def _remove_operations(driver=None):
    command_name = "abort_operation" if driver.get_config()["api_version"] == 4 else "abort_op"

    if yt_commands.get("//sys/scheduler/instances/@count", driver=driver) == 0:
        return

    operations_from_orchid = []
    try:
        operations_from_orchid = yt_commands.ls("//sys/scheduler/orchid/scheduler/operations", driver=driver)
    except YtError as err:
        print >>sys.stderr, format_error(err)

    requests = []
    for operation_id in operations_from_orchid:
        if not operation_id.startswith("*"):
            requests.append(yt_commands.make_batch_request(command_name, operation_id=operation_id))

    responses = yt_commands.execute_batch(requests, driver=driver)
    for response in responses:
        err = yt_commands.get_batch_error(response)
        if err is not None:
            print >>sys.stderr, format_error(err)

    _abort_transactions(driver=driver)

    for response in yt_commands.execute_batch([
            yt_commands.make_batch_request("remove", path="//sys/operations/*"),
            yt_commands.make_batch_request("remove", path="//sys/operations_archive", force=True)
        ], driver=driver):
        assert not yt_commands.get_batch_output(response)

def _wait_for_jobs_to_vanish(driver=None):
    nodes = yt_commands.ls("//sys/cluster_nodes", driver=driver)
    def check_no_jobs():
        requests = [yt_commands.make_batch_request("get", path="//sys/cluster_nodes/{0}/orchid/job_controller/active_job_count".format(node), return_only_value=True)
                    for node in nodes]
        responses = yt_commands.execute_batch(requests, driver=driver)
        return all(yt_commands.get_batch_output(response).get("scheduler", 0) == 0 for response in responses)
    try:
        wait(check_no_jobs, iter=300)
    except WaitFailed:
        requests = [yt_commands.make_batch_request("list", path="//sys/cluster_nodes/{0}/orchid/job_controller/active_jobs/scheduler".format(node), return_only_value=True)
                    for node in nodes]
        responses = yt_commands.execute_batch(requests, driver=driver)
        print >>sys.stderr, "There are remaining scheduler jobs:"
        for node, response in zip(nodes, responses):
            print >>sys.stderr, "Node {}: {}".format(node, response)

def find_ut_file(file_name):
    if arcadia_interop.yatest_common is not None:
        import library.python.resource as rs
        with open(file_name, 'wb') as bc:
            bc_content = rs.find("/llvm_bc/" + file_name.split(".")[0])
            bc.write(bc_content)
        return file_name

    unittester_path = find_executable("unittester-ytlib")
    assert unittester_path is not None
    for unittests_path in [
        os.path.join(os.path.dirname(unittester_path), "..", "yt", "ytlib", "query_client", "ut"),
        os.path.dirname(unittester_path)
    ]:
        result_path = os.path.join(unittests_path, file_name)
        if os.path.exists(result_path):
            return result_path
    else:
        raise RuntimeError("Cannot find '{0}'".format(file_name))

##################################################################

def patch_subclass(parent, skip_condition, reason=""):
    """Work around a pytest.mark.skipif bug
    https://github.com/pytest-dev/pytest/issues/568
    The issue causes all subclasses of a TestCase subclass to be skipped if any one
    of them is skipped.
    This fix circumvents the issue by overriding Python's existing subclassing mechanism.
    Instead of having `cls` be a subclass of `parent`, this decorator adds each attribute
    of `parent` to `cls` without using Python inheritance. When appropriate, it also adds
    a boolean condition under which to skip tests for the decorated class.
    :param parent: The "superclass" from which the decorated class should inherit
        its non-overridden attributes
    :type parent: class
    :param skip_condition: A boolean condition that, when True, will cause all tests in
        the decorated class to be skipped
    :type skip_condition: bool
    :param reason: reason for skip.
    :type reason: str
    """
    def patcher(cls):
        def build_skipped_method(method, cls, skip_condition, reason):
            if hasattr(method, "skip_condition"):
                skip_condition = skip_condition or method.skip_condition(cls)

            argspec = inspect.getargspec(method)
            formatted_args = inspect.formatargspec(*argspec)

            function_code = "@pytest.mark.skipif(skip_condition, reason=reason)\n"\
                            "def _wrapper({0}):\n"\
                            "    return method({0})\n"\
                                .format(formatted_args.lstrip('(').rstrip(')'))
            exec function_code in locals(), globals()

            return _wrapper

        # two passes required so that skips have access to all class attributes
        for attr in parent.__dict__:
            if attr in cls.__dict__:
                continue
            if attr.startswith("__"):
                continue
            if not attr.startswith("test_"):
                setattr(cls, attr, parent.__dict__[attr])

        for attr in parent.__dict__:
            if attr.startswith("test_"):
                setattr(cls, attr, build_skipped_method(parent.__dict__[attr],
                                                        cls, skip_condition, reason))
                for key in parent.__dict__[attr].__dict__:
                    if key == "parametrize" or "flaky" in key or "skip" in key or key == "authors":
                        cls.__dict__[attr].__dict__[key] = parent.__dict__[attr].__dict__[key]

                    if key == "pytestmark":
                        if key in cls.__dict__[attr].__dict__:
                            cls.__dict__[attr].__dict__[key] += parent.__dict__[attr].__dict__[key]
                        else:
                            cls.__dict__[attr].__dict__[key] = parent.__dict__[attr].__dict__[key]

        return cls

    return patcher

##################################################################

linux_only = pytest.mark.skipif('not sys.platform.startswith("linux")')
unix_only = pytest.mark.skipif('not sys.platform.startswith("linux") and not sys.platform.startswith("darwin")')

patch_porto_env_only = lambda parent: patch_subclass(parent, not porto_avaliable(), reason="you need configured porto to run it")

def skip_if_porto(func):
    def wrapper(func, self, *args, **kwargs):
        if hasattr(self, "USE_PORTO_FOR_SERVERS") and self.USE_PORTO_FOR_SERVERS:
            pytest.skip("This test does not support porto isolation")
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)

def is_asan_build():
    if arcadia_interop.yatest_common is not None:
        return arcadia_interop.yatest_common.context.sanitize == "address"

    binary = find_executable("ytserver-master")
    version = subprocess.check_output([binary, "--version"])
    return "asan" in version

def is_gcc_build():
    if arcadia_interop.yatest_common is not None:
        return arcadia_interop.yatest_common.c_compiler_path().endswith("gcc")

        return False

    binary = find_executable("ytserver-clickhouse")
    svnrevision = subprocess.check_output([binary, "--svnrevision"])
    return "GCC" in svnrevision

def check_root_privileges():
    if arcadia_interop.is_inside_distbuild():
        pytest.skip("root is not available inside distbuild")

    for binary in ["ytserver-exec", "ytserver-job-proxy", "ytserver-node",
                   "ytserver-tools"]:
        binary_path = find_executable(binary)
        if binary_path is None:
            pytest.fail('Executable {} is not found in PATH'.format(binary))
        binary_stat = os.stat(binary_path)
        if (binary_stat.st_mode & stat.S_ISUID) == 0:
            pytest.fail('This test requires a suid bit set for "{}"'.format(binary))
        if binary_stat.st_uid != 0:
            pytest.fail('This test requires "{}" being owned by root'.format(binary))

# doesn't work with @patch_porto_env_only on the same class, wrap each method
def require_ytserver_root_privileges(func):
    def wrap_func(self, *args, **kwargs):
        check_root_privileges()
        func(self, *args, **kwargs)

    return wrap_func


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

    return pytest.mark.parametrize("external", [False, True])(
        decorator.decorate(func, wrapper))

def require_enabled_core_dump(func):
    def wrapper(func, self, *args, **kwargs):
        rlimit_core = resource.getrlimit(resource.RLIMIT_CORE)
        if rlimit_core[0] == 0:
            pytest.skip('This test requires enabled core dump (how about "ulimit -c unlimited"?)')
        return func(self, *args, **kwargs)

    return decorator.decorate(func, wrapper)

def resolve_test_paths(name):
    path_to_sandbox = os.path.join(SANDBOX_ROOTDIR, name)
    path_to_environment = os.path.join(path_to_sandbox, "run")
    return path_to_sandbox, path_to_environment

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
                print >>sys.stderr, "Check took %lf seconds" % delta
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
    NODE_PORT_SET_SIZE = None

    DELTA_DRIVER_CONFIG = {}
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

    USE_PORTO_FOR_SERVERS = False
    USE_DYNAMIC_TABLES = False
    USE_MASTER_CACHE = False
    USE_PERMISSION_CACHE = True
    ENABLE_BULK_INSERT = False
    ENABLE_TMP_PORTAL = False
    ENABLE_TABLET_BALANCER = False

    REQUIRE_YTSERVER_ROOT_PRIVILEGES = False
    REQUIRE_SUID_TOOL = False

    NUM_REMOTE_CLUSTERS = 0

    # To be redefined in successors
    @classmethod
    def modify_master_config(cls, config, index):
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
        if cluster_index == 0:
            return value

        param_name = "{0}_REMOTE_{1}".format(name, cluster_index - 1)
        if hasattr(cls, param_name):
            return getattr(cls, param_name)

        return value

    @classmethod
    def create_yt_cluster_instance(cls, index, path):
        modify_configs_func = functools.partial(
            cls.apply_config_patches,
            cluster_index=index)

        if arcadia_interop.yatest_common is None:
            capture_stderr_to_file = bool(int(os.environ.get("YT_CAPTURE_STDERR_TO_FILE", "0")))
        else:
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
            http_proxy_count=cls.get_param("NUM_HTTP_PROXIES", index) if cls.get_param("ENABLE_HTTP_PROXY", index) else 0,
            http_proxy_ports=cls.get_param("HTTP_PROXY_PORTS", index),
            rpc_proxy_count=cls.get_param("NUM_RPC_PROXIES", index) if cls.get_param("ENABLE_RPC_PROXY", index) else 0,
            watcher_config={"disable_logrotate": True},
            node_port_set_size=cls.get_param("NODE_PORT_SET_SIZE", index),
            kill_child_processes=True,
            use_porto_for_servers=cls.get_param("USE_PORTO_FOR_SERVERS", index),
            port_locks_path=os.path.join(SANDBOX_ROOTDIR, "ports"),
            fqdn="localhost",
            enable_master_cache=cls.get_param("USE_MASTER_CACHE", index),
            enable_permission_cache=cls.get_param("USE_PERMISSION_CACHE", index),
            modify_configs_func=modify_configs_func,
            cell_tag=index * 10,
            driver_backend=cls.get_param("DRIVER_BACKEND", index),
            enable_structured_master_logging=True,
            enable_structured_scheduler_logging=True,
            capture_stderr_to_file=capture_stderr_to_file)

        instance._cluster_name = cls.get_cluster_name(index)

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
        if cls.get_param("REQUIRE_YTSERVER_ROOT_PRIVILEGES", False):
            need_suid = True
            check_root_privileges()

        if cls.get_param("USE_PORTO_FOR_SERVERS", False):
            need_suid = True
            if arcadia_interop.is_inside_distbuild():
                pytest.skip("porto is not available on distbuild")

        if cls.get_param("REQUIRE_SUID_TOOL", False):
            need_suid = True
            if arcadia_interop.is_inside_distbuild():
                pytest.skip("SUID ytserver-tool is not available on distbuild")

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

        prepare_yatest_environment(need_suid=need_suid) # It initializes SANDBOX_ROOTDIR
        cls.path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        # For running in parallel
        if arcadia_interop.yatest_common is None:
            cls.run_id = "run_" + uuid.uuid4().hex[:8] if not run_id else run_id
            cls.path_to_run = os.path.join(cls.path_to_test, cls.run_id)
        else:
            cls.run_id = None
            cls.path_to_run = cls.path_to_test

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

        yt_commands.is_multicell = cls.NUM_SECONDARY_MASTER_CELLS > 0
        yt_commands.path_to_run_tests = cls.path_to_run
        yt_commands.init_drivers([cls.Env] + cls.remote_envs)

        cls.Env.start(start_secondary_master_cells=cls.START_SECONDARY_MASTER_CELLS, on_masters_started_func=cls.on_masters_started)
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
                    "cluster_directory_synchronizer": instance.configs["driver"]["cluster_directory_synchronizer"]
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
                yt_commands.set("//sys/accounts/tmp/@resource_limits/tablet_count", 10000, driver=driver)
                yt_commands.set("//sys/accounts/tmp/@resource_limits/tablet_static_memory", 1024 * 1024 * 1024, driver=driver)

    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index):
        for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
            for index, config in enumerate(configs["master"][tag]):
                configs["master"][tag][index] = update_inplace(config, cls.get_param("DELTA_MASTER_CONFIG", cluster_index))
                cls.modify_master_config(configs["master"][tag][index], index)
        for index, config in enumerate(configs["scheduler"]):
            configs["scheduler"][index] = update_inplace(config, cls.get_param("DELTA_SCHEDULER_CONFIG", cluster_index))
            cls.modify_scheduler_config(configs["scheduler"][index])
        for index, config in enumerate(configs["controller_agent"]):
            delta_config = cls.get_param("DELTA_CONTROLLER_AGENT_CONFIG", cluster_index)
            configs["controller_agent"][index] = update_inplace(
                update_inplace(config, YTEnvSetup._DEFAULT_DELTA_CONTROLLER_AGENT_CONFIG),
                delta_config)

            cls.modify_controller_agent_config(configs["controller_agent"][index])
        for index, config in enumerate(configs["node"]):
            configs["node"][index] = update_inplace(config, cls.get_param("DELTA_NODE_CONFIG", cluster_index))
            cls.modify_node_config(configs["node"][index])

        for index, config in enumerate(configs["http_proxy"]):
            configs["http_proxy"][index] = update_inplace(config, cls.get_param("DELTA_PROXY_CONFIG", cluster_index))
            cls.modify_proxy_config(configs["http_proxy"])

        for index, config in enumerate(configs["rpc_proxy"]):
            configs["rpc_proxy"][index] = update_inplace(config, cls.get_param("DELTA_RPC_PROXY_CONFIG", cluster_index))
            cls.modify_rpc_proxy_config(configs["rpc_proxy"])

        for key, config in configs["driver"].iteritems():
            configs["driver"][key] = update_inplace(config, cls.get_param("DELTA_DRIVER_CONFIG", cluster_index))

    @classmethod
    def teardown_class(cls):
        if cls.liveness_checkers:
            map(lambda c: c.stop(), cls.liveness_checkers)

        for env in [cls.Env] + cls.remote_envs:
            if env is None:
                continue
            env.stop()
            env.kill_cgroups()

        yt_commands.terminate_drivers()
        gc.collect()

        if not os.path.exists(cls.path_to_run):
            return

        if SANDBOX_STORAGE_ROOTDIR is not None:
            makedirp(SANDBOX_STORAGE_ROOTDIR)

            if not arcadia_interop.is_inside_distbuild():
                if cls.get_param("USE_PORTO_FOR_SERVERS", False):
                    # XXX(psushin): unlink all porto volumes.
                    remove_all_volumes(cls.path_to_run)

                # XXX(asaitgalin): Unmount everything.
                subprocess.check_call(["sudo", "find", cls.path_to_run, "-type", "d", "-exec",
                                       "mountpoint", "-q", "{}", ";", "-exec", "sudo",
                                       "umount", "{}", ";"])

                # XXX(asaitgalin): Ensure tests running user has enough permissions to manipulate YT sandbox.
                chown_command = ["sudo", "chown", "-R", "{0}:{1}".format(os.getuid(), os.getgid()), cls.path_to_run]

                p = subprocess.Popen(chown_command, stderr=subprocess.PIPE)
                _, stderr = p.communicate()
                if p.returncode != 0:
                    print >>sys.stderr, stderr
                    raise subprocess.CalledProcessError(p.returncode, " ".join(chown_command))

                # XXX(psushin): porto volume directories may have weirdest permissions ever.
                chmod_command = ["chmod", "-R", "+rw", cls.path_to_run]

                p = subprocess.Popen(chmod_command, stderr=subprocess.PIPE)
                _, stderr = p.communicate()
                if p.returncode != 0:
                    print >>sys.stderr, stderr
                    raise subprocess.CalledProcessError(p.returncode, " ".join(chmod_command))

                # XXX(dcherednik): Delete named pipes.
                subprocess.check_call(["find", cls.path_to_run, "-type", "p", "-delete"])

            destination_path = os.path.join(SANDBOX_STORAGE_ROOTDIR, cls.test_name)
            if cls.run_id:
                destination_path = os.path.join(destination_path, cls.run_id)
            if os.path.exists(destination_path):
                shutil.rmtree(destination_path, ignore_errors=True)

            runtime_data = [os.path.join(cls.path_to_run, "runtime_data")] + glob.glob(cls.path_to_run + "/*/runtime_data")
            for dir in runtime_data:
                if os.path.exists(dir):
                    shutil.rmtree(dir, ignore_errors=True)

            print >>sys.stderr, "Moving test artifacts from", cls.path_to_run, "to", destination_path
            shutil.move(cls.path_to_run, destination_path)
            print >>sys.stderr, "Move completed"

    def setup_method(self, method):
        for cluster_index in xrange(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                continue

            if self.get_param("NUM_NODES", cluster_index) > 0:
                dynamic_node_config = get_dynamic_node_config()
                yt_commands.set("//sys/cluster_nodes/@config", dynamic_node_config)
                for node in yt_commands.ls("//sys/cluster_nodes"):
                    wait(lambda: yt_commands.get_applied_node_dynamic_config(node) == dynamic_node_config["%true"])

            if self.USE_DYNAMIC_TABLES:
                yt_commands.set("//sys/@config/tablet_manager/tablet_balancer/tablet_balancer_schedule", "1", driver=driver)
                yt_commands.set("//sys/@config/tablet_manager/tablet_cell_balancer/enable_verbose_logging", True, driver=driver)
                yt_commands.set(
                    "//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer",
                    self.ENABLE_TABLET_BALANCER,
                    driver=driver)

            if self.ENABLE_BULK_INSERT:
                yt_commands.set("//sys/@config/tablet_manager/enable_bulk_insert", True, driver=driver)

            yt_commands.wait_for_nodes(driver=driver)
            yt_commands.wait_for_chunk_replicator(driver=driver)

            if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
                yt_commands.create("document", "//sys/controller_agents/config", attributes={"value": {}}, force=True, driver=driver)
                yt_commands.create("document", "//sys/scheduler/config", attributes={"value": {}}, force=True, driver=driver)

                if self.ENABLE_BULK_INSERT:
                    yt_commands.set("//sys/controller_agents/config/enable_bulk_insert_for_everyone", True)

                def _get_config_versions(orchids):
                    requests = [yt_commands.make_batch_request("get", path=orchid + "/config_revision") for orchid in orchids]
                    res = yt_commands.execute_batch(requests, driver=driver)
                    return list(map(lambda r: yt_commands.get_batch_output(r)["value"], res))

                def _wait_for_configs(orchids):
                    old_versions = _get_config_versions(orchids)

                    def _wait_func():
                        new_versions = _get_config_versions(orchids)
                        return all(new >= old+2 for old, new in zip(old_versions, new_versions))

                    wait(_wait_func)

                orchids = []
                for instance in yt_commands.ls("//sys/controller_agents/instances"):
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
                        "acl": [{"action": "allow", "permissions": ["read", "write", "remove"], "subjects": ["users"]},
                                {"action": "allow", "permissions": ["read"], "subjects": ["everyone"]}],
                    },
                    force=True,
                    driver=driver)
            else:
                yt_commands.create(
                    "map_node",
                    "//tmp",
                    attributes={
                        "account": "tmp",
                        "acl": [{"action": "allow", "permissions": ["read", "write", "remove"], "subjects": ["users"]}],
                        "opaque": True
                    },
                    force=True,
                    driver=driver)
            yt_commands.reset_events_on_fs()

    def teardown_method(self, method):
        yt_commands._zombie_responses[:] = []

        for env in [self.Env] + self.remote_envs:
            env.check_liveness(callback_func=emergency_exit_within_tests)

        for cluster_index in xrange(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                continue

            _reset_nodes(driver=driver)

            if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
                _remove_operations(driver=driver)
                _wait_for_jobs_to_vanish(driver=driver)
                yt_commands.remove("//sys/scheduler/config", force=True, driver=driver)
                yt_commands.remove("//sys/controller_agents/config", force=True, driver=driver)

            _abort_transactions(driver=driver)

            yt_commands.remove("//tmp", driver=driver)
            if self.ENABLE_TMP_PORTAL:
                # XXX(babenko): portals
                wait(lambda: not yt_commands.exists("//tmp&", driver=driver))

            yt_commands.gc_collect(driver=driver)

            scheduler_count = self.get_param("NUM_SCHEDULERS", cluster_index)
            if scheduler_count > 0:
                scheduler_pool_trees_root = self.Env.configs["scheduler"][0]["scheduler"].get("pool_trees_root", "//sys/pool_trees")
            else:
                scheduler_pool_trees_root = "//sys/pool_trees"
            _restore_globals(
                scheduler_count=scheduler_count,
                scheduler_pool_trees_root=scheduler_pool_trees_root,
                driver=driver)

            _remove_objects(
                enable_secondary_cells_cleanup=self.get_param("ENABLE_SECONDARY_CELLS_CLEANUP", cluster_index),
                driver=driver)

            _restore_default_bundle_options(driver=driver)

            yt_commands.gc_collect(driver=driver)

            yt_commands.clear_metadata_caches(driver=driver)


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
