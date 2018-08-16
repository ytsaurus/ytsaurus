import yt_commands

from yt.environment import YTInstance, init_operation_archive
from yt.common import makedirp, YtError, format_error
from yt.environment.porto_helpers import porto_avaliable, remove_all_volumes
from yt.test_helpers import wait

from yt.common import update_inplace

import pytest

import gc
import os
import sys
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

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath("tests.sandbox"))
SANDBOX_STORAGE_ROOTDIR = os.environ.get("TESTS_SANDBOX_STORAGE")

##################################################################

def _abort_transactions(driver=None):
    for tx in yt_commands.ls("//sys/transactions", attributes=["title"], driver=driver):
        title = tx.attributes.get("title", "")
        id = str(tx)
        if "Scheduler lock" in title:
            continue
        if "Controller agent incarnation" in title:
            continue
        if "Lease for node" in title:
            continue
        try:
            yt_commands.abort_transaction(id, driver=driver)
        except:
            pass

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
    nodes = yt_commands.ls("//sys/nodes", attributes=attributes, driver=driver)

    for node in nodes:
        node_name = str(node)
        for attribute in boolean_attributes:
            if node.attributes[attribute]:
                yt_commands.set("//sys/nodes/{0}/@{1}".format(node_name, attribute), False, driver=driver)
        if node.attributes["resource_limits_overrides"] != {}:
            yt_commands.set("//sys/nodes/%s/@resource_limits_overrides" % node_name, {}, driver=driver)
        if node.attributes["user_tags"] != []:
            yt_commands.set("//sys/nodes/%s/@user_tags" % node_name, [], driver=driver)

def _remove_operations(driver=None):
    if yt_commands.get("//sys/scheduler/instances/@count", driver=driver) == 0:
        return

    operation_from_orchid = []
    try:
        operation_from_orchid = yt_commands.ls("//sys/scheduler/orchid/scheduler/operations", driver=driver)
    except YtError as err:
        print >>sys.stderr, format_error(err)

    for operation_id in operation_from_orchid:
        try:
            yt_commands.abort_op(operation_id, driver=driver)
        except YtError as err:
            print >>sys.stderr, format_error(err)

    _abort_transactions(driver=driver)
    yt_commands.remove("//sys/operations/*", driver=driver)
    yt_commands.remove("//sys/operations_archive", force=True, driver=driver)

def _wait_for_jobs_to_vanish(driver=None):
    def check_no_jobs():
        for node in yt_commands.ls("//sys/nodes", driver=driver):
            jobs = yt_commands.get("//sys/nodes/{0}/orchid/job_controller/active_job_count".format(node), driver=driver)
            if jobs.get("scheduler", 0) > 0:
                return False
        return True

    wait(check_no_jobs)

def _reset_dynamic_cluster_config(driver=None):
    yt_commands.set("//sys/@config", {}, driver=driver)

def _remove_accounts(driver=None):
    accounts = yt_commands.ls("//sys/accounts", attributes=["builtin", "resource_usage"], driver=driver)
    for account in accounts:
        if not account.attributes["builtin"]:
            print >>sys.stderr, account.attributes["resource_usage"]
            yt_commands.remove_account(str(account), driver=driver)

def _remove_users(driver=None):
    users = yt_commands.ls("//sys/users", attributes=["builtin"], driver=driver)
    for user in users:
        if not user.attributes["builtin"] and str(user) != "application_operations":
            yt_commands.remove_user(str(user), driver=driver)

def _remove_groups(driver=None):
    groups = yt_commands.ls("//sys/groups", attributes=["builtin"], driver=driver)
    for group in groups:
        if not group.attributes["builtin"]:
            yt_commands.remove_group(str(group), driver=driver)

def _remove_tablet_cells(driver=None):
    cells = yt_commands.get_tablet_cells(driver=driver)
    yt_commands.sync_remove_tablet_cells(cells, driver=driver)

def _remove_tablet_cell_bundles(driver=None):
    bundles = yt_commands.ls("//sys/tablet_cell_bundles", attributes=["builtin"], driver=driver)
    for bundle in bundles:
        try:
            if not bundle.attributes["builtin"]:
                yt_commands.remove_tablet_cell_bundle(str(bundle), driver=driver)
            else:
                yt_commands.set("//sys/tablet_cell_bundles/{0}/@options".format(bundle), {
                    "changelog_account": "sys",
                    "snapshot_account": "sys"})
                yt_commands.set("//sys/tablet_cell_bundles/{0}/@dynamic_options".format(bundle), {})
                yt_commands.set("//sys/tablet_cell_bundles/{0}/@tablet_balancer_config".format(bundle), {})
        except:
            pass

def _remove_racks(driver=None):
    racks = yt_commands.get_racks(driver=driver)
    for rack in racks:
        yt_commands.remove_rack(rack, driver=driver)

def _remove_data_centers(driver=None):
    data_centers = yt_commands.get_data_centers(driver=driver)
    for dc in data_centers:
        yt_commands.remove_data_center(dc, driver=driver)

def _restore_default_pool_tree(driver=None):
    yt_commands.remove("//sys/pool_trees/default/*", driver=driver)

def _remove_tablet_actions(driver=None):
    actions = yt_commands.get_tablet_actions()
    for action in actions:
        yt_commands.remove_tablet_action(action, driver=driver)

def find_ut_file(file_name):
    unittester_path = find_executable("unittester-ytlib")
    assert unittester_path is not None
    for unittests_path in [
        os.path.join(os.path.dirname(unittester_path), "..", "yt", "ytlib", "unittests"),
        os.path.dirname(unittester_path)
    ]:
        result_path = os.path.join(unittests_path, file_name)
        if os.path.exists(result_path):
            return result_path
    else:
        raise RuntimeErorr("Cannot find '{0}'".format(file_name))

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
                    if key == "parametrize" or "flaky" in key or "skip" in key:
                        cls.__dict__[attr].__dict__[key] = parent.__dict__[attr].__dict__[key]
        return cls

    return patcher

##################################################################

linux_only = pytest.mark.skipif('not sys.platform.startswith("linux")')
unix_only = pytest.mark.skipif('not sys.platform.startswith("linux") and not sys.platform.startswith("darwin")')

patch_porto_env_only = lambda parent: patch_subclass(parent, not porto_avaliable(), reason="you need configured porto to run it")

def skip_if_porto(func):
    def wrapped_func(self, *args, **kwargs):
        if hasattr(self, "USE_PORTO_FOR_SERVERS") and self.USE_PORTO_FOR_SERVERS:
            pytest.skip("This test does not support porto isolation")
        func(self, *args, **kwargs)
    return wrapped_func


# doesn't work with @patch_porto_env_only on the same class, wrap each method
def require_ytserver_root_privileges(func_or_class):

    def check_root_privileges():
        for binary in ["ytserver-exec", "ytserver-job-proxy", "ytserver-node", "ytserver-tools"]:
            binary_path = find_executable(binary)
            binary_stat = os.stat(binary_path)
            if (binary_stat.st_mode & stat.S_ISUID) == 0:
                pytest.fail('This test requires a suid bit set for "{}"'.format(binary))
            if binary_stat.st_uid != 0:
                pytest.fail('This test requires "{}" being owned by root'.format(binary))

    if inspect.isclass(func_or_class):
        class Wrap(func_or_class):
            @classmethod
            def setup_class(cls):
                check_root_privileges()
                func_or_class.setup_class()

        return Wrap
    else:
        def wrap_func(self, *args, **kwargs):
            check_root_privileges()
            func_or_class(self, *args, **kwargs)

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
    def wrapped_func(self, *args, **kwargs):
        rlimit_core = resource.getrlimit(resource.RLIMIT_CORE)
        if rlimit_core[0] == 0:
            pytest.skip('This test requires enabled core dump (how about "ulimit -c unlimited"?)')
        func(self, *args, **kwargs)
    return wrapped_func

def resolve_test_paths(name):
    path_to_sandbox = os.path.join(SANDBOX_ROOTDIR, name)
    path_to_environment = os.path.join(path_to_sandbox, "run")
    return path_to_sandbox, path_to_environment

def _pytest_finalize_func(environment, process_call_args):
    print >>sys.stderr, 'Process run by command "{0}" is dead!'.format(" ".join(process_call_args))
    environment.stop()

    print >>sys.stderr, "Killing pytest process"
    os._exit(42)

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
    NUM_NONVOTING_MASTERS = 0
    NUM_SECONDARY_MASTER_CELLS = 0
    START_SECONDARY_MASTER_CELLS = True
    ENABLE_MULTICELL_TEARDOWN = True
    NUM_NODES = 5
    NUM_SCHEDULERS = 0
    NUM_CONTROLLER_AGENTS = None
    ENABLE_PROXY = False
    ENABLE_RPC_PROXY = False
    DRIVER_BACKEND = "native"
    NUM_SKYNET_MANAGERS = 0

    DELTA_DRIVER_CONFIG = {}
    DELTA_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}
    DELTA_CONTROLLER_AGENT_CONFIG = {}

    USE_PORTO_FOR_SERVERS = False
    USE_DYNAMIC_TABLES = False
    USE_MASTER_CACHE = False

    NUM_REMOTE_CLUSTERS = 0

    SINGLE_SETUP_TEARDOWN = False

    # To be redefined in successors
    @classmethod
    def modify_master_config(cls, config):
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

        instance = YTInstance(
            path,
            master_count=cls.get_param("NUM_MASTERS", index),
            nonvoting_master_count=cls.get_param("NUM_NONVOTING_MASTERS", index),
            secondary_master_cell_count=cls.get_param("NUM_SECONDARY_MASTER_CELLS", index),
            node_count=cls.get_param("NUM_NODES", index),
            scheduler_count=cls.get_param("NUM_SCHEDULERS", index),
            controller_agent_count=cls.get_param("NUM_CONTROLLER_AGENTS", index),
            has_proxy=cls.get_param("ENABLE_PROXY", index),
            has_rpc_proxy=cls.get_param("ENABLE_RPC_PROXY", index),
            skynet_manager_count=cls.get_param("NUM_SKYNET_MANAGERS", index),
            kill_child_processes=True,
            use_porto_for_servers=cls.get_param("USE_PORTO_FOR_SERVERS", index),
            port_locks_path=os.path.join(SANDBOX_ROOTDIR, "ports"),
            fqdn="localhost",
            enable_master_cache=cls.get_param("USE_MASTER_CACHE", index),
            modify_configs_func=modify_configs_func,
            cell_tag=index * 10,
            enable_structured_master_logging=True)

        instance._cluster_name = cls.get_cluster_name(index)
        instance._driver_backend = cls.get_param("DRIVER_BACKEND", index)

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

        if test_name is None:
            test_name = cls.__name__
        cls.test_name = test_name
        path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        # Should be set before env start for correct behaviour of teardown
        cls.liveness_checkers = []

        cls.path_to_test = path_to_test
        # For running in parallel
        cls.run_id = "run_" + uuid.uuid4().hex[:8] if not run_id else run_id
        cls.path_to_run = os.path.join(path_to_test, cls.run_id)

        primary_cluster_path = cls.path_to_run
        if cls.NUM_REMOTE_CLUSTERS > 0:
            primary_cluster_path = os.path.join(cls.path_to_run, "primary")

        cls.Env = cls.create_yt_cluster_instance(0, primary_cluster_path)
        cls.remote_envs = []  # TODO: Rename env
        for cluster_index in xrange(1, cls.NUM_REMOTE_CLUSTERS + 1):
            cluster_path = os.path.join(cls.path_to_run, cls.get_cluster_name(cluster_index))
            cls.remote_envs.append(cls.create_yt_cluster_instance(cluster_index, cluster_path))

        yt_commands.is_multicell = cls.NUM_SECONDARY_MASTER_CELLS > 0
        yt_commands.path_to_run_tests = cls.path_to_run
        yt_commands.init_drivers([cls.Env] + cls.remote_envs)

        cls.Env.start(use_proxy_from_package=False, start_secondary_master_cells=cls.START_SECONDARY_MASTER_CELLS, on_masters_started_func=cls.on_masters_started)
        for index, env in enumerate(cls.remote_envs):
            env.start(start_secondary_master_cells=cls.get_param("START_SECONDARY_MASTER_CELLS", index))

        yt_commands.wait_drivers()

        for env in [cls.Env] + cls.remote_envs:
            # To avoid strange hangups.
            if env.master_count > 0:
                liveness_checker = Checker(lambda: env.check_liveness(callback_func=_pytest_finalize_func))
                liveness_checker.daemon = True
                liveness_checker.start()
                cls.liveness_checkers.append(liveness_checker)

        if cls.remote_envs:
            clusters = {}
            for instance in [cls.Env] + cls.remote_envs:
                connection_config = {
                    "primary_master": instance.configs["master"][0]["primary_master"],
                    "secondary_masters": instance.configs["master"][0]["secondary_masters"],
                    "timestamp_provider": instance.configs["master"][0]["timestamp_provider"],
                    "transaction_manager": instance.configs["master"][0]["transaction_manager"],
                    "table_mount_cache": instance.configs["driver"]["table_mount_cache"],
                    "cell_directory_synchronizer": instance.configs["driver"]["cell_directory_synchronizer"],
                    "cluster_directory_synchronizer": instance.configs["driver"]["cluster_directory_synchronizer"]
                }
                clusters[instance._cluster_name] = connection_config

            for cluster_index in xrange(cls.NUM_REMOTE_CLUSTERS + 1):
                driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
                if driver is None:
                    continue
                yt_commands.set("//sys/clusters", clusters, driver=driver)

            sleep(1.0)

        if cls.USE_DYNAMIC_TABLES:
            for cluster_index in xrange(cls.NUM_REMOTE_CLUSTERS + 1):
                driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
                if driver is None:
                    continue
                # Raise dynamic tables limits since they are zero by default.
                yt_commands.set("//sys/accounts/tmp/@resource_limits/tablet_count", 10000, driver=driver)
                yt_commands.set("//sys/accounts/tmp/@resource_limits/tablet_static_memory", 1024 * 1024 * 1024, driver=driver)

        if cls.SINGLE_SETUP_TEARDOWN:
            cls._setup_method()

    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index):
        for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
            for index, config in enumerate(configs["master"][tag]):
                configs["master"][tag][index] = update_inplace(config, cls.get_param("DELTA_MASTER_CONFIG", cluster_index))
                cls.modify_master_config(configs["master"][tag][index])
        for index, config in enumerate(configs["scheduler"]):
            configs["scheduler"][index] = update_inplace(config, cls.get_param("DELTA_SCHEDULER_CONFIG", cluster_index))
            cls.modify_scheduler_config(configs["scheduler"][index])
        for index, config in enumerate(configs["controller_agent"]):
            delta_config = cls.get_param("DELTA_CONTROLLER_AGENT_CONFIG", cluster_index)
            configs["controller_agent"][index] = update_inplace(config, delta_config)
            cls.modify_controller_agent_config(configs["controller_agent"][index])
        for index, config in enumerate(configs["node"]):
            configs["node"][index] = update_inplace(config, cls.get_param("DELTA_NODE_CONFIG", cluster_index))
            cls.modify_node_config(configs["node"][index])
        for key, config in configs["driver"].iteritems():
            configs["driver"][key] = update_inplace(config, cls.get_param("DELTA_DRIVER_CONFIG", cluster_index))

    @classmethod
    def teardown_class(cls):
        if cls.SINGLE_SETUP_TEARDOWN:
            cls._teardown_method()

        if cls.liveness_checkers:
            map(lambda c: c.stop(), cls.liveness_checkers)

        for env in [cls.Env] + cls.remote_envs:
            env.stop()
            env.kill_cgroups()

        yt_commands.terminate_drivers()
        gc.collect()

        if not os.path.exists(cls.path_to_run):
            return

        if SANDBOX_STORAGE_ROOTDIR is not None:
            makedirp(SANDBOX_STORAGE_ROOTDIR)

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

            destination_path = os.path.join(SANDBOX_STORAGE_ROOTDIR, cls.test_name, cls.run_id)
            if os.path.exists(destination_path):
                shutil.rmtree(destination_path)

            shutil.move(cls.path_to_run, destination_path)


    @classmethod
    def _setup_method(cls):
        for cluster_index in xrange(cls.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
            if driver is None:
                continue

            if cls.USE_DYNAMIC_TABLES:
                yt_commands.set("//sys/@config/tablet_manager/tablet_balancer/tablet_balancer_schedule", "1", driver=driver)
                yt_commands.set("//sys/@config/tablet_manager/tablet_cell_balancer/enable_verbose_logging", True, driver=driver)

            yt_commands.wait_for_nodes(driver=driver)
            yt_commands.wait_for_chunk_replicator(driver=driver)
        yt_commands.reset_events_on_fs()

    @classmethod
    def _teardown_method(cls):
        yt_commands._zombie_responses[:] = []

        for env in [cls.Env] + cls.remote_envs:
            env.check_liveness(callback_func=_pytest_finalize_func)

        for cluster_index in xrange(cls.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=cls.get_cluster_name(cluster_index))
            if driver is None:
                continue

            _reset_nodes(driver=driver)
            if cls.get_param("NUM_SCHEDULERS", cluster_index) > 0:
                _remove_operations(driver=driver)
                _wait_for_jobs_to_vanish(driver=driver)
                _restore_default_pool_tree(driver=driver)
            _abort_transactions(driver=driver)
            yt_commands.remove("//tmp", driver=driver)
            yt_commands.create("map_node", "//tmp",
                               attributes={
                                "account": "tmp",
                                "acl": [{"action": "allow", "permissions": ["read", "write", "remove"], "subjects": ["users"]}],
                                "opaque": True
                               },
                               driver=driver)
            _remove_accounts(driver=driver)
            _remove_users(driver=driver)
            _remove_groups(driver=driver)
            if cls.get_param("USE_DYNAMIC_TABLES", cluster_index):
                yt_commands.gc_collect(driver=driver)
                _remove_tablet_cells(driver=driver)
                _remove_tablet_cell_bundles(driver=driver)
                _remove_tablet_actions(driver=driver)
            _remove_racks(driver=driver)
            _remove_data_centers(driver=driver)
            _reset_dynamic_cluster_config(driver=driver)

            yt_commands.gc_collect(driver=driver)
            yt_commands.clear_metadata_caches(driver=driver)

    def setup_method(self, method):
        if not self.SINGLE_SETUP_TEARDOWN:
            self._setup_method()

    def teardown_method(self, method):
        if not self.SINGLE_SETUP_TEARDOWN:
            self._teardown_method()

