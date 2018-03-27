import yt_commands

from yt.environment import YTInstance
from yt.common import makedirp, YtError, format_error
from yt.environment.porto_helpers import porto_avaliable, remove_all_volumes

from yt.common import update_inplace

import pytest

import gc
import os
import sys
import logging
import resource
import shutil
import functools
import inspect
import stat
import subprocess
import uuid
import __builtin__
from distutils.spawn import find_executable
from time import sleep, time
from threading import Thread

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath("tests.sandbox"))
SANDBOX_STORAGE_ROOTDIR = os.environ.get("TESTS_SANDBOX_STORAGE")

##################################################################


try:
    from yt.environment.helpers import wait
except ImportError:
    # COMPAT(ignat)
    def wait(predicate, iter=100, sleep_backoff=0.3):
        for _ in xrange(iter):
            if predicate():
                return
            sleep(sleep_backoff)
        pytest.fail("wait failed")

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

patch_porto_env_only = lambda parent: patch_subclass(parent, False, reason="you need configured porto to run it")

def skip_if_porto(func):
    def wrapped_func(self, *args, **kwargs):
        if hasattr(self, "USE_PORTO_FOR_SERVERS") and self.USE_PORTO_FOR_SERVERS:
            pytest.skip("This test does not support porto isolation")
        func(self, *args, **kwargs)
    return wrapped_func

def require_ytserver_root_privileges(func):
    def wrapped_func(self, *args, **kwargs):
        ytserver_node_path = find_executable("ytserver-node")
        ytserver_node_stat = os.stat(ytserver_node_path)
        if (ytserver_node_stat.st_mode & stat.S_ISUID) == 0:
            pytest.fail('This test requires a suid bit set for "ytserver-node"')
        if ytserver_node_stat.st_uid != 0:
            pytest.fail('This test requires "ytserver-node" being owned by root')
        func(self, *args, **kwargs)
    return wrapped_func

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
    NUM_SKYNET_MANAGERS = 0

    DELTA_DRIVER_CONFIG = {}
    DELTA_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}
    DELTA_CONTROLLER_AGENT_CONFIG = {}

    USE_PORTO_FOR_SERVERS = False
    USE_DYNAMIC_TABLES = False

    NUM_REMOTE_CLUSTERS = 0

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
            modify_configs_func=modify_configs_func,
            cell_tag=index * 10)

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

    def setup_method(self, method):
        self.transactions_at_start = []
        for cluster_index in xrange(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                self.transactions_at_start.append(set())
                continue

            self.transactions_at_start.append(set(yt_commands.get_transactions(driver=driver)))
            self.wait_for_nodes(driver=driver)
            self.wait_for_chunk_replicator(driver=driver)
        yt_commands.reset_events_on_fs()

    def teardown_method(self, method):
        yt_commands._zombie_responses[:] = []

        for env in [self.Env] + self.remote_envs:
            env.check_liveness(callback_func=_pytest_finalize_func)

        for cluster_index in xrange(self.NUM_REMOTE_CLUSTERS + 1):
            driver = yt_commands.get_driver(cluster=self.get_cluster_name(cluster_index))
            if driver is None:
                continue

            self._reset_nodes(driver=driver)
            if self.get_param("NUM_SCHEDULERS", cluster_index) > 0:
                self._remove_operations(driver=driver)
                self._wait_for_jobs_to_vanish(driver=driver)
                self._restore_default_pool_tree(driver=driver)
            self._abort_transactions(driver=driver)
            yt_commands.remove("//tmp", driver=driver)
            yt_commands.create("map_node", "//tmp",
                               attributes={
                                "account": "tmp",
                                "acl": [{"action": "allow", "permissions": ["read", "write", "remove"], "subjects": ["users"]}],
                                "opaque": True
                               },
                               driver=driver)
            self._remove_accounts(driver=driver)
            self._remove_users(driver=driver)
            self._remove_groups(driver=driver)
            if self.get_param("USE_DYNAMIC_TABLES", cluster_index):
                yt_commands.gc_collect(driver=driver)
                self._remove_tablet_cells(driver=driver)
                self._remove_tablet_cell_bundles(driver=driver)
            self._remove_racks(driver=driver)
            self._remove_data_centers(driver=driver)
            if self.get_param("ENABLE_MULTICELL_TEARDOWN", cluster_index):
                self._remove_tablet_actions(driver=driver)
            self._reset_dynamic_cluster_config(driver=driver)

            yt_commands.gc_collect(driver=driver)
            yt_commands.clear_metadata_caches(driver=driver)

    def set_node_banned(self, address, flag, driver=None):
        yt_commands.set("//sys/nodes/%s/@banned" % address, flag, driver=driver)
        ban, state = ("banned", "offline") if flag else ("unbanned", "online")
        print >>sys.stderr, "Waiting for node %s to become %s..." % (address, ban)
        wait(lambda: yt_commands.get("//sys/nodes/%s/@state" % address, driver=driver) == state)

    def set_node_decommissioned(self, address, flag, driver=None):
        yt_commands.set("//sys/nodes/%s/@decommissioned" % address, flag, driver=driver)
        print >>sys.stderr, "Node %s is %s" % (address, "decommissioned" if flag else "not decommissioned")

    def wait_for_nodes(self, driver=None):
        print >>sys.stderr, "Waiting for nodes to become online..."
        wait(lambda: all(n.attributes["state"] == "online"
                         for n in yt_commands.ls("//sys/nodes", attributes=["state"], driver=driver)))

    def wait_for_chunk_replicator(self, driver=None):
        print >>sys.stderr, "Waiting for chunk replicator to become enabled..."
        wait(lambda: yt_commands.get("//sys/@chunk_replicator_enabled", driver=driver))

    def wait_for_cells(self, cell_ids=None, driver=None):
        print >>sys.stderr, "Waiting for tablet cells to become healthy..."
        def get_cells():
            cells = yt_commands.ls("//sys/tablet_cells", attributes=["health", "id", "peers"], driver=driver)
            if cell_ids == None:
                return cells
            return [cell for cell in cells if cell.attributes["id"] in cell_ids]

        def check_cells():
            cells = get_cells()
            for cell in cells:
                if cell.attributes["health"] != "good":
                    return False
                node = cell.attributes["peers"][0]["address"]
                try:
                    if not yt_commands.exists("//sys/nodes/{0}/orchid/tablet_cells/{1}".format(node, cell.attributes["id"]), driver=driver):
                        return False
                except yt_commands.YtResponseError:
                    return False
            return True

        wait(check_cells)

    def sync_create_cells(self, cell_count, tablet_cell_bundle="default", driver=None):
        cell_ids = []
        for _ in xrange(cell_count):
            cell_id = yt_commands.create_tablet_cell(attributes={
                "tablet_cell_bundle": tablet_cell_bundle
            }, driver=driver)
            cell_ids.append(cell_id)
        self.wait_for_cells(cell_ids, driver=driver)
        return cell_ids

    def wait_for_tablet_state(self, path, states, driver=None):
        print >>sys.stderr, "Waiting for tablets to become %s..." % ", ".join(str(state) for state in states)
        wait(lambda: all(any(x["state"] == state for state in states)
                        for x in yt_commands.get(path + "/@tablets", driver=driver)))

    def wait_until_sealed(self, path, driver=None):
        wait(lambda: yt_commands.get(path + "/@sealed", driver=driver))

    def _wait_for_tablets(self, path, state, **kwargs):
        driver = kwargs.pop("driver", None)
        tablet_count = yt_commands.get(path + '/@tablet_count', driver=driver)
        first_tablet_index = kwargs.get("first_tablet_index", 0)
        last_tablet_index = kwargs.get("last_tablet_index", tablet_count - 1)
        wait(lambda: all(x["state"] == state for x in
             yt_commands.get(path + "/@tablets", driver=driver)[first_tablet_index:last_tablet_index + 1]))

    def sync_mount_table(self, path, freeze=False, **kwargs):
        yt_commands.mount_table(path, freeze=freeze, **kwargs)

        print >>sys.stderr, "Waiting for tablets to become mounted..."
        self._wait_for_tablets(path, "frozen" if freeze else "mounted", **kwargs)

    def sync_unmount_table(self, path, **kwargs):
        yt_commands.unmount_table(path, **kwargs)

        print >>sys.stderr, "Waiting for tablets to become unmounted..."
        self._wait_for_tablets(path, "unmounted", **kwargs)

    def sync_freeze_table(self, path, **kwargs):
        yt_commands.freeze_table(path, **kwargs)

        print >>sys.stderr, "Waiting for tablets to become frozen..."
        self._wait_for_tablets(path, "frozen", **kwargs)

    def sync_unfreeze_table(self, path, **kwargs):
        yt_commands.unfreeze_table(path, **kwargs)

        print >>sys.stderr, "Waiting for tablets to become mounted..."
        self._wait_for_tablets(path, "mounted", **kwargs)

    def sync_flush_table(self, path, driver=None):
        self.sync_freeze_table(path, driver=driver)
        self.sync_unfreeze_table(path, driver=driver)

    def sync_compact_table(self, path, driver=None):
        chunk_ids = __builtin__.set(yt_commands.get(path + "/@chunk_ids", driver=driver))
        yt_commands.set(path + "/@forced_compaction_revision",
                        yt_commands.get(path + "/@revision", driver=driver), driver=driver)
        yt_commands.set(path + "/@forced_compaction_revision",
                        yt_commands.get(path + "/@revision", driver=driver), driver=driver)
        yt_commands.remount_table(path, driver=driver)

        print >>sys.stderr, "Waiting for tablets to become compacted..."
        wait(lambda: len(chunk_ids.intersection(__builtin__.set(yt_commands.get(path + "/@chunk_ids", driver=driver)))) == 0)

    def _abort_transactions(self, driver=None):
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

    def sync_enable_table_replica(self, replica_id, driver=None):
        yt_commands.alter_table_replica(replica_id, enabled=True, driver=driver)

        print >>sys.stderr, "Waiting for replica to become enabled..."
        wait(lambda: yt_commands.get("#{0}/@state".format(replica_id), driver=driver) == "enabled")

    def sync_disable_table_replica(self, replica_id, driver=None):
        yt_commands.alter_table_replica(replica_id, enabled=False, driver=driver)

        print >>sys.stderr, "Waiting for replica to become disabled..."
        wait(lambda: yt_commands.get("#{0}/@state".format(replica_id), driver=driver) == "disabled")

    def _reset_nodes(self, driver=None):
        nodes = yt_commands.ls("//sys/nodes", attributes=["banned", "decommissioned", "resource_limits_overrides", "user_tags"],
                               driver=driver)
        for node in nodes:
            node_name = str(node)
            if node.attributes["banned"]:
                yt_commands.set("//sys/nodes/%s/@banned" % node_name, False, driver=driver)
            if node.attributes["decommissioned"]:
                yt_commands.set("//sys/nodes/%s/@decommissioned" % node_name, False, driver=driver)
            if node.attributes["resource_limits_overrides"] != {}:
                yt_commands.set("//sys/nodes/%s/@resource_limits_overrides" % node_name, {}, driver=driver)
            if node.attributes["user_tags"] != []:
                yt_commands.set("//sys/nodes/%s/@user_tags" % node_name, [], driver=driver)

    def _remove_operations(self, driver=None):
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

        self._abort_transactions(driver=driver)
        yt_commands.remove("//sys/operations/*", driver=driver)
        yt_commands.remove("//sys/operations_archive", force=True, driver=driver)

    def _wait_for_jobs_to_vanish(self, driver=None):
        def check_no_jobs():
            for node in yt_commands.ls("//sys/nodes", driver=driver):
                jobs = yt_commands.get("//sys/nodes/{0}/orchid/job_controller/active_job_count".format(node), driver=driver)
                if jobs.get("scheduler", 0) > 0:
                    return False
            return True

        wait(check_no_jobs)

    def _reset_dynamic_cluster_config(self, driver=None):
        yt_commands.set("//sys/@config", {}, driver=driver)

    def _remove_accounts(self, driver=None):
        accounts = yt_commands.ls("//sys/accounts", attributes=["builtin", "resource_usage"], driver=driver)
        for account in accounts:
            if not account.attributes["builtin"]:
                print >>sys.stderr, account.attributes["resource_usage"]
                yt_commands.remove_account(str(account), driver=driver)

    def _remove_users(self, driver=None):
        users = yt_commands.ls("//sys/users", attributes=["builtin"], driver=driver)
        for user in users:
            if not user.attributes["builtin"] and str(user) != "application_operations":
                yt_commands.remove_user(str(user), driver=driver)

    def _remove_groups(self, driver=None):
        groups = yt_commands.ls("//sys/groups", attributes=["builtin"], driver=driver)
        for group in groups:
            if not group.attributes["builtin"]:
                yt_commands.remove_group(str(group), driver=driver)

    def _remove_tablet_cells(self, driver=None):
        cells = yt_commands.get_tablet_cells(driver=driver)
        for id in cells:
            yt_commands.remove_tablet_cell(id, driver=driver)

    def _remove_tablet_cell_bundles(self, driver=None):
        bundles = yt_commands.ls("//sys/tablet_cell_bundles", attributes=["builtin"], driver=driver)
        for bundle in bundles:
            if not bundle.attributes["builtin"]:
                yt_commands.remove_tablet_cell_bundle(str(bundle), driver=driver)
            else:
                yt_commands.set("//sys/tablet_cell_bundles/{0}/@options".format(bundle), {
                    "changelog_account": "sys",
                    "snapshot_account": "sys"})
                yt_commands.set("//sys/tablet_cell_bundles/{0}/@tablet_balancer_config".format(bundle), {})

    def _remove_racks(self, driver=None):
        racks = yt_commands.get_racks(driver=driver)
        for rack in racks:
            yt_commands.remove_rack(rack, driver=driver)

    def _remove_data_centers(self, driver=None):
        data_centers = yt_commands.get_data_centers(driver=driver)
        for dc in data_centers:
            yt_commands.remove_data_center(dc, driver=driver)

    def _restore_default_pool_tree(self, driver=None):
        yt_commands.remove("//sys/pool_trees/default/*", driver=driver)

    def _remove_tablet_actions(self, driver=None):
        actions = yt_commands.get_tablet_actions()
        for action in actions:
            yt_commands.remove_tablet_action(action, driver=driver)

    def _find_ut_file(self, file_name):
        unittester_path = find_executable("unittester-ytlib")
        assert unittester_path is not None
        unittests_path = os.path.join(os.path.dirname(unittester_path), "..", "yt", "ytlib", "unittests")
        assert os.path.exists(unittests_path)
        result_path = os.path.join(unittests_path, file_name)
        assert os.path.exists(result_path)
        return result_path

