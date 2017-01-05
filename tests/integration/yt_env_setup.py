import yt_commands

from yt.environment import YTInstance
from yt.common import makedirp, update, YtError
import yt_driver_bindings

import pytest

import gc
import os
import sys
import logging
import resource
import shutil
import stat
import subprocess
import uuid
import __builtin__
from distutils.spawn import find_executable
from time import sleep, time
from threading import Thread

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath("tests.sandbox"))
SANDBOX_STORAGE_ROOTDIR = os.environ.get("TESTS_SANDBOX_STORAGE")

linux_only = pytest.mark.skipif('not sys.platform.startswith("linux")')
unix_only = pytest.mark.skipif('not sys.platform.startswith("linux") and not sys.platform.startswith("darwin")')

def skip_if_multicell(func):
    def wrapped_func(self, *args, **kwargs):
        if hasattr(self, "NUM_SECONDARY_MASTER_CELLS") and self.NUM_SECONDARY_MASTER_CELLS > 0:
            pytest.skip("This test does not support multicell mode")
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

def wait(predicate):
    for _ in xrange(100):
        if predicate():
            return
        sleep(1.0)
    pytest.fail("wait failed")

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
                print >>sys.stderr, "check takes %lf seconds" % delta
            sleep(1.0)

    def stop(self):
        self._active = False
        self.join()

class YTEnvSetup(object):
    NUM_MASTERS = 3
    NUM_NONVOTING_MASTERS = 0
    NUM_SECONDARY_MASTER_CELLS = 0
    START_SECONDARY_MASTER_CELLS = True
    NUM_NODES = 5
    NUM_SCHEDULERS = 0

    DELTA_DRIVER_CONFIG = {}
    DELTA_MASTER_CONFIG = {}
    DELTA_NODE_CONFIG = {}
    DELTA_SCHEDULER_CONFIG = {}

    NUM_REMOTE_CLUSTERS = 0

    # To be redefined in successors
    @classmethod
    def modify_master_config(cls, config):
        pass

    @classmethod
    def modify_scheduler_config(cls, config):
        pass

    @classmethod
    def modify_node_config(cls, config):
        pass

    @classmethod
    def on_masters_started(cls):
        pass

    @classmethod
    def setup_class(cls, test_name=None):
        logging.basicConfig(level=logging.INFO)

        if test_name is None:
            test_name = cls.__name__
        cls.test_name = test_name
        path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        # Should be set before env start for correct behaviour of teardown
        cls.liveness_checker = None

        cls.path_to_test = path_to_test
        # For running in parallel
        cls.run_id = "run_" + uuid.uuid4().hex[:8]
        cls.path_to_run = os.path.join(path_to_test, cls.run_id)

        cls.Env = YTInstance(
            cls.path_to_run,
            master_count=cls.NUM_MASTERS,
            nonvoting_master_count=cls.NUM_NONVOTING_MASTERS,
            secondary_master_cell_count=cls.NUM_SECONDARY_MASTER_CELLS,
            node_count=cls.NUM_NODES,
            scheduler_count=cls.NUM_SCHEDULERS,
            kill_child_processes=True,
            port_locks_path=os.path.join(SANDBOX_ROOTDIR, "ports"),
            fqdn="localhost",
            modify_configs_func=cls.apply_config_patches)

        # Init driver before starting the cluster. Some hooks use the driver during starting up (e.g. on_masters_started()).
        if cls.Env.configs["driver"]:
            secondary_driver_configs = [cls.Env.configs["driver_secondary_{0}".format(i)]
                                        for i in xrange(cls.NUM_SECONDARY_MASTER_CELLS)]
            yt_commands.init_drivers(cls.Env.configs["driver"], secondary_driver_configs)
            yt_commands.is_multicell = (cls.NUM_SECONDARY_MASTER_CELLS > 0)
            yt_driver_bindings.configure_logging(cls.Env.driver_logging_config)

        cls.Env.start(start_secondary_master_cells=cls.START_SECONDARY_MASTER_CELLS, on_masters_started_func=cls.on_masters_started)

        yt_commands.path_to_run_tests = cls.path_to_run

        # To avoid strange hangups.
        if cls.NUM_MASTERS > 0:
            cls.liveness_checker = Checker(lambda: cls.Env.check_liveness(callback_func=_pytest_finalize_func))
            cls.liveness_checker.daemon = True
            cls.liveness_checker.start()

    @classmethod
    def apply_config_patches(cls, configs, ytserver_version):
        for tag in [configs["master"]["primary_cell_tag"]] + configs["master"]["secondary_cell_tags"]:
            for config in configs["master"][tag]:
                update(config, cls.DELTA_MASTER_CONFIG)
                cls.modify_master_config(config)
        for config in configs["scheduler"]:
            update(config, cls.DELTA_SCHEDULER_CONFIG)
            cls.modify_scheduler_config(config)
        for config in configs["node"]:
            update(config, cls.DELTA_NODE_CONFIG)
            cls.modify_node_config(config)
        for config in configs["driver"].values():
            update(config, cls.DELTA_DRIVER_CONFIG)

    @classmethod
    def teardown_class(cls):
        if cls.liveness_checker is not None:
            cls.liveness_checker.stop()

        cls.Env.stop()
        cls.Env.kill_cgroups()
        yt_commands.terminate_drivers()
        gc.collect()

        if not os.path.exists(cls.path_to_run):
            return

        if SANDBOX_STORAGE_ROOTDIR is not None:
            makedirp(SANDBOX_STORAGE_ROOTDIR)

            # XXX(asaitgalin): Ensure tests running user has enough permissions to manipulate YT sandbox.
            chown_command = ["sudo", "chown", "-R", "{0}:{1}".format(os.getuid(), os.getgid()), cls.path_to_run]
            p = subprocess.Popen(chown_command, stderr=subprocess.PIPE)
            _, stderr = p.communicate()
            if p.returncode != 0:
                print >>sys.stderr, stderr
                raise subprocess.CalledProcessError(p.returncode, " ".join(chown_command))

            # XXX(dcherednik): Delete named pipes
            subprocess.check_call(["find", cls.path_to_run, "-type", "p", "-delete"])

            destination_path = os.path.join(SANDBOX_STORAGE_ROOTDIR, cls.test_name, cls.run_id)
            if os.path.exists(destination_path):
                shutil.rmtree(destination_path)

            shutil.move(cls.path_to_run, destination_path)

    def setup_method(self, method):
        if self.NUM_MASTERS > 0:
            self.transactions_at_start = set(yt_commands.get_transactions())
            self.wait_for_nodes()
            self.wait_for_chunk_replicator()

    def teardown_method(self, method):
        self.Env.check_liveness(callback_func=_pytest_finalize_func)
        if self.NUM_MASTERS > 0:
            self._abort_transactions()
            yt_commands.set('//tmp', {})
            self._remove_operations()
            self._wait_jobs_to_abort()
            yt_commands.gc_collect()
            yt_commands.clear_metadata_caches()
            self._reset_nodes()
            self._reenable_chunk_replicator()
            self._remove_accounts()
            self._remove_users()
            self._remove_groups()
            self._remove_tablet_cells()
            self._remove_tablet_cell_bundles()
            self._remove_racks()
            self._remove_data_centers()
            self._remove_pools()

            yt_commands.gc_collect()

    def set_node_banned(self, address, flag):
        yt_commands.set("//sys/nodes/%s/@banned" % address, flag)
        ban, state = ("banned", "offline") if flag else ("unbanned", "online")
        print "Waiting for node %s to become %s..." % (address, ban)
        wait(lambda: yt_commands.get("//sys/nodes/%s/@state" % address) == state)

    def wait_for_nodes(self):
        print "Waiting for nodes to become online..."
        wait(lambda: all(n.attributes["state"] == "online" for n in yt_commands.ls("//sys/nodes", attributes=["state"])))

    def wait_for_chunk_replicator(self):
        print "Waiting for chunk replicator to become enabled..."
        wait(lambda: yt_commands.get("//sys/@chunk_replicator_enabled"))

    def wait_for_cells(self):
        print "Waiting for tablet cells to become healthy..."
        wait(lambda: all(c.attributes["health"] == "good" for c in yt_commands.ls("//sys/tablet_cells", attributes=["health"])))

    def sync_create_cells(self, cell_count, tablet_cell_bundle="default"):
        for _ in xrange(cell_count):
            yt_commands.create_tablet_cell(attributes={
                "tablet_cell_bundle": tablet_cell_bundle
            })
        self.wait_for_cells()

    def wait_for_tablet_state(self, path, states):
        print "Waiting for tablets to become %s..." % ", ".join(str(state) for state in states)
        wait(lambda: all(any(x["state"] == state for state in states) for x in yt_commands.get(path + "/@tablets")))

    def wait_until_sealed(self, path):
        wait(lambda: yt_commands.get(path + "/@sealed"))

    def _wait_for_tablets(self, path, state, **kwargs):
        tablet_count = yt_commands.get(path + '/@tablet_count')
        first_tablet_index = kwargs.get("first_tablet_index", 0)
        last_tablet_index = kwargs.get("last_tablet_index", tablet_count - 1)
        wait(lambda: all(x["state"] == state for x in yt_commands.get(path + "/@tablets")[first_tablet_index:last_tablet_index + 1]))

    def sync_mount_table(self, path, freeze=False, **kwargs):
        yt_commands.mount_table(path, freeze=freeze, **kwargs)

        print "Waiting for tablets to become mounted..."
        self._wait_for_tablets(path, "frozen" if freeze else "mounted", **kwargs)

    def sync_unmount_table(self, path, **kwargs):
        yt_commands.unmount_table(path, **kwargs)

        print "Waiting for tablets to become unmounted..."
        self._wait_for_tablets(path, "unmounted", **kwargs)

    def sync_freeze_table(self, path, **kwargs):
        yt_commands.freeze_table(path, **kwargs)

        print "Waiting for tablets to become frozen..."
        self._wait_for_tablets(path, "frozen", **kwargs)

    def sync_unfreeze_table(self, path, **kwargs):
        yt_commands.unfreeze_table(path, **kwargs)

        print "Waiting for tablets to become mounted..."
        self._wait_for_tablets(path, "mounted", **kwargs)

    def sync_flush_table(self, path):
        self.sync_freeze_table(path)
        self.sync_unfreeze_table(path)

    def sync_compact_table(self, path):
        chunk_ids = __builtin__.set(yt_commands.get(path + "/@chunk_ids"))
        yt_commands.set(path + "/@forced_compaction_revision", yt_commands.get(path + "/@revision"))
        yt_commands.set(path + "/@forced_compaction_revision", yt_commands.get(path + "/@revision"))
        yt_commands.remount_table(path)

        print "Waiting for tablets to become compacted..."
        wait(lambda: len(chunk_ids.intersection(__builtin__.set(yt_commands.get(path + "/@chunk_ids")))) == 0)

    def _abort_transactions(self):
         for tx in yt_commands.ls("//sys/transactions", attributes=["title"]):
            title = tx.attributes.get("title", "")
            id = str(tx)
            if "Scheduler lock" in title:
                continue
            if "Lease for node" in title:
                continue
            try:
                yt_commands.abort_transaction(id)
            except:
                pass

    def sync_disable_table_replica(self, replica_id):
        yt_commands.disable_table_replica(replica_id)

        print "Waiting for replica to become disabled..."
        wait(lambda: yt_commands.get("#{0}/@state".format(replica_id)) == "disabled")

    def _reset_nodes(self):
        nodes = yt_commands.ls("//sys/nodes", attributes=["banned", "resource_limits_overrides", "user_tags"])
        for node in nodes:
            node_name = str(node)
            if node.attributes["banned"]:
                yt_commands.set("//sys/nodes/%s/@banned" % node_name, False)
            if node.attributes["resource_limits_overrides"] != {}:
                yt_commands.set("//sys/nodes/%s/@resource_limits_overrides" % node_name, {})
            if node.attributes["user_tags"] != []:
                yt_commands.set("//sys/nodes/%s/@user_tags" % node_name, [])

    def _remove_operations(self):
        if self.NUM_SCHEDULERS == 0:
            return

        try:
            for operation_id in yt_commands.ls("//sys/scheduler/orchid/scheduler/operations"):
                yt_commands.abort_op(operation_id)
        except YtError:
            pass
        for operation in yt_commands.ls("//sys/operations"):
            yt_commands.remove("//sys/operations/" + operation, recursive=True)

    def _wait_jobs_to_abort(self):
        def check_jobs_are_missing():
            for node in yt_commands.ls("//sys/nodes"):
                jobs = yt_commands.get("//sys/nodes/{0}/orchid/job_controller/active_job_count".format(node))
                if jobs.get("scheduler", 0) > 0:
                    return False
            return True

        wait(check_jobs_are_missing)

    def _reenable_chunk_replicator(self):
        if yt_commands.exists("//sys/@disable_chunk_replicator"):
            yt_commands.remove("//sys/@disable_chunk_replicator")

    def _remove_accounts(self):
        accounts = yt_commands.ls("//sys/accounts", attributes=["builtin", "resource_usage"])
        for account in accounts:
            if not account.attributes["builtin"]:
                print >>sys.stderr, account.attributes["resource_usage"]
                yt_commands.remove_account(str(account))

    def _remove_users(self):
        users = yt_commands.ls("//sys/users", attributes=["builtin"])
        for user in users:
            if not user.attributes["builtin"]:
                yt_commands.remove_user(str(user))

    def _remove_groups(self):
        groups = yt_commands.ls("//sys/groups", attributes=["builtin"])
        for group in groups:
            if not group.attributes["builtin"]:
                yt_commands.remove_group(str(group))

    def _remove_tablet_cells(self):
        cells = yt_commands.get_tablet_cells()
        for id in cells:
            yt_commands.remove_tablet_cell(id)

    def _remove_tablet_cell_bundles(self):
        bundles = yt_commands.ls("//sys/tablet_cell_bundles", attributes=["builtin"])
        for bundle in bundles:
            if not bundle.attributes["builtin"]:
                yt_commands.remove_tablet_cell_bundle(str(bundle))

    def _remove_racks(self):
        racks = yt_commands.get_racks()
        for rack in racks:
            yt_commands.remove_rack(rack)

    def _remove_data_centers(self):
        data_centers = yt_commands.get_data_centers()
        for dc in data_centers:
            yt_commands.remove_data_center(dc)

    def _remove_pools(self):
        yt_commands.remove("//sys/pools/*")

    def _find_ut_file(self, file_name):
        unittester_path = find_executable("unittester")
        assert unittester_path is not None
        unittests_path = os.path.join(os.path.dirname(unittester_path), "..", "yt", "unittests")
        assert os.path.exists(unittests_path)
        result_path = os.path.join(unittests_path, file_name)
        assert os.path.exists(result_path)
        return result_path

