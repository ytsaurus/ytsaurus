import yt_commands

from yt.environment import YTEnv
from yt.common import makedirp
import yt_driver_bindings

import pytest

import gc
import os
import sys
import logging
import uuid
import shutil
from time import sleep
from threading import Thread

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath('tests.sandbox'))
SANDBOX_STORAGE_ROOTDIR = os.environ.get("TESTS_SANDBOX_STORAGE")
TOOLS_ROOTDIR = os.path.abspath('tools')

linux_only = pytest.mark.skipif('not sys.platform.startswith("linux")')
unix_only = pytest.mark.skipif('not sys.platform.startswith("linux") and not sys.platform.startswith("darwin")')

def skip_if_multicell(func):
    def wrapped_func(self, *args, **kwargs):
        if hasattr(self, "NUM_SECONDARY_MASTER_CELLS") and self.NUM_SECONDARY_MASTER_CELLS > 0:
            pytest.skip("This test does not support multicell mode")
        func(self, *args, **kwargs)
    return wrapped_func

def resolve_test_paths(name):
    path_to_sandbox = os.path.join(SANDBOX_ROOTDIR, name)
    path_to_environment = os.path.join(path_to_sandbox, 'run')
    return path_to_sandbox, path_to_environment

def _wait(predicate):
    for _ in xrange(100):
        if predicate():
            return
        sleep(1.0)
    pytest.fail("wait failed")

def _pytest_finalize_func(environment, process_call_args):
    print >>sys.stderr, 'Process run by command "{0}" is dead!'.format(" ".join(process_call_args))
    environment.clear_environment()

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
            self._check_function()
            sleep(1.0)

    def stop(self):
        self._active = False
        self.join()

class YTEnvSetup(YTEnv):
    @classmethod
    def setup_class(cls, test_name=None):
        logging.basicConfig(level=logging.INFO)

        if test_name is None:
            test_name = cls.__name__
        cls.test_name = test_name
        path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        # Should create before env start for correct behaviour of teardown.
        cls.liveness_checker = None

        cls.path_to_test = path_to_test
        # For running in parallel
        cls.run_id = "run_" + uuid.uuid4().hex[:8]
        cls.path_to_run = os.path.join(path_to_test, cls.run_id)
        pids_filename = os.path.join(cls.path_to_run, "pids.txt")

        cls.Env = cls()
        cls.Env.start(cls.path_to_run, pids_filename, kill_child_processes=True,
                      port_locks_path=os.path.join(SANDBOX_ROOTDIR, "ports"))

        if cls.Env.configs["driver"]:
            yt_commands.init_driver(cls.Env.configs["driver"])
            yt_commands.is_multicell = (cls.Env.NUM_SECONDARY_MASTER_CELLS > 0)
            yt_driver_bindings.configure_logging(cls.Env.driver_logging_config)

        # To avoid strange hangups.
        if cls.NUM_MASTERS > 0:
            cls.liveness_checker = Checker(lambda: cls.Env.check_liveness(callback_func=_pytest_finalize_func))
            cls.liveness_checker.daemon = True
            cls.liveness_checker.start()

    @classmethod
    def teardown_class(cls):
        if cls.liveness_checker is not None:
            cls.liveness_checker.stop()

        cls.Env.clear_environment()
        yt_commands.driver = None
        gc.collect()

        if SANDBOX_STORAGE_ROOTDIR is not None and os.path.exists(cls.path_to_run):
            makedirp(SANDBOX_STORAGE_ROOTDIR)

            destination_path = os.path.join(SANDBOX_STORAGE_ROOTDIR, cls.test_name, cls.run_id)
            if os.path.exists(destination_path):
                shutil.rmtree(destination_path)

            shutil.move(cls.path_to_run, destination_path)

    def setup_method(self, method):
        if self.Env.NUM_MASTERS > 0:
            self.transactions_at_start = set(yt_commands.get_transactions())
            self.wait_for_nodes()

    def teardown_method(self, method):
        self.Env.check_liveness(callback_func=_pytest_finalize_func)
        if self.Env.NUM_MASTERS > 0:
            for tx in yt_commands.ls("//sys/transactions", attributes=["title"]):
                title = tx.attributes.get("title", "")
                if "Scheduler lock" in title or "Lease for node" in title:
                    continue
                try:
                    yt_commands.abort_transaction(tx)
                except:
                    pass

            yt_commands.set('//tmp', {})
            yt_commands.gc_collect()
            yt_commands.clear_metadata_caches()

            self._unban_nodes()
            self._remove_accounts()
            self._remove_users()
            self._remove_groups()
            self._remove_tablet_cells()
            self._remove_racks()

            yt_commands.gc_collect()

    def set_node_banned(self, address, flag):
        yt_commands.set("//sys/nodes/%s/@banned" % address, flag)
        # Give it enough time to register or unregister the node
        sleep(1.0)
        if flag:
            assert yt_commands.get("//sys/nodes/%s/@state" % address) == "offline"
            print "Node %s is banned" % address
        else:
            assert yt_commands.get("//sys/nodes/%s/@state" % address) == "online"
            print "Node %s is unbanned" % address

    def wait_for_nodes(self):
        _wait(lambda: all(n.attributes["state"] == "online" for n in yt_commands.ls("//sys/nodes", attributes=["state"])))

    def wait_for_cells(self):
        print "Waiting for tablet cells to become healthy..."
        _wait(lambda: all(c.attributes["health"] == "good" for c in yt_commands.ls("//sys/tablet_cells", attributes=["health"])))

    def sync_create_cells(self, size, count):
        for _ in xrange(count):
            yt_commands.create_tablet_cell(size)
        self.wait_for_cells()

    def wait_for_tablet_state(self, path, states):
        print "Waiting for tablets to become %s..." % ", ".join(str(state) for state in states)
        _wait(lambda: all(any(x["state"] == state for state in states) for x in yt_commands.get(path + "/@tablets")))

    def wait_until_sealed(self, path):
        _wait(lambda: yt_commands.get(path + "/@sealed"))

    def _wait_for_tablets(self, path, state, **kwargs):
        tablet_count = yt_commands.get(path + '/@tablet_count')
        first_tablet_index = kwargs.get("first_tablet_index", 0)
        last_tablet_index = kwargs.get("last_tablet_index", tablet_count - 1)
        _wait(lambda: all(x["state"] == state for x in yt_commands.get(path + "/@tablets")[first_tablet_index:last_tablet_index + 1]))

    def sync_mount_table(self, path, **kwargs):
        yt_commands.mount_table(path, **kwargs)

        print "Waiting for tablets to become mounted..."
        self._wait_for_tablets(path, "mounted", **kwargs)

    def sync_unmount_table(self, path, **kwargs):
        yt_commands.unmount_table(path, **kwargs)

        print "Waiting for tablets to become unmounted..."
        self._wait_for_tablets(path, "unmounted", **kwargs)

    def sync_compact_table(self, path):
        self.sync_unmount_table(path)
        yt_commands.set(path + "/@forced_compaction_revision", yt_commands.get(path + "/@revision"))
        self.sync_mount_table(path)

        print "Waiting for tablets to become compacted..."
        _wait(lambda: all(x["statistics"]["chunk_count"] == 1 for x in yt_commands.get(path + "/@tablets")))

    def _abort_transactions(self, txs):
        for tx in txs:
            try:
                yt_commands.abort_transaction(tx)
            except:
                pass

    def _unban_nodes(self):
        nodes = yt_commands.ls("//sys/nodes", attributes=["banned"])
        for node in nodes:
            if node.attributes["banned"]:
                yt_commands.set("//sys/nodes/%s/@banned" % str(node), False)

    def _remove_accounts(self):
        accounts = yt_commands.ls('//sys/accounts', attributes=['builtin'])
        for account in accounts:
            if not account.attributes['builtin']:
                yt_commands.remove_account(str(account))

    def _remove_users(self):
        users = yt_commands.ls('//sys/users', attributes=['builtin'])
        for user in users:
            if not user.attributes['builtin']:
                yt_commands.remove_user(str(user))

    def _remove_groups(self):
        groups = yt_commands.ls('//sys/groups', attributes=['builtin'])
        for group in groups:
            if not group.attributes['builtin']:
                yt_commands.remove_group(str(group))

    def _remove_tablet_cells(self):
        cells = yt_commands.get_tablet_cells()
        for id in cells:
            yt_commands.remove_tablet_cell(id)

    def _remove_racks(self):
        racks = yt_commands.get_racks()
        for rack in racks:
            yt_commands.remove_rack(rack)

    def _find_ut_file(self, file_name):
        from distutils.spawn import find_executable
        ytserver_path = find_executable("ytserver")
        assert ytserver_path is not None
        unittests_path = os.path.join(os.path.dirname(ytserver_path), "..", "yt", "unittests")
        assert os.path.exists(unittests_path)
        result_path = os.path.join(unittests_path, file_name)
        assert os.path.exists(result_path)
        return result_path
