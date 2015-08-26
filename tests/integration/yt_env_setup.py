import yt_commands

from yt.environment import YTEnv
import yt_driver_bindings

import gc
import os
import logging
import uuid
import pytest

from time import sleep

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath('tests.sandbox'))
TOOLS_ROOTDIR = os.path.abspath('tools')

linux_only = pytest.mark.skipif('not sys.platform.startswith("linux")')

mark_multicell = pytest.mark.skipif( \
    'os.environ.get("ENABLE_YT_MULTICELL_TESTS") != "1"',
    reason = "Multicell tests require explicit ENABLE_YT_MULTICELL_TESTS=1")

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

def _working_dir(test_name):
    path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)
    return os.path.join(path_to_test, "run")

def _wait(predicate):
    while not predicate():
        sleep(1.0)

def _pytest_finalize_func(environment, process_call_args):
    pytest.exit('Process run by command "{0}" is dead! Tests terminated.' \
                .format(" ".join(process_call_args)))
    environment.clear_environment(safe=False)

class YTEnvSetup(YTEnv):
    @classmethod
    def setup_class(cls, test_name=None):
        logging.basicConfig(level=logging.INFO)

        if test_name is None:
            test_name = cls.__name__
        path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        # For running parallel
        path_to_run = os.path.join(path_to_test, "run_" + str(uuid.uuid4().hex)[:8])
        pids_filename = os.path.join(path_to_run, 'pids.txt')

        cls.path_to_test = path_to_test
        cls.Env = cls()
        cls.Env.start(path_to_run, pids_filename)

        if cls.Env.configs['driver']:
            yt_commands.init_driver(cls.Env.configs['driver'])
            yt_commands.is_multicell = (cls.Env.NUM_SECONDARY_MASTER_CELLS > 0)
            yt_driver_bindings.configure_logging(cls.Env.driver_logging_config)

    @classmethod
    def teardown_class(cls):
        cls.Env.clear_environment()
        yt_commands.driver = None
        gc.collect()

    def setup_method(self, method):
        if self.Env.NUM_MASTERS > 0:
            self.transactions_at_start = set(yt_commands.get_transactions())
            self.wait_for_nodes()

    def teardown_method(self, method):
        self.Env.check_liveness(callback_func=_pytest_finalize_func)
        if self.Env.NUM_MASTERS > 0:
            current_txs = set(yt_commands.get_transactions())
            txs_to_abort = current_txs.difference(self.transactions_at_start)
            self._abort_transactions(list(txs_to_abort))

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

    def sync_mount_table(self, path):
        yt_commands.mount_table(path)

        print "Waiting for tablets to become mounted..."
        _wait(lambda: all(x["state"] == "mounted" for x in yt_commands.get(path + "/@tablets")))

    def sync_unmount_table(self, path):
        yt_commands.unmount_table(path)

        print "Waiting for tablets to become unmounted..."
        _wait(lambda: all(x["state"] == "unmounted" for x in yt_commands.get(path + "/@tablets")))

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

