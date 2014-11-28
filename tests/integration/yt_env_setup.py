import yt_commands

from yt.environment import YTEnv
import yt.bindings.driver

import gc
import os
import logging
import shutil
import uuid
from datetime import datetime

from functools import wraps

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath('tests.sandbox'))
TOOLS_ROOTDIR = os.path.abspath('tools')

def resolve_test_paths(name):
    path_to_sandbox = os.path.join(SANDBOX_ROOTDIR, name)
    path_to_environment = os.path.join(path_to_sandbox, 'run')
    return path_to_sandbox, path_to_environment

def _working_dir(test_name):
    path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)
    return os.path.join(path_to_test, "run")

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
        cls.Env.set_environment(path_to_run, pids_filename)

        if cls.Env.configs['driver']:
            yt_commands.init_driver(cls.Env.configs['driver'])
            yt.bindings.driver.configure_logging(cls.Env.driver_logging_config)
            yt.bindings.driver.configure_tracing(cls.Env.driver_tracing_config)

    @classmethod
    def teardown_class(cls):
        cls.Env.clear_environment()
        gc.collect()

    def setup_method(self, method):
        if self.Env.NUM_MASTERS > 0:
            self.transactions_at_start = set(yt_commands.get_transactions())

    def teardown_method(self, method):
        self.Env.check_liveness()
        if self.Env.NUM_MASTERS > 0:
            current_txs = set(yt_commands.get_transactions())
            txs_to_abort = current_txs.difference(self.transactions_at_start)
            self._abort_transactions(list(txs_to_abort))

            yt_commands.set('//tmp', {})

            accounts = yt_commands.get_accounts()
            self._remove_accounts(accounts)

            users = yt_commands.get_users()
            self._remove_users(users)

            groups = yt_commands.get_groups()
            self._remove_groups(groups)

            tablet_cells = yt_commands.get_tablet_cells()
            self._remove_tablet_cells(tablet_cells)

            racks = yt_commands.get_racks()
            self._remove_racks(racks)

    def _abort_transactions(self, txs):
        for tx in txs:
            try:
                yt_commands.abort_transaction(tx)
            except:
                pass

    def _remove_accounts(self, accounts):
        for account in accounts:
            if account != 'sys' and account != 'tmp' and account != 'intermediate':
                yt_commands.remove_account(account)

    def _remove_users(self, users):
        for user in users:
            if user != 'root' and user != 'guest':
                yt_commands.remove_user(user)

    def _remove_groups(self, groups):
        for group in groups:
            if group != 'everyone' and group != 'users' and group != 'superusers':
                yt_commands.remove_group(group)
    
    def _remove_tablet_cells(self, cells):
        for id in cells:
            yt_commands.remove_tablet_cell(id)

    def _remove_racks(self, racks):
        for rack in racks:
            yt_commands.remove_rack(rack)

# decorator form
ATTRS = [
    'NUM_MASTERS',
    'NUM_NODES',
    'NUM_SCHEDULERS',
    'DELTA_MASTER_CONFIG',
    'DELTA_NODE_CONFIG',
    'DELTA_SCHEDULER_CONFIG']

def ytenv(**attrs):
    def make_decorator(f):
        @wraps(f)
        def wrapped(*args, **kw):
            env = YTEnv()
            for i in ATTRS:
                if i in attrs:
                    setattr(env, i, attrs.get(i))
            working_dir = _working_dir(f.__name__)
            env.setUp(working_dir)
            f(*args, **kw)
            env.tearDown()
        return wrapped
    return make_decorator
