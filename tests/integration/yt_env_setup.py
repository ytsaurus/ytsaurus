import yt_commands

from yt.environment import YTEnv

import os
import logging
import shutil
from datetime import datetime

from functools import wraps

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath('tests.sandbox'))
TOOLS_ROOTDIR = os.path.abspath('tools')

def _working_dir(test_name):
    path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)
    return os.path.join(path_to_test, "run")

class YTEnvSetup(YTEnv):
    @classmethod
    def setup_class(cls):
        logging.basicConfig(level=logging.INFO)

        test_name = cls.__name__
        path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        # For running parallel
        counter = 0
        while True:
            counter += 1
            path_to_run = os.path.join(path_to_test, "run" + str(counter))
            try:
                if os.path.exists(path_to_run):
                    age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(path_to_run)) 
                    if age.days > 0 or age.seconds > 60:
                        shutil.rmtree(path_to_run, ignore_errors=True)
                os.makedirs(path_to_run)
                break
            except OSError:
                pass

        pids_filename = os.path.join(path_to_run, 'pids.txt')

        cls.path_to_test = path_to_test
        try:
            import settings_local
            ports = settings_local.ports
        except Exception, e:
            ports = {
                "master": 29000,
                "node": 29100,
                "scheduler": 29200,
                "proxy": 29300}

        cls.Env = cls()
        cls.Env.set_environment(path_to_run, pids_filename, ports)

    @classmethod
    def teardown_class(cls):
        cls.Env.clear_environment()

    def setup_method(self, method):
        path_to_test_case = os.path.join(self.path_to_test, method.__name__)

        if os.path.exists(path_to_test_case):
            shutil.rmtree(path_to_test_case)
        os.makedirs(path_to_test_case)
        os.chdir(path_to_test_case)
        if self.Env.NUM_MASTERS > 0:
            self.transactions_at_start = set(yt_commands.get_transactions())

    def teardown_method(self, method):
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

            tablet_cells = yt_commands.ls('//sys/tablet_cells')
            self._remove_tablet_cells(tablet_cells)

    def _abort_transactions(self, txs):
        for tx in txs:
            try:
                yt_commands.abort_transaction(tx)
            except:
                pass

    def _remove_accounts(self, accounts):
        for account in accounts:
            if account != 'sys' and account != 'tmp':
                yt_commands.remove_account(account)

    def _remove_users(self, users):
        for user in users:
            if user != 'root' and user != 'guest':
                yt_commands.remove_user(user)

    def _remove_groups(self, groups):
        for group in groups:
            if group != 'everyone' and group != 'users':
                yt_commands.remove_group(group)
    
    def _remove_tablet_cells(self, cells):
        for id in cells:
            yt_commands.remove_tablet_cell(id)

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
