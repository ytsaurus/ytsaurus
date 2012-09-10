import os
import logging

from functools import wraps

from yt.environment import YTEnv

import yt_commands

SANDBOX_ROOTDIR = os.path.abspath('tests.sandbox')
TOOLS_ROOTDIR = os.path.abspath('tools')
PIDS_FILENAME = os.path.join(SANDBOX_ROOTDIR, 'pids.txt')

def _working_dir(test_name):
    path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)
    return os.path.join(path_to_test, "run")

class YTEnvSetup(YTEnv):
    @classmethod
    def setup_class(cls):
        logging.basicConfig(level=logging.INFO)

        test_name = cls.__name__
        path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)

        os.system('rm -rf ' + path_to_test)
        os.makedirs(path_to_test)

        path_to_run = os.path.join(path_to_test, "run")

        cls.path_to_test = path_to_test
        ports = {
            "master": 28001,
            "node": 27001,
            "scheduler": 28101,
            "proxy": 28080}
        cls.Env = cls()
        cls.Env.set_environment(path_to_run, PIDS_FILENAME, ports)

    @classmethod
    def teardown_class(cls):
        cls.Env.clear_environment()

    def setup_method(self, method):
        path_to_test_case = os.path.join(self.path_to_test, method.__name__)

        os.makedirs(path_to_test_case)
        os.chdir(path_to_test_case)
        if self.Env.NUM_MASTERS > 0:
            self.transactions_at_start = set(yt_commands.get_transactions())
            yt_commands.set_str('//tmp', '{}')

    def teardown_method(self, method):
        if self.Env.NUM_MASTERS > 0:
            current_txs = set(yt_commands.get_transactions())
            txs_to_abort = current_txs.difference(self.transactions_at_start)
            self._abort_transactions(list(txs_to_abort))

    def _abort_transactions(self, tx_ids):
        if tx_ids:
            logging.info('Aborting {0} txs'.format(tx_ids))
        for tx in tx_ids:
            try:
                yt_commands.abort_transaction(tx)
            except:
                pass

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
