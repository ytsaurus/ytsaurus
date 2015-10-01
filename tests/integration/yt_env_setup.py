import yt_commands

from yt.environment import YTEnv
import yt_driver_bindings

import gc
import os
import sys
import logging
import uuid

from time import sleep
from functools import wraps
import pytest

SANDBOX_ROOTDIR = os.environ.get("TESTS_SANDBOX", os.path.abspath('tests.sandbox'))
TOOLS_ROOTDIR = os.path.abspath('tools')

def resolve_test_paths(name):
    path_to_sandbox = os.path.join(SANDBOX_ROOTDIR, name)
    path_to_environment = os.path.join(path_to_sandbox, 'run')
    return path_to_sandbox, path_to_environment

def _working_dir(test_name):
    path_to_test = os.path.join(SANDBOX_ROOTDIR, test_name)
    return os.path.join(path_to_test, "run")

def _wait(predicate):
    while not predicate():
        sleep(1)

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
            yt_driver_bindings.configure_logging(cls.Env.driver_logging_config)

    @classmethod
    def teardown_class(cls):
        cls.Env.clear_environment()
        yt_commands.driver = None
        gc.collect()

    def setup_method(self, method):
        if self.Env.NUM_MASTERS > 0:
            self._wait_nodes()

    def teardown_method(self, method):
        self.Env.check_liveness(callback_func=_pytest_finalize_func)
        if self.Env.NUM_MASTERS > 0:
            for tx in yt_commands.get_transactions():
                try:
                    title = yt_commands.get("#{0}/@title".format(tx))
                    if "Scheduler lock" in title or "Lease for node" in title:
                        continue
                except:
                    pass
                try:
                    yt_commands.abort_transaction(tx)
                except:
                    pass

            yt_commands.set('//tmp', {})
            yt_commands.gc_collect()
            yt_commands.clear_metadata_caches()

            self._remove_accounts()
            self._remove_users()
            self._remove_groups()
            self._remove_tablet_cells()
            self._remove_racks()

            yt_commands.gc_collect()

    def _wait_nodes(self):
        for attempt in xrange(1, 100):
            if yt_commands.get("//sys/nodes/@offline"):
                sleep(0.1)
            else:
                break

    def _sync_create_cells(self, size, count):
        ids = []
        for _ in xrange(count):
            ids.append(yt_commands.create_tablet_cell(size))

        print "Waiting for tablet cells to become healthy..."
        _wait(lambda: all(yt_commands.get("//sys/tablet_cells/" + id + "/@health") == "good" for id in ids))

    def _wait_for_tablet_state(self, path, states):
        print "Waiting for tablets to become %s..." % ", ".join(str(state) for state in states)
        _wait(lambda: all(any(x["state"] == state for state in states) for x in yt_commands.get(path + "/@tablets")))

    def _sync_mount_table(self, path):
        yt_commands.mount_table(path)

        print "Waiting for tablets to become mounted..."
        _wait(lambda: all(x["state"] == "mounted" for x in yt_commands.get(path + "/@tablets")))

    def _sync_unmount_table(self, path):
        yt_commands.unmount_table(path)

        print "Waiting for tablets to become unmounted..."
        _wait(lambda: all(x["state"] == "unmounted" for x in yt_commands.get(path + "/@tablets")))

    def _remove_accounts(self):
        accounts = yt_commands.ls('//sys/accounts', attr=['builtin'])
        for account in accounts:
            if not account.attributes['builtin']:
                yt_commands.remove_account(str(account))

    def _remove_users(self):
        users = yt_commands.ls('//sys/users', attr=['builtin'])
        for user in users:
            if not user.attributes['builtin']:
                yt_commands.remove_user(str(user))

    def _remove_groups(self):
        groups = yt_commands.ls('//sys/groups', attr=['builtin'])
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
