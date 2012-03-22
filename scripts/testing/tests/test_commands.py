from yt_env_setup import YTEnvSetup
from yt_env_util import *

import yson_parser

def expect_error(result):
    stdout, stderr, exitcode = result
    assert exitcode != 0
    print stderr
    return stderr

def expect_ok(result):
    stdout, stderr, exitcode = result
    assert exitcode == 0
    print stdout
    return stdout

def lock(path, **kw): return command('lock', path, **kw)
def get(path, **kw): return command('get', path, **kw)
def remove(path, **kw): return command('remove', path, **kw)
def set(path, value, **kw): return command('set', path, value, **kw)

def start_transaction():
    raw_tx = expect_ok(command('start_tx'))
    tx_id = raw_tx.replace('"', '').strip('\n')
    return tx_id

def commit_transaction(**kw): return command('commit_tx', **kw)
def abort_transaction(**kw): return command('abort_tx', **kw)

#########################################

#helpers:

def assert_eq(result, expected):
    actual = expect_ok(result).strip('\n')
    assert actual == expected

def get_transactions(**kw):
    yson_map = expect_ok(get('/sys/transactions/', **kw)).strip('\n')
    return yson_parser.parse_string(yson_map)

#########################################

class TestTxCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    def test_simple(self):
        tx_id = start_transaction()
        
        #check that transaction is on the master (also within a tx)
        assert get_transactions() == {tx_id: None}
        assert get_transactions(tx = tx_id) == {tx_id: None}
        
        commit_transaction(tx = tx_id)

        #check that transaction no longer exists
        assert get_transactions() == {}

        #check the same for abort
        tx_id = start_transaction()

        assert get_transactions() == {tx_id: None}
        assert get_transactions(tx = tx_id) == {tx_id: None}
        
        abort_transaction(tx = tx_id)
        assert get_transactions() == {}


class TestLockCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    #TODO(panin): check error messages
    def test_invalid_cases(self):
        # outside of transaction
        expect_error(lock('/'))

        # at non-existsing node
        tx_id = start_transaction()
        expect_error(lock('/non_existent', tx = tx_id))

        # error while parsing mode
        expect_error(lock('/', mode = 'invalid', tx = tx_id))

        #taking None lock is forbidden
        expect_error(lock('/', mode = 'None', tx = tx_id))

        # attributes do not have @lock_mode
        expect_ok(set('/value', '42<attr=some>', tx = tx_id))
        expect_error(lock('/value@attr@lock_mode', tx = tx_id))
       
        abort_transaction(tx = tx_id)

    def test_display_locks(self):
        tx_id = start_transaction()
        
        expect_ok(set('/map', '{list = [1; 2; 3] <attr=some>}', tx = tx_id))

        # check that lock is set on nested nodes
        assert_eq(get('/map@lock_mode', tx = tx_id), '"exclusive"')
        assert_eq(get('/map/list@lock_mode', tx = tx_id), '"exclusive"')
        assert_eq(get('/map/list/0@lock_mode', tx = tx_id), '"exclusive"')

        abort_transaction(tx = tx_id)

    def test_lock_combinations(self):
        expect_ok(set('/a/b/c', '42'))

        tx1 = start_transaction()
        tx2 = start_transaction()

        expect_ok(lock('/a/b', tx = tx1))

        # now taking lock for any element in /a/b/c case en error
        expect_error(lock('/a', tx = tx2))
        expect_error(lock('/a/b', tx = tx2))
        expect_error(lock('/a/b/c', tx = tx2))

        expect_ok(abort_transaction(tx = tx1))
        expect_ok(abort_transaction(tx = tx2))

        expect_ok(remove('/a'))
        #TODO(panin): shared locks


