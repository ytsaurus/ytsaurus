import pytest

from yt_env_setup import YTEnvSetup
from yt_env_util import *

import yson_parser
import yson

import time
import os

def expect_error(result):
    stdout, stderr, exitcode = result
    print stderr
    assert exitcode != 0
    return stderr

def expect_ok(result):
    stdout, stderr, exitcode = result
    print stdout
    assert exitcode == 0
    return stdout

def lock(path, **kw): return command('lock', path, **kw)
def get(path, **kw): return command('get', path, **kw)
def remove(path, **kw): return command('remove', path, **kw)
def set(path, value, **kw): return command('set', path, value, **kw)
def ls(path, **kw): return command('list', path, **kw)

def create(object_type, path, **kw): return command('create', object_type, path, **kw)
def read(path, **kw): return command('read', path, **kw)
def write(path, value, **kw): return command('write', path, value, **kw)

def start_transaction(**kw):
    raw_tx = expect_ok(command('start_tx', **kw))
    tx_id = raw_tx.replace('"', '').strip('\n')
    return tx_id

def commit_transaction(**kw): return command('commit_tx', **kw)
def renew_transaction(**kw): return command('renew_tx', **kw)
def abort_transaction(**kw): return command('abort_tx', **kw)

def upload(path, **kw): return command('upload', path, **kw)

#########################################

#helpers:

def assert_eq(result, expected):
    actual = expect_ok(result).strip('\n')
    assert actual == expected

def get_transactions(**kw):
    yson_map = expect_ok(get('//sys/transactions', **kw)).strip('\n')
    return yson_parser.parse_string(yson_map)

def sort_list(value):
    result = yson_parser.parse_string(value)
    result.sort()
    return yson.dumps(result)


#########################################

class TestCypressCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 0

    def test_invalid_cases(self):
        # path not starting with /
        expect_error( set('a', '20'))

        # path starting with single /
        expect_error( set('/a', '20'))

        # empty path
        expect_error( set('', '20'))

        # empty token in path
        expect_error( set('//a//b', '30'))

        # change the type of root
        expect_error( set('/', '[]'))

        # set the root to the empty map
        # expect_error( set('/', '{}'))

        # remove the root
        expect_error( remove('/'))       
        # get non existent child
        expect_error( get('//b'))

        # remove non existent child
        expect_error( remove('//b'))

    def test_attributes(self):
        expect_ok( set('//t', '<attr=100;mode=rw> {nodes=[1; 2]}'))
        assert_eq( get('//t/@attr'), '100')
        assert_eq( get('//t/@mode'), '"rw"')

        expect_ok( remove('//t/@'))
        expect_error( get('//t/@attr'))
        expect_error( get('//t/@mode'))

        # changing attributes
        expect_ok( set('//t/a', '< author=ignat > []'))
        assert_eq( get('//t/a'), '[]')
        assert_eq( get('//t/a/@author'), '"ignat"')

        expect_ok( set('//t/a/@author', '"not_ignat"'))
        assert_eq( get('//t/a/@author'), '"not_ignat"')

        #nested attributes (actually shows <>)
        expect_ok( set('//t/b', '<dir = <file = <>-100> #> []'))
        assert_eq( get('//t/b/@dir/@'), '{"file"=<>-100}') 
        assert_eq( get('//t/b/@dir/@file'), '<>-100')
        assert_eq( get('//t/b/@dir/@file/@'), '{}')

class TestTxCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    def test_simple(self):

        tx_id = start_transaction()
        
        #check that transaction is on the master (also within a tx)
        assert get_transactions() == {tx_id: None}
        assert get_transactions(tx = tx_id) == {tx_id: None}

        expect_ok( commit_transaction(tx = tx_id))
        #check that transaction no longer exists
        assert get_transactions() == {}

        #couldn't commit commited transaction
        expect_error( commit_transaction(tx = tx_id))
        #could (!) abort commited transaction
        expect_ok( abort_transaction(tx = tx_id))

        ##############################################################3
        #check the same for abort
        tx_id = start_transaction()

        assert get_transactions() == {tx_id: None}
        assert get_transactions(tx = tx_id) == {tx_id: None}
        
        expect_ok( abort_transaction(tx = tx_id))
        #check that transaction no longer exists
        assert get_transactions() == {}

        #couldn't commit aborted transaction
        expect_error( commit_transaction(tx = tx_id))
        #could (!) abort aborted transaction
        expect_ok( abort_transaction(tx = tx_id))

    def test_changes_inside_tx(self):
        expect_ok(set('//value', '42'))

        tx_id = start_transaction()
        expect_ok( set('//value', '100', tx = tx_id))
        assert_eq( get('//value',        tx = tx_id), '100')
        assert_eq( get('//value'), '42')
        commit_transaction(tx = tx_id)
        assert_eq( get('//value'), '100')

        tx_id = start_transaction()
        expect_ok( set('//value', '100500', tx = tx_id))
        abort_transaction(tx = tx_id)
        assert_eq( get('//value'), '100')

    def test_timeout(self):
        tx_id = start_transaction(opts = 'timeout=4000')

        # check that transaction is still alive after 2 seconds
        time.sleep(2)
        assert get_transactions() == {tx_id: None}

        # check that transaction is expired after 4 seconds
        time.sleep(2)
        assert get_transactions() == {}

    def test_renew(self):
        tx_id = start_transaction(opts = 'timeout=4000')

        time.sleep(2)
        assert get_transactions() == {tx_id: None}
        expect_ok( renew_transaction(tx = tx_id))

        time.sleep(2)
        assert get_transactions() == {tx_id: None}
        
        abort_transaction(tx = tx_id)


class TestLockCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    #TODO(panin): check error messages
    def test_invalid_cases(self):
        # outside of transaction
        expect_error( lock('/'))

        # at non-existsing node
        tx_id = start_transaction()
        expect_error( lock('//non_existent', tx = tx_id))

        # error while parsing mode
        expect_error( lock('/', mode = 'invalid', tx = tx_id))

        #taking None lock is forbidden
        expect_error( lock('/', mode = 'None', tx = tx_id))

        # attributes do not have @lock_mode
        expect_ok( set('//value', '<attr=some> 42', tx = tx_id))
        expect_error( lock('//value/@attr/@lock_mode', tx = tx_id))
       
        expect_ok( abort_transaction(tx = tx_id))

    def test_display_locks(self):
        tx_id = start_transaction()
        
        expect_ok( set('//map', '{list = <attr=some> [1; 2; 3]}', tx = tx_id))

        # check that lock is set on nested nodes
        assert_eq( get('//map/@lock_mode',        tx = tx_id), '"exclusive"')
        assert_eq( get('//map/list/@lock_mode',   tx = tx_id), '"exclusive"')
        assert_eq( get('//map/list/0/@lock_mode', tx = tx_id), '"exclusive"')

        abort_transaction(tx = tx_id)


    def test_shared_locks_on_different_types(self):

        types_to_check = """
        string_node
        integer_node
        double_node
        map_node
        list_node

        file
        table
        chunk_map
        lost_chunk_map
        overreplicated_chunk_map
        underreplicated_chunk_map
        chunk_list_map
        transaction_map
        node_map
        lock_map
        holder
        holder_map
        orchid
        """.split()

        # check creation of different types and shared locks on them
        for object_type in types_to_check:
            tx_id = start_transaction()
            if object_type != "file":
                expect_ok( create(object_type, '//some', tx = tx_id))
            else:
                #file can't be created via create
                expect_error( create(object_type, '//some', tx = tx_id))
            
            if object_type != "table":
                expect_error( lock('//some', mode = 'shared', tx = tx_id))
            else:
                # shared locks are available only on tables 
                expect_ok( lock('//some', mode = 'shared', tx = tx_id))

            expect_ok( abort_transaction(tx = tx_id))
    
    @pytest.mark.xfail(run = False, reason = 'Issue #196')
    def test_snapshot_lock(self):
        expect_ok(set('//node', '42'))
        
        tx_id = start_transaction()
        expect_ok(lock('//node', mode = 'snapshot', tx = tx_id))
        
        expect_ok(set('//node', '100'))
        # check that node under snapshot lock wasn't changed
        assert_eq(get('//node', tx = tx_id), '42')

        expect_ok(remove('//node'))
        # check that node under snapshot lock still exist
        assert_eq(get('//node', tx = tx_id), '42')
        
        expect_ok(abort_transaction(tx = tx_id))

    @pytest.mark.xfail(run = False, reason = 'Switched off before choosing the right semantics of recursive locks')
    def test_lock_combinations(self):

        expect_ok( set('//a', '{}'))
        expect_ok( set('//a/b', '{}'))
        expect_ok( set('//a/b/c', '42'))

        tx1 = start_transaction()
        tx2 = start_transaction()

        expect_ok( lock('//a/b', tx = tx1))

        # now taking lock for any element in //a/b/c cause en error
        expect_error( lock('//a', tx = tx2))
        expect_error( lock('//a/b', tx = tx2))
        expect_error( lock('//a/b/c', tx = tx2))

        expect_ok( abort_transaction(tx = tx1))
        expect_ok( abort_transaction(tx = tx2))

        expect_ok(remove('//a'))


class TestTableCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5

    # test that chunks are not available from chunk_lists
    # Issue #198
    def test_chunk_ids(self):
        expect_ok(create('table', '//t'))
        expect_ok(write('//t', '{a=10}'))

        result = expect_ok(ls('//sys/chunks'))
        chunk_id = yson_parser.parse_string(result)[0]

        expect_error(get('//sys/chunk_lists/"' + chunk_id + '"'))
        expect_ok(remove('//t'))


    def test_simple(self):
        expect_ok( create('table', '//table'))

        assert_eq( read('//table'), '')
        assert_eq( get('//table/@row_count'), '0')

        expect_ok( write('//table', '[{b="hello"}]'))
        assert_eq( read('//table'), '{"b"="hello"};')
        assert_eq( get('//table/@row_count'), '1')

        expect_ok( write('//table', '[{b="2";a="1"};{x="10";y="20";a="30"}]'))
        assert_eq( read('//table'), '{"b"="hello"};\n{"a"="1";"b"="2"};\n{"a"="30";"x"="10";"y"="20"};')
        assert_eq( get('//table/@row_count'), '3')

        expect_ok( remove('//table'))

    def test_invalid_cases(self):
        # we can write only list or maps
        expect_ok( create('table', '//table'))

        expect_error( write('//table', 'string'))
        expect_error( write('//table', '100'))
        expect_error( write('//table', '3.14'))
        expect_error( write('//table', '<>'))

        expect_ok( remove('//table'))


#TODO(panin): tests of scheduler
class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5

    def test_on_masters(self):
        result = expect_ok( ls('//sys/masters'))
        masters = yson_parser.parse_string(result)
        assert len(masters) == self.NUM_MASTERS

        q = '"'
        for master in masters:
            path_to_orchid = '//sys/masters/'  + q + master + q + '/orchid'
            path = path_to_orchid + '/value'

            assert_eq( get(path_to_orchid + '/@service_name'), '"master"')

            some_map = '{"a"=1;"b"=2}'

            expect_ok( set(path, some_map))
            assert_eq( get(path), some_map)
            result = expect_ok( ls(path))
            assert sort_list(result) == '["a";"b";]'
            expect_ok( remove(path))
            expect_error( get(path))


    def test_on_holders(self):
        result = expect_ok( ls('//sys/holders'))
        holders = yson_parser.parse_string(result)
        assert len(holders) == self.NUM_HOLDERS

        q = '"'
        for holder in holders:
            path_to_orchid = '//sys/holders/'  + q + holder + q + '/orchid'
            path = path_to_orchid + '/value'

            assert_eq( get(path_to_orchid + '/@service_name'), '"node"')

            some_map = '{"a"=1;"b"=2}'

            expect_ok( set(path, some_map))
            assert_eq( get(path), some_map)
            result = expect_ok( ls(path))
            assert sort_list(result) == '["a";"b";]'
            expect_ok( remove(path))
            expect_error( get(path))


class TestCanceledUpload(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 3

    DELTA_HOLDER_CONFIG = {'chunk_holder' : {'session_timeout': 100}}

    # should be called on empty holders
    #@pytest.mark.xfail(run = False, reason = 'Replace blocking read from empty stream with something else')
    def test(self):
        tx_id = start_transaction(opts = 'timeout=2000')

        # uploading from empty stream will fail
        process = run_command('upload', '//file', tx = tx_id)
        time.sleep(1)
        process.kill()
        time.sleep(1)

        # now check that there are no temp files
        for i in xrange(self.NUM_HOLDERS):
            # TODO(panin): refactor
            holder_config = self.Env.configs['holder'][i]
            chunk_store_path = holder_config['chunk_holder']['store_locations'][0]['path']
            self._check_no_temp_file(chunk_store_path)

    def _check_no_temp_file(self, chunk_store):
        for root, dirs, files in os.walk(chunk_store):
            for file in files:
                assert not file.endswith('~'), 'Found temporary file: ' + file  
