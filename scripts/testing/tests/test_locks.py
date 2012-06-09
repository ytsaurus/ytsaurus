
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *


class TestLocks(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    #TODO(panin): check error messages
    def test_invalid_cases(self):
        # outside of transaction
        with pytest.raises(YTError): lock('/')

        # at non-existsing node
        tx_id = start_transaction()
        with pytest.raises(YTError): lock('//non_existent', tx = tx_id)

        # error while parsing mode
        with pytest.raises(YTError): lock('/', mode = 'invalid', tx = tx_id)

        #taking None lock is forbidden
        with pytest.raises(YTError): lock('/', mode = 'None', tx = tx_id)

        # attributes do not have @lock_mode
        set('//value', '<attr=some> 42', tx = tx_id)
        with pytest.raises(YTError): lock('//value/@attr/@lock_mode', tx = tx_id)
       
        abort_transaction(tx = tx_id)

    def test_display_locks(self):
        tx_id = start_transaction()
        
        set('//map', '{list = <attr=some> [1; 2; 3]}', tx = tx_id)

        # check that lock is set on nested nodes
        assert get('//map/@lock_mode', tx = tx_id) == '"exclusive"'
        assert get('//map/list/@lock_mode', tx = tx_id) == '"exclusive"'
        assert get('//map/list/0/@lock_mode', tx = tx_id) == '"exclusive"'

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
                create(object_type, '//some', tx = tx_id)
            else:
                #file can't be created via create
                with pytest.raises(YTError): create(object_type, '//some', tx = tx_id)
            
            if object_type != "table":
                with pytest.raises(YTError): lock('//some', mode = 'shared', tx = tx_id)
            else:
                # shared locks are available only on tables 
                lock('//some', mode = 'shared', tx = tx_id)

            abort_transaction(tx = tx_id)
    
    @pytest.mark.xfail(run = False, reason = 'Issue #196')
    def test_snapshot_lock(self):
        set('//node', '42')
        
        tx_id = start_transaction()
        lock('//node', mode = 'snapshot', tx = tx_id)
        
        set('//node', '100')
        # check that node under snapshot lock wasn't changed
        assert get('//node', tx = tx_id) == '42'

        remove('//node')
        # check that node under snapshot lock still exist
        assert get('//node', tx = tx_id) == '42'
        
        abort_transaction(tx = tx_id)

    @pytest.mark.xfail(run = False, reason = 'Switched off before choosing the right semantics of recursive locks')
    def test_lock_combinations(self):

        set('//a', '{}')
        set('//a/b', '{}')
        set('//a/b/c', '42')

        tx1 = start_transaction()
        tx2 = start_transaction()

        lock('//a/b', tx = tx1)

        # now taking lock for any element in //a/b/c cause en error
        with pytest.raises(YTError): lock('//a', tx = tx2)
        with pytest.raises(YTError): lock('//a/b', tx = tx2)
        with pytest.raises(YTError): lock('//a/b/c', tx = tx2)

        abort_transaction(tx = tx1)
        abort_transaction(tx = tx2)

        remove('//a')