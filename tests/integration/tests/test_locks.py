import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestLocks(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    #TODO(panin): check error messages
    def test_invalid_cases(self):
        # outside of transaction
        with pytest.raises(YTError): lock('/')

        # at non-existsing node
        tx = start_transaction()
        with pytest.raises(YTError): lock('//tmp/non_existent', tx = tx)

        # error while parsing mode
        with pytest.raises(YTError): lock('/', mode = 'invalid', tx = tx)

        #taking None lock is forbidden
        with pytest.raises(YTError): lock('/', mode = 'None', tx = tx)

        # attributes do not have @lock_mode
        set_str('//tmp/value', '<attr=some> 42', tx = tx)
        with pytest.raises(YTError): lock('//tmp/value/@attr/@lock_mode', tx = tx)
       
        abort_transaction(tx)

    def test_display_locks(self):
        tx = start_transaction()
        
        set_str('//tmp/map', '{list = <attr=some> [1; 2; 3]}', tx = tx)

        # check that lock is set on nested nodes
        assert get('//tmp/map/@lock_mode', tx = tx) == 'exclusive'
        assert get('//tmp/map/list/@lock_mode', tx = tx) == 'exclusive'
        assert get('//tmp/map/list/0/@lock_mode', tx = tx) == 'exclusive'

        abort_transaction(tx)


    @pytest.mark.xfail(run = False, reason = 'Issue #293')
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
            tx = start_transaction()
            if object_type != "file":
                create(object_type, '//tmp/some', tx = tx)
                assert get('//tmp/some/@type', tx = tx) == object_type
            else:
                #file can't be created via create
                with pytest.raises(YTError): create(object_type, '//tmp/some', tx = tx)
            
            if object_type != "table":
                with pytest.raises(YTError): lock('//tmp/some', mode = 'shared', tx = tx)
            else:
                # shared locks are available only on tables 
                lock('//tmp/some', mode = 'shared', tx = tx)

            abort_transaction(tx)

    def test_shared_lock_inside_tx(self):
        tx_outer = start_transaction()
        create('table', '//tmp/table', tx=tx_outer)

        tx_inner = start_transaction(tx=tx_outer)
        lock('//tmp/table', mode='shared', tx=tx_inner)
    
    def test_snapshot_lock(self):
        set('//tmp/node', 42)
        
        tx = start_transaction()
        lock('//tmp/node', mode = 'snapshot', tx = tx)
        
        set('//tmp/node', 100)
        # check that node under snapshot lock wasn't changed
        assert get('//tmp/node', tx = tx) == 42

        # can't change value under snapshot lock
        with pytest.raises(YTError): set('//tmp/node', 200, tx = tx)
        
        abort_transaction(tx)

    @pytest.mark.xfail(run = False, reason = 'Switched off before choosing the right semantics of recursive locks')
    def test_lock_combinations(self):

        set('//tmp/a', {})
        set('//tmp/a/b', {})
        set('//tmp/a/b/c', 42)

        tx1 = start_transaction()
        tx2 = start_transaction()

        lock('//tmp/a/b', tx = tx1)

        # now taking lock for any element in //tmp/a/b/c cause en error
        with pytest.raises(YTError): lock('//tmp/a', tx = tx2)
        with pytest.raises(YTError): lock('//tmp/a/b', tx = tx2)
        with pytest.raises(YTError): lock('//tmp/a/b/c', tx = tx2)

        abort_transaction(tx1)
        abort_transaction(tx2)

        remove('//tmp/a')

    def test_remove_map_subtree_lock(self):
        set('//tmp/a', {'b' : 1})
        tx = start_transaction()
        lock('//tmp/a/b', mode = 'exclusive', tx = tx);
        with pytest.raises(YTError): remove('//tmp/a')

    def test_remove_list_subtree_lock(self):
        set('//tmp/a', [1])
        tx = start_transaction()
        lock('//tmp/a/0', mode = 'exclusive', tx = tx);
        with pytest.raises(YTError): remove('//tmp/a')
