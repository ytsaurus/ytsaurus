import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from time import sleep

##################################################################

class TestTransactions(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3

    def test_simple1(self):
        tx = start_transaction()
        
        assert exists('//sys/transactions/' + tx)

        commit_transaction(tx)

        assert not exists('//sys/transactions/' + tx)

        #cannot commit committed transaction
        with pytest.raises(YtError): commit_transaction(tx)

    def test_simple2(self):
        tx = start_transaction()

        assert exists('//sys/transactions/' + tx)
        
        abort_transaction(tx)
        
        assert not exists('//sys/transactions/' + tx)

        #cannot commit aborted transaction
        with pytest.raises(YtError): commit_transaction(tx)

    def test_changes_inside_tx(self):
        set('//tmp/value', '42')

        tx = start_transaction()
        set('//tmp/value', '100', tx = tx)

        # check that changes are not seen outside of transaction
        assert get('//tmp/value', tx = tx) == '100'
        assert get('//tmp/value') == '42'

        commit_transaction(tx)
        # changes after commit are applied
        assert get('//tmp/value') == '100'

        tx = start_transaction()
        set('//tmp/value', '100500', tx = tx)
        abort_transaction(tx)

        #changes after abort are not applied
        assert get('//tmp/value') == '100'

        remove('//tmp/value')

    def test_nested_tx1(self):
        set('//tmp/t1', 0)

        tx_outer = start_transaction()

        tx1 = start_transaction(tx = tx_outer)
        set('//tmp/t1', 1, tx=tx1)

        tx2 = start_transaction(tx = tx_outer)

        assert get('//tmp/t1', tx=tx_outer) == 0

        commit_transaction(tx1)
        assert get('//tmp/t1', tx=tx_outer) == 1
        assert get('//tmp/t1') == 0

    def test_nested_tx2(self):
        tx_outer = start_transaction()

        tx1 = start_transaction(tx = tx_outer)
        tx2 = start_transaction(tx = tx_outer)

        # can't be committed as long there are uncommitted transactions
        with pytest.raises(YtError): commit_transaction(tx_outer)

        sleep(1)

        # an attempt to commit tx_outer aborts everything
        assert not exists('//sys/transactions/' + tx_outer)
        assert not exists('//sys/transactions/' + tx1)
        assert not exists('//sys/transactions/' + tx2)

    def test_nested_tx3(self):
        tx_outer = start_transaction()

        tx1 = start_transaction(tx = tx_outer)
        tx2 = start_transaction(tx = tx_outer)

        # can be aborted..
        abort_transaction(tx_outer)
        
        # and this aborts all nested transactions
        assert not exists('//sys/transactions/' + tx_outer)
        assert not exists('//sys/transactions/' + tx1)
        assert not exists('//sys/transactions/' + tx2)

    def test_timeout(self):
        tx = start_transaction(opt = '/timeout=2000')

        # check that transaction is still alive after 2 seconds
        sleep(1.0)
        assert exists('//sys/transactions/' + tx)

        # check that transaction is expired after 4 seconds
        sleep(1.5)
        assert not exists('//sys/transactions/' + tx)

    def test_ping(self):
        tx = start_transaction(opt = '/timeout=2000')

        sleep(1)
        assert exists('//sys/transactions/' + tx)
        ping_transaction(tx)

        sleep(1.5)
        assert exists('//sys/transactions/' + tx)
        
    def test_expire_outer(self):
        tx_outer = start_transaction(opt = '/timeout=2000')
        tx_inner = start_transaction(tx = tx_outer)

        sleep(1)
        assert exists('//sys/transactions/' + tx_inner)
        assert exists('//sys/transactions/' + tx_outer)
        ping_transaction(tx_inner)

        sleep(1.5)
        # check that outer tx expired (and therefore inner was aborted)
        assert not exists('//sys/transactions/' + tx_inner)
        assert not exists('//sys/transactions/' + tx_outer)

    def test_ping_ancestors(self):
        tx_outer = start_transaction(opt = '/timeout=2000')
        tx_inner = start_transaction(tx = tx_outer)

        sleep(1)
        assert exists('//sys/transactions/' + tx_inner)
        assert exists('//sys/transactions/' + tx_outer)
        ping_transaction(tx_inner, ping_ancestor_txs=True)

        sleep(1.5)
        # check that all tx are still alive
        assert exists('//sys/transactions/' + tx_inner)
        assert exists('//sys/transactions/' + tx_outer)

    def test_tx_not_staged(self):
        tx_outer = start_transaction()
        tx_inner = start_transaction(tx = tx_outer)
        assert get('#' + tx_outer + '/@staged_object_ids') == []
        assert get('#' + tx_inner + '/@staged_object_ids') == []

    def test_transaction_maps(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx = tx1)
        tx3 = start_transaction(tx = tx1)

        txs = get_transactions()
        assert tx1 in txs
        assert tx2 in txs
        assert tx3 in txs
        topmost_txs = get_topmost_transactions()
        assert tx1 in topmost_txs
        assert not (tx2 in topmost_txs)
        assert not (tx3 in topmost_txs)

        abort_transaction(tx2)
        txs = get_transactions()
        assert tx1 in txs
        assert not (tx2 in txs)
        assert tx3 in txs
        topmost_txs = get_topmost_transactions()
        assert tx1 in topmost_txs
        assert not (tx2 in topmost_txs)
        assert not (tx3 in topmost_txs)

        abort_transaction(tx1)
        txs = get_transactions()
        assert not(tx1 in txs)
        assert not (tx2 in txs)
        assert not (tx3 in txs)
        topmost_txs = get_topmost_transactions()
        assert not (tx1 in topmost_txs)
        assert not (tx2 in topmost_txs)
        assert not (tx3 in topmost_txs)

    def test_revision1(self):
        set('//tmp/a', 'b')
        r1 = get('//tmp/a/@revision')
        
        set('//tmp/a2', 'b2')
        r2 = get('//tmp/a/@revision')
        assert r2 == r1

    def test_revision2(self):
        r1 = get('//tmp/@revision')
        
        set('//tmp/a', 'b')
        r2 = get('//tmp/@revision')
        assert r2 > r1

    def test_revision3(self):
        set('//tmp/a', 'b')
        r1 = get('//tmp/a/@revision')
        
        tx = start_transaction()
        
        set('//tmp/a', 'c', tx=tx)
        r2 = get('//tmp/a/@revision')
        r3 = get('//tmp/a/@revision', tx=tx)
        assert r2 == r1
        assert r3 > r1

        commit_transaction(tx)
        r4 = get('//tmp/a/@revision')
        assert r4 > r1
        assert r4 > r3

    def test_abort_snapshot_lock(self):
        create('file', '//tmp/file')
        upload('//tmp/file', 'some_data')

        tx = start_transaction()

        lock('//tmp/file', mode='snapshot', tx=tx)
        remove('//tmp/file')
        abort_transaction(tx)

    def test_commit_snapshot_lock(self):
        create('file', '//tmp/file')
        upload('//tmp/file', 'some_data')

        tx = start_transaction()

        lock('//tmp/file', mode='snapshot', tx=tx)
        remove('//tmp/file')
        commit_transaction(tx)
