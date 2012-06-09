
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time

class TestTxCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    def test_simple(self):

        tx_id = start_transaction()
        
        #check that transaction is on the master (also within a tx)
        assertItemsEqual(get_transactions(), [tx_id])
        assertItemsEqual(get_transactions(tx = tx_id), [tx_id])

        commit_transaction(tx = tx_id)
        #check that transaction no longer exists
        assertItemsEqual(get_transactions(), [])

        #couldn't commit commited transaction
        with pytest.raises(YTError): commit_transaction(tx = tx_id)
        #could (!) abort commited transaction
        abort_transaction(tx = tx_id)

        ##############################################################3
        #check the same for abort
        tx_id = start_transaction()

        assertItemsEqual(get_transactions(), [tx_id])
        assertItemsEqual(get_transactions(tx = tx_id), [tx_id])
        
        abort_transaction(tx = tx_id)
        #check that transaction no longer exists
        assertItemsEqual(get_transactions(), [])

        #couldn't commit aborted transaction
        with pytest.raises(YTError): commit_transaction(tx = tx_id)
        #could (!) abort aborted transaction
        abort_transaction(tx = tx_id)

    def test_changes_inside_tx(self):
        set('//value', '42')

        tx_id = start_transaction()
        set('//value', '100', tx = tx_id)
        assert get('//value', tx = tx_id) == '100'
        assert get('//value') == '42'
        commit_transaction(tx = tx_id)
        assert get('//value') == '100'

        tx_id = start_transaction()
        set('//value', '100500', tx = tx_id)
        abort_transaction(tx = tx_id)
        assert get('//value') == '100'

    def test_timeout(self):
        tx_id = start_transaction(opts = 'timeout=4000')

        # check that transaction is still alive after 2 seconds
        time.sleep(2)
        assertItemsEqual(get_transactions(), [tx_id])

        # check that transaction is expired after 4 seconds
        time.sleep(2)
        assertItemsEqual(get_transactions(), [])

    def test_renew(self):
        tx_id = start_transaction(opts = 'timeout=4000')

        time.sleep(2)
        assertItemsEqual(get_transactions(), [tx_id])
        renew_transaction(tx = tx_id)

        time.sleep(3)
        assertItemsEqual(get_transactions(), [tx_id])
        
        abort_transaction(tx = tx_id)