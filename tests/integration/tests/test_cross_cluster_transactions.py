import pytest

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *
from yt_helpers import *

from yt.environment.helpers import assert_items_equal

##################################################################

class TestCrossClusterTransactions(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 0
    NUM_REMOTE_CLUSTERS = 1
    USE_DYNAMIC_TABLES = True

    def setup(self):
        self.primary_driver = get_driver(cluster="primary")
        self.remote_driver = get_driver(cluster="remote_0")

    @authors("babenko")
    def test_tablet_transaction(self):
        sync_create_cells(1, driver=self.primary_driver)
        sync_create_cells(1, driver=self.remote_driver)

        TABLE_ATTRIBUTES = {
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ]
        }

        create("table", "//tmp/primary", attributes=TABLE_ATTRIBUTES, driver=self.primary_driver)
        sync_mount_table("//tmp/primary", driver=self.primary_driver)

        create("table", "//tmp/remote", attributes=TABLE_ATTRIBUTES, driver=self.remote_driver)
        sync_mount_table("//tmp/remote", driver=self.remote_driver)

        tx = start_transaction(type="tablet", driver=self.primary_driver)
        assert start_transaction(type="tablet", transaction_id_override=tx, driver=self.remote_driver) == tx

        PRIMARY_ROWS = [{"key": 1, "value": "hello"}]
        insert_rows("//tmp/primary", PRIMARY_ROWS, driver=self.primary_driver, tx=tx)

        REMOTE_ROWS = [{"key": 2, "value": "world"}]
        insert_rows("//tmp/remote", REMOTE_ROWS, driver=self.remote_driver, tx=tx)

        self.primary_driver.register_foreign_transaction(transaction_id=tx, foreign_driver=self.remote_driver)

        commit_transaction(tx)

        assert_items_equal(select_rows("* from [//tmp/primary]", driver=self.primary_driver), PRIMARY_ROWS)
        assert_items_equal(select_rows("* from [//tmp/remote]", driver=self.remote_driver), REMOTE_ROWS)

class TestCrossClusterTransactionsRpcProxy(TestCrossClusterTransactions):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
