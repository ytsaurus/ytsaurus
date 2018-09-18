import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import YsonEntity

from time import sleep

from yt.environment.helpers import assert_items_equal

##################################################################

class TestTabletTransactions(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 0
    USE_DYNAMIC_TABLES = True

    def _create_table(self, path):
        create("table", path,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
            })

    def test_sticky_tablet_transactions(self):
        sync_create_cells(1)
        self._create_table("//tmp/t")
        sync_mount_table("//tmp/t")

        def _keys(i, j):
            return [{"key": x} for x in range(i, j)]

        def _rows(i, j):
            return [{"key": x, "value": str(x)} for x in xrange(i, j)]

        assert select_rows("* from [//tmp/t]") == []

        tx1 = start_transaction(type="tablet", sticky=True)
        insert_rows("//tmp/t", _rows(0, 1), tx=tx1)

        tx2 = start_transaction(type="tablet", sticky=True)
        delete_rows("//tmp/t", _keys(0, 1), tx=tx2)

        # cannot see transaction effects until not committed
        assert select_rows("* from [//tmp/t]") == []
        assert lookup_rows("//tmp/t", _keys(0, 1)) == []

        commit_transaction(tx1, sticky=True)
        assert select_rows("* from [//tmp/t]") == _rows(0, 1)
        assert lookup_rows("//tmp/t", _keys(0, 1)) == _rows(0, 1)

        # cannot see unsynchronized transaction effects
        assert select_rows("* from [//tmp/t]", tx=tx2) == []
        assert lookup_rows("//tmp/t", _keys(0, 1), tx=tx2) == []

        # cannot commit transaction twice
        with pytest.raises(YtError): commit_transaction(tx1, sticky=True)

        # cannot commit conflicting transaction
        with pytest.raises(YtError): commit_transaction(tx2, sticky=True)

