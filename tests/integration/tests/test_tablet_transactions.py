import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import YsonEntity

from time import sleep

from yt.environment.helpers import assert_items_equal

##################################################################

class TestTablets(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 0

    def _wait(self, predicate):
        while not predicate():
            sleep(1)

    def _create_table(self, path, atomicity="full"):
        create("table", path,
            attributes = {
                "schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "string"}],
                "key_columns": ["key"],
                "atomicity": atomicity
            })

    def _create_table_with_computed_column(self, path):
        create("table", path,
            attributes = {
                "schema": [
                    {"name": "key1", "type": "int64"},
                    {"name": "key2", "type": "int64", "expression": "key1 * 100 + 3"},
                    {"name": "value", "type": "string"}],
                "key_columns": ["key1", "key2"]
            })

    def _create_table_with_hash(self, path):
        create("table", path,
            attributes = {
                "schema": [
                    {"name": "hash", "type": "uint64", "expression": "farm_hash(key)"},
                    {"name": "key", "type": "int64"},
                    {"name": "value", "type": "string"}],
                "key_columns": ["hash", "key"]
            })

    def _get_tablet_leader_address(self, tablet_id):
        cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
        peers = get("//sys/tablet_cells/" + cell_id + "/@peers")
        leader_peer = list(x for x in peers if x["state"] == "leading")[0]
        return leader_peer["address"]

    def _find_tablet_orchid(self, address, tablet_id):
        cells = get("//sys/nodes/" + address + "/orchid/tablet_cells", ignore_opaque=True)
        for (cell_id, cell_data) in cells.iteritems():
            if cell_data["state"] == "leading":
                tablets = cell_data["tablets"]
                if tablet_id in tablets:
                    return tablets[tablet_id]
        return None

    def _get_pivot_keys(self, path):
        tablets = get(path + "/@tablets")
        return [tablet["pivot_key"] for tablet in tablets]

    def test_sticky_tablet_transactions(self):
        self._sync_create_cells(1, 1)
        self._create_table("//tmp/t")
        self._sync_mount_table("//tmp/t")

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

        commit_transaction(tx1)
        assert select_rows("* from [//tmp/t]") == _rows(0, 1)
        assert lookup_rows("//tmp/t", _keys(0, 1)) == _rows(0, 1)

        # cannot see unsynchronized transaction effects
        assert select_rows("* from [//tmp/t]", tx=tx2) == []
        assert lookup_rows("//tmp/t", _keys(0, 1), tx=tx2) == []

        # cannot commit transaction twice
        with pytest.raises(YtError): commit_transaction(tx1)

        # cannot commit conflicting transaction
        with pytest.raises(YtError): commit_transaction(tx2)

