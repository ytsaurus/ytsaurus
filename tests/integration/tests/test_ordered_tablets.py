import pytest

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from yt.environment.helpers import assert_items_equal

##################################################################

class TestOrderedTablets(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 16
    NUM_SCHEDULERS = 0

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout" : 1000,
            "peer_revocation_timeout" : 3000
        }
    }

    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 2
    }
    
    def _create_simple_table(self, path):
        create("table", path,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "double"},
                    {"name": "c", "type": "string"}]
            })


    def test_mount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert not "pivot_key" in tablet
        tablet_id = tablet["tablet_id"]
        cell_id = tablet["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]

    def test_unmount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        self.sync_mount_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_insert(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

    def test_flush(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@row_count") == 100

    def test_insert_with_explicit_tablet_index(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", 10)
        self.sync_mount_table("//tmp/t")

        for i in xrange(10):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
            
        for i in xrange(10):
            assert select_rows("a from [//tmp/t] where [$tablet_index] = " + str(i)) == [{"a": i}]
    
    def _test_select_from_single_tablet(self, dynamic):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        query_rows = [{"$tablet_index": 0, "$row_index": i, "a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", write_rows)

        if not dynamic:
            self.sync_unmount_table("//tmp/t")
            assert get("//tmp/t/@chunk_count") == 1
            self.sync_mount_table("//tmp/t")

        assert select_rows("* from [//tmp/t]") == query_rows
        assert select_rows("* from [//tmp/t] where [$row_index] between 10 and 20") == query_rows[10:21]
        assert select_rows("* from [//tmp/t] where [$tablet_index] in (-10, 20)") == []
        assert select_rows("a from [//tmp/t]") == [{"a": a} for a in xrange(100)]
        assert select_rows("a + 1 as aa from [//tmp/t] where a < 10") == [{"aa": a} for a in xrange(1, 11)]

    def test_select_from_dynamic_single_tablet(self):
        self._test_select_from_single_tablet(dynamic=True)

    def test_select_from_chunk_single_tablet(self):
        self._test_select_from_single_tablet(dynamic=False)

    def test_select_from_dynamic_multi_tablet(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", 10)
        self.sync_mount_table("//tmp/t")
        assert get("//tmp/t/@tablet_count") == 10

        for i in xrange(10):
            rows = [{"a": j} for j in xrange(100)]
            insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("a from [//tmp/t]"), [{"a": j} for i in xrange(10) for j in xrange(100)])

    def test_select_from_multi_store(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        for k in xrange(5):
            write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
            insert_rows("//tmp/t", write_rows)
            if k < 4:
                self.sync_unmount_table("//tmp/t")
                assert get("//tmp/t/@chunk_count") == k + 1
                self.sync_mount_table("//tmp/t")

        query_rows = [{"$tablet_index": 0, "$row_index": i, "a": i % 100} for i in xrange(10, 490)]
        assert select_rows("[$tablet_index], [$row_index], a from [//tmp/t] where [$row_index] between 10 and 489") == query_rows
        
    def test_select_with_limits(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", write_rows)

        query_rows = [{"a": i} for i in xrange(100)]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10") == query_rows[10:]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] > 10") == query_rows[11:]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] = 10") == query_rows[10:11]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] < 10") == query_rows[:10]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] <= 10") == query_rows[:11]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10 and [$row_index] < 20") == query_rows[10:20]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10 and [$row_index] <= 20") == query_rows[10:21]

##################################################################

class TestOrderedTabletsMulticell(TestOrderedTablets):
    NUM_SECONDARY_MASTER_CELLS = 2
