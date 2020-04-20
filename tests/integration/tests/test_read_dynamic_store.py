import pytest
import __builtin__

from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import wait
from yt_commands import *
from yt.yson import YsonEntity, loads, dumps

from time import sleep
from random import randint, choice, sample
from string import ascii_lowercase

import random

from yt.environment.helpers import assert_items_equal

##################################################################

@authors("ifsmirnov")
class TestReadSortedDynamicTables(TestSortedDynamicTablesBase):
    NUM_SCHEDULERS = 1
    ENABLE_BULK_INSERT = True

    simple_rows = [{"key": i, "value": str(i)} for i in range(10)]

    def _prepare_simple_table(self, path):
        self._create_simple_table(path)
        set(path + "/@enable_dynamic_store_read", True)
        sync_mount_table(path)
        insert_rows(path, self.simple_rows[::2])
        sync_flush_table(path)
        insert_rows(path, self.simple_rows[1::2])

    def _validate_tablet_statistics(self, table):
        cell_id = ls("//sys/tablet_cells")[0]

        def check_statistics(statistics):
            return statistics["tablet_count"] == get(table + "/@tablet_count") and \
                   statistics["chunk_count"] == get(table + "/@chunk_count") and \
                   statistics["uncompressed_data_size"] == get(table + "/@uncompressed_data_size") and \
                   statistics["compressed_data_size"] == get(table + "/@compressed_data_size") and \
                   statistics["disk_space"] == get(table + "/@resource_usage/disk_space")

        tablet_statistics = get("//tmp/t/@tablet_statistics")
        assert check_statistics(tablet_statistics)

        wait(lambda: check_statistics(get("#{0}/@total_statistics".format(cell_id))))

    def _validate_cell_statistics(self, **kwargs):
        cell_id = ls("//sys/tablet_cells")[0]

        def _wait_func():
            statistics = get("#{}/@total_statistics".format(cell_id))
            for k, v in kwargs.iteritems():
                if statistics[k] != v:
                    return False
            return True

        wait(_wait_func)

    @authors("ifsmirnov")
    def test_basic_read1(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(3000)]

        insert_rows("//tmp/t", rows[:1500])
        assert read_table("//tmp/t") == rows[:1500]
        assert read_table("//tmp/t[100:500]") == rows[100:500]

        ts = generate_timestamp()
        ypath_with_ts = "<timestamp={}>//tmp/t".format(ts)

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        insert_rows("//tmp/t", rows[1500:])
        assert read_table("//tmp/t") == rows
        assert read_table(ypath_with_ts) == rows[:1500]

        sync_freeze_table("//tmp/t")
        assert read_table("//tmp/t") == rows
        assert read_table("//tmp/t", tx=tx) == rows
        assert read_table(ypath_with_ts, tx=tx) == rows[:1500]

        self._validate_tablet_statistics("//tmp/t")

    def test_basic_read2(self):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")

        assert read_table("//tmp/t") == self.simple_rows
        assert read_table("//tmp/t[2:5]") == self.simple_rows[2:5]
        assert read_table("//tmp/t{key}") == [{"key": r["key"]} for r in self.simple_rows]
        assert read_table("//tmp/t{value}") == [{"value": r["value"]} for r in self.simple_rows]

    def test_read_removed(self):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")

        id = get("//tmp/t/@id")

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)
        sync_unmount_table("//tmp/t")
        remove("//tmp/t")

        assert read_table("#" + id, tx=tx) == self.simple_rows

    @pytest.mark.parametrize("mode", ["sorted", "ordered", "unordered"])
    def test_merge(self, mode):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")

        create("table", "//tmp/p")
        merge(in_="//tmp/t", out="//tmp/p", mode=mode)

        if mode == "unordered":
            assert_items_equal(read_table("//tmp/p"), self.simple_rows)
        else:
            assert read_table("//tmp/p") == self.simple_rows

    def test_map(self):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")
        create("table", "//tmp/p")
        map(in_="//tmp/t", out="//tmp/p", command="cat", ordered=True)
        assert read_table("//tmp/p") == self.simple_rows

    def test_reduce(self):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")
        create("table", "//tmp/p")
        reduce(in_="//tmp/t", out="//tmp/p", command="cat", reduce_by=["key"])
        assert_items_equal(read_table("//tmp/p"), self.simple_rows)

    def test_sort(self):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")
        create("table", "//tmp/p")
        sort(in_="//tmp/t", out="//tmp/p", sort_by=["key"])
        assert read_table("//tmp/p") == self.simple_rows

    def test_copy_frozen(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", self.simple_rows[::2])
        sync_freeze_table("//tmp/t")
        copy("//tmp/t", "//tmp/copy")
        sync_unfreeze_table("//tmp/t")
        assert get("//tmp/copy/@enable_dynamic_store_read")
        sync_mount_table("//tmp/copy", freeze=True)
        sync_unfreeze_table("//tmp/copy")
        insert_rows("//tmp/t", self.simple_rows[1::2])
        insert_rows("//tmp/copy", self.simple_rows[1:-1:2])
        assert read_table("//tmp/t") == self.simple_rows
        assert read_table("//tmp/copy") == self.simple_rows[:-1]

    def test_multiple_versions(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        assert read_table("//tmp/t") == [{"key": 1, "value": "a"}]

    def test_dynamic_store_id_pool(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        set("//tmp/t/@replication_factor", 10)
        set("//tmp/t/@chunk_writer", {"upload_replication_factor": 10})
        set("//tmp/t/@max_dynamic_store_row_count", 5)
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        set("//tmp/t/@dynamic_store_auto_flush_period", YsonEntity())
        sync_mount_table("//tmp/t")
        rows = []
        for i in range(40):
            row = {"key": i, "value": str(i)}
            rows.append(row)
            for retry in range(5):
                try:
                    insert_rows("//tmp/t", [row])
                    break
                except:
                    sleep(0.5)
                    pass
            else:
                raise Exception("Failed to insert rows")

        # Wait till the active store is not overflown.
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        def _wait_func():
            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            for store in orchid["eden"]["stores"].itervalues():
                if store["store_state"] == "active_dynamic":
                    return store["row_count"] < 5
            # Getting orchid is non-atomic, so we may miss the active store.
            return False
        wait(_wait_func)

        self._validate_tablet_statistics("//tmp/t")

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        assert read_table("//tmp/t") == rows
        expected_store_count = get("//tmp/t/@chunk_count")

        def _store_count_by_type():
            root_chunk_list_id = get("//tmp/t/@chunk_list_id")
            tablet_chunk_list_id = get("#{}/@child_ids/0".format(root_chunk_list_id))
            stores = get("#{}/@tree".format(tablet_chunk_list_id))
            count = {"dynamic_store": 0, "chunk": 0}
            for store in stores:
                count[store.attributes.get("type", "chunk")] += 1
            return count

        assert _store_count_by_type()["chunk"] == 0
        assert _store_count_by_type()["dynamic_store"] > 8

        set("//tmp/t/@chunk_writer", {"upload_replication_factor": 2})
        remount_table("//tmp/t")

        # Wait till all stores are flushed.
        def _wait_func():
            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            for store in orchid["eden"]["stores"].itervalues():
                if store["store_state"] == "passive_dynamic":
                    return False
            return True
        wait(_wait_func)

        # NB: Usually the last flush should request dynamic store id. However, in rare cases
        # two flushes run concurrently and the id is not requested, thus only one dynamic store remains.
        wait(lambda: expected_store_count <= get("//tmp/t/@chunk_count") <= expected_store_count + 1)
        assert 1 <= _store_count_by_type()["dynamic_store"] <= 2

        assert read_table("//tmp/t", tx=tx) == rows

        self._validate_tablet_statistics("//tmp/t")

    @pytest.mark.parametrize("enable_dynamic_store_read", [True, False])
    def test_read_with_timestamp(self, enable_dynamic_store_read):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", enable_dynamic_store_read)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        ts = generate_timestamp()
        assert ts > get("//tmp/t/@unflushed_timestamp")

        if enable_dynamic_store_read:
            assert read_table("<timestamp={}>//tmp/t".format(ts)) == [{"key": 1, "value": "a"}]

            create("table", "//tmp/p")
            map(in_="<timestamp={}>//tmp/t".format(ts), out="//tmp/p", command="cat")
            assert read_table("//tmp/p") == [{"key": 1, "value": "a"}]
        else:
            with pytest.raises(YtError):
                read_table("<timestamp={}>//tmp/t".format(ts))

            create("table", "//tmp/p")
            with pytest.raises(YtError):
                map(in_="<timestamp={}>//tmp/t".format(ts), out="//tmp/p", command="cat")

    def test_bulk_insert_overwrite(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        create("table", "//tmp/p")
        write_table("//tmp/p", [{"key": 2, "value": "b"}])

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        assert read_table("//tmp/t", tx=tx) == [{"key": 1, "value": "a"}]

        merge(in_="//tmp/p", out="//tmp/t", mode="ordered")
        assert read_table("//tmp/t") == [{"key": 2, "value": "b"}]

        with pytest.raises(YtError):
            # We've lost the data, but at least master didn't crash.
            read_table("//tmp/t", tx=tx)

        self._validate_tablet_statistics("//tmp/t")

    def test_accounting(self):
        cell_id = sync_create_cells(1)[0]

        def _get_cell_statistics():
            statistics = get("#{}/@total_statistics".format(cell_id))
            statistics.pop("disk_space_per_medium", None)

        empty_statistics = _get_cell_statistics()

        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=YsonEntity())

        sync_mount_table("//tmp/t")
        self._validate_cell_statistics(tablet_count=1, chunk_count=2, store_count=1)

        sync_freeze_table("//tmp/t")
        self._validate_cell_statistics(tablet_count=1, chunk_count=0, store_count=0)

        sync_unfreeze_table("//tmp/t")
        self._validate_cell_statistics(tablet_count=1, chunk_count=2, store_count=1)

        insert_rows("//tmp/t", [{"key": 1}])
        sync_freeze_table("//tmp/t")
        self._validate_cell_statistics(tablet_count=1, chunk_count=1, store_count=1)

        sync_unmount_table("//tmp/t")
        wait(lambda: _get_cell_statistics() == empty_statistics)

        sync_mount_table("//tmp/t", freeze=True)
        self._validate_cell_statistics(tablet_count=1, chunk_count=1, store_count=1)

        sync_unfreeze_table("//tmp/t")
        self._validate_cell_statistics(tablet_count=1, chunk_count=3, store_count=2)

        sync_unmount_table("//tmp/t", force=True)
        wait(lambda: _get_cell_statistics() == empty_statistics)

class TestReadSortedDynamicTablesMulticell(TestReadSortedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2
