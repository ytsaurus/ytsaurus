import pytest
import __builtin__

from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import *

import random

from yt.environment.helpers import assert_items_equal

##################################################################

class TestAggregateColumns(TestSortedDynamicTablesBase):
    def _create_table_with_aggregate_column(self, path, aggregate="sum", **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "time", "type": "int64"},
                {"name": "value", "type": "int64", "aggregate": aggregate}]
            })
        create_dynamic_table(path, **attributes)

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_aggregate_columns(self, optimize_for):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        def verify_row(key, expected):
            actual = lookup_rows("//tmp/t", [{"key": key}])
            assert_items_equal(actual, expected)
            actual = select_rows("key, time, value from [//tmp/t]")
            assert_items_equal(actual, expected)

        def test_row(row, expected, **kwargs):
            insert_rows("//tmp/t", [row], **kwargs)
            verify_row(row["key"], [expected])

        def verify_after_flush(row):
            verify_row(row["key"], [row])
            assert_items_equal(read_table("//tmp/t"), [row])

        test_row({"key": 1, "time": 1, "value": 10}, {"key": 1, "time": 1, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 2, "value": 10}, {"key": 1, "time": 2, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 3, "value": 10}, {"key": 1, "time": 3, "value": 30}, aggregate=True)

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 3, "value": 30})
        test_row({"key": 1, "time": 4, "value": 10}, {"key": 1, "time": 4, "value": 40}, aggregate=True)
        test_row({"key": 1, "time": 5, "value": 10}, {"key": 1, "time": 5, "value": 50}, aggregate=True)
        test_row({"key": 1, "time": 6, "value": 10}, {"key": 1, "time": 6, "value": 60}, aggregate=True)

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 6, "value": 60})
        test_row({"key": 1, "time": 7, "value": 10}, {"key": 1, "time": 7, "value": 70}, aggregate=True)
        test_row({"key": 1, "time": 8, "value": 10}, {"key": 1, "time": 8, "value": 80}, aggregate=True)
        test_row({"key": 1, "time": 9, "value": 10}, {"key": 1, "time": 9, "value": 90}, aggregate=True)

        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row({"key": 1, "time": 10, "value": 10}, {"key": 1, "time": 10, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 11, "value": 10}, {"key": 1, "time": 11, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 12, "value": 10}, {"key": 1, "time": 12, "value": 30}, aggregate=True)

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 12, "value": 30})
        test_row({"key": 1, "time": 13, "value": 10}, {"key": 1, "time": 13, "value": 40}, aggregate=True)
        test_row({"key": 1, "time": 14, "value": 10}, {"key": 1, "time": 14, "value": 50}, aggregate=True)
        test_row({"key": 1, "time": 15, "value": 10}, {"key": 1, "time": 15, "value": 60}, aggregate=True)

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 15, "value": 60})
        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row({"key": 1, "time": 16, "value": 10}, {"key": 1, "time": 16, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 17, "value": 10}, {"key": 1, "time": 17, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 18, "value": 10}, {"key": 1, "time": 18, "value": 30}, aggregate=True)

        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 18, "value": 30})
        test_row({"key": 1, "time": 19, "value": 10}, {"key": 1, "time": 19, "value": 10})
        test_row({"key": 1, "time": 20, "value": 10}, {"key": 1, "time": 20, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 21, "value": 10}, {"key": 1, "time": 21, "value": 10})

        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 21, "value": 10})

    @authors("savrus")
    def test_aggregate_min_max(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="min", optimize_for="scan")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [
            {"key": 1, "time": 1, "value": 10},
            {"key": 2, "time": 1, "value": 20},
            {"key": 3, "time": 1}], aggregate=True)
        insert_rows("//tmp/t", [
            {"key": 1, "time": 2, "value": 30},
            {"key": 2, "time": 2, "value": 40},
            {"key": 3, "time": 2}], aggregate=True)
        assert_items_equal(select_rows("max(value) as max from [//tmp/t] group by 1"), [{"max": 20}])

    @authors("savrus")
    def test_aggregate_first(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="first")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 10}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 20}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 10}]

    @authors("savrus")
    @pytest.mark.parametrize("aggregate", ["min", "max", "sum", "first"])
    def test_aggregate_update(self, aggregate):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate=aggregate)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "time": 1}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 1, "value": None}]
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 10}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 10}]
        insert_rows("//tmp/t", [{"key": 1, "time": 3}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 3, "value": 10}]

    @authors("savrus")
    def test_aggregate_alter(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "time", "type": "int64"},
            {"name": "value", "type": "int64"}]
        create("table", "//tmp/t", attributes={"dynamic": True, "schema": schema})
        sync_mount_table("//tmp/t")

        def verify_row(key, expected):
            actual = lookup_rows("//tmp/t", [{"key": key}])
            assert_items_equal(actual, expected)
            actual = select_rows("key, time, value from [//tmp/t]")
            assert_items_equal(actual, expected)

        def test_row(row, expected, **kwargs):
            insert_rows("//tmp/t", [row], **kwargs)
            verify_row(row["key"], [expected])

        test_row({"key": 1, "time": 1, "value": 10}, {"key": 1, "time": 1, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 2, "value": 20}, {"key": 1, "time": 2, "value": 20}, aggregate=True)

        sync_unmount_table("//tmp/t")
        schema[2]["aggregate"] = "sum"
        alter_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        verify_row(1, [{"key": 1, "time": 2, "value": 20}])
        test_row({"key": 1, "time": 3, "value": 10}, {"key": 1, "time": 3, "value": 30}, aggregate=True)

    @authors("savrus")
    def test_aggregate_non_atomic(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="sum", atomicity="none")
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet", atomicity="none")
        tx2 = start_transaction(type="tablet", atomicity="none")

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 10}], aggregate=True, atomicity="none", tx=tx1)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 20}], aggregate=True, atomicity="none", tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 30}]

    @pytest.mark.parametrize("merge_rows_on_flush, min_data_ttl, min_data_versions",
        [a + b for a in [(False,), (True,)] \
               for b in [(0, 0), (1, 10000)]])
    @authors("babenko")
    def test_aggregate_merge_rows_on_flush(self, merge_rows_on_flush, min_data_ttl, min_data_versions):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t",
            merge_rows_on_flush=merge_rows_on_flush,
            min_data_ttl=min_data_ttl,
            min_data_versions=min_data_versions,
            max_data_ttl=1000000,
            max_data_versions=1)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 1000}], aggregate=False)
        delete_rows("//tmp/t", [{"key": 1}])
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 2000}], aggregate=True)
        delete_rows("//tmp/t", [{"key": 1}])
        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 10}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 20}], aggregate=True)

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 30}])

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 30}])

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 100}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 200}], aggregate=True)

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 330}])

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 330}])

        sync_compact_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 330}])

    @authors("savrus")
    @pytest.mark.parametrize("aggregate", ["avg", "cardinality"])
    def test_invalid_aggregate(self, aggregate):
        sync_create_cells(1)
        with pytest.raises(YtError):
            self._create_table_with_aggregate_column("//tmp/t", aggregate=aggregate)

##################################################################

class TestAggregateColumnsMulticell(TestAggregateColumns):
    NUM_SECONDARY_MASTER_CELLS = 2
    
class TestAggregateColumnsRpcProxy(TestAggregateColumns):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
