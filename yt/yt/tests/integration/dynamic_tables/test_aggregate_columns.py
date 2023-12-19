from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors,
    create, create_dynamic_table, alter_table, read_table,
    start_transaction, commit_transaction,
    lookup_rows, select_rows, insert_rows, delete_rows,
    sync_create_cells, sync_mount_table, sync_flush_table, sync_compact_table, sync_unmount_table)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import yt.yson as yson

from yt.yson import get_bytes

from yt.xdelta_aggregate_column.bindings import State
from yt.xdelta_aggregate_column.bindings import StateEncoder
from yt.xdelta_aggregate_column.bindings import XDeltaCodec


##################################################################


class TestAggregateColumns(TestSortedDynamicTablesBase):
    def _create_table_with_aggregate_column(self, path, aggregate="sum", **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "time", "type": "int64"},
                        {"name": "value", "type": "int64", "aggregate": aggregate},
                    ]
                }
            )
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

        test_row(
            {"key": 1, "time": 1, "value": 10},
            {"key": 1, "time": 1, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 2, "value": 10},
            {"key": 1, "time": 2, "value": 20},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 3, "value": 10},
            {"key": 1, "time": 3, "value": 30},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 3, "value": 30})
        test_row(
            {"key": 1, "time": 4, "value": 10},
            {"key": 1, "time": 4, "value": 40},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 5, "value": 10},
            {"key": 1, "time": 5, "value": 50},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 6, "value": 10},
            {"key": 1, "time": 6, "value": 60},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 6, "value": 60})
        test_row(
            {"key": 1, "time": 7, "value": 10},
            {"key": 1, "time": 7, "value": 70},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 8, "value": 10},
            {"key": 1, "time": 8, "value": 80},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 9, "value": 10},
            {"key": 1, "time": 9, "value": 90},
            aggregate=True,
        )

        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row(
            {"key": 1, "time": 10, "value": 10},
            {"key": 1, "time": 10, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 11, "value": 10},
            {"key": 1, "time": 11, "value": 20},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 12, "value": 10},
            {"key": 1, "time": 12, "value": 30},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 12, "value": 30})
        test_row(
            {"key": 1, "time": 13, "value": 10},
            {"key": 1, "time": 13, "value": 40},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 14, "value": 10},
            {"key": 1, "time": 14, "value": 50},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 15, "value": 10},
            {"key": 1, "time": 15, "value": 60},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 15, "value": 60})
        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row(
            {"key": 1, "time": 16, "value": 10},
            {"key": 1, "time": 16, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 17, "value": 10},
            {"key": 1, "time": 17, "value": 20},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 18, "value": 10},
            {"key": 1, "time": 18, "value": 30},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 18, "value": 30})
        test_row({"key": 1, "time": 19, "value": 10}, {"key": 1, "time": 19, "value": 10})
        test_row(
            {"key": 1, "time": 20, "value": 10},
            {"key": 1, "time": 20, "value": 20},
            aggregate=True,
        )
        test_row({"key": 1, "time": 21, "value": 10}, {"key": 1, "time": 21, "value": 10})

        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 21, "value": 10})

    @authors("savrus")
    def test_aggregate_min_max(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="min", optimize_for="scan")
        sync_mount_table("//tmp/t")

        insert_rows(
            "//tmp/t",
            [
                {"key": 1, "time": 1, "value": 10},
                {"key": 2, "time": 1, "value": 20},
                {"key": 3, "time": 1},
            ],
            aggregate=True,
        )
        insert_rows(
            "//tmp/t",
            [
                {"key": 1, "time": 2, "value": 30},
                {"key": 2, "time": 2, "value": 40},
                {"key": 3, "time": 2},
            ],
            aggregate=True,
        )
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
            {"name": "value", "type": "int64"},
        ]
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

        test_row(
            {"key": 1, "time": 1, "value": 10},
            {"key": 1, "time": 1, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 2, "value": 20},
            {"key": 1, "time": 2, "value": 20},
            aggregate=True,
        )

        sync_unmount_table("//tmp/t")
        schema[2]["aggregate"] = "sum"
        alter_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        verify_row(1, [{"key": 1, "time": 2, "value": 20}])
        test_row(
            {"key": 1, "time": 3, "value": 10},
            {"key": 1, "time": 3, "value": 30},
            aggregate=True,
        )

    @authors("savrus")
    def test_aggregate_non_atomic(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="sum", atomicity="none")
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet", atomicity="none")
        tx2 = start_transaction(type="tablet", atomicity="none")

        insert_rows(
            "//tmp/t",
            [{"key": 1, "time": 1, "value": 10}],
            aggregate=True,
            atomicity="none",
            tx=tx1,
        )
        insert_rows(
            "//tmp/t",
            [{"key": 1, "time": 2, "value": 20}],
            aggregate=True,
            atomicity="none",
            tx=tx2,
        )

        commit_transaction(tx1)
        commit_transaction(tx2)

        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 30}]

    @pytest.mark.parametrize(
        "merge_rows_on_flush, min_data_ttl, min_data_versions",
        [a + b for a in [(False,), (True,)] for b in [(0, 0), (1, 1)]],
    )
    @authors("babenko")
    def test_aggregate_merge_rows_on_flush(self, merge_rows_on_flush, min_data_ttl, min_data_versions):
        sync_create_cells(1)
        self._create_table_with_aggregate_column(
            "//tmp/t",
            merge_rows_on_flush=merge_rows_on_flush,
            min_data_ttl=min_data_ttl,
            min_data_versions=min_data_versions,
            max_data_ttl=1000000,
            max_data_versions=1,
        )
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

    @authors("leasid")
    def test_aggregate_xdelta(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "time", "type": "int64"},
            {"name": "value", "type": "string", "aggregate": "xdelta"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        encoder = StateEncoder(None)
        codec = XDeltaCodec(None)

        # basic case: write patches
        base = b""
        state = b"123456"
        patch = encoder.create_patch_state((base, state))
        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": patch}], aggregate=True)

        state1 = b"567890"
        patch = encoder.create_patch_state((state, state1))
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.PATCH_TYPE
        result_state = codec.apply_patch((base, result.payload_data, len(state1)))
        assert result_state == state1

        state2 = b"7890"
        patch = encoder.create_patch_state((state1, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 3, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.PATCH_TYPE
        result_state = codec.apply_patch((base, result.payload_data, len(state2)))
        assert result_state == state2

        # overwrite state
        base = state
        base_state = encoder.create_base_state(base)
        insert_rows("//tmp/t", [{"key": 1, "time": 4, "value": base_state}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == base

        patch = encoder.create_patch_state((base, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 5, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == state2

        # test null as patch
        patch = encoder.create_patch_state((state2, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 6, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == state2

        # plant error
        patch = encoder.create_patch_state((state1, state2))  # inconsistent patch - not applicable for stored base
        insert_rows("//tmp/t", [{"key": 1, "time": 7, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.ERROR_TYPE
        assert result.has_error_code
        assert result.error_code > 0  # base hash error

        # fix error
        base_state = encoder.create_base_state(base)
        insert_rows("//tmp/t", [{"key": 1, "time": 8, "value": base_state}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == base

        patch = encoder.create_patch_state((base, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 9, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == state2

        # delete rows
        delete_rows("//tmp/t", [{"key": 1}])
        row = lookup_rows("//tmp/t", [{"key": 1}])
        assert_items_equal(row, [])

    @authors("aleksandra-zh")
    def test_aggregate_replica_set(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "_yt_replica_set"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [['a-b-c-d', 1, 2], ['a-b-c-f', 3, 4]],
            []
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [['a-b-c-d', 1, 2], ['a-b-c-f', 3, 4]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [],
            [['a-b-c-d', 1, 2]]
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [['a-b-c-f', 3, 4]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [['a-b-c-d', 1, 2], ['a-b-c-f', 3, 4]],
            []
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [['a-b-c-d', 1, 2], ['a-b-c-f', 3, 4]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [],
            [['a-b-c-d', 1, 2], ['a-b-c-f', 3, 4]]
        ])}], aggregate=True)

        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == []


##################################################################

class TestAggregateColumnsMulticell(TestAggregateColumns):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestAggregateColumnsRpcProxy(TestAggregateColumns):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
