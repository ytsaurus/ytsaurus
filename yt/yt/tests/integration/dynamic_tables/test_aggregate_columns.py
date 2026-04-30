from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors,
    create, create_dynamic_table, alter_table, read_table, write_table,
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


@pytest.mark.enabled_multidaemon
class TestAggregateColumns(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

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
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_aggregate_alter(self, optimize_for):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "time", "type": "int64"},
            {"name": "value", "type": "int64"},
        ]
        create("table", "//tmp/t", attributes={"dynamic": True, "schema": schema, "optimize_for": optimize_for})
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

    @authors("abatovkin")
    @pytest.mark.parametrize("precision", [7, 10, 14])
    def test_aggregate_hll(self, precision):
        sync_create_cells(1)

        register_count = 1 << precision

        def make_empty_hll():
            return b"\x00" * register_count

        def hll_add(state, fingerprint):
            registers = bytearray(state)
            fingerprint |= (1 << 63)
            index = fingerprint & (register_count - 1)
            shifted = fingerprint >> precision
            zeroes_plus_one = 0
            while zeroes_plus_one < 64 and (shifted & 1) == 0:
                shifted >>= 1
                zeroes_plus_one += 1
            zeroes_plus_one += 1
            if registers[index] < zeroes_plus_one:
                registers[index] = zeroes_plus_one
            return bytes(registers)

        def hll_merge(state1, state2):
            r1 = bytearray(state1)
            r2 = bytearray(state2)
            return bytes(max(a, b) for a, b in zip(r1, r2))

        def farm_hash_int64(value):
            # Use a simple hash for test purposes; the exact hash function
            # does not matter as long as we are consistent within the test
            import hashlib
            h = hashlib.md5(str(value).encode()).digest()
            return int.from_bytes(h[:8], "little")

        aggregate_name = "hll_{}_merge_state".format(precision)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": aggregate_name},
        ]
        create_dynamic_table("//tmp/t_hll", schema=schema)
        sync_mount_table("//tmp/t_hll")

        hll1 = make_empty_hll()
        for i in range(100):
            hll1 = hll_add(hll1, farm_hash_int64(i))

        hll2 = make_empty_hll()
        for i in range(50, 150):
            hll2 = hll_add(hll2, farm_hash_int64(i))

        insert_rows("//tmp/t_hll", [{"key": 1, "hll_state": hll1}], aggregate=True)
        insert_rows("//tmp/t_hll", [{"key": 1, "hll_state": hll2}], aggregate=True)

        expected_merged = hll_merge(hll1, hll2)

        rows = lookup_rows("//tmp/t_hll", [{"key": 1}])
        assert len(rows) == 1
        actual_state = get_bytes(rows[0]["hll_state"])
        assert actual_state == expected_merged

        sync_flush_table("//tmp/t_hll")

        rows = lookup_rows("//tmp/t_hll", [{"key": 1}])
        assert len(rows) == 1
        actual_state = get_bytes(rows[0]["hll_state"])
        assert actual_state == expected_merged

        hll3 = make_empty_hll()
        for i in range(200, 300):
            hll3 = hll_add(hll3, farm_hash_int64(i))

        insert_rows("//tmp/t_hll", [{"key": 1, "hll_state": hll3}], aggregate=True)
        expected_merged = hll_merge(expected_merged, hll3)

        sync_flush_table("//tmp/t_hll")
        sync_compact_table("//tmp/t_hll")

        rows = lookup_rows("//tmp/t_hll", [{"key": 1}])
        assert len(rows) == 1
        actual_state = get_bytes(rows[0]["hll_state"])
        assert actual_state == expected_merged

    @authors("abatovkin")
    def test_aggregate_hll_overwrite(self):
        """Test that writing without aggregate flag overwrites the HLL state."""
        sync_create_cells(1)

        precision = 14
        register_count = 1 << precision

        def make_empty_hll():
            return b"\x00" * register_count

        def hll_add(state, fingerprint):
            registers = bytearray(state)
            fingerprint |= (1 << 63)
            index = fingerprint & (register_count - 1)
            shifted = fingerprint >> precision
            zeroes_plus_one = 0
            while zeroes_plus_one < 64 and (shifted & 1) == 0:
                shifted >>= 1
                zeroes_plus_one += 1
            zeroes_plus_one += 1
            if registers[index] < zeroes_plus_one:
                registers[index] = zeroes_plus_one
            return bytes(registers)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": "hll_14_merge_state"},
        ]
        create_dynamic_table("//tmp/t_hll_overwrite", schema=schema)
        sync_mount_table("//tmp/t_hll_overwrite")

        hll1 = make_empty_hll()
        for i in range(100):
            hll1 = hll_add(hll1, i * 997)

        insert_rows("//tmp/t_hll_overwrite", [{"key": 1, "hll_state": hll1}], aggregate=True)

        hll2 = make_empty_hll()
        for i in range(5):
            hll2 = hll_add(hll2, i * 31)

        insert_rows("//tmp/t_hll_overwrite", [{"key": 1, "hll_state": hll2}])

        rows = lookup_rows("//tmp/t_hll_overwrite", [{"key": 1}])
        assert len(rows) == 1
        actual_state = get_bytes(rows[0]["hll_state"])
        assert actual_state == hll2

    @authors("abatovkin")
    def test_aggregate_hll_multiple_keys(self):
        """Test HLL aggregate columns with multiple keys."""
        sync_create_cells(1)

        precision = 7
        register_count = 1 << precision

        def make_empty_hll():
            return b"\x00" * register_count

        def hll_add(state, fingerprint):
            registers = bytearray(state)
            fingerprint |= (1 << 63)
            index = fingerprint & (register_count - 1)
            shifted = fingerprint >> precision
            zeroes_plus_one = 0
            while zeroes_plus_one < 64 and (shifted & 1) == 0:
                shifted >>= 1
                zeroes_plus_one += 1
            zeroes_plus_one += 1
            if registers[index] < zeroes_plus_one:
                registers[index] = zeroes_plus_one
            return bytes(registers)

        def hll_merge(state1, state2):
            return bytes(max(a, b) for a, b in zip(bytearray(state1), bytearray(state2)))

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": "hll_7_merge_state"},
        ]
        create_dynamic_table("//tmp/t_hll_multi", schema=schema)
        sync_mount_table("//tmp/t_hll_multi")

        states = {}
        for key in range(3):
            hll = make_empty_hll()
            for i in range(50):
                hll = hll_add(hll, (key + 1) * 1000 + i)
            states[key] = hll
            insert_rows("//tmp/t_hll_multi", [{"key": key, "hll_state": hll}], aggregate=True)

        hll_extra_0 = make_empty_hll()
        for i in range(50, 100):
            hll_extra_0 = hll_add(hll_extra_0, 1000 + i)
        insert_rows("//tmp/t_hll_multi", [{"key": 0, "hll_state": hll_extra_0}], aggregate=True)
        states[0] = hll_merge(states[0], hll_extra_0)

        hll_extra_2 = make_empty_hll()
        for i in range(50, 100):
            hll_extra_2 = hll_add(hll_extra_2, 3000 + i)
        insert_rows("//tmp/t_hll_multi", [{"key": 2, "hll_state": hll_extra_2}], aggregate=True)
        states[2] = hll_merge(states[2], hll_extra_2)

        for key in range(3):
            rows = lookup_rows("//tmp/t_hll_multi", [{"key": key}])
            assert len(rows) == 1
            actual_state = get_bytes(rows[0]["hll_state"])
            assert actual_state == states[key]

        sync_flush_table("//tmp/t_hll_multi")
        sync_compact_table("//tmp/t_hll_multi")

        for key in range(3):
            rows = lookup_rows("//tmp/t_hll_multi", [{"key": key}])
            assert len(rows) == 1
            actual_state = get_bytes(rows[0]["hll_state"])
            assert actual_state == states[key]

    @authors("abatovkin")
    @pytest.mark.parametrize("precision", [7, 14])
    def test_aggregate_hll_cardinality_estimation(self, precision):
        """End-to-end test: build HLL from raw values, insert as aggregate column,
        merge across multiple writes/flush/compaction, and verify cardinality estimate."""
        import hashlib
        import math

        sync_create_cells(1)

        register_count = 1 << precision

        def farm_hash(value):
            h = hashlib.md5(str(value).encode()).digest()
            return int.from_bytes(h[:8], "little")

        def make_empty_hll():
            return bytearray(register_count)

        def hll_add(state, raw_value):
            fingerprint = farm_hash(raw_value)
            fingerprint |= (1 << 63)
            index = fingerprint & (register_count - 1)
            shifted = fingerprint >> precision
            zeroes_plus_one = 0
            while zeroes_plus_one < 64 and (shifted & 1) == 0:
                shifted >>= 1
                zeroes_plus_one += 1
            zeroes_plus_one += 1
            if state[index] < zeroes_plus_one:
                state[index] = zeroes_plus_one

        def hll_estimate(state):
            m = register_count
            alpha = 0.7213 / (1.0 + 1.079 / m)
            raw = alpha * m * m / sum(2.0 ** (-r) for r in state)
            # small range correction
            zeros = sum(1 for r in state if r == 0)
            if raw <= 2.5 * m and zeros > 0:
                return m * math.log(m / zeros)
            return raw

        aggregate_name = "hll_{}_merge_state".format(precision)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": aggregate_name},
        ]
        create_dynamic_table("//tmp/t_hll_card", schema=schema)
        sync_mount_table("//tmp/t_hll_card")

        # Simulate 3 batches of user visits:
        # batch 1: users 0..999 (1000 unique)
        # batch 2: users 500..1499 (500 new, 500 overlap)
        # batch 3: users 1000..1999 (500 new, 500 overlap)
        # Total unique: 2000
        batches = [
            range(0, 1000),
            range(500, 1500),
            range(1000, 2000),
        ]

        for batch in batches:
            hll = make_empty_hll()
            for user_id in batch:
                hll_add(hll, user_id)
            insert_rows("//tmp/t_hll_card", [{"key": 1, "hll_state": bytes(hll)}], aggregate=True)

        rows = lookup_rows("//tmp/t_hll_card", [{"key": 1}])
        estimate = hll_estimate(get_bytes(rows[0]["hll_state"]))
        # HLL with precision 7 (128 registers) has ~9% standard error,
        # but our simplified estimator (no bias correction) can deviate more.
        # HLL with precision 14 (16384 registers) has ~0.8% standard error.
        max_error = 0.20 if precision == 7 else 0.05
        assert abs(estimate - 2000) / 2000 < max_error, \
            "Cardinality estimate {} is too far from expected 2000 (error: {:.1%})".format(
                estimate, abs(estimate - 2000) / 2000)

        sync_flush_table("//tmp/t_hll_card")
        sync_compact_table("//tmp/t_hll_card")

        rows = lookup_rows("//tmp/t_hll_card", [{"key": 1}])
        estimate_after_compact = hll_estimate(get_bytes(rows[0]["hll_state"]))
        assert estimate_after_compact == estimate, \
            "Cardinality changed after compaction: {} -> {}".format(estimate, estimate_after_compact)

    @authors("abatovkin")
    def test_aggregate_hll_batch_states_for_realtime_counters(self):
        """Build per-batch HLL states from raw events, merge them via an aggregate column,
        and query approximate unique users from the accumulated state."""
        sync_create_cells(1)

        create_dynamic_table("//tmp/hll_batches", schema=[
            {"name": "batch_id", "type": "int64", "sort_order": "ascending"},
            {"name": "counter_id", "type": "int64", "sort_order": "ascending"},
            {"name": "user_id", "type": "int64", "sort_order": "ascending"},
            {"name": "event_count", "type": "int64"},
        ], enable_dynamic_store_read=True)
        sync_mount_table("//tmp/hll_batches")

        batches = {
            0: {1: range(0, 10), 2: range(100, 110)},
            1: {1: range(5, 15), 2: range(103, 113)},
            2: {1: range(10, 20), 2: range(106, 116)},
            3: {1: range(15, 25), 2: range(109, 119)},
        }

        source_rows = []
        for batch_id, counters in batches.items():
            for counter_id, users in counters.items():
                for user_id in users:
                    source_rows.append({
                        "counter_id": counter_id,
                        "batch_id": batch_id,
                        "user_id": user_id,
                        "event_count": 1,
                    })
        insert_rows("//tmp/hll_batches", source_rows)

        schema = [
            {"name": "counter_id", "type": "int64", "sort_order": "ascending"},
            {"name": "users_hll_state", "type": "string", "aggregate": "hll_14_merge_state"},
        ]
        create_dynamic_table("//tmp/hll_counters", schema=schema)
        sync_mount_table("//tmp/hll_counters")

        exact_users = {1: set(), 2: set()}

        def get_estimates():
            return select_rows(
                "counter_id, cardinality_merge(users_hll_state) as approx_users "
                "from [//tmp/hll_counters] group by counter_id order by counter_id limit 1000"
            )

        for batch_id in sorted(batches):
            batch_states = select_rows(
                "counter_id, cardinality_state(user_id) as users_hll_state "
                "from [//tmp/hll_batches] "
                "where batch_id = {} "
                "group by counter_id".format(batch_id)
            )
            insert_rows("//tmp/hll_counters", batch_states, aggregate=True)

            for counter_id, users in batches[batch_id].items():
                exact_users[counter_id].update(users)

            estimate_rows = get_estimates()
            assert len(estimate_rows) == 2
            for row in estimate_rows:
                expected = len(exact_users[row["counter_id"]])
                assert abs(row["approx_users"] - expected) <= 2, \
                    "Unexpected estimate after batch {} for counter {}: {} vs {}".format(
                        batch_id, row["counter_id"], row["approx_users"], expected)

        rows = lookup_rows("//tmp/hll_counters", [{"counter_id": 1}, {"counter_id": 2}])
        assert len(rows) == 2

        estimate_rows_before_compact = get_estimates()

        sync_flush_table("//tmp/hll_counters")
        sync_compact_table("//tmp/hll_counters")

        estimate_rows = get_estimates()
        assert estimate_rows == estimate_rows_before_compact
        for row in estimate_rows:
            expected = 25 if row["counter_id"] == 1 else 19
            assert abs(row["approx_users"] - expected) <= 2, \
                "Unexpected estimate after compaction for counter {}: {} vs {}".format(
                    row["counter_id"], row["approx_users"], expected)

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

    @authors("hitsedesen")
    def test_aggregate_dict_sum(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "dict_sum"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        insert_rows(
            "//tmp/t", [{"key": 1}, {"key": 2, "value": {"a": 11, "b": {"c": {"d": 7}}, "e": {"f": {"g": 13}}, "h": 5}}]
        )
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [
            {"key": 1, "value": yson.YsonEntity()},
            {"key": 2, "value": {"a": 11, "b": {"c": {"d": 7}}, "e": {"f": {"g": 13}}, "h": 5}},
        ]
        insert_rows(
            "//tmp/t",
            [{"key": 1, "value": {"a": 3}}, {"key": 2, "value": {"a": 3, "b": {"c": {"d": 17}}}}],
            aggregate=True,
        )
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [
            {"key": 1, "value": {"a": 3}},
            {"key": 2, "value": {"a": 14, "b": {"c": {"d": 24}}, "e": {"f": {"g": 13}}, "h": 5}},
        ]
        insert_rows("//tmp/t", [{"key": 2, "value": {"a": -14, "b": {"c": {"d": -24}}}}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [{"key": 1, "value": {"a": 3}}, {"key": 2, "value": {"e": {"f": {"g": 13}}, "h": 5}}]
        insert_rows("//tmp/t", [{"key": 2, "value": {"h": 25, "q": 1}}])
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [{"key": 1, "value": {"a": 3}}, {"key": 2, "value": {"h": 25, "q": 1}}]

    @authors("aleksandra-zh", "grphil")
    def test_aggregate_stored_replica_set(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "_yt_stored_replica_set"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [
            {
                "key": 1,
                "value": yson.YsonList([
                    [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]],
                    []
                ])
            },
            {
                "key": 2,
                "value": yson.YsonList([
                    [[yson.YsonUint64(1), 1, yson.YsonUint64(1), yson.YsonUint64(1)],
                     [yson.YsonUint64(2), 2, yson.YsonUint64(2), yson.YsonUint64(2)]],
                    [],
                ])
            }], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]]
        value = lookup_rows("//tmp/t", [{"key": 2}])[0]["value"]
        assert value == [
            [yson.YsonUint64(1), 1, yson.YsonUint64(1), yson.YsonUint64(1)],
            [yson.YsonUint64(2), 2, yson.YsonUint64(2), yson.YsonUint64(2)],
        ]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [],
            [[yson.YsonUint64(20), 1, yson.YsonUint64(2)]]
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [[yson.YsonUint64(30), 3, yson.YsonUint64(4)]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]],
            []
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [],
            [[yson.YsonUint64(30), 3, yson.YsonUint64(4)], [yson.YsonUint64(20), 1, yson.YsonUint64(2)]]
        ])}], aggregate=True)

        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == []

        insert_rows("//tmp/t", [{"key": 2, "value": yson.YsonList([
            [[yson.YsonUint64(1), 1, yson.YsonUint64(1), yson.YsonUint64(2)]],
            [],
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 2}])[0]["value"]
        assert value == [[yson.YsonUint64(1), 1, yson.YsonUint64(1), yson.YsonUint64(2)], [yson.YsonUint64(2), 2, yson.YsonUint64(2), yson.YsonUint64(2)]]

        insert_rows("//tmp/t", [{"key": 2, "value": yson.YsonList([
            [],
            [[yson.YsonUint64(2), 2, yson.YsonUint64(2)]]
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 2}])[0]["value"]
        assert value == [[yson.YsonUint64(1), 1, yson.YsonUint64(1), yson.YsonUint64(2)]]

        insert_rows("//tmp/t", [{"key": 2, "value": yson.YsonList([
            [],
            [[yson.YsonUint64(1), 1, yson.YsonUint64(1), yson.YsonUint64(3)]]
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 2}])[0]["value"]
        assert value == []

    @authors("aleksandra-zh")
    def test_aggregate_last_seen_replica_set(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "_yt_last_seen_replica_set"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        original_rows = [
            [yson.YsonUint64(i), 16, yson.YsonUint64(2)] for i in range(3)
        ]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(original_rows)}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == original_rows

        new_row = [[yson.YsonUint64(20), 16, yson.YsonUint64(2)]]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(new_row)}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == original_rows[1:] + new_row

    @authors("aleksandra-zh")
    def test_aggregate_erasure_last_seen_replica_set(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "_yt_last_seen_replica_set"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        rows = [
            [yson.YsonUint64(i), i, yson.YsonUint64(2)] for i in range(16)
        ]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(rows)}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == rows

        new_row_3 = [[yson.YsonUint64(20), 3, yson.YsonUint64(2)]]
        new_row_5 = [[yson.YsonUint64(30), 5, yson.YsonUint64(2)]]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(new_row_3)}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(new_row_5)}], aggregate=True)

        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        rows[3] = new_row_3[0]
        rows[5] = new_row_5[0]
        assert value == rows

##################################################################


@pytest.mark.enabled_multidaemon
class TestAggregateColumnsMulticell(TestAggregateColumns):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


@pytest.mark.enabled_multidaemon
class TestAggregateColumnsRpcProxy(TestAggregateColumns):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
