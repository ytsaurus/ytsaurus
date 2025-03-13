from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_helpers import profiler_factory

from yt_commands import (
    authors, print_debug, wait, create, ls, get, set, remove, exists, copy, insert_rows,
    lookup_rows, delete_rows, create_dynamic_table, generate_uuid,
    alter_table, read_table, write_table, remount_table, generate_timestamp,
    sync_create_cells, sync_mount_table, sync_unmount_table, sync_freeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table, update_nodes_dynamic_config, set_node_banned,
    get_cell_leader_address, get_tablet_leader_address, WaitFailed, raises_yt_error,
    wait_for_cells, build_snapshot, sort, merge)

from yt_type_helpers import make_schema

import yt_error_codes

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

from yt_driver_bindings import Driver

import pytest

from copy import deepcopy
from random import randint, choice, sample, shuffle
import random
import time

################################################################################


@pytest.mark.enabled_multidaemon
class TestLookup(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 2
    NUM_SCHEDULERS = 1

    @authors("savrus")
    def test_lookup_repeated_keys(self):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)

        keys = [{"key": i % 2} for i in range(10)]
        expected = [{"key": i % 2, "value": str(i % 2)} for i in range(10)]
        assert lookup_rows("//tmp/t", keys) == expected

        expected = [{"value": str(i % 2)} for i in range(10)]
        assert lookup_rows("//tmp/t", keys, column_names=["value"]) == expected

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_lookup_versioned(self, optimize_for):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        for prefix in ["a", "b"]:
            rows = [{"key": i, "value": prefix + ":" + str(i)} for i in range(10)]
            insert_rows("//tmp/t", rows)
            generate_timestamp()

        keys = [{"key": i} for i in range(10)]
        actual = lookup_rows("//tmp/t", keys, versioned=True)

        assert len(actual) == len(keys)

        for i, key in enumerate(keys):
            row = actual[i]
            assert "write_timestamps" in row.attributes
            assert len(row.attributes["write_timestamps"]) == 2
            assert "delete_timestamps" in row.attributes
            assert row["key"] == key["key"]
            assert len(row["value"]) == 2
            assert "%s" % row["value"][0] == "b:" + str(key["key"])
            assert "%s" % row["value"][1] == "a:" + str(key["key"])

    @authors("lukyan")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_lookup_versioned_composite(self, optimize_for):
        sync_create_cells(1)

        int64_list = {"type_name": "list", "item": "int64"}
        optional_int64_list = {"type_name": "optional", "item": int64_list}

        create_dynamic_table("//tmp/t", schema=[
            {"name": "k1", "type": "int64", "sort_order": "ascending"},
            {"name": "k2", "type_v3": int64_list, "sort_order": "ascending"},
            {"name": "v1", "type_v3": int64_list},
            {"name": "v2", "type_v3": optional_int64_list},
        ])

        sync_mount_table("//tmp/t")

        insert_rows(
            "//tmp/t",
            [
                {"k1": 1, "k2": [1, 2], "v1": [1, 2], "v2": [1, 2]},
            ]
        )

        lookup_rows("//tmp/t", [{"k1": 1, "k2": [1, 2]}], versioned=True)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_lookup_versioned_YT_6800(self, optimize_for):
        sync_create_cells(1)

        self._create_simple_table(
            "//tmp/t",
            min_data_versions=0,
            min_data_ttl=0,
            max_data_versions=1,
            max_data_ttl=1000000,
            optimize_for=optimize_for,
        )
        sync_mount_table("//tmp/t")

        for prefix in ["a", "b", "c"]:
            rows = [{"key": i, "value": prefix + ":" + str(i)} for i in range(10)]
            insert_rows("//tmp/t", rows)
            generate_timestamp()

        keys = [{"key": i} for i in range(10)]
        actual = lookup_rows("//tmp/t", keys, versioned=True)

        assert len(actual) == len(keys)

        for i, key in enumerate(keys):
            row = actual[i]
            assert "write_timestamps" in row.attributes
            assert len(row.attributes["write_timestamps"]) == 3
            assert "delete_timestamps" in row.attributes
            assert row["key"] == key["key"]
            assert len(row["value"]) == 3
            assert "%s" % row["value"][0] == "c:" + str(key["key"])
            assert "%s" % row["value"][1] == "b:" + str(key["key"])
            assert "%s" % row["value"][2] == "a:" + str(key["key"])

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_lookup_versioned_filter(self, optimize_for, enable_hash_chunk_index):
        if enable_hash_chunk_index and optimize_for == "scan":
            return

        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "string"},
        ]
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": optimize_for,
                "schema": schema,
            },
        )

        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value1": "0"}], update=True)
        keys = [{"key": 0}]
        full_row = lookup_rows("//tmp/t", keys, versioned=True)[0]

        def _check(row):
            assert row.attributes["write_timestamps"] == full_row.attributes["write_timestamps"]
            assert row.attributes["delete_timestamps"] == full_row.attributes["delete_timestamps"]
            assert len(row) == 0

        actual = lookup_rows("//tmp/t", keys, column_names=["value2"], versioned=True)
        _check(actual[0])

        sync_flush_table("//tmp/t")

        actual = lookup_rows("//tmp/t", keys, column_names=["value2"], versioned=True)
        _check(actual[0])

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_lookup_versioned_filter_alter(self, optimize_for):
        sync_create_cells(1)
        schema1 = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
        ]
        schema2 = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "string"},
        ]
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": optimize_for,
                "schema": schema1,
            },
        )

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value1": "0"}], update=True)
        keys = [{"key": 0}]
        full_row = lookup_rows("//tmp/t", keys, versioned=True)[0]
        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=schema2)
        sync_mount_table("//tmp/t")
        row = lookup_rows("//tmp/t", keys, column_names=["value2"], versioned=True)[0]
        assert row.attributes["write_timestamps"] == full_row.attributes["write_timestamps"]
        assert row.attributes["delete_timestamps"] == full_row.attributes["delete_timestamps"]
        assert len(row) == 0

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_lookup_versioned_retention(self, optimize_for):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "string"},
            {"name": "value3", "type": "string"},
        ]
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": optimize_for,
                "schema": schema,
            },
        )

        sync_mount_table("//tmp/t")
        for i in range(10):
            insert_rows("//tmp/t", [{"key": 0, "value1": str(i)}], update=True)
            insert_rows("//tmp/t", [{"key": 0, "value2": str(i)}], update=True)
        keys = [{"key": 0}]

        retention_config = {
            "min_data_ttl": 0,
            "max_data_ttl": 1000 * 60 * 10,
            "min_data_versions": 1,
            "max_data_versions": 1,
        }

        full_row = lookup_rows("//tmp/t", keys, versioned=True, retention_config=retention_config)[0]
        assert len(full_row.attributes["write_timestamps"]) == 2
        assert len(full_row.attributes["delete_timestamps"]) == 0
        assert len(full_row) == 3

        def _check(row):
            assert row.attributes["write_timestamps"] == full_row.attributes["write_timestamps"]
            assert row.attributes["delete_timestamps"] == full_row.attributes["delete_timestamps"]
            assert len(row) == 0

        actual = lookup_rows(
            "//tmp/t",
            keys,
            column_names=["value3"],
            versioned=True,
            retention_config=retention_config,
        )
        _check(actual[0])

        sync_flush_table("//tmp/t")

        actual = lookup_rows(
            "//tmp/t",
            keys,
            column_names=["value3"],
            versioned=True,
            retention_config=retention_config,
        )
        _check(actual[0])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("in_memory_mode", ["none", "uncompressed"])
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("new_scan_reader", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_stress_versioned_lookup(self, in_memory_mode, optimize_for, new_scan_reader, enable_hash_chunk_index):
        # This test checks that versioned lookup gives the same result for scan and lookup versioned formats.
        random.seed(12345)

        if in_memory_mode == "none" and optimize_for == "lookup":
            return

        if new_scan_reader and optimize_for != "scan":
            return

        if enable_hash_chunk_index and optimize_for == "scan":
            return

        schema = [
            {"name": "k1", "type": "int64", "sort_order": "ascending"},
            {"name": "k2", "type": "int64", "sort_order": "ascending"},
            {"name": "v1", "type": "int64"},
            {"name": "v2", "type": "int64"},
            {"name": "v3", "type": "int64"},
        ]

        delete_probability = 20  # percent
        value_probability = 70  # percent
        read_iters = 50
        lookup_iters = 50

        timestamps = []

        def _random_write(table, keys):
            global timestamps

            for key in keys:
                for v in "v1", "v2", "v3":
                    if random.randint(0, 99) < value_probability:
                        key.update({v: random.randint(1, 10)})
            insert_rows(table, keys)

        def _random_key():
            return {"k1": random.randint(1, 10), "k2": random.randint(1, 10)}

        sync_create_cells(1)
        self._create_simple_table("//tmp/expected", schema=schema, optimize_for="lookup")
        sync_mount_table("//tmp/expected")

        for _ in range(read_iters):
            keys = [_random_key() for _ in range(5)]
            if random.randint(0, 99) < delete_probability:
                delete_rows("//tmp/expected", keys)
            else:
                _random_write("//tmp/expected", keys)
            timestamps += [generate_timestamp()]

        sync_unmount_table("//tmp/expected")
        copy("//tmp/expected", "//tmp/actual")
        set("//tmp/actual/@optimize_for", optimize_for)
        set("//tmp/actual/@in_memory_mode", in_memory_mode)
        set("//tmp/actual/@enable_new_scan_reader_for_lookup", new_scan_reader)
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/actual")
        sync_mount_table("//tmp/expected")
        sync_mount_table("//tmp/actual")
        sync_compact_table("//tmp/actual")

        for i in range(lookup_iters):
            keys = [_random_key() for i in range(5)]
            ts = random.choice(timestamps)
            for versioned in True, False:
                expected = lookup_rows("//tmp/expected", keys, versioned=versioned, timestamp=ts)
                actual = lookup_rows("//tmp/actual", keys, versioned=versioned, timestamp=ts)
                assert expected == actual

    @authors("ifsmirnov")
    def test_versioned_lookup_dynamic_store(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_store_rotation", False)
        sync_mount_table("//tmp/t")

        timestamps = [generate_timestamp()]

        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        timestamps += [lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]]
        timestamps += [generate_timestamp()]

        delete_rows("//tmp/t", [{"key": 1}])
        timestamps += [lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["delete_timestamps"][0]]
        timestamps += [generate_timestamp()]

        insert_rows("//tmp/t", [{"key": 1, "value": "b"}])
        timestamps += [lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]]
        timestamps += [generate_timestamp()]

        delete_rows("//tmp/t", [{"key": 1}])
        timestamps += [lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["delete_timestamps"][0]]
        timestamps += [generate_timestamp()]

        assert timestamps == sorted(timestamps)

        # Check one lookup explicitly.
        result = lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=timestamps[6])[0]
        assert result.attributes["write_timestamps"] == [timestamps[5], timestamps[1]]
        assert result.attributes["delete_timestamps"] == [timestamps[3]]
        value = result["value"]
        assert len(value) == 2
        assert value[0].attributes["timestamp"] == timestamps[5]
        assert str(value[0]) == "b"
        assert value[1].attributes["timestamp"] == timestamps[1]
        assert str(value[1]) == "a"

        # Check all lookups against chunk stores.
        actual = [lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts) for ts in timestamps]

        set("//tmp/t/@enable_store_rotation", True)
        remount_table("//tmp/t")
        sync_freeze_table("//tmp/t")

        expected = [lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts) for ts in timestamps]

        assert expected == actual

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_versioned_lookup_unversioned_chunks(self, optimize_for):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "v1", "type": "int64"},
            {"name": "v2", "type": "int64"},
        ]

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(schema, strict=True, unique_keys=True),
                "optimize_for": optimize_for,
            },
        )

        timestamps = [generate_timestamp()]
        write_table("//tmp/t", [{"key": 0}])
        timestamps += [generate_timestamp()]
        write_table(
            "<append=true>//tmp/t",
            [{"key": 1, "v1": 1}, {"key": 2, "v2": 2}],
            append=True,
        )
        timestamps += [generate_timestamp()]
        write_table("<append=true>//tmp/t", [{"key": 3, "v1": 3, "v2": 4}], append=True)
        timestamps += [generate_timestamp()]

        assert len(read_table("//tmp/t"))

        sync_create_cells(1)
        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        def _check(expected, actual):
            assert len(expected) == len(actual)
            for row in actual:
                key = row["key"]
                if row.attributes["write_timestamps"]:
                    assert key in expected
                    for column in "v1", "v2":
                        values = row.get(column)
                        if values is None:
                            assert column not in expected[key]
                            continue
                        assert len(values) == 1
                        if type(values[0]) is yson.YsonEntity:
                            assert column not in expected[key]
                        else:
                            assert int(values[0]) == expected[key][column]
                else:
                    assert key not in expected

        expected = {}
        keys = [{"key": i} for i in range(4)]

        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        _check(expected, actual)

        expected[0] = {}
        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        _check(expected, actual)

        expected[1] = {"v1": 1}
        expected[2] = {"v2": 2}
        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        _check(expected, actual)

        expected[3] = {"v1": 3, "v2": 4}
        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        _check(expected, actual)

    @authors("ifsmirnov")
    def test_versioned_lookup_early_timestamp(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_store_rotation", False)
        sync_mount_table("//tmp/t")

        ts = generate_timestamp()
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])

        assert lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts) == []

        set("//tmp/t/@enable_store_rotation", True)
        remount_table("//tmp/t")
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts) == []

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_versioned_lookup_early_timestamp_after_alter(self, optimize_for):
        sync_create_cells(1)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": make_schema(schema, strict=True, unique_keys=True),
                "optimize_for": optimize_for,
            },
        )

        ts0 = generate_timestamp()
        write_table("//tmp/t", [{"key": 1, "value": "a"}])
        ts1 = generate_timestamp()

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts0) == []
        assert len(lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts1)) == 1

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_lookup_from_chunks(self, optimize_for, enable_hash_chunk_index):
        if enable_hash_chunk_index and optimize_for == "scan":
            return

        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)

        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")

        pivots = [[]] + [[x] for x in range(100, 1000, 100)]
        sync_reshard_table("//tmp/t", pivots)
        assert self._get_pivot_keys("//tmp/t") == pivots

        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(0, 1000, 2)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        actual = lookup_rows("//tmp/t", [{"key": i} for i in range(0, 1000)])
        assert_items_equal(actual, rows)

        rows = [{"key": i, "value": str(i)} for i in range(1, 1000, 2)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(0, 1000)]
        actual = lookup_rows("//tmp/t", [{"key": i} for i in range(0, 1000)])
        assert_items_equal(actual, rows)

        for tablet in range(10):
            path = "//tmp/t/@tablets/{0}/performance_counters/static_chunk_row_lookup_count".format(tablet)
            wait(lambda: get(path) > 0)
            assert get(path) == 100

    @authors("ifsmirnov")
    def test_lookup_rich_ypath(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert list(lookup_rows("//tmp/t", [{"key": 1}])) == []
        with pytest.raises(YtError):
            lookup_rows("//tmp/t[1:2]", [{"key": 1}])
        with pytest.raises(YtError):
            lookup_rows("//tmp/t{key}", [{"key": 1}])

    @authors("ifsmirnov")
    def test_lookup_from_multiple_nodes(self):
        cells_per_node = 4
        # Let some nodes be only partially occupied.
        cell_count = cells_per_node * self.NUM_NODES - self.NUM_NODES // 2
        sync_create_cells(cell_count)

        random.seed(1234)

        tablet_count = int(cell_count * 2.5)
        key_count = 1000
        pivot_keys = [[]] + [[k] for k in sorted(random.sample(list(range(1, key_count)), tablet_count - 1))]
        self._create_simple_table("//tmp/t", pivot_keys=pivot_keys)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(key_count)]
        keys = [{"key": i} for i in range(key_count)]

        insert_rows("//tmp/t", rows)
        assert lookup_rows("//tmp/t", keys) == rows

    @authors("akozhikhov")
    def test_hedging_manager_sensors(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", chunk_reader={
            "hedging_manager": {
                "max_backup_request_ratio": 0.5,
            },
            "prefer_local_replicas": False,
            "use_block_cache": False,
            "use_uncompressed_block_cache": False,
        })
        sync_mount_table("//tmp/t")

        request_counter = profiler_factory().at_tablet_node("//tmp/t").counter(
            name="hedging_manager/primary_request_count")

        keys = [{"key": i} for i in range(10)]
        rows = [{"key": i, "value": str(i)} for i in range(10)]

        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")
        assert lookup_rows("//tmp/t", keys) == rows

        time.sleep(1)

        assert lookup_rows("//tmp/t", keys) == rows

        wait(lambda: request_counter.get_delta() > 0)

    @authors("akozhikhov")
    def test_lookup_row_count_sensors(self):
        table_path = "//tmp/very_unique_t"
        sync_create_cells(1)
        self._create_simple_table(table_path)
        sync_mount_table(table_path)

        sensors = [None] * 4

        def _init_sensors():
            sensors[0] = profiler_factory().at_tablet_node(
                table_path,
                fixed_tags={"table_path": table_path}).counter(name="lookup/row_count")
            sensors[1] = profiler_factory().at_tablet_node(
                table_path,
                fixed_tags={"table_path": table_path}).counter(name="lookup/missing_row_count")
            sensors[2] = profiler_factory().at_tablet_node(
                table_path,
                fixed_tags={"table_path": table_path}).counter(name="lookup/unmerged_row_count")
            sensors[3] = profiler_factory().at_tablet_node(
                table_path,
                fixed_tags={"table_path": table_path}).counter(name="lookup/unmerged_missing_row_count")
            for sensor in sensors:
                if sensor.start_value != 0:
                    return False
            return True
        wait(lambda: _init_sensors())

        insert_rows(table_path, [{"key": 0, "value": "0"}, {"key": 2, "value": "2"}])
        sync_flush_table(table_path)

        insert_rows(table_path, [{"key": 1, "value": "1"}, {"key": 2, "value": "22"}])

        assert lookup_rows(table_path, [{"key": 0}, {"key": 1}, {"key": 2}, {"key": 3}]) == \
            [{"key": 0, "value": "0"}, {"key": 1, "value": "1"}, {"key": 2, "value": "22"}]

        wait(lambda: sensors[0].get_delta() == 3)
        wait(lambda: sensors[1].get_delta() == 1)
        wait(lambda: sensors[2].get_delta() == 4)
        wait(lambda: sensors[3].get_delta() == 4)

    @authors("akozhikhov")
    def test_lookup_overflow(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", chunk_reader={
            "prefer_local_replicas": False,
            "use_block_cache": False,
            "use_uncompressed_block_cache": False,
            "window_size": 5,
            "group_size": 5,
        })
        set("//tmp/t/@chunk_writer", {"block_size": 5})
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(2000)]
        rows = [{"key": i, "value": str(i)} for i in range(2000)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", keys) == rows

    @authors("akozhikhov")
    def test_lookup_from_suspicious_node(self):
        set("//sys/@config/tablet_manager/store_chunk_reader", {"probe_peer_count": self.NUM_NODES - 1})

        self._separate_tablet_and_data_nodes()
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=self.NUM_NODES - 1)
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        set("//tmp/t/@chunk_writer", {"upload_replication_factor": self.NUM_NODES - 1})
        set("//tmp/t/@chunk_reader", {"use_block_cache": False, "use_uncompressed_block_cache": False})
        sync_mount_table("//tmp/t")

        row = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", row)
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 1}]) == row

        set_node_banned(self._nodes[1], True)

        # Banned node is marked as suspicious and will be avoided within next lookup.
        assert lookup_rows("//tmp/t", [{"key": 1}]) == row

        assert lookup_rows("//tmp/t", [{"key": 1}]) == row

        set_node_banned(self._nodes[1], False)

        # Node shall not be suspicious anymore.
        assert lookup_rows("//tmp/t", [{"key": 1}]) == row

    def _get_key_filter_lookup_checker(self, table):
        self_ = self

        class Checker:
            def __init__(self):
                self.profiling = self_._get_key_filter_profiling_wrapper("lookup", table)

            def check(self, lookup_keys, expected):
                missing_key_count = len(lookup_keys) - len(expected)

                def _check_counters():
                    input, filtered_out, false_positive = self.profiling.get_deltas()
                    return input == len(lookup_keys) and filtered_out + false_positive == missing_key_count

                assert lookup_rows(table, lookup_keys, verbose=False) == expected
                wait(_check_counters)
                self.profiling.commit()

        return Checker()

    @authors("ifsmirnov", "akozhikhov", "dave11ar")
    def test_key_filter(self):
        chunk_writer_config = {
            "key_filter" : {
                "block_size": 100,
                "enable": True,
            },
            "key_prefix_filter": {
                "block_size": 100,
                "enable": True,
                "prefix_lengths": [1],
            }
        }

        sync_create_cells(1)

        table_path = f"//tmp/t{generate_uuid()}"

        self._create_simple_table(
            table_path,
            chunk_writer=chunk_writer_config,
            mount_config={
                "enable_key_filter_for_lookup": True,
            },
        )
        sync_mount_table(table_path)

        keys = [{"key": i} for i in range(10000)]
        rows = [{"key": i, "value": str(i)} for i in range(1, 9999, 2)]
        insert_rows(table_path, rows)
        sync_flush_table(table_path)

        key_filter_checker = self._get_key_filter_lookup_checker(table_path)

        key_filter_checker.check([{"key": 2}, {"key": 42}], [])
        key_filter_checker.check([{"key": 0}, {"key": 1}], [{"key": 1, "value": "1"}],)
        key_filter_checker.check([{"key": 5000}], [])
        key_filter_checker.check([{"key": 42}, {"key": 322}, {"key": 9997}], [{"key": 9997, "value": "9997"}])
        key_filter_checker.check([{"key": 9998}], [])

        key_filter_checker.check(keys, rows)

    @authors("akozhikhov", "dave11ar")
    def test_key_filter_with_schema_alter(self):
        schema1 = [
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        schema2 = [
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]

        sync_create_cells(1)

        table_path = f"//tmp/t{generate_uuid()}"

        self._create_simple_table(
            table_path,
            schema=schema1,
            chunk_writer={
                "key_filter" : {
                    "block_size": 100,
                    "enable": True,
                },
            },
            mount_config={
                "enable_key_filter_for_lookup": True,
            },
        )

        sync_mount_table(table_path)

        insert_rows(table_path, [{"key1": 0, "value": "0"}])
        sync_flush_table(table_path)

        key_filter_checker = self._get_key_filter_lookup_checker(table_path)

        key_filter_checker.check([{"key1": 0}], [{"key1": 0, "value": "0"}])

        sync_unmount_table(table_path)
        alter_table(table_path, schema=schema2)
        sync_mount_table(table_path)

        key_filter_checker.check(
            [{"key1": 0, "key2": yson.YsonEntity()}],
            [{"key1": 0, "key2": yson.YsonEntity(), "value": "0"}],
        )

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for, chunk_format", [
        ("lookup", "table_versioned_slim"),
        ("lookup", "table_versioned_simple"),
        ("scan", "table_versioned_columnar"),
    ])
    def test_any_values_madness(self, optimize_for, chunk_format):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any"},
            {"name": "value2", "type": "any"},
        ]

        rows = [{"key": 0, "value": "zero", "value2": "start"}, {"key": 1, "value": [{}, {}], "value2": "finish"}]

        create("table", "//tmp/in")
        write_table("//tmp/in", rows)

        schema = make_schema(
            schema,
            unique_keys=True,
        )
        self._create_simple_static_table(
            "//tmp/t",
            schema=schema,
        )

        merge(in_="//tmp/in", out="//tmp/t", mode="ordered")

        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@optimize_for", optimize_for)
        set("//tmp/t/@chunk_format", chunk_format)
        set("//tmp/t/@in_memory_mode", "compressed")

        sync_create_cells(1)
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@preload_state") == "complete")

        assert lookup_rows("//tmp/t", [{"key": 0}, {"key": 1}]) == rows

        insert_rows("//tmp/t", [{"key": 1, "value": [{}, {}, {}], "value2": "start_finish"}], aggregate=True)
        rows[1] = {"key": 1, "value": [{}, {}, {}], "value2": "start_finish"}

        sync_compact_table("//tmp/t")

        create("table", "//tmp/out")
        merge(in_="//tmp/t", out="//tmp/out", mode="ordered")

        assert get("//tmp/t/@chunk_ids") != get("//tmp/out/@chunk_ids")
        assert read_table("//tmp/out") == rows

        sort(
            in_="//tmp/t",
            out="//tmp/out",
            sort_by=["key"],
            spec={
                "partition_count": 2,
            },
        )

        assert read_table("//tmp/out") == rows

    @authors("dave11ar")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("reader_type", ["dynamic_store", "chunk", "data_node_lookup"])
    def test_lookup_with_timestamp_columns(self, optimize_for, reader_type):
        path = "//tmp/t"

        sync_create_cells(1)

        create_dynamic_table(
            path,
            schema=[
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                {"name": "v1", "type": "string"},
                {"name": "v2", "type": "string"},
                {"name": "v3", "type": "string"},
                {"name": "v4", "type": "int64", "aggregate": "sum"},
                {"name": "v5", "type": "string"},
            ],
            optimize_for=optimize_for,
        )

        need_to_flush = False

        def _maybe_flush():
            if need_to_flush:
                sync_flush_table(path)

        if reader_type == "dynamic_store":
            set(f"{path}/@enable_store_rotation", False)
        elif reader_type == "chunk":
            need_to_flush = True
        elif reader_type == "data_node_lookup":
            need_to_flush = True
            self._enable_data_node_lookup(path)
        else:
            assert False

        sync_mount_table(path)

        insert_rows(
            path=path,
            data=[{"k": 1, "v1": "a", "v2": "a", "v3": "a", "v4": 1, "v5": "a"}],
            update=False,
        )
        insert_rows(
            path=path,
            data=[{"k": 1, "v1": "b", "v3": "b"}],
            update=True,
        )
        insert_rows(
            path=path,
            data=[{"k": 1, "v2": "c", "v5": "c"}],
            update=True,
        )
        _maybe_flush()

        def check_vs(row, v1=None, v2=None, v3=None, v4=None, v5=None):
            def check(name, value):
                if value:
                    assert row[name] == value
                else:
                    assert name not in row

            assert row["k"] == 1
            check("v1", v1)
            check("v2", v2)
            check("v3", v3)
            check("v4", v4)
            check("v5", v5)

        def check_timestamps(row, order):
            previous_ts = 0
            for columns in order:
                current_ts = row[f"$timestamp:{columns[0]}"]
                assert previous_ts < current_ts

                for i in range(1, len(columns)):
                    assert row[f"$timestamp:{columns[i]}"] == current_ts

                previous_ts = current_ts

        def lookup_row_checked(**kwargs):
            rows = lookup_rows(path, [{"k": 1}], **kwargs, with_timestamps=True)
            assert len(rows) == 1
            return rows[0]

        row = lookup_row_checked()
        check_vs(row, "b", "c", "b", 1, "c")
        check_timestamps(row, [["v4"], ["v1", "v3"], ["v2", "v5"]])

        first_timestamp = row["$timestamp:v4"]
        row = lookup_row_checked(timestamp=first_timestamp)
        check_vs(row, "a", "a", "a", 1, "a")
        check_timestamps(row, [["v1", "v2", "v3", "v4", "v5"]])
        assert row["$timestamp:v1"] == first_timestamp

        row = lookup_row_checked(column_names=["k", "v1", "$timestamp:v1", "v2", "v3", "$timestamp:v5", "v4", "v5", "$timestamp:v4", "$timestamp:v3"])
        check_vs(row, "b", "c", "b", 1, "c")
        check_timestamps(row, [["v4"], ["v1", "v3"], ["v5"]])

        row = lookup_row_checked(column_names=["k", "$timestamp:v2", "$timestamp:v1", "$timestamp:v5", "$timestamp:v3", "$timestamp:v4"])
        check_vs(row)
        check_timestamps(row, [["v4"], ["v1", "v3"], ["v2", "v5"]])

        row = lookup_row_checked(column_names=["k", "$timestamp:v2", "v1", "v4", "$timestamp:v3", "$timestamp:v4"])
        check_vs(row, v1="b", v4=1)
        check_timestamps(row, [["v4"], ["v3"], ["v2"]])

        with raises_yt_error('Versioned lookup does not support versioned read mode "latest_timestamp"'):
            lookup_rows(path, [{"k": 1}], versioned=True, with_timestamps=True)

        with raises_yt_error('No such column "$timestamp:v3"'):
            lookup_rows(path, [{"k": 1}], column_names=["k", "v1", "v2", "$timestamp:v3", "v4"])

        with raises_yt_error('No such column "$timestamp:v42"'):
            lookup_rows(path, [{"k": 1}], column_names=["k", "v1", "v2", "$timestamp:v42", "v4"])

        def check_aggregate(aggregate, v1, v4, expected_v4, last_value):
            insert_rows(
                path=path,
                data=[{"k": 1, "v1": v1, "v4": v4}],
                update=True,
                aggregate=aggregate,
            )
            _maybe_flush()

            row = lookup_row_checked(column_names=["k", "$timestamp:v2", "v1", "v4", "v3", "v2", "v5", "$timestamp:v3", "$timestamp:v4", "$timestamp:v1", "$timestamp:v5"])
            check_vs(row, v1, "c", "b", expected_v4, "c")
            check_timestamps(row, [["v3"], ["v2", "v5"], ["v1", "v4"]])

            new_last_value = row["$timestamp:v4"]
            assert new_last_value > last_value
            return new_last_value

        check_aggregate(
            aggregate=True,
            v1="e",
            v4=4,
            expected_v4=7,
            last_value=check_aggregate(
                aggregate=False,
                v1="d",
                v4=3,
                expected_v4=3,
                last_value=row["$timestamp:v4"],
            ),
        )

        delete_rows(path, [{"k": 1}])
        _maybe_flush()
        assert lookup_rows(
            path,
            [{"k": 1}],
            column_names=["k", "$timestamp:v2", "v1", "v4", "v3", "v2", "v5", "$timestamp:v3", "$timestamp:v4", "$timestamp:v1", "$timestamp:v5"],
            with_timestamps=True,
        ) == []

    @authors("sabdenovch")
    @pytest.mark.parametrize("in_memory", [False, True])
    def test_migrate_to_query_pool(self, in_memory):
        set("//sys/cluster_nodes/@config", {"%true": {
            "query_agent": {
                f"use_query_pool_for{'_in_memory' if in_memory else ''}_lookups": True
            }
        }})
        sync_create_cells(1)

        table_path = "//tmp/t"

        self._create_simple_table(
            table_path,
            mount_config={
                "enable_key_filter_for_lookup": True,
            },
            in_memory_mode="uncompressed" if in_memory else "none",
        )
        sync_mount_table(table_path)

        keys = [{"key": i} for i in range(10000)]
        rows = [{"key": i, "value": str(i)} for i in range(1, 9999, 2)]
        insert_rows(table_path, rows)

        assert rows == lookup_rows(table_path, keys)


@pytest.mark.enabled_multidaemon
class TestAlternativeLookupMethods(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 2

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_simple(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=1)
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(1)]
        rows = [{"key": i, "value": str(i) * 2} for i in range(1)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", []) == []
        assert lookup_rows("//tmp/t", keys) == rows

        # TableSchema is cached on data node now
        assert lookup_rows("//tmp/t", keys) == rows

        # Nothing is returned upon nonexistent key request.
        keys.append({"key": 3})
        assert lookup_rows("//tmp/t", keys) == rows

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_with_alter(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        for iter in range(3):
            rows = [{"key": i, "value": str(i)} for i in range(iter * 3, (iter + 1) * 3)]
            insert_rows("//tmp/t", rows)
            sync_flush_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 3

        keys = [{"key": 1}]
        rows = [{"key": 1, "value": str(1)}]
        assert lookup_rows("//tmp/t", keys) == rows

        keys = [{"key": i} for i in [0, 3, 6]]
        rows = [{"key": i, "value": str(i)} for i in [0, 3, 6]]
        assert lookup_rows("//tmp/t", keys) == rows

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in [0, 3, 6]]
        rows = [{"key": i, "key2": yson.YsonEntity(), "value": str(i)} for i in [0, 3, 6]]
        assert lookup_rows("//tmp/t", keys) == rows

        keys = [{"key": i, "key2": yson.YsonEntity()} for i in [0, 3, 6]]
        rows = [{"key": i, "key2": yson.YsonEntity(), "value": str(i)} for i in [0, 3, 6]]
        assert lookup_rows("//tmp/t", keys) == rows

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_overlapping_chunks(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        for iter in range(3):
            rows = [{"key": i, "value": str(i + iter)} for i in range(iter * 2, (iter + 1) * 2 + 1)]
            insert_rows("//tmp/t", rows)
            sync_flush_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 3

        keys = [{"key": i} for i in range(7)]
        values = [0, 1, 3, 4, 6, 7, 8]
        rows = [{"key": i, "value": str(values[i])} for i in range(7)]

        assert lookup_rows("//tmp/t", keys) == rows

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
            {"name": "value2", "type": "boolean"},
        ])
        sync_mount_table("//tmp/t")

        rows = [
            {
                "key": i,
                "key2": yson.YsonEntity(),
                "value": str(values[i]),
                "value2": yson.YsonEntity(),
            }
            for i in range(7)
        ]
        assert lookup_rows("//tmp/t", keys) == rows

        rows = [{"key2": yson.YsonEntity(), "value": str(values[i])} for i in range(7)]
        assert lookup_rows("//tmp/t", keys, column_names=["key2", "value"]) == rows

        rows = [{"key": i, "value2": yson.YsonEntity()} for i in range(7)]
        assert lookup_rows("//tmp/t", keys, column_names=["key", "value2"]) == rows

        rows = [{"key": i, "value": str(values[i])} for i in range(7)]
        assert (
            lookup_rows(
                "//tmp/t",
                keys + [{"key": 10}],
                column_names=["key", "value"],
                keep_missing_rows=True,
            )
            == rows + [None]
        )

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_with_timestamp(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "one"}])
        write_ts_1 = lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]

        insert_rows("//tmp/t", [{"key": 2, "value": "two"}])
        write_ts_2 = lookup_rows("//tmp/t", [{"key": 2}], versioned=True)[0].attributes["write_timestamps"][0]

        assert write_ts_2 > write_ts_1

        sync_flush_table("//tmp/t")

        assert write_ts_1 == lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]
        assert write_ts_2 == lookup_rows("//tmp/t", [{"key": 2}], versioned=True)[0].attributes["write_timestamps"][0]
        assert lookup_rows("//tmp/t", [{"key": 1}], timestamp=write_ts_1) == [{"key": 1, "value": "one"}]

        insert_rows("//tmp/t", [{"key": 1, "value": "oneone"}])
        sync_flush_table("//tmp/t")
        assert write_ts_1 < lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]

        assert lookup_rows("//tmp/t", [{"key": 1}], timestamp=write_ts_1) == [{"key": 1, "value": "one"}]

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_stress(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        seq_keys = list(range(50))
        seq_values = list(range(1000))

        current_value = {}

        for _ in range(25):
            keys_subset = random.sample(seq_keys, 10)
            values_subset = random.sample(seq_values, 10)

            for i in range(10):
                current_value[keys_subset[i]] = str(values_subset[i])

            rows = [{"key": keys_subset[i], "value": str(values_subset[i])} for i in range(10)]
            insert_rows("//tmp/t", rows)
            sync_flush_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 25

        expected_keys = []
        expected_values = []
        for key, value in current_value.items():
            expected_keys.append({"key": key})
            expected_values.append({"key": key, "value": value})

        assert lookup_rows("//tmp/t", expected_keys) == expected_values

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_local_reader(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=self.NUM_NODES)
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        if exists("//tmp/t/@chunk_reader"):
            set("//tmp/t/@chunk_reader/prefer_local_replicas", True)
            set("//tmp/t/@chunk_reader/prefer_local_host", True)
        else:
            set("//tmp/t/@chunk_reader", {"prefer_local_replicas": True, "prefer_local_host": True})
        set("//tmp/t/@enable_compaction_and_partitioning", False)

        sync_mount_table("//tmp/t")

        row = [{"key": 1, "value": "one"}]
        insert_rows("//tmp/t", row)
        sync_flush_table("//tmp/t")
        assert lookup_rows("//tmp/t", [{"key": 1}]) == row

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_parallel_alternative_lookup_stress(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=1, lookup_cache_rows_per_tablet=5)
        self._create_partitions(partition_count=5)
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        reference = {}
        for i in range(0, 10):
            rows = [{"key": j, "value": str(random.randint(0, 100))} for j in range(i, i + 2)]
            for row in rows:
                reference[row["key"]] = row["value"]
            insert_rows("//tmp/t", rows)
            sync_flush_table("//tmp/t")

        random_keys = []
        random_rows = []
        all_keys = []
        all_rows = []

        for key, value in reference.items():
            all_keys.append({"key": key})
            all_rows.append({"key": key, "value": value})
            if bool(random.getrandbits(1)):
                random_keys.append({"key": key})
                random_rows.append({"key": key, "value": value})

        for use_lookup_cache in [False, True, True]:
            assert lookup_rows("//tmp/t", [], use_lookup_cache=use_lookup_cache) == []
            assert lookup_rows("//tmp/t", random_keys, use_lookup_cache=use_lookup_cache) == random_rows
        for use_lookup_cache in [False, True, True]:
            assert lookup_rows("//tmp/t", all_keys, use_lookup_cache=use_lookup_cache) == all_rows

    @authors("akozhikhov")
    def test_error_upon_net_throttler_overdraft(self):
        return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t", chunk_reader={
            "enable_local_throttling": True,
            "use_block_cache": False,
            "use_uncompressed_block_cache": False,
            "prefer_local_replicas": False,
        })
        sync_mount_table("//tmp/t")

        row = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", row)
        sync_flush_table("//tmp/t")

        update_nodes_dynamic_config({
            "tablet_node": {
                "throttlers": {
                    "user_backend_in": {
                        "limit": 50,
                    }
                }
            }
        })

        assert lookup_rows("//tmp/t", [{"key": 1}]) == row
        with raises_yt_error(yt_error_codes.RequestThrottled):
            lookup_rows("//tmp/t", [{"key": 1}])

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_hedging_options(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        if exists("//tmp/t/@chunk_reader"):
            set("//tmp/t/@chunk_reader/lookup_rpc_hedging_delay", 0)
        else:
            set("//tmp/t/@chunk_reader", {"lookup_rpc_hedging_delay": 0})
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(1)]
        rows = [{"key": i, "value": str(i) * 2} for i in range(1)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        for _ in range(5):
            assert lookup_rows("//tmp/t", keys) == rows

        remove("//tmp/t/@chunk_reader/lookup_rpc_hedging_delay")
        set("//tmp/t/@chunk_reader/hedging_manager", {"max_backup_request_ratio": 1.0, "max_hedging_delay": 0})
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        for _ in range(5):
            assert lookup_rows("//tmp/t", keys) == rows

    @authors("akozhikhov")
    @pytest.mark.parametrize("enable_data_node_lookup", [False, True])
    @pytest.mark.parametrize("enable_hash_chunk_index", [False, True])
    def test_alternative_lookup_performance_counters(self, enable_data_node_lookup, enable_hash_chunk_index):
        if not enable_data_node_lookup and not enable_hash_chunk_index:
            return

        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        if enable_data_node_lookup:
            self._enable_data_node_lookup("//tmp/t")
        if enable_hash_chunk_index:
            self._enable_hash_chunk_index("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        lookup_rows("//tmp/t", [{"key": 0}, {"key": 1}])

        row_count = "//tmp/t/@tablets/0/performance_counters/static_chunk_row_lookup_count"
        wait(lambda: get(row_count) > 0)
        assert get(row_count) == 1


@pytest.mark.enabled_multidaemon
class TestLookupWithRelativeNetworkThrottler(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_NODES = 2

    DELTA_NODE_CONFIG = {
        "network_bandwidth": 1000,
    }

    @authors("akozhikhov")
    def test_lookup_with_relative_network_throttler(self):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=1, chunk_reader={
            "use_block_cache": False,
            "use_uncompressed_block_cache": False,
            "prefer_local_replicas": False
        })

        sync_mount_table("//tmp/t")

        row = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", row)
        sync_flush_table("//tmp/t")

        def _check():
            start_time = time.time()
            for _ in range(3):
                assert lookup_rows("//tmp/t", [{"key": 1}]) == row
            return time.time() - start_time

        update_nodes_dynamic_config({
            "default": {
                "limit": 1000,
            }
        }, path="out_throttlers", replace=True)

        # TODO(akozhikhov): Check if overdraft instead.
        assert _check() < 1

        update_nodes_dynamic_config({
            "default": {
                "relative_limit": 0.1,
            }
        }, path="out_throttlers", replace=True)

        assert _check() > 1

################################################################################


@pytest.mark.enabled_multidaemon
class TestLookupCache(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 2

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "timestamp_provider": {
                "update_period": 100,
            },
        },
        "resource_limits": {
            "memory_limits": {
                "lookup_rows_cache": {
                    "type": "static",
                    "value": 1 * 1024 * 1024,
                },
            },
        },
    }

    COUNTER_NAME = "lookup"

    def _read(self, table, keys, column_names=None, **kwargs):
        return lookup_rows(table, [{"key": key} for key in keys], column_names=column_names, **kwargs)

    def _create_simple_table(self, path, hunks, **kwargs):
        value_column_schema = {"name": "value", "type": "string"}
        if hunks:
            value_column_schema["max_inline_hunk_size"] = 12

        create_dynamic_table(
            path,
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                value_column_schema,
            ],
            **kwargs)

    @authors("lukyan")
    @pytest.mark.parametrize("hunks", [False, True])
    def test_lookup_cache(self, hunks):
        sync_create_cells(1)

        def _make_value(i):
            return str(i) + ("payload" * (i % 5) if hunks else "")

        self._create_simple_table("//tmp/t", hunks, lookup_cache_rows_per_tablet=50)
        set("//tmp/t/@mount_config/insert_meta_upon_store_update", False)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": _make_value(i)} for i in range(0, 1000, 2)]
        insert_rows("//tmp/t", rows)

        sync_flush_table("//tmp/t")

        for step in range(1, 5):
            expected = [{"key": i, "value": _make_value(i)} for i in range(100, 200, 2 * step)]
            actual = self._read("//tmp/t", range(100, 200, 2 * step), use_lookup_cache=True)
            assert_items_equal(actual, expected)

        # Lookup key without polluting cache to increment static_chunk_row_lookup_count.
        self._read("//tmp/t", [2])

        path = "//tmp/t/@tablets/0/performance_counters/static_chunk_row_lookup_count"
        wait(lambda: get(path) > 50)
        assert get(path) == 51

        # Modify some rows.
        rows = [{"key": i, "value": _make_value(i + 1)} for i in range(100, 200, 2)]
        insert_rows("//tmp/t", rows)

        # Check lookup result.
        actual = self._read("//tmp/t", range(100, 200, 2), use_lookup_cache=True)
        assert_items_equal(actual, rows)

        # Flush table.
        sync_flush_table("//tmp/t")

        # And check that result after flush is equal.
        actual = self._read("//tmp/t", range(100, 200, 2), use_lookup_cache=True)
        assert_items_equal(actual, rows)

        # Lookup key without cache.
        self._read("//tmp/t", [2])

        wait(lambda: get(path) > 51)
        assert get(path) == 52

        node = get_tablet_leader_address(get("//tmp/t/@tablets/0/tablet_id"))
        sync_unmount_table("//tmp/t")
        wait(lambda: get(f"//sys/cluster_nodes/{node}/@statistics/memory/lookup_rows_cache/used") == 0)

    @authors("lukyan")
    @pytest.mark.parametrize("hunks", [False, True])
    def test_lookup_cache_options(self, hunks):
        sync_create_cells(1)

        def _make_value(i):
            return str(i) + ("payload" * (i % 5) if hunks else "")

        self._create_simple_table("//tmp/t", hunks)

        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": _make_value(i)} for i in range(0, 1000, 2)]
        insert_rows("//tmp/t", rows)

        sync_flush_table("//tmp/t")

        # Cache is not configured yet.
        actual = self._read("//tmp/t", range(100, 200, 2), use_lookup_cache=False)
        expected = [{"key": i, "value": _make_value(i)} for i in range(100, 200, 2)]
        assert_items_equal(actual, expected)

        # Lookup key without polluting cache to increment static_chunk_row_lookup_count.
        self._read("//tmp/t", [2])

        path = "//tmp/t/@tablets/0/performance_counters/static_chunk_row_lookup_count"
        wait(lambda: get(path) > 50)
        assert get(path) == 51

        set("//tmp/t/@lookup_cache_rows_ratio", 0.1)
        set("//tmp/t/@enable_lookup_cache_by_default", True)
        remount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
        address = get_cell_leader_address(cell_id)

        def _check_tablet_config():
            return get(f"//sys/cluster_nodes/{address}/orchid/tablet_cells/{cell_id}/tablets/{tablet_id}/config/enable_lookup_cache_by_default")

        wait(_check_tablet_config)

        # Populate cache and use it.
        for step in range(1, 5):
            expected = [{"key": i, "value": _make_value(i)} for i in range(100, 200, 2 * step)]
            actual = self._read("//tmp/t", range(100, 200, 2 * step))
            assert_items_equal(actual, expected)

        # Lookup key without cache.
        self._read("//tmp/t", [2], use_lookup_cache=False)

        path = "//tmp/t/@tablets/0/performance_counters/static_chunk_row_lookup_count"
        wait(lambda: get(path) > 101)
        assert get(path) == 102

    @authors("lukyan")
    @pytest.mark.parametrize("hunks", [False, True])
    def test_lookup_cache_flush(self, hunks):
        sync_create_cells(1)

        def _make_value(i):
            return str(i) + ("payload" * (i % 5) if hunks else "")

        self._create_simple_table("//tmp/t", hunks, lookup_cache_rows_per_tablet=50)

        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": _make_value(i)} for i in range(0, 300, 2)]
        insert_rows("//tmp/t", rows)

        expected = [{"key": i, "value": _make_value(i)} for i in range(100, 200, 2)]
        actual = self._read("//tmp/t", range(100, 200, 2), use_lookup_cache=True)
        assert_items_equal(actual, expected)

        # Insert rows again to increase last store timestamp.
        rows = [{"key": i, "value": _make_value(2 * i)} for i in range(0, 300, 4)]
        insert_rows("//tmp/t", rows)

        sync_flush_table("//tmp/t")

        # Lookup again. Check that rows are in cache.
        expected = [{"key": i, "value": _make_value(2 * i if i % 4 == 0 else i)} for i in range(100, 200, 2)]
        actual = self._read("//tmp/t", range(100, 200, 2), use_lookup_cache=True)
        assert_items_equal(actual, expected)

        # Lookup key without cache.
        self._read("//tmp/t", [2])

        path = "//tmp/t/@tablets/0/performance_counters/static_chunk_row_lookup_count"
        wait(lambda: get(path) > 0)
        assert get(path) == 1

    @authors("lukyan")
    @pytest.mark.timeout(200)
    @pytest.mark.parametrize("hunks", [False, True])
    def test_lookup_cache_stress(self, hunks):
        sync_create_cells(1)

        def _make_value(i):
            return str(i) + ("payload" * (i % 5) if hunks else "")

        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "v", "type": "int64"},
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"},
                {"name": "s", "type": "string", "max_inline_hunk_size": 12 if hunks else None},
                {"name": "t", "type": "string", "max_inline_hunk_size": 12 if hunks else None}],
            lookup_cache_rows_per_tablet=100)

        sync_mount_table("//tmp/t")

        count = 500

        # Decorate to simplify grep.
        def _decorate_key(key):
            return 12300000 + key

        for wave in range(1, 30):
            rows = [{
                "key": _decorate_key(k),
                "v": wave * count + k,
                choice(["a", "b", "c"]): randint(1, 10000),
                choice(["s", "t"]): _make_value(randint(1, 10000))}
                for k in sample(list(range(1, count)), 200)]
            insert_rows("//tmp/t", rows, update=True)
            print_debug("Insert rows ", rows)

            keys = [{"key": _decorate_key(k)} for k in sample(list(range(1, count)), 100)]
            delete_rows("//tmp/t", keys)
            print_debug("Delete rows ", keys)

            for _ in range(1, 10):
                keys = [_decorate_key(k) for k in sample(list(range(1, count)), 10)]

                ts = generate_timestamp()
                no_cache = self._read("//tmp/t", keys, timestamp=ts)
                cache = self._read("//tmp/t", keys, use_lookup_cache=True, timestamp=ts)
                assert no_cache == cache

            sync_flush_table("//tmp/t")

        tablet_profiling = self._get_table_profiling("//tmp/t")
        assert tablet_profiling.get_counter(f"{self.COUNTER_NAME}/cache_hits") > 0
        assert tablet_profiling.get_counter(f"{self.COUNTER_NAME}/cache_misses") > 0

    @authors("lukyan")
    @pytest.mark.timeout(200)
    @pytest.mark.parametrize("hunks", [False, True])
    def test_lookup_cache_stress2(self, hunks):
        sync_create_cells(1)

        def _make_value(i):
            return str(i) + ("payload" * (i % 5) if hunks else "")

        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "v", "type": "int64"},
                {"name": "i", "type": "int64"},
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"},
                {"name": "s", "type": "string", "max_inline_hunk_size": 12 if hunks else None},
                {"name": "t", "type": "string", "max_inline_hunk_size": 12 if hunks else None},
                {"name": "md5", "type": "string"}],
            lookup_cache_rows_per_tablet=100)

        sync_mount_table("//tmp/t")

        count = 500

        verify_map = {}
        revision_map = {}

        optional_columns = ["a", "b", "c", "s", "t"]
        required_columns = ["v", "i"]

        def _get_checksum(row):
            row_data = " ".join(str(yson.dumps(row.get(col, yson.YsonEntity())))
                                for col in (required_columns + optional_columns))
            return row_data

        def _check_row(row, check):
            check_values = dict(list(zip(required_columns + optional_columns, check.split(" "))))
            for name, value in row.items():
                if name in ["key", "md5"]:
                    continue
                assert str(yson.dumps(value)) == check_values[name]

        # Decorate to simplify grep.
        def _decorate_key(key):
            return 12300000 + key

        for wave in range(1, 30):
            rows = [{
                "key": _decorate_key(k),
                "v": k,
                "i": wave,
                choice(["a", "b", "c"]): randint(1, 10000),
                choice(["s", "t"]): _make_value(randint(1, 10000))}
                for k in sample(list(range(1, count)), 200)]

            for row in rows:
                key = row["key"]
                item = verify_map.get(key, {})
                item.update(row)
                row["md5"] = _get_checksum(item)
                verify_map[key] = item

            print_debug("Insert rows ", rows)
            insert_rows("//tmp/t", rows, update=True)

            keys = [{"key": _decorate_key(k)} for k in sample(list(range(1, count)), 100)]
            for key in keys:
                if key["key"] in verify_map:
                    del verify_map[key["key"]]
            print_debug("Delete rows ", keys)
            delete_rows("//tmp/t", keys)

            for _ in range(1, 10):
                keys = [_decorate_key(k) for k in sample(list(range(1, count)), 10)]
                lookup_value_columns = \
                    ["key", "md5"] +\
                    required_columns +\
                    sample(optional_columns, randint(2, len(optional_columns)))
                result = self._read(
                    "//tmp/t",
                    keys,
                    column_names=lookup_value_columns,
                    use_lookup_cache=True)

                for row in result:
                    assert row["key"] == _decorate_key(row["v"])
                    _check_row(row, row["md5"])
                    revision = row["i"]
                    assert revision >= revision_map.get(row["key"], 0)
                    revision_map[row["key"]] = revision

            sync_flush_table("//tmp/t")

        tablet_profiling = self._get_table_profiling("//tmp/t")
        assert tablet_profiling.get_counter(f"{self.COUNTER_NAME}/cache_hits") > 0
        assert tablet_profiling.get_counter(f"{self.COUNTER_NAME}/cache_misses") > 0

    @authors("lukyan")
    def test_lookup_cache_hunks_cell_restart(self):
        sync_create_cells(1)

        def _make_value(i):
            return str(i) + ("payload" * (i % 5))

        self._create_simple_table("//tmp/t", True, lookup_cache_rows_per_tablet=50)

        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": _make_value(i)} for i in range(0, 300, 2)]
        insert_rows("//tmp/t", rows)

        cell_id = ls("//sys/tablet_cells")[0]
        peers = get("#" + cell_id + "/@peers")
        leader_address = list(x["address"] for x in peers if x["state"] == "leading")[0]

        build_snapshot(cell_id=cell_id)

        set_node_banned(leader_address, True)
        wait_for_cells([cell_id], decommissioned_addresses=[leader_address])

        expected = [{"key": i, "value": _make_value(i)} for i in range(100, 200, 2)]
        actual = self._read("//tmp/t", range(100, 200, 2), use_lookup_cache=True)
        assert_items_equal(actual, expected)

    @authors("dave11ar")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("hunks", [False, True])
    @pytest.mark.parametrize("test_size", [30])
    def test_lookup_with_timestamp_columns_row_cache(self, optimize_for, hunks, test_size):
        path = "//tmp/t"

        sync_create_cells(1)

        def decision(probability):
            return random.random() < probability

        def _make_value(i):
            return str(i) + ("payload" * (i % 5) if hunks else "")

        def _make_value_schema(name):
            schema = {"name": name, "type": "string", "max_inline_hunk_size": 12}

            if hunks:
                schema["max_inline_hunk_size"] = 12

            return schema

        create_dynamic_table(
            path,
            schema=[
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                _make_value_schema("v0"),
                _make_value_schema("v1"),
                _make_value_schema("v2"),
                _make_value_schema("v3"),
                _make_value_schema("v4"),
            ],
            optimize_for=optimize_for,
            lookup_cache_rows_per_tablet=int(test_size / 2),
        )

        sync_mount_table(path)

        different_values = test_size * 2
        columns = ["k", "v0", "v1", "v2", "$timestamp:v0", "$timestamp:v1", "$timestamp:v2"]
        keys = [k for k in range(0, test_size)]
        keys_for_lookup = [{"k": k} for k in keys]

        for k in keys:
            insert_rows(
                path,
                [{
                    "k": k,
                    "v0": _make_value(randint(0, different_values)),
                    "v1": _make_value(randint(0, different_values)),
                    "v2": _make_value(randint(0, different_values)),
                    "v3": _make_value(randint(0, different_values)),
                    "v4": _make_value(randint(0, different_values)),
                }]
            )

        def _update_rows(column):
            for k in keys:
                if decision(1 / 3):
                    insert_rows(
                        path,
                        [{
                            "k": k,
                            column: _make_value(randint(0, test_size)),
                        }],
                        update=True,
                    )

        _update_rows("v3")
        _update_rows("v4")
        _update_rows("v1")
        _update_rows("v2")

        sync_flush_table(path)

        rows = lookup_rows(path, keys_for_lookup, with_timestamps=True)
        rows_map = {row["k"]: row for row in rows}

        assert rows == lookup_rows(path, keys_for_lookup, with_timestamps=True, use_lookup_cache=True)

        for _ in range(test_size * 3):
            test_keys = []
            test_columns = []

            for key in keys:
                if decision(0.2):
                    test_keys.append({"k": key})

            for column in columns:
                if decision(2 / 3):
                    test_columns.append(column)

            shuffle(test_keys)
            shuffle(test_columns)

            test_rows = lookup_rows(path, test_keys, column_names=test_columns, use_lookup_cache=True, with_timestamps=True)

            for i in range(len(test_keys)):
                row = rows_map[test_keys[i]["k"]]

                for column, value in test_rows[i].items():
                    assert value == row[column]

################################################################################


@pytest.mark.enabled_multidaemon
class TestLookupMulticell(TestLookup):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestLookupRpcProxy(TestLookup):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    @authors("akozhikhov", "alexelexa")
    def test_detailed_lookup_profiling(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_detailed_profiling", True)
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "one"}]
        insert_rows("//tmp/t", rows)

        node_lookup_duration_histogram = profiler_factory().at_tablet_node("//tmp/t").histogram(
            name="lookup/duration")

        rpc_proxy = ls("//sys/rpc_proxies")[0]

        rpc_driver_config = deepcopy(self.Env.configs["rpc_driver"])
        rpc_driver_config["proxy_addresses"] = [rpc_proxy]
        rpc_driver_config["api_version"] = 3
        rpc_driver = Driver(config=rpc_driver_config)

        proxy_lookup_duration_histogram = profiler_factory().at_rpc_proxy(rpc_proxy).histogram(
            name="rpc_proxy/detailed_table_statistics/lookup_duration",
            fixed_tags={"table_path": "//tmp/t", "user": "root"})

        def _check():
            def _check(lookup_duration_histogram):
                try:
                    bins = lookup_duration_histogram.get_bins(verbose=True)
                    bin_counters = [bin["count"] for bin in bins]
                    if sum(bin_counters) != 1:
                        return False
                    if len(bin_counters) < 20:
                        return False
                    return True
                except YtError as e:
                    # TODO(eshcherbin): get rid of this.
                    if "No sensors have been collected so far" not in str(e):
                        raise e

            assert lookup_rows("//tmp/t", [{"key": 1}], driver=rpc_driver) == rows

            try:
                wait(lambda: _check(node_lookup_duration_histogram), iter=5, sleep_backoff=0.5)
                wait(lambda: _check(proxy_lookup_duration_histogram), iter=5, sleep_backoff=0.5)
                return True
            except WaitFailed:
                return False

        wait(lambda: _check())
        assert profiler_factory().at_rpc_proxy(rpc_proxy).get(
            name="rpc_proxy/detailed_table_statistics/lookup_mount_cache_wait_time",
            tags={"table_path": "//tmp/t"},
            postprocessor=lambda data: data.get('all_time_max'),
            summary_as_max_for_all_time=True,
            export_summary_as_max=True,
            verbose=False,
            default=0) > 0

    @authors("akozhikhov")
    def test_lookup_request_timeout_slack(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(100)]
        rows = [{"key": i, "value": str(i)} for i in range(100)]

        insert_rows("//tmp/t", rows)
        assert lookup_rows("//tmp/t", keys, timeout=1000) == rows

        def _set_timeout_slack_options(value):
            set("//sys/rpc_proxies/@config", {"cluster_connection": {"lookup_rows_request_timeout_slack": value}})

            def _config_updated():
                for proxy_name in ls("//sys/rpc_proxies"):
                    config = get("//sys/rpc_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
                    if config["cluster_connection"]["lookup_rows_request_timeout_slack"] != value:
                        return False
                return True
            wait(_config_updated)

        assert lookup_rows("//tmp/t", keys, timeout=1000, enable_partial_result=True) == rows

        _set_timeout_slack_options(1)
        assert lookup_rows("//tmp/t", keys, timeout=1000, enable_partial_result=True) == rows

        _set_timeout_slack_options(1000)
        assert lookup_rows("//tmp/t", keys, timeout=1000, enable_partial_result=True,) == []

################################################################################


@pytest.mark.enabled_multidaemon
class TestLookupOutThrottlingLegacyThrottler(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    DELTA_NODE_CONFIG = {
        "enable_fair_throttler": False,
        "query_agent":  {
            "account_user_backend_out_traffic": True,
        }
    }

    @authors("capone212")
    def test_lookup_repeated_keys(self):
        update_nodes_dynamic_config({
            "query_agent" : {
                "account_user_backend_out_traffic" : True,
            }
        })
        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows)

        keys = [{"key": i % 2} for i in range(10)]
        expected = [{"key": i % 2, "value": str(i % 2)} for i in range(10)]
        assert lookup_rows("//tmp/t", keys) == expected

        expected = [{"value": str(i % 2)} for i in range(10)]
        assert lookup_rows("//tmp/t", keys, column_names=["value"]) == expected

################################################################################


@pytest.mark.enabled_multidaemon
class TestLookupOutThrottlingFairThrottler(TestLookupOutThrottlingLegacyThrottler):
    ENABLE_MULTIDAEMON = True
    DELTA_NODE_CONFIG = {
        "enable_fair_throttler": True,
        "query_agent":  {
            "account_user_backend_out_traffic": True,
        }
    }
