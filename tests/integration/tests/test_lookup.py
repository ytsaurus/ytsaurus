import pytest
import __builtin__

from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import wait 
from yt_commands import *
from yt.yson import YsonEntity

import random

from yt.environment.helpers import assert_items_equal

################################################################################

class TestLookup(TestSortedDynamicTablesBase):
    @authors("savrus")
    def test_lookup_repeated_keys(self):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)

        keys = [{"key": i % 2} for i in xrange(10)]
        expected = [{"key": i % 2, "value": str(i % 2)} for i in xrange(10)]
        assert lookup_rows("//tmp/t", keys) == expected

        expected = [{"value": str(i % 2)} for i in xrange(10)]
        assert lookup_rows("//tmp/t", keys, column_names=["value"]) == expected

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_lookup_versioned(self, optimize_for):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        for prefix in ["a", "b"]:
            rows = [{"key": i, "value": prefix + ":" + str(i)} for i in xrange(10)]
            insert_rows("//tmp/t", rows)
            generate_timestamp()

        keys = [{"key": i} for i in xrange(10)]
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

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_lookup_versioned_YT_6800(self, optimize_for):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t",
            min_data_versions=0, min_data_ttl=0,
            max_data_versions=1000, max_data_ttl=1000000,
            optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        for prefix in ["a", "b", "c"]:
            rows = [{"key": i, "value": prefix + ":" + str(i)} for i in xrange(10)]
            insert_rows("//tmp/t", rows)
            generate_timestamp()

        keys = [{"key": i} for i in xrange(10)]
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
    def test_lookup_versioned_filter(self, optimize_for):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "string"}]
        create("table", "//tmp/t", attributes={
            "dynamic": True,
            "optimize_for": optimize_for,
            "schema": schema})

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
            {"name": "value1", "type": "string"}]
        schema2 = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "string"}]
        create("table", "//tmp/t", attributes={
            "dynamic": True,
            "optimize_for": optimize_for,
            "schema": schema1})

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
            {"name": "value3", "type": "string"}]
        create("table", "//tmp/t", attributes={
            "dynamic": True,
            "optimize_for": optimize_for,
            "schema": schema})

        sync_mount_table("//tmp/t")
        for i in xrange(10):
            insert_rows("//tmp/t", [{"key": 0, "value1": str(i)}], update=True)
            insert_rows("//tmp/t", [{"key": 0, "value2": str(i)}], update=True)
        keys = [{"key": 0}]

        retention_config = {
            "min_data_ttl": 0,
            "max_data_ttl": 1000 * 60 * 10,
            "min_data_versions": 1,
            "max_data_versions": 1}

        full_row = lookup_rows("//tmp/t", keys, versioned=True, retention_config=retention_config)[0]
        assert len(full_row.attributes["write_timestamps"]) == 2
        assert len(full_row.attributes["delete_timestamps"]) == 0
        assert len(full_row) == 3

        def _check(row):
            assert row.attributes["write_timestamps"] == full_row.attributes["write_timestamps"]
            assert row.attributes["delete_timestamps"] == full_row.attributes["delete_timestamps"]
            assert len(row) == 0

        actual = lookup_rows("//tmp/t", keys, column_names=["value3"], versioned=True, retention_config=retention_config)
        _check(actual[0])

        sync_flush_table("//tmp/t")

        actual = lookup_rows("//tmp/t", keys, column_names=["value3"], versioned=True, retention_config=retention_config)
        _check(actual[0])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("in_memory_mode", ["none", "uncompressed"])
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_stress_versioned_lookup(self, in_memory_mode, optimize_for):
        # This test checks that versioned lookup gives the same result for scan and lookup versioned formats.
        random.seed(12345)

        if in_memory_mode == "none" and optimize_for == "lookup":
            return

        schema = [
            {"name": "k1", "type": "int64", "sort_order": "ascending"},
            {"name": "k2", "type": "int64", "sort_order": "ascending"},
            {"name": "v1", "type": "int64"},
            {"name": "v2", "type": "int64"},
            {"name": "v3", "type": "int64"},
        ]

        delete_probability = 20 # percent
        value_probability = 70 # percent
        read_iters = 50
        lookup_iters = 50

        timestamps = []

        def random_write(table, keys):
            global timestamps

            for key in keys:
                for v in "v1", "v2", "v3":
                    if random.randint(0, 99) < value_probability:
                        key.update({v: random.randint(1, 10)})
            insert_rows(table, keys)

        def random_key():
            return {"k1": random.randint(1, 10), "k2": random.randint(1, 10)}

        sync_create_cells(1)
        self._create_simple_table("//tmp/expected", schema=schema, optimize_for="lookup")
        sync_mount_table("//tmp/expected")

        for i in range(read_iters):
            keys = [random_key() for i in range(5)]
            if random.randint(0, 99) < delete_probability:
                delete_rows("//tmp/expected", keys)
            else:
                random_write("//tmp/expected", keys)
            timestamps += [generate_timestamp()]

        sync_unmount_table("//tmp/expected")
        copy("//tmp/expected", "//tmp/actual")
        set("//tmp/actual/@optimize_for", optimize_for)
        set("//tmp/actual/@in_memory_mode", in_memory_mode)
        sync_mount_table("//tmp/expected")
        sync_mount_table("//tmp/actual")
        sync_compact_table("//tmp/actual")

        for i in range(lookup_iters):
            keys = [random_key() for i in range(5)]
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
        actual = [
            lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts)
            for ts in timestamps]

        set("//tmp/t/@enable_store_rotation", True)
        remount_table("//tmp/t")
        sync_freeze_table("//tmp/t")

        expected = [
            lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts)
            for ts in timestamps]

        assert expected == actual

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_versioned_lookup_unversioned_chunks(self, optimize_for):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "v1", "type": "int64"},
            {"name": "v2", "type": "int64"},
        ]

        create("table", "//tmp/t", attributes={
            "schema": make_schema(schema, strict=True, unique_keys=True),
            "optimize_for": optimize_for})

        timestamps = [generate_timestamp()]
        write_table("//tmp/t", [{"key": 0}])
        timestamps += [generate_timestamp()]
        write_table("<append=true>//tmp/t", [{"key": 1, "v1": 1}, {"key": 2, "v2": 2}], append=True)
        timestamps += [generate_timestamp()]
        write_table("<append=true>//tmp/t", [{"key": 3, "v1": 3, "v2": 4}], append=True)
        timestamps += [generate_timestamp()]

        assert len(read_table("//tmp/t"))

        sync_create_cells(1)
        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")

        def check(expected, actual):
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
                        if type(values[0]) == YsonEntity:
                            assert column not in expected[key]
                        else:
                            assert int(values[0]) == expected[key][column]
                else:
                    assert key not in expected

        expected = {}
        keys = [{"key": i} for i in range(4)]

        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        check(expected, actual)

        expected[0] = {}
        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        check(expected, actual)

        expected[1] = {"v1": 1}
        expected[2] = {"v2": 2}
        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        check(expected, actual)

        expected[3] = {"v1": 3, "v2": 4}
        actual = lookup_rows("//tmp/t", keys, versioned=True, timestamp=timestamps.pop(0))
        check(expected, actual)

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

        create("table", "//tmp/t", attributes={
            "schema": make_schema(schema, strict=True, unique_keys=True),
            "optimize_for": optimize_for})

        ts0 = generate_timestamp()
        write_table("//tmp/t", [{"key": 1, "value": "a"}])
        ts1 = generate_timestamp()

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts0) == []
        assert len(lookup_rows("//tmp/t", [{"key": 1}], versioned=True, timestamp=ts1)) == 1

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_lookup_from_chunks(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)

        pivots = [[]] + [[x] for x in range(100, 1000, 100)]
        sync_reshard_table("//tmp/t", pivots)
        assert self._get_pivot_keys("//tmp/t") == pivots

        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(0, 1000, 2)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        actual = lookup_rows("//tmp/t", [{'key': i} for i in xrange(0, 1000)])
        assert_items_equal(actual, rows)

        rows = [{"key": i, "value": str(i)} for i in xrange(1, 1000, 2)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(0, 1000)]
        actual = lookup_rows("//tmp/t", [{'key': i} for i in xrange(0, 1000)])
        assert_items_equal(actual, rows)

        for tablet in xrange(10):
            path = "//tmp/t/@tablets/{0}/performance_counters/static_chunk_row_lookup_count".format(tablet)
            wait(lambda: get(path) > 0)
            assert get(path) == 200

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


class TestLookupFromDataNode(TestSortedDynamicTablesBase):
    schema = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]

    def _set_nodes_state(self):
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@disable_write_sessions".format(nodes[0]), True)
        for node in nodes[1:]:
            set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), True)

    @authors("akozhikhov")
    def test_lookup_from_data_node_simple(self):
        self._set_nodes_state()
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=1)
        set("//tmp/t/@enable_data_node_lookup", True)

        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in range(2)]
        rows = [{"key": i, "value": str(i) * 2} for i in range(2)]
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
    @pytest.mark.parametrize("replication_factor", [1, 3])
    def test_lookup_from_data_node_with_alter(self, replication_factor):
        self._set_nodes_state()
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=replication_factor)
        set("//tmp/t/@enable_data_node_lookup", True)
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

        schema = self.schema[:1] + [{"name": "key2", "type": "int64", "sort_order": "ascending"}] + self.schema[1:]
        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        keys = [{"key": i} for i in [0, 3, 6]]
        rows = [{"key": i, "key2": YsonEntity(), "value": str(i)} for i in [0, 3, 6]]
        assert lookup_rows("//tmp/t", keys) == rows

        keys = [{"key": i, "key2": YsonEntity()} for i in [0, 3, 6]]
        rows = [{"key": i, "key2": YsonEntity(), "value": str(i)} for i in [0, 3, 6]]
        assert lookup_rows("//tmp/t", keys) == rows

    @authors("akozhikhov")
    @pytest.mark.parametrize("replication_factor", [1, 3])
    def test_lookup_from_data_node_chunks_with_overlap(self, replication_factor):
        self._set_nodes_state()
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=replication_factor, schema=self.schema)
        set("//tmp/t/@enable_data_node_lookup", True)
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

        schema = self.schema[:1] +\
                 [{"name": "key2", "type": "int64", "sort_order": "ascending"}] +\
                 self.schema[1:] +\
                 [{"name": "value2", "type": "boolean"}]
        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "key2": YsonEntity(), "value": str(values[i]), "value2": YsonEntity()} for i in range(7)]
        assert lookup_rows("//tmp/t", keys) == rows

        rows = [{"key2": YsonEntity(), "value": str(values[i])} for i in range(7)]
        assert lookup_rows("//tmp/t", keys, column_names=["key2", "value"]) == rows

        rows = [{"key": i, "value2": YsonEntity()} for i in range(7)]
        assert lookup_rows("//tmp/t", keys, column_names=["key", "value2"]) == rows

        rows = [{"key": i, "value": str(values[i])} for i in range(7)]
        assert lookup_rows("//tmp/t", keys + [{"key": 10}], column_names=["key", "value"], keep_missing_rows=True) ==\
               rows + [None]

    @authors("akozhikhov")
    @pytest.mark.parametrize("replication_factor", [1, 3])
    def test_lookup_from_data_node_with_timestamp(self, replication_factor):
        self._set_nodes_state()
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=replication_factor, schema=self.schema)
        set("//tmp/t/@enable_data_node_lookup", True)
        set("//tmp/t/@enable_compaction_and_partitioning", False)

        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "one"}])
        write_ts_1 = lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]

        insert_rows("//tmp/t", [{"key": 2, "value": "two"}])
        write_ts_2 = lookup_rows("//tmp/t", [{"key": 2}], versioned=True)[0].attributes["write_timestamps"][0]

        assert(write_ts_2 > write_ts_1)

        sync_flush_table("//tmp/t")

        assert write_ts_1 == lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]
        assert write_ts_2 == lookup_rows("//tmp/t", [{"key": 2}], versioned=True)[0].attributes["write_timestamps"][0]
        assert lookup_rows("//tmp/t", [{"key": 1}], timestamp=write_ts_1) == [{"key": 1, "value": "one"}]

        insert_rows("//tmp/t", [{"key": 1, "value": "oneone"}])
        sync_flush_table("//tmp/t")
        assert write_ts_1 < lookup_rows("//tmp/t", [{"key": 1}], versioned=True)[0].attributes["write_timestamps"][0]

        assert lookup_rows("//tmp/t", [{"key": 1}], timestamp=write_ts_1) == [{"key": 1, "value": "one"}]


    @authors("akozhikhov")
    @pytest.mark.parametrize("replication_factor", [1, 3])
    def test_lookup_from_data_node_stress(self, replication_factor):
        self._set_nodes_state()
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=replication_factor)
        set("//tmp/t/@enable_data_node_lookup", True)
        set("//tmp/t/@enable_compaction_and_partitioning", False)

        sync_mount_table("//tmp/t")

        seq_keys = range(50)
        seq_values = range(1000)

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
        for key, value in current_value.iteritems():
            expected_keys.append({"key": key})
            expected_values.append({"key": key, "value": value})

        assert lookup_rows("//tmp/t", expected_keys) == expected_values

    @authors("akozhikhov")
    def test_lookup_from_data_node_local_reader(self):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", replication_factor=self.NUM_NODES)
        set("//tmp/t/@enable_data_node_lookup", True)
        set("//tmp/t/@enable_compaction_and_partitioning", False)

        sync_mount_table("//tmp/t")

        row = [{"key": 1, "value": "one"}]
        insert_rows("//tmp/t", row)
        sync_flush_table("//tmp/t")
        assert lookup_rows("//tmp/t", [{"key": 1}]) == row


################################################################################

class TestLookupMulticell(TestLookup):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestLookupRpcProxy(TestLookup):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
