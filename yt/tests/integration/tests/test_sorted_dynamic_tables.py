import pytest
import __builtin__

from test_dynamic_tables import TestDynamicTablesBase

from yt_env_setup import wait
from yt_commands import *
from yt.yson import YsonEntity, loads

from time import sleep

from yt.environment.helpers import assert_items_equal

##################################################################

class TestSortedDynamicTablesBase(TestDynamicTablesBase):
    def _create_simple_table(self, path, **extra_attributes):
        attributes = {
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ]
        }
        attributes.update(extra_attributes)
        create("table", path, attributes=attributes)

    def _create_table_with_computed_column(self, path, optimize_for="lookup"):
        create("table", path,
            attributes={
                "dynamic": True,
                "optimize_for": optimize_for,
                "schema": [
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending", "expression": "key1 * 100 + 3"},
                    {"name": "value", "type": "string"}]
            })

    def _create_table_with_hash(self, path, optimize_for="lookup"):
        create("table", path,
            attributes={
                "dynamic": True,
                "optimize_for": optimize_for,
                "schema": [
                    {"name": "hash", "type": "uint64", "expression": "farm_hash(key)", "sort_order": "ascending"},
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
            })

    def _create_table_with_aggregate_column(self, path, aggregate = "sum", optimize_for="lookup", atomicity="full"):
        create("table", path,
            attributes={
                "dynamic": True,
                "optimize_for" : optimize_for,
                "atomicity": atomicity,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "time", "type": "int64"},
                    {"name": "value", "type": "int64", "aggregate": aggregate}]
            })

    def _wait_for_in_memory_stores_preload(self, table):
        for tablet in get(table + "/@tablets"):
            tablet_id = tablet["tablet_id"]
            address = self._get_tablet_leader_address(tablet_id)
            def all_preloaded():
                orchid = self._find_tablet_orchid(address, tablet_id)
                if not orchid:
                    return False
                for store in orchid["eden"]["stores"].itervalues():
                    if store["store_state"] == "persistent" and store["preload_state"] != "complete":
                        return False
                for partition in orchid["partitions"]:
                    for store in partition["stores"].itervalues():
                        if store["preload_state"] != "complete":
                            return False
                return True
            wait(lambda: all_preloaded())

    def _reshard_with_retries(self, path, pivots):
        resharded = False
        for i in xrange(4):
            try:
                self.sync_unmount_table(path)
                reshard_table(path, pivots)
                resharded = True
            except:
                pass
            self.sync_mount_table(path)
            if resharded:
                break
            sleep(5)
        assert resharded


##################################################################

class TestSortedDynamicTables(TestSortedDynamicTablesBase):
    DELTA_NODE_CONFIG = {
        "cluster_connection" : {
            "timestamp_provider" : {
                "update_period": 100
            }
        }
    }

    DELTA_MASTER_CONFIG = {
        "timestamp_provider": {
            "update_period": 500
        }
    }

    def test_mount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1
        tablet_id = tablets[0]["tablet_id"]
        cell_id = tablets[0]["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]

    def test_unmount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert tablet["pivot_key"] == []

        self.sync_mount_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_mount_unmount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

        self.sync_unmount_table("//tmp/t")
        with pytest.raises(YtError): lookup_rows("//tmp/t", keys)

        self.sync_mount_table("//tmp/t")
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

    def test_sorted_tablet_node_profiling(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", enable_profiling=True)
        self.sync_mount_table("//tmp/t")

        tablet_profiling = self._get_tablet_profiling("//tmp/t")
        select_profiling = self._get_profiling("//tmp/t")

        def get_all_counters(count_name):
            return (
                tablet_profiling.get_counter("lookup/" + count_name),
                tablet_profiling.get_counter("select/" + count_name),
                tablet_profiling.get_counter("write/" + count_name),
                tablet_profiling.get_counter("commit/" + count_name))

        assert get_all_counters("row_count") == (0, 0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0, 0)
        assert tablet_profiling.get_counter("lookup/cpu_time") == 0
        assert select_profiling.get_counter("select/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        sleep(2)

        assert get_all_counters("row_count") == (0, 0, 1, 1)
        assert get_all_counters("data_weight") == (0, 0, 10, 10)
        assert tablet_profiling.get_counter("lookup/cpu_time") == 0
        assert select_profiling.get_counter("select/cpu_time") == 0

        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

        sleep(2)

        assert get_all_counters("row_count") == (1, 0, 1, 1)
        assert get_all_counters("data_weight") == (10, 0, 10, 10)
        assert tablet_profiling.get_counter("lookup/cpu_time") > 0
        assert select_profiling.get_counter("select/cpu_time") == 0

        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, rows)

        sleep(2)

        assert get_all_counters("row_count") == (1, 1, 1, 1)
        assert get_all_counters("data_weight") == (10, 10, 10, 10)
        assert tablet_profiling.get_counter("lookup/cpu_time") > 0
        assert select_profiling.get_counter("select/cpu_time") > 0

    def test_sorted_default_enabled_tablet_node_profiling(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t_unique_name")
        self.sync_mount_table("//tmp/t_unique_name")

        table_profiling = self._get_table_profiling("//tmp/t_unique_name")

        def get_all_counters(count_name):
            return (
                table_profiling.get_counter("lookup/" + count_name),
                table_profiling.get_counter("write/" + count_name),
                table_profiling.get_counter("commit/" + count_name))

        assert get_all_counters("row_count") == (0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0)
        assert table_profiling.get_counter("lookup/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t_unique_name", rows)

        sleep(2)

        assert get_all_counters("row_count") == (0, 1, 1)
        assert get_all_counters("data_weight") == (0, 10, 10)
        assert table_profiling.get_counter("lookup/cpu_time") == 0

        actual = lookup_rows("//tmp/t_unique_name", keys)
        assert_items_equal(actual, rows)

        sleep(2)

        assert get_all_counters("row_count") == (1, 1, 1)
        assert get_all_counters("data_weight") == (10, 10, 10)
        assert table_profiling.get_counter("lookup/cpu_time") > 0

    def test_sorted_tablet_node_profiling_remount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        keys = [{"key": 1}]
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        profiling = self._get_tablet_profiling("//tmp/t")

        def get_lookup_row_counter():
            return profiling.get_counter("lookup/row_count")

        lookup_rows("//tmp/t", keys)
        sleep(2)
        assert get_lookup_row_counter() == 0

        set("//tmp/t/@enable_profiling", True)
        lookup_rows("//tmp/t", keys)
        sleep(2)
        assert get_lookup_row_counter() == 0

        remount_table("//tmp/t")
        sleep(1)
        lookup_rows("//tmp/t", keys)
        sleep(2)
        assert get_lookup_row_counter() == 1

        set("//tmp/t/@enable_profiling", False)
        remount_table("//tmp/t")
        sleep(1)
        lookup_rows("//tmp/t", keys)
        sleep(2)
        assert get_lookup_row_counter() == 1

        set("//tmp/t/@enable_profiling", True)
        remount_table("//tmp/t")
        sleep(1)
        lookup_rows("//tmp/t", keys)
        sleep(2)
        assert get_lookup_row_counter() == 2

    def test_reshard_unmounted(self):
        self.sync_create_cells(1)
        create("table", "//tmp/t",attributes={
            "dynamic": True,
            "schema": [
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                {"name": "l", "type": "uint64", "sort_order": "ascending"},
                {"name": "value", "type": "int64"}
            ]})

        reshard_table("//tmp/t", [[]])
        assert self._get_pivot_keys("//tmp/t") == [[]]

        reshard_table("//tmp/t", [[], [100]])
        assert self._get_pivot_keys("//tmp/t") == [[], [100]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[], []])
        assert self._get_pivot_keys("//tmp/t") == [[], [100]]

        reshard_table("//tmp/t", [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[101]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[300]], first_tablet_index=3, last_tablet_index=3)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        reshard_table("//tmp/t", [[100], [150], [200]], first_tablet_index=1, last_tablet_index=2)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[100], [100]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

        with pytest.raises(YtError): reshard_table("//tmp/t", [[], [100, 200]])
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

    def test_reshard_partly_unmounted(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", [[], [100], [200], [300]])
        self.sync_mount_table("//tmp/t")
        with pytest.raises(YtError): reshard_table("//tmp/t", [[100], [250], [300]], first_tablet_index=1, last_tablet_index=3)
        self.sync_unmount_table("//tmp/t", first_tablet_index=1, last_tablet_index=3)
        reshard_table("//tmp/t", [[100], [250], [300]], first_tablet_index=1, last_tablet_index=3)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [250], [300]]

    def test_reshard_tablet_count(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", [[], [1]])
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "A"*256} for i in xrange(2)])
        self.sync_flush_table("//tmp/t")
        self.sync_compact_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")
        chunks = get("//tmp/t/@chunk_ids")
        assert len(chunks) == 2
        reshard_table("//tmp/t", [[]])
        assert self._get_pivot_keys("//tmp/t") == [[]]
        reshard_table("//tmp/t", 2)
        assert self._get_pivot_keys("//tmp/t") == [[], [1]]

    def test_force_unmount_on_remove(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)
        assert self._find_tablet_orchid(address, tablet_id) is not None

        remove("//tmp/t")
        sleep(1)
        assert self._find_tablet_orchid(address, tablet_id) is None

    def test_lookup_repeated_keys(self):
        self.sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)

        keys = [{"key": i % 2} for i in xrange(10)]
        expected = [{"key": i % 2, "value": str(i % 2)} for i in xrange(10)]
        assert lookup_rows("//tmp/t", keys) == expected

        expected = [{"value": str(i % 2)} for i in xrange(10)]
        assert lookup_rows("//tmp/t", keys, column_names=["value"]) == expected

    def test_lookup_versioned(self):
        self.sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

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

    def test_lookup_versioned_YT_6800(self):
        self.sync_create_cells(1)

        self._create_simple_table("//tmp/t",
            min_data_versions=0, min_data_ttl=0,
            max_data_versions=1000, max_data_ttl=1000000)
        self.sync_mount_table("//tmp/t")

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

    def test_overflow_row_data_weight(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        set("//tmp/t/@max_dynamic_store_row_data_weight", 100)
        self.sync_mount_table("//tmp/t")
        rows = [{"key": 0, "value": "A" * 100}]
        insert_rows("//tmp/t", rows)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", rows)

    def test_read_invalid_limits(self):
        self.sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        with pytest.raises(YtError): read_table("//tmp/t[#5:]")
        with pytest.raises(YtError): read_table("<ranges=[{lower_limit={offset = 0};upper_limit={offset = 1}}]>//tmp/t")

    @pytest.mark.parametrize("erasure_codec", ["none", "reed_solomon_6_3", "lrc_12_2_2"])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_read_table(self, optimize_for, erasure_codec):
        self.sync_create_cells(1)

        self._create_simple_table("//tmp/t", optimize_for=optimize_for, erasure_codec=erasure_codec)
        self.sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        self.sync_freeze_table("//tmp/t")

        assert read_table("//tmp/t") == rows1
        assert get("//tmp/t/@chunk_count") == 1

        ts = generate_timestamp()

        self.sync_unfreeze_table("//tmp/t")
        rows2 = [{"key": i, "value": str(i+1)} for i in xrange(10)]
        insert_rows("//tmp/t", rows2)
        self.sync_unmount_table("//tmp/t")
        sleep(1)

        assert read_table("<timestamp=%s>//tmp/t" %(ts)) == rows1
        assert get("//tmp/t/@chunk_count") == 2

    def test_read_snapshot_lock(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        def get_chunk_tree(path):
            root_chunk_list_id = get(path + "/@chunk_list_id")
            root_chunk_list = get("#" + root_chunk_list_id + "/@")
            tablet_chunk_lists = [get("#" + x + "/@") for x in root_chunk_list["child_ids"]]
            assert all([root_chunk_list_id in chunk_list["parent_ids"] for chunk_list in tablet_chunk_lists])
            assert get(path + "/@chunk_count") == sum([len(chunk_list["child_ids"]) for chunk_list in tablet_chunk_lists])
            return root_chunk_list, tablet_chunk_lists

        def verify_chunk_tree_refcount(path, root_ref_count, tablet_ref_counts):
            root, tablets = get_chunk_tree(path)
            assert root["ref_counter"] == root_ref_count
            assert [tablet["ref_counter"] for tablet in tablets] == tablet_ref_counts

        verify_chunk_tree_refcount("//tmp/t", 1, [1])

        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1])

        rows1 = [{"key": i, "value": str(i)} for i in xrange(0, 10, 2)]
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1])
        assert read_table("//tmp/t") == rows1
        assert read_table("//tmp/t", tx=tx) == []

        with pytest.raises(YtError):
            read_table("<timestamp={0}>//tmp/t".format(generate_timestamp()), tx=tx)

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1])

        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1])

        reshard_table("//tmp/t", [[], [5]])
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1, 1])

        self.sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        rows2 = [{"key": i, "value": str(i)} for i in xrange(1, 5, 2)]
        insert_rows("//tmp/t", rows2)
        self.sync_unmount_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 2])
        assert_items_equal(read_table("//tmp/t"), rows1 + rows2)
        assert read_table("//tmp/t", tx=tx) == rows1

        self.sync_mount_table("//tmp/t")
        rows3 = [{"key": i, "value": str(i)} for i in xrange(5, 10, 2)]
        insert_rows("//tmp/t", rows3)
        self.sync_unmount_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])
        assert_items_equal(read_table("//tmp/t"), rows1 + rows2 + rows3)
        assert read_table("//tmp/t", tx=tx) == rows1

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1, 1])

        self.sync_mount_table("//tmp/t")
        self.sync_compact_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])
        assert_items_equal(read_table("//tmp/t"), rows1 + rows2 + rows3)
        assert_items_equal(read_table("//tmp/t", tx=tx), rows1 + rows2 + rows3)

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

    def test_read_table_ranges(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", pivot_keys=[[], [5]])
        set("//tmp/t/@min_compaction_store_count", 5)
        self.sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        self.sync_flush_table("//tmp/t")

        rows2 = [{"key": i, "value": str(i+1)} for i in xrange(1,5)]
        insert_rows("//tmp/t", rows2)
        self.sync_flush_table("//tmp/t")

        rows3 = [{"key": i, "value": str(i+2)} for i in xrange(5,9)]
        insert_rows("//tmp/t", rows3)
        self.sync_flush_table("//tmp/t")

        rows4 = [{"key": i, "value": str(i+3)} for i in xrange(0,3)]
        insert_rows("//tmp/t", rows4)
        self.sync_flush_table("//tmp/t")

        rows5 = [{"key": i, "value": str(i+4)} for i in xrange(7,10)]
        insert_rows("//tmp/t", rows5)
        self.sync_flush_table("//tmp/t")

        self.sync_freeze_table("//tmp/t")

        rows = []
        def update(new):
            def update_row(row):
                for r in rows:
                    if r["key"] == row["key"]:
                        r["value"] = row["value"]
                        return
                rows.append(row)
            for row in new:
                update_row(row)

        for r in [rows1, rows2, rows3, rows4, rows5]:
            update(r)

        assert read_table("//tmp/t[(2):(9)]") == rows[2:9]
        assert get("//tmp/t/@chunk_count") == 6

    def test_read_table_when_chunk_crosses_tablet_boundaries(self):
        create("table", "//tmp/t",
            attributes={
                "dynamic": False,
                "external": False,
                "schema": make_schema([
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}],
                    unique_keys=True)
            })
        rows = [{"key": i, "value": str(i)} for i in xrange(6)]
        write_table("//tmp/t", rows)
        alter_table("//tmp/t", dynamic=True)

        def do_test():
            for i in xrange(6):
                assert read_table("//tmp/t[{0}:{1}]".format(i, i+1)) == rows[i:i+1]
            for i in xrange(0, 6, 2):
                assert read_table("//tmp/t[{0}:{1}]".format(i, i+2)) == rows[i:i+2]
            for i in xrange(1, 6, 2):
                assert read_table("//tmp/t[{0}:{1}]".format(i, i+2)) == rows[i:i+2]
        do_test()
        reshard_table("//tmp/t", [[], [2], [4]])
        do_test()

    def test_write_table(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        with pytest.raises(YtError): write_table("//tmp/t", [{"key": 1, "value": 2}])

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_computed_columns(self, optimize_for):
        self.sync_create_cells(1)
        self._create_table_with_computed_column("//tmp/t", optimize_for)
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key1": 1, "value": "2"}])
        expected = [{"key1": 1, "key2": 103, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        insert_rows("//tmp/t", [{"key1": 2, "value": "2"}])
        expected = [{"key1": 1, "key2": 103, "value": "2"}]
        actual = lookup_rows("//tmp/t", [{"key1" : 1}])
        assert_items_equal(actual, expected)
        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = lookup_rows("//tmp/t", [{"key1": 2}])
        assert_items_equal(actual, expected)

        delete_rows("//tmp/t", [{"key1": 1}])
        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key1": 3, "key2": 3, "value": "3"}])
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key1": 2, "key2": 203}])
        with pytest.raises(YtError): delete_rows("//tmp/t", [{"key1": 2, "key2": 203}])

        expected = []
        actual = lookup_rows("//tmp/t", [{"key1": 3}])
        assert_items_equal(actual, expected)

        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_computed_hash(self, optimize_for):
        self.sync_create_cells(1)

        self._create_table_with_hash("//tmp/t", optimize_for)
        self.sync_mount_table("//tmp/t")

        row1 = [{"key": 1, "value": "2"}]
        insert_rows("//tmp/t", row1)
        actual = select_rows("key, value from [//tmp/t]")
        assert_items_equal(actual, row1)

        row2 = [{"key": 2, "value": "2"}]
        insert_rows("//tmp/t", row2)
        actual = lookup_rows("//tmp/t", [{"key": 1}], column_names=["key", "value"])
        assert_items_equal(actual, row1)
        actual = lookup_rows("//tmp/t", [{"key": 2}], column_names=["key", "value"])
        assert_items_equal(actual, row2)

        delete_rows("//tmp/t", [{"key": 1}])
        actual = select_rows("key, value from [//tmp/t]")
        assert_items_equal(actual, row2)

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_computed_column_update_consistency(self, optimize_for):
        self.sync_create_cells(1)

        create("table", "//tmp/t",

            attributes={
                "dynamic": True,
                "optimize_for" : optimize_for,
                "schema": [
                    {"name": "key1", "type": "int64", "expression": "key2", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value1", "type": "string"},
                    {"name": "value2", "type": "string"}]
            })

        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key2": 1, "value1": "2"}])
        expected = [{"key1": 1, "key2": 1, "value1": "2", "value2" : YsonEntity()}]
        actual = lookup_rows("//tmp/t", [{"key2" : 1}])
        assert_items_equal(actual, expected)

        insert_rows("//tmp/t", [{"key2": 1, "value2": "3"}], update=True)
        expected = [{"key1": 1, "key2": 1, "value1": "2", "value2": "3"}]
        actual = lookup_rows("//tmp/t", [{"key2" : 1}])
        assert_items_equal(actual, expected)

        insert_rows("//tmp/t", [{"key2": 1, "value1": "4"}], update=True)
        expected = [{"key1": 1, "key2": 1, "value1": "4", "value2": "3"}]
        actual = lookup_rows("//tmp/t", [{"key2" : 1}])
        assert_items_equal(actual, expected)

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_aggregate_columns(self, optimize_for):
        self.sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", optimize_for=optimize_for)
        self.sync_mount_table("//tmp/t")

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

        self.sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 3, "value": 30})
        test_row({"key": 1, "time": 4, "value": 10}, {"key": 1, "time": 4, "value": 40}, aggregate=True)
        test_row({"key": 1, "time": 5, "value": 10}, {"key": 1, "time": 5, "value": 50}, aggregate=True)
        test_row({"key": 1, "time": 6, "value": 10}, {"key": 1, "time": 6, "value": 60}, aggregate=True)

        self.sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 6, "value": 60})
        test_row({"key": 1, "time": 7, "value": 10}, {"key": 1, "time": 7, "value": 70}, aggregate=True)
        test_row({"key": 1, "time": 8, "value": 10}, {"key": 1, "time": 8, "value": 80}, aggregate=True)
        test_row({"key": 1, "time": 9, "value": 10}, {"key": 1, "time": 9, "value": 90}, aggregate=True)

        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row({"key": 1, "time": 10, "value": 10}, {"key": 1, "time": 10, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 11, "value": 10}, {"key": 1, "time": 11, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 12, "value": 10}, {"key": 1, "time": 12, "value": 30}, aggregate=True)

        self.sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 12, "value": 30})
        test_row({"key": 1, "time": 13, "value": 10}, {"key": 1, "time": 13, "value": 40}, aggregate=True)
        test_row({"key": 1, "time": 14, "value": 10}, {"key": 1, "time": 14, "value": 50}, aggregate=True)
        test_row({"key": 1, "time": 15, "value": 10}, {"key": 1, "time": 15, "value": 60}, aggregate=True)

        self.sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 15, "value": 60})
        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row({"key": 1, "time": 16, "value": 10}, {"key": 1, "time": 16, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 17, "value": 10}, {"key": 1, "time": 17, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 18, "value": 10}, {"key": 1, "time": 18, "value": 30}, aggregate=True)

        self.sync_flush_table("//tmp/t")
        self.sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 18, "value": 30})
        test_row({"key": 1, "time": 19, "value": 10}, {"key": 1, "time": 19, "value": 10})
        test_row({"key": 1, "time": 20, "value": 10}, {"key": 1, "time": 20, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 21, "value": 10}, {"key": 1, "time": 21, "value": 10})

        self.sync_flush_table("//tmp/t")
        self.sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 21, "value": 10})

    def test_aggregate_min_max(self):
        self.sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", "min", "scan")
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [
            {"key": 1, "time": 1, "value": 10},
            {"key": 2, "time": 1, "value": 20},
            {"key": 3, "time": 1}], aggregate=True)
        insert_rows("//tmp/t", [
            {"key": 1, "time": 2, "value": 30},
            {"key": 2, "time": 2, "value": 40},
            {"key": 3, "time": 2}], aggregate=True)
        assert_items_equal(select_rows("max(value) as max from [//tmp/t] group by 1"), [{"max": 20}])

    def test_aggregate_first(self):
        self.sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="first")
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 10}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 20}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 10}]

    @pytest.mark.parametrize("aggregate", ["min", "max", "sum", "first"])
    def test_aggregate_update(self, aggregate):
        self.sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate=aggregate)
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "time": 1}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 1, "value": None}]
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 10}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 10}]
        insert_rows("//tmp/t", [{"key": 1, "time": 3}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 3, "value": 10}]

    def test_aggregate_alter(self):
        self.sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "time", "type": "int64"},
            {"name": "value", "type": "int64"}]
        create("table", "//tmp/t", attributes={"dynamic": True, "schema": schema})
        self.sync_mount_table("//tmp/t")

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

        self.sync_unmount_table("//tmp/t")
        schema[2]["aggregate"] = "sum"
        alter_table("//tmp/t", schema=schema)
        self.sync_mount_table("//tmp/t")

        verify_row(1, [{"key": 1, "time": 2, "value": 20}])
        test_row({"key": 1, "time": 3, "value": 10}, {"key": 1, "time": 3, "value": 30}, aggregate=True)

    def test_aggregate_non_atomic(self):
        self.sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="sum", atomicity="none")
        self.sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet", sticky=True, atomicity="none")
        tx2 = start_transaction(type="tablet", sticky=True, atomicity="none")

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 10}], aggregate=True, atomicity="none", tx=tx1)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 20}], aggregate=True, atomicity="none", tx=tx2)

        commit_transaction(tx1, sticky=True)
        commit_transaction(tx2, sticky=True)

        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 30}]

    @pytest.mark.parametrize("aggregate", ["avg", "cardinality"])
    def test_invalid_aggregate(self, aggregate):
        self.sync_create_cells(1)
        with pytest.raises(YtError):
            self._create_table_with_aggregate_column("//tmp/t", aggregate=aggregate)

    def test_reshard_data(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for="scan")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(3)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        self._reshard_with_retries("//tmp/t", [[], [1]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        self._reshard_with_retries("//tmp/t", [[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        self._reshard_with_retries("//tmp/t", [[]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    def test_reshard_single_chunk(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", enable_compaction_and_partitioning=False)
        self.sync_mount_table("//tmp/t")

        def reshard(pivots):
            self.sync_unmount_table("//tmp/t")
            reshard_table("//tmp/t", pivots)
            self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(3)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        reshard([[], [1]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        reshard([[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        reshard([[]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_any_value_type(self, optimize_for):
        self.sync_create_cells(1)
        create("table", "//tmp/t1",
            attributes={
                "dynamic": True,
                "optimize_for" : optimize_for,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "any"}]
            })

        self.sync_mount_table("//tmp/t1")

        rows = [
            {"key": 11, "value": 100},
            {"key": 12, "value": False},
            {"key": 13, "value": True},
            {"key": 14, "value": 2**63 + 1 },
            {"key": 15, "value": 'stroka'},
            {"key": 16, "value": [1, {"attr": 3}, 4]},
            {"key": 17, "value": {"numbers": [0,1,42]}}]

        insert_rows("//tmp/t1", rows)
        actual = select_rows("* from [//tmp/t1]")
        assert_items_equal(actual, rows)
        actual = lookup_rows("//tmp/t1", [{"key": row["key"]} for row in rows])
        assert_items_equal(actual, rows)

    def _prepare_allowed(self, permission):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        create_user("u")
        set("//tmp/t/@inherit_acl", False)
        set("//tmp/t/@acl", [make_ace("allow", "u", permission)])

    def _prepare_denied(self, permission):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        create_user("u")
        set("//tmp/t/@acl", [make_ace("deny", "u", permission)])

    def test_select_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = select_rows("* from [//tmp/t]", authenticated_user="u")
        assert_items_equal(actual, expected)

    def test_select_denied(self):
        self._prepare_denied("read")
        with pytest.raises(YtError): select_rows("* from [//tmp/t]", authenticated_user="u")

    def test_lookup_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key" : 1}], authenticated_user="u")
        assert_items_equal(actual, expected)

    def test_lookup_denied(self):
        self._prepare_denied("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        with pytest.raises(YtError): lookup_rows("//tmp/t", [{"key" : 1}], authenticated_user="u")

    def test_insert_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}], authenticated_user="u")
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key" : 1}])
        assert_items_equal(actual, expected)

    def test_insert_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 1, "value": "test"}], authenticated_user="u")

    def test_delete_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        delete_rows("//tmp/t", [{"key": 1}], authenticated_user="u")
        expected = []
        actual = lookup_rows("//tmp/t", [{"key" : 1}])
        assert_items_equal(actual, expected)

    def test_delete_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError): delete_rows("//tmp/t", [{"key": 1}], authenticated_user="u")

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_lookup_from_chunks(self, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for = optimize_for)

        pivots = [[]] + [[x] for x in range(100, 1000, 100)]
        reshard_table("//tmp/t", pivots)
        assert self._get_pivot_keys("//tmp/t") == pivots

        self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(0, 1000, 2)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        actual = lookup_rows("//tmp/t", [{'key': i} for i in xrange(0, 1000)])
        assert_items_equal(actual, rows)

        rows = [{"key": i, "value": str(i)} for i in xrange(1, 1000, 2)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(0, 1000)]
        actual = lookup_rows("//tmp/t", [{'key': i} for i in xrange(0, 1000)])
        assert_items_equal(actual, rows)

        for tablet in xrange(10):
            path = "//tmp/t/@tablets/{0}/performance_counters/static_chunk_row_lookup_count".format(tablet)
            wait(lambda: get(path) > 0)
            assert get(path) == 200

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed"])
    def test_data_weight_performance_counters(self, optimize_for, in_memory_mode):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for, in_memory_mode=in_memory_mode)
        self.sync_mount_table("//tmp/t")

        path = "//tmp/t/@tablets/0/performance_counters"

        insert_rows("//tmp/t", [{"key": 0, "value": "hello"}])

        wait(lambda: get(path + "/dynamic_row_write_data_weight_count") > 0)

        select_rows("* from [//tmp/t]")

        # Dynamic read must change, lookup must not change
        wait(lambda: get(path + "/dynamic_row_read_data_weight_count") > 0)
        assert get(path + "/dynamic_row_lookup_data_weight_count") == 0

        lookup_rows("//tmp/t", [{"key": 0}])

        # Dynamic read lookup change, read must not change
        wait(lambda: get(path + "/dynamic_row_lookup_data_weight_count") > 0)
        assert get(path + "/dynamic_row_read_data_weight_count") == get(path + "/dynamic_row_lookup_data_weight_count")

        # Static read/lookup must not change
        assert get(path + "/static_chunk_row_read_data_weight_count") == 0
        assert get(path + "/static_chunk_row_lookup_data_weight_count") == 0

        self.sync_flush_table("//tmp/t")

        select_rows("* from [//tmp/t]")

        # Static read must change, lookup must not change
        wait(lambda: get(path + "/static_chunk_row_read_data_weight_count") > 0)
        assert get(path + "/static_chunk_row_lookup_data_weight_count") == 0

        lookup_rows("//tmp/t", [{"key": 0}])

        # Static lookup must change, read must not change
        wait(lambda: get(path + "/static_chunk_row_lookup_data_weight_count") > 0)
        assert get(path + "/static_chunk_row_read_data_weight_count") == get(path + "/static_chunk_row_lookup_data_weight_count")

    def test_store_rotation(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        set("//tmp/t/@max_dynamic_store_row_count", 10)
        self.sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)

        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)

        sleep(3.0)

        tablet_data = self._find_tablet_orchid(address, tablet_id)
        assert len(tablet_data["eden"]["stores"]) == 1
        assert len(tablet_data["partitions"]) == 1
        assert len(tablet_data["partitions"][0]["stores"]) == 1

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory(self, mode, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)

        set("//tmp/t/@in_memory_mode", mode)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        self.sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            set("//tmp/t/@in_memory_mode", "none")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)

        def _check_preload_state(state):
            sleep(1.0)
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            assert len(tablet_data["eden"]["stores"]) == 1
            for partition in tablet_data["partitions"]:
                assert all(s["preload_state"] == state for _, s in partition["stores"].iteritems())
            actual_preload_completed = get("//tmp/t/@tablets/0/statistics/preload_completed_store_count")
            if state == "complete":
                assert actual_preload_completed >= 1
            else:
                assert actual_preload_completed == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_pending_store_count") == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_failed_store_count") == 0

        # Check preload after mount.
        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        keys = [{"key" : row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Check preload after flush.
        rows = [{"key": i, "value": str(i + 1)} for i in xrange(10)]
        keys = [{"key" : row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        self.sync_flush_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Check preload after compaction.
        self.sync_compact_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Disable in-memory mode
        self.sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "none")
        self.sync_mount_table("//tmp/t")
        _check_preload_state("disabled")
        assert lookup_rows("//tmp/t", keys) == rows

        # Re-enable in-memory mode
        self.sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", mode)
        self.sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

    def test_lookup_hash_table(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        set("//tmp/t/@in_memory_mode", "uncompressed")
        set("//tmp/t/@enable_lookup_hash_table", True)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        self.sync_mount_table("//tmp/t")

        def _rows(i, j):
            return [{"key": k, "value": str(k)} for k in xrange(i, j)]

        def _keys(i, j):
            return [{"key": k} for k in xrange(i, j)]

        # check that we can insert rows
        insert_rows("//tmp/t", _rows(0, 5))
        assert lookup_rows("//tmp/t", _keys(0, 5)) == _rows(0, 5)

        # check that we can insert rows till capacity
        insert_rows("//tmp/t", _rows(5, 10))
        assert lookup_rows("//tmp/t", _keys(0, 10)) == _rows(0, 10)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")

        # check that stores are rotated on-demand
        insert_rows("//tmp/t", _rows(10, 20))
        # ensure slot gets scanned
        sleep(3)
        insert_rows("//tmp/t", _rows(20, 30))
        assert lookup_rows("//tmp/t", _keys(10, 30)) == _rows(10, 30)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")

        # check that we can delete rows
        delete_rows("//tmp/t", _keys(0, 10))
        assert lookup_rows("//tmp/t", _keys(0, 10)) == []

        # check that everything survives after recovery
        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")
        assert lookup_rows("//tmp/t", _keys(0, 50)) == _rows(10, 30)

        # check that we can extend key
        self.sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}]);
        self.sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")
        assert lookup_rows("//tmp/t", _keys(0, 50), column_names=["key", "value"]) == _rows(10, 30)

    def test_update_key_columns_fail1(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@key_columns", ["key", "key2"])

    def test_update_key_columns_fail2(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@key_columns", ["key2", "key3"])

    def test_update_key_columns_fail3(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@key_columns", [])

    def test_alter_table_fails(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        # We have to insert at least one row to the table because any
        # valid schema can be set for an empty table without any checks.
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        self.sync_unmount_table("//tmp/t")
        with pytest.raises(YtError): alter_table("//tmp/t", schema=[
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        with pytest.raises(YtError): alter_table("//tmp/t", schema=[
            {"name": "key", "type": "uint64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        with pytest.raises(YtError): alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"}])

        self._create_table_with_computed_column("//tmp/t1")
        self.sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"key1": 1, "value": "test"}])
        self.sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError): alter_table("//tmp/t1", schema=[
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        with pytest.raises(YtError): alter_table("//tmp/t1", schema=[
            {"name": "key1", "type": "int64", "expression": "key2 * 100 + 3", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        with pytest.raises(YtError): alter_table("//tmp/t1", schema=[
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "expression": "key1 * 100", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        with pytest.raises(YtError): alter_table("//tmp/t1", schema=[
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "expression": "key1 * 100 + 3", "sort_order": "ascending"},
            {"name": "key3", "type": "int64", "expression": "key1 * 100 + 3", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])

        create("table", "//tmp/t2", attributes={"schema": [
            {"name": "key", "type": "int64", "sort_order": "ascending"}]})
        with pytest.raises(YtError): alter_table("//tmp/t2", dynamic=True)
        alter_table("//tmp/t2", schema=[
            {"name": "key", "type": "any", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        with pytest.raises(YtError): alter_table("//tmp/t2", dynamic=True)

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_update_key_columns_success(self, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for = optimize_for)

        self.sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        self.sync_mount_table("//tmp/t")

        rows2 = [{"key": i, "key2": 0, "value": str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows2)

        assert lookup_rows("//tmp/t", [{"key" : 77}]) == [{"key": 77, "key2": YsonEntity(), "value": "77"}]
        assert lookup_rows("//tmp/t", [{"key" : 77, "key2": 1}]) == []
        assert lookup_rows("//tmp/t", [{"key" : 77, "key2": 0}]) == [{"key": 77, "key2": 0, "value": "77"}]
        assert select_rows("sum(1) as s from [//tmp/t] where is_null(key2) group by 0") == [{"s": 100}]

    def test_create_table_with_invalid_schema(self):
        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={
                "dynamic": True,
                "schema": make_schema([{"name": "key", "type": "int64", "sort_order": "ascending"}])
                })
        assert not exists("//tmp/t")

    def test_atomicity_mode_should_match(self):
        def do(a1, a2):
            self.sync_create_cells(1)
            self._create_simple_table("//tmp/t", atomicity=a1)
            self.sync_mount_table("//tmp/t")
            rows = [{"key": i, "value": str(i)} for i in xrange(100)]
            with pytest.raises(YtError): insert_rows("//tmp/t", rows, atomicity=a2)
            remove("//tmp/t")

        do("full", "none")
        do("none", "full")

    @pytest.mark.parametrize("atomicity", ["full", "none"])
    def test_tablet_snapshots(self, atomicity):
        self.sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]

        self._create_simple_table("//tmp/t", atomicity=atomicity)
        self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows, atomicity=atomicity)

        build_snapshot(cell_id=cell_id)

        snapshots = ls("//sys/tablet_cells/" + cell_id + "/snapshots")
        assert len(snapshots) == 1

        self.Env.kill_nodes()
        # Wait to make sure all leases have expired
        time.sleep(3.0)
        self.Env.start_nodes()

        self.wait_for_cells()

        # Wait to make sure all tablets are up
        time.sleep(3.0)

        keys = [{"key": i} for i in xrange(100)]
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_stress_tablet_readers(self, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for = optimize_for)
        self.sync_mount_table("//tmp/t")

        values = dict()

        def verify():
            expected = [{"key": key, "value": values[key]} for key in values.keys()]
            actual = select_rows("* from [//tmp/t]")
            assert_items_equal(actual, expected)

            keys = list(values.keys())[::2]
            for i in xrange(len(keys)):
                if i % 3 == 0:
                    j = (i * 34567) % len(keys)
                    keys[i], keys[j] = keys[j], keys[i]

            expected = [{"key": key, "value": values[key]} for key in keys]

            if len(keys) > 0:
                actual = select_rows("* from [//tmp/t] where key in (%s)" % ",".join([str(key) for key in keys]))
                assert_items_equal(actual, expected)

            actual = lookup_rows("//tmp/t", [{"key": key} for key in keys])
            assert actual == expected

        verify()

        rounds = 10
        items = 100

        for wave in xrange(1, rounds):
            rows = [{"key": i, "value": str(i + wave * 100)} for i in xrange(0, items, wave)]
            for row in rows:
                values[row["key"]] = row["value"]
            print "Write rows ", rows
            insert_rows("//tmp/t", rows)

            verify()

            pivots = ([[]] + [[x] for x in xrange(0, items, items / wave)]) if wave % 2 == 0 else [[]]
            self._reshard_with_retries("//tmp/t", pivots)

            verify()

            keys = sorted(list(values.keys()))[::(wave * 12345) % items]
            print "Delete keys ", keys
            rows = [{"key": key} for key in keys]
            delete_rows("//tmp/t", rows)
            for key in keys:
                values.pop(key)

            verify()

    def test_rff_requires_async_last_committed(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 3}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", optimize_for = "scan", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        keys = [{"key": 1}]
        with pytest.raises(YtError): lookup_rows("//tmp/t", keys, read_from="follower")

    def test_rff_when_only_leader_exists(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        assert lookup_rows("//tmp/t", keys, read_from="follower") == rows

    def test_rff_lookup(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 3}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", optimize_for = "scan", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        sleep(1.0)
        assert lookup_rows("//tmp/t", keys, read_from="follower", timestamp=AsyncLastCommittedTimestamp) == rows

    def test_lookup_with_backup(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 3}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        sleep(1.0)
        for delay in xrange(0, 10):
            assert lookup_rows("//tmp/t", keys, read_from="follower", backup_request_delay=delay, timestamp=AsyncLastCommittedTimestamp) == rows

    def test_erasure(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for = "scan")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert get("#" + chunk_id + "/@erasure_codec") == "lrc_12_2_2"

        self.sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    def test_keep_missing_rows(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}, {"key": 2}]
        expect_rows = rows + [None]
        insert_rows("//tmp/t", rows)
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True);
        assert len(actual) == 2
        assert_items_equal(rows[0], actual[0])
        assert actual[1] == None

    def test_chunk_statistics(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        self.sync_unmount_table("//tmp/t")
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        statistics1 = get("#" + chunk_list_id + "/@statistics")
        self.sync_mount_table("//tmp/t")
        self.sync_compact_table("//tmp/t")
        statistics2 = get("#" + chunk_list_id + "/@statistics")
        # Disk space is not stable since it includes meta
        del statistics1["regular_disk_space"]
        del statistics2["regular_disk_space"]
        assert statistics1 == statistics2

    def test_tablet_statistics(self):
        cell_ids = self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        self.sync_freeze_table("//tmp/t")
        def check_statistics(statistics):
            assert statistics["tablet_count"] == 1
            assert statistics["tablet_count_per_memory_mode"]["none"] == 1
            assert statistics["chunk_count"] == get("//tmp/t/@chunk_count")
            assert statistics["uncompressed_data_size"] == get("//tmp/t/@uncompressed_data_size")
            assert statistics["compressed_data_size"] == get("//tmp/t/@compressed_data_size")
            assert statistics["disk_space"] == get("//tmp/t/@resource_usage/disk_space")
            assert statistics["disk_space_per_medium"]["default"] == get("//tmp/t/@resource_usage/disk_space_per_medium/default")
        statistics = get("//tmp/t/@tablet_statistics")
        assert statistics["overlapping_store_count"] == statistics["store_count"]
        check_statistics(statistics)
        statistics = get("#{0}/@total_statistics".format(cell_ids[0]))
        check_statistics(statistics)

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_timestamp_access(self, optimize_for):
        self.sync_create_cells(3)
        self._create_simple_table("//tmp/t", optimize_for = optimize_for)
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", rows)

        assert lookup_rows("//tmp/t", keys, timestamp=MinTimestamp) == []
        assert select_rows("* from [//tmp/t]", timestamp=MinTimestamp) == []

    def test_column_groups(self):
        self.sync_create_cells(1)
        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending", "group": "a"},
                    {"name": "value", "type": "string", "group": "a"}]
            })
        self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        assert lookup_rows("//tmp/t", keys) == rows
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    def test_freeze_empty(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        self.sync_freeze_table("//tmp/t")
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"key": 0}])
        self.sync_unfreeze_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_freeze_nonempty(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        rows = [{"key": 0, "value": "test"}]
        insert_rows("//tmp/t", rows)
        self.sync_freeze_table("//tmp/t")
        assert get("//tmp/t/@chunk_count") == 1
        assert select_rows("* from [//tmp/t]") == rows
        self.sync_unfreeze_table("//tmp/t")
        assert select_rows("* from [//tmp/t]") == rows
        self.sync_unmount_table("//tmp/t")

    def test_unmount_frozen(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        rows = [{"key": 0}]
        insert_rows("//tmp/t", rows)
        self.sync_freeze_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_mount_as_frozen(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "2"}]
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t", freeze=True)
        assert select_rows("* from [//tmp/t]") == rows

    def test_access_to_frozen(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "2"}]
        insert_rows("//tmp/t", rows)
        self.sync_freeze_table("//tmp/t")
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows
        assert select_rows("* from [//tmp/t]") == rows
        with pytest.raises(YtError): insert_rows("//tmp/t", rows)

    def _prepare_copy(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t1")
        reshard_table("//tmp/t1", [[]] + [[i * 100] for i in xrange(10)])

    def test_copy_failure(self):
        self._prepare_copy()
        self.sync_mount_table("//tmp/t1")
        with pytest.raises(YtError): copy("//tmp/t1", "//tmp/t2")

    def test_copy_empty(self):
        self._prepare_copy()
        copy("//tmp/t1", "//tmp/t2")

        root_chunk_list_id1 = get("//tmp/t1/@chunk_list_id")
        root_chunk_list_id2 = get("//tmp/t2/@chunk_list_id")
        assert root_chunk_list_id1 != root_chunk_list_id2

        assert get("#{0}/@ref_counter".format(root_chunk_list_id1)) == 1
        assert get("#{0}/@ref_counter".format(root_chunk_list_id2)) == 1

        child_ids1 = get("#{0}/@child_ids".format(root_chunk_list_id1))
        child_ids2 = get("#{0}/@child_ids".format(root_chunk_list_id2))
        assert child_ids1 == child_ids2

        for child_id in child_ids1:
            assert get("#{0}/@ref_counter".format(child_id)) == 2
            assert_items_equal(get("#{0}/@owning_nodes".format(child_id)), ["//tmp/t1", "//tmp/t2"])

    @pytest.mark.parametrize("unmount_func, mount_func, unmounted_state", [
        ["sync_unmount_table", "sync_mount_table", "unmounted"],
        ["sync_freeze_table", "sync_unfreeze_table", "frozen"]])
    def test_copy_simple(self, unmount_func, mount_func, unmounted_state):
        self._prepare_copy()
        self.sync_mount_table("//tmp/t1")
        rows = [{"key": i * 100 - 50} for i in xrange(10)]
        insert_rows("//tmp/t1", rows)
        getattr(self, unmount_func)("//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t1/@tablet_state") == unmounted_state
        assert get("//tmp/t2/@tablet_state") == "unmounted"
        getattr(self, mount_func)("//tmp/t1")
        self.sync_mount_table("//tmp/t2")
        assert_items_equal(select_rows("key from [//tmp/t1]"), rows)
        assert_items_equal(select_rows("key from [//tmp/t2]"), rows)

    @pytest.mark.parametrize("unmount_func, mount_func, unmounted_state", [
        ["sync_unmount_table", "sync_mount_table", "unmounted"],
        ["sync_freeze_table", "sync_unfreeze_table", "frozen"]])
    def test_copy_and_fork(self, unmount_func, mount_func, unmounted_state):
        self._prepare_copy()
        self.sync_mount_table("//tmp/t1")
        rows = [{"key": i * 100 - 50} for i in xrange(10)]
        insert_rows("//tmp/t1", rows)
        getattr(self, unmount_func)("//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t1/@tablet_state") == unmounted_state
        assert get("//tmp/t2/@tablet_state") == "unmounted"
        getattr(self, mount_func)("//tmp/t1")
        self.sync_mount_table("//tmp/t2")
        ext_rows1 = [{"key": i * 100 - 51} for i in xrange(10)]
        ext_rows2 = [{"key": i * 100 - 52} for i in xrange(10)]
        insert_rows("//tmp/t1", ext_rows1)
        insert_rows("//tmp/t2", ext_rows2)
        assert_items_equal(select_rows("key from [//tmp/t1]"), rows + ext_rows1)
        assert_items_equal(select_rows("key from [//tmp/t2]"), rows + ext_rows2)

    def test_copy_and_compact(self):
        self._prepare_copy()
        self.sync_mount_table("//tmp/t1")
        rows = [{"key": i * 100 - 50} for i in xrange(10)]
        insert_rows("//tmp/t1", rows)
        self.sync_unmount_table("//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        self.sync_mount_table("//tmp/t1")
        self.sync_mount_table("//tmp/t2")

        original_chunk_ids1 = __builtin__.set(get("//tmp/t1/@chunk_ids"))
        original_chunk_ids2 = __builtin__.set(get("//tmp/t2/@chunk_ids"))
        assert original_chunk_ids1 == original_chunk_ids2

        ext_rows1 = [{"key": i * 100 - 51} for i in xrange(10)]
        ext_rows2 = [{"key": i * 100 - 52} for i in xrange(10)]
        insert_rows("//tmp/t1", ext_rows1)
        insert_rows("//tmp/t2", ext_rows2)

        set("//tmp/x", 1)
        revision = get("//tmp/@revision")
        set("//tmp/t1/@forced_compaction_revision", revision)
        remount_table("//tmp/t1")
        set("//tmp/t2/@forced_compaction_revision", revision)
        remount_table("//tmp/t2")

        wait(lambda: len(__builtin__.set(get("//tmp/t1/@chunk_ids")).intersection(original_chunk_ids1)) == 0)
        wait(lambda: len(__builtin__.set(get("//tmp/t2/@chunk_ids")).intersection(original_chunk_ids2)) == 0)

        compacted_chunk_ids1 = __builtin__.set(get("//tmp/t1/@chunk_ids"))
        compacted_chunk_ids2 = __builtin__.set(get("//tmp/t2/@chunk_ids"))
        assert len(compacted_chunk_ids1.intersection(compacted_chunk_ids2)) == 0

        assert_items_equal(select_rows("key from [//tmp/t1]"), rows + ext_rows1)
        assert_items_equal(select_rows("key from [//tmp/t2]"), rows + ext_rows2)

    def test_mount_static_table_fails(self):
        self.sync_create_cells(1)
        create("table", "//tmp/t",
            attributes={
                "dynamic": False,
                "external": False,
                "schema": [
                     {"name": "key", "type": "int64", "sort_order": "ascending"},
                     {"name": "value", "type": "string"}]
            })
        assert not get("//tmp/t/@schema/@unique_keys")
        with pytest.raises(YtError): alter_table("//tmp/t", dynamic=True)

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("in_memory_mode, enable_lookup_hash_table", [
        ["none", False],
        ["compressed", False],
        ["uncompressed", True]])
    def test_mount_static_table(self, in_memory_mode, enable_lookup_hash_table, optimize_for):
        self.sync_create_cells(1)
        create("table", "//tmp/t",
            attributes={
                "dynamic": False,
                "external": False,
                "optimize_for": optimize_for,
                "schema": make_schema([
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                    {"name": "avalue", "type": "int64", "aggregate": "sum"}],
                    unique_keys=True)
            })
        rows = [{"key": i, "value": str(i), "avalue": 1} for i in xrange(2)]
        keys = [{"key": row["key"]} for row in rows] + [{"key": -1}, {"key": 1000}]

        start_ts = generate_timestamp()
        write_table("//tmp/t", rows)
        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@in_memory_mode", in_memory_mode)
        set("//tmp/t/@enable_lookup_hash_table", enable_lookup_hash_table)
        end_ts = generate_timestamp()

        self.sync_mount_table("//tmp/t")
        sleep(1.0)

        assert lookup_rows("//tmp/t", keys, timestamp=start_ts) == []
        actual = lookup_rows("//tmp/t", keys)
        assert actual == rows
        actual = lookup_rows("//tmp/t", keys, timestamp=end_ts)
        assert actual == rows
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert actual == rows + [None, None]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, rows)

        rows = [{"key": i, "avalue": 1} for i in xrange(2)]
        insert_rows("//tmp/t", rows, aggregate=True, update=True)

        expected = [{"key": i, "value": str(i), "avalue": 2} for i in xrange(2)]
        actual = lookup_rows("//tmp/t", keys)
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        expected = [{"key": i, "avalue": 2} for i in xrange(2)]
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"])
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"], keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("key, avalue from [//tmp/t]")
        assert_items_equal(actual, expected)

        self.sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=[
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "nvalue", "type": "string"},
                    {"name": "value", "type": "string"},
                    {"name": "avalue", "type": "int64", "aggregate": "sum"}])

        self.sync_mount_table("//tmp/t")
        sleep(1.0)

        insert_rows("//tmp/t", rows, aggregate=True, update=True)

        expected = [{"key": i, "key2": None, "nvalue": None, "value": str(i), "avalue": 3} for i in xrange(2)]
        actual = lookup_rows("//tmp/t", keys)
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        expected = [{"key": i, "avalue": 3} for i in xrange(2)]
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"])
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"], keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("key, avalue from [//tmp/t]")
        assert_items_equal(actual, expected)

    def test_chunk_list_kind(self):
        self.sync_create_cells(1)
        create("table", "//tmp/t",
            attributes={
                "dynamic": False,
                "external": False,
                "schema": make_schema([
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}],
                    unique_keys=True)
            })
        write_table("//tmp/t", [{"key": 1, "value": "1"}])
        chunk_list = get("//tmp/t/@chunk_list_id")
        assert get("#{0}/@kind".format(chunk_list)) == "static"

        alter_table("//tmp/t", dynamic=True)
        root_chunk_list = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list = get("#{0}/@child_ids/0".format(root_chunk_list))
        assert get("#{0}/@kind".format(root_chunk_list)) == "sorted_dynamic_root"
        assert get("#{0}/@kind".format(tablet_chunk_list)) == "sorted_dynamic_tablet"


    def test_no_commit_ordering(self):
        self._create_simple_table("//tmp/t")
        assert not exists("//tmp/t/@commit_ordering")


    def test_set_pivot_keys_upon_construction_fail(self):
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[])
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[[10], [20]])
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[[], [1], [1]])

    def test_set_pivot_keys_upon_construction_success(self):
        self._create_simple_table("//tmp/t", pivot_keys=[[], [1], [2], [3]])
        assert get("//tmp/t/@tablet_count") == 4


    def test_type_conversion(self):
        self.sync_create_cells(1)
        create("table", "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "int64", "type": "int64", "sort_order": "ascending"},
                    {"name": "uint64", "type": "uint64"},
                    {"name": "boolean", "type": "boolean"},
                    {"name": "double", "type": "double"},
                    {"name": "any", "type": "any"}]
            })
        self.sync_mount_table("//tmp/t")

        row1 = {
            "int64": yson.YsonUint64(3),
            "uint64": 42,
            "boolean": "false",
            "double": 18,
            "any": {}
        }
        row2 = {
            "int64": yson.YsonUint64(3)
        }

        yson_with_type_conversion = loads("<enable_type_conversion=%true>yson")
        yson_without_type_conversion = loads("<enable_integral_type_conversion=%false>yson")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [row1], input_format=yson_without_type_conversion)
        insert_rows("//tmp/t", [row1], input_format=yson_with_type_conversion)

        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [row2], input_format=yson_without_type_conversion)
        assert len(lookup_rows("//tmp/t", [row2], input_format=yson_with_type_conversion)) == 1

        with pytest.raises(YtError):
            delete_rows("//tmp/t", [row2], input_format=yson_without_type_conversion)
        delete_rows("//tmp/t", [row2], input_format=yson_with_type_conversion)

        assert select_rows("* from [//tmp/t]") == []


    def test_retained_timestamp(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        t12 = get("//tmp/t/@retained_timestamp")
        t13 = get("//tmp/t/@unflushed_timestamp")
        assert t13 > t12

        t1 = generate_timestamp()
        assert t1 > t12
        # Wait for timestamp provider at the node.
        sleep(1)
        self.sync_mount_table("//tmp/t")
        # Wait for master to receive node statistics.
        sleep(1)
        t2 = get("//tmp/t/@unflushed_timestamp")
        assert t2 > t1
        assert get("//tmp/t/@retained_timestamp") == MinTimestamp

        rows = [{"key": i, "value": str(i)} for i in xrange(2)]
        insert_rows("//tmp/t", rows)
        self.sync_flush_table("//tmp/t")
        self.sync_compact_table("//tmp/t")
        t3 = get("//tmp/t/@retained_timestamp")
        t4 = get("//tmp/t/@unflushed_timestamp")
        assert t3 > MinTimestamp
        assert t2 < t4
        assert t3 < t4

        sleep(1)
        t11 = get("//tmp/t/@unflushed_timestamp")
        assert t4 < t11

        tx = start_transaction()
        lock("//tmp/t", mode="snapshot", tx=tx)
        t5 = get("//tmp/t/@retained_timestamp", tx=tx)
        t6 = get("//tmp/t/@unflushed_timestamp", tx=tx)
        sleep(1)
        self.sync_flush_table("//tmp/t")
        self.sync_compact_table("//tmp/t")
        sleep(1)
        t7 = get("//tmp/t/@retained_timestamp")
        t8 = get("//tmp/t/@unflushed_timestamp")
        t9 = get("//tmp/t/@retained_timestamp", tx=tx)
        t10 = get("//tmp/t/@unflushed_timestamp", tx=tx)
        assert t5 == t9
        assert t6 == t10
        assert t5 < t7
        assert t6 < t8
        abort_transaction(tx)

        self.sync_freeze_table("//tmp/t")
        sleep(1)
        t14 = get("//tmp/t/@unflushed_timestamp")
        assert t14 > t8

    def test_expired_timestamp(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@min_data_ttl", 0)

        ts = generate_timestamp()
        sleep(1)
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0}])
        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        self.sync_compact_table("//tmp/t")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 0}], timestamp=ts)

    def test_writer_config(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@chunk_writer", {"block_size": 1024})
        set("//tmp/t/@compression_codec", "none")
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": "A"*1024} for i in xrange(10)])
        self.sync_unmount_table("//tmp/t")

        chunks = get("//tmp/t/@chunk_ids")
        assert len(chunks) == 1
        assert get("#" + chunks[0] + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunks[0] + "/@max_block_size") < 1024 * 2

    def test_reshard_with_uncovered_chunk_fails(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@min_data_ttl", 0)
        self.sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i)} for i in xrange(3)]
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")

        chunk_id = get("//tmp/t/@chunk_ids")[0]
        reshard_table("//tmp/t", [[], [1], [2]])
        root_chunk_list = get("//tmp/t/@chunk_list_id")
        tablet_chunk_lists = get("#{0}/@child_ids".format(root_chunk_list))

        mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        wait(lambda: get("//tmp/t/@tablets/1/state") == "mounted")
        delete_rows("//tmp/t", [{"key": 1}])
        self.sync_unmount_table("//tmp/t")

        set("//tmp/t/@forced_compaction_revision", get("//tmp/t/@revision"))
        set("//tmp/t/@forced_compaction_revision", get("//tmp/t/@revision"))
        mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        wait(lambda: get("//tmp/t/@tablets/1/state") == "mounted")
        wait(lambda: chunk_id not in get("#{0}/@child_ids".format(tablet_chunk_lists[1])))
        self.sync_unmount_table("//tmp/t")

        assert get("#{0}/@child_ids".format(tablet_chunk_lists[0])) == [chunk_id]
        assert chunk_id not in get("#{0}/@child_ids".format(tablet_chunk_lists[1]))
        assert get("#{0}/@child_ids".format(tablet_chunk_lists[2])) == [chunk_id]
        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[]])

##################################################################

class TestSortedDynamicTablesMemoryLimit(TestSortedDynamicTablesBase):
    NUM_NODES = 1
    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "resource_limits": {
                "tablet_static_memory": 20000
            },
            "tablet_manager": {
                "preload_backoff_time": 5000
            },
        },
    }

    def _get_statistics(self, table):
        return get(table + "/@tablets/0/statistics")

    def _wait_preload(self, table):
        def is_preloaded():
            statistics = self._get_statistics(table)
            return (
                statistics["preload_completed_store_count"] > 0 and
                statistics["preload_pending_store_count"] == 0 and
                statistics["preload_failed_store_count"] == 0)

        wait(is_preloaded)

    def _wait_preload_failed(self, table):
        def is_preload_failed():
            statistics = self._get_statistics(table)
            return (
                statistics["preload_pending_store_count"] == 0 and
                statistics["preload_failed_store_count"] > 0)

        wait(is_preload_failed)

    def test_in_memory_limit_exceeded(self):
        LARGE = "//tmp/large"
        SMALL = "//tmp/small"

        def table_create(table):
            self._create_simple_table(
                table,
                optimize_for="lookup",
                in_memory_mode="uncompressed",
                max_dynamic_store_row_count=10,
                replication_factor=1,
                read_quorum=1,
                write_quorum=1,
            )

            self.sync_mount_table(table)

        def check_lookup(table, keys, rows):
            assert lookup_rows(table, keys) == rows

        def generate_string(amount):
            return "x" * amount

        def table_insert_rows(length, table):
            rows = [{"key": i, "value": generate_string(length)} for i in xrange(10)]
            keys = [{"key": row["key"]} for row in rows]
            insert_rows(table, rows)
            return keys, rows

        tablet_cell_attributes = {
            "changelog_replication_factor": 1,
            "changelog_read_quorum": 1,
            "changelog_write_quorum": 1,
            "changelog_account": "sys",
            "snapshot_account": "sys"
        }

        set("//sys/tablet_cell_bundles/default/@options", tablet_cell_attributes)

        self.sync_create_cells(1)

        table_create(LARGE)
        table_create(SMALL)

        # create large table over memory limit
        large_data = table_insert_rows(10000, LARGE)
        self.sync_flush_table(LARGE)
        self.sync_unmount_table(LARGE)

        # create small table for final preload checking
        small_data = table_insert_rows(1000, SMALL)
        self.sync_flush_table(SMALL)
        self.sync_unmount_table(SMALL)

        # mount large table to trigger memory limit
        self.sync_mount_table(LARGE)
        self._wait_preload(LARGE)
        check_lookup(LARGE, *large_data)

        # mount small table, preload must fail
        self.sync_mount_table(SMALL)
        self._wait_preload_failed(SMALL)

        # unmounting large table releases the memory to allow small table to be preloaded
        self.sync_unmount_table(LARGE)
        self._wait_preload(SMALL)
        check_lookup(SMALL, *small_data)

        # cleanup
        self.sync_unmount_table(SMALL)

##################################################################

class TestSortedDynamicTablesMetadataCaching(TestSortedDynamicTablesBase):
    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 2,

        "table_mount_cache": {
            "expire_after_successful_update_time": 60000,
            "refresh_time": 60000,
            "expire_after_failed_update_time": 1000,
            "expire_after_access_time": 300000
        }
    }

    # Reimplement dynamic table commands without calling clear_metadata_caches()

    def mount_table(self, path, **kwargs):
        kwargs["path"] = path
        return execute_command("mount_table", kwargs)

    def unmount_table(self, path, **kwargs):
        kwargs["path"] = path
        return execute_command("unmount_table", kwargs)

    def reshard_table(self, path, arg, **kwargs):
        kwargs["path"] = path
        kwargs["pivot_keys"] = arg
        return execute_command("reshard_table", kwargs)

    def sync_mount_table(self, path, **kwargs):
        self.mount_table(path, **kwargs)
        print "Waiting for tablets to become mounted..."
        self._wait_for_tablets(path, "mounted", **kwargs)

    def sync_unmount_table(self, path, **kwargs):
        self.unmount_table(path, **kwargs)
        print "Waiting for tablets to become unmounted..."
        self._wait_for_tablets(path, "unmounted", **kwargs)


    def test_select_with_expired_schema(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.reshard_table("//tmp/t", [[], [1]])
        self.sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i)} for i in xrange(2)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        self.sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=[
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}])
        self.sync_mount_table("//tmp/t")
        expected = [{"key": i, "key2": None, "value": str(i)} for i in xrange(2)]
        assert_items_equal(select_rows("* from [//tmp/t]"), expected)

    def test_metadata_cache_invalidation(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t1", enable_compaction_and_partitioning=False)
        self.sync_mount_table("//tmp/t1")

        rows = [{"key": i, "value": str(i)} for i in xrange(3)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t1", rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        self.sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError): lookup_rows("//tmp/t1", keys)
        clear_metadata_caches()
        self.sync_mount_table("//tmp/t1")

        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        self.sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError): select_rows("* from [//tmp/t1]")
        clear_metadata_caches()
        self.sync_mount_table("//tmp/t1")

        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        def reshard_mounted_table(path, pivots):
            self.sync_unmount_table("//tmp/t1")
            self.reshard_table("//tmp/t1", pivots)
            self.sync_mount_table("//tmp/t1")

        reshard_mounted_table("//tmp/t1", [[], [1]])
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        reshard_mounted_table("//tmp/t1", [[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        reshard_mounted_table("//tmp/t1", [[]])
        rows = [{"key": i, "value": str(i+1)} for i in xrange(3)]
        with pytest.raises(YtError): insert_rows("//tmp/t1", rows)
        insert_rows("//tmp/t1", rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

##################################################################

class TestSortedDynamicTablesMulticell(TestSortedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestSortedDynamicTablesMetadataCachingMulticell(TestSortedDynamicTablesMetadataCaching):
    NUM_SECONDARY_MASTER_CELLS = 2

