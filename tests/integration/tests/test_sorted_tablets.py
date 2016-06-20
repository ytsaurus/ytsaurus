import pytest

from yt_env_setup import YTEnvSetup, make_schema, make_ace, wait
from yt_commands import *
from yt.yson import YsonEntity, YsonList

from time import sleep

from yt.environment.helpers import assert_items_equal

##################################################################

class TestSortedTablets(YTEnvSetup):
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

    def _create_simple_table(self, path, atomicity="full", optimize_for="lookup", tablet_cell_bundle="default"):
        create("table", path,
            attributes={
                "dynamic": True,
                "atomicity": atomicity,
                "optimize_for": optimize_for,
                "tablet_cell_bundle": tablet_cell_bundle,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
            })

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

    def _create_table_with_aggregate_column(self, path, aggregate = "sum", optimize_for="lookup"):
        create("table", path,
            attributes={
                "dynamic": True,
                "optimize_for" : optimize_for,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "time", "type": "int64"},
                    {"name": "value", "type": "int64", "aggregate": aggregate}]
            })

    def _get_tablet_leader_address(self, tablet_id):
        cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
        peers = get("//sys/tablet_cells/" + cell_id + "/@peers")
        leader_peer = list(x for x in peers if x["state"] == "leading")[0]
        return leader_peer["address"]

    def _find_tablet_orchid(self, address, tablet_id):
        path = "//sys/nodes/" + address + "/orchid/tablet_cells"
        cells = ls(path)
        for cell_id in cells:
            if get(path + "/" + cell_id + "/state") == "leading":
                tablets = ls(path + "/" + cell_id + "/tablets")
                if tablet_id in tablets:
                    return get(path + "/" + cell_id + "/tablets/" + tablet_id, ignore_opaque=True)
        return None

    def _get_pivot_keys(self, path):
        tablets = get(path + "/@tablets")
        return [tablet["pivot_key"] for tablet in tablets]

    def _ban_all_peers(self, cell_id):
        peers = get("#" + cell_id + "/@peers")
        for x in peers:
            self.set_node_banned(x["address"], True)
        clear_metadata_caches()
    
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

    def test_read_invalid_limits(self):
        self.sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        with pytest.raises(YtError):  read_table("//tmp/t[#5:]")
        with pytest.raises(YtError):  read_table("<ranges=[{lower_limit={chunk_index = 0};upper_limit={chunk_index = 1}}]>//tmp/t")

    def _test_read_table(self, optimize_for):
        self.sync_create_cells(1)

        self._create_simple_table("//tmp/t", optimize_for=optimize_for)
        self.sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        self.sync_unmount_table("//tmp/t")

        assert read_table("//tmp/t") == rows1
        assert get("//tmp/t/@chunk_count") == 1

    def test_read_table_scan(self):
        self._test_read_table("scan")

    def test_read_table_lookup(self):
        self._test_read_table("lookup")

    def test_read_snapshot_lock(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        def get_chunk_tree(path):
            root_chunk_list_id = get(path + "/@chunk_list_id")
            root_chunk_list = get("#" + root_chunk_list_id + "/@")
            tablet_chunk_lists = [get("#" + x + "/@") for x in root_chunk_list["child_ids"]]
            assert all([root_chunk_list_id in chunk_list["parent_ids"] for chunk_list in tablet_chunk_lists])
            assert get("//tmp/t/@chunk_count") == sum([len(chunk_list["child_ids"]) for chunk_list in tablet_chunk_lists])
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

    def test_write_table(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        with pytest.raises(YtError): write_table("//tmp/t", [{"key": 1, "value": 2}])

    def _test_computed_columns(self, optimize_for):
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

    def test_computed_columns_lookup(self):
        self._test_computed_columns("lookup")

    def test_computed_columns_scan(self):
        self._test_computed_columns("scan")

    def _test_computed_hash(self, optimize_for):
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

    def test_computed_hash_scan(self):
        self._test_computed_hash("scan")

    def test_computed_hash_lookup(self):
        self._test_computed_hash("lookup")

    def _test_computed_column_update_consistency(self, optimize_for):
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

    def test_computed_column_update_consistency_scan(self):
        self._test_computed_column_update_consistency("scan")

    def test_computed_column_update_consistency_lookup(self):
        self._test_computed_column_update_consistency("lookup")

    def _test_aggregate_columns(self, optimize_for):
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

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 3, "value": 30})
        test_row({"key": 1, "time": 4, "value": 10}, {"key": 1, "time": 4, "value": 40}, aggregate=True)
        test_row({"key": 1, "time": 5, "value": 10}, {"key": 1, "time": 5, "value": 50}, aggregate=True)
        test_row({"key": 1, "time": 6, "value": 10}, {"key": 1, "time": 6, "value": 60}, aggregate=True)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 6, "value": 60})
        test_row({"key": 1, "time": 7, "value": 10}, {"key": 1, "time": 7, "value": 70}, aggregate=True)
        test_row({"key": 1, "time": 8, "value": 10}, {"key": 1, "time": 8, "value": 80}, aggregate=True)
        test_row({"key": 1, "time": 9, "value": 10}, {"key": 1, "time": 9, "value": 90}, aggregate=True)

        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row({"key": 1, "time": 10, "value": 10}, {"key": 1, "time": 10, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 11, "value": 10}, {"key": 1, "time": 11, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 12, "value": 10}, {"key": 1, "time": 12, "value": 30}, aggregate=True)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 12, "value": 30})
        test_row({"key": 1, "time": 13, "value": 10}, {"key": 1, "time": 13, "value": 40}, aggregate=True)
        test_row({"key": 1, "time": 14, "value": 10}, {"key": 1, "time": 14, "value": 50}, aggregate=True)
        test_row({"key": 1, "time": 15, "value": 10}, {"key": 1, "time": 15, "value": 60}, aggregate=True)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 15, "value": 60})
        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row({"key": 1, "time": 16, "value": 10}, {"key": 1, "time": 16, "value": 10}, aggregate=True)
        test_row({"key": 1, "time": 17, "value": 10}, {"key": 1, "time": 17, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 18, "value": 10}, {"key": 1, "time": 18, "value": 30}, aggregate=True)

        self.sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 18, "value": 30})
        test_row({"key": 1, "time": 19, "value": 10}, {"key": 1, "time": 19, "value": 10})
        test_row({"key": 1, "time": 20, "value": 10}, {"key": 1, "time": 20, "value": 20}, aggregate=True)
        test_row({"key": 1, "time": 21, "value": 10}, {"key": 1, "time": 21, "value": 10})

        self.sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 21, "value": 10})

    def test_aggregate_columns_scan(self):
        self._test_aggregate_columns("scan")

    def test_aggregate_columns_lookup(self):
        self._test_aggregate_columns("lookup")

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

    def test_reshard_data(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t1", optimize_for = "scan")
        self.sync_mount_table("//tmp/t1")

        def reshard(pivots):
            self.sync_unmount_table("//tmp/t1")
            reshard_table("//tmp/t1", pivots)
            self.sync_mount_table("//tmp/t1")

        rows = [{"key": i, "value": str(i)} for i in xrange(3)]
        insert_rows("//tmp/t1", rows)
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        reshard([[], [1]])
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        reshard([[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        reshard([[]])
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

    def _test_any_value_type(self, optimize_for):
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

    def test_any_value_scan(self):
        self._test_any_value_type("scan")

    def test_any_value_lookup(self):
        self._test_any_value_type("lookup")

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

    def _test_read_from_chunks(self, optimize_for):
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

        sleep(1)
        for tablet in xrange(10):
            path = "//tmp/t/@tablets/%s/performance_counters" % tablet
            assert get(path + "/static_chunk_row_lookup_count") == 200
            #assert get(path + "/static_chunk_row_lookup_false_positive_count") < 4
            #assert get(path + "/static_chunk_row_lookup_true_negative_count") > 90

    def test_read_from_chunks_scan(self):
        self._test_read_from_chunks("scan")

    def test_read_from_chunks_lookup(self):
        self._test_read_from_chunks("lookup")

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

    def _test_in_memory(self, mode, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)

        set("//tmp/t/@in_memory_mode", mode)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        self.sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)

        def _check_preload_state(state):
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            assert len(tablet_data["eden"]["stores"]) == 1
            assert len(tablet_data["partitions"]) == 1
            assert len(tablet_data["partitions"][0]["stores"]) >= 1
            assert all(s["preload_state"] == state for _, s in tablet_data["partitions"][0]["stores"].iteritems())
            actual_preload_completed = get("//tmp/t/@tablets/0/statistics/preload_completed_store_count")
            if state == "complete":
                assert actual_preload_completed >= 1
            else:
                assert actual_preload_completed == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_pending_store_count") == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_failed_store_count") == 0

        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        keys = [{"key" : row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        sleep(3.0)

        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        set("//tmp/t/@in_memory_mode", "none")
        remount_table("//tmp/t")

        sleep(3.0)

        _check_preload_state("disabled")
        assert lookup_rows("//tmp/t", keys) == rows

        set("//tmp/t/@in_memory_mode", mode)
        remount_table("//tmp/t")

        sleep(3.0)

        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

    def test_in_memory_compressed_lookup(self):
        self._test_in_memory("compressed", "lookup")

    def test_in_memory_uncompressed_lookup(self):
        self._test_in_memory("uncompressed", "lookup")

    def test_in_memory_compressed_scan(self):
        self._test_in_memory("compressed", "scan")

    def test_in_memory_uncompressed_scan(self):
        self._test_in_memory("uncompressed", "scan")

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

        # check that stores are rotated on-demand
        insert_rows("//tmp/t", _rows(10, 20))
        # ensure slot gets scanned
        sleep(3)
        insert_rows("//tmp/t", _rows(20, 30))
        assert lookup_rows("//tmp/t", _keys(10, 30)) == _rows(10, 30)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        # check that we can delete rows
        delete_rows("//tmp/t", _keys(0, 10))
        assert lookup_rows("//tmp/t", _keys(0, 10)) == []

        # check that everything survives after recovery
        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        assert lookup_rows("//tmp/t", _keys(0, 50)) == _rows(10, 30)

        # check that we can extend key
        self.sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}]);
        self.sync_mount_table("//tmp/t")
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

    def test_update_schema_fails(self):
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

    def _test_update_key_columns_success(self, optimize_for):
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

    def test_update_key_columns_success_scan(self):
        self._test_update_key_columns_success("scan")

    def test_update_key_columns_success_lookup(self):
        self._test_update_key_columns_success("lookup")

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

    def _test_snapshots(self, atomicity):
        self.sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]

        self._create_simple_table("//tmp/t", atomicity=atomicity)
        self.sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows, atomicity=atomicity)

        build_snapshot(cell_id=cell_id)

        snapshots = ls("//sys/tablet_cells/" + cell_id + "/snapshots")
        assert len(snapshots) == 1

        self.Env.kill_service("node")
        # Wait to make sure all leases have expired
        time.sleep(3.0)
        self.Env.start_nodes("node")

        self.wait_for_cells()

        # Wait to make all tablets are up
        time.sleep(3.0)

        keys = [{"key": i} for i in xrange(100)]
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

    def test_atomic_snapshots(self):
        self._test_snapshots("full")

    def test_nonatomic_snapshots(self):
        self._test_snapshots("none")

    def _test_stress_tablet_readers(self, optimize_for):
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

            self.sync_unmount_table("//tmp/t")
            pivots = ([[]] + [[x] for x in xrange(0, items, items / wave)]) if wave % 2 == 0 else [[]]
            reshard_table("//tmp/t", pivots)
            self.sync_mount_table("//tmp/t")

            verify()

            keys = sorted(list(values.keys()))[::(wave * 12345) % items]
            print "Delete keys ", keys
            rows = [{"key": key} for key in keys]
            delete_rows("//tmp/t", rows)
            for key in keys:
                values.pop(key)

            verify()

    def test_stress_tablet_readers_scan(self):
        self._test_stress_tablet_readers("scan")

    def test_stress_tablet_readers_lookup(self):
        self._test_stress_tablet_readers("lookup")

    def test_recover_from_snapshot(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count" : 2}})
        self.sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        self.sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        cell_id = ls("//sys/tablet_cells")[0]
        build_snapshot(cell_id=cell_id)
        assert get("//sys/tablet_cells/" + cell_id + "/snapshots/@count") == 1
        self._ban_all_peers(cell_id)
        sleep(3.0)

        assert get("#" + cell_id + "/@health") == "good"
        assert lookup_rows("//tmp/t", keys) == rows

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

##################################################################

class TestSortedTabletsMulticell(TestSortedTablets):
    NUM_SECONDARY_MASTER_CELLS = 2


