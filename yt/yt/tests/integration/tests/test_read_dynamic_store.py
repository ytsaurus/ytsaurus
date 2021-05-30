import pytest

from test_dynamic_tables import DynamicTablesBase
from test_sorted_dynamic_tables import TestSortedDynamicTablesBase
from test_ordered_dynamic_tables import TestOrderedDynamicTablesBase

from yt_env_setup import wait, Restarter, NODES_SERVICE
from yt_commands import *  # noqa
from yt.yson import YsonEntity, to_yson_type

from time import sleep
from copy import deepcopy

from yt.environment.helpers import assert_items_equal

##################################################################


def _validate_tablet_statistics(table):
    cell_id = ls("//sys/tablet_cells")[0]

    def check_statistics(statistics):
        return (
            statistics["tablet_count"] == get(table + "/@tablet_count")
            and statistics["chunk_count"] == get(table + "/@chunk_count")
            and statistics["uncompressed_data_size"] == get(table + "/@uncompressed_data_size")
            and statistics["compressed_data_size"] == get(table + "/@compressed_data_size")
            and statistics["disk_space"] == get(table + "/@resource_usage/disk_space")
        )

    tablet_statistics = get("//tmp/t/@tablet_statistics")
    assert check_statistics(tablet_statistics)

    wait(lambda: check_statistics(get("#{0}/@total_statistics".format(cell_id))))


def _make_path_with_range(path, lower_limit, upper_limit):
    range = {}
    if lower_limit:
        range["lower_limit"] = {
            "tablet_index": lower_limit[0],
            "row_index": lower_limit[1]
        }
    if upper_limit:
        range["upper_limit"] = {
            "tablet_index": upper_limit[0],
            "row_index": upper_limit[1]
        }
    return to_yson_type(path, attributes={"ranges": [range]})

##################################################################


@authors("ifsmirnov")
class TestReadSortedDynamicTables(TestSortedDynamicTablesBase):
    NUM_SCHEDULERS = 1
    ENABLE_BULK_INSERT = True

    simple_rows = [{"key": i, "value": str(i)} for i in range(10)]

    def _prepare_simple_table(self, path, **attributes):
        self._create_simple_table(path, **attributes)
        set(path + "/@enable_dynamic_store_read", True)
        sync_mount_table(path)
        insert_rows(path, self.simple_rows[::2])
        sync_flush_table(path)
        insert_rows(path, self.simple_rows[1::2])

    def _validate_cell_statistics(self, **kwargs):
        cell_id = ls("//sys/tablet_cells")[0]

        def _wait_func():
            statistics = get("#{}/@total_statistics".format(cell_id))
            for k, v in kwargs.iteritems():
                if statistics[k] != v:
                    return False
            return True

        wait(_wait_func)

    def _get_node_env_id(self, node):
        for i in range(self.NUM_NODES):
            if self.Env.get_node_address(i) == node:
                return i
        assert False

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

        _validate_tablet_statistics("//tmp/t")

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

    @pytest.mark.parametrize("freeze", [True, False])
    def test_bulk_insert_overwrite(self, freeze):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=YsonEntity())
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        create("table", "//tmp/p")
        write_table("//tmp/p", [{"key": 2, "value": "b"}])

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        if freeze:
            sync_freeze_table("//tmp/t")

        assert read_table("//tmp/t", tx=tx) == [{"key": 1, "value": "a"}]

        merge(in_="//tmp/p", out="//tmp/t", mode="ordered")
        assert read_table("//tmp/t") == [{"key": 2, "value": "b"}]

        if freeze:
            expected = read_table(
                "//tmp/t",
                tx=tx,
                table_reader={"dynamic_store_reader": {"retry_count": 1}},
            )
            actual = [{"key": 1, "value": "a"}]
            assert_items_equal(expected, actual)
        else:
            with pytest.raises(YtError):
                # We've lost the data, but at least master didn't crash.
                read_table(
                    "//tmp/t",
                    tx=tx,
                    table_reader={"dynamic_store_reader": {"retry_count": 1}},
                )

        _validate_tablet_statistics("//tmp/t")

        if freeze:
            sync_unfreeze_table("//tmp/t")

        sync_freeze_table("//tmp/t")
        copy("//tmp/t", "//tmp/copy")
        assert get("//tmp/copy/@enable_dynamic_store_read")
        sync_mount_table("//tmp/copy")
        assert_items_equal(read_table("//tmp/copy"), [{"key": 2, "value": "b"}])

    def test_accounting(self):
        cell_id = sync_create_cells(1)[0]

        def _get_cell_statistics():
            statistics = get("#{}/@total_statistics".format(cell_id))
            statistics.pop("disk_space_per_medium", None)

        empty_statistics = _get_cell_statistics()

        self._create_simple_table("//tmp/t")
        set("//tmp/t/@dynamic_store_auto_flush_period", YsonEntity())

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

    @pytest.mark.parametrize(
        "disturbance_type",
        ["cell_move", "unmount", "unmount_and_delete", "force_unmount"],
    )
    def test_locate(self, disturbance_type):
        cell_id = sync_create_cells(1)[0]

        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "a"}]
        insert_rows("//tmp/t", rows)

        create("table", "//tmp/out")
        op = merge(
            in_="//tmp/t",
            out="//tmp/out",
            spec={"testing": {"delay_inside_materialize": 10000}},
            track=False,
        )
        wait(lambda: op.get_state() == "materializing")

        if disturbance_type == "cell_move":
            self._disable_tablet_cells_on_peer(cell_id)
        elif disturbance_type == "unmount":
            sync_unmount_table("//tmp/t")
        elif disturbance_type == "unmount_and_delete":
            sync_unmount_table("//tmp/t")
            remove("//tmp/t")
        elif disturbance_type == "force_unmount":
            unmount_table("//tmp/t", force=True)

        op.track()
        if disturbance_type != "force_unmount":
            assert_items_equal(read_table("//tmp/out"), rows)

    def test_read_nothing_from_located_chunk(self):
        cell_id = sync_create_cells(1)[0]
        node = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set("//sys/cluster_nodes/{}/@disable_scheduler_jobs".format(node), True)
        node_id = self._get_node_env_id(node)

        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=YsonEntity())
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i} for i in range(50000)])
        timestamp = generate_timestamp()
        insert_rows("//tmp/t", [{"key": i} for i in range(50000, 60000)])

        create("table", "//tmp/out")
        op = map(
            in_="<timestamp={}>//tmp/t".format(timestamp),
            out="//tmp/out",
            spec={
                "job_io": {"table_reader": {"dynamic_store_reader": {"window_size": 1}}},
                "mapper": {"format": "json"},
                "max_failed_job_count": 1,
            },
            command="sleep 10; cat",
            ordered=True,
            track=False,
        )

        wait(lambda: op.get_state() in ("running", "completed"))
        sleep(1)
        sync_freeze_table("//tmp/t")

        with Restarter(self.Env, NODES_SERVICE, indexes=[node_id]):
            op.track()

        assert op.get_state() == "completed"

        # Stupid testing libs require quadratic time to compare lists
        # of unhashable items.
        actual = [row["key"] for row in read_table("//tmp/out", verbose=False)]
        expected = range(50000)
        assert actual == expected

    def test_map_without_dynamic_stores(self):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t", enable_store_rotation=False)
        create("table", "//tmp/p")
        map(
            in_="//tmp/t",
            out="//tmp/p",
            command="cat",
            ordered=True,
            spec={
                "enable_dynamic_store_read": False,
            },
        )
        assert read_table("//tmp/p") == self.simple_rows[::2]

    def test_dynamic_store_unavailable(self):
        sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]

        self._prepare_simple_table("//tmp/t_in", dynamic_store_auto_flush_period=YsonEntity())
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": "foo{}".format(i)} for i in range(10)]
        insert_rows("//tmp/t_in", rows)

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@disable_tablet_cells".format(node), True)

        wait(lambda: get("#{}/@health".format(cell_id)) == "failed")

        op = merge(in_="//tmp/t_in", out="//tmp/t_out", mode="ordered", track=False)
        wait(lambda: len(op.get_running_jobs()) > 0)

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@disable_tablet_cells".format(node), False)

        wait(lambda: get("#{}/@health".format(cell_id)) == "good")

        op.track()

        assert read_table("//tmp/t_out") == rows


class TestReadSortedDynamicTablesMulticell(TestReadSortedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


@authors("ifsmirnov")
class TestReadOrderedDynamicTables(TestOrderedDynamicTablesBase):
    NUM_SCHEDULERS = 1

    simple_rows = [{"a": i, "b": 1.0 * i, "c": str(i)} for i in range(10)]

    def _prepare_simple_table(self, path, **attributes):
        self._create_simple_table(path, **attributes)
        set(path + "/@enable_dynamic_store_read", True)
        sync_mount_table(path)
        insert_rows(path, self.simple_rows[:5])
        sync_flush_table(path)
        insert_rows(path, self.simple_rows[5:])

    # Verify read-table, {ordered,unordered}{merge,map}, sort, map-reduce.
    def _verify_all_operations(self, input_path, expected_output):
        assert read_table(input_path) == expected_output

        create("table", "//tmp/verify", force=True)
        merge(in_=input_path, out="//tmp/verify", mode="ordered")
        assert read_table("//tmp/verify") == expected_output

        create("table", "//tmp/verify", force=True)
        merge(in_=input_path, out="//tmp/verify", mode="unordered")
        assert_items_equal(read_table("//tmp/verify"), expected_output)

        create("table", "//tmp/verify", force=True)
        map(in_=input_path, out="//tmp/verify", command="cat", ordered=True)
        assert read_table("//tmp/verify") == expected_output

        create("table", "//tmp/verify", force=True)
        map(in_=input_path, out="//tmp/verify", command="cat", ordered=False)
        assert_items_equal(read_table("//tmp/verify"), expected_output)

        create("table", "//tmp/verify", force=True)
        sort(in_=input_path, out="//tmp/verify", sort_by=["a", "b"])
        assert_items_equal(read_table("//tmp/verify"), expected_output)

        create("table", "//tmp/verify", force=True)
        map_reduce(in_=input_path, out="//tmp/verify", sort_by=["a"], reducer_command="cat")
        assert_items_equal(read_table("//tmp/verify"), expected_output)

    def test_basic_read(self):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")

        assert read_table("//tmp/t") == self.simple_rows

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        self._verify_all_operations("//tmp/t", self.simple_rows)

        sync_freeze_table("//tmp/t")
        assert read_table("//tmp/t", tx=tx) == self.simple_rows

    def test_multiple_tablets_multiple_stores(self):
        sync_create_cells(1)
        tablet_count = 5
        batches_per_tablet = 4
        rows_per_batch = 5
        rows_per_tablet = rows_per_batch * batches_per_tablet
        self._create_simple_table(
            "//tmp/t",
            replication_factor=10,
            chunk_writer={"upload_replication_factor": 10},
            tablet_count=tablet_count,
            max_dynamic_store_row_count=rows_per_batch,
            dynamic_store_auto_flush_period=YsonEntity())
        sync_mount_table("//tmp/t")

        row_count = tablet_count * batches_per_tablet * rows_per_batch
        rows = [{"a": i, "b": 1.0 * i, "c": str(i)} for i in range(row_count)]
        offset = 0
        for tablet_index in range(tablet_count):
            for batch_index in range(batches_per_tablet):
                batch = deepcopy(rows[offset:offset + rows_per_batch])
                offset += rows_per_batch
                for row in batch:
                    row["$tablet_index"] = tablet_index
                for retry in range(10):
                    try:
                        insert_rows("//tmp/t", batch)
                        break
                    except YtError:
                        sleep(0.5)
                else:
                    assert False, "Failed to insert rows"

        get("//tmp/t/@chunk_count")
        get("//tmp/t/@chunk_ids")
        self._verify_all_operations("//tmp/t", rows)
        path_with_range = _make_path_with_range("//tmp/t", (1, 6), (3, 11))
        expected = rows[rows_per_tablet + 6:rows_per_tablet * 3 + 11]
        self._verify_all_operations(path_with_range, expected)

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        set("//tmp/t/@chunk_writer", {"upload_replication_factor": 1})
        remount_table("//tmp/t")
        sync_flush_table("//tmp/t")
        assert read_table(path_with_range, tx=tx) == expected

    @pytest.mark.parametrize("disturbance_type", ["cell_move", "unmount"])
    def test_locate_row_index(self, disturbance_type):
        cell_id = sync_create_cells(1)[0]

        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"a": i, "b": i * 1.0, "c": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows[:5])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", rows[5:])

        operations = []

        create("table", "//tmp/out1")
        op1 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 6), (0, 8)),
            out="//tmp/out1",
            spec={"testing": {"delay_inside_materialize": 10000}},
            track=False,
        )
        operations.append(("//tmp/out1", op1, rows[6:8]))

        create("table", "//tmp/out2")
        op1 = merge(
            in_=_make_path_with_range("//tmp/t", None, (0, 8)),
            out="//tmp/out2",
            spec={"testing": {"delay_inside_materialize": 10000}},
            track=False,
        )
        operations.append(("//tmp/out2", op1, rows[:8]))

        create("table", "//tmp/out3")
        op1 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 6), None),
            out="//tmp/out3",
            spec={"testing": {"delay_inside_materialize": 10000}},
            track=False,
        )
        operations.append(("//tmp/out3", op1, rows[6:]))

        create("table", "//tmp/out4")
        op1 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 6), (0, 15)),
            out="//tmp/out4",
            spec={"testing": {"delay_inside_materialize": 10000}},
            track=False,
        )
        operations.append(("//tmp/out4", op1, rows[6:15]))

        for _, op, _ in operations:
            wait(lambda: op.get_state() == "materializing")

        if disturbance_type == "cell_move":
            self._disable_tablet_cells_on_peer(cell_id)
        elif disturbance_type == "unmount":
            sync_unmount_table("//tmp/t")
        else:
            assert False

        for _, op, _ in operations:
            op.track()

        for output_table, op, rows in operations:
            op.track()
            assert_items_equal(read_table(output_table), rows)


##################################################################


@authors("ifsmirnov")
class TestReadGenericDynamicTables(DynamicTablesBase):
    NUM_SCHEDULERS = 1
    ENABLE_BULK_INSERT = True

    def _get_node_env_id(self, node):
        for i in range(self.NUM_NODES):
            if self.Env.get_node_address(i) == node:
                return i
        assert False

    @pytest.mark.parametrize("sorted", [True, False])
    def test_dynamic_store_id_pool(self, sorted):
        sync_create_cells(1)
        if sorted:
            self._create_sorted_table("//tmp/t")
        else:
            self._create_ordered_table("//tmp/t")
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
            stores = orchid["eden"]["stores"] if sorted else orchid["stores"]
            for store in stores.itervalues():
                if store["store_state"] == "active_dynamic":
                    return store["row_count"] < 5
            # Getting orchid is non-atomic, so we may miss the active store.
            return False

        wait(_wait_func)

        _validate_tablet_statistics("//tmp/t")

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
            if orchid is None:
                return False
            stores = orchid["eden"]["stores"] if sorted else orchid["stores"]
            for store in stores.itervalues():
                if store["store_state"] == "passive_dynamic":
                    return False
            return True

        wait(_wait_func)

        # NB: Usually the last flush should request dynamic store id. However, in rare cases
        # two flushes run concurrently and the id is not requested, thus only one dynamic store remains.
        wait(lambda: expected_store_count <= get("//tmp/t/@chunk_count") <= expected_store_count + 1)
        assert 1 <= _store_count_by_type()["dynamic_store"] <= 2

        assert read_table("//tmp/t", tx=tx) == rows

        _validate_tablet_statistics("//tmp/t")

    @pytest.mark.parametrize("sorted", [True, False])
    @pytest.mark.parametrize("disturbance_type", ["cell_move", "unmount"])
    def test_locate_preserves_limits(self, disturbance_type, sorted):
        cell_id = sync_create_cells(1)[0]
        node = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set("//sys/cluster_nodes/{}/@disable_scheduler_jobs".format(node), True)
        node_id = self._get_node_env_id(node)

        if sorted:
            self._create_sorted_table("//tmp/t")
        else:
            self._create_ordered_table("//tmp/t")
        set("//tmp/t/@dynamic_store_auto_flush_period", YsonEntity())
        sync_mount_table("//tmp/t")

        rows = [{"key": i} for i in range(50000)]
        insert_rows("//tmp/t", rows)

        if sorted:
            input_path = "//tmp/t[50:49950]"
        else:
            input_path = _make_path_with_range("//tmp/t", (0, 50), (0, 49950))

        create("table", "//tmp/out")
        op = map(
            in_=input_path,
            out="//tmp/out",
            spec={
                "job_io": {"table_reader": {"dynamic_store_reader": {"window_size": 1}}},
                "mapper": {"format": "json"},
                "max_failed_job_count": 1,
            },
            command="sleep 10; cat",
            ordered=True,
            track=False,
        )

        wait(lambda: op.get_state() in ("running", "completed"))
        sleep(1)

        if disturbance_type == "unmount":
            sync_unmount_table("//tmp/t")

        with Restarter(self.Env, NODES_SERVICE, indexes=[node_id]):
            op.track()

        # Stupid testing libs require quadratic time to compare lists
        # of unhashable items.
        actual = [row["key"] for row in read_table("//tmp/out", verbose=False)]
        expected = range(50, 49950)
        assert actual == expected

    @pytest.mark.parametrize("sorted", [True, False])
    def test_locate_flushed_to_no_chunk(self, sorted):
        sync_create_cells(1)
        if sorted:
            self._create_sorted_table("//tmp/t")
        else:
            self._create_ordered_table("//tmp/t")
        sync_mount_table("//tmp/t")

        # One dynamic store will be flushed to a real chunk, another to no chunk.
        rows = [{"key": 1, "value": "a"}]
        insert_rows("//tmp/t", rows)

        create("table", "//tmp/out")
        op = map(
            in_="//tmp/t",
            out="//tmp/out",
            command="cat",
            spec={
                "testing": {"delay_inside_materialize": 10000},
                "job_count": 2,
            },
            track=False,
        )
        wait(lambda: op.get_state() == "materializing")

        sync_unmount_table("//tmp/t")

        op.track()
        assert_items_equal(read_table("//tmp/out"), rows)

    # YT-14639
    @pytest.mark.parametrize("sorted", [True, False])
    def test_enableness_preserved_by_actions_consistently(self, sorted):
        set("//sys/@config/tablet_manager/enable_dynamic_store_read_by_default", False)
        cells = sync_create_cells(2)
        if sorted:
            self._create_sorted_table("//tmp/t", enable_dynamic_store_read=None)
        else:
            self._create_ordered_table("//tmp/t", enable_dynamic_store_read=None)
        sync_mount_table("//tmp/t", target_cell_ids=[cells[0]])

        chunk_list_id = get("//tmp/t/@chunk_list_id")

        set("//sys/@config/tablet_manager/enable_dynamic_store_read_by_default", True)
        print_debug("Original cell: {}".format(get("//tmp/t/@tablets/0/cell_id")))
        get("#{}/@tree".format(chunk_list_id))

        action_id = create(
            "tablet_action",
            "",
            attributes={
                "kind": "move",
                "tablet_ids": [get("//tmp/t/@tablets/0/tablet_id")],
                "cell_ids": [cells[1]],
                "keep_finished": True,
            })
        wait(lambda: get("#{}/@state".format(action_id)) == "completed")
        print_debug("Cell after move: {}".format(get("//tmp/t/@tablets/0/cell_id")))
        get("#{}/@tree".format(chunk_list_id))

        # Flush should not fail.
        sync_freeze_table("//tmp/t")
