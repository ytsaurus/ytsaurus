from yt_dynamic_tables_base import DynamicTablesBase
from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase
from .test_ordered_dynamic_tables import TestOrderedDynamicTablesBase

from yt_env_setup import Restarter, NODES_SERVICE

from yt_commands import (
    authors, print_debug, wait, create, ls, get, set, copy,
    remove, start_transaction,
    commit_transaction, lock, insert_rows, read_table, write_table, map, reduce, map_reduce, merge, sort,
    unmount_table, remount_table,
    generate_timestamp, get_tablet_leader_address, sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_flush_table, sync_reshard_table,
    get_singular_chunk_id, update_op_parameters,
    disable_scheduler_jobs_on_node, set_node_banned, disable_write_sessions_on_node, disable_tablet_cells_on_node,
    enable_tablet_cells_on_node)

from yt.common import YtError
import yt.yson as yson

import pytest

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


def _make_range(lower_limit, upper_limit):
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
    return range


def _make_path_with_range(path, lower_limit, upper_limit):
    range = _make_range(lower_limit, upper_limit)
    return yson.to_yson_type(path, attributes={"ranges": [range]})

##################################################################


@authors("ifsmirnov")
class TestReadSortedDynamicTables(TestSortedDynamicTablesBase):
    NUM_TEST_PARTITIONS = 2
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
            for k, v in kwargs.items():
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
        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
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

    def test_tablet_removed_from_abandoned_chunk(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        create("table", "//tmp/p")
        write_table("//tmp/p", [{"key": 2, "value": "b"}])

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        root_chunk_list_id = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list_id = get("#{}/@child_ids/0".format(root_chunk_list_id))
        dynamic_store_ids = get("#{}/@child_ids".format(tablet_chunk_list_id))

        for ds in dynamic_store_ids:
            assert get("#{}/@tablet_id".format(ds)) == tablet_id

        assert read_table("//tmp/t", tx=tx) == [{"key": 1, "value": "a"}]
        merge(in_="//tmp/p", out="//tmp/t", mode="ordered")
        assert read_table("//tmp/t") == [{"key": 2, "value": "b"}]

        for ds in dynamic_store_ids:
            assert get("#{}/@tablet_id".format(ds)) == "0-0-0-0"

        # Destroy the tablet. Simply removing the table wouldn't help since someone still holds
        # the lock and prevents unmounting the tablet.
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])

        for ds in dynamic_store_ids:
            assert get("#{}/@tablet_id".format(ds)) == "0-0-0-0"

    def test_accounting(self):
        cell_id = sync_create_cells(1)[0]

        def _get_cell_statistics():
            statistics = get("#{}/@total_statistics".format(cell_id))
            statistics.pop("disk_space_per_medium", None)

        empty_statistics = _get_cell_statistics()

        self._create_simple_table("//tmp/t")
        set("//tmp/t/@dynamic_store_auto_flush_period", yson.YsonEntity())

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
        disable_scheduler_jobs_on_node(node, "test read nothing from located chunk")
        node_index = get("//sys/cluster_nodes/{}/@annotations/yt_env_index".format(node))

        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
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

        with Restarter(self.Env, NODES_SERVICE, indexes=[node_index]):
            op.track()

        assert op.get_state() == "completed"

        # Stupid testing libs require quadratic time to compare lists
        # of unhashable items.
        actual = [row["key"] for row in read_table("//tmp/out", verbose=False)]
        expected = list(range(50000))
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

        self._prepare_simple_table("//tmp/t_in", dynamic_store_auto_flush_period=yson.YsonEntity())
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": "foo{}".format(i)} for i in range(10)]
        insert_rows("//tmp/t_in", rows)

        for node in ls("//sys/cluster_nodes"):
            disable_tablet_cells_on_node(node, "test dynamic store unavailable")

        wait(lambda: get("#{}/@health".format(cell_id)) == "failed")

        op = merge(in_="//tmp/t_in", out="//tmp/t_out", mode="ordered", track=False)
        wait(lambda: len(op.get_running_jobs()) > 0)

        for node in ls("//sys/cluster_nodes"):
            enable_tablet_cells_on_node(node)

        wait(lambda: get("#{}/@health".format(cell_id)) == "good")

        op.track()

        assert read_table("//tmp/t_out") == rows

    def test_dynamic_store_not_scraped(self):
        cell_id = sync_create_cells(1)[0]
        cell_node = get("#{}/@peers/0/address".format(cell_id))
        disable_write_sessions_on_node(cell_node, "test dynamic store not scraped")

        self._prepare_simple_table(
            "//tmp/t_in",
            enable_store_rotation=False,
            replication_factor=1)
        chunk_id = get_singular_chunk_id("//tmp/t_in")
        stored_replicas = get("#{}/@stored_replicas".format(chunk_id))
        assert len(stored_replicas) == 1
        chunk_node = stored_replicas[0]
        assert chunk_node != cell_node

        set_node_banned(chunk_node, True)

        create("table", "//tmp/t_out")
        op = merge(in_="//tmp/t_in", out="//tmp/t_out", mode="ordered", track=False)
        wait(lambda: op.get_state() == "materializing")

        set_node_banned(chunk_node, False)
        op.track()

    def test_dynamic_store_not_unavailable_after_job_aborted(self):
        cell_id = sync_create_cells(1)[0]
        cell_node = get("#{}/@peers/0/address".format(cell_id))
        disable_write_sessions_on_node(
            cell_node,
            "test dynamic store not unavailable after job aborted")

        self._prepare_simple_table(
            "//tmp/t_in",
            enable_store_rotation=False,
            replication_factor=1)
        chunk_id = get_singular_chunk_id("//tmp/t_in")
        stored_replicas = get("#{}/@stored_replicas".format(chunk_id))
        assert len(stored_replicas) == 1
        chunk_node = stored_replicas[0]
        assert chunk_node != cell_node

        create("table", "//tmp/t_out")
        op = merge(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mode="ordered",
            track=False,
            spec={
                "resource_limits": {"user_slots": 0},
                "max_failed_job_count": 1000,
                "job_io": {
                    "table_reader": {
                        "pass_count": 1,
                        "retry_count": 1,
                    },
                },
            })
        wait(lambda: op.get_state() == "running")

        set_node_banned(chunk_node, True)
        wait(lambda: get("#{}/@replication_status/default/lost".format(chunk_id)))

        update_op_parameters(
            op.id,
            parameters={
                "scheduling_options_per_pool_tree": {
                    "default": {"resource_limits": {"user_slots": 1}}
                }
            })

        ca_address = ls("//sys/controller_agents/instances")[0]
        orchid_path = "//sys/controller_agents/instances/{}/orchid/controller_agent/operations/{}/unavailable_input_chunks".format(
            ca_address,
            op.id)

        wait(lambda: get(orchid_path))
        assert get(orchid_path) == [chunk_id]

        set_node_banned(chunk_node, False)

        op.track()


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
            dynamic_store_auto_flush_period=yson.YsonEntity(),
            dynamic_store_overflow_threshold=1.0)
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
                for retry_index in range(10):
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

    @pytest.mark.parametrize("disturbance_type", ["cell_move", "unmount", "flush_under_lock"])
    def test_locate_row_index(self, disturbance_type):
        cell_id = sync_create_cells(1)[0]

        self._create_simple_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"a": i, "b": i * 1.0, "c": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows[:5])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", rows[5:])

        if disturbance_type == "flush_under_lock":
            tx = start_transaction(timeout=60000)
            lock("//tmp/t", mode="snapshot", tx=tx)
            sync_freeze_table("//tmp/t")
            spec = {}
        else:
            tx = "0-0-0-0"
            spec = {"testing": {"delay_inside_materialize": 5000}}

        operations = []

        create("table", "//tmp/out1")
        op1 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 6), (0, 8)),
            out="//tmp/out1",
            spec=spec,
            track=False,
            tx=tx,
        )
        operations.append(("//tmp/out1", op1, rows[6:8]))

        create("table", "//tmp/out2")
        op2 = merge(
            in_=_make_path_with_range("//tmp/t", None, (0, 8)),
            out="//tmp/out2",
            spec=spec,
            track=False,
            tx=tx,
        )
        operations.append(("//tmp/out2", op2, rows[:8]))

        create("table", "//tmp/out3")
        op3 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 6), None),
            out="//tmp/out3",
            spec=spec,
            track=False,
            tx=tx,
        )
        operations.append(("//tmp/out3", op3, rows[6:]))

        create("table", "//tmp/out4")
        op4 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 6), (0, 15)),
            out="//tmp/out4",
            spec=spec,
            track=False,
            tx=tx,
        )
        operations.append(("//tmp/out4", op4, rows[6:15]))

        create("table", "//tmp/out5")
        op5 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 10), (0, 15)),
            out="//tmp/out5",
            spec=spec,
            track=False,
            tx=tx,
        )
        operations.append(("//tmp/out5", op5, []))

        create("table", "//tmp/out6")
        op6 = merge(
            in_=_make_path_with_range("//tmp/t", (0, 4), (0, 6)),
            out="//tmp/out6",
            spec=spec,
            track=False,
            tx=tx,
        )
        operations.append(("//tmp/out6", op6, rows[4:6]))

        if disturbance_type != "flush_under_lock":
            for _, op, _ in operations:
                wait(lambda: op.get_state() == "materializing")

        if disturbance_type == "cell_move":
            self._disable_tablet_cells_on_peer(cell_id)
        elif disturbance_type == "unmount":
            sync_unmount_table("//tmp/t")
        elif disturbance_type == "flush_under_lock":
            pass
        else:
            assert False

        for _, op, _ in operations:
            op.track()

        if tx != "0-0-0-0":
            commit_transaction(tx)

        for output_table, op, rows in operations:
            assert_items_equal(read_table(output_table), rows)

    @pytest.mark.parametrize("disturbance_type", ["cell_move", "unmount", "flush_under_lock"])
    def test_locate_control_attributes(self, disturbance_type):
        cell_id = sync_create_cells(1)[0]

        self._create_simple_table(
            "//tmp/t",
            dynamic_store_auto_flush_period=yson.YsonEntity(),
            tablet_count=3)
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "$tablet_index": i // 5} for i in range(15)]
        insert_rows("//tmp/t", rows)

        ranges = [
            _make_range((0, 2), (1, 4)),
            _make_range((1, 1), (2, 3)),
            {},
        ]
        expected = [
            list(range(0 * 5 + 2, 1 * 5 + 4)),
            list(range(1 * 5 + 1, 2 * 5 + 3)),
            list(range(15)),
        ]

        path = yson.to_yson_type("//tmp/t", attributes={"ranges": ranges})

        spec = {
            "mapper": {
                "input_format": yson.loads(b"<format=text>yson"),
                "output_format": yson.loads(b"<columns=[a]>schemaful_dsv"),
                "enable_input_table_index": True,
            },
            "job_io": {
                "control_attributes": {
                    "enable_row_index": True,
                    "enable_tablet_index": True,
                    "enable_range_index": True,
                    "enable_table_index": True,
                }
            }
        }

        if disturbance_type == "flush_under_lock":
            tx = start_transaction(timeout=60000)
            lock("//tmp/t", mode="snapshot", tx=tx)
            sync_freeze_table("//tmp/t")
        else:
            tx = "0-0-0-0"
            spec["testing"] = {"delay_inside_materialize": 5000}

        create("table", "//tmp/out")
        create("table", "//tmp/empty_table")
        op = map(
            in_=["//tmp/empty_table", path],
            out="//tmp/out",
            command="cat",
            spec=spec,
            track=False,
            tx=tx,
        )

        if disturbance_type != "flush_under_lock":
            wait(lambda: op.get_state() == "materializing")

        if disturbance_type == "cell_move":
            self._disable_tablet_cells_on_peer(cell_id)
        elif disturbance_type == "unmount":
            sync_unmount_table("//tmp/t")
        elif disturbance_type == "flush_under_lock":
            pass
        else:
            assert False

        op.track()

        if tx != "0-0-0-0":
            commit_transaction(tx)

        actual = [[], [], []]
        current_range_index = None
        current_tablet_index = None
        current_row_index = None

        output_rows = read_table("//tmp/out")
        assert yson.loads(str.encode(output_rows[0]["a"].rstrip(";"))).attributes["table_index"] == 1
        for yson_row in output_rows[1:]:
            row = yson.loads(str.encode(yson_row["a"].rstrip(";")))
            if "tablet_index" in row.attributes:
                current_tablet_index = row.attributes["tablet_index"]
            elif "row_index" in row.attributes:
                current_row_index = row.attributes["row_index"]
            elif "range_index" in row.attributes:
                current_range_index = row.attributes["range_index"]
            else:
                assert not row.attributes
                key = row["a"]
                assert key % 5 == current_row_index
                assert key // 5 == current_tablet_index
                current_row_index += 1
                actual[current_range_index].append(key)

        for values in actual:
            values.sort()
        assert expected == actual

    # YT-19452
    @pytest.mark.parametrize("mode", ["unordered", "ordered"])
    def test_no_row_count_mismatch(self, mode):
        sync_create_cells(1)
        self._prepare_simple_table("//tmp/t")
        create("table", "//tmp/d")

        merge(in_="//tmp/t", out="//tmp/d", mode=mode, spec={"force_transform": True})
        assert get("//tmp/d/@row_count") == 10


##################################################################


@authors("ifsmirnov")
class TestReadGenericDynamicTables(DynamicTablesBase):
    NUM_SCHEDULERS = 1
    ENABLE_BULK_INSERT = True

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
        set("//tmp/t/@dynamic_store_auto_flush_period", yson.YsonEntity())
        set("//tmp/t/@dynamic_store_overflow_threshold", 1.0)
        sync_mount_table("//tmp/t")
        rows = []
        for i in range(40):
            row = {"key": i, "value": str(i)}
            rows.append(row)
            for retry_index in range(5):
                try:
                    insert_rows("//tmp/t", [row])
                    break
                except YtError:
                    sleep(0.5)
            else:
                raise Exception("Failed to insert rows")

        # Wait till the active store is not overflown.
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _wait_func():
            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            stores = orchid["eden"]["stores"] if sorted else orchid["stores"]
            for store in stores.values():
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
            for store in stores.values():
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
        disable_write_sessions_on_node(node, "test locate preserves limtis")
        node_index = get("//sys/cluster_nodes/{}/@annotations/yt_env_index".format(node))

        if sorted:
            self._create_sorted_table("//tmp/t")
        else:
            self._create_ordered_table("//tmp/t")
        set("//tmp/t/@dynamic_store_auto_flush_period", yson.YsonEntity())
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

        with Restarter(self.Env, NODES_SERVICE, indexes=[node_index]):
            op.track()

        # Stupid testing libs require quadratic time to compare lists
        # of unhashable items.
        actual = [row["key"] for row in read_table("//tmp/out", verbose=False)]
        expected = list(range(50, 49950))
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
