from yt_dynamic_tables_base import DynamicTablesBase

from yt_env_setup import parametrize_external

from yt_commands import (
    authors, print_debug, wait, create, ls, get, set,
    remove, exists,
    create_tablet_cell_bundle, start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows,
    lock_rows, alter_table, read_table, write_table, remount_table,
    generate_timestamp, wait_for_cells, sync_create_cells,
    sync_mount_table, sync_unmount_table, sync_freeze_table,
    sync_unfreeze_table, sorted_dicts, mount_table, update_nodes_dynamic_config,
    sync_reshard_table, sync_flush_table, sync_compact_table,
    get_singular_chunk_id, create_dynamic_table, get_tablet_leader_address,
    raises_yt_error, build_snapshot, AsyncLastCommittedTimestamp, MinTimestamp,
    disable_write_sessions_on_node, set_node_banned, set_nodes_banned, disable_tablet_cells_on_node)

import yt_error_codes

from yt_helpers import profiler_factory

from yt_type_helpers import make_schema

from yt.environment.helpers import assert_items_equal
from yt.common import YtError, YtResponseError
import yt.yson as yson

import pytest

from random import randint, choice, sample
from string import ascii_lowercase
import random
import time
import builtins

##################################################################


def get_tablet_follower_addresses(tablet_id):
    cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
    peers = get("//sys/tablet_cells/" + cell_id + "/@peers")
    follower_peers = list(x for x in peers if x["state"] == "following")
    return [peer["address"] for peer in follower_peers]


##################################################################

class TestSortedDynamicTablesBase(DynamicTablesBase):
    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "timestamp_provider": {
                "update_period": 100
            }
        }
    }

    def _create_simple_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

    def _create_simple_static_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": make_schema([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}],
                unique_keys=True)
            })
        create("table", path, attributes=attributes)

    def _create_table_with_computed_column(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "key1", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64", "sort_order": "ascending", "expression": "key1 * 100 + 3"},
                {"name": "value", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

    def _create_table_with_hash(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "hash", "type": "uint64", "expression": "farm_hash(key)", "sort_order": "ascending"},
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

    def _wait_for_in_memory_stores_preload(self, table, first_tablet_index=None, last_tablet_index=None):
        def all_preloaded(address):
            orchid = self._find_tablet_orchid(address, tablet_id)
            if not orchid:
                return False
            for store in orchid["eden"]["stores"].values():
                if store["store_state"] == "persistent" and store["preload_state"] != "complete":
                    return False
            for partition in orchid["partitions"]:
                for store in partition["stores"].values():
                    if store["preload_state"] != "complete":
                        return False
            return True

        tablets = get(table + "/@tablets")
        if last_tablet_index is not None:
            tablets = tablets[:last_tablet_index + 1]
        if first_tablet_index is not None:
            tablets = tablets[first_tablet_index:]

        for tablet in tablets:
            tablet_id = tablet["tablet_id"]
            for address in get_tablet_follower_addresses(tablet_id) + [get_tablet_leader_address(tablet_id)]:
                wait(lambda: all_preloaded(address))

    def _wait_for_in_memory_stores_preload_failed(self, table):
        tablets = get(table + "/@tablets")
        for tablet in tablets:
            tablet_id = tablet["tablet_id"]
            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            if not orchid:
                return False
            for store in orchid["eden"]["stores"].values():
                if store["store_state"] == "persistent" and store["preload_state"] == "failed":
                    return True
            for partition in orchid["partitions"]:
                for store in partition["stores"].values():
                    if store["preload_state"] == "failed":
                        return True
            return False

    def _reshard_with_retries(self, path, pivots):
        resharded = False
        for i in range(4):
            try:
                sync_unmount_table(path)
                sync_reshard_table(path, pivots)
                resharded = True
            except YtError:
                pass
            sync_mount_table(path)
            if resharded:
                break
            time.sleep(5)
        assert resharded

    def _create_partitions(self, partition_count, do_overlap=False):
        assert partition_count > 1
        partition_count += 1 - int(do_overlap)

        def _force_compact_tablet(tablet_index):
            set("//tmp/t/@forced_compaction_revision", 1)

            chunk_list_id = get("//tmp/t/@chunk_list_id")
            tablet_chunk_list_id = get("#{0}/@child_ids/{1}".format(chunk_list_id, tablet_index))
            tablet_chunk_ids = builtins.set(get("#{}/@child_ids".format(tablet_chunk_list_id)))
            assert len(tablet_chunk_ids) > 0
            for id in tablet_chunk_ids:
                type = get("#{}/@type".format(id))
                assert type == "chunk" or type == "chunk_view"

            sync_mount_table("//tmp/t", first_tablet_index=tablet_index, last_tablet_index=tablet_index)

            def _check():
                new_tablet_chunk_ids = builtins.set(get("#{}/@child_ids".format(tablet_chunk_list_id)))
                assert len(new_tablet_chunk_ids) > 0
                return len(new_tablet_chunk_ids.intersection(tablet_chunk_ids)) == 0
            wait(lambda: _check())

            sync_unmount_table("//tmp/t")

        def _write_row(tablet_index, key_count=2):
            sync_mount_table("//tmp/t", first_tablet_index=tablet_index, last_tablet_index=tablet_index)
            rows = [{"key": tablet_index * 2 + i} for i in range(key_count)]
            insert_rows("//tmp/t", rows)
            sync_unmount_table("//tmp/t")
            _force_compact_tablet(tablet_index=tablet_index)

        set("//tmp/t/@min_partition_data_size", 1)

        if do_overlap:
            # We write overlapping chunk to trigger creation of chunk view.
            _write_row(tablet_index=0, key_count=3)

        partition_boundaries = [[]] + [[2 * i] for i in range(1, partition_count)]
        sync_reshard_table("//tmp/t", partition_boundaries)

        for tablet_index in range(1, partition_count):
            _write_row(tablet_index=tablet_index)

        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t", [[]])

    def _separate_tablet_and_data_nodes(self):
        self._nodes = ls("//sys/cluster_nodes")
        assert len(self._nodes) == self.NUM_NODES

        disable_write_sessions_on_node(self._nodes[0], "separate tablet and data nodes")
        for node in self._nodes[1:]:
            disable_tablet_cells_on_node(node, "separate tablet and data nodes")

    def _set_ban_for_chunk_parts(self, part_indices, banned_flag, chunk_id):
        chunk_replicas = get("#{}/@stored_replicas".format(chunk_id))

        nodes_to_ban = []
        for part_index in part_indices:
            nodes = list(str(r) for r in chunk_replicas if r.attributes["index"] == part_index)
            nodes_to_ban += nodes

        set_nodes_banned(nodes_to_ban, banned_flag)

    def _enable_hash_chunk_index(self, path):
        set("{}/@compression_codec".format(path), "none")
        set("{}/@mount_config/enable_hash_chunk_index_for_lookup".format(path), True)
        set("{}/@chunk_format".format(path), "table_versioned_indexed")

    def _enable_data_node_lookup(self, path):
        set("{}/@enable_data_node_lookup".format(path), True)
        if exists("{}/@chunk_reader".format(path)):
            set("{}/@chunk_reader/prefer_local_replicas".format(path), False)
        else:
            set("{}/@chunk_reader".format(path), {"prefer_local_replicas": False})


##################################################################


class TestSortedDynamicTables(TestSortedDynamicTablesBase):
    NUM_TEST_PARTITIONS = 6

    @authors("ifsmirnov")
    def test_merge_rows_on_flush_removes_row(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@min_data_versions", 0)
        set("//tmp/t/@max_data_versions", 0)
        set("//tmp/t/@min_data_ttl", 0)
        set("//tmp/t/@max_data_ttl", 0)
        set("//tmp/t/@merge_rows_on_flush", True)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        assert select_rows("* from [//tmp/t]") == [{"key": 1, "value": "a"}]

        sync_unmount_table("//tmp/t")
        assert get("//tmp/t/@chunk_count") == 0

    @authors("savrus")
    def test_overflow_row_data_weight(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_store_rotation", False)
        set("//tmp/t/@max_dynamic_store_row_data_weight", 100)
        sync_mount_table("//tmp/t")
        rows = [{"key": 0, "value": "A" * 100}]
        insert_rows("//tmp/t", rows)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", rows)

    @authors("lukyan")
    def test_transaction_locks(self):
        sync_create_cells(1)

        attributes = {"schema": [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "int64", "lock": "a"},
            {"name": "b", "type": "int64", "lock": "b"},
            {"name": "c", "type": "int64", "lock": "c"}]
        }
        create_dynamic_table("//tmp/t", **attributes)
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 1, "a": 1}], update=True, tx=tx1)
        lock_rows("//tmp/t", [{"key": 1}], locks=["a", "c"], tx=tx1, lock_type="shared_weak")
        insert_rows("//tmp/t", [{"key": 1, "b": 2}], update=True, tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        assert lookup_rows("//tmp/t", [{"key": 1}], column_names=["key", "a", "b"]) == [{"key": 1, "a": 1, "b": 2}]

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")
        tx3 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 2, "a": 1}], update=True, tx=tx1)
        lock_rows("//tmp/t", [{"key": 2}], locks=["a", "c"], tx=tx1, lock_type="shared_weak")

        insert_rows("//tmp/t", [{"key": 2, "b": 2}], update=True, tx=tx2)
        lock_rows("//tmp/t", [{"key": 2}], locks=["c"], tx=tx2, lock_type="shared_weak")

        lock_rows("//tmp/t", [{"key": 2}], locks=["a"], tx=tx3, lock_type="shared_weak")

        commit_transaction(tx1)
        commit_transaction(tx2)

        with pytest.raises(YtError):
            commit_transaction(tx3)

        assert lookup_rows("//tmp/t", [{"key": 2}], column_names=["key", "a", "b"]) == [{"key": 2, "a": 1, "b": 2}]

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        lock_rows("//tmp/t", [{"key": 3}], locks=["a"], tx=tx1, lock_type="shared_weak")
        insert_rows("//tmp/t", [{"key": 3, "a": 1}], update=True, tx=tx2)

        commit_transaction(tx2)

        with pytest.raises(YtError):
            commit_transaction(tx1)

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        lock_rows("//tmp/t", [{"key": 3}], locks=["a"], tx=tx1, lock_type="shared_strong")
        insert_rows("//tmp/t", [{"key": 3, "a": 1}], update=True, tx=tx2)

        commit_transaction(tx1)

        with pytest.raises(YtError):
            commit_transaction(tx2)

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("lock_type", ["exclusive", "shared_write", "shared_strong"])
    def test_tablet_locks_persist_in_snapshots(self, lock_type):
        if self.DRIVER_BACKEND == "rpc":
            if lock_type == "exclusive":
                # TODO(ponasenko-rs): Remove after YT-20282.
                pytest.skip("Rpc proxy client drops exclusive locks without data")
            elif lock_type == "shared_write":
                # Shared write locks aren't supported on rpc proxy.
                pytest.skip()

        sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64", "lock": "value_lock"}
        ]

        create_dynamic_table("//tmp/t", schema=schema)
        set("//tmp/t/@enable_shared_write_locks", True)
        sync_mount_table("//tmp/t")

        tx = start_transaction(type="tablet")

        lock_tx = start_transaction(type="tablet")
        lock_rows("//tmp/t", [{"key": 0}], locks=["value_lock"], lock_type=lock_type, tx=lock_tx)
        commit_transaction(lock_tx)

        build_snapshot(cell_id=cell_id)
        snapshots = ls("//sys/tablet_cells/" + cell_id + "/snapshots")
        assert len(snapshots) == 1

        with self.CellsDisabled(clusters=["primary"], tablet_bundles=[get(f'#{cell_id}/@tablet_cell_bundle')]):
            pass

        insert_rows("//tmp/t", [{"key": 0, "value": 0}], tx=tx)
        with pytest.raises(YtError):
            commit_transaction(tx)

    @authors("ponasenko-rs")
    def test_transaction_shared_write_locks(self):
        if self.DRIVER_BACKEND == "rpc":
            # Shared write locks aren't supported on rpc proxy.
            pytest.skip()

        sync_create_cells(1)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "int64", "lock": "la"},
            {"name": "b", "type": "int64", "lock": "lb"},
            {"name": "c", "type": "int64", "lock": "lc"}
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        set("//tmp/t/@enable_shared_write_locks", True)
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 1, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        insert_rows("//tmp/t", [{"key": 1, "a": 2}], update=True, lock_type="shared_write", tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 1}], column_names=["key", "a"]) == [{"key": 1, "a": 2}])

        # write conflict
        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")
        tx3 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 2, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        insert_rows("//tmp/t", [{"key": 2, "a": 2}], update=True, tx=tx2)
        insert_rows("//tmp/t", [{"key": 2, "a": 3}], update=True, lock_type="shared_write", tx=tx3)

        commit_transaction(tx1)
        with pytest.raises(YtError):
            commit_transaction(tx2)
        commit_transaction(tx3)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 2}], column_names=["key", "a"]) == [{"key": 2, "a": 3}])

        # read conflict
        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")
        tx3 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 3, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        lock_rows("//tmp/t", [{"key": 3}], locks=["la"], tx=tx2)
        insert_rows("//tmp/t", [{"key": 3, "a": 3}], update=True, lock_type="shared_write", tx=tx3)

        commit_transaction(tx1)
        with pytest.raises(YtError):
            commit_transaction(tx2)
        commit_transaction(tx3)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 3}], column_names=["key", "a"]) == [{"key": 3, "a": 3}])

        # no conflict after shared write locks
        tx1 = start_transaction(type="tablet")
        insert_rows("//tmp/t", [{"key": 4, "a": 1}], update=True, tx=tx1)
        commit_transaction(tx1)

        wait(lambda:  lookup_rows("//tmp/t", [{"key": 4}], column_names=["key", "a"]) == [{"key": 4, "a": 1}])

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("has_explicit_lock_group", [True, False])
    def test_aggregate_shared_write_locks(self, has_explicit_lock_group):
        if self.DRIVER_BACKEND == "rpc":
            # Shared write locks aren't supported on rpc proxy.
            pytest.skip()

        sync_create_cells(1)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64", "aggregate": "sum", "required": True},
        ]
        if has_explicit_lock_group:
            schema[1]["lock"] = "value_lock"

        create_dynamic_table("//tmp/t", schema=schema)
        set("//tmp/t/@enable_shared_write_locks", True)
        sync_mount_table("//tmp/t")

        tx_ids = [start_transaction(type="tablet") for _ in range(10)]

        for tx_id in tx_ids:
            insert_rows("//tmp/t", [{"key": 0, "value": 1}], aggregate=True, lock_type="shared_write", tx=tx_id)
            commit_transaction(tx_id)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}], column_names=["key", "value"]) == [{"key": 0, "value": 10}])

    @authors("ponasenko-rs")
    def test_enable_shared_write_locks(self):
        if self.DRIVER_BACKEND == "rpc":
            # Shared write locks aren't supported on rpc proxy.
            pytest.skip()

        sync_create_cells(1)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "int64", "lock": "la"}
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 1, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        insert_rows("//tmp/t", [{"key": 1, "a": 2}], update=True, lock_type="shared_write", tx=tx2)

        with pytest.raises(YtError):
            commit_transaction(tx1)
        with pytest.raises(YtError):
            commit_transaction(tx2)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 1}], column_names=["key", "a"]) == [])

        set("//tmp/t/@enable_shared_write_locks", True)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 1, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        insert_rows("//tmp/t", [{"key": 1, "a": 2}], update=True, lock_type="shared_write", tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 1}], column_names=["key", "a"]) == [{"key": 1, "a": 2}])

    @authors("babenko", "savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_any_value_type(self, optimize_for):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t1",
            attributes={
                "dynamic": True,
                "optimize_for": optimize_for,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "any"}]
            }
        )

        sync_mount_table("//tmp/t1")

        rows = [
            {"key": 11, "value": 100},
            {"key": 12, "value": False},
            {"key": 13, "value": True},
            {"key": 14, "value": 2**63 + 1},
            {"key": 15, "value": "stroka"},
            {"key": 16, "value": [1, {"attr": 3}, 4]},
            {"key": 17, "value": {"numbers": [0, 1, 42]}}]

        insert_rows("//tmp/t1", rows)
        actual = select_rows("* from [//tmp/t1]")
        assert_items_equal(actual, rows)
        actual = lookup_rows("//tmp/t1", [{"key": row["key"]} for row in rows])
        assert_items_equal(actual, rows)

    @authors("babenko", "savrus")
    def test_yt_13441_empty_store_set(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t", freeze=True)
        assert select_rows("* from [//tmp/t] where key in (1)") == []

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    @pytest.mark.parametrize("new_scan_reader", [False, True])
    def test_in_memory(self, mode, optimize_for, new_scan_reader):
        if new_scan_reader and optimize_for != "scan":
            return

        cell_id = sync_create_cells(1)[0]
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for,
            in_memory_mode=mode,
            max_dynamic_store_row_count=10,
            enable_new_scan_reader_for_lookup=new_scan_reader,
            enable_new_scan_reader_for_select=new_scan_reader)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            set("//tmp/t/@in_memory_mode", "none")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        def _check_preload_state(state):
            time.sleep(1.0)
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            assert len(tablet_data["eden"]["stores"]) == 1
            for partition in tablet_data["partitions"]:
                assert all(s["preload_state"] == state for _, s in partition["stores"].items())
            actual_preload_completed = get("//tmp/t/@tablets/0/statistics/preload_completed_store_count")
            if state == "complete":
                assert actual_preload_completed >= 1
            else:
                assert actual_preload_completed == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_pending_store_count") == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_failed_store_count") == 0
            assert get("//tmp/t/@preload_state") == "complete"

        # Check preload after mount.
        rows = [{"key": i, "value": str(i)} for i in range(10)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Check preload after flush.
        rows = [{"key": i, "value": str(i + 1)} for i in range(10)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Check preload after compaction.
        sync_compact_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Disable in-memory mode
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "none")
        sync_mount_table("//tmp/t")
        _check_preload_state("none")
        assert lookup_rows("//tmp/t", keys) == rows

        # Re-enable in-memory mode
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", mode)
        sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Check cell statistics
        tablet_statistics = get("//tmp/t/@tablets/0/statistics")

        def _check():
            cell_statistics = get("//sys/tablet_cells/{}/@total_statistics".format(cell_id))
            return \
                cell_statistics["preload_completed_store_count"] == \
                tablet_statistics["preload_completed_store_count"] and \
                cell_statistics["preload_pending_store_count"] == 0
        wait(_check)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("enable_lookup_hash_table", [True, False])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("new_scan_reader", [False, True])
    def test_preload_block_range(self, enable_lookup_hash_table, optimize_for, new_scan_reader):
        if new_scan_reader and optimize_for != "scan":
            return

        create_tablet_cell_bundle("b", attributes={"options": {"peer_count": 3}})
        sync_create_cells(1, tablet_cell_bundle="b")
        set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 2**30)
        self._create_simple_table(
            "//tmp/t",
            tablet_cell_bundle="b",
            optimize_for=optimize_for,
            in_memory_mode="uncompressed",
            enable_lookup_hash_table=enable_lookup_hash_table,
            chunk_writer={"block_size": 1024},
            enable_new_scan_reader_for_lookup=new_scan_reader,
            enable_new_scan_reader_for_select=new_scan_reader)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(10000)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        memory_size = get("//tmp/t/@tablet_statistics/uncompressed_data_size")

        lower_bound = 3800
        upper_bound = 5200
        expected = rows[lower_bound:upper_bound]

        sync_reshard_table("//tmp/t", [[], [lower_bound], [upper_bound]])
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        self._wait_for_in_memory_stores_preload("//tmp/t", first_tablet_index=1, last_tablet_index=1)

        node = get_tablet_leader_address(get("//tmp/t/@tablets/1/tablet_id"))

        def _check_memory_usage():
            memory_usage = get("//sys/cluster_nodes/{}/@statistics/memory/tablet_static/used".format(node))
            return 0 < memory_usage < memory_size
        if optimize_for == "lookup":
            wait(_check_memory_usage)

        assert lookup_rows("//tmp/t", [{"key": i} for i in range(lower_bound, upper_bound)]) == expected
        wait(lambda: lookup_rows(
            "//tmp/t",
            [{"key": i} for i in range(lower_bound, upper_bound)],
            read_from="follower",
            timestamp=AsyncLastCommittedTimestamp
        ) == expected)

        assert_items_equal(
            select_rows("* from [//tmp/t] where key >= {} and key < {}".format(lower_bound, upper_bound)),
            expected)

    @authors("savrus", "sandello")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_lookup_hash_table(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for,
            nable_new_scan_reader_for_lookup=True)

        set("//tmp/t/@in_memory_mode", "uncompressed")
        set("//tmp/t/@enable_lookup_hash_table", True)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        sync_mount_table("//tmp/t")

        def _rows(i, j):
            return [{"key": k, "value": str(k)} for k in range(i, j)]

        def _keys(i, j):
            return [{"key": k} for k in range(i, j)]

        # check that we can insert rows
        insert_rows("//tmp/t", _rows(0, 5))
        assert lookup_rows("//tmp/t", _keys(0, 5)) == _rows(0, 5)

        # check that we can insert rows till capacity
        insert_rows("//tmp/t", _rows(5, 10))
        assert lookup_rows("//tmp/t", _keys(0, 10)) == _rows(0, 10)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")

        # check that stores are rotated on-demand
        insert_rows("//tmp/t", _rows(10, 20))
        # ensure slot gets scanned
        time.sleep(3)
        insert_rows("//tmp/t", _rows(20, 30))
        assert lookup_rows("//tmp/t", _keys(10, 30)) == _rows(10, 30)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")

        # check that we can delete rows
        delete_rows("//tmp/t", _keys(0, 10))
        assert lookup_rows("//tmp/t", _keys(0, 10)) == []

        # check that everything survives after recovery
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")
        assert lookup_rows("//tmp/t", _keys(0, 50)) == _rows(10, 30)

        # check that we can extend key
        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")
        assert lookup_rows("//tmp/t", _keys(0, 50), column_names=["key", "value"]) == _rows(10, 30)

    @authors("babenko", "levysotsky")
    def test_update_key_columns_fail1(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@key_columns", ["key", "key2"])

    @authors("babenko")
    def test_update_key_columns_fail2(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@key_columns", ["key2", "key3"])

    @authors("babenko")
    def test_update_key_columns_fail3(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@key_columns", [])

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_update_key_columns_success(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": str(i)} for i in range(100)]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        sync_mount_table("//tmp/t")

        rows2 = [{"key": i, "key2": 0, "value": str(i)} for i in range(100)]
        insert_rows("//tmp/t", rows2)

        assert lookup_rows("//tmp/t", [{"key": 77}]) == [{"key": 77, "key2": yson.YsonEntity(), "value": "77"}]
        assert lookup_rows("//tmp/t", [{"key": 77, "key2": 1}]) == []
        assert lookup_rows("//tmp/t", [{"key": 77, "key2": 0}]) == [{"key": 77, "key2": 0, "value": "77"}]
        assert select_rows("sum(1) as s from [//tmp/t] where is_null(key2) group by 0") == [{"s": 100}]

    @authors("babenko")
    def test_atomicity_mode_should_match(self):
        def do(a1, a2):
            sync_create_cells(1)
            self._create_simple_table("//tmp/t", atomicity=a1)
            sync_mount_table("//tmp/t")
            rows = [{"key": i, "value": str(i)} for i in range(100)]
            with pytest.raises(YtError):
                insert_rows("//tmp/t", rows, atomicity=a2)
            remove("//tmp/t")

        do("full", "none")
        do("none", "full")

    @authors("babenko")
    @pytest.mark.parametrize("atomicity", ["full", "none"])
    def test_tablet_snapshots(self, atomicity):
        sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]

        self._create_simple_table("//tmp/t", atomicity=atomicity)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(100)]
        insert_rows("//tmp/t", rows, atomicity=atomicity)

        build_snapshot(cell_id=cell_id)

        snapshots = ls("//sys/tablet_cells/" + cell_id + "/snapshots")
        assert len(snapshots) == 1

        with self.CellsDisabled(clusters=["primary"], tablet_bundles=[get(f'#{cell_id}/@tablet_cell_bundle')]):
            pass

        keys = [{"key": i} for i in range(100)]
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("new_scan_reader", [False, True])
    def test_stress_tablet_readers(self, optimize_for, new_scan_reader):
        if new_scan_reader and optimize_for != "scan":
            return

        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for,
            enable_new_scan_reader_for_lookup=new_scan_reader,
            enable_new_scan_reader_for_select=new_scan_reader)
        sync_mount_table("//tmp/t")

        values = dict()

        items = 100

        def verify():
            expected = [{"key": key, "value": values[key]} for key in list(values.keys())]
            actual = select_rows("* from [//tmp/t]")
            assert_items_equal(actual, expected)

            keys = list(values.keys())[::2]
            for i in range(len(keys)):
                if i % 3 == 0:
                    j = (i * 34567) % len(keys)
                    keys[i], keys[j] = keys[j], keys[i]

            expected = [{"key": key, "value": values[key]} for key in keys]

            if len(keys) > 0:
                actual = select_rows("* from [//tmp/t] where key in (%s)" % ",".join([str(key) for key in keys]))
                assert_items_equal(actual, expected)

            actual = lookup_rows("//tmp/t", [{"key": key} for key in keys])
            assert actual == expected

            keys = sample(range(1, items), 30)
            expected = [{"key": key, "value": values[key]} if key in values else None for key in keys]
            actual = lookup_rows("//tmp/t", [{"key": key} for key in keys], keep_missing_rows=True)
            assert actual == expected

        verify()

        rounds = 10

        for wave in range(1, rounds):
            rows = [{"key": i, "value": str(i + wave * 100)} for i in range(0, items, wave)]
            for row in rows:
                values[row["key"]] = row["value"]
            print_debug("Write rows ", rows)
            insert_rows("//tmp/t", rows)

            verify()

            pivots = ([[]] + [[x] for x in range(0, items, items // wave)]) if wave % 2 == 0 else [[]]
            self._reshard_with_retries("//tmp/t", pivots)

            verify()

            keys = sorted(list(values.keys()))[::(wave * 12345) % items]
            print_debug("Delete keys ", keys)
            rows = [{"key": key} for key in keys]
            delete_rows("//tmp/t", rows)
            for key in keys:
                values.pop(key)

            verify()

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("in_memory_mode", ["none", "uncompressed"])
    @pytest.mark.parametrize("new_scan_reader", [False, True])
    def test_stress_chunk_view(self, optimize_for, in_memory_mode, new_scan_reader):
        if new_scan_reader and optimize_for != "scan":
            return

        random.seed(98765)

        sync_create_cells(1)

        key_range = 100
        num_writes_per_iteration = 50
        num_deletes_per_iteration = 10
        num_write_iterations = 3
        num_lookup_iterations = 30

        def random_row():
            return {"key": randint(1, key_range), "value": "".join(choice(ascii_lowercase) for i in range(5))}

        # Prepare both tables.
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for,
            in_memory_mode=in_memory_mode,
            enable_new_scan_reader_for_lookup=new_scan_reader,
            enable_new_scan_reader_for_select=new_scan_reader)
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        self._create_simple_table("//tmp/correct")
        sync_mount_table("//tmp/correct")

        if in_memory_mode != "none":
            self._wait_for_in_memory_stores_preload("//tmp/t")
            self._wait_for_in_memory_stores_preload("//tmp/correct")

        for iter in range(num_write_iterations):
            insert_keys = [random_row() for i in range(num_writes_per_iteration)]
            delete_keys = [{"key": randint(1, key_range)} for i in range(num_deletes_per_iteration)]

            insert_rows("//tmp/t", insert_keys)
            delete_rows("//tmp/t", delete_keys)
            insert_rows("//tmp/correct", insert_keys)
            delete_rows("//tmp/correct", delete_keys)

            sync_flush_table("//tmp/t")
            num_pivots = randint(0, 5)
            pivots = [[]] + [[i] for i in sorted(sample(list(range(1, key_range + 1)), num_pivots))]
            sync_unmount_table("//tmp/t")
            sync_reshard_table("//tmp/t", pivots)
            sync_mount_table("//tmp/t")

            if in_memory_mode != "none":
                self._wait_for_in_memory_stores_preload("//tmp/t")

        for iter in range(num_lookup_iterations):
            # Lookup keys.
            keys = [{"key": randint(1, key_range)} for i in range(num_deletes_per_iteration)]

            expected = list(lookup_rows("//tmp/correct", keys))
            actual = list(lookup_rows("//tmp/t", keys))
            assert expected == actual

            # Lookup ranges.
            ranges_count = randint(1, 5)
            keys = sorted(sample(list(range(1, key_range + 1)), ranges_count * 2))
            query = "* from [{}] where " + " or ".join(
                    "({} <= key and key < {})".format(lower, upper)
                    for lower, upper
                    in zip(keys[::2], keys[1::2]))
            expected = list(select_rows(query.format("//tmp/correct")))
            actual = list(select_rows(query.format("//tmp/t")))
            assert sorted_dicts(expected) == sorted_dicts(actual)

    @authors("ifsmirnov")
    def test_save_chunk_view_to_snapshot(self):
        [cell_id] = sync_create_cells(1)
        print_debug(get("//sys/cluster_nodes", attributes=["tablet_slots"]))
        print_debug(get("//sys/tablet_cell_bundles/default/@options"))
        set("//sys/@config/tablet_manager/tablet_cell_balancer/rebalance_wait_time", 500)

        self._create_simple_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(2)])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")

        print_debug(get("//sys/tablet_cells/{}/@peers".format(cell_id)))
        build_snapshot(cell_id=cell_id)

        peer = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set_node_banned(peer, True)

        wait_for_cells([cell_id], decommissioned_addresses=[peer])

        assert list(lookup_rows("//tmp/t", [{"key": 0}])) == [{"key": 0, "value": "0"}]
        assert list(lookup_rows("//tmp/t", [{"key": 1}])) == [{"key": 1, "value": "1"}]

    @authors("babenko")
    def test_rff_requires_async_last_committed(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count": 3}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", optimize_for="scan", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        keys = [{"key": 1}]
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", keys, read_from="follower")

    @authors("babenko")
    def test_rff_when_only_leader_exists(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        assert lookup_rows("//tmp/t", keys, read_from="follower") == rows

    @authors("babenko")
    def test_rff_lookup(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count": 3}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", optimize_for="scan", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        wait(lambda: lookup_rows("//tmp/t", keys, read_from="follower", timestamp=AsyncLastCommittedTimestamp) == rows)

    @authors("babenko")
    def test_lookup_with_backup(self):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count": 3}})
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        time.sleep(1.0)
        for delay in range(0, 10):
            assert lookup_rows(
                "//tmp/t",
                keys,
                read_from="follower",
                rpc_hedging_delay=delay,
                timestamp=AsyncLastCommittedTimestamp) == rows

    @authors("babenko")
    def test_erasure(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for="scan")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert get("#" + chunk_id + "/@erasure_codec") == "lrc_12_2_2"

        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @authors("savrus")
    def test_keep_missing_rows(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}, {"key": 2}]
        insert_rows("//tmp/t", rows)
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert len(actual) == 2
        assert_items_equal(rows[0], actual[0])
        assert actual[1] == yson.YsonEntity()

    @authors("savrus", "levysotsky")
    def test_chunk_statistics(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        sync_unmount_table("//tmp/t")
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        statistics1 = get("#" + chunk_list_id + "/@statistics")
        sync_mount_table("//tmp/t")
        sync_compact_table("//tmp/t")
        statistics2 = get("#" + chunk_list_id + "/@statistics")
        # Disk space is not stable since it includes meta
        del statistics1["regular_disk_space"]
        del statistics2["regular_disk_space"]
        # Chunk count includes dynamic stores
        del statistics1["chunk_count"]
        del statistics1["logical_chunk_count"]
        del statistics2["chunk_count"]
        del statistics2["logical_chunk_count"]
        assert statistics1 == statistics2

    @authors("babenko")
    def test_tablet_statistics(self):
        cell_ids = sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        sync_freeze_table("//tmp/t")

        def check_statistics(statistics):
            return (
                statistics["tablet_count"] == 1 and
                statistics["tablet_count_per_memory_mode"]["none"] == 1 and
                statistics["chunk_count"] == get("//tmp/t/@chunk_count") and
                statistics["uncompressed_data_size"] == get("//tmp/t/@uncompressed_data_size") and
                statistics["compressed_data_size"] == get("//tmp/t/@compressed_data_size") and
                statistics["disk_space"] == get("//tmp/t/@resource_usage/disk_space") and
                statistics["disk_space_per_medium"]["default"] == get("//tmp/t/@resource_usage/disk_space_per_medium/default")
            )

        tablet_statistics = get("//tmp/t/@tablet_statistics")
        assert tablet_statistics["overlapping_store_count"] == tablet_statistics["store_count"]
        assert check_statistics(tablet_statistics)

        wait(lambda: check_statistics(get("#{0}/@total_statistics".format(cell_ids[0]))))

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_timestamp_access(self, optimize_for):
        sync_create_cells(3)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", rows)

        assert lookup_rows("//tmp/t", keys, timestamp=MinTimestamp) == []
        assert select_rows("* from [//tmp/t]", timestamp=MinTimestamp) == []

    @authors("savrus")
    def test_column_groups(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "optimize_for": "scan",
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending", "group": "a"},
                    {"name": "value", "type": "string", "group": "a"}]
            })
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert lookup_rows("//tmp/t", keys) == rows
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @authors("savrus")
    @parametrize_external
    def test_chunk_list_kind(self, external):
        sync_create_cells(1)
        self._create_simple_static_table("//tmp/t", external=external)
        write_table("//tmp/t", [{"key": 1, "value": "1"}])
        chunk_list = get("//tmp/t/@chunk_list_id")
        assert get("#{0}/@kind".format(chunk_list)) == "static"

        alter_table("//tmp/t", dynamic=True)
        root_chunk_list = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list = get("#{0}/@child_ids/0".format(root_chunk_list))
        assert get("#{0}/@kind".format(root_chunk_list)) == "sorted_dynamic_root"
        assert get("#{0}/@kind".format(tablet_chunk_list)) == "sorted_dynamic_tablet"

    @authors("babenko")
    def test_no_commit_ordering(self):
        self._create_simple_table("//tmp/t")
        assert not exists("//tmp/t/@commit_ordering")

    @authors("max42")
    def test_type_conversion(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "int64", "type": "int64", "sort_order": "ascending"},
                    {"name": "uint64", "type": "uint64"},
                    {"name": "boolean", "type": "boolean"},
                    {"name": "double", "type": "double"},
                    {"name": "any", "type": "any"}]
            })
        sync_mount_table("//tmp/t")

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

        yson_with_type_conversion = yson.loads(b"<enable_type_conversion=%true>yson")
        yson_without_type_conversion = yson.loads(b"<enable_integral_type_conversion=%false>yson")

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

    @authors("savrus")
    def test_retained_timestamp(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        t12 = get("//tmp/t/@retained_timestamp")
        t13 = get("//tmp/t/@unflushed_timestamp")
        assert t13 > t12

        t1 = generate_timestamp()
        assert t1 > t12
        # Wait for timestamp provider at the node.
        time.sleep(1)
        sync_mount_table("//tmp/t")
        # Wait for master to receive node statistics.
        time.sleep(1)
        t2 = get("//tmp/t/@unflushed_timestamp")
        assert t2 > t1
        assert get("//tmp/t/@retained_timestamp") == MinTimestamp

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")
        t3 = get("//tmp/t/@retained_timestamp")
        t4 = get("//tmp/t/@unflushed_timestamp")
        assert t3 > MinTimestamp
        assert t2 < t4
        assert t3 < t4

        time.sleep(1)
        t11 = get("//tmp/t/@unflushed_timestamp")
        assert t4 < t11

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)
        t5 = get("//tmp/t/@retained_timestamp", tx=tx)
        t6 = get("//tmp/t/@unflushed_timestamp", tx=tx)
        time.sleep(1)
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")
        time.sleep(1)
        t7 = get("//tmp/t/@retained_timestamp")
        t8 = get("//tmp/t/@unflushed_timestamp")
        t9 = get("//tmp/t/@retained_timestamp", tx=tx)
        t10 = get("//tmp/t/@unflushed_timestamp", tx=tx)
        assert t5 == t9
        assert t6 == t10
        assert t5 < t7
        assert t6 < t8
        abort_transaction(tx)

        sync_freeze_table("//tmp/t")
        time.sleep(1)
        t14 = get("//tmp/t/@unflushed_timestamp")
        assert t14 > t8

    @authors("savrus", "levysotsky")
    def test_expired_timestamp(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@min_data_ttl", 0)

        ts = generate_timestamp()
        time.sleep(1)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0}])
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        sync_compact_table("//tmp/t")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 0}], timestamp=ts)

    @authors("savrus")
    def test_writer_config(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@chunk_writer", {"block_size": 1024})
        set("//tmp/t/@compression_codec", "none")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": "A" * 1024} for i in range(10)])
        sync_unmount_table("//tmp/t")

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@compressed_data_size") > 1024 * 10
        assert get("#" + chunk_id + "/@max_block_size") < 1024 * 2

    @authors("savrus", "gridem")
    def test_expired_timestamp_read_remount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", min_data_ttl=0, min_data_versions=0)

        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        ts = generate_timestamp()
        assert lookup_rows("//tmp/t", keys, timestamp=ts) == rows

        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        with pytest.raises(YtResponseError):
            lookup_rows("//tmp/t", keys, timestamp=ts)
        with pytest.raises(YtResponseError):
            select_rows("* from [//tmp/t]", timestamp=ts)

        remount_table("//tmp/t")

        with pytest.raises(YtResponseError):
            lookup_rows("//tmp/t", keys, timestamp=ts)
        with pytest.raises(YtResponseError):
            select_rows("* from [//tmp/t]", timestamp=ts)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        with pytest.raises(YtResponseError):
            lookup_rows("//tmp/t", keys, timestamp=ts)
        with pytest.raises(YtResponseError):
            select_rows("* from [//tmp/t]", timestamp=ts)

    @authors("avmatrosov")
    def test_expired_timestamp_read_flush(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", min_data_ttl=0, min_data_versions=0, merge_rows_on_flush=True)

        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)

        ts = generate_timestamp()

        assert lookup_rows("//tmp/t", keys, timestamp=ts) == rows

        sync_flush_table("//tmp/t")

        with pytest.raises(YtResponseError):
            lookup_rows("//tmp/t", keys, timestamp=ts)
        with pytest.raises(YtResponseError):
            select_rows("* from [//tmp/t]", timestamp=ts)

    @authors("avmatrosov")
    def test_chunk_profiling(self):
        path = "//tmp/t"
        sync_create_cells(1)
        self._create_simple_table(path)
        sync_mount_table(path)

        profiler = profiler_factory().at_tablet_node(path, fixed_tags={
            "table_path": path,
            "method": "compaction",
            "account": "tmp",
            "medium": "default"
        })
        disk_space_counter = profiler.counter("chunk_writer/disk_space")
        data_weight_counter = profiler.counter("chunk_writer/data_weight")
        data_bytes_counter = profiler.counter("chunk_reader_statistics/data_bytes_read_from_disk")

        insert_rows(path, [{"key": 0, "value": "test"}])
        sync_compact_table(path)

        wait(lambda: disk_space_counter.get_delta() > 0)
        wait(lambda: data_weight_counter.get_delta() > 0)
        wait(lambda: data_bytes_counter.get_delta() > 0)

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("extend_twice", [False, True])
    def test_read_with_alter(self, optimize_for, extend_twice):
        sync_create_cells(1)
        schema1 = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"}]
        schema2 = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "string"}]
        if extend_twice:
            schema2 = schema2[:2] + [{"name": "key3", "type": "int64", "sort_order": "ascending"}] + schema2[2:]

        create("table", "//tmp/t", attributes={
            "dynamic": True,
            "optimize_for": optimize_for,
            "schema": schema1})

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value1": "0"}], update=True)

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=schema2)
        sync_mount_table("//tmp/t")

        expected_row = [{"key": 0, "key2": yson.YsonEntity(), "value1": "0", "value2": yson.YsonEntity()}]
        if extend_twice:
            expected_row[0]["key3"] = yson.YsonEntity()

        assert read_table("<ranges=[{lower_limit={key=[0;]}}]>//tmp/t") == expected_row
        assert read_table("<ranges=[{lower_limit={key=[0; <type=min>#;]}}]>//tmp/t") == expected_row
        assert read_table("<ranges=[{lower_limit={key=[<type=min>#; <type=max>#]}}]>//tmp/t") == expected_row

        assert read_table("<ranges=[{lower_limit={key=[0; <type=max>#;]}}]>//tmp/t") == []
        assert read_table("<ranges=[{lower_limit={key=[0; <type=null>#; <type=max>#]}}]>//tmp/t") == []
        assert read_table("<ranges=[{lower_limit={key=[0; <type=null>#; <type=null>#; <type=null>#]}}]>//tmp/t") == []

    @authors("ifsmirnov")
    def test_backing_stores(self):
        def _has_backing_store(address, tablet_id):
            orchid = self._find_tablet_orchid(address, tablet_id)
            for store in list(orchid["partitions"][0]["stores"].values()):
                if "backing_store" in store:
                    return True
            return False

        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            backing_store_retention_time=10000,
            max_dynamic_store_row_count=5,
            dynamic_store_auto_flush_period=yson.YsonEntity())
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in range(5)])
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 1)

        address = get_tablet_leader_address(tablet_id)

        assert _has_backing_store(address, tablet_id)
        wait(lambda: not _has_backing_store(address, tablet_id))

        set("//tmp/t/@backing_store_retention_time", 1000000000)
        remount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in range(5)])
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 2)

        assert _has_backing_store(address, tablet_id)
        set("//sys/tablet_cell_bundles/default/@dynamic_options/max_backing_store_memory_ratio", 0.00000001)
        wait(lambda: not _has_backing_store(address, tablet_id))

    @authors("ifsmirnov")
    def test_forced_chunk_view_compaction_revision(self):
        if self.is_multicell():
            return
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@mount_config/enable_narrow_chunk_view_compaction", False)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [2]])
        sync_mount_table("//tmp/t")

        def _get_chunk_view_count():
            chunk_list_id = get("//tmp/t/@chunk_list_id")
            tree = get("#{}/@tree".format(chunk_list_id))
            count = 0
            for tablet in tree:
                for store in tablet:
                    if store.attributes.get("type") == "chunk_view":
                        count += 1
            return count

        assert _get_chunk_view_count() == 2
        set("//tmp/t/@forced_chunk_view_compaction_revision", 1)
        remount_table("//tmp/t")
        wait(lambda: _get_chunk_view_count() == 0)

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_in_memory_erasure(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)
        set("//tmp/t/@in_memory_mode", "uncompressed")
        set("//tmp/t/@erasure_codec", "isa_lrc_12_2_2")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@preload_state") == "complete")
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows

    @authors("ifsmirnov")
    def test_retention_timestamp(self):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            schema=make_schema(
                [
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "u", "type": "string"},
                    {"name": "v", "type": "string"},
                ],
                unique_keys=True,
            ),
        )

        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"k": 1, "u": "u1", "v": "v1"}])
        ts2 = generate_timestamp()
        insert_rows("//tmp/t", [{"k": 1, "u": "u2"}], update=True)
        insert_rows("//tmp/t", [{"k": 2, "v": "v3"}])
        ts3 = generate_timestamp()

        expected1 = [
            {"k": 1, "u": "u2", "v": "v1"},
            {"k": 2, "u": yson.YsonEntity(), "v": "v3"},
        ]
        expected2 = [
            {"k": 1, "u": "u2", "v": yson.YsonEntity()},
            {"k": 2, "u": yson.YsonEntity(), "v": "v3"},
        ]
        keys = [{"k": 1}, {"k": 2}]

        create("table", "//tmp/t_out")

        def _check():
            assert read_table("//tmp/t") == expected1
            assert read_table("<retention_timestamp={}>//tmp/t".format(ts2)) == expected2
            assert read_table("<retention_timestamp={}>//tmp/t".format(ts3)) == []

            assert lookup_rows("//tmp/t", keys) == expected1
            assert lookup_rows("//tmp/t", keys, retention_timestamp=ts2) == expected2
            assert lookup_rows("//tmp/t", keys, retention_timestamp=ts3) == []

            assert_items_equal(select_rows("* from [//tmp/t]"), expected1)
            assert_items_equal(select_rows("* from [//tmp/t]", retention_timestamp=ts2), expected2)
            assert_items_equal(select_rows("* from [//tmp/t]", retention_timestamp=ts3), [])

        _check()
        sync_flush_table("//tmp/t")
        _check()

    @authors("ifsmirnov")
    def test_retention_timestamp_bounds(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        ts1 = generate_timestamp()
        ts2 = generate_timestamp()
        with pytest.raises(YtError):
            read_table("<timestamp={};retention_timestamp={}>//tmp/t".format(ts1, ts2))
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 1}], timestamp=ts1, retention_timestamp=ts2)
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", timestamp=ts1, retention_timestamp=ts2)

        read_table("<timestamp={};retention_timestamp={}>//tmp/t".format(ts1, ts1))
        lookup_rows("//tmp/t", [{"key": 1}], timestamp=ts1, retention_timestamp=ts1)
        select_rows("* from [//tmp/t]", timestamp=ts1, retention_timestamp=ts1)

    @authors("ifsmirnov")
    def test_retention_timestamp_with_timestamp(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        insert_rows("//tmp/t", [{"key": 2, "value": "2"}])
        delete_rows("//tmp/t", [{"key": 2}])
        ts1 = generate_timestamp()
        insert_rows("//tmp/t", [{"key": 2, "value": "2"}])
        ts2 = generate_timestamp()
        insert_rows("//tmp/t", [{"key": 3, "value": "3"}])

        sync_unmount_table("//tmp/t")

        actual = read_table("<timestamp={};retention_timestamp={}>//tmp/t".format(ts2, ts1))
        assert actual == [{"key": 2, "value": "2"}]

    @authors("ifsmirnov")
    def test_retention_timestamp_precise(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        insert_rows("//tmp/t", [{"key": 2, "value": "2"}])

        versioned_rows = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}], versioned=True)
        ts1, ts2 = [row.attributes["write_timestamps"][0] for row in versioned_rows]

        def _check(retention_ts, ts, expected_keys):
            expected = [{"key": k, "value": str(k)} for k in expected_keys]
            actual = lookup_rows(
                "//tmp/t", [{"key": 1}, {"key": 2}],
                timestamp=ts, retention_timestamp=retention_ts)
            assert actual == expected

        _check(ts1, ts2, [1, 2])
        _check(ts1 + 1, ts2, [2])
        _check(ts1, ts1, [1])
        _check(ts1 + 1, ts2 - 1, [])

    @authors("alexelexa")
    def test_lookup_if_cell_not_assigned_to_node(self):
        create_tablet_cell_bundle("b")
        cell_id = sync_create_cells(1, tablet_cell_bundle="b")[0]
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        sync_mount_table("//tmp/t")

        assert get("//sys/tablet_cell_bundles/b/@health") == "good"
        set("//sys/tablet_cell_bundles/b/@node_tag_filter", "invalid")
        wait(lambda: get("//sys/tablet_cell_bundles/b/@health") == "failed")

        with pytest.raises(YtError, match="Cell {} has no assigned peers".format(cell_id)):
            lookup_rows("//tmp/t", [{"key": 1}])


class TestSortedDynamicTablesMulticell(TestSortedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSortedDynamicTablesPortal(TestSortedDynamicTablesMulticell):
    ENABLE_TMP_PORTAL = True

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "timestamp_provider": {
                "update_period": 100
            }
        },
        "tablet_node": {
            "tablet_manager": {
                # This options is not related to portals, but we want to test
                # modes both with and without row shuffle, so we do shuffle in
                # this test.
                "shuffle_locked_rows": True,
            }
        }
    }


class TestSortedDynamicTablesRpcProxy(TestSortedDynamicTables):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    @authors("gritukan")
    def test_write_retries_stress(self):
        set("//sys/rpc_proxies/@config", {
            "cluster_connection": {
                "tablet_write_backoff": {
                    "retry_count": 5,
                    "min_backoff": 10,
                    "max_backoff": 50,
                },
            },
        })

        update_nodes_dynamic_config({
            "tablet_node": {
                "tablet_cell_write_manager": {
                    "write_failure_probability": 0.2,
                },
            },
        })

        proxy_name = ls("//sys/rpc_proxies")[0]

        def config_updated():
            config = get("//sys/rpc_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config["cluster_connection"]["tablet_write_backoff"]["invocation_count"] == 5
        wait(config_updated)

        cell_count = 5
        sync_create_cells(cell_count)
        cell_ids = ls("//sys/tablet_cells")
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64", "aggregate": "sum", "required": True},
        ]
        self._create_sorted_table("//tmp/t", schema=schema)
        sync_reshard_table("//tmp/t", [[]] + [[i * 10] for i in range(cell_count - 1)])
        for i in range(len(cell_ids)):
            mount_table(
                "//tmp/t",
                first_tablet_index=i,
                last_tablet_index=i,
                cell_id=cell_ids[i],
            )

        key_count = 50
        expected = [0] * 50

        iterations = 100
        for _ in range(iterations):
            rows = []
            if randint(1, 2) == 1:
                # 1PC.
                row_count = 1
            else:
                # 2PC.
                row_count = randint(2, 4)

            for _ in range(row_count):
                rows.append({"key": randint(0, key_count - 1), "value": randint(0, 10**9)})

            committed = False
            try:
                insert_rows("//tmp/t", rows, aggregate=True)
                committed = True
            except YtError:
                pass

            if committed:
                for row in rows:
                    expected[row["key"]] += row["value"]

        rows = lookup_rows("//tmp/t", [{"key": i} for i in range(key_count)])
        actual = [0] * 50
        for row in rows:
            actual[row["key"]] = row["value"]

        assert expected == actual


##################################################################


class TestSortedDynamicTablesSpecialColumns(TestSortedDynamicTablesBase):
    @authors("ifsmirnov")
    def test_required_columns(self):
        schema = [
            {"name": "key_req", "type": "int64", "sort_order": "ascending", "required": True},
            {"name": "key_opt", "type": "int64", "sort_order": "ascending"},
            {"name": "value_req", "type": "string", "required": True},
            {"name": "value_opt", "type": "string"}]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict()])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key_req=1, value_opt="data")])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key_opt=1, value_req="data", value_opt="data")])

        insert_rows("//tmp/t", [dict(key_req=1, value_req="data")])
        insert_rows("//tmp/t", [dict(key_req=1, key_opt=1, value_req="data", value_opt="data")])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key_req=1, key_opt=1, value_opt="other_data")], update=True)

        assert lookup_rows("//tmp/t", [dict(key_req=1, key_opt=1)]) == \
            [dict(key_req=1, key_opt=1, value_req="data", value_opt="data")]

        insert_rows("//tmp/t", [dict(key_req=1, key_opt=1, value_req="updated")], update=True)

        assert lookup_rows("//tmp/t", [dict(key_req=1, key_opt=1)]) == \
            [dict(key_req=1, key_opt=1, value_req="updated", value_opt="data")]

        with pytest.raises(YtError):
            delete_rows("//tmp/t", [dict(key_opt=1)])
        delete_rows("//tmp/t", [dict(key_req=1234)])
        delete_rows("//tmp/t", [dict(key_req=1, key_opt=1)])
        assert lookup_rows("//tmp/t", [dict(key_req=1, key_opt=1)]) == []

    @authors("ifsmirnov")
    def test_required_computed_columns(self):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {
                "name": "computed",
                "type": "int64",
                "sort_order": "ascending",
                "expression": "key * 10",
                "required": True,
            },
            {"name": "value", "type": "string"}]

        sync_create_cells(1)
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", schema=schema)

    @authors("ifsmirnov")
    def test_required_aggregate_columns(self):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64", "aggregate": "sum", "required": True}]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key=1)])
        insert_rows("//tmp/t", [dict(key=1, value=2)])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key=1)])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_select_omits_required_column(self, optimize_for):
        schema = make_schema([
            {"name": "key", "type": "int64", "sort_order": "ascending", "required": True},
            {"name": "value", "type": "string", "required": True},
        ], unique_keys=True)

        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=schema, dynamic=False, optimize_for=optimize_for)
        write_table("//tmp/t", {"key": 1, "value": "a"})
        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t", freeze=True)

        def _check():
            assert select_rows("key from [//tmp/t]") == [{"key": 1}]
            assert select_rows("key from [//tmp/t] where key in (1)") == [{"key": 1}]
            assert select_rows("value from [//tmp/t]") == [{"value": "a"}]

            assert lookup_rows("//tmp/t", [{"key": 1}], column_names=["key"]) == [{"key": 1}]
            assert lookup_rows("//tmp/t", [{"key": 1}], column_names=["value"]) == [{"value": "a"}]

        _check()
        sync_unfreeze_table("//tmp/t")
        sync_compact_table("//tmp/t")
        _check()

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_computed_columns(self, optimize_for):
        sync_create_cells(1)
        self._create_table_with_computed_column("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key1": 1, "value": "2"}])
        expected = [{"key1": 1, "key2": 103, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        insert_rows("//tmp/t", [{"key1": 2, "value": "2"}])
        expected = [{"key1": 1, "key2": 103, "value": "2"}]
        actual = lookup_rows("//tmp/t", [{"key1": 1}])
        assert_items_equal(actual, expected)
        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = lookup_rows("//tmp/t", [{"key1": 2}])
        assert_items_equal(actual, expected)

        delete_rows("//tmp/t", [{"key1": 1}])
        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key1": 3, "key2": 3, "value": "3"}])
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key1": 2, "key2": 203}])
        with pytest.raises(YtError):
            delete_rows("//tmp/t", [{"key1": 2, "key2": 203}])

        expected = []
        actual = lookup_rows("//tmp/t", [{"key1": 3}])
        assert_items_equal(actual, expected)

        expected = [{"key1": 2, "key2": 203, "value": "2"}]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_computed_hash(self, optimize_for):
        sync_create_cells(1)

        self._create_table_with_hash("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

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

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_computed_column_update_consistency(self, optimize_for):
        sync_create_cells(1)
        create_dynamic_table("//tmp/t", optimize_for=optimize_for, schema=[
            {"name": "key1", "type": "int64", "expression": "key2", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "string"}]
        )
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key2": 1, "value1": "2"}])
        expected = [{"key1": 1, "key2": 1, "value1": "2", "value2": yson.YsonEntity()}]
        actual = lookup_rows("//tmp/t", [{"key2": 1}])
        assert_items_equal(actual, expected)

        insert_rows("//tmp/t", [{"key2": 1, "value2": "3"}], update=True)
        expected = [{"key1": 1, "key2": 1, "value1": "2", "value2": "3"}]
        actual = lookup_rows("//tmp/t", [{"key2": 1}])
        assert_items_equal(actual, expected)

        insert_rows("//tmp/t", [{"key2": 1, "value1": "4"}], update=True)
        expected = [{"key1": 1, "key2": 1, "value1": "4", "value2": "3"}]
        actual = lookup_rows("//tmp/t", [{"key2": 1}])
        assert_items_equal(actual, expected)


class TestSortedDynamicTablesSpecialColumnsMulticell(TestSortedDynamicTablesSpecialColumns):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSortedDynamicTablesSpecialColumnsRpcProxy(TestSortedDynamicTablesSpecialColumns):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestSortedDynamicTablesSpecialColumnsPortal(TestSortedDynamicTablesSpecialColumnsMulticell):
    ENABLE_TMP_PORTAL = True

################################################################################


class TestSortedDynamicTablesMemoryLimit(TestSortedDynamicTablesBase):
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "resource_limits": {
                "tablet_static_memory": 40000
            },
            "tablet_manager": {
                "preload_backoff_time": 5000
            },
        },
    }

    @authors("savrus", "gridem")
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

            sync_mount_table(table)

        def check_lookup(table, keys, rows):
            assert lookup_rows(table, keys) == rows

        def generate_string(amount):
            return "x" * amount

        def table_insert_rows(length, table):
            rows = [{"key": i, "value": generate_string(length)} for i in range(10)]
            keys = [{"key": row["key"]} for row in rows]
            insert_rows(table, rows)
            return keys, rows

        sync_create_cells(1)

        table_create(LARGE)
        table_create(SMALL)

        # create large table over memory limit
        table_insert_rows(10000, LARGE)
        sync_flush_table(LARGE)
        sync_unmount_table(LARGE)

        # create small table for final preload checking
        small_data = table_insert_rows(1000, SMALL)
        sync_flush_table(SMALL)
        sync_unmount_table(SMALL)

        # mount large table, preload must fail
        sync_mount_table(LARGE)
        self._wait_for_in_memory_stores_preload_failed(LARGE)
        wait(lambda: get(LARGE + "/@preload_state") == "failed")

        for node in ls("//sys/cluster_nodes"):
            get("//sys/cluster_nodes/{}/@".format(node))
            get("//sys/cluster_nodes/{}/orchid/@".format(node))

        # mount small table, preload must fail
        sync_mount_table(SMALL)
        self._wait_for_in_memory_stores_preload_failed(SMALL)
        wait(lambda: get(SMALL + "/@preload_state") == "failed")

        # unmounting large table releases the memory to allow small table to be preloaded
        sync_unmount_table(LARGE)
        self._wait_for_in_memory_stores_preload(SMALL)
        check_lookup(SMALL, *small_data)
        wait(lambda: get(SMALL + "/@preload_state") == "complete")

        # cleanup
        sync_unmount_table(SMALL)

    @authors("lukyan")
    def test_enable_partial_result(self):
        cells = sync_create_cells(2)

        path = "//tmp/t"

        self._create_simple_table(
            path,
            optimize_for="lookup",
            in_memory_mode="uncompressed",
            max_dynamic_store_row_count=10,
            replication_factor=1,
            read_quorum=1,
            write_quorum=1,
        )

        sync_reshard_table(path, [[]] + [[i * 10] for i in range(3)])

        sync_mount_table(path, first_tablet_index=0, last_tablet_index=1, cell_id=cells[0])
        sync_mount_table(path, first_tablet_index=2, last_tablet_index=3, cell_id=cells[1])

        # size of one chunk must be greater than granularity of memory tracker
        def gen_rows(x, y, size=1700, value="x"):
            return [{"key": i, "value": value * size} for i in range(x, y)]

        insert_rows(path, gen_rows(0, 10))
        insert_rows(path, gen_rows(10, 20))
        insert_rows(path, gen_rows(20, 30))

        sync_flush_table(path)

        def preload_succeeded(statistics, table):
            return (
                statistics["preload_completed_store_count"] > 0 and
                statistics["preload_pending_store_count"] == 0 and
                statistics["preload_failed_store_count"] == 0 and
                get("{}/@preload_state".format(table)) == "complete")

        def preload_failed(statistics, table):
            return (
                statistics["preload_completed_store_count"] == 0 and
                statistics["preload_pending_store_count"] == 0 and
                statistics["preload_failed_store_count"] > 0 and
                get("{}/@preload_state".format(table)) == "failed")

        def wait_preload(table, tablet, predicate):
            wait(lambda: predicate(get("{}/@tablets/{}/statistics".format(table, tablet)), table))

        sync_unmount_table(path)
        sync_mount_table(path, first_tablet_index=0, last_tablet_index=1, cell_id=cells[0])
        sync_mount_table(path, first_tablet_index=3, last_tablet_index=3, cell_id=cells[1])
        wait_preload(path, 1, preload_succeeded)
        wait_preload(path, 3, preload_succeeded)
        sync_mount_table(path, first_tablet_index=2, last_tablet_index=2, cell_id=cells[1])
        wait_preload(path, 2, preload_failed)

        keys = [{"key": i} for i in range(0, 30)]

        expected = gen_rows(0, 10) + [None for i in range(10, 20)] + gen_rows(20, 30)

        actual = lookup_rows("//tmp/t", keys, enable_partial_result=True)
        assert_items_equal(actual, expected)


class TestSortedDynamicTablesMemoryLimitRpcProxy(TestSortedDynamicTablesMemoryLimit):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

################################################################################


class TestSortedDynamicTablesMultipleWriteBatches(TestSortedDynamicTablesBase):
    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 10
    }

    @authors("babenko")
    def test_multiple_write_batches(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": "a"} for i in range(100)]
        insert_rows("//tmp/t", rows)
        assert select_rows("* from [//tmp/t]") == rows

################################################################################


class TestSortedDynamicTablesTabletDynamicMemory(TestSortedDynamicTablesBase):
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "resource_limits": {
                "tablet_dynamic_memory": 2 * 2**20,
                "slots": 2,
            },
            "store_flusher": {
                "min_forced_flush_data_size": 1,
            },
            "tablet_manager": {
                "pool_chunk_size": 65 * 2**10,
            },
            "tablet_snapshot_eviction_timeout": 0,
        },
    }

    BUNDLE_OPTIONS = {
        "changelog_replication_factor": 1,
        "changelog_read_quorum": 1,
        "changelog_write_quorum": 1,
    }

    @authors("ifsmirnov")
    @pytest.mark.parametrize("eviction_type", ["remove_cell", "update_weight", "disable_limit"])
    def test_tablet_dynamic_multiple_bundles(self, eviction_type):
        create_tablet_cell_bundle("b1", attributes={"options": self.BUNDLE_OPTIONS})
        create_tablet_cell_bundle("b2", attributes={"options": self.BUNDLE_OPTIONS})
        cell1 = sync_create_cells(1, tablet_cell_bundle="b1")[0]
        cell2 = sync_create_cells(1, tablet_cell_bundle="b2")[0]

        self._create_simple_table("//tmp/t1", tablet_cell_bundle="b1", replication_factor=10,
                                  chunk_writer={"upload_replication_factor": 10})
        sync_mount_table("//tmp/t1")

        self._create_simple_table(
            "//tmp/t2",
            tablet_cell_bundle="b2",
            dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t2")

        _get_row = ({"key": i, "value": str(i) * 100} for i in range(10**9))

        while True:
            try:
                insert_rows("//tmp/t1", [next(_get_row) for i in range(500)])
            except YtError as err:
                if err.contains_code(yt_error_codes.AllWritesDisabled):
                    break

        insert_rows("//tmp/t2", [next(_get_row)])
        with raises_yt_error(yt_error_codes.AllWritesDisabled):
            insert_rows("//tmp/t1", [next(_get_row)])

        remove("//tmp/t2")

        node = ls("//sys/cluster_nodes")[0]
        orchid_prefix = "//sys/cluster_nodes/{}/orchid/tablet_cells".format(node)
        dynamic_options_prefix = "{}/{}/dynamic_options".format(orchid_prefix, cell1)
        pool_weights_prefix = "//sys/cluster_nodes/{}/orchid/tablet_slot_manager/dynamic_memory_pool_weights".format(node)
        b1_pool_weight_path = "{}/b1".format(pool_weights_prefix)

        if eviction_type == "remove_cell":
            remove("#{}".format(cell2))
            node = ls("//sys/cluster_nodes")[0]
            wait(lambda: cell2 not in ls(orchid_prefix))
            wait(lambda: "b2" not in get(pool_weights_prefix))
        elif eviction_type == "update_weight":
            set("//sys/tablet_cell_bundles/b1/@dynamic_options/dynamic_memory_pool_weight", 1000)
            node = ls("//sys/cluster_nodes")[0]
            wait(lambda: get(dynamic_options_prefix + "/dynamic_memory_pool_weight") == 1000)
            wait(lambda: get(b1_pool_weight_path) == 1000)
        elif eviction_type == "disable_limit":
            set("//sys/tablet_cell_bundles/b1/@dynamic_options/enable_tablet_dynamic_memory_limit", False)
            node = ls("//sys/cluster_nodes")[0]
            wait(lambda: not get(dynamic_options_prefix + "/enable_tablet_dynamic_memory_limit"))
        else:
            assert False

        insert_rows("//tmp/t1", [next(_get_row)])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("ratio", [0.3, 0.6])
    @pytest.mark.parametrize("locality", ["bundle", "glocal"])
    def test_forced_rotation_memory_ratio(self, ratio, locality):
        def _wait_func():
            config = get(config_manager + "/applied_config")
            return config.get("tablet_node", {}).get("store_flusher", {}).get("forced_rotation_memory_ratio", None) == ratio

        create_tablet_cell_bundle("b", attributes={"options": self.BUNDLE_OPTIONS})
        if locality == "bundle":
            set("//sys/tablet_cell_bundles/b/@dynamic_options/forced_rotation_memory_ratio", ratio)
        else:
            set("//sys/cluster_nodes/@config", {"%true": {
                "tablet_node": {"store_flusher": {"forced_rotation_memory_ratio": ratio}}
            }})
            node = ls("//sys/cluster_nodes")[0]
            config_manager = "//sys/cluster_nodes/{}/orchid/dynamic_config_manager".format(node)
            wait(_wait_func)

        cell_id = sync_create_cells(1, tablet_cell_bundle="b")[0]

        self._create_simple_table("//tmp/t", tablet_cell_bundle="b", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")

        _get_row = ({"key": i, "value": str(i) * 100} for i in range(10**9))

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        node = get_tablet_leader_address(tablet_id)
        orchid_root = "//sys/cluster_nodes/{}/orchid/tablet_cells/{}/tablets/{}".format(
            node,
            cell_id,
            tablet_id)

        for store_id in ls(orchid_root + "/eden/stores"):
            if get(orchid_root + "/eden/stores/{}/store_state".format(store_id)) == "active_dynamic":
                break
        else:
            assert False

        def _get_active_store():
            for retry in range(5):
                orchid = self._find_tablet_orchid(address, tablet_id)
                for store_id, attributes in orchid["eden"]["stores"].items():
                    if attributes["store_state"] == "active_dynamic":
                        return (store_id, attributes["pool_size"])
            assert False

        while True:
            insert_rows("//tmp/t", [next(_get_row) for i in range(100)])

            # Wait for slot scan.
            time.sleep(0.2)

            store = get(orchid_root + "/eden/stores/{}".format(store_id))
            if store["store_state"] == "passive_dynamic":
                # Store rotated.
                pool_size = store["pool_size"]
                expected = self.DELTA_NODE_CONFIG["tablet_node"]["resource_limits"]["tablet_dynamic_memory"] * ratio
                assert expected - 100000 < pool_size < expected + 100000
                break


class TestSortedDynamicTablesMultipleSlotsPerNode(TestSortedDynamicTablesBase):
    NUM_NODES = 1
    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "resource_limits": {
                "slots": 2,
            },
        },
    }

    @authors("ifsmirnov")
    def test_compaction_after_alter(self):
        cells = sync_create_cells(2)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}]

        self._create_simple_table(
            "//tmp/t",
            schema=schema,
            replication_factor=1,
            enable_dynamic_store_read=False)

        set("//tmp/t/@mount_config/enable_narrow_chunk_view_compaction", False)
        sync_mount_table("//tmp/t", cell_id=cells[0])
        rows = [{"key": 1, "value": "foo"}]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        schema[1:1] = [{"name": "key2", "type": "double", "sort_order": "ascending"}]
        alter_table("//tmp/t", schema=schema)
        chunk_id = get("//tmp/t/@chunk_ids/0")
        set("//tmp/t/@forced_compaction_revision", 1)
        sync_mount_table("//tmp/t", cell_id=cells[1])
        wait(lambda: get("//tmp/t/@chunk_ids/0") != chunk_id)

        rows[0]["key2"] = None
        assert_items_equal(read_table("//tmp/t"), rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)


class TestReshardWithSlicing(TestSortedDynamicTablesBase):
    NUM_TEST_PARTITIONS = 2

    @staticmethod
    def _value_by_optimize_for(optimize_for):
        if optimize_for == 'scan':
            return 'a' * 66000
        return 'value'

    @authors("alexelexa")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_reshard_empty_table(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)
        tablet_count = 3
        with pytest.raises(YtError):
            sync_reshard_table("//tmp/t", tablet_count, enable_slicing=True)
        with pytest.raises(YtError):
            sync_reshard_table(
                "//tmp/t",
                tablet_count,
                enable_slicing=True,
                first_tablet_index=0,
                last_tablet_index=0)

    @authors("alexelexa")
    @pytest.mark.parametrize(
        "first_tablet_index,last_tablet_index",
        [(0, 0), (0, 1), (1, 3), (3, 4), (4, 4), (0, 4), (None, None)])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_reshard_sizes(self, first_tablet_index, last_tablet_index, optimize_for):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)
        set("//tmp/t/@chunk_writer", {"block_size": 1})

        def reshard_and_check(tablet_count, tablet_count_expected, first_tablet_index=None, last_tablet_index=None):
            sync_reshard_table(
                "//tmp/t",
                tablet_count,
                enable_slicing=True,
                first_tablet_index=first_tablet_index,
                last_tablet_index=last_tablet_index)
            assert get("//tmp/t/@tablet_count") == tablet_count_expected

        sync_mount_table("//tmp/t")

        value = self._value_by_optimize_for(optimize_for)
        rows = [{"key": i, "value": value} for i in range(420)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")

        reshard_and_check(3, 3, first_tablet_index=0, last_tablet_index=0)
        reshard_and_check(5, 5)

        tablet_count_expected = 7
        if last_tablet_index is not None and first_tablet_index is not None:
            tablet_count_expected += 5 - (last_tablet_index - first_tablet_index + 1)

        reshard_and_check(7, tablet_count_expected, first_tablet_index=first_tablet_index, last_tablet_index=last_tablet_index)
        sync_compact_table("//tmp/t")

        first_tablet_index = first_tablet_index if first_tablet_index is not None else 0
        last_tablet_index = first_tablet_index + 6

        tablets_info = get("//tmp/t/@tablets")
        total_size = sum(info["statistics"]["uncompressed_data_size"]
                         for info in tablets_info[first_tablet_index : last_tablet_index+1])

        expected_size = total_size // (last_tablet_index - first_tablet_index + 1)
        for index in range(first_tablet_index, last_tablet_index+1):
            assert abs(tablets_info[index]["statistics"]["uncompressed_data_size"] - expected_size) <= max(0.1 * expected_size, 100)

    @authors("alexelexa")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_reshard_and_compaction_small(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)
        set("//tmp/t/@chunk_writer", {"block_size": 1})

        def reshard_and_check(tablet_count, tablet_count_expected, first_tablet_index, last_tablet_index):
            sync_unmount_table("//tmp/t")
            sync_reshard_table(
                "//tmp/t",
                tablet_count,
                enable_slicing=True,
                first_tablet_index=first_tablet_index,
                last_tablet_index=last_tablet_index)
            sync_compact_table("//tmp/t")
            assert get("//tmp/t/@tablet_count") == tablet_count_expected

        sync_mount_table("//tmp/t")

        value = self._value_by_optimize_for(optimize_for)
        rows = [{"key": i, "value": value} for i in range(10)]
        insert_rows("//tmp/t", rows)
        with pytest.raises(YtError):
            reshard_and_check(2 * len(rows), 2 * len(rows), first_tablet_index=None, last_tablet_index=None)
        reshard_and_check(3, 3, first_tablet_index=0, last_tablet_index=0)
        reshard_and_check(5, 5, first_tablet_index=None, last_tablet_index=None)

    @authors("alexelexa")
    @pytest.mark.timeout(120)
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_reshard_and_compaction_big(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)
        set("//tmp/t/@chunk_writer", {"block_size": 100})

        def reshard_and_check(tablet_count, tablet_count_expected, first_tablet_index=None,
                              last_tablet_index=None, with_compaction=True):
            sync_unmount_table("//tmp/t")
            sync_reshard_table(
                "//tmp/t",
                tablet_count,
                enable_slicing=True,
                first_tablet_index=first_tablet_index,
                last_tablet_index=last_tablet_index)
            if with_compaction:
                sync_compact_table("//tmp/t")
            assert get("//tmp/t/@tablet_count") == tablet_count_expected

        def check_tablets_sizes(first_tablet_index, last_tablet_index):
            tablets_info = get("//tmp/t/@tablets")
            total_size = sum(
                info["statistics"]["uncompressed_data_size"]
                for info in tablets_info[first_tablet_index : last_tablet_index+1])

            expected_size = total_size // (last_tablet_index - first_tablet_index + 1)
            for index in range(first_tablet_index, last_tablet_index+1):
                assert abs(tablets_info[index]["statistics"]["uncompressed_data_size"] - expected_size) <= 0.1 * expected_size

        sync_mount_table("//tmp/t")

        value = self._value_by_optimize_for(optimize_for)
        rows = [{"key": i, "value": value} for i in range(1000)]
        insert_rows("//tmp/t", rows)

        base_tablet_count = 5
        reshard_and_check(base_tablet_count, base_tablet_count)
        check_tablets_sizes(0, base_tablet_count - 1)

        current_tablet_count = 5
        first_last_tablet_indices = [(0, 0), (0, 1), (1, 3), (3, 4), (4, 4), (0, 4)]
        for first_tablet_index, last_tablet_index in first_last_tablet_indices:
            if current_tablet_count != base_tablet_count:
                reshard_and_check(base_tablet_count, base_tablet_count)
                check_tablets_sizes(0, base_tablet_count - 1)

            tablet_count = 7
            tablet_count_expected = tablet_count + base_tablet_count - (last_tablet_index - first_tablet_index + 1)

            reshard_and_check(tablet_count, tablet_count_expected, first_tablet_index=first_tablet_index, last_tablet_index=last_tablet_index)
            check_tablets_sizes(first_tablet_index, first_tablet_index + tablet_count - 1)
            current_tablet_count = tablet_count_expected

    @authors("alexelexa")
    @pytest.mark.parametrize("with_alter", [True, False])
    @pytest.mark.parametrize("with_pivots", [True, False])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("with_after_alter_reshard", [True, False])
    def test_reshard_after_alter(self, with_alter, with_pivots, optimize_for, with_after_alter_reshard):
        if with_after_alter_reshard and not with_alter:
            return

        if with_after_alter_reshard and not with_pivots:
            return

        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)
        set("//tmp/t/@chunk_writer", {"block_size": 1})
        if with_pivots:
            sync_reshard_table("//tmp/t", [[], [65]])

        sync_mount_table("//tmp/t")

        value = self._value_by_optimize_for(optimize_for)
        rows = [{"key": i, "value": value} for i in range(99)]

        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        new_schema = [
            {
                "name": "key",
                "type": "int64",
                "sort_order": "ascending",
            },
            {
                "name": "new_key",
                "type": "int64",
                "sort_order": "ascending",
            },
            {"name": "value", "type": "string"},
        ]

        tablet_count = 3
        expected = [[], [32], [65]]

        if with_alter:
            alter_table("//tmp/t", schema=new_schema)

            if with_pivots:
                if with_after_alter_reshard:
                    sync_reshard_table("//tmp/t", [[], [80, 0]])
                else:
                    expected[-1] = expected[-1] + [yson.YsonEntity()]

        sync_reshard_table("//tmp/t", tablet_count, enable_slicing=True)
        assert get("//tmp/t/@tablet_count") == tablet_count
        assert self._get_pivot_keys("//tmp/t") == expected

    @authors("alexelexa")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_reshard_multi(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)
        set("//tmp/t/@chunk_writer", {"block_size": 5})
        set("//tmp/t/@enable_compaction_and_partitioning", False)

        def reshard_and_check(tablet_count):
            sync_unmount_table("//tmp/t")
            sync_reshard_table("//tmp/t", tablet_count, enable_slicing=True)
            assert get("//tmp/t/@tablet_count") == tablet_count

        sync_mount_table("//tmp/t")

        value = self._value_by_optimize_for(optimize_for)
        rows = [{"key": i, "value": value} for i in range(150)]
        insert_rows("//tmp/t", rows)

        sync_flush_table("//tmp/t")

        rows = [{"key": i, "value": value} for i in range(150, 300)]
        insert_rows("//tmp/t", rows)

        tablet_count = 4
        reshard_and_check(tablet_count)
        reshard_and_check(tablet_count)

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        sync_compact_table("//tmp/t")

        reshard_and_check(tablet_count)

    @authors("alexelexa")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_too_small_chunk_views(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for)
        set("//tmp/t/@chunk_writer", {"block_size": 1000})
        set("//tmp/t/@enable_compaction_and_partitioning", False)

        sync_mount_table("//tmp/t")

        value = self._value_by_optimize_for(optimize_for)
        rows = [{"key": i, "value": value} for i in range(150)]
        insert_rows("//tmp/t", rows)

        sync_flush_table("//tmp/t")

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[]] + [[x] for x in range(1, 50)])
        assert get("//tmp/t/@tablet_count") == 50

        sync_mount_table("//tmp/t")

        sync_unmount_table("//tmp/t")
        with pytest.raises(YtError):
            sync_reshard_table("//tmp/t", 4, enable_slicing=True, first_tablet_index=0, last_tablet_index=1)

    @authors("alexelexa")
    def test_replicated_table_reshard(self):
        sync_create_cells(1)
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        create("replicated_table", "//tmp/t", attributes={
            "dynamic": True,
            "schema": schema,
            "replication_factor": 1})

        with pytest.raises(YtError):
            sync_reshard_table("//tmp/t", 4, enable_slicing=True)


##################################################################


class TestSortedDynamicTablesChunkFormat(TestSortedDynamicTablesBase):
    @authors("babenko")
    def test_validate_chunk_format_on_mount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        assert get("//tmp/t/@optimize_for") == "lookup"

        set("//tmp/t/@chunk_format", "table_unversioned_schemaful")
        with raises_yt_error("is not a valid versioned chunk format"):
            mount_table("//tmp/t")

        set("//tmp/t/@chunk_format", "table_versioned_columnar")
        with raises_yt_error('is not a valid "lookup" chunk format'):
            mount_table("//tmp/t")

    @authors("babenko")
    def test_indexed_chunk_format(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", chunk_format="table_versioned_indexed", compression_codec="none")
        assert get("//tmp/t/@optimize_for") == "lookup"

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "test"}])

        sync_unmount_table("//tmp/t")
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get(f"#{chunk_id}/@chunk_format") == "table_versioned_indexed"

    @authors("akozhikhov")
    @pytest.mark.parametrize(
        "chunk_format",
        ["table_versioned_simple", "table_versioned_slim", "table_versioned_columnar", "table_versioned_indexed"])
    def test_major_timestamp_in_compaction(self, chunk_format):
        sync_create_cells(1)

        self._create_simple_table(
            "//tmp/t",
            chunk_format=chunk_format)
        if chunk_format == "table_versioned_indexed":
            set("//tmp/t/@compression_codec", "none")

        set("//tmp/t/@enable_compaction_and_partitioning", False)
        set("//tmp/t/@enable_lsm_verbose_logging", True)
        sync_mount_table("//tmp/t")

        # Make it the largest chunk, so it will be skipped from compaction after sorting by size.
        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in range(1000)])
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 0}, {"key": 1}]) == [{"key": 0, "value": "0"}, {"key": 1, "value": "1"}]
        first_chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(first_chunk_ids) == 1

        insert_rows("//tmp/t", [{"key": 0, "value": "00"}, {"key": 1, "value": "11"}])
        sync_flush_table("//tmp/t")
        delete_rows("//tmp/t", [{"key": 0}])
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 0}, {"key": 1}]) == [{"key": 1, "value": "11"}]

        set("//tmp/t/@min_compaction_store_count", 2)
        # The first chunk will be the only skipped chunk as it is the largest one.
        set("//tmp/t/@compaction_data_size_base", get("#{}/@compressed_data_size".format(first_chunk_ids[0])) - 1)
        # Otherwise retention timestamp is capped.
        set("//tmp/t/@min_data_ttl", 0)

        set("//tmp/t/@enable_compaction_and_partitioning", True)

        chunk_ids_before_compaction = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids_before_compaction) == 3

        remount_table("//tmp/t")

        def _compaction_finished():
            chunk_ids = get("//tmp/t/@chunk_ids")
            if len(chunk_ids) != 2:
                return False
            return builtins.set(chunk_ids).intersection(builtins.set(chunk_ids_before_compaction)) == \
                builtins.set(first_chunk_ids)

        wait(lambda: _compaction_finished())

        assert lookup_rows("//tmp/t", [{"key": 0}, {"key": 1}]) == [{"key": 1, "value": "11"}]
