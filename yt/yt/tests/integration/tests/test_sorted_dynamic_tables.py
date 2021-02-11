import pytest
import __builtin__

from test_dynamic_tables import DynamicTablesBase

from yt_helpers import Metric

from yt_env_setup import wait, parametrize_external, Restarter, NODES_SERVICE
from yt_commands import *
from yt.yson import YsonEntity, loads

from flaky import flaky

from time import sleep
from random import randint, choice, sample
from string import ascii_lowercase

import random

from yt.environment.helpers import assert_items_equal

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
        tablets = get(table + "/@tablets")
        if last_tablet_index is not None:
            tablets = tablets[:last_tablet_index + 1]
        if first_tablet_index is not None:
            tablets = tablets[first_tablet_index:]

        for tablet in tablets:
            tablet_id = tablet["tablet_id"]
            def all_preloaded(address):
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
            for address in get_tablet_follower_addresses(tablet_id) + [get_tablet_leader_address(tablet_id)]:
                wait(lambda: all_preloaded(address))

    def _wait_for_in_memory_stores_preload_failed(self, table):
        tablets = get(table + "/@tablets")
        for tablet in tablets:
            tablet_id = tablet["tablet_id"]
            orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id), tablet_id)
            if not orchid:
                return False
            for store in orchid["eden"]["stores"].itervalues():
                if store["store_state"] == "persistent" and store["preload_state"] == "failed":
                    return True
            for partition in orchid["partitions"]:
                for store in partition["stores"].itervalues():
                    if store["preload_state"] == "failed":
                        return True
            return False

    def _reshard_with_retries(self, path, pivots):
        resharded = False
        for i in xrange(4):
            try:
                sync_unmount_table(path)
                sync_reshard_table(path, pivots)
                resharded = True
            except:
                pass
            sync_mount_table(path)
            if resharded:
                break
            sleep(5)
        assert resharded

    def _create_partitions(self, partition_count, do_overlap=False):
        assert partition_count > 1
        partition_count += 1 - int(do_overlap)

        def _force_compact_tablet(tablet_index):
            set("//tmp/t/@forced_compaction_revision", 1)

            chunk_list_id = get("//tmp/t/@chunk_list_id")
            tablet_chunk_list_id = get("#{0}/@child_ids/{1}".format(chunk_list_id, tablet_index))
            tablet_chunk_ids = __builtin__.set(get("#{}/@child_ids".format(tablet_chunk_list_id)))
            assert len(tablet_chunk_ids) > 0
            for id in tablet_chunk_ids:
                type = get("#{}/@type".format(id))
                assert type == "chunk" or type == "chunk_view"

            sync_mount_table("//tmp/t", first_tablet_index=tablet_index, last_tablet_index=tablet_index)

            def _check():
                new_tablet_chunk_ids = __builtin__.set(get("#{}/@child_ids".format(tablet_chunk_list_id)))
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


##################################################################

class TestSortedDynamicTables(TestSortedDynamicTablesBase):
    NUM_TEST_PARTITIONS = 3

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

    @authors("lukyan")
    def test_lookup_cache(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", lookup_cache_rows_per_tablet=50)

        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(0, 1000, 2)]
        insert_rows("//tmp/t", rows)

        sync_flush_table("//tmp/t")

        for step in xrange(1, 5):
            expected = [{"key": i, "value": str(i)} for i in xrange(100, 200, 2 * step)]
            actual = lookup_rows("//tmp/t", [{"key": i} for i in xrange(100, 200, 2 * step)], use_lookup_cache=True)
            assert_items_equal(actual, expected)

        # Lookup non-existent key without polluting cache.
        lookup_rows("//tmp/t", [{"key": 1}])

        path = "//tmp/t/@tablets/0/performance_counters/static_chunk_row_lookup_count"
        wait(lambda: get(path) > 50)
        assert get(path) == 51

        # Modify some rows.
        rows = [{"key": i, "value": str(i + 1)} for i in xrange(100, 200, 2)]
        insert_rows("//tmp/t", rows)

        # Check lookup result.
        actual = lookup_rows("//tmp/t", [{"key": i} for i in xrange(100, 200, 2)], use_lookup_cache=True)
        assert_items_equal(actual, rows)

        # Flush table.
        sync_flush_table("//tmp/t")

        # And check that result after flush is equal.
        actual = lookup_rows("//tmp/t", [{"key": i} for i in xrange(100, 200, 2)], use_lookup_cache=True)
        assert_items_equal(actual, rows)

        # Lookup non existent key adds two lookups (in two chunks).
        lookup_rows("//tmp/t", [{"key": 1}], use_lookup_cache=True)

        wait(lambda: get(path) > 51)
        assert get(path) == 53

    @authors("lukyan")
    def test_lookup_cache_flush(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", lookup_cache_rows_per_tablet=50)

        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in xrange(0, 300, 2)]
        insert_rows("//tmp/t", rows)

        expected = [{"key": i, "value": str(i)} for i in xrange(100, 200, 2)]
        actual = lookup_rows("//tmp/t", [{"key": i} for i in xrange(100, 200, 2)], use_lookup_cache=True)
        assert_items_equal(actual, expected)

        # Insert rows again to increase last store timestamp.
        rows = [{"key": i, "value": str(2 * i)} for i in xrange(0, 300, 4)]
        insert_rows("//tmp/t", rows)

        sync_flush_table("//tmp/t")

        # Lookup again. Check that rows are in cache.
        expected = [{"key": i, "value": str(2 * i if i % 4 == 0 else i)} for i in xrange(100, 200, 2)]
        actual = lookup_rows("//tmp/t", [{"key": i} for i in xrange(100, 200, 2)], use_lookup_cache=True)
        assert_items_equal(actual, expected)

        # Lookup non-existent key without polluting cache.
        lookup_rows("//tmp/t", [{"key": 1}])

        path = "//tmp/t/@tablets/0/performance_counters/static_chunk_row_lookup_count"
        wait(lambda: get(path) > 0)
        assert get(path) == 1

    @authors("lukyan")
    def test_lookup_cache_stress(self):
        sync_create_cells(1)

        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                {"name": "v", "type": "int64"},
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"},
                {"name": "s", "type": "string"},
                {"name": "t", "type": "string"}],
            lookup_cache_rows_per_tablet=100)

        sync_mount_table("//tmp/t")

        count = 500

        # Decorate to simplify grep.
        def decorate_key(key):
            return 12300000 + key

        for wave in xrange(1, 30):
            rows = [{
                "k": decorate_key(k),
                "v": wave * count + k,
                choice(["a", "b", "c"]): randint(1, 10000),
                choice(["s", "t"]): str(randint(1, 10000))}
                for k in sample(range(1, count), 200)]
            insert_rows("//tmp/t", rows, update=True)
            print_debug("Insert rows ", rows)

            keys = [{"k": decorate_key(k)} for k in sample(range(1, count), 100)]
            delete_rows("//tmp/t", keys)
            print_debug("Delete rows ", keys)

            for i in xrange(1, 10):
                keys = [{"k": decorate_key(k)} for k in sample(range(1, count), 10)]

                ts = generate_timestamp()
                no_cache = lookup_rows("//tmp/t", keys, timestamp=ts)
                cache = lookup_rows("//tmp/t", keys, use_lookup_cache=True, timestamp=ts)
                assert no_cache == cache

            sync_flush_table("//tmp/t")

        tablet_profiling = self._get_table_profiling("//tmp/t")
        assert tablet_profiling.get_counter("lookup/cache_hits") > 0
        assert tablet_profiling.get_counter("lookup/cache_misses") > 0

    @authors("lukyan")
    def test_lookup_cache_stress2(self):
        sync_create_cells(1)

        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                {"name": "v", "type": "int64"},
                {"name": "i", "type": "int64"},
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "int64"},
                {"name": "c", "type": "int64"},
                {"name": "s", "type": "string"},
                {"name": "t", "type": "string"},
                {"name": "md5", "type": "string"}],
            lookup_cache_rows_per_tablet=100)

        sync_mount_table("//tmp/t")

        count = 500

        verify_map = {}
        revision_map = {}

        def get_checksum(row):
            row_data = " ".join(yson.dumps(row.get(col, yson.YsonEntity())) for col in ["v", "i", "a", "b", "c", "s", "t"])
            return row_data

        # Decorate to simplify grep.
        def decorate_key(key):
            return 12300000 + key

        for wave in xrange(1, 30):
            rows = [{
                "k": decorate_key(k),
                "v": k,
                "i": wave,
                choice(["a", "b", "c"]): randint(1, 10000),
                choice(["s", "t"]): str(randint(1, 10000))}
                for k in sample(range(1, count), 200)]

            for row in rows:
                key = row["k"]
                item = verify_map.get(key, {})
                item.update(row)
                row["md5"] = get_checksum(item)
                verify_map[key] = item

            print_debug("Insert rows ", rows)
            insert_rows("//tmp/t", rows, update=True)

            keys = [{"k": decorate_key(k)} for k in sample(range(1, count), 100)]
            for key in keys:
                if key["k"] in verify_map:
                    del verify_map[key["k"]]
            print_debug("Delete rows ", keys)
            delete_rows("//tmp/t", keys)

            for i in xrange(1, 10):
                keys = [{"k": decorate_key(k)} for k in sample(range(1, count), 10)]
                result = lookup_rows("//tmp/t", keys, use_lookup_cache=True)

                for row in result:
                    assert row["k"] == decorate_key(row["v"])
                    assert get_checksum(row) == row["md5"]
                    revision = row["i"]
                    assert revision >= revision_map.get(row["k"], 0)
                    revision_map[row["k"]] = revision

            sync_flush_table("//tmp/t")

        tablet_profiling = self._get_table_profiling("//tmp/t")
        assert tablet_profiling.get_counter("lookup/cache_hits") > 0
        assert tablet_profiling.get_counter("lookup/cache_misses") > 0

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory(self, mode, optimize_for):
        cell_id = sync_create_cells(1)[0]
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)

        set("//tmp/t/@in_memory_mode", mode)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            set("//tmp/t/@in_memory_mode", "none")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

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
            assert get("//tmp/t/@preload_state") == "complete"

        # Check preload after mount.
        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert lookup_rows("//tmp/t", keys) == rows

        # Check preload after flush.
        rows = [{"key": i, "value": str(i + 1)} for i in xrange(10)]
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
            return (
                cell_statistics["preload_completed_store_count"] ==
                    tablet_statistics["preload_completed_store_count"] and
                cell_statistics["preload_pending_store_count"] == 0
            )
        wait(_check)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("enable_lookup_hash_table", [True, False])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_preload_block_range(self, enable_lookup_hash_table, optimize_for):
        create_tablet_cell_bundle("b", attributes={"options": {"peer_count": 3}})
        sync_create_cells(1, tablet_cell_bundle="b")
        set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 2**30)
        self._create_simple_table("//tmp/t", tablet_cell_bundle="b")
        set("//tmp/t/@chunk_writer", {"block_size": 1024})
        set("//tmp/t/@in_memory_mode", "uncompressed")
        set("//tmp/t/@enable_lookup_hash_table", enable_lookup_hash_table)
        set("//tmp/t/@optimize_for", optimize_for)
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
    def test_lookup_hash_table(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        set("//tmp/t/@in_memory_mode", "uncompressed")
        set("//tmp/t/@enable_lookup_hash_table", True)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        sync_mount_table("//tmp/t")

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

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        # ensure data is preloaded
        self._wait_for_in_memory_stores_preload("//tmp/t")

        # check that stores are rotated on-demand
        insert_rows("//tmp/t", _rows(10, 20))
        # ensure slot gets scanned
        sleep(3)
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
        rows1 = [{"key": i, "value": str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}])
        sync_mount_table("//tmp/t")

        rows2 = [{"key": i, "key2": 0, "value": str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows2)

        assert lookup_rows("//tmp/t", [{"key": 77}]) == [{"key": 77, "key2": YsonEntity(), "value": "77"}]
        assert lookup_rows("//tmp/t", [{"key": 77, "key2": 1}]) == []
        assert lookup_rows("//tmp/t", [{"key": 77, "key2": 0}]) == [{"key": 77, "key2": 0, "value": "77"}]
        assert select_rows("sum(1) as s from [//tmp/t] where is_null(key2) group by 0") == [{"s": 100}]

    @authors("babenko")
    def test_atomicity_mode_should_match(self):
        def do(a1, a2):
            sync_create_cells(1)
            self._create_simple_table("//tmp/t", atomicity=a1)
            sync_mount_table("//tmp/t")
            rows = [{"key": i, "value": str(i)} for i in xrange(100)]
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

        rows = [{"key": i, "value": str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows, atomicity=atomicity)

        build_snapshot(cell_id=cell_id)

        snapshots = ls("//sys/tablet_cells/" + cell_id + "/snapshots")
        assert len(snapshots) == 1

        with Restarter(self.Env, NODES_SERVICE):
            # Wait to make sure all leases have expired
            time.sleep(3.0)

        wait_for_cells()

        # Wait to make sure all tablets are up
        time.sleep(3.0)

        keys = [{"key": i} for i in xrange(100)]
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_stress_tablet_readers(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

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
            print_debug("Write rows ", rows)
            insert_rows("//tmp/t", rows)

            verify()

            pivots = ([[]] + [[x] for x in xrange(0, items, items / wave)]) if wave % 2 == 0 else [[]]
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
    def test_stress_chunk_view(self, optimize_for, in_memory_mode):
        random.seed(98765)

        sync_create_cells(1)

        key_range=100
        num_writes_per_iteration=50
        num_deletes_per_iteration=10
        num_write_iterations=3
        num_lookup_iterations=30

        def random_row():
            return {"key": randint(1, key_range), "value": "".join(choice(ascii_lowercase) for i in range(5))}

        # Prepare both tables.
        self._create_simple_table("//tmp/t", optimize_for=optimize_for, in_memory_mode=in_memory_mode)
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
            pivots = [[]] + [[i] for i in sorted(sample(range(1, key_range+1), num_pivots))]
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
            keys = sorted(sample(range(1, key_range+1), ranges_count * 2))
            query = "* from [{}] where " + " or ".join(
                    "({} <= key and key < {})".format(l, r)
                    for l, r
                    in zip(keys[::2], keys[1::2]))
            expected = list(select_rows(query.format("//tmp/correct")))
            actual = list(select_rows(query.format("//tmp/t")))
            assert sorted(expected) == sorted(actual)

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
        set("//sys/cluster_nodes/{}/@banned".format(peer), True)

        wait_for_cells([cell_id])

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

        sleep(1.0)
        for delay in xrange(0, 10):
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
        expect_rows = rows + [None]
        insert_rows("//tmp/t", rows)
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert len(actual) == 2
        assert_items_equal(rows[0], actual[0])
        assert actual[1] == YsonEntity()

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
                statistics["disk_space_per_medium"]["default"] ==
                    get("//tmp/t/@resource_usage/disk_space_per_medium/default")
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
        sleep(1)
        sync_mount_table("//tmp/t")
        # Wait for master to receive node statistics.
        sleep(1)
        t2 = get("//tmp/t/@unflushed_timestamp")
        assert t2 > t1
        assert get("//tmp/t/@retained_timestamp") == MinTimestamp

        rows = [{"key": i, "value": str(i)} for i in xrange(2)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")
        t3 = get("//tmp/t/@retained_timestamp")
        t4 = get("//tmp/t/@unflushed_timestamp")
        assert t3 > MinTimestamp
        assert t2 < t4
        assert t3 < t4

        sleep(1)
        t11 = get("//tmp/t/@unflushed_timestamp")
        assert t4 < t11

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)
        t5 = get("//tmp/t/@retained_timestamp", tx=tx)
        t6 = get("//tmp/t/@unflushed_timestamp", tx=tx)
        sleep(1)
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")
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

        sync_freeze_table("//tmp/t")
        sleep(1)
        t14 = get("//tmp/t/@unflushed_timestamp")
        assert t14 > t8

    @authors("savrus", "levysotsky")
    def test_expired_timestamp(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@min_data_ttl", 0)

        ts = generate_timestamp()
        sleep(1)
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

        insert_rows("//tmp/t", [{"key": i, "value": "A"*1024} for i in xrange(10)])
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
    @flaky(max_runs=5)
    def test_chunk_profiling(self):
        path = "//tmp/t"
        sync_create_cells(1)
        self._create_simple_table(path)
        sync_mount_table(path)

        filter = {"table_path": path, "method": "compaction"}

        disk_space_metric = Metric.at_tablet_node(path, "chunk_writer/disk_space", with_tags=filter, aggr_method="max")
        data_weight_metric = Metric.at_tablet_node(path, "chunk_writer/data_weight", with_tags=filter)
        data_bytes_metric = Metric.at_tablet_node(
            path, "chunk_reader_statistics/data_bytes_read_from_disk", with_tags=filter)

        insert_rows(path, [{"key": 0, "value": "test"}])
        sync_compact_table(path)

        assert disk_space_metric.update().get(verbose=True) > 0
        assert data_weight_metric.update().get(verbose=True) > 0
        assert data_bytes_metric.update().get(verbose=True) > 0

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
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            backing_store_retention_time=10000,
            max_dynamic_store_row_count=5,
            dynamic_store_auto_flush_period=YsonEntity())
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in range(5)])
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 1)

        address = get_tablet_leader_address(tablet_id)
        def _has_backing_store():
            orchid = self._find_tablet_orchid(address, tablet_id)
            for store in orchid["partitions"][0]["stores"].values():
                if "backing_store" in store:
                    return True
            return False

        assert _has_backing_store()
        wait(lambda: not _has_backing_store())

        set("//tmp/t/@backing_store_retention_time", 1000000000)
        remount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in range(5)])
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 2)

        assert _has_backing_store()
        set("//sys/tablet_cell_bundles/default/@dynamic_options/max_backing_store_memory_ratio", 0.00000001)
        wait(lambda: not _has_backing_store())


class TestSortedDynamicTablesMulticell(TestSortedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSortedDynamicTablesPortal(TestSortedDynamicTablesMulticell):
    ENABLE_TMP_PORTAL = True


class TestSortedDynamicTablesRpcProxy(TestSortedDynamicTables):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

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
        expected = [{"key1": 1, "key2": 1, "value1": "2", "value2": YsonEntity()}]
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
                "tablet_static_memory": 20000
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
            rows = [{"key": i, "value": generate_string(length)} for i in xrange(10)]
            keys = [{"key": row["key"]} for row in rows]
            insert_rows(table, rows)
            return keys, rows

        sync_create_cells(1)

        table_create(LARGE)
        table_create(SMALL)

        # create large table over memory limit
        large_data = table_insert_rows(10000, LARGE)
        sync_flush_table(LARGE)
        sync_unmount_table(LARGE)

        # create small table for final preload checking
        small_data = table_insert_rows(1000, SMALL)
        sync_flush_table(SMALL)
        sync_unmount_table(SMALL)

        # mount large table to trigger memory limit
        sync_mount_table(LARGE)
        self._wait_for_in_memory_stores_preload(LARGE)
        check_lookup(LARGE, *large_data)

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

        sync_reshard_table(path, [[]] + [[i * 10] for i in xrange(3)])

        sync_mount_table(path, first_tablet_index=0, last_tablet_index=1, cell_id=cells[0])
        sync_mount_table(path, first_tablet_index=2, last_tablet_index=3, cell_id=cells[1])

        def gen_rows(x, y, size=1500, value="x"):
            return [{"key": i, "value": value * size} for i in xrange(x, y)]

        insert_rows(path, gen_rows(0, 10))
        insert_rows(path, gen_rows(10, 20))
        insert_rows(path, gen_rows(20, 30))

        sync_flush_table(path)

        def is_preloaded(statistics):
            return (
                statistics["preload_completed_store_count"] > 0 and
                statistics["preload_pending_store_count"] == 0 and
                statistics["preload_failed_store_count"] == 0)

        def wait_preload(table, tablet):
            wait(lambda: is_preloaded(get("{}/@tablets/{}/statistics".format(table, tablet))))

        sync_unmount_table(path)
        sync_mount_table(path, first_tablet_index=0, last_tablet_index=1, cell_id=cells[0])
        sync_mount_table(path, first_tablet_index=3, last_tablet_index=3, cell_id=cells[1])
        wait_preload(path, 1)
        wait_preload(path, 3)
        sync_mount_table(path, first_tablet_index=2, last_tablet_index=2, cell_id=cells[1])

        keys = [{"key": i} for i in xrange(0, 30)]

        expected = gen_rows(0, 10) + [None for i in xrange(10, 20)] + gen_rows(20, 30)

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

        rows = [{"key": i, "value": "a"} for i in xrange(100)]
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

        self._create_simple_table("//tmp/t2", tablet_cell_bundle="b2", dynamic_store_auto_flush_period=YsonEntity())
        sync_mount_table("//tmp/t2")

        _get_row = ({"key": i, "value": str(i) * 100} for i in xrange(10**9))

        while True:
            try:
                insert_rows("//tmp/t1", [_get_row.next() for i in range(500)])
            except YtError as err:
                if err.contains_code(AllWritesDisabled):
                    break

        insert_rows("//tmp/t2", [_get_row.next()])
        with raises_yt_error(AllWritesDisabled):
            insert_rows("//tmp/t1", [_get_row.next()])

        remove("//tmp/t2")

        node = ls("//sys/cluster_nodes")[0]
        orchid_prefix = "//sys/cluster_nodes/{}/orchid/tablet_cells".format(node)
        dynamic_options_prefix = "{}/{}/dynamic_options".format(orchid_prefix, cell1)

        if eviction_type == "remove_cell":
            remove("#{}".format(cell2))
            node = ls("//sys/cluster_nodes")[0]
            wait(lambda: cell2 not in ls(orchid_prefix))
        elif eviction_type == "update_weight":
            set("//sys/tablet_cell_bundles/b1/@dynamic_options/dynamic_memory_pool_weight", 1000)
            node = ls("//sys/cluster_nodes")[0]
            wait(lambda: get(dynamic_options_prefix + "/dynamic_memory_pool_weight") == 1000)
        elif eviction_type == "disable_limit":
            set("//sys/tablet_cell_bundles/b1/@dynamic_options/enable_tablet_dynamic_memory_limit", False)
            node = ls("//sys/cluster_nodes")[0]
            wait(lambda: not get(dynamic_options_prefix + "/enable_tablet_dynamic_memory_limit"))
        else:
            assert False

        insert_rows("//tmp/t1", [_get_row.next()])

    @authors("ifsmirnov")
    @pytest.mark.parametrize("ratio", [0.3, 0.6])
    @pytest.mark.parametrize("locality", ["bundle", "glocal"])
    def test_forced_rotation_memory_ratio(self, ratio, locality):
        create_tablet_cell_bundle("b", attributes={"options": self.BUNDLE_OPTIONS})
        if locality == "bundle":
            set("//sys/tablet_cell_bundles/b/@dynamic_options/forced_rotation_memory_ratio", ratio)
        else:
            set("//sys/cluster_nodes/@config", {"%true": {
                "tablet_node": {"store_flusher": {"forced_rotation_memory_ratio": ratio}}
            }})
            node = ls("//sys/cluster_nodes")[0]
            config_manager = "//sys/cluster_nodes/{}/orchid/dynamic_config_manager".format(node)
            def _wait_func():
                config = get(config_manager + "/applied_config")
                return config.get("tablet_node", {}).get("store_flusher", {}).get("forced_rotation_memory_ratio", None) == ratio
            wait(_wait_func)

        cell_id = sync_create_cells(1, tablet_cell_bundle="b")[0]

        self._create_simple_table("//tmp/t", tablet_cell_bundle="b", dynamic_store_auto_flush_period=YsonEntity())
        sync_mount_table("//tmp/t")

        _get_row = ({"key": i, "value": str(i) * 100} for i in xrange(10**9))

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        node = get_tablet_leader_address(tablet_id)
        orchid_root = "//sys/cluster_nodes/{}/orchid/tablet_cells/{}/tablets/{}".format(
            node,
            cell_id,
            tablet_id)

        for store_id in ls(orchid_root + "/eden/stores"):
            if get(orchid_root + "/eden/stores/{}/store_state".format(store_id)) == "active_dynamic":
                original_store_id = store_id
                break
        else:
            assert False

        def _get_active_store():
            for retry in range(5):
                orchid = self._find_tablet_orchid(address, tablet_id)
                for store_id, attributes in orchid["eden"]["stores"].iteritems():
                    if attributes["store_state"] == "active_dynamic":
                        return (store_id, attributes["pool_size"])
            assert False

        while True:
            insert_rows("//tmp/t", [_get_row.next() for i in range(100)])

            # Wait for slot scan.
            sleep(0.2)

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
