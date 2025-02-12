from yt_dynamic_tables_base import DynamicTablesBase

from yt_env_setup import Restarter, NODES_SERVICE

from yt_commands import (
    authors, print_debug, wait, ls, get, set, copy, concatenate,
    start_transaction, abort_transaction, remount_table,
    lock, insert_rows, select_rows, delete_rows, trim_rows, alter_table, read_table, write_table,
    mount_table, reshard_table, generate_timestamp, wait_for_cells,
    get_tablet_leader_address, sync_create_cells, sync_mount_table, sync_unmount_table, sync_freeze_table,
    sync_unfreeze_table, sync_reshard_table, sync_flush_table,
    get_singular_chunk_id, create_dynamic_table, build_snapshot, generate_uuid,
    raises_yt_error, remove)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

import pytest

import time
import random

##################################################################


class TestOrderedDynamicTablesBase(DynamicTablesBase):
    def _create_simple_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "a", "type": "int64"},
                        {"name": "b", "type": "double"},
                        {"name": "c", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    def _wait_for_in_memory_stores_preload(self, table):
        for tablet in get(table + "/@tablets"):
            tablet_id = tablet["tablet_id"]
            address = get_tablet_leader_address(tablet_id)

            def all_preloaded():
                tablet_data = self._find_tablet_orchid(address, tablet_id)
                return all(
                    s["preload_state"] == "complete"
                    for s in tablet_data["stores"].values()
                    if s["store_state"] == "persistent"
                )

            wait(lambda: all_preloaded())

    def _verify_cumulative_statistics_match_statistics(self, chunk_list_id):
        attrs = get(
            "#{}/@".format(chunk_list_id),
            attributes=["statistics", "cumulative_statistics"],
        )
        statistics = attrs["statistics"]
        cumulative_statistics = attrs["cumulative_statistics"]
        assert len(cumulative_statistics) > 0
        assert cumulative_statistics[-1]["chunk_count"] == statistics["chunk_count"]
        assert cumulative_statistics[-1]["row_count"] == statistics["logical_row_count"]
        # Intentionally not compared because it is not "logical" and contains garbage after trim.
        # assert cumulative_statistics[-1]["data_size"] == statistics["uncompressed_data_size"]

    def _verify_chunk_tree_statistics(self, table):
        chunk_list_id = get(table + "/@chunk_list_id")
        statistics = get("#{0}/@statistics".format(chunk_list_id))
        tablet_chunk_lists = get("#{0}/@child_ids".format(chunk_list_id))
        tablet_statistics = [get("#{0}/@statistics".format(c)) for c in tablet_chunk_lists]
        assert statistics["row_count"] == sum([c["row_count"] for c in tablet_statistics])
        assert statistics["chunk_count"] == sum([c["chunk_count"] for c in tablet_statistics])
        assert statistics["logical_row_count"] == sum([c["logical_row_count"] for c in tablet_statistics])

        self._verify_cumulative_statistics_match_statistics(chunk_list_id)
        for tablet_chunk_list in tablet_chunk_lists:
            self._verify_cumulative_statistics_match_statistics(tablet_chunk_list)

    def _get_single_tablet_cumulative_statistics(self, table):
        root_chunk_list_id = get(f"{table}/@chunk_list_id")
        tablet_chunk_list_id = get(f"#{root_chunk_list_id}/@child_ids/0")
        return get(f"#{tablet_chunk_list_id}/@cumulative_statistics")


##################################################################


class TestOrderedDynamicTables(TestOrderedDynamicTablesBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 4

    @authors("babenko")
    def test_mount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert "pivot_key" not in tablet
        tablet_id = tablet["tablet_id"]
        cell_id = tablet["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]

    @authors("babenko", "levysotsky")
    def test_unmount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        sync_mount_table("//tmp/t")

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        sync_mount_table("//tmp/t")
        sync_unmount_table("//tmp/t")

    @authors("savrus")
    def test_access_to_frozen(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        rows = [{"a": 1}]
        insert_rows("//tmp/t", rows)
        sync_freeze_table("//tmp/t")
        assert select_rows("a from [//tmp/t]") == rows
        with pytest.raises(YtError):
            insert_rows("//tmp/t", rows)

    @authors("gridem")
    def test_ordered_tablet_node_profiling(self):
        sync_create_cells(1)

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        sync_mount_table(table_path)

        tablet_profiling = self._get_table_profiling(table_path)

        def get_all_counters(count_name):
            return (
                tablet_profiling.get_counter("select/" + count_name),
                tablet_profiling.get_counter("write/" + count_name),
                tablet_profiling.get_counter("commit/" + count_name),
            )

        assert get_all_counters("row_count") == (0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0)
        assert tablet_profiling.get_counter("select/cpu_time") == 0

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(10)]
        insert_rows(table_path, rows)

        wait(lambda: get_all_counters("row_count") == (0, 10, 10))
        wait(lambda: get_all_counters("data_weight") == (0, 250, 250))
        assert tablet_profiling.get_counter("select/cpu_time") == 0

        select_rows("* from [{}]".format(table_path))

        wait(lambda: get_all_counters("row_count") == (10, 10, 10))
        wait(lambda: get_all_counters("data_weight") == (410, 250, 250))
        wait(lambda: tablet_profiling.get_counter("select/cpu_time") > 0)

    @authors("babenko", "levysotsky")
    def test_insert(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(0, 100)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

    @authors("babenko")
    def test_flush(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(0, 100)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")

        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@row_count") == 100

    @authors("babenko")
    def test_insert_with_explicit_tablet_index(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 10)
        sync_mount_table("//tmp/t")

        for i in range(10):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])

        for i in range(10):
            assert select_rows("a from [//tmp/t] where [$tablet_index] = " + str(i)) == [{"a": i}]

        # Check range inference YT-12099
        assert select_rows("a from [//tmp/t] where [$tablet_index] >= -1 limit 10") == [{"a": i} for i in range(10)]
        assert select_rows("a from [//tmp/t] where [$tablet_index] >= null limit 10") == [{"a": i} for i in range(10)]

    @authors("babenko")
    @pytest.mark.parametrize("dynamic", [True, False])
    def test_select_from_single_tablet(self, dynamic):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        write_rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(100)]
        query_rows = [
            {
                "$tablet_index": 0,
                "$row_index": i,
                "a": i,
                "b": i * 0.5,
                "c": "payload" + str(i),
            }
            for i in range(100)
        ]
        insert_rows("//tmp/t", write_rows)

        if not dynamic:
            sync_unmount_table("//tmp/t")
            assert get("//tmp/t/@chunk_count") == 1
            sync_mount_table("//tmp/t")

        assert select_rows("* from [//tmp/t]") == query_rows
        assert select_rows("[$row_index], a from [//tmp/t]") == [
            {"$row_index": row["$row_index"], "a": row["a"]} for row in query_rows
        ]
        assert select_rows("c, b from [//tmp/t]") == [{"b": i * 0.5, "c": "payload" + str(i)} for i in range(100)]
        assert select_rows("* from [//tmp/t] where [$row_index] between 10 and 20") == query_rows[10:21]
        assert select_rows("* from [//tmp/t] where [$tablet_index] in (-10, 20)") == []
        assert select_rows("a from [//tmp/t]") == [{"a": a} for a in range(100)]
        assert select_rows("a + 1 as aa from [//tmp/t] where a < 10") == [{"aa": a} for a in range(1, 11)]

    @authors("babenko")
    def test_select_from_dynamic_multi_tablet(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 10)
        sync_mount_table("//tmp/t")
        assert get("//tmp/t/@tablet_count") == 10

        for i in range(10):
            rows = [{"a": j} for j in range(100)]
            insert_rows("//tmp/t", rows)

        assert_items_equal(
            select_rows("a from [//tmp/t]"),
            [{"a": j} for i in range(10) for j in range(100)],
        )

    @authors("babenko")
    def test_select_from_multi_store(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        for k in range(5):
            write_rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(100)]
            insert_rows("//tmp/t", write_rows)
            if k < 4:
                sync_unmount_table("//tmp/t")
                assert get("//tmp/t/@chunk_count") == k + 1
                sync_mount_table("//tmp/t")

        query_rows = [{"$tablet_index": 0, "$row_index": i, "a": i % 100} for i in range(10, 490)]
        assert (
            select_rows("[$tablet_index], [$row_index], a from [//tmp/t] where [$row_index] between 10 and 489")
            == query_rows
        )

    @authors("babenko")
    def test_select_with_limits(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        write_rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(100)]
        insert_rows("//tmp/t", write_rows)

        query_rows = [{"a": i} for i in range(100)]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10") == query_rows[10:]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] > 10") == query_rows[11:]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] = 10") == query_rows[10:11]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] < 10") == query_rows[:10]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] <= 10") == query_rows[:11]
        assert (
            select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10 and [$row_index] < 20")
            == query_rows[10:20]
        )
        assert (
            select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10 and [$row_index] <= 20")
            == query_rows[10:21]
        )

    @authors("babenko")
    def test_dynamic_to_static(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(100)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", dynamic=False)

        assert not get("//tmp/t/@dynamic")
        assert get("//tmp/t/@row_count") == 100
        assert read_table("//tmp/t") == rows

    @authors("babenko")
    def test_static_to_dynamic(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        for i in range(10):
            write_table("<append=true>//tmp/t", [{"a": j} for j in range(100)])

        read_table("//tmp/t")
        assert get("//tmp/t/@row_count") == 1000

        alter_table("//tmp/t", dynamic=True)
        assert get("//tmp/t/@dynamic")

        sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t]") == [{"a": i % 100} for i in range(1000)]

    @authors("savrus", "babenko")
    def test_no_duplicate_chunks_in_dynamic(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("//tmp/t", [{"a": 0}])
        concatenate(["//tmp/t", "//tmp/t"], "//tmp/t")
        alter_table("//tmp/t", dynamic=True)
        with pytest.raises(YtError):
            mount_table("//tmp/t")

    @authors("savrus")
    def test_chunk_list_kind(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("//tmp/t", [{"a": 0}])
        chunk_list = get("//tmp/t/@chunk_list_id")
        assert get("#{0}/@kind".format(chunk_list)) == "static"

        alter_table("//tmp/t", dynamic=True)
        root_chunk_list = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list = get("#{0}/@child_ids/0".format(root_chunk_list))
        assert get("#{0}/@kind".format(root_chunk_list)) == "ordered_dynamic_root"
        assert get("#{0}/@kind".format(tablet_chunk_list)) == "ordered_dynamic_tablet"

        alter_table("//tmp/t", dynamic=False)
        chunk_list = get("//tmp/t/@chunk_list_id")
        assert get("#{0}/@kind".format(chunk_list)) == "static"

    @authors("babenko")
    def test_trim_failure(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            trim_rows("//tmp/t", -1, 0)
        with pytest.raises(YtError):
            trim_rows("//tmp/t", +1, 0)
        with pytest.raises(YtError):
            trim_rows("//tmp/t", 0, 100)

    @authors("babenko")
    def test_trim_noop(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        trim_rows("//tmp/t", 0, -10)
        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 0)

    @authors("babenko")
    def test_trim_drops_chunks(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False, enable_dynamic_store_read=False)

        for i in range(10):
            write_table("<append=true>//tmp/t", [{"a": j} for j in range(100)])

        chunk_ids = get("//tmp/t/@chunk_ids")

        assert get("//tmp/t/@row_count") == 1000

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        root_chunk_list_id = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list_id = get("#{0}/@child_ids/0".format(root_chunk_list_id))

        for i in range(10):
            trim_rows("//tmp/t", 0, i * 100 + 10)
            wait(
                lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == i * 100 + 10
                and get("#{0}/@statistics/row_count".format(tablet_chunk_list_id)) == 100 * (10 - i)
                and get("#{0}/@child_ids".format(tablet_chunk_list_id)) == chunk_ids[i:]
            )

        trim_rows("//tmp/t", 0, 1000)
        wait(lambda: get("#{0}/@statistics/row_count".format(tablet_chunk_list_id)) == 0)

    @authors("babenko")
    def test_read_obeys_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"a": i} for i in range(100)])
        trim_rows("//tmp/t", 0, 30)
        assert select_rows("a from [//tmp/t]") == [{"a": i} for i in range(30, 100)]

    @authors("babenko")
    def test_make_static_after_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("<append=true>//tmp/t", [{"a": j} for j in range(0, 100)])
        write_table("<append=true>//tmp/t", [{"a": j * 10} for j in range(0, 100)])

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        trim_rows("//tmp/t", 0, 110)
        time.sleep(0.2)

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", dynamic=False)

        assert read_table("//tmp/t") == [{"a": j * 10, "b": None, "c": None} for j in range(0, 100)]

    @authors("babenko")
    def test_trimmed_rows_preserved_on_unmount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("<append=true>//tmp/t", [{"a": j} for j in range(0, 100)])
        write_table("<append=true>//tmp/t", [{"a": j} for j in range(100, 300)])

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] between 110 and 120") == [
            {"a": j} for j in range(110, 121)
        ]

        trim_rows("//tmp/t", 0, 100)

        time.sleep(0.2)

        sync_unmount_table("//tmp/t")

        assert get("//tmp/t/@resource_usage/chunk_count") == 1
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        assert get("#{0}/@statistics/row_count".format(chunk_list_id)) == 200
        assert get("//tmp/t/@tablets/0/flushed_row_count") == 300
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 100

        sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] between 110 and 120") == [
            {"a": j} for j in range(110, 121)
        ]

    @authors("babenko")
    def test_trim_optimizes_chunk_list(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        for i in range(20):
            write_table("<append=true>//tmp/t", [{"a": i}])

        chunk_ids = get("//tmp/t/@chunk_ids")

        alter_table("//tmp/t", dynamic=True)

        def _check(expected_child_count, expected_trimmed_child_count, expected_chunk_ids):
            root_chunk_list_id = get("//tmp/t/@chunk_list_id")
            tablet_chunk_list_id = get("#{0}/@child_ids/0".format(root_chunk_list_id))
            assert get("#{0}/@child_count".format(tablet_chunk_list_id)) == expected_child_count
            assert get("#{0}/@trimmed_child_count".format(tablet_chunk_list_id)) == expected_trimmed_child_count
            assert get("//tmp/t/@chunk_ids") == expected_chunk_ids

        def _trim(trimmed_row_count):
            sync_mount_table("//tmp/t")
            cell_id = get("//tmp/t/@tablets/0/cell_id")
            tablet_id = get("//tmp/t/@tablets/0/tablet_id")
            address = get_tablet_leader_address(tablet_id)
            trim_rows("//tmp/t", 0, trimmed_row_count)
            # NB: 21 == 20 (static stores) + 1 (dynamic store)
            wait(
                lambda: len(
                    get(
                        "//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/stores".format(
                            address, cell_id, tablet_id
                        )
                    )
                )
                == 21 - trimmed_row_count
            )
            sync_unmount_table("//tmp/t")

        _check(20, 0, chunk_ids)
        _trim(3)
        _check(20, 3, chunk_ids[3:])
        _trim(18)
        _check(2, 0, chunk_ids[18:])

    @authors("babenko")
    def test_reshard_adds_tablets(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 5)
        sync_mount_table("//tmp/t")
        for i in range(5):
            insert_rows(
                "//tmp/t",
                [{"$tablet_index": i, "a": i}, {"$tablet_index": i, "a": i + 100}],
            )
            trim_rows("//tmp/t", i, 1)
        sync_unmount_table("//tmp/t")
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_reshard_table("//tmp/t", 5, first_tablet_index=1, last_tablet_index=3)
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 7
        for i in range(7):
            tablet = tablets[i]
            if i >= 4 and i <= 5:
                assert tablet["flushed_row_count"] == 0
                assert tablet["trimmed_row_count"] == 0
                assert select_rows("a from [//tmp/t] where [$tablet_index] = {0}".format(i)) == []
            else:
                assert tablet["flushed_row_count"] == 2
                assert tablet["trimmed_row_count"] == 1
                if i < 4:
                    j = i
                else:
                    j = i - 2
                assert select_rows("a from [//tmp/t] where [$tablet_index] = {0}".format(i)) == [{"a": j + 100}]
        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("babenko")
    def test_reshard_joins_tablets(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 5)
        sync_mount_table("//tmp/t")
        for i in range(5):
            insert_rows(
                "//tmp/t",
                [{"$tablet_index": i, "a": i}, {"$tablet_index": i, "a": i + 100}],
            )
            if i < 2 or i > 3:
                trim_rows("//tmp/t", i, 1)
        sync_unmount_table("//tmp/t")
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_reshard_table("//tmp/t", 2, first_tablet_index=1, last_tablet_index=3)
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 4
        for i in range(4):
            tablet = tablets[i]
            print_debug(i, "->", tablet)
            if i == 2:
                assert tablet["flushed_row_count"] == 4
                assert tablet["trimmed_row_count"] == 0
                assert select_rows("a from [//tmp/t] where [$tablet_index] = {0}".format(i)) == [
                    {"a": 2},
                    {"a": 102},
                    {"a": 3},
                    {"a": 103},
                ]
            else:
                assert tablet["flushed_row_count"] == 2
                assert tablet["trimmed_row_count"] == 1
                if i < 3:
                    j = i
                else:
                    j = i + 1
                assert select_rows("a from [//tmp/t] where [$tablet_index] = {0}".format(i)) == [{"a": j + 100}]
        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("babenko")
    def test_reshard_join_fails_on_trimmed_rows(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 2)
        sync_mount_table("//tmp/t")
        for i in range(2):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
            trim_rows("//tmp/t", i, 1)
        sync_unmount_table("//tmp/t")
        with pytest.raises(YtError):
            reshard_table("//tmp/t", 1)
        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("savrus")
    def test_reshard_after_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 1)
        sync_unmount_table("//tmp/t")

        def _verify(expected_flushed, expected_trimmed):
            tablet = get("//tmp/t/@tablets/0")
            assert tablet["flushed_row_count"] == expected_flushed
            assert tablet["trimmed_row_count"] == expected_trimmed

        _verify(1, 1)
        sync_reshard_table("//tmp/t", 1)
        self._verify_chunk_tree_statistics("//tmp/t")
        _verify(1, 1)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        sync_unmount_table("//tmp/t")
        _verify(2, 1)
        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("savrus")
    def test_snapshot_lock_after_reshard_after_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 2)
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"$tablet_index": 0, "a": 1}])
        insert_rows("//tmp/t", [{"$tablet_index": 1, "a": 2}])
        sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 1)
        trim_rows("//tmp/t", 1, 1)

        def check():
            chunk_list = get("//tmp/t/@chunk_list_id")
            tablet_chunk_lists = get("#{0}/@child_ids".format(chunk_list))
            props = [get("#{0}/@statistics".format(item)) for item in tablet_chunk_lists]
            return all([p["row_count"] != p["logical_row_count"] for p in props])

        wait(check)

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", 1)
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_mount_table("//tmp/t")

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        insert_rows("//tmp/t", [{"$tablet_index": 0, "a": 3}])
        sync_flush_table("//tmp/t")
        abort_transaction(tx)

        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", 2)
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_mount_table("//tmp/t")

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        insert_rows("//tmp/t", [{"$tablet_index": 0, "a": 4}])
        sync_flush_table("//tmp/t")

        insert_rows("//tmp/t", [{"$tablet_index": 1, "a": 5}])
        sync_flush_table("//tmp/t")

        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("ifsmirnov")
    def test_chunk_read_limit_after_chunk_list_cow(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=[{"name": "a", "type": "int64"}])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"a": 0}])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 2}])
        sync_flush_table("//tmp/t")

        path = "<ranges=[{lower_limit={chunk_index=0};upper_limit={chunk_index=2}}]>//tmp/t"

        assert list(read_table(path)) == [{"a": 0}, {"a": 1}]

        trim_rows("//tmp/t", 0, 1)
        wait(lambda: list(read_table(path)) == [{"a": 1}, {"a": 2}])

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        insert_rows("//tmp/t", [{"a": 3}])
        sync_flush_table("//tmp/t")
        assert list(read_table(path)) == [{"a": 1}, {"a": 2}]

        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("ifsmirnov")
    def test_reshard_fully_trimmed(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=[{"name": "a", "type": "int64"}])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"a": 0}])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 2)
        wait(lambda: get("//tmp/t/@chunk_ids") == [])

        def _get_cumulative(entry_type):
            return [s[entry_type] for s in self._get_single_tablet_cumulative_statistics("//tmp/t")]

        # Do not forget two dynamic stores.
        assert _get_cumulative("chunk_count") == [-2, -1, 0] + [1, 2]
        assert _get_cumulative("row_count") == [0, 1, 2] + [2, 2]

        sync_unmount_table("//tmp/t")

        for i in range(2):
            sync_reshard_table("//tmp/t", 1)
            assert _get_cumulative("chunk_count") == [0]
            assert _get_cumulative("row_count") == [2]

    @authors("ifsmirnov")
    def test_chunk_read_limit_after_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=[{"name": "a", "type": "int64"}])
        sync_mount_table("//tmp/t")

        expected_rows = []
        trimmed_row_count = 0

        def _add_chunk():
            idx = len(expected_rows)
            expected_rows.append(idx)
            insert_rows("//tmp/t", [{"a": idx}])
            sync_flush_table("//tmp/t")

        def _trim_chunks(count):
            nonlocal trimmed_row_count
            while trimmed_row_count < count:
                expected_rows[trimmed_row_count] = None
                trimmed_row_count += 1
            trim_rows("//tmp/t", 0, trimmed_row_count)

        def _validate_read(lower_chunk_index, upper_chunk_index):
            expected = expected_rows[lower_chunk_index + trimmed_row_count:upper_chunk_index + trimmed_row_count]
            ranges = "<ranges=[{{lower_limit={{chunk_index={}}};upper_limit={{chunk_index={}}}}}]>".format(
                lower_chunk_index,
                upper_chunk_index)
            actual = [x["a"] for x in read_table(ranges + "//tmp/t")]
            assert expected == actual

        def _validate_cumulative_chunk_count(expected):
            cumulative_statistics = self._get_single_tablet_cumulative_statistics("//tmp/t")
            assert [s["chunk_count"] for s in cumulative_statistics] == expected

        def _validate_cumulative_row_count(expected):
            cumulative_statistics = self._get_single_tablet_cumulative_statistics("//tmp/t")
            assert [s["row_count"] for s in cumulative_statistics] == expected

        for i in range(20):
            _add_chunk()

        # Do not forget take two dynamic stores into account.
        _validate_cumulative_chunk_count(list(range(23)))
        _validate_cumulative_row_count(list(range(21)) + [20, 20])

        _trim_chunks(8)
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 20 - 8)
        sync_flush_table("//tmp/t")
        _validate_read(3, 5)
        _validate_read(5, 15)
        _validate_read(0, 20)
        _validate_read(10, 18)
        _validate_read(17, 20)
        _validate_read(16, 17)
        _validate_read(18, 18)
        _validate_read(18, 20)

        # [-7, -6, ..., 12]
        _validate_cumulative_chunk_count([x - 8 for x in range(23)])
        _validate_cumulative_row_count(list(range(21)) + [20, 20])

        # NB: Chunks are physically removed from the chunk list in portions of at least 17 pcs.
        _trim_chunks(17)
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 20 - 17)
        sync_flush_table("//tmp/t")
        _validate_read(10, 18)
        _validate_read(17, 20)
        _validate_read(16, 17)
        _validate_read(18, 18)
        _validate_read(18, 20)

        _validate_cumulative_chunk_count([0, 1, 2, 3] + [4, 5])
        _validate_cumulative_row_count([17, 18, 19, 20] + [20, 20])

        _trim_chunks(18)
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 20 - 18)
        _validate_cumulative_chunk_count([-1, 0, 1, 2] + [3, 4])
        _validate_cumulative_row_count([17, 18, 19, 20] + [20, 20])

        _trim_chunks(19)
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 20 - 19)
        _validate_cumulative_chunk_count([-2, -1, 0, 1] + [2, 3])
        _validate_cumulative_row_count([17, 18, 19, 20] + [20, 20])

    @authors("ifsmirnov")
    def test_cumulative_statistics_after_cow(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=[{"name": "a", "type": "int64"}])
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"a": 0}])
        sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 1)
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 0)
        sync_unmount_table("//tmp/t")

        assert len(self._get_single_tablet_cumulative_statistics("//tmp/t")) == 2

        sync_reshard_table("//tmp/t", 1)
        assert len(self._get_single_tablet_cumulative_statistics("//tmp/t")) == 1
        assert get("//tmp/t/@chunk_ids") == []
        self._verify_chunk_tree_statistics("//tmp/t")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 0}])
        sync_flush_table("//tmp/t")
        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("babenko", "levysotsky")
    def test_freeze_empty(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        sync_freeze_table("//tmp/t")
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"a": 0}])
        sync_unfreeze_table("//tmp/t")
        sync_unmount_table("//tmp/t")

    @authors("babenko")
    def test_change_commit_ordering(self):
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@commit_ordering", "weak")
        set("//tmp/t/@commit_ordering", "strong")
        with pytest.raises(YtError):
            set("//tmp/t/@commit_ordering", "cool")

    @authors("babenko", "levysotsky")
    def test_no_commit_ordering_change_for_mounted(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@commit_ordering", "strong")

    @authors("babenko")
    def test_set_commit_ordering_upon_construction(self):
        self._create_simple_table("//tmp/t", commit_ordering="strong")
        assert get("//tmp/t/@commit_ordering") == "strong"

    @authors("babenko")
    def test_set_tablet_count_upon_construction_fail(self):
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", tablet_count=0)
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", tablet_count=-1)
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[[]])

    @authors("babenko")
    def test_set_tablet_count_upon_construction_success(self):
        self._create_simple_table("//tmp/t", tablet_count=10)
        assert get("//tmp/t/@tablet_count") == 10

    @authors("babenko")
    def test_tablet_snapshots(self):
        sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]

        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(0, 100)]
        insert_rows("//tmp/t", rows)

        build_snapshot(cell_id=cell_id)

        snapshots = ls("//sys/tablet_cells/" + cell_id + "/snapshots")
        assert len(snapshots) == 1

        with Restarter(self.Env, NODES_SERVICE):
            # Wait to make sure all leases have expired
            time.sleep(3.0)

        wait_for_cells()

        # Wait to make sure all tablets are up
        time.sleep(3.0)

        actual = select_rows("a, b, c from [//tmp/t]")
        assert_items_equal(actual, rows)

    @authors("savrus")
    @pytest.mark.parametrize("erasure_codec", ["none", "reed_solomon_6_3", "lrc_12_2_2"])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.ignore_in_opensource_ci
    def test_read_table(self, optimize_for, erasure_codec):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for, erasure_codec=erasure_codec)
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(0, 100)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        actual = read_table("//tmp/t")
        assert actual == rows

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory(self, mode, optimize_for):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)

        set("//tmp/t/@in_memory_mode", mode)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        def _check_preload_state(state):
            time.sleep(1.0)
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            assert all(
                s["preload_state"] == state
                for s in tablet_data["stores"].values()
                if s["store_state"] == "persistent"
            )
            actual_preload_completed = get("//tmp/t/@tablets/0/statistics/preload_completed_store_count")
            if state == "complete":
                assert actual_preload_completed >= 1
            else:
                assert actual_preload_completed == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_pending_store_count") == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_failed_store_count") == 0

        # Check preload after mount.
        rows1 = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in range(0, 10)]
        insert_rows("//tmp/t", rows1)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert select_rows("a, b, c from [//tmp/t]") == rows1

        # Check preload after flush.
        rows2 = [{"a": i, "b": i * 0.5, "c": "payload" + str(i + 1)} for i in range(0, 10)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert select_rows("a, b, c from [//tmp/t]") == rows1 + rows2

        # Disable in-memory mode
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "none")
        sync_mount_table("//tmp/t")
        _check_preload_state("none")
        assert select_rows("a, b, c from [//tmp/t]") == rows1 + rows2

        # Re-enable in-memory mode
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", mode)
        sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert select_rows("a, b, c from [//tmp/t]") == rows1 + rows2

    @authors("babenko", "levysotsky")
    def test_reshard_trimmed_shared_yt_6948(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", tablet_count=5, enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")
        for i in range(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        for i in range(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
        sync_unmount_table("//tmp/t")
        assert get("//tmp/t/@chunk_count") == 10
        sync_mount_table("//tmp/t")
        for i in range(5):
            trim_rows("//tmp/t", i, 1)
        # There are 2 extra dynamic stores in each tablet.
        wait(lambda: get("//tmp/t/@chunk_count") == 5 + 2 * 5)
        sync_unmount_table("//tmp/t")
        copy("//tmp/t", "//tmp/t2")
        sync_reshard_table("//tmp/t", 1)
        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("savrus", "levysotsky")
    def test_copy_trimmed_yt_7422(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 0}])
        sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 1)
        sync_freeze_table("//tmp/t")
        copy("//tmp/t", "//tmp/t2")
        sync_unfreeze_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        sync_freeze_table("//tmp/t")
        assert get("//tmp/t/@tablets/0/flushed_row_count") == 2

    @authors("babenko")
    def test_timestamp_column(self):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "a", "type": "string"},
                {"name": "$timestamp", "type": "uint64"},
            ],
        )
        sync_mount_table("//tmp/t")

        timestamp0 = generate_timestamp()
        insert_rows("//tmp/t", [{"a": "hello"}])
        insert_rows("//tmp/t", [{"a": "world"}])
        timestamp3 = generate_timestamp()

        timestamp1 = select_rows("[$timestamp] from [//tmp/t] where [$row_index] = 0")[0]["$timestamp"]
        timestamp2 = select_rows("[$timestamp] from [//tmp/t] where [$row_index] = 1")[0]["$timestamp"]

        assert timestamp0 < timestamp1
        assert timestamp1 < timestamp2
        assert timestamp2 < timestamp3

    @staticmethod
    def _expect_cumulative_data_weights(table, weights):
        rows = select_rows("[$cumulative_data_weight] from [{}]".format(table))
        assert len(rows) == len(weights)
        cumulative_data_weights = []
        for row in rows:
            cumulative_data_weights.append(row["$cumulative_data_weight"])
        assert cumulative_data_weights == weights

    @staticmethod
    def _get_table_row_count_from_chunk_list(path, tablet):
        root_chunk_list_id = get("{}/@chunk_list_id".format(path))
        tablet_chunk_list_id = get("#{}/@child_ids/{}".format(root_chunk_list_id, tablet))
        return get("#{}/@statistics".format(tablet_chunk_list_id))["row_count"]

    @authors("achulkov2")
    def test_cumulative_data_weight_column(self):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "test", "type": "string"},
                {"name": "$cumulative_data_weight", "type": "int64"},
            ],
        )
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"test": "aaa"}])
        insert_rows("//tmp/t", [{"test": "bb"}])

        self._expect_cumulative_data_weights("//tmp/t", [12, 23])

        sync_flush_table("//tmp/t")

        for size in range(1, 6):
            insert_rows("//tmp/t", [{"test": "c" * size}])
            sync_flush_table("//tmp/t")

        self._expect_cumulative_data_weights("//tmp/t", [12, 23, 33, 44, 56, 69, 83])

        trim_rows("//tmp/t", 0, 4)
        self._expect_cumulative_data_weights("//tmp/t", [56, 69, 83])

        wait(lambda: self._get_table_row_count_from_chunk_list("//tmp/t", 0) == 3)

        insert_rows("//tmp/t", [{"test": "eeee"}])
        insert_rows("//tmp/t", [{"test": "f"}])

        self._expect_cumulative_data_weights("//tmp/t", [56, 69, 83, 96, 106])

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"test": "ggg"}])
        insert_rows("//tmp/t", [{"test": "hhhh"}])

        self._expect_cumulative_data_weights("//tmp/t", [56, 69, 83, 96, 106, 118, 131])

    @authors("achulkov2")
    def test_cumulative_data_weight_includes_timestamp(self):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "test", "type": "string"},
                {"name": "$cumulative_data_weight", "type": "int64"},
                {"name": "$timestamp", "type": "uint64"},
            ],
        )
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"test": "hey you"}])
        insert_rows("//tmp/t", [{"test": "hello world!"}])

        self._expect_cumulative_data_weights("//tmp/t", [24, 24 + 29])

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"test": "a"}])
        insert_rows("//tmp/t", [{"test": "bb"}])

        self._expect_cumulative_data_weights("//tmp/t", [24, 24 + 29, 24 + 29 + 18, 24 + 29 + 18 + 19])

    @authors("achulkov2")
    def test_data_weight_attributes(self):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/t",
            schema=[
                {"name": "test", "type": "string"},
            ],
        )
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"test": "hey you"}])
        insert_rows("//tmp/t", [{"test": "hello world!"}])
        sync_flush_table("//tmp/t")

        insert_rows("//tmp/t", [{"test": "abacaba"}])
        sync_flush_table("//tmp/t")

        insert_rows("//tmp/t", [{"test": "test"}])
        sync_flush_table("//tmp/t")

        trim_rows("//tmp/t", 0, 3)
        wait(lambda: self._get_table_row_count_from_chunk_list("//tmp/t", 0) == 1)

        root_chunk_list_id = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list_id = get("#{0}/@child_ids/0".format(root_chunk_list_id))
        stats = get("#{0}/@statistics".format(tablet_chunk_list_id))
        assert stats["data_weight"] == 5
        assert stats["logical_data_weight"] == 8 + 13 + 8 + 5
        assert stats["trimmed_data_weight"] == 8 + 13 + 8

    @authors("savrus", "levysotsky")
    def test_data_ttl(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", min_data_ttl=0, max_data_ttl=0, min_data_versions=0)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 0}])
        sync_flush_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 1)

    @authors("lexolordan")
    def test_row_count_to_keep(self):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            min_data_ttl=0,
            max_data_ttl=0,
            min_data_versions=0,
            row_count_to_keep=2,
        )
        sync_mount_table("//tmp/t")

        def _create_chunk(rows):
            insert_rows("//tmp/t", rows)
            sync_flush_table("//tmp/t")

        _create_chunk([{"a": 0}, {"a": 1}])
        _create_chunk([{"a": 2}])
        _create_chunk([{"a": 3}])
        _create_chunk([{"a": 4}])

        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 3)

    @authors("ifsmirnov")
    def test_required_columns(self):
        schema = [
            {"name": "a", "type": "int64", "required": True},
            {"name": "b", "type": "int64"},
        ]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict()])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(b=1)])

        insert_rows("//tmp/t", [dict(a=1)])
        insert_rows("//tmp/t", [dict(a=1, b=1)])

    @authors("ifsmirnov")
    def test_required_computed_column_fails(self):
        schema = [
            {"name": "key", "type": "int64"},
            {
                "name": "computed",
                "type": "int64",
                "expression": "key * 10",
                "required": True,
            },
            {"name": "value", "type": "string"},
        ]

        sync_create_cells(1)
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", schema=schema)

    @authors("ifsmirnov")
    def test_required_aggregate_columns(self):
        schema = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "int64", "aggregate": "sum", "required": True},
        ]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key=1)])
        insert_rows("//tmp/t", [dict(key=1, value=2)])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key=1)])

    @authors("akozhikhov")
    def test_delete_rows_error(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        try:
            delete_rows("//tmp/t", [{"key": 0}])
        except YtError as err:
            if "Table //tmp/t is not sorted" != err.inner_errors[0]["message"]:
                raise

    @authors("ifsmirnov")
    def test_forced_unmount_after_trim(self):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        insert_rows("//tmp/t", [{"a": 2}])
        trim_rows("//tmp/t", 0, 1)

        def _select():
            return sorted(row["a"] for row in select_rows("* from [//tmp/t]"))

        assert _select() == [2]

        # Forced unmount with trimmed dynamic store. Trimmed row count is rolled back.
        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 1)
        sync_unmount_table("//tmp/t", force=True)
        assert get("//tmp/t/@chunk_count") == 0
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 0

        sync_mount_table("//tmp/t")
        assert _select() == []
        insert_rows("//tmp/t", [{"a": 3}])
        insert_rows("//tmp/t", [{"a": 4}])
        assert _select() == [3, 4]
        sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 1)
        assert _select() == [4]

        # Forced unmount with trimmed chunk. No rollback.
        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 1)
        sync_unmount_table("//tmp/t", force=True)
        assert get("//tmp/t/@chunk_count") == 1
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 1
        sync_mount_table("//tmp/t")
        assert _select() == [4]

    @authors("ifsmirnov")
    def test_create_with_trimmed_row_count(self):
        sync_create_cells(1)

        with raises_yt_error():
            self._create_simple_table("//tmp/t", trimmed_row_counts=[1])
        with raises_yt_error():
            self._create_simple_table("//tmp/t", trimmed_row_counts=[1, 2], tablet_count=4)

        self._create_simple_table("//tmp/t", trimmed_row_counts=[10, 20], tablet_count=2)
        tablets = get("//tmp/t/@tablets")
        assert tablets[0]["flushed_row_count"] == 10
        assert tablets[0]["flushed_row_count"] == 10
        assert tablets[1]["trimmed_row_count"] == 20
        assert tablets[1]["trimmed_row_count"] == 20
        self._verify_chunk_tree_statistics("//tmp/t")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 10, "$tablet_index": 0}, {"a": 20, "$tablet_index": 1}])
        assert_items_equal(
            select_rows("[$row_index], a from [//tmp/t]"),
            [{"a": 10, "$row_index": 10}, {"a": 20, "$row_index": 20}])

        sync_unmount_table("//tmp/t")
        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("ifsmirnov")
    def test_reshard_with_trimmed_row_count(self):
        if self.DRIVER_BACKEND == "rpc":
            pytest.skip("No trimmed_row_counts in rpc proxy")

        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 5)
        sync_mount_table("//tmp/t")
        for i in range(5):
            insert_rows(
                "//tmp/t",
                [{"$tablet_index": i, "a": i}, {"$tablet_index": i, "a": i + 100}],
            )
            trim_rows("//tmp/t", i, 1)
        sync_unmount_table("//tmp/t")

        with raises_yt_error():
            sync_reshard_table("//tmp/t", 5, trimmed_row_counts=[1])
        with raises_yt_error():
            sync_reshard_table("//tmp/t", 3, first_tablet_index=0, last_tablet_index=2, trimmed_row_counts=[1, 2, 3])
        with raises_yt_error():
            sync_reshard_table("//tmp/t", 3, first_tablet_index=0, last_tablet_index=0, trimmed_row_counts=[1, 2, 3])

        sync_reshard_table("//tmp/t", 5, first_tablet_index=1, last_tablet_index=3, trimmed_row_counts=[8, 9])

        tablets = get("//tmp/t/@tablets")
        assert [t["flushed_row_count"] for t in tablets] == [2, 2, 2, 2, 8, 9, 2]
        assert [t["trimmed_row_count"] for t in tablets] == [1, 1, 1, 1, 8, 9, 1]

        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("ifsmirnov")
    def test_stress_reshard_with_trimmed_row_count(self):
        if self.DRIVER_BACKEND == "rpc":
            pytest.skip("No trimmed_row_counts in rpc proxy")

        sync_create_cells(1)

        trimmed_row_counts = [1, 2, 3, 4, 5]
        self._create_simple_table("//tmp/t", tablet_count=5, trimmed_row_counts=trimmed_row_counts)
        rnd = random.Random(131827381923)
        for i in range(20):
            first = rnd.randint(0, len(trimmed_row_counts) - 1)
            last = rnd.randint(0, len(trimmed_row_counts) - 1)
            if first > last:
                first, last = last, first
            old_count = last - first + 1
            new_count = rnd.randint(1, 5)
            created_tablet_count = max(0, new_count - old_count)
            if rnd.randint(0, 1) == 0:
                new_trimmed_row_counts = [rnd.randint(0, 10) for j in range(created_tablet_count)]
            else:
                new_trimmed_row_counts = []
            sync_reshard_table("//tmp/t", new_count, first_tablet_index=first, last_tablet_index=last, trimmed_row_counts=new_trimmed_row_counts)

            if new_count > old_count:
                trimmed_row_counts[last+1:last+1] = new_trimmed_row_counts or [0] * created_tablet_count
            else:
                del trimmed_row_counts[first+new_count:first+old_count]

            self._verify_chunk_tree_statistics("//tmp/t")
            assert [t["trimmed_row_count"] for t in get("//tmp/t/@tablets")] == trimmed_row_counts

    @authors("alexelexa")
    def test_tablet_size_limit(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@mount_config/max_ordered_tablet_data_weight", 1)
        sync_reshard_table("//tmp/t", 3)
        sync_mount_table("//tmp/t")

        for i in range(2):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])

        for i in range(2):
            with raises_yt_error():
                insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])

        insert_rows("//tmp/t", [{"$tablet_index": 2, "a": 2}])
        with raises_yt_error():
            insert_rows("//tmp/t", [{"$tablet_index": 2, "a": i}])

        remove("//tmp/t/@mount_config/max_ordered_tablet_data_weight")
        remount_table("//tmp/t")

        for i in range(3):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@mount_config/max_ordered_tablet_data_weight", 1)
        sync_mount_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        trim_rows("//tmp/t", 0, 2)
        wait(lambda: len(chunk_ids) > len(get("//tmp/t/@chunk_ids")))

        with raises_yt_error():
            insert_rows("//tmp/t", [{"$tablet_index": 1, "a": 4}])

        insert_rows("//tmp/t", [{"$tablet_index": 0, "a": 3}])

    @authors("alexelexa")
    def test_large_tablet_size_limit(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        max_data_weight = 200000
        set("//tmp/t/@mount_config/max_ordered_tablet_data_weight", max_data_weight)
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        average_row_size = 79

        def _make_rows(index=0):
            return [{"a": i, "c": chr(ord('a') + index) * 70} for i in range(100)]

        def _get_table_data_weight():
            address = get_tablet_leader_address(tablet_id)
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            stores = tablet_data["stores"].values()

            size = 0
            for store in stores:
                if store["store_state"] == "persistent":
                    size += store["row_count"] * average_row_size
                elif store["store_state"] == "active_dynamic":
                    size += store["pool_capacity"]
            return size

        insert_rows("//tmp/t", _make_rows())
        sync_flush_table("//tmp/t")

        index = 1
        while _get_table_data_weight() < max_data_weight:
            insert_rows("//tmp/t", _make_rows(index))
            index += 1

        assert _get_table_data_weight() >= max_data_weight

        with raises_yt_error():
            insert_rows("//tmp/t", _make_rows(index))


class TestOrderedDynamicTablesMulticell(TestOrderedDynamicTables):
    ENABLE_MULTIDAEMON = False  # There are component restarts in the base class.
    NUM_SECONDARY_MASTER_CELLS = 2


class TestOrderedDynamicTablesPortal(TestOrderedDynamicTablesMulticell):
    ENABLE_MULTIDAEMON = False  # There are component restarts in the base class.
    ENABLE_TMP_PORTAL = True


class TestOrderedDynamicTablesRpcProxy(TestOrderedDynamicTables):
    ENABLE_MULTIDAEMON = False  # There are component restarts in the base class.
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


##################################################################


@pytest.mark.enabled_multidaemon
class TestOrderedDynamicTablesMultipleWriteBatches(TestOrderedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    DELTA_DRIVER_CONFIG = {"max_rows_per_write_request": 10}

    @authors("babenko")
    def test_multiple_write_batches(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "c": "text"} for i in range(100)]
        insert_rows("//tmp/t", rows)
        assert select_rows("a, c from [//tmp/t]") == rows
