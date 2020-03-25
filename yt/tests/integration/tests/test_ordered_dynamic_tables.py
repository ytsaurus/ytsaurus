import pytest

from test_dynamic_tables import DynamicTablesBase

from yt_env_setup import wait, Restarter, NODES_SERVICE
from yt_commands import *

from yt.environment.helpers import assert_items_equal

from flaky import flaky

from time import sleep

##################################################################

class TestOrderedDynamicTablesBase(DynamicTablesBase):
    def _create_simple_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "double"},
                {"name": "c", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

    def _wait_for_in_memory_stores_preload(self, table):
        for tablet in get(table + "/@tablets"):
            tablet_id = tablet["tablet_id"]
            address = get_tablet_leader_address(tablet_id)
            def all_preloaded():
                tablet_data = self._find_tablet_orchid(address, tablet_id)
                return all(s["preload_state"] == "complete" for s in tablet_data["stores"].itervalues() if s["store_state"] == "persistent")
            wait(lambda: all_preloaded())

    def _verify_cumulative_statistics_match_statistics(self, chunk_list_id):
        attrs = get("#{}/@".format(chunk_list_id), attributes=["statistics", "cumulative_statistics"])
        statistics = attrs["statistics"]
        cumulative_statistics = attrs["cumulative_statistics"]
        assert len(cumulative_statistics) > 0
        assert cumulative_statistics[-1]["chunk_count"] == statistics["logical_chunk_count"]
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
        assert statistics["logical_chunk_count"] == sum([c["logical_chunk_count"] for c in tablet_statistics])

        self._verify_cumulative_statistics_match_statistics(chunk_list_id)
        for tablet_chunk_list in tablet_chunk_lists:
            self._verify_cumulative_statistics_match_statistics(tablet_chunk_list)

##################################################################

class TestOrderedDynamicTables(TestOrderedDynamicTablesBase):
    @authors("babenko")
    def test_mount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert not "pivot_key" in tablet
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
        with pytest.raises(YtError): insert_rows("//tmp/t", rows)

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
                tablet_profiling.get_counter("commit/" + count_name))

        assert get_all_counters("row_count") == (0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0)
        assert tablet_profiling.get_counter("select/cpu_time") == 0

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in xrange(10)]
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

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

    @authors("babenko")
    def test_flush(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
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

        for i in xrange(10):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])

        for i in xrange(10):
            assert select_rows("a from [//tmp/t] where [$tablet_index] = " + str(i)) == [{"a": i}]

    @authors("babenko")
    @pytest.mark.parametrize("dynamic", [True, False])
    def test_select_from_single_tablet(self, dynamic):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        query_rows = [{"$tablet_index": 0, "$row_index": i, "a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", write_rows)

        if not dynamic:
            sync_unmount_table("//tmp/t")
            assert get("//tmp/t/@chunk_count") == 1
            sync_mount_table("//tmp/t")

        assert select_rows("* from [//tmp/t]") == query_rows
        assert select_rows("[$row_index], a from [//tmp/t]") == [{"$row_index": row["$row_index"], "a": row["a"]} for row in query_rows]
        assert select_rows("c, b from [//tmp/t]") == [{"b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        assert select_rows("* from [//tmp/t] where [$row_index] between 10 and 20") == query_rows[10:21]
        assert select_rows("* from [//tmp/t] where [$tablet_index] in (-10, 20)") == []
        assert select_rows("a from [//tmp/t]") == [{"a": a} for a in xrange(100)]
        assert select_rows("a + 1 as aa from [//tmp/t] where a < 10") == [{"aa": a} for a in xrange(1, 11)]

    @authors("babenko")
    def test_select_from_dynamic_multi_tablet(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", 10)
        sync_mount_table("//tmp/t")
        assert get("//tmp/t/@tablet_count") == 10

        for i in xrange(10):
            rows = [{"a": j} for j in xrange(100)]
            insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("a from [//tmp/t]"), [{"a": j} for i in xrange(10) for j in xrange(100)])

    @authors("babenko")
    def test_select_from_multi_store(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        for k in xrange(5):
            write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
            insert_rows("//tmp/t", write_rows)
            if k < 4:
                sync_unmount_table("//tmp/t")
                assert get("//tmp/t/@chunk_count") == k + 1
                sync_mount_table("//tmp/t")

        query_rows = [{"$tablet_index": 0, "$row_index": i, "a": i % 100} for i in xrange(10, 490)]
        assert select_rows("[$tablet_index], [$row_index], a from [//tmp/t] where [$row_index] between 10 and 489") == query_rows

    @authors("babenko")
    def test_select_with_limits(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", write_rows)

        query_rows = [{"a": i} for i in xrange(100)]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10") == query_rows[10:]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] > 10") == query_rows[11:]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] = 10") == query_rows[10:11]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] < 10") == query_rows[:10]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] <= 10") == query_rows[:11]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10 and [$row_index] < 20") == query_rows[10:20]
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] >= 10 and [$row_index] <= 20") == query_rows[10:21]

    @authors("babenko")
    def test_dynamic_to_static(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
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

        for i in xrange(10):
            write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(100)])

        read_table("//tmp/t")
        assert get("//tmp/t/@row_count") == 1000

        alter_table("//tmp/t", dynamic=True)
        assert get("//tmp/t/@dynamic")

        sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t]") == [{"a": i % 100} for i in xrange(1000)]

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

        with pytest.raises(YtError): trim_rows("//tmp/t", -1, 0)
        with pytest.raises(YtError): trim_rows("//tmp/t", +1, 0)
        with pytest.raises(YtError): trim_rows("//tmp/t", 0, 100)

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
        self._create_simple_table("//tmp/t", dynamic=False)

        for i in xrange(10):
            write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(100)])

        chunk_ids = get("//tmp/t/@chunk_ids")

        assert get("//tmp/t/@row_count") == 1000

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        root_chunk_list_id = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list_id = get("#{0}/@child_ids/0".format(root_chunk_list_id))

        for i in xrange(10):
            trim_rows("//tmp/t", 0, i * 100 + 10)
            wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == i * 100 + 10 and
                         get("#{0}/@statistics/row_count".format(tablet_chunk_list_id)) == 100 * (10 - i) and
                         get("#{0}/@child_ids".format(tablet_chunk_list_id)) == chunk_ids[i:])

        trim_rows("//tmp/t", 0, 1000)
        wait(lambda: get("#{0}/@statistics/row_count".format(tablet_chunk_list_id)) == 0)

    @authors("babenko")
    def test_read_obeys_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"a": i} for i in xrange(100)])
        trim_rows("//tmp/t", 0, 30)
        assert select_rows("a from [//tmp/t]") == [{"a": i} for i in xrange(30, 100)]

    @authors("babenko")
    def test_make_static_after_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(0, 100)])
        write_table("<append=true>//tmp/t", [{"a": j * 10} for j in xrange(0, 100)])

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        trim_rows("//tmp/t", 0, 110)
        sleep(0.2)

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", dynamic=False)

        assert read_table("//tmp/t") == [{"a": j * 10, "b": None, "c": None} for j in xrange(0, 100)]

    @authors("babenko")
    def test_trimmed_rows_perserved_on_unmount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(0, 100)])
        write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(100, 300)])

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] between 110 and 120") == [{"a": j} for j in xrange(110, 121)]

        trim_rows("//tmp/t", 0, 100)

        sleep(0.2)

        sync_unmount_table("//tmp/t")

        assert get("//tmp/t/@resource_usage/chunk_count") == 1
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        assert get("#{0}/@statistics/row_count".format(chunk_list_id)) == 200
        assert get("//tmp/t/@tablets/0/flushed_row_count") == 300
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 100

        sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] between 110 and 120") == [{"a": j} for j in xrange(110, 121)]

    @authors("babenko")
    def test_trim_optimizes_chunk_list(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        for i in xrange(20):
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
            wait(lambda: len(get("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/stores".format(address, cell_id, tablet_id))) == 21 - trimmed_row_count)
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
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}, {"$tablet_index": i, "a": i + 100}])
            trim_rows("//tmp/t", i, 1)
        sync_unmount_table("//tmp/t")
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_reshard_table("//tmp/t", 5, first_tablet_index=1, last_tablet_index=3)
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 7
        for i in xrange(7):
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
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}, {"$tablet_index": i, "a": i + 100}])
            if i < 2 or i > 3:
                trim_rows("//tmp/t", i, 1)
        sync_unmount_table("//tmp/t")
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_reshard_table("//tmp/t", 2, first_tablet_index=1, last_tablet_index=3)
        self._verify_chunk_tree_statistics("//tmp/t")
        sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 4
        for i in xrange(4):
            tablet = tablets[i]
            print_debug(i, '->', tablet)
            if i == 2:
                assert tablet["flushed_row_count"] == 4
                assert tablet["trimmed_row_count"] == 0
                assert select_rows("a from [//tmp/t] where [$tablet_index] = {0}".format(i)) == [{"a": 2}, {"a": 102}, {"a": 3}, {"a": 103}]
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
        for i in xrange(2):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
            trim_rows("//tmp/t", i, 1)
        sync_unmount_table("//tmp/t")
        with pytest.raises(YtError): reshard_table("//tmp/t", 1)
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
            props = [get("#{0}/@statistics".format(l)) for l in tablet_chunk_lists]
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
        wait(lambda: list(read_table(path)) == [{"a": 1}])

        tx = start_transaction(timeout=60000)
        lock("//tmp/t", mode="snapshot", tx=tx)

        insert_rows("//tmp/t", [{"a": 3}])
        sync_flush_table("//tmp/t")
        assert list(read_table(path)) == [{"a": 1}]

        self._verify_chunk_tree_statistics("//tmp/t")

    @authors("ifsmirnov")
    def test_chunk_read_limit_after_trim(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=[{"name": "a", "type": "int64"}])
        sync_mount_table("//tmp/t")

        expected_rows = []
        trimmed_row_count = [0] # Cannot otherwise modify this variable from _trim_chunks.

        def _add_chunk():
            idx = len(expected_rows)
            expected_rows.append(idx)
            insert_rows("//tmp/t", [{"a": idx}])
            sync_flush_table("//tmp/t")

        def _trim_chunks(count):
            while trimmed_row_count[0] < count:
                expected_rows[trimmed_row_count[0]] = None
                trimmed_row_count[0] += 1
            trim_rows("//tmp/t", 0, trimmed_row_count[0])

        def _validate_read(lower_chunk_index, upper_chunk_index):
            expected = [i for i in expected_rows[lower_chunk_index:upper_chunk_index] if i is not None]
            # str.format doesn't like extra {}-s in the format string.
            ranges = "<ranges=[{lower_limit={chunk_index=" + str(lower_chunk_index) + "};"
            ranges += "upper_limit={chunk_index=" + str(upper_chunk_index) + "}}]>"
            actual = [x['a'] for x in read_table(ranges + "//tmp/t")]
            assert expected == actual

        for i in range(20):
            _add_chunk()

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

        # NB: chunks are physically removed from the chunk list in portions of at least 17 pcs.
        _trim_chunks(17)
        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 20 - 17)
        sync_flush_table("//tmp/t")
        _validate_read(10, 18)
        _validate_read(17, 20)
        _validate_read(16, 17)
        _validate_read(18, 18)
        _validate_read(18, 20)

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

        def _get_cumulative_statistics():
            root_chunk_list = get("//tmp/t/@chunk_list_id")
            tablet_chunk_list = get("#{}/@child_ids/0".format(root_chunk_list))
            cumulative_statistics = get("#{}/@cumulative_statistics".format(tablet_chunk_list))
            return cumulative_statistics

        assert len(_get_cumulative_statistics()) == 2

        sync_reshard_table("//tmp/t", 1)
        assert len(_get_cumulative_statistics()) == 1
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
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"a": 0}])
        sync_unfreeze_table("//tmp/t")
        sync_unmount_table("//tmp/t")

    @authors("babenko")
    def test_change_commit_ordering(self):
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@commit_ordering", "weak")
        set("//tmp/t/@commit_ordering", "strong")
        with pytest.raises(YtError): set("//tmp/t/@commit_ordering", "cool")

    @authors("babenko", "levysotsky")
    def test_no_commit_ordering_change_for_mounted(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@commit_ordering", "strong")

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

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
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
    def test_read_table(self, optimize_for, erasure_codec):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for, erasure_codec=erasure_codec)
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
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
            sleep(1.0)
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            assert all(s["preload_state"] == state for s in tablet_data["stores"].itervalues() if s["store_state"] == "persistent")
            actual_preload_completed = get("//tmp/t/@tablets/0/statistics/preload_completed_store_count")
            if state == "complete":
                assert actual_preload_completed >= 1
            else:
                assert actual_preload_completed == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_pending_store_count") == 0
            assert get("//tmp/t/@tablets/0/statistics/preload_failed_store_count") == 0

        # Check preload after mount.
        rows1 = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 10)]
        insert_rows("//tmp/t", rows1)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert select_rows("a, b, c from [//tmp/t]") == rows1

        # Check preload after flush.
        rows2 = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i + 1)} for i in xrange(0, 10)]
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
        self._create_simple_table("//tmp/t", tablet_count=5)
        sync_mount_table("//tmp/t")
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
        sync_unmount_table("//tmp/t")
        assert get("//tmp/t/@chunk_count") == 10
        sync_mount_table("//tmp/t")
        for i in xrange(5):
            trim_rows("//tmp/t", i, 1)
        wait(lambda: get("//tmp/t/@chunk_count") == 5)
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
        create_dynamic_table("//tmp/t", schema=[
            {"name": "a", "type": "string"},
            {"name": "$timestamp", "type": "uint64"}])
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
        self._create_simple_table("//tmp/t", min_data_ttl=0, max_data_ttl=0, min_data_versions=0, row_count_to_keep=2)
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
                {"name": "b", "type": "int64"}]

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
                {"name": "computed", "type": "int64", "expression": "key * 10", "required": True},
                {"name": "value", "type": "string"}]

        sync_create_cells(1)
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", schema=schema)

    @authors("ifsmirnov")
    def test_required_aggregate_columns(self):
        schema = [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "int64", "aggregate": "sum", "required": True}]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key=1)])
        insert_rows("//tmp/t", [dict(key=1, value=2)])
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [dict(key=1)])

class TestOrderedDynamicTablesMulticell(TestOrderedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestOrderedDynamicTablesPortal(TestOrderedDynamicTablesMulticell):
    ENABLE_TMP_PORTAL = True

class TestOrderedDynamicTablesRpcProxy(TestOrderedDynamicTables):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

##################################################################

class TestOrderedDynamicTablesMultipleWriteBatches(TestOrderedDynamicTablesBase):
    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 10
    }

    @authors("babenko")
    def test_multiple_write_batches(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"a": i, "c": "text"} for i in xrange(100)]
        insert_rows("//tmp/t", rows)
        assert select_rows("a, c from [//tmp/t]") == rows
