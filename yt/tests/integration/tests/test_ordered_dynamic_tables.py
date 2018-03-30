import pytest

from test_dynamic_tables import TestDynamicTablesBase

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

from yt.environment.helpers import assert_items_equal

from time import sleep

##################################################################

class TestOrderedDynamicTables(TestDynamicTablesBase):
    def _create_simple_table(self, path, dynamic=True, **extra_attributes):
        attributes={
            "dynamic": dynamic,
            "external": False,
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "double"},
                {"name": "c", "type": "string"}
            ]
        }
        attributes.update(extra_attributes)
        create("table", path, attributes=attributes)

    def _wait_for_in_memory_stores_preload(self, table):
        for tablet in get(table + "/@tablets"):
            tablet_id = tablet["tablet_id"]
            address = self._get_tablet_leader_address(tablet_id)
            def all_preloaded():
                tablet_data = self._find_tablet_orchid(address, tablet_id)
                return all(s["preload_state"] == "complete" for s in tablet_data["stores"].itervalues() if s["store_state"] == "persistent")
            wait(lambda: all_preloaded())

    def test_mount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert not "pivot_key" in tablet
        tablet_id = tablet["tablet_id"]
        cell_id = tablet["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]

    def test_unmount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self.sync_mount_table("//tmp/t")

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        self.sync_mount_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_access_to_frozen(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        rows = [{"a": 1}]
        insert_rows("//tmp/t", rows)
        self.sync_freeze_table("//tmp/t")
        assert select_rows("a from [//tmp/t]") == rows
        with pytest.raises(YtError): insert_rows("//tmp/t", rows)

    def test_ordered_tablet_node_profiling(self):
        path = "//tmp/x"
        self.sync_create_cells(1)
        self._create_simple_table(path, enable_profiling=True)
        self.sync_mount_table(path)

        tablet_profiling = self._get_tablet_profiling(path)
        select_profiling = self._get_profiling(path)

        def get_all_counters(count_name):
            return (
                tablet_profiling.get_counter("select/" + count_name),
                tablet_profiling.get_counter("write/" + count_name),
                tablet_profiling.get_counter("commit/" + count_name))

        assert get_all_counters("row_count") == (0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0)
        assert select_profiling.get_counter("select/cpu_time") == 0

        rows = [{"a": i, "b": i * 0.5, "c": "payload" + str(i)} for i in xrange(9)]
        insert_rows(path, rows)

        sleep(2)  # sleep is needed to ensure that the profiling counters are updated properly

        rows = [{"a": 100, "b": 0.5, "c": "data"}]
        insert_rows(path, rows)

        sleep(2)

        assert get_all_counters("row_count") == (0, 10, 10)
        assert get_all_counters("data_weight") == (0, 246, 246)
        assert select_profiling.get_counter("select/cpu_time") == 0

        select_rows("* from [{}]".format(path))

        sleep(2)

        assert get_all_counters("row_count") == (10, 10, 10)
        assert get_all_counters("data_weight") == (406, 246, 246)
        assert select_profiling.get_counter("select/cpu_time") > 0

    def test_insert(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

    def test_flush(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@row_count") == 100

    def test_insert_with_explicit_tablet_index(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", 10)
        self.sync_mount_table("//tmp/t")

        for i in xrange(10):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])

        for i in xrange(10):
            assert select_rows("a from [//tmp/t] where [$tablet_index] = " + str(i)) == [{"a": i}]

    def _test_select_from_single_tablet(self, dynamic):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        query_rows = [{"$tablet_index": 0, "$row_index": i, "a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", write_rows)

        if not dynamic:
            self.sync_unmount_table("//tmp/t")
            assert get("//tmp/t/@chunk_count") == 1
            self.sync_mount_table("//tmp/t")

        assert select_rows("* from [//tmp/t]") == query_rows
        assert select_rows("[$row_index], a from [//tmp/t]") == [{"$row_index": row["$row_index"], "a": row["a"]} for row in query_rows]
        assert select_rows("c, b from [//tmp/t]") == [{"b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        assert select_rows("* from [//tmp/t] where [$row_index] between 10 and 20") == query_rows[10:21]
        assert select_rows("* from [//tmp/t] where [$tablet_index] in (-10, 20)") == []
        assert select_rows("a from [//tmp/t]") == [{"a": a} for a in xrange(100)]
        assert select_rows("a + 1 as aa from [//tmp/t] where a < 10") == [{"aa": a} for a in xrange(1, 11)]

    def test_select_from_dynamic_single_tablet(self):
        self._test_select_from_single_tablet(dynamic=True)

    def test_select_from_chunk_single_tablet(self):
        self._test_select_from_single_tablet(dynamic=False)

    def test_select_from_dynamic_multi_tablet(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", 10)
        self.sync_mount_table("//tmp/t")
        assert get("//tmp/t/@tablet_count") == 10

        for i in xrange(10):
            rows = [{"a": j} for j in xrange(100)]
            insert_rows("//tmp/t", rows)

        assert_items_equal(select_rows("a from [//tmp/t]"), [{"a": j} for i in xrange(10) for j in xrange(100)])

    def test_select_from_multi_store(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        for k in xrange(5):
            write_rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
            insert_rows("//tmp/t", write_rows)
            if k < 4:
                self.sync_unmount_table("//tmp/t")
                assert get("//tmp/t/@chunk_count") == k + 1
                self.sync_mount_table("//tmp/t")

        query_rows = [{"$tablet_index": 0, "$row_index": i, "a": i % 100} for i in xrange(10, 490)]
        assert select_rows("[$tablet_index], [$row_index], a from [//tmp/t] where [$row_index] between 10 and 489") == query_rows

    def test_select_with_limits(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

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

    def test_dynamic_to_static(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", dynamic=False)

        assert not get("//tmp/t/@dynamic")
        assert get("//tmp/t/@row_count") == 100
        assert read_table("//tmp/t") == rows

    def test_static_to_dynamic(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        for i in xrange(10):
            write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(100)])

        read_table("//tmp/t")
        assert get("//tmp/t/@row_count") == 1000

        alter_table("//tmp/t", dynamic=True)
        assert get("//tmp/t/@dynamic")

        self.sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t]") == [{"a": i % 100} for i in xrange(1000)]

    def test_no_duplicate_chunks_in_dynamic(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("//tmp/t", [{"a": 0}])
        concatenate(["//tmp/t", "//tmp/t"], "//tmp/t")
        with pytest.raises(YtError): alter_table("//tmp/t", dynamic=True)

    def test_chunk_list_kind(self):
        self.sync_create_cells(1)
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

    def test_trim_failure(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        with pytest.raises(YtError): trim_rows("//tmp/t", -1, 0)
        with pytest.raises(YtError): trim_rows("//tmp/t", +1, 0)
        with pytest.raises(YtError): trim_rows("//tmp/t", 0, 100)

    def test_trim_noop(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        trim_rows("//tmp/t", 0, -10)
        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 0)

    def test_trim_drops_chunks(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        for i in xrange(10):
            write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(100)])

        chunk_ids = get("//tmp/t/@chunk_ids")

        assert get("//tmp/t/@row_count") == 1000

        alter_table("//tmp/t", dynamic=True)
        self.sync_mount_table("//tmp/t")

        root_chunk_list_id = get("//tmp/t/@chunk_list_id")
        tablet_chunk_list_id = get("#{0}/@child_ids/0".format(root_chunk_list_id))

        for i in xrange(10):
            trim_rows("//tmp/t", 0, i * 100 + 10)
            wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == i * 100 + 10 and
                         get("#{0}/@statistics/row_count".format(tablet_chunk_list_id)) == 100 * (10 - i) and
                         get("#{0}/@child_ids".format(tablet_chunk_list_id)) == chunk_ids[i:])

        trim_rows("//tmp/t", 0, 1000)
        wait(lambda: get("#{0}/@statistics/row_count".format(tablet_chunk_list_id)) == 0)

    def test_read_obeys_trim(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"a": i} for i in xrange(100)])
        trim_rows("//tmp/t", 0, 30)
        assert select_rows("a from [//tmp/t]") == [{"a": i} for i in xrange(30, 100)]

    def test_make_static_after_trim(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(0, 100)])
        write_table("<append=true>//tmp/t", [{"a": j * 10} for j in xrange(0, 100)])

        alter_table("//tmp/t", dynamic=True)
        self.sync_mount_table("//tmp/t")

        trim_rows("//tmp/t", 0, 110)
        sleep(0.2)

        self.sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", dynamic=False)

        assert read_table("//tmp/t") == [{"a": j * 10, "b": None, "c": None} for j in xrange(0, 100)]

    def test_trimmed_rows_perserved_on_unmount(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", dynamic=False)

        write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(0, 100)])
        write_table("<append=true>//tmp/t", [{"a": j} for j in xrange(100, 300)])

        alter_table("//tmp/t", dynamic=True)
        self.sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] between 110 and 120") == [{"a": j} for j in xrange(110, 121)]

        trim_rows("//tmp/t", 0, 100)

        sleep(0.2)

        self.sync_unmount_table("//tmp/t")

        assert get("//tmp/t/@resource_usage/chunk_count") == 1
        chunk_list_id = get("//tmp/t/@chunk_list_id")
        assert get("#{0}/@statistics/row_count".format(chunk_list_id)) == 200
        assert get("//tmp/t/@tablets/0/flushed_row_count") == 300
        assert get("//tmp/t/@tablets/0/trimmed_row_count") == 100

        self.sync_mount_table("//tmp/t")
        assert select_rows("a from [//tmp/t] where [$tablet_index] = 0 and [$row_index] between 110 and 120") == [{"a": j} for j in xrange(110, 121)]

    def test_trim_optimizes_chunk_list(self):
        self.sync_create_cells(1)
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
            self.sync_mount_table("//tmp/t")
            trim_rows("//tmp/t", 0, trimmed_row_count)
            sleep(0.2)
            self.sync_unmount_table("//tmp/t")

        _check(20, 0, chunk_ids)
        _trim(3)
        _check(20, 3, chunk_ids[3:])
        _trim(18)
        _check(2, 0, chunk_ids[18:])

    def test_reshard_adds_tablets(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", 5)
        self.sync_mount_table("//tmp/t")
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}, {"$tablet_index": i, "a": i + 100}])
            trim_rows("//tmp/t", i, 1)
        self.sync_unmount_table("//tmp/t")
        reshard_table("//tmp/t", 5, first_tablet_index=1, last_tablet_index=3)
        self.sync_mount_table("//tmp/t")
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

    def test_reshard_joins_tablets(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", 5)
        self.sync_mount_table("//tmp/t")
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}, {"$tablet_index": i, "a": i + 100}])
            if i < 2 or i > 3:
                trim_rows("//tmp/t", i, 1)
        self.sync_unmount_table("//tmp/t")
        reshard_table("//tmp/t", 2, first_tablet_index=1, last_tablet_index=3)
        self.sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 4
        for i in xrange(4):
            tablet = tablets[i]
            print i, '->', tablet
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

    def test_reshard_join_fails_on_trimmed_rows(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        reshard_table("//tmp/t", 2)
        self.sync_mount_table("//tmp/t")
        for i in xrange(2):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
            trim_rows("//tmp/t", i, 1)
        self.sync_unmount_table("//tmp/t")
        with pytest.raises(YtError): reshard_table("//tmp/t", 1)

    def test_reshard_after_trim(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        self.sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 1)
        self.sync_unmount_table("//tmp/t")

        def _verify(expected_flushed, expected_trimmed):
            tablet = get("//tmp/t/@tablets/0")
            assert tablet["flushed_row_count"] == expected_flushed
            assert tablet["trimmed_row_count"] == expected_trimmed

        _verify(1, 1)
        reshard_table("//tmp/t", 1)
        _verify(1, 1)
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        self.sync_unmount_table("//tmp/t")
        _verify(2, 1)

    def test_freeze_empty(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        self.sync_freeze_table("//tmp/t")
        with pytest.raises(YtError): insert_rows("//tmp/t", [{"a": 0}])
        self.sync_unfreeze_table("//tmp/t")
        self.sync_unmount_table("//tmp/t")

    def test_change_commit_ordering(self):
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@commit_ordering", "weak")
        set("//tmp/t/@commit_ordering", "strong")
        with pytest.raises(YtError): set("//tmp/t/@commit_ordering", "cool")

    def test_no_commit_ordering_change_for_mounted(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@commit_ordering", "strong")

    def test_set_commit_ordering_upon_construction(self):
        self._create_simple_table("//tmp/t", commit_ordering="strong")
        assert get("//tmp/t/@commit_ordering") == "strong"


    def test_set_tablet_count_upon_construction_fail(self):
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", tablet_count=0)
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", tablet_count=-1)
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[[]])

    def test_set_tablet_count_upon_construction_success(self):
        self._create_simple_table("//tmp/t", tablet_count=10)
        assert get("//tmp/t/@tablet_count") == 10


    def test_tablet_snapshots(self):
        self.sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]

        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

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

        actual = select_rows("a, b, c from [//tmp/t]")
        assert_items_equal(actual, rows)

    @pytest.mark.parametrize("erasure_codec", ["none", "reed_solomon_6_3", "lrc_12_2_2"])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_read_table(self, optimize_for, erasure_codec):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for, erasure_codec=erasure_codec)
        self.sync_mount_table("//tmp/t")

        rows = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i)} for i in xrange(0, 100)]
        insert_rows("//tmp/t", rows)

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")

        actual = read_table("//tmp/t")
        assert actual == rows

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory(self, mode, optimize_for):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for=optimize_for)

        set("//tmp/t/@in_memory_mode", mode)
        set("//tmp/t/@max_dynamic_store_row_count", 10)
        self.sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = self._get_tablet_leader_address(tablet_id)

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

        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert select_rows("a, b, c from [//tmp/t]") == rows1

        # Check preload after flush.
        rows2 = [{"a": i, "b": i * 0.5, "c" : "payload" + str(i + 1)} for i in xrange(0, 10)]
        insert_rows("//tmp/t", rows2)
        self.sync_flush_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert select_rows("a, b, c from [//tmp/t]") == rows1 + rows2

        # Disable in-memory mode
        self.sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", "none")
        self.sync_mount_table("//tmp/t")
        _check_preload_state("disabled")
        assert select_rows("a, b, c from [//tmp/t]") == rows1 + rows2

        # Re-enable in-memory mode
        self.sync_unmount_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", mode)
        self.sync_mount_table("//tmp/t")
        self._wait_for_in_memory_stores_preload("//tmp/t")
        _check_preload_state("complete")
        assert select_rows("a, b, c from [//tmp/t]") == rows1 + rows2

    def test_reshard_trimmed_shared_yt_6948(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", tablet_count=5)
        self.sync_mount_table("//tmp/t")
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
        self.sync_unmount_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        for i in xrange(5):
            insert_rows("//tmp/t", [{"$tablet_index": i, "a": i}])
        self.sync_unmount_table("//tmp/t")
        assert get("//tmp/t/@chunk_count") == 10
        self.sync_mount_table("//tmp/t")
        for i in xrange(5):
            trim_rows("//tmp/t", i, 1)
        wait(lambda: get("//tmp/t/@chunk_count") == 5)
        self.sync_unmount_table("//tmp/t")
        copy("//tmp/t", "//tmp/t2")
        reshard_table("//tmp/t", 1)

    def test_copy_trimmed_yt_7422(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 0}])
        self.sync_flush_table("//tmp/t")
        trim_rows("//tmp/t", 0, 1)
        self.sync_freeze_table("//tmp/t")
        copy("//tmp/t", "//tmp/t2")
        self.sync_unfreeze_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 1}])
        self.sync_freeze_table("//tmp/t")
        assert get("//tmp/t/@tablets/0/flushed_row_count") == 2

    def test_timestamp_column(self):
        self.sync_create_cells(1)
        create("table", "//tmp/t", attributes={
            "dynamic": True,
            "schema": [
                {"name": "a", "type": "string"},
                {"name": "$timestamp", "type": "uint64"}
            ]
        })
        self.sync_mount_table("//tmp/t")

        timestamp0 = generate_timestamp()
        insert_rows("//tmp/t", [{"a": "hello"}])
        insert_rows("//tmp/t", [{"a": "world"}])
        timestamp3 = generate_timestamp()

        timestamp1 = select_rows("[$timestamp] from [//tmp/t] where [$row_index] = 0")[0]["$timestamp"]
        timestamp2 = select_rows("[$timestamp] from [//tmp/t] where [$row_index] = 1")[0]["$timestamp"]

        assert timestamp0 < timestamp1
        assert timestamp1 < timestamp2
        assert timestamp2 < timestamp3

    def test_data_ttl(self):
        self.sync_create_cells(1)
        self._create_simple_table("//tmp/t", min_data_ttl=0, max_data_ttl=0, min_data_versions=0)
        self.sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"a": 0}])
        self.sync_flush_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablets/0/trimmed_row_count") == 1)

##################################################################

class TestOrderedDynamicTablesMulticell(TestOrderedDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2
