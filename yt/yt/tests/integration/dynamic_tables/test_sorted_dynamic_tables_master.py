from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import parametrize_external

from yt_commands import (
    authors, wait, create, get, set, copy, remove, ls,
    exists, insert_rows,
    select_rows, lookup_rows, delete_rows, alter_table, read_table, write_table,
    mount_table, reshard_table, generate_timestamp,
    sync_create_cells, sync_mount_table, sync_unmount_table, sync_freeze_table, sync_unfreeze_table,
    sync_reshard_table, sync_flush_table, sync_compact_table,
    get_first_chunk_id, create_dynamic_table, get_tablet_leader_address,
    raises_yt_error)

from yt_type_helpers import make_schema

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

import pytest

import time
import builtins

################################################################################


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesMountUnmountFreeze(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    @authors("babenko", "ignat")
    def test_mount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        sync_mount_table("//tmp/t")
        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1
        tablet_id = tablets[0]["tablet_id"]
        cell_id = tablets[0]["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]

    @authors("babenko")
    def test_unmount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        sync_mount_table("//tmp/t")

        tablets = get("//tmp/t/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        assert tablet["pivot_key"] == []

        sync_mount_table("//tmp/t")
        sync_unmount_table("//tmp/t")

    @authors("savrus")
    def test_mount_unmount(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows("//tmp/t", rows)
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

        sync_unmount_table("//tmp/t")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", keys)

        sync_mount_table("//tmp/t")
        actual = lookup_rows("//tmp/t", keys)
        assert_items_equal(actual, rows)

    @authors("babenko")
    def test_force_unmount_on_remove(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        assert self._find_tablet_orchid(address, tablet_id) is not None

        remove("//tmp/t")
        wait(lambda: self._find_tablet_orchid(address, tablet_id) is None)

    @authors("babenko", "levysotsky")
    def test_freeze_empty(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        sync_freeze_table("//tmp/t")
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 0}])
        sync_unfreeze_table("//tmp/t")
        sync_unmount_table("//tmp/t")

    @authors("babenko", "levysotsky")
    def test_freeze_nonempty(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        rows = [{"key": 0, "value": "test"}]
        insert_rows("//tmp/t", rows)
        sync_freeze_table("//tmp/t")
        wait(lambda: get("//tmp/t/@expected_tablet_state") == "frozen")
        assert get("//tmp/t/@chunk_count") == 1
        assert select_rows("* from [//tmp/t]") == rows
        sync_unfreeze_table("//tmp/t")
        assert select_rows("* from [//tmp/t]") == rows
        sync_unmount_table("//tmp/t")

    @authors("babenko", "levysotsky")
    def test_unmount_frozen(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        rows = [{"key": 0}]
        insert_rows("//tmp/t", rows)
        sync_freeze_table("//tmp/t")
        sync_unmount_table("//tmp/t")

    @authors("babenko", "levysotsky")
    def test_mount_as_frozen(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "2"}]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t", freeze=True)
        assert select_rows("* from [//tmp/t]") == rows

    @authors("savrus")
    def test_access_to_frozen(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "2"}]
        insert_rows("//tmp/t", rows)
        sync_freeze_table("//tmp/t")
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows
        assert select_rows("* from [//tmp/t]") == rows
        with pytest.raises(YtError):
            insert_rows("//tmp/t", rows)

    @authors("savrus")
    @parametrize_external
    def test_mount_static_table_fails(self, external):
        sync_create_cells(1)
        self._create_simple_static_table(
            "//tmp/t",
            external=external,
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
        )
        assert not get("//tmp/t/@schema/@unique_keys")
        with pytest.raises(YtError):
            alter_table("//tmp/t", dynamic=True)

    @parametrize_external
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize(
        "in_memory_mode, enable_lookup_hash_table",
        [["none", False], ["compressed", False], ["uncompressed", True]],
    )
    @authors("savrus")
    def test_mount_static_table(self, in_memory_mode, enable_lookup_hash_table, optimize_for, external):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            dynamic=False,
            optimize_for=optimize_for,
            external=external,
            schema=make_schema(
                [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                    {"name": "avalue", "type": "int64", "aggregate": "sum"},
                ],
                unique_keys=True,
            ),
        )
        rows = [{"key": i, "value": str(i), "avalue": 1} for i in range(2)]
        keys = [{"key": row["key"]} for row in rows] + [{"key": -1}, {"key": 1000}]

        start_ts = generate_timestamp()
        write_table("//tmp/t", rows)
        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@in_memory_mode", in_memory_mode)
        set("//tmp/t/@enable_lookup_hash_table", enable_lookup_hash_table)
        end_ts = generate_timestamp()

        sync_mount_table("//tmp/t")

        if in_memory_mode != "none":
            self._wait_for_in_memory_stores_preload("//tmp/t")

        assert lookup_rows("//tmp/t", keys, timestamp=start_ts) == []
        actual = lookup_rows("//tmp/t", keys)
        assert actual == rows
        actual = lookup_rows("//tmp/t", keys, timestamp=end_ts)
        assert actual == rows
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert actual == rows + [None, None]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, rows)

        rows = [{"key": i, "avalue": 1} for i in range(2)]
        insert_rows("//tmp/t", rows, aggregate=True, update=True)

        expected = [{"key": i, "value": str(i), "avalue": 2} for i in range(2)]
        actual = lookup_rows("//tmp/t", keys)
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        expected = [{"key": i, "avalue": 2} for i in range(2)]
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"])
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"], keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("key, avalue from [//tmp/t]")
        assert_items_equal(actual, expected)

        sync_unmount_table("//tmp/t")

        alter_table(
            "//tmp/t",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64", "sort_order": "ascending"},
                {"name": "nvalue", "type": "string"},
                {"name": "value", "type": "string"},
                {"name": "avalue", "type": "int64", "aggregate": "sum"},
            ],
        )

        sync_mount_table("//tmp/t")
        time.sleep(1.0)

        insert_rows("//tmp/t", rows, aggregate=True, update=True)

        expected = [{"key": i, "key2": None, "nvalue": None, "value": str(i), "avalue": 3} for i in range(2)]
        actual = lookup_rows("//tmp/t", keys)
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("* from [//tmp/t]")
        assert_items_equal(actual, expected)

        expected = [{"key": i, "avalue": 3} for i in range(2)]
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"])
        assert actual == expected
        actual = lookup_rows("//tmp/t", keys, column_names=["key", "avalue"], keep_missing_rows=True)
        assert actual == expected + [None, None]
        actual = select_rows("key, avalue from [//tmp/t]")
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_set_pivot_keys_upon_construction_fail(self):
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[])
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[[10], [20]])
        with pytest.raises(YtError):
            self._create_simple_table("//tmp/t", pivot_keys=[[], [1], [1]])

    @authors("babenko")
    def test_set_pivot_keys_upon_construction_success(self):
        self._create_simple_table("//tmp/t", pivot_keys=[[], [1], [2], [3]])
        assert get("//tmp/t/@tablet_count") == 4

    @authors("savrus")
    def test_create_table_with_invalid_schema(self):
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/t",
                attributes={
                    "dynamic": True,
                    "schema": make_schema([{"name": "key", "type": "int64", "sort_order": "ascending"}]),
                },
            )
        assert not exists("//tmp/t")

    @authors("ifsmirnov")
    def test_enable_avenues_switch(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _is_mounted_with_avenue(tablet_id):
            endpoint_id = get(f"#{tablet_id}/@node_endpoint_id")
            # Check against tablet cell type.
            return not endpoint_id.split("-")[2].endswith("2bc")

        sync_mount_table("//tmp/t")
        assert _is_mounted_with_avenue(tablet_id)
        sync_unmount_table("//tmp/t")

        set("//sys/@config/tablet_manager/use_avenues", False)
        sync_mount_table("//tmp/t")
        assert not _is_mounted_with_avenue(tablet_id)
        sync_unmount_table("//tmp/t")

        set("//sys/tablet_cell_bundles/default/@use_avenues", True)
        sync_mount_table("//tmp/t")
        assert _is_mounted_with_avenue(tablet_id)
        sync_unmount_table("//tmp/t")

        set("//sys/tablet_cell_bundles/default/@use_avenues", "some_trash")
        sync_mount_table("//tmp/t")
        assert not _is_mounted_with_avenue(tablet_id)
        sync_unmount_table("//tmp/t")

        remove("//sys/tablet_cell_bundles/default/@use_avenues")

    @authors("ifsmirnov")
    def test_avenue_mailboxes(self):
        cell_id = sync_create_cells(1)[0]
        self._create_simple_table("//tmp/t")

        if self.NUM_SECONDARY_MASTER_CELLS > 0:
            cell_tag = get("//tmp/t/@external_cell_tag")
            master_instance = ls(f"//sys/secondary_masters/{cell_tag}")[0]
            master_orchid_path = f"//sys/secondary_masters/{cell_tag}/{master_instance}/orchid"
        else:
            master_instance = ls("//sys/primary_masters")[0]
            master_orchid_path = f"//sys/primary_masters/{master_instance}/orchid"

        tablet_cell_orchid_path = f"//sys/tablet_cells/{cell_id}/orchid"

        def _get_master_orchid(path):
            return get(master_orchid_path + "/" + path)

        def _get_cell_orchid(path):
            return get(tablet_cell_orchid_path + "/" + path)

        sync_mount_table("//tmp/t")
        node_endpoint = list(_get_master_orchid("hive/avenue_mailboxes"))[0]
        master_endpoint = list(_get_master_orchid("hive/avenue_mailboxes"))[0]
        assert node_endpoint.split("-")[-1] == master_endpoint.split("-")[-1]

        sync_unmount_table("//tmp/t")
        assert len(_get_master_orchid("hive/avenue_mailboxes")) == 0
        wait(lambda: len(_get_cell_orchid("hive/avenue_mailboxes")) == 0)

        sync_mount_table("//tmp/t")
        assert len(_get_master_orchid("hive/avenue_mailboxes")) == 1
        assert len(_get_cell_orchid("hive/avenue_mailboxes")) == 1

        sync_unmount_table("//tmp/t", force=True)
        assert len(_get_master_orchid("hive/avenue_mailboxes")) == 0
        wait(lambda: len(_get_cell_orchid("hive/avenue_mailboxes")) == 0)


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesMountUnmountFreezeMulticell(TestSortedDynamicTablesMountUnmountFreeze):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesMountUnmountFreezeRpcProxy(TestSortedDynamicTablesMountUnmountFreeze):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesMountUnmountFreezePortal(TestSortedDynamicTablesMountUnmountFreezeMulticell):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_PORTAL = True


################################################################################


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesCopyReshard(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    def _prepare_copy(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t1")
        sync_reshard_table("//tmp/t1", [[]] + [[i * 100] for i in range(10)])

    @authors("babenko")
    def test_copy_failure(self):
        self._prepare_copy()
        sync_mount_table("//tmp/t1")
        with pytest.raises(YtError):
            copy("//tmp/t1", "//tmp/t2")

    @authors("babenko")
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

    @pytest.mark.parametrize(
        "unmount_func, mount_func, unmounted_state",
        [
            [sync_unmount_table, sync_mount_table, "unmounted"],
            [sync_freeze_table, sync_unfreeze_table, "frozen"],
        ],
    )
    @authors("babenko", "levysotsky")
    def test_copy_simple(self, unmount_func, mount_func, unmounted_state):
        self._prepare_copy()
        sync_mount_table("//tmp/t1")
        rows = [{"key": i * 100 - 50} for i in range(10)]
        insert_rows("//tmp/t1", rows)
        unmount_func("//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t1/@tablet_state") == unmounted_state
        assert get("//tmp/t2/@tablet_state") == "unmounted"
        mount_func("//tmp/t1")
        sync_mount_table("//tmp/t2")
        assert_items_equal(select_rows("key from [//tmp/t1]"), rows)
        assert_items_equal(select_rows("key from [//tmp/t2]"), rows)

    @pytest.mark.parametrize(
        "unmount_func, mount_func, unmounted_state",
        [
            [sync_unmount_table, sync_mount_table, "unmounted"],
            [sync_freeze_table, sync_unfreeze_table, "frozen"],
        ],
    )
    @authors("babenko")
    def test_copy_and_fork(self, unmount_func, mount_func, unmounted_state):
        self._prepare_copy()
        sync_mount_table("//tmp/t1")
        rows = [{"key": i * 100 - 50} for i in range(10)]
        insert_rows("//tmp/t1", rows)
        unmount_func("//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t1/@tablet_state") == unmounted_state
        assert get("//tmp/t2/@tablet_state") == "unmounted"
        mount_func("//tmp/t1")
        sync_mount_table("//tmp/t2")
        ext_rows1 = [{"key": i * 100 - 51} for i in range(10)]
        ext_rows2 = [{"key": i * 100 - 52} for i in range(10)]
        insert_rows("//tmp/t1", ext_rows1)
        insert_rows("//tmp/t2", ext_rows2)
        assert_items_equal(select_rows("key from [//tmp/t1]"), rows + ext_rows1)
        assert_items_equal(select_rows("key from [//tmp/t2]"), rows + ext_rows2)

    @authors("babenko")
    def test_copy_and_compact(self):
        self._prepare_copy()
        sync_mount_table("//tmp/t1")
        rows = [{"key": i * 100 - 50} for i in range(10)]
        insert_rows("//tmp/t1", rows)
        sync_unmount_table("//tmp/t1")
        copy("//tmp/t1", "//tmp/t2")
        sync_mount_table("//tmp/t1")
        sync_mount_table("//tmp/t2")

        original_chunk_ids1 = builtins.set(get("//tmp/t1/@chunk_ids"))
        original_chunk_ids2 = builtins.set(get("//tmp/t2/@chunk_ids"))
        assert original_chunk_ids1 == original_chunk_ids2

        ext_rows1 = [{"key": i * 100 - 51} for i in range(10)]
        ext_rows2 = [{"key": i * 100 - 52} for i in range(10)]
        insert_rows("//tmp/t1", ext_rows1)
        insert_rows("//tmp/t2", ext_rows2)

        sync_compact_table("//tmp/t1")
        sync_compact_table("//tmp/t2")

        compacted_chunk_ids1 = builtins.set(get("//tmp/t1/@chunk_ids"))
        compacted_chunk_ids2 = builtins.set(get("//tmp/t2/@chunk_ids"))
        assert len(compacted_chunk_ids1.intersection(compacted_chunk_ids2)) == 0

        assert_items_equal(select_rows("key from [//tmp/t1]"), rows + ext_rows1)
        assert_items_equal(select_rows("key from [//tmp/t2]"), rows + ext_rows2)

    @authors("babenko", "ignat")
    def test_reshard_unmounted(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "l", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "int64"},
                ],
            },
        )

        sync_reshard_table("//tmp/t", [[]])
        assert self._get_pivot_keys("//tmp/t") == [[]]

        sync_reshard_table("//tmp/t", [[], [100]])
        assert self._get_pivot_keys("//tmp/t") == [[], [100]]

        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[], []])
        assert self._get_pivot_keys("//tmp/t") == [[], [100]]

        sync_reshard_table("//tmp/t", [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[101]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[300]], first_tablet_index=3, last_tablet_index=3)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [200]]

        sync_reshard_table("//tmp/t", [[100], [150], [200]], first_tablet_index=1, last_tablet_index=2)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[100], [100]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

        with pytest.raises(YtError):
            reshard_table("//tmp/t", [[], [100, 200]])
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [150], [200]]

    @authors("babenko", "levysotsky")
    def test_reshard_partly_unmounted(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [100], [200], [300]])
        sync_mount_table("//tmp/t")
        with pytest.raises(YtError):
            reshard_table(
                "//tmp/t",
                [[100], [250], [300]],
                first_tablet_index=1,
                last_tablet_index=3,
            )
        sync_unmount_table("//tmp/t", first_tablet_index=1, last_tablet_index=3)
        sync_reshard_table("//tmp/t", [[100], [250], [300]], first_tablet_index=1, last_tablet_index=3)
        assert self._get_pivot_keys("//tmp/t") == [[], [100], [250], [300]]

    @authors("savrus", "levysotsky")
    def test_reshard_tablet_count(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1]])
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": "A" * 256} for i in range(2)])
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")
        sync_unmount_table("//tmp/t")
        chunks = get("//tmp/t/@chunk_ids")
        assert len(chunks) == 2
        sync_reshard_table("//tmp/t", [[]])
        assert self._get_pivot_keys("//tmp/t") == [[]]
        sync_reshard_table("//tmp/t", 2)
        assert self._get_pivot_keys("//tmp/t") == [[], [1]]

    @authors("savrus")
    def test_reshard_data(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", optimize_for="scan")
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(3)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        self._reshard_with_retries("//tmp/t", [[], [1]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        self._reshard_with_retries("//tmp/t", [[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        self._reshard_with_retries("//tmp/t", [[]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @authors("savrus")
    def test_reshard_single_chunk(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", enable_compaction_and_partitioning=False)
        sync_mount_table("//tmp/t")

        def reshard(pivots):
            sync_unmount_table("//tmp/t")
            sync_reshard_table("//tmp/t", pivots)
            sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(3)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        reshard([[], [1]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        reshard([[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

        reshard([[]])
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)

    @authors("ifsmirnov", "savrus")
    def test_reshard_with_uncovered_chunk(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@min_data_ttl", 0)
        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i)} for i in range(3)]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        chunk_id = get_first_chunk_id("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1], [2]])

        def get_tablet_chunk_lists():
            return get("#{0}/@child_ids".format(get("//tmp/t/@chunk_list_id")))

        mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        wait(lambda: get("//tmp/t/@tablets/1/state") == "mounted")
        delete_rows("//tmp/t", [{"key": 1}])
        sync_unmount_table("//tmp/t")

        set("//tmp/t/@forced_compaction_revision", 1)
        mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        wait(lambda: get("//tmp/t/@tablets/1/state") == "mounted")
        tablet_chunk_lists = get_tablet_chunk_lists()
        wait(lambda: chunk_id not in get("#{0}/@child_ids".format(tablet_chunk_lists[1])))

        sync_unmount_table("//tmp/t")

        def get_chunk_under_chunk_view(chunk_view_id):
            return get("#{0}/@chunk_id".format(chunk_view_id))

        tablet_chunk_lists = get_tablet_chunk_lists()
        assert get_chunk_under_chunk_view(get("#{0}/@child_ids/0".format(tablet_chunk_lists[0]))) == chunk_id
        assert chunk_id not in get("#{0}/@child_ids".format(tablet_chunk_lists[1]))
        assert get_chunk_under_chunk_view(get("#{0}/@child_ids/0".format(tablet_chunk_lists[2]))) == chunk_id

        sync_reshard_table("//tmp/t", [[]])

        # Avoiding compaction.
        sync_mount_table("//tmp/t", freeze=True)
        assert list(lookup_rows("//tmp/t", [{"key": i} for i in range(3)])) == [
            {"key": i, "value": str(i)} for i in (0, 2)
        ]

    @authors("ifsmirnov")
    def test_reshard_uniform(self):
        create_dynamic_table("//tmp/t1", schema=[
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ])
        reshard_table("//tmp/t1", 2, uniform=True)
        assert get("//tmp/t1/@pivot_keys") == [[], [0]]
        reshard_table("//tmp/t1", 5, uniform=True)
        assert get("//tmp/t1/@pivot_keys") == [
            [], [-5534023222112865485], [-1844674407370955162], [1844674407370955161], [5534023222112865484]
        ]

        create_dynamic_table("//tmp/t2", schema=[
            {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(key)"},
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ])
        reshard_table("//tmp/t2", 10, uniform=True)
        pivots = [p[0] for p in get("//tmp/t2/@pivot_keys")[1:]]
        assert pivots == [2**64 * i // 10 for i in range(1, 10)]

        create_dynamic_table("//tmp/t3", schema=[
            {"name": "key", "type": "double", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ])
        with raises_yt_error():
            reshard_table("//tmp/t3", 10, uniform=True)

        create_dynamic_table("//tmp/t4", schema=[
            {"name": "key", "type": "int16", "sort_order": "ascending"},
            {"name": "value", "type": "string"}
        ])
        reshard_table("//tmp/t4", 5, uniform=True)
        assert get("//tmp/t4/@pivot_keys") == [[], [-19661], [-6554], [6553], [19660]]

    @authors("max42", "savrus")
    def test_alter_table_fails(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        # We have to insert at least one row to the table because any
        # valid schema can be set for an empty table without any checks.
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        sync_unmount_table("//tmp/t")
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t",
                schema=[
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            )
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t",
                schema=[
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            )
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t",
                schema=[
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value1", "type": "string"},
                ],
            )

        self._create_table_with_computed_column("//tmp/t1")
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"key1": 1, "value": "test"}])
        sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t1",
                schema=[
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            )
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t1",
                schema=[
                    {
                        "name": "key1",
                        "type": "int64",
                        "expression": "key2 * 100 + 3",
                        "sort_order": "ascending",
                    },
                    {"name": "key2", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            )
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t1",
                schema=[
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {
                        "name": "key2",
                        "type": "int64",
                        "expression": "key1 * 100",
                        "sort_order": "ascending",
                    },
                    {"name": "value", "type": "string"},
                ],
            )
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t1",
                schema=[
                    {"name": "key1", "type": "int64", "sort_order": "ascending"},
                    {
                        "name": "key2",
                        "type": "int64",
                        "expression": "key1 * 100 + 3",
                        "sort_order": "ascending",
                    },
                    {
                        "name": "key3",
                        "type": "int64",
                        "expression": "key1 * 100 + 3",
                        "sort_order": "ascending",
                    },
                    {"name": "value", "type": "string"},
                ],
            )

        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]},
        )
        with pytest.raises(YtError):
            alter_table("//tmp/t2", dynamic=True)
        with pytest.raises(YtError):
            alter_table(
                "//tmp/t2",
                schema=[
                    {"name": "key", "type": "any", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            )
        with pytest.raises(YtError):
            alter_table("//tmp/t2", dynamic=True)

    @authors("gritukan")
    def test_alter_key_column(self):
        old_schema = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": "ascending",
                },
                {"name": "value", "type": "string", "required": False},
            ]
        )
        new_schema = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": "ascending",
                },
                {
                    "name": "x",
                    "type": "int64",
                    "required": False,
                    "sort_order": "ascending",
                },
                {"name": "value", "type": "string", "required": False},
            ]
        )
        bad_schema_1 = make_schema(
            [
                {
                    "name": "x",
                    "type": "int64",
                    "required": False,
                    "sort_order": "ascending",
                },
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": "ascending",
                },
                {"name": "value", "type": "string", "required": False},
            ]
        )
        bad_schema_2 = make_schema(
            [
                {
                    "name": "key",
                    "type": "int64",
                    "required": False,
                    "sort_order": "ascending",
                },
                {"name": "value", "type": "string", "required": False},
                {
                    "name": "x",
                    "type": "int64",
                    "required": False,
                    "sort_order": "ascending",
                },
            ]
        )

        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        insert_rows("//tmp/t", [{"key": 2, "value": "2"}])
        sync_unmount_table("//tmp/t")

        with pytest.raises(YtError):
            alter_table("//tmp/t", schema=bad_schema_1)

        with pytest.raises(YtError):
            alter_table("//tmp/t", schema=bad_schema_2)

        alter_table("//tmp/t", schema=new_schema)

        sync_mount_table("//tmp/t")
        assert read_table("//tmp/t") == [
            {"key": 1, "value": "1", "x": yson.YsonEntity()},
            {"key": 2, "value": "2", "x": yson.YsonEntity()},
        ]
        sync_unmount_table("//tmp/t")

        with pytest.raises(YtError):
            alter_table("//tmp/t", schema=old_schema)


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesCopyReshardMulticell(TestSortedDynamicTablesCopyReshard):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesCopyReshardRpcProxy(TestSortedDynamicTablesCopyReshard):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesCopyReshardPortal(TestSortedDynamicTablesCopyReshardMulticell):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_PORTAL = True
