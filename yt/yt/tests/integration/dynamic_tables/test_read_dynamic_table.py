from yt_dynamic_tables_base import DynamicTablesBase
from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import parametrize_external

from yt_commands import (
    authors, wait, get, set, exists, start_transaction, abort_transaction, lock,
    insert_rows, select_rows, alter_table,
    read_table, write_table, generate_timestamp, sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table, sync_flush_table,
    sync_compact_table, get_driver, make_externalized_tx_id, print_debug)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import time

################################################################################


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesReadTable(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    @authors("psushin")
    def test_read_invalid_limits(self):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        with pytest.raises(YtError):
            read_table("//tmp/t[#5:]")
        with pytest.raises(YtError):
            read_table("<ranges=[{lower_limit={offset = 0};upper_limit={offset = 1}}]>//tmp/t")

    @authors("savrus")
    @pytest.mark.parametrize("erasure_codec", ["none", "reed_solomon_6_3", "lrc_12_2_2"])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.ignore_in_opensource_ci
    def test_read_table(self, optimize_for, erasure_codec):
        sync_create_cells(1)

        self._create_simple_table("//tmp/t", optimize_for=optimize_for, erasure_codec=erasure_codec)
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows1)
        sync_freeze_table("//tmp/t")

        assert read_table("//tmp/t") == rows1
        assert get("//tmp/t/@chunk_count") == 1

        ts = generate_timestamp()

        sync_unfreeze_table("//tmp/t")
        rows2 = [{"key": i, "value": str(i + 1)} for i in range(10)]
        insert_rows("//tmp/t", rows2)
        sync_unmount_table("//tmp/t")

        assert read_table("<timestamp=%s>//tmp/t" % (ts)) == rows1
        assert get("//tmp/t/@chunk_count") == 2

    @authors("savrus")
    def test_read_snapshot_lock(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", enable_dynamic_store_read=False)
        sync_mount_table("//tmp/t")

        table_id = get("//tmp/t/@id")

        def _find_driver():
            for i in range(self.NUM_SECONDARY_MASTER_CELLS):
                driver = get_driver(i + 1)
                if exists("#{0}".format(table_id), driver=driver):
                    return driver
            return None

        driver = _find_driver()

        def _multicell_lock(table, *args, **kwargs):
            lock(table, *args, **kwargs)
            if "tx" in kwargs:
                expected_tx = kwargs["tx"]
                if get(f"#{table_id}/@foreign", driver=driver):
                    expected_tx = make_externalized_tx_id(expected_tx, get(f"#{table_id}/@native_cell_tag"))
                    print_debug("expected transaction_id:", expected_tx)
            else:
                expected_tx = None

            def _check():
                locks = get("#{0}/@locks".format(table_id), driver=driver)
                if expected_tx is not None:
                    for item in locks:
                        if item["transaction_id"] == expected_tx:
                            return True
                    return False
                else:
                    return len(locks) > 0

            wait(_check)

        def get_chunk_tree(path):
            root_chunk_list_id = get(path + "/@chunk_list_id")
            root_chunk_list = get("#" + root_chunk_list_id + "/@")
            tablet_chunk_lists = [get("#" + x + "/@") for x in root_chunk_list["child_ids"]]
            assert all([root_chunk_list_id in chunk_list["parent_ids"] for chunk_list in tablet_chunk_lists])
            # Validate against @chunk_count just to make sure that statistics arrive
            # from secondary master to primary one.
            assert get(path + "/@chunk_count") == sum(
                [len(chunk_list["child_ids"]) for chunk_list in tablet_chunk_lists]
            )

            return root_chunk_list, tablet_chunk_lists

        def verify_chunk_tree_refcount(path, root_ref_count, tablet_ref_counts):
            root, tablets = get_chunk_tree(path)
            assert root["ref_counter"] == root_ref_count
            assert [tablet["ref_counter"] for tablet in tablets] == tablet_ref_counts

        verify_chunk_tree_refcount("//tmp/t", 1, [1])

        tx = start_transaction(timeout=60000, sticky=True)
        _multicell_lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1])

        rows1 = [{"key": i, "value": str(i)} for i in range(0, 10, 2)]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1])
        assert read_table("//tmp/t") == rows1
        assert read_table("//tmp/t", tx=tx) == []

        with pytest.raises(YtError):
            read_table("<timestamp={0}>//tmp/t".format(generate_timestamp()), tx=tx)

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1])

        tx = start_transaction(timeout=60000, sticky=True)
        _multicell_lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1])

        sync_reshard_table("//tmp/t", [[], [5]])
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

        tx = start_transaction(timeout=60000, sticky=True)
        _multicell_lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1, 1])

        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        rows2 = [{"key": i, "value": str(i)} for i in range(1, 5, 2)]
        insert_rows("//tmp/t", rows2)
        sync_unmount_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 2])
        assert_items_equal(read_table("//tmp/t"), rows1 + rows2)
        time.sleep(16)
        assert read_table("//tmp/t", tx=tx) == rows1

        sync_mount_table("//tmp/t")
        rows3 = [{"key": i, "value": str(i)} for i in range(5, 10, 2)]
        insert_rows("//tmp/t", rows3)
        sync_unmount_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])
        assert_items_equal(read_table("//tmp/t"), rows1 + rows2 + rows3)
        assert read_table("//tmp/t", tx=tx) == rows1

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

        tx = start_transaction(timeout=60000, sticky=True)
        _multicell_lock("//tmp/t", mode="snapshot", tx=tx)
        verify_chunk_tree_refcount("//tmp/t", 2, [1, 1])

        sync_mount_table("//tmp/t")
        sync_compact_table("//tmp/t")
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])
        assert_items_equal(read_table("//tmp/t"), rows1 + rows2 + rows3)
        assert_items_equal(read_table("//tmp/t", tx=tx), rows1 + rows2 + rows3)

        abort_transaction(tx)
        verify_chunk_tree_refcount("//tmp/t", 1, [1, 1])

    @pytest.mark.ignore_in_opensource_ci
    @authors("savrus")
    def test_read_table_ranges(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", pivot_keys=[[], [5]])
        set("//tmp/t/@min_compaction_store_count", 5)
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i)} for i in range(10)]
        insert_rows("//tmp/t", rows1)
        sync_flush_table("//tmp/t")

        rows2 = [{"key": i, "value": str(i + 1)} for i in range(1, 5)]
        insert_rows("//tmp/t", rows2)
        sync_flush_table("//tmp/t")

        rows3 = [{"key": i, "value": str(i + 2)} for i in range(5, 9)]
        insert_rows("//tmp/t", rows3)
        sync_flush_table("//tmp/t")

        rows4 = [{"key": i, "value": str(i + 3)} for i in range(0, 3)]
        insert_rows("//tmp/t", rows4)
        sync_flush_table("//tmp/t")

        rows5 = [{"key": i, "value": str(i + 4)} for i in range(7, 10)]
        insert_rows("//tmp/t", rows5)
        sync_flush_table("//tmp/t")

        sync_freeze_table("//tmp/t")

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

    @pytest.mark.ignore_in_opensource_ci
    @authors("savrus")
    @parametrize_external
    def test_read_table_when_chunk_crosses_tablet_boundaries(self, external):
        self._create_simple_static_table("//tmp/t", external=external)
        rows = [{"key": i, "value": str(i)} for i in range(6)]
        write_table("//tmp/t", rows)
        alter_table("//tmp/t", dynamic=True)

        def do_test():
            for i in range(6):
                assert read_table("//tmp/t[{0}:{1}]".format(i, i + 1)) == rows[i:i + 1]
            for i in range(0, 6, 2):
                assert read_table("//tmp/t[{0}:{1}]".format(i, i + 2)) == rows[i:i + 2]
            for i in range(1, 6, 2):
                assert read_table("//tmp/t[{0}:{1}]".format(i, i + 2)) == rows[i:i + 2]

        do_test()
        sync_reshard_table("//tmp/t", [[], [2], [4]])
        do_test()

    @authors("babenko", "levysotsky", "savrus")
    def test_write_table(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            write_table("//tmp/t", [{"key": 1, "value": 2}])


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesReadTableMulticell(TestSortedDynamicTablesReadTable):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesReadTableRpcProxy(TestSortedDynamicTablesReadTable):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


@pytest.mark.enabled_multidaemon
class TestSortedDynamicTablesReadTablePortal(TestSortedDynamicTablesReadTableMulticell):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_PORTAL = True


@pytest.mark.enabled_multidaemon
class TestReadDynamicTableFormats(DynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    @pytest.mark.ignore_in_opensource_ci
    @authors("aleexfi")
    def test_read_table_with_complex_schema(self):
        schema = [
            {
                "name": "key",
                "type_v3": "string",
                "sort_order": "ascending"
            },
            {
                "name": "value",
                "type_v3": {
                    "type_name": "struct",
                    "members": [
                        {
                            "name": "foo",
                            "type": "int32",
                        },
                        {
                            "name": "bar",
                            "type": "int32",
                        }
                    ],
                }
            },
            {
                "name": "mapping",
                "type_v3": {
                    "type_name": "dict",
                    "key": "int64",
                    "value": "int64",
                }
            }
        ]

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        items = [
            {
                "key": "foo",
                "value": {
                    "foo": 1,
                    "bar": 2,
                },
                "mapping": [
                    (1, 2),
                    (3, 4),
                ],
            },
            {
                "key": "bar",
                "value": {
                    "foo": 42,
                    "bar": 42,
                },
                "mapping": [
                    (42, 42),
                    (43, 42),
                ],
            }
        ]

        insert_rows("//tmp/t", items)
        sync_flush_table("//tmp/t")

        rows = select_rows("* from [//tmp/t] order by key limit 2")
        assert len(rows) == len(rows)
        for row, expected in zip(rows, items[::-1]):
            assert_items_equal(row, expected)
