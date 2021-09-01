from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt.test_helpers.profiler import Profiler

from yt_commands import (
    authors, wait, create, exists, get, set, set_banned_flag, insert_rows, remove, select_rows,
    lookup_rows, delete_rows, remount_table,
    alter_table, read_table, map, reshard_table, sync_create_cells,
    sync_mount_table, sync_unmount_table, sync_flush_table, sync_compact_table, gc_collect)

from yt.common import YtError
from yt.test_helpers import assert_items_equal
from yt.packages.six.moves import xrange

import pytest

import time

import __builtin__

################################################################################


class TestSortedDynamicTablesHunks(TestSortedDynamicTablesBase):
    NUM_TEST_PARTITIONS = 4

    NUM_SCHEDULERS = 1

    SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "enable_hunks": True
        }
    }

    def _get_table_schema(self, schema, max_inline_hunk_size):
        schema[1]["max_inline_hunk_size"] = max_inline_hunk_size
        return schema

    def _create_table(self, optimize_for="lookup", max_inline_hunk_size=10, schema=SCHEMA):
        self._create_simple_table("//tmp/t",
                                  schema=self._get_table_schema(schema, max_inline_hunk_size),
                                  enable_dynamic_store_read=False,
                                  hunk_chunk_reader={
                                      "max_hunk_count_per_read": 2,
                                      "max_total_hunk_length_per_read": 60,
                                  },
                                  hunk_chunk_writer={
                                      "desired_block_size": 50
                                  },
                                  min_hunk_compaction_total_hunk_length=1,
                                  max_hunk_compaction_garbage_ratio=0.5,
                                  enable_lsm_verbose_logging=True,
                                  optimize_for=optimize_for)

    def _get_store_chunk_ids(self, path):
        chunk_ids = get(path + "/@chunk_ids")
        return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id)) == "table"]

    def _get_hunk_chunk_ids(self, path):
        chunk_ids = get(path + "/@chunk_ids")
        return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id)) == "hunk"]

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_flush_inline(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in xrange(10)]
        rows = [{"key": i, "value": "value" + str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(select_rows("* from [//tmp/t] where value = \"{}\"".format(rows[0]["value"])), [rows[0]])
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 0

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_ids[0])) == []

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_flush_to_hunk_chunk(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in xrange(10)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]

        assert get("#{}/@ref_counter".format(store_chunk_id)) == 1

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id)) == [
            {"chunk_id": hunk_chunk_ids[0], "hunk_count": 10, "total_hunk_length": 260}
        ]
        assert get("#{}/@ref_counter".format(hunk_chunk_id)) == 2
        assert get("#{}/@data_weight".format(hunk_chunk_id)) == 260
        assert get("#{}/@uncompressed_data_size".format(hunk_chunk_id)) == 340
        assert get("#{}/@compressed_data_size".format(hunk_chunk_id)) == 340

        assert get("//tmp/t/@chunk_format_statistics/hunk_default/chunk_count") == 1

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        assert_items_equal(select_rows("* from [//tmp/t] where value = \"{}\"".format(rows[0]["value"])), [rows[0]])

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(store_chunk_id)) and not exists("#{}".format(hunk_chunk_id)))

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_compaction(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        keys1 = [{"key": i} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows1)
        assert_items_equal(lookup_rows("//tmp/t", keys1), rows1)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        rows2 = [{"key": i, "value": "value" + str(i) + "y" * 20} for i in xrange(10, 20)]
        keys2 = [{"key": i} for i in xrange(10, 20)]
        insert_rows("//tmp/t", rows2)
        select_rows("* from [//tmp/t]")
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

        sync_unmount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@chunk_count") == 4)

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 2
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 2

        store_chunk_id1 = store_chunk_ids[0]
        store_chunk_id2 = store_chunk_ids[1]
        hunk_chunk_id1 = get("#{}/@hunk_chunk_refs/0/chunk_id".format(store_chunk_id1))
        hunk_chunk_id2 = get("#{}/@hunk_chunk_refs/0/chunk_id".format(store_chunk_id2))
        assert_items_equal(hunk_chunk_ids, [hunk_chunk_id1, hunk_chunk_id2])

        if get("#{}/@hunk_chunk_refs/0/total_hunk_length".format(store_chunk_id1)) \
                > get("#{}/@hunk_chunk_refs/0/total_hunk_length".format(store_chunk_id2)):
            store_chunk_id1, store_chunk_id2 = store_chunk_id2, store_chunk_id1
            hunk_chunk_id1, hunk_chunk_id2 = hunk_chunk_id2, hunk_chunk_id1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id1)) == [
            {"chunk_id": hunk_chunk_id1, "hunk_count": 10, "total_hunk_length": 260}
        ]
        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id2)) == [
            {"chunk_id": hunk_chunk_id2, "hunk_count": 10, "total_hunk_length": 270}
        ]

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 3)

        compacted_store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(compacted_store_chunk_ids) == 1
        compacted_store_id = compacted_store_chunk_ids[0]

        assert_items_equal(get("#{}/@hunk_chunk_refs".format(compacted_store_id)), [
            {"chunk_id": hunk_chunk_id1, "hunk_count": 10, "total_hunk_length": 260},
            {"chunk_id": hunk_chunk_id2, "hunk_count": 10, "total_hunk_length": 270},
        ])

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

    @authors("babenko")
    def test_hunk_sweep(self):
        sync_create_cells(1)
        self._create_table()

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        keys1 = [{"key": i} for i in xrange(10)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in xrange(10, 20)]
        keys2 = [{"key": i} for i in xrange(10, 20)]
        insert_rows("//tmp/t", rows1 + rows2)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        delete_rows("//tmp/t", keys1)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows2)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 2
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        assert_items_equal(select_rows("* from [//tmp/t]"), rows2)
        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows2)

        set("//tmp/t/@min_data_ttl", 60000)
        sync_compact_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 2)
        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        set("//tmp/t/@min_data_ttl", 0)
        set("//tmp/t/@min_data_versions", 1)
        sync_compact_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 1)
        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 0

    @authors("babenko")
    def test_reshard(self):
        sync_create_cells(1)
        self._create_table()

        sync_mount_table("//tmp/t")
        # This chunk will intersect both of new tablets and will produce chunk views.
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in [0, 10, 20, 30, 40, 50]]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id1 = store_chunk_ids[0]

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id1 = hunk_chunk_ids[0]

        sync_mount_table("//tmp/t")
        # This chunk will be fully contained in the first tablet.
        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in [11, 12, 13]]
        insert_rows("//tmp/t", rows2)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 2
        store_chunk_id2 = list(__builtin__.set(store_chunk_ids) - __builtin__.set([store_chunk_id1]))[0]

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 2
        hunk_chunk_id2 = list(__builtin__.set(hunk_chunk_ids) - __builtin__.set([hunk_chunk_id1]))[0]

        gc_collect()
        assert get("#{}/@ref_counter".format(store_chunk_id1)) == 1
        assert get("#{}/@ref_counter".format(hunk_chunk_id1)) == 2
        assert get("#{}/@ref_counter".format(store_chunk_id2)) == 1
        assert get("#{}/@ref_counter".format(hunk_chunk_id2)) == 2

        reshard_table("//tmp/t", [[], [30]])

        gc_collect()
        assert get("#{}/@ref_counter".format(store_chunk_id1)) == 2
        assert get("#{}/@ref_counter".format(hunk_chunk_id1)) == 3
        assert get("#{}/@ref_counter".format(store_chunk_id2)) == 1
        assert get("#{}/@ref_counter".format(hunk_chunk_id2)) == 2

        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_compaction_writes_hunk_chunk(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for, max_inline_hunk_size=1000)

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in xrange(10, 20)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=10))

        chunk_ids_before_compaction = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids_before_compaction) == 1
        chunk_id_before_compaction = chunk_ids_before_compaction[0]
        assert get("#{}/@hunk_chunk_refs".format(chunk_id_before_compaction)) == []

        sync_mount_table("//tmp/t")

        sync_compact_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_ids") != chunk_ids_before_compaction)
        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        compacted_store_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(compacted_store_id)), [
            {"chunk_id": hunk_chunk_id, "hunk_count": 10, "total_hunk_length": 260},
        ])

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_compaction_inlines_hunks(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for, max_inline_hunk_size=10)

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in xrange(10, 20)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_unmount_table("//tmp/t")

        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=1000))

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id, "hunk_count": 10, "total_hunk_length": 260},
        ])

        sync_mount_table("//tmp/t")

        sync_compact_table("//tmp/t")

        wait(lambda: len(get("//tmp/t/@chunk_ids")) == 1)
        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id)) == []

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_compaction_rewrites_hunk_chunk(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for, max_inline_hunk_size=10)

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        keys = [{"key": i} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id0 = hunk_chunk_ids[0]
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id0, "hunk_count": 10, "total_hunk_length": 260},
        ])

        sync_mount_table("//tmp/t")
        delete_rows("//tmp/t", keys[1:])
        sync_unmount_table("//tmp/t")

        sync_mount_table("//tmp/t")

        chunk_ids_before_compaction1 = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids_before_compaction1) == 3

        set("//tmp/t/@min_data_ttl", 0)
        set("//tmp/t/@min_data_versions", 1)
        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")

        def _check1():
            chunk_ids = get("//tmp/t/@chunk_ids")
            return chunk_ids != chunk_ids_before_compaction1 and len(chunk_ids) == 2
        wait(_check1)

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id1 = hunk_chunk_ids[0]
        assert hunk_chunk_id0 == hunk_chunk_id1
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id1, "hunk_count": 1, "total_hunk_length": 26},
        ])

        chunk_ids_before_compaction2 = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids_before_compaction2) == 2

        set("//tmp/t/@forced_store_compaction_revision", 1)
        remount_table("//tmp/t")

        def _check2():
            chunk_ids = get("//tmp/t/@chunk_ids")
            return chunk_ids != chunk_ids_before_compaction2 and len(chunk_ids) == 2
        wait(_check2)

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id2 = hunk_chunk_ids[0]
        assert hunk_chunk_id1 != hunk_chunk_id2
        assert_items_equal(get("#{}/@hunk_chunk_refs".format(store_chunk_id)), [
            {"chunk_id": hunk_chunk_id2, "hunk_count": 1, "total_hunk_length": 26},
        ])
        assert get("#{}/@hunk_count".format(hunk_chunk_id2)) == 1
        assert get("#{}/@total_hunk_length".format(hunk_chunk_id2)) == 26

    @authors("ifsmirnov")
    @pytest.mark.parametrize("in_memory_mode", ["compressed", "uncompressed"])
    def test_hunks_not_counted_in_tablet_static(self, in_memory_mode):
        def _check_account_resource_usage(expected_memory_usage):
            return \
                get("//sys/accounts/tmp/@resource_usage/tablet_static_memory") == expected_memory_usage and \
                get("//sys/tablet_cell_bundles/default/@resource_usage/tablet_static_memory") == expected_memory_usage

        sync_create_cells(1)
        self._create_table()

        set("//tmp/t/@in_memory_mode", in_memory_mode)
        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        store_chunk_id = store_chunk_ids[0]
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1
        hunk_chunk_id = hunk_chunk_ids[0]

        compressed_size = get("#{}/@compressed_data_size".format(store_chunk_id))
        uncompressed_size = get("#{}/@uncompressed_data_size".format(store_chunk_id))
        hunk_compressed_size = get("#{}/@compressed_data_size".format(hunk_chunk_id))
        hunk_uncompressed_size = get("#{}/@uncompressed_data_size".format(hunk_chunk_id))

        def _validate_tablet_statistics():
            tablet_statistics = get("//tmp/t/@tablet_statistics")
            return \
                tablet_statistics["compressed_data_size"] == compressed_size + hunk_compressed_size and \
                tablet_statistics["uncompressed_data_size"] == uncompressed_size + hunk_uncompressed_size and \
                tablet_statistics["hunk_compressed_data_size"] == hunk_compressed_size and \
                tablet_statistics["hunk_uncompressed_data_size"] == hunk_uncompressed_size
        wait(_validate_tablet_statistics)

        memory_size = compressed_size if in_memory_mode == "compressed" else uncompressed_size

        assert get("//tmp/t/@tablet_statistics/memory_size") == memory_size

        wait(lambda: _check_account_resource_usage(memory_size))

        sync_unmount_table("//tmp/t")
        wait(lambda: _check_account_resource_usage(0))

    @authors("gritukan")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("hunk_type", ["inline", "chunk"])
    def test_hunks_in_operation(self, optimize_for, hunk_type):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        if hunk_type == "inline":
            rows = [{"key": i, "value": "value" + str(i)} for i in xrange(10)]
        else:
            rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        if hunk_type == "inline":
            assert len(hunk_chunk_ids) == 0
        else:
            assert len(hunk_chunk_ids) == 1

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )
        assert read_table("//tmp/t_out") == rows

    @authors("gritukan")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("hunk_type", ["inline", "chunk"])
    def test_hunks_in_operation_any_column(self, optimize_for, hunk_type):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "max_inline_chunk_size": 10},
        ]

        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for, schema=schema)
        sync_mount_table("//tmp/t")

        if hunk_type == "inline":
            rows = [{"key": i, "value": "value" + str(i)} for i in xrange(10)]
        else:
            rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        if hunk_type == "inline":
            assert len(hunk_chunk_ids) == 0
        else:
            assert len(hunk_chunk_ids) == 1

        create("table", "//tmp/t_out")
        map(
            in_="//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )
        assert read_table("//tmp/t_out") == rows

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_alter_to_hunks(self, optimize_for):
        sync_create_cells(1)
        self._create_table(optimize_for=optimize_for, max_inline_hunk_size=None)
        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 0

        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=10))

        sync_mount_table("//tmp/t")
        rows2 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10, 20)]
        insert_rows("//tmp/t", rows2)

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 2
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

    @authors("babenko")
    def test_alter_must_preserve_hunks(self):
        self._create_table()
        with pytest.raises(YtError):
            alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=None))

    @authors("akozhikhov")
    def test_hunks_with_filtered_columns(self):
        sync_create_cells(1)
        self._create_table()
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        def _check(column_names):
            row = [{column: rows[0][column] for column in column_names}]
            assert_items_equal(
                lookup_rows("//tmp/t", [{"key": 0}], column_names=column_names),
                row)
            assert_items_equal(
                select_rows("{} from [//tmp/t] where value = \"{}\"".format(
                    ", ".join(column_names),
                    rows[0]["value"])),
                row)

        _check(["key"])
        _check(["value"])
        _check(["value", "key"])

    @authors("akozhikhov")
    def test_lookup_hunks_from_suspicious_nodes(self):
        self._separate_tablet_and_data_nodes()
        sync_create_cells(1)

        self._create_table()
        set("//tmp/t/@replication_factor", self.NUM_NODES - 1)
        set("//tmp/t/@hunk_chunk_reader",
            {"periodic_update_delay": 500,
             "evict_after_successful_access_time": 1000})
        set("//tmp/t/@hunk_chunk_writer",
            {"upload_replication_factor": self.NUM_NODES - 1})
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "value" + "x" * 20}]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows

        # Banned nodes are marked as suspicious and will be avoided within next lookups.
        set_banned_flag(True, self._nodes[1:5])
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows
        # Drop CFR cache
        time.sleep(2)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows

        # Node shall not be suspicious anymore.
        set_banned_flag(False, self._nodes[1:5])
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows
        # Drop CFR cache
        time.sleep(2)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows
        assert lookup_rows("//tmp/t", [{"key": 1}]) == rows

    @authors("akozhikhov")
    def test_hunks_profiling_flush(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(5)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in xrange(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)

        chunk_data_weight = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/data_weight",
            tags={"method": "store_flush"})
        hunk_chunk_data_weight = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/hunks/data_weight",
            tags={"method": "store_flush"})

        inline_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/hunks/inline_value_count",
            tags={"column": "value", "method": "store_flush"})
        ref_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/hunks/ref_value_count",
            tags={"column": "value", "method": "store_flush"})

        sync_flush_table("//tmp/t")

        wait(lambda: chunk_data_weight.get_delta() > 0)
        wait(lambda: hunk_chunk_data_weight.get_delta() > 0)

        wait(lambda: inline_hunk_value_count.get_delta() == 10)
        wait(lambda: ref_hunk_value_count.get_delta() == 5)

    @authors("akozhikhov")
    def test_hunks_profiling_compaction(self):
        sync_create_cells(1)
        self._create_table(max_inline_hunk_size=10)
        set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(5)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in xrange(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)

        reader_hunk_chunk_transmitted = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_reader/hunks/chunk_reader_statistics/data_bytes_transmitted",
            tags={"method": "compaction"})
        reader_hunk_chunk_data_weight = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_reader/hunks/data_weight",
            tags={"method": "compaction"})

        reader_inline_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_reader/hunks/inline_value_count",
            tags={"column": "value"})
        reader_ref_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_reader/hunks/ref_value_count",
            tags={"column": "value"})

        writer_chunk_data_weight = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/data_weight",
            tags={"method": "compaction"})
        writer_hunk_chunk_data_weight = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/hunks/data_weight",
            tags={"method": "compaction"})

        writer_inline_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/hunks/inline_value_count",
            tags={"column": "value", "method": "compaction"})
        writer_ref_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="chunk_writer/hunks/ref_value_count",
            tags={"column": "value", "method": "compaction"})

        sync_unmount_table("//tmp/t")
        alter_table("//tmp/t", schema=self._get_table_schema(schema=self.SCHEMA, max_inline_hunk_size=30))
        sync_mount_table("//tmp/t")

        sync_compact_table("//tmp/t")

        wait(lambda: reader_hunk_chunk_transmitted.get_delta() > 0)
        wait(lambda: reader_hunk_chunk_data_weight.get_delta() > 0)

        wait(lambda: reader_inline_hunk_value_count.get_delta() == 10)
        wait(lambda: reader_ref_hunk_value_count.get_delta() == 5)

        wait(lambda: writer_chunk_data_weight.get_delta() > 0)
        assert writer_hunk_chunk_data_weight.get_delta() == 0

        wait(lambda: writer_inline_hunk_value_count.get_delta() == 15)
        assert writer_ref_hunk_value_count.get_delta() == 0

    @authors("akozhikhov")
    def test_hunks_profiling_lookup(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        keys1 = [{"key": i} for i in xrange(10)]
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(5)]
        keys2 = [{"key": i} for i in xrange(10, 20)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in xrange(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_flush_table("//tmp/t")

        hunk_chunk_transmitted = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="lookup/hunks/chunk_reader_statistics/data_bytes_transmitted")
        hunk_chunk_data_weight = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="lookup/hunks/data_weight")

        inline_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="lookup/hunks/inline_value_count")
        ref_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="lookup/hunks/ref_value_count")

        columnar_inline_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="lookup/hunks/inline_value_count",
            tags={"column": "value"})
        columnar_ref_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="lookup/hunks/ref_value_count",
            tags={"column": "value"})

        backend_request_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="lookup/hunks/backend_request_count")

        assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

        wait(lambda: hunk_chunk_transmitted.get_delta() > 0)
        wait(lambda: hunk_chunk_data_weight.get_delta() > 0)

        wait(lambda: inline_hunk_value_count.get_delta() == 20)
        wait(lambda: ref_hunk_value_count.get_delta() == 10)

        wait(lambda: columnar_inline_hunk_value_count.get_delta() == 10)
        wait(lambda: columnar_ref_hunk_value_count.get_delta() == 5)

        wait(lambda: backend_request_count.get_delta() == 1)

    @authors("akozhikhov")
    def test_hunks_profiling_select(self):
        sync_create_cells(1)
        self._create_table()
        set("//tmp/t/@enable_hunk_columnar_profiling", True)
        sync_mount_table("//tmp/t")

        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(5)]
        rows2 = [{"key": i, "value": "value" + str(i)} for i in xrange(5, 15)]
        insert_rows("//tmp/t", rows1 + rows2)
        sync_flush_table("//tmp/t")

        hunk_chunk_transmitted = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="select/hunks/chunk_reader_statistics/data_bytes_transmitted")
        hunk_chunk_data_weight = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="select/hunks/data_weight")

        inline_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="select/hunks/inline_value_count",
            tags={"column": "value"})
        ref_hunk_value_count = Profiler.at_tablet_node(self.Env.create_native_client(), "//tmp/t").counter(
            name="select/hunks/ref_value_count",
            tags={"column": "value"})

        assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)

        wait(lambda: hunk_chunk_transmitted.get_delta() > 0)
        wait(lambda: hunk_chunk_data_weight.get_delta() > 0)

        wait(lambda: inline_hunk_value_count.get_delta() == 10)
        wait(lambda: ref_hunk_value_count.get_delta() == 5)
