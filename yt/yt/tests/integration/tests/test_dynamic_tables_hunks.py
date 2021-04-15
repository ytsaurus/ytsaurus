from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt.test_helpers import assert_items_equal, wait

from yt_commands import *

################################################################################


class TestSortedDynamicTablesHunks(TestSortedDynamicTablesBase):
    SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string", "max_inline_hunk_size": 10},
    ]

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "enable_hunks": True
        }
    }

    def _create_table(self):
        self._create_simple_table("//tmp/t", schema=self.SCHEMA, enable_dynamic_store_read=False)

    def _get_store_chunk_ids(self, path):
        chunk_ids = get(path + "/@chunk_ids")
        return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id)) == "table"]

    def _get_hunk_chunk_ids(self, path):
        chunk_ids = get(path + "/@chunk_ids")
        return [chunk_id for chunk_id in chunk_ids if get("#{}/@chunk_type".format(chunk_id)) == "hunk"]

    @authors("babenko")
    def test_flush_inline(self):
        sync_create_cells(1)
        self._create_table()

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in xrange(10)]
        rows = [{"key": i, "value": "value" + str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        # assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        # assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        # assert_items_equal(select_rows("* from [//tmp/t] where value = \"{}\"".format(rows[0]["value"])), [rows[0]])
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 0

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_ids[0])) == []

        sync_mount_table("//tmp/t")

        # assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        # assert_items_equal(lookup_rows("//tmp/t", keys), rows)

    @authors("babenko")
    def test_flush_to_hunk_chunk(self):
        sync_create_cells(1)
        self._create_table()

        sync_mount_table("//tmp/t")
        keys = [{"key": i} for i in xrange(10)]
        rows = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        insert_rows("//tmp/t", rows)
        # assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        # assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        sync_unmount_table("//tmp/t")

        store_chunk_ids = self._get_store_chunk_ids("//tmp/t")
        assert len(store_chunk_ids) == 1
        hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_ids) == 1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_ids[0])) == [
            {"chunk_id": hunk_chunk_ids[0], "hunk_count": 10, "total_hunk_length": 260}
        ]

        sync_mount_table("//tmp/t")

        # assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        # assert_items_equal(lookup_rows("//tmp/t", keys), rows)
        # assert_items_equal(select_rows("* from [//tmp/t] where value = \"{}\"".format(rows[0]["value"])), [rows[0]])

    @authors("babenko")
    def test_compaction(self):
        sync_create_cells(1)
        self._create_table()

        sync_mount_table("//tmp/t")
        rows1 = [{"key": i, "value": "value" + str(i) + "x" * 20} for i in xrange(10)]
        keys1 = [{"key": i} for i in xrange(10)]
        insert_rows("//tmp/t", rows1)
        # assert_items_equal(select_rows("* from [//tmp/t]"), rows1)
        # assert_items_equal(lookup_rows("//tmp/t", keys1), rows1)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        rows2 = [{"key": i, "value": "value" + str(i) + "y" * 20} for i in xrange(10, 20)]
        keys2 = [{"key": i} for i in xrange(10, 20)]
        insert_rows("//tmp/t", rows2)
        # select_rows("* from [//tmp/t]")
        # assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

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

        if get("#{}/@hunk_chunk_refs/0/total_hunk_length".format(store_chunk_id1)) > get("#{}/@hunk_chunk_refs/0/total_hunk_length".format(store_chunk_id2)):
            store_chunk_id1, store_chunk_id2 = store_chunk_id2, store_chunk_id1
            hunk_chunk_id1, hunk_chunk_id2 = hunk_chunk_id2, hunk_chunk_id1

        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id1)) == [
            {"chunk_id": hunk_chunk_id1, "hunk_count": 10, "total_hunk_length": 260}
        ]
        assert get("#{}/@hunk_chunk_refs".format(store_chunk_id2)) == [
            {"chunk_id": hunk_chunk_id2, "hunk_count": 10, "total_hunk_length": 270}
        ]

        sync_mount_table("//tmp/t")

        # assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        # assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

        set("//tmp/t/@forced_compaction_revision", 1)
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

        # assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        # assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)

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
        # assert_items_equal(select_rows("* from [//tmp/t]"), rows1 + rows2)
        # assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows1 + rows2)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        delete_rows("//tmp/t", keys1)
        # assert_items_equal(select_rows("* from [//tmp/t]"), rows2)
        # assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows2)
        sync_unmount_table("//tmp/t")

        assert len(self._get_store_chunk_ids("//tmp/t")) == 2
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        sync_mount_table("//tmp/t")
        # assert_items_equal(select_rows("* from [//tmp/t]"), rows2)
        # assert_items_equal(lookup_rows("//tmp/t", keys1 + keys2), rows2)

        set("//tmp/t/@min_data_ttl", 60000)
        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 2)
        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 1

        set("//tmp/t/@min_data_ttl", 0)
        set("//tmp/t/@min_data_versions", 1)
        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")

        wait(lambda: get("//tmp/t/@chunk_count") == 1)
        assert len(self._get_store_chunk_ids("//tmp/t")) == 1
        assert len(self._get_hunk_chunk_ids("//tmp/t")) == 0
