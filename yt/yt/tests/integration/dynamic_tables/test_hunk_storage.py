from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, exists, wait, remove, sync_mount_table, sync_create_cells,
    sync_unmount_table, write_hunks, read_hunks, raises_yt_error,
    sync_freeze_table, sync_unfreeze_table, copy, move, alter_table,
    lock_hunk_store, unlock_hunk_store, start_transaction, commit_transaction, abort_transaction, lock
)

from yt.test_helpers import assert_items_equal

from yt_type_helpers import make_schema

from yt.common import YtError

import yt.yson as yson

import pytest

import time

##################################################################


class TestHunkStorage(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    USE_DYNAMIC_TABLES = True

    def _get_active_store_id(self, hunk_storage, tablet_index=0):
        tablets = get("{}/@tablets".format(hunk_storage))
        tablet_id = tablets[tablet_index]["tablet_id"]
        wait(lambda: exists("//sys/tablets/{}/orchid/active_store_id".format(tablet_id)))
        return get("//sys/tablets/{}/orchid/active_store_id".format(tablet_id))

    def _create_hunk_storage(self, name, **attributes):
        if "store_rotation_period" not in attributes:
            attributes.update({"store_rotation_period": 2000})
        if "store_removal_grace_period" not in attributes:
            attributes.update({"store_removal_grace_period": 4000})

        create("hunk_storage", name, attributes=attributes)

    def _create_ordered_table(self, name, attributes={}):
        ordered_schema = make_schema(
            [
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
            ],
            strict=True,
        )

        attributes["dynamic"] = True
        attributes["schema"] = ordered_schema
        create("table", name, attributes=attributes)

    def _remove_hunk_storage(self, path):
        wait(lambda: get(f"{path}/@associated_nodes") == [])
        remove(path)

    @authors("gritukan")
    def test_create_remove(self):
        self._create_hunk_storage("//tmp/h")
        assert get("//tmp/h/@type") == "hunk_storage"
        hunk_storage_id = get("//tmp/h/@id")

        assert get("//tmp/h/@erasure_codec") == "none"
        assert get("//tmp/h/@replication_factor") == 3
        assert get("//tmp/h/@read_quorum") == 2
        assert get("//tmp/h/@write_quorum") == 2

        root_chunk_list_id = get("//tmp/h/@chunk_list_id")
        assert get("#{}/@kind".format(root_chunk_list_id)) == "hunk_storage_root"
        children = get("#{}/@child_ids".format(root_chunk_list_id))
        assert len(children) == 1
        tablet_chunk_list_id = children[0]
        assert get("#{}/@kind".format(tablet_chunk_list_id)) == "hunk_tablet"

        tablets = get("//tmp/h/@tablets")
        assert len(tablets) == 1
        tablet_id = tablets[0]["tablet_id"]
        assert get("#{}/@type".format(tablet_id)) == "hunk_tablet"
        assert get("#{}/@chunk_list_id".format(tablet_id)) == tablet_chunk_list_id

        remove("//tmp/h")
        assert not exists("//tmp/h")
        wait(lambda: not exists("#{}".format(hunk_storage_id)))
        wait(lambda: not exists("#{}".format(tablet_id)))
        wait(lambda: not exists("#{}".format(root_chunk_list_id)))
        wait(lambda: not exists("#{}".format(tablet_chunk_list_id)))

    @authors("gritukan")
    def test_mount_unmount(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")

        sync_mount_table("//tmp/h")
        tablets = get("//tmp/h/@tablets")
        assert len(tablets) == 1

        tablet = tablets[0]
        tablet_id = tablet["tablet_id"]
        cell_id = tablet["cell_id"]

        tablet_ids = get("//sys/tablet_cells/" + cell_id + "/@tablet_ids")
        assert tablet_ids == [tablet_id]

        chunk_list_id = get("#{}/@chunk_list_id".format(tablet_id))
        wait(lambda: len(get("#{}/@child_ids".format(chunk_list_id))) == 2)
        for chunk_id in get("#{}/@child_ids".format(chunk_list_id)):
            assert get("#{}/@type".format(chunk_id)) == "journal_chunk"

        sync_unmount_table("//tmp/h")
        assert get("//sys/tablet_cells/" + cell_id + "/@tablet_ids") == []

        wait(lambda: not exists("#{}".format(chunk_id)))

    @authors("gritukan")
    def test_freeze_forbidden(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")

        sync_mount_table("//tmp/h")

        with raises_yt_error("Hunk storage does not support freeze"):
            sync_freeze_table("//tmp/h")
        with raises_yt_error("Hunk storage does not support unfreeze"):
            sync_unfreeze_table("//tmp/h")

    @authors("gritukan")
    def test_copy_forbidden(self):
        self._create_hunk_storage("//tmp/h")
        with raises_yt_error("Hunk storage does not support clone mode"):
            copy("//tmp/h", "//tmp/g")

    @authors("gritukan")
    def test_move_forbidden(self):
        self._create_hunk_storage("//tmp/h")
        with raises_yt_error("Hunk storage does not support clone mode"):
            move("//tmp/h", "//tmp/g")

    @authors("gritukan", "akozhikhov")
    @pytest.mark.parametrize("erasure_codec", ["none", "reed_solomon_3_3"])
    def test_write_simple(self, erasure_codec):
        sync_create_cells(1)
        if erasure_codec == "none":
            self._create_hunk_storage("//tmp/h")
        else:
            self._create_hunk_storage("//tmp/h", read_quorum=4, write_quorum=5)
            set("//tmp/h/@erasure_codec", erasure_codec)
            set("//tmp/h/@replication_factor", 1)
        sync_mount_table("//tmp/h")

        store_id = self._get_active_store_id("//tmp/h")
        hunks = write_hunks("//tmp/h", ["a", "bb"])
        assert hunks == [
            {"chunk_id": store_id, "block_index": 0, "block_offset": 0, "length": 9,
             "erasure_codec": erasure_codec, "block_size": 19},
            {"chunk_id": store_id, "block_index": 0, "block_offset": 9, "length": 10,
             "erasure_codec": erasure_codec, "block_size": 19},
        ]

        assert read_hunks(hunks) == [
            {"payload": "a"},
            {"payload": "bb"},
        ]

        wait(lambda: get("#{}/@sealed".format(store_id)))
        assert get("#{}/@row_count".format(store_id)) == 1

        assert read_hunks(hunks) == [
            {"payload": "a"},
            {"payload": "bb"},
        ]

        wait(lambda: not exists("#{}".format(store_id)))

    @authors("akozhikhov")
    def test_bad_erasure_codec(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        set("//tmp/h/@erasure_codec", "reed_solomon_6_3")
        with raises_yt_error("bytewise"):
            sync_mount_table("//tmp/h")

    @authors("akozhikhov")
    def test_bad_journal_params(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h", read_quorum=4, write_quorum=5)

        set("//tmp/h/@erasure_codec", "none")
        set("//tmp/h/@replication_factor", 2)
        with raises_yt_error("read_quorum"):
            sync_mount_table("//tmp/h")

        set("//tmp/h/@erasure_codec", "reed_solomon_3_3")
        set("//tmp/h/@replication_factor", 2)
        with raises_yt_error("replication_factor"):
            sync_mount_table("//tmp/h")

        set("//tmp/h/@erasure_codec", "reed_solomon_3_3")
        set("//tmp/h/@replication_factor", 1)
        sync_mount_table("//tmp/h")

    @authors("gritukan")
    def test_store_rotation_1(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        sync_mount_table("//tmp/h")

        store_id = self._get_active_store_id("//tmp/h")
        wait(lambda: self._get_active_store_id("//tmp/h") != store_id)

    @authors("gritukan")
    def test_store_rotation_2(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        set("//tmp/h/@hunk_store_writer", {"desired_hunk_count_per_chunk": 1})
        set("//tmp/h/@store_rotation_period", 1000000)
        sync_mount_table("//tmp/h")

        store_id = self._get_active_store_id("//tmp/h")

        hunk1 = write_hunks("//tmp/h", ["a"])[0]
        wait(lambda: self._get_active_store_id("//tmp/h") != store_id)

        hunk2 = write_hunks("//tmp/h", ["a"])[0]
        assert hunk1["chunk_id"] != hunk2["chunk_id"]

    @authors("gritukan")
    def test_store_rotation_3(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        set("//tmp/h/@hunk_store_writer", {"desired_chunk_size": 10})
        set("//tmp/h/@store_rotation_period", 1000000)
        sync_mount_table("//tmp/h")

        store_id = self._get_active_store_id("//tmp/h")

        hunk1 = write_hunks("//tmp/h", ["a" * 100])[0]
        wait(lambda: self._get_active_store_id("//tmp/h") != store_id)

        hunk2 = write_hunks("//tmp/h", ["a"])[0]
        assert hunk1["chunk_id"] != hunk2["chunk_id"]

    @authors("gritukan")
    def test_store_removal_grace_period(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        set("//tmp/h/@hunk_store_writer", {"desired_chunk_size": 10})
        set("//tmp/h/@store_rotation_period", 1000000)
        set("//tmp/h/@store_removal_grace_period", 2000)

        sync_mount_table("//tmp/h")

        hunk = write_hunks("//tmp/h", ["a" * 100])[0]
        chunk_id = hunk["chunk_id"]
        assert exists("#{}".format(chunk_id))

        time.sleep(1)
        assert exists("#{}".format(chunk_id))

        time.sleep(1.5)
        assert not exists("#{}".format(chunk_id))

    @authors("gritukan")
    def test_hunk_store_lock(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")

        set("//tmp/h/@hunk_store_writer", {"desired_chunk_size": 500})
        set("//tmp/h/@store_rotation_period", 1000)
        set("//tmp/h/@store_removal_grace_period", 1000)

        sync_mount_table("//tmp/h")

        hunk = write_hunks("//tmp/h", ["a" * 100])[0]
        store_id = hunk["chunk_id"]
        lock_hunk_store("//tmp/h", 0, store_id)

        time.sleep(3)

        assert exists("#{}".format(store_id))

        unlock_hunk_store("//tmp/h", 0, store_id)
        wait(lambda: not exists("#{}".format(store_id)))

    @authors("gritukan")
    def test_invalid_hunk_store_lock(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        sync_mount_table("//tmp/h")

        with raises_yt_error("No such store"):
            lock_hunk_store("//tmp/h", 0, "1-1-1-1")
        with raises_yt_error("No such store"):
            unlock_hunk_store("//tmp/h", 0, "1-1-1-1")

        store_id = self._get_active_store_id("//tmp/h")
        with raises_yt_error("is not locked"):
            unlock_hunk_store("//tmp/h", 0, store_id)

    @authors("gritukan")
    @pytest.mark.xfail(run=False, reason="WIP")
    def test_force_unmount(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        set("//tmp/h/@store_rotation_period", 1000000)
        sync_mount_table("//tmp/h")

        hunk = write_hunks("//tmp/h", ["arbuzich"])[0]
        chunk_id = hunk["chunk_id"]

        sync_unmount_table("//tmp/h", force=True)

        wait(lambda: get("#{}/@sealed".format(chunk_id)))
        assert read_hunks([hunk]) == [{"payload": "arbuzich"}]

        sync_mount_table("//tmp/h")
        wait(lambda: not exists("#{}".format(chunk_id)))

    @authors("aleksandra-zh")
    def test_link_hunk_storage_node(self):
        self._create_hunk_storage("//tmp/h")

        self._create_ordered_table("//tmp/t")
        set("//tmp/t/@hunk_storage_node", "//tmp/h")

        assert get("//tmp/h/@associated_nodes") == ["//tmp/t"]

        with raises_yt_error("Cannot remove a hunk storage that is being used by nodes"):
            remove("//tmp/h")

        remove("//tmp/t")
        self._remove_hunk_storage("//tmp/h")

    @authors("aleksandra-zh")
    def test_create_table_with_hunk_storage_node(self):
        # TODO(aleksandra-zh): Rework attribute.
        return

        self._create_hunk_storage("//tmp/h")

        with pytest.raises(YtError):
            create("table", "//tmp/t", attributes={"hunk_storage_node": "//tmp/h"})

        self._create_ordered_table("//tmp/t", {"hunk_storage_node": "//tmp/h"})

        assert get("//tmp/h/@associated_nodes") == ["//tmp/t"]

        remove("//tmp/t")
        self._remove_hunk_storage("//tmp/h")

    @authors("aleksandra-zh")
    def test_remove_table_hunk_storage_node(self):
        self._create_hunk_storage("//tmp/h")

        self._create_ordered_table("//tmp/t")
        set("//tmp/t/@hunk_storage_node", "//tmp/h")
        assert get("//tmp/h/@associated_nodes") == ["//tmp/t"]

        remove("//tmp/t/@hunk_storage_node")
        self._remove_hunk_storage("//tmp/h")

    @authors("aleksandra-zh")
    def test_hunk_storage_node_node_type(self):
        self._create_ordered_table("//tmp/t1")
        self._create_ordered_table("//tmp/t2")

        with raises_yt_error("Unexpected node type"):
            set("//tmp/t1/@hunk_storage_node", "//tmp/t2")

    @authors("aleksandra-zh")
    def test_alter_table_with_hunk_storage_node(self):
        self._create_hunk_storage("//tmp/h")

        self._create_ordered_table("//tmp/t")
        set("//tmp/t/@hunk_storage_node", "//tmp/h")

        with raises_yt_error("Cannot alter table with a hunk storage node to static"):
            alter_table("//tmp/t", dynamic=False)

    @authors("aleksandra-zh")
    def test_copy_linked_hunk_storage_node(self):
        self._create_hunk_storage("//tmp/h")

        self._create_ordered_table("//tmp/t1")
        set("//tmp/t1/@hunk_storage_node", "//tmp/h")
        copy("//tmp/t1", "//tmp/t2")

        assert_items_equal(get("//tmp/h/@associated_nodes"), ["//tmp/t1", "//tmp/t2"])

        with raises_yt_error("Cannot remove a hunk storage that is being used by nodes"):
            remove("//tmp/h")

        remove("//tmp/t1")
        remove("//tmp/t2")
        self._remove_hunk_storage("//tmp/h")

    @authors("aleksandra-zh")
    def test_linked_hunk_storage_node_tx1(self):
        self._create_hunk_storage("//tmp/h")

        tx = start_transaction()
        self._create_ordered_table("//tmp/t1")
        set("//tmp/t1/@hunk_storage_node", "//tmp/h", tx=tx)

        assert get("//tmp/h/@associated_nodes") == [yson.to_yson_type("//tmp/t1", attributes={"transaction_id": tx})]
        commit_transaction(tx)
        assert get("//tmp/h/@associated_nodes") == ["//tmp/t1"]

        remove("//tmp/t1")
        self._remove_hunk_storage("//tmp/h")

    @authors("aleksandra-zh")
    def test_linked_hunk_storage_node_tx2(self):
        self._create_hunk_storage("//tmp/h")

        tx = start_transaction()
        self._create_ordered_table("//tmp/t1")
        set("//tmp/t1/@hunk_storage_node", "//tmp/h", tx=tx)

        assert get("//tmp/h/@associated_nodes") == [yson.to_yson_type("//tmp/t1", attributes={"transaction_id": tx})]
        abort_transaction(tx)

        self._remove_hunk_storage("//tmp/h")

    @authors("aleksandra-zh")
    def test_linked_hunk_storage_node_tx3(self):
        self._create_hunk_storage("//tmp/h")

        self._create_ordered_table("//tmp/t1")
        set("//tmp/t1/@hunk_storage_node", "//tmp/h")

        tx = start_transaction()
        lock("//tmp/t1", tx=tx, mode="snapshot")
        assert_items_equal(get("//tmp/h/@associated_nodes"), [
            "//tmp/t1",
            yson.to_yson_type("//tmp/t1", attributes={"transaction_id": tx}),
        ])

        commit_transaction(tx)
        assert get("//tmp/h/@associated_nodes") == ["//tmp/t1"]

        remove("//tmp/t1")
        self._remove_hunk_storage("//tmp/h")

    @authors("aleksandra-zh")
    def test_copy_linked_hunk_storage_node_tx(self):
        self._create_hunk_storage("//tmp/h")

        self._create_ordered_table("//tmp/t1")
        set("//tmp/t1/@hunk_storage_node", "//tmp/h")

        tx = start_transaction()
        copy("//tmp/t1", "//tmp/t2", tx=tx)
        table2_id = get("//tmp/t2/@id", tx=tx)

        assert_items_equal(get("//tmp/h/@associated_nodes"), [
            "//tmp/t1",
            f"#{table2_id}",
            yson.to_yson_type("//tmp/t2", attributes={"transaction_id": tx}),
        ])

        commit_transaction(tx)

        assert_items_equal(get("//tmp/h/@associated_nodes"), ["//tmp/t1", "//tmp/t2"])

        with raises_yt_error("Cannot remove a hunk storage that is being used by nodes"):
            remove("//tmp/h")

        remove("//tmp/t1")
        remove("//tmp/t2")
        self._remove_hunk_storage("//tmp/h")


class TestHunkStorageMulticell(TestHunkStorage):
    NUM_SECONDARY_MASTER_CELLS = 1


class TestHunkStoragePortal(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True
    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 4

    def _create_hunk_storage(self, name):
        create("hunk_storage", name, attributes={
            "store_rotation_period": 2000,
            "store_removal_grace_period": 4000,
        })

    def _create_ordered_table(self, name, attributes={}):
        ordered_schema = make_schema(
            [
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
            ],
            strict=True,
        )

        attributes["dynamic"] = True
        attributes["schema"] = ordered_schema
        create("table", name, attributes=attributes)

    @authors("aleksandra-zh")
    def test_cross_shard_hunk_storage_node_link(self):
        self._create_ordered_table("//tmp/t1")
        self._create_hunk_storage("//portals/h1")
        assert "//portals/h1/@native_cell_tag" != "//tmp/t1/@native_cell_tag"

        with pytest.raises(YtError):
            set("//tmp/t1/@hunk_storage_node", "//portals/h1")

        self._create_ordered_table("//portals/t2")

        self._create_hunk_storage("//tmp/h2")
        assert "//tmp/h2/@native_cell_tag" != "//portals/t2/@native_cell_tag"

        with pytest.raises(YtError):
            set("//portals/t2/@hunk_storage_node", "//tmp/h2")
