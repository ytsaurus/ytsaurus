from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, exists, wait, remove, sync_mount_table, sync_create_cells,
    sync_unmount_table, write_hunks, read_hunks, raises_yt_error,
    sync_freeze_table, sync_unfreeze_table, copy, move,
    lock_hunk_store, unlock_hunk_store,
)

import time

##################################################################


class TestHunkStorage(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    def _get_active_store_id(self, hunk_storage, tablet_index=0):
        tablets = get("{}/@tablets".format(hunk_storage))
        tablet_id = tablets[tablet_index]["tablet_id"]
        wait(lambda: exists("//sys/tablets/{}/orchid/active_store_id".format(tablet_id)))
        return get("//sys/tablets/{}/orchid/active_store_id".format(tablet_id))

    def _create_hunk_storage(self, name):
        create("hunk_storage", name, attributes={
            "store_rotation_period": 2000,
            "store_removal_grace_period": 4000,
        })

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

    @authors("gritukan")
    def test_write_simple(self):
        sync_create_cells(1)
        self._create_hunk_storage("//tmp/h")
        sync_mount_table("//tmp/h")

        store_id = self._get_active_store_id("//tmp/h")
        hunks = write_hunks("//tmp/h", ["a", "bb"])
        assert hunks == [
            {"chunk_id": store_id, "block_index": 0, "block_offset": 0, "length": 1, "erasure_codec": "none"},
            {"chunk_id": store_id, "block_index": 0, "block_offset": 1, "length": 2, "erasure_codec": "none"},
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

        set("//tmp/h/@hunk_store_writer", {"desired_chunk_size": 10})
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
