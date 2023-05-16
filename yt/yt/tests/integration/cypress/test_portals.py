from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, raises_yt_error, wait, create, ls, get, set, copy, move, remove, link,
    exists, concatenate, create_account, create_user, create_tablet_cell_bundle, remove_tablet_cell_bundle,
    make_ace, check_permission, start_transaction, abort_transaction, commit_transaction,
    lock, externalize,
    internalize, select_rows, read_file, write_file, read_table,
    write_table, map,
    sync_create_cells, create_dynamic_table, insert_rows, lookup_rows,
    sync_mount_table, sync_unmount_table, sync_freeze_table,
    get_singular_chunk_id, cluster_resources_equal, get_driver,
    generate_uuid, )

from yt_helpers import get_current_time

from yt.common import YtError
from yt.test_helpers import assert_items_equal
import yt.yson as yson

import pytest


##################################################################


def _purge_resolve_cache(path):
    tx = start_transaction()
    lock(path, tx=tx)
    abort_transaction(tx)
    assert not get(path + "/@resolve_cached")


def _maybe_purge_resolve_cache(flag, path):
    if flag:
        _purge_resolve_cache(path)


##################################################################


class TestPortals(YTEnvSetup):
    NUM_TEST_PARTITIONS = 2

    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True
    NUM_SCHEDULERS = 1

    @authors("babenko")
    def test_cannot_create_portal_exit(self):
        with pytest.raises(YtError):
            create("portal_exit", "//tmp/e")

    @authors("babenko")
    def test_cannot_create_portal_to_primary1(self):
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 10})

    @authors("babenko")
    def test_cannot_create_portal_to_primary2(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p/q", attributes={"exit_cell_tag": 10})

    @authors("babenko")
    def test_validate_cypress_node_host_cell_role1(self):
        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["chunk_host"]}})
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

    @authors("aleksandra-zh")
    def test_validate_cypress_node_host_cell_role2(self):
        set("//sys/@config/multicell_manager/remove_secondary_cell_default_roles", True)
        set("//sys/@config/multicell_manager/cell_descriptors", {})
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

        set("//sys/@config/multicell_manager/remove_secondary_cell_default_roles", False)
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

    @authors("babenko")
    def test_need_exit_cell_tag_on_create(self):
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p")

    @authors("babenko")
    def test_create_portal(self):
        entrance_id = create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        assert get("//tmp/p&/@type") == "portal_entrance"
        effective_acl = get("//tmp/@effective_acl")
        assert get("//tmp/p&/@path") == "//tmp/p"

        assert exists("//sys/portal_entrances/{}".format(entrance_id))

        exit_id = get("//tmp/p&/@exit_node_id")
        assert get("#{}/@type".format(exit_id), driver=get_driver(1)) == "portal_exit"
        assert get("#{}/@entrance_node_id".format(exit_id), driver=get_driver(1)) == entrance_id
        assert get("#{}/@inherit_acl".format(exit_id), driver=get_driver(1))
        assert get("#{}/@effective_acl".format(exit_id), driver=get_driver(1)) == effective_acl
        assert get("#{}/@path".format(exit_id), driver=get_driver(1)) == "//tmp/p"

        assert exists("//sys/portal_exits/{}".format(exit_id), driver=get_driver(1))

    @authors("babenko")
    def test_cannot_enable_acl_inheritance(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        get("//tmp/p&/@exit_node_id")
        with pytest.raises(YtError):
            set("//tmp/p/@inherit_acl", True, driver=get_driver(1))

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_portal_reads(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        exit_id = get("//tmp/p&/@exit_node_id")

        assert get("//tmp/p") == {}
        assert get("//tmp/p/@type") == "portal_exit"
        assert get("//tmp/p/@id") == exit_id

        create("table", "#{}/t".format(exit_id), driver=get_driver(1))
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert get("//tmp/p") == {"t": yson.YsonEntity()}

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_portal_writes(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p/t")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert get("//tmp/p") == {"t": yson.YsonEntity()}

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_remove_portal(self, purge_resolve_cache):
        entrance_id = create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        exit_id = get("//tmp/p&/@exit_node_id")
        table_id = create("table", "//tmp/p/t")
        shard_id = get("//tmp/p/t/@shard_id")

        for i in range(10):
            create("map_node", "//tmp/p/m" + str(i))

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        remove("//tmp/p")

        wait(
            lambda: not exists("#{}".format(exit_id))
            and not exists("#{}".format(entrance_id), driver=get_driver(1))
            and not exists("#{}".format(table_id), driver=get_driver(1))
            and not exists("#{}".format(shard_id))
        )

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_remove_all_portal_children(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        remove("//tmp/p/*")

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_portal_set(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        set("//tmp/p/key", "value", force=True)
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert get("//tmp/p/key") == "value"
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        set("//tmp/p/map/key", "value", force=True, recursive=True)
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert get("//tmp/p/map/key") == "value"

    @pytest.mark.parametrize(
        "with_outer_tx,external_cell_tag,purge_resolve_cache",
        [
            (with_outer_tx, external_cell_tag, purge_resolve_cache)
            for with_outer_tx in [False, True]
            for external_cell_tag in [11, 12]
            for purge_resolve_cache in [False, True]
        ],
    )
    @authors("babenko")
    def test_read_write_table_in_portal(self, with_outer_tx, external_cell_tag, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("map_node", "//tmp/p/m")

        PAYLOAD = [{"key": "value"}]

        if with_outer_tx:
            tx = start_transaction()
        else:
            tx = "0-0-0-0"

        create(
            "table",
            "//tmp/p/m/t",
            attributes={"external": True, "external_cell_tag": external_cell_tag},
            tx=tx,
        )
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        write_table("//tmp/p/m/t", PAYLOAD, tx=tx)
        assert get("//tmp/p/m/t/@row_count", tx=tx) == len(PAYLOAD)
        assert get("//tmp/p/m/t/@chunk_count", tx=tx) == 1
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert read_table("//tmp/p/m/t", tx=tx) == PAYLOAD

        if with_outer_tx:
            commit_transaction(tx)

        assert read_table("//tmp/p/m/t") == PAYLOAD

        chunk_id = get_singular_chunk_id("//tmp/p/m/t")
        assert get("#{}/@owning_nodes".format(chunk_id)) == ["//tmp/p/m/t"]

    @pytest.mark.parametrize(
        "with_outer_tx,external_cell_tag,purge_resolve_cache",
        [
            (with_outer_tx, external_cell_tag, purge_resolve_cache)
            for with_outer_tx in [False, True]
            for external_cell_tag in [11, 12]
            for purge_resolve_cache in [False, True]
        ],
    )
    @authors("babenko")
    def test_read_write_file_in_portal(self, with_outer_tx, external_cell_tag, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("map_node", "//tmp/p/m")

        PAYLOAD = b"a" * 100

        if with_outer_tx:
            tx = start_transaction()
        else:
            tx = "0-0-0-0"

        create(
            "file",
            "//tmp/p/m/f",
            attributes={"external": True, "external_cell_tag": external_cell_tag},
            tx=tx,
        )
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        write_file("//tmp/p/m/f", PAYLOAD, tx=tx)
        assert get("//tmp/p/m/f/@uncompressed_data_size", tx=tx) == len(PAYLOAD)
        assert get("//tmp/p/m/f/@chunk_count", tx=tx) == 1
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert read_file("//tmp/p/m/f", tx=tx) == PAYLOAD

        if with_outer_tx:
            commit_transaction(tx)

        assert read_file("//tmp/p/m/f") == PAYLOAD

        chunk_id = get_singular_chunk_id("//tmp/p/m/f")
        assert get("#{}/@owning_nodes".format(chunk_id)) == ["//tmp/p/m/f"]

    @authors("babenko")
    def test_account_lifetime(self):
        create_account("a")
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})
        create(
            "table",
            "//tmp/p1/t",
            attributes={"account": "a", "external": True, "external_cell_tag": 11},
        )
        create(
            "table",
            "//tmp/p2/t",
            attributes={"account": "a", "external": True, "external_cell_tag": 12},
        )
        remove("//sys/accounts/a")
        assert get("//sys/accounts/a/@life_stage") == "removal_pre_committed"
        wait(lambda: get("//sys/accounts/a/@life_stage", driver=get_driver(1)) == "removal_started")
        wait(lambda: get("//sys/accounts/a/@life_stage", driver=get_driver(2)) == "removal_started")
        remove("//tmp/p1/t")
        wait(lambda: get("//sys/accounts/a/@life_stage", driver=get_driver(1)) == "removal_pre_committed")
        assert get("//sys/accounts/a/@life_stage", driver=get_driver(2)) == "removal_started"
        remove("//tmp/p2/t")
        wait(lambda: not exists("//sys/accounts/a"))

    @authors("babenko")
    def test_expiration_time(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create(
            "table",
            "//tmp/p/t",
            attributes={"expiration_time": str(get_current_time())},
        )
        wait(lambda: not exists("//tmp/p/t"))

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_remove_table_in_portal(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        table_id = create("table", "//tmp/p/t", attributes={"external": True, "external_cell_tag": 12})
        wait(lambda: exists("#{}".format(table_id), driver=get_driver(2)))
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        remove("//tmp/p/t")
        wait(lambda: not exists("#{}".format(table_id), driver=get_driver(2)))

    @authors("babenko")
    def test_root_shard(self):
        shard_id = get("//@shard_id")
        assert exists("//sys/cypress_shards/{}".format(shard_id))
        assert get("//@id") == get("#{}/@root_node_id".format(shard_id))
        assert get("#{}/@account_statistics/sys/node_count".format(shard_id)) > 0
        assert get("#{}/@total_account_statistics/node_count".format(shard_id)) > 0

    @authors("babenko")
    def test_root_shard_statistics(self):
        shard_id = get("//@shard_id")
        create_account("a")
        create_account("b")

        assert not exists("#{}/@account_statistics/a".format(shard_id))

        create("table", "//tmp/t1", attributes={"account": "a"})

        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 1
        assert get("#{}/@total_account_statistics/node_count".format(shard_id)) >= 1

        create("table", "//tmp/t2", attributes={"account": "a"})

        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 2
        assert get("#{}/@total_account_statistics/node_count".format(shard_id)) >= 2

        set("//tmp/t2/@account", "b")

        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 1
        assert get("#{}/@account_statistics/b/node_count".format(shard_id)) == 1
        assert get("#{}/@total_account_statistics/node_count".format(shard_id)) >= 2

        remove("//tmp/*")

        wait(lambda: not exists("#{}/@account_statistics/a".format(shard_id)))

    @authors("babenko")
    def test_portal_shard_statistics(self):
        create_account("a")
        create_account("b")
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        shard_id = get("//tmp/p/@shard_id")

        create("table", "//tmp/p/a", attributes={"account": "a"})
        create("table", "//tmp/p/b", attributes={"account": "b"})

        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 1
        assert get("#{}/@account_statistics/b/node_count".format(shard_id)) == 1
        assert get("#{}/@account_statistics/tmp/node_count".format(shard_id)) == 1
        assert get("#{}/@total_account_statistics/node_count".format(shard_id)) == 3

    @authors("babenko")
    def test_copy_move_shard_friendly(self):
        shard_id = get("//@shard_id")
        create("table", "//tmp/t1")
        assert get("//tmp/t1/@shard_id") == shard_id
        copy("//tmp/t1", "//tmp/t2")
        assert get("//tmp/t2/@shard_id") == shard_id
        move("//tmp/t2", "//tmp/t3")
        assert get("//tmp/t3/@shard_id") == shard_id

    @authors("babenko")
    def test_portal_shard(self):
        create_account("a")
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        assert get("//tmp/p&/@shard_id") == get("//@shard_id")
        shard_id = get("//tmp/p/@shard_id")
        assert shard_id != get("//@shard_id")
        assert get("#{}/@account_statistics/tmp/node_count".format(shard_id)) == 1
        create("table", "//tmp/p/t", attributes={"account": "a"})
        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 1
        remove("//tmp/p/t")
        wait(lambda: not exists("#{}/@account_statistics/a".format(shard_id)))
        remove("//tmp/p")
        wait(lambda: not exists("#{}".format(shard_id)))

    @authors("babenko")
    def test_link_shard_in_tx_yt_11898(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        link("//tmp/t", "//tmp/l", tx=tx)
        # This list call must not crash.
        for x in ls("//tmp", attributes=["shard_id", "target_path", "broken"], tx=tx):
            if str(x) == "l":
                assert not x.attributes["broken"]
                assert x.attributes["target_path"] == "//tmp/t"

    @authors("babenko")
    def test_special_exit_attrs(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        assert get("//tmp/p/@key") == "p"
        assert get("//tmp/p/@parent_id") == get("//tmp/@id")

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    @pytest.mark.skipif("True", reason="YT-11197")
    def test_cross_shard_links_forbidden(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        with pytest.raises(YtError):
            link("//tmp", "//tmp/p/l")

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_intra_shard_links(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p/t")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        link("//tmp/p/t", "//tmp/p/t_")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        link("//tmp/p", "//tmp/p/_")

        assert_items_equal(ls("//tmp/p/_"), ["t", "t_", "_"])
        assert get("//tmp/p/t_&/@target_path") == "//tmp/p/t"
        assert get("//tmp/p/t_/@id") == get("//tmp/p/t/@id")

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_intra_shard_copy_move(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p/t")
        assert exists("//tmp/p/t")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        copy("//tmp/p/t", "//tmp/p/t1")
        assert exists("//tmp/p/t")
        assert exists("//tmp/p/t1")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        move("//tmp/p/t", "//tmp/p/t2")

        assert not exists("//tmp/p/t")
        assert exists("//tmp/p/t2")

    @authors("babenko")
    @pytest.mark.parametrize("in_tx", [False, True])
    def test_cross_cell_copy(self, in_tx):
        create_account("a")
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        create("map_node", "//tmp/p1/m", attributes={"account": "a"})
        create("document", "//tmp/p1/m/d", attributes={"value": {"hello": "world"}})
        create("list_node", "//tmp/p1/m/l")
        create("int64_node", "//tmp/p1/m/l/end")
        set("//tmp/p1/m/l/-1", 123)
        create("uint64_node", "//tmp/p1/m/l/end")
        set("//tmp/p1/m/l/-1", 345)
        create("double_node", "//tmp/p1/m/l/end")
        set("//tmp/p1/m/l/-1", 3.14)
        create("string_node", "//tmp/p1/m/l/end")
        set("//tmp/p1/m/l/-1", "test")

        create("file", "//tmp/p1/m/f", attributes={"external_cell_tag": 13})
        assert get("//tmp/p1/m/f/@account") == "a"
        FILE_PAYLOAD = b"PAYLOAD"
        write_file("//tmp/p1/m/f", FILE_PAYLOAD)

        wait(lambda: get("//sys/accounts/a/@resource_usage/chunk_count") == 1)

        create(
            "table",
            "//tmp/p1/m/t1",
            attributes={
                "external_cell_tag": 13,
                "optimize_for": "scan",
                "account": "tmp",
            },
        )
        assert get("//tmp/p1/m/t1/@account") == "tmp"
        TABLE_PAYLOAD = [{"key": "value"}]
        write_table("//tmp/p1/m/t1", TABLE_PAYLOAD)

        # Will be copied while unmounted.
        create_dynamic_table(
            "//tmp/p1/m/t2",
            external_cell_tag=13,
            optimize_for="scan",
            account="tmp",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
        assert get("//tmp/p1/m/t2/@account") == "tmp"
        sync_create_cells(1)
        sync_mount_table("//tmp/p1/m/t2")
        insert_rows("//tmp/p1/m/t2", [{"key": 42, "value": "hello"}])
        sync_unmount_table("//tmp/p1/m/t2")

        # Will be copied while frozen.
        create_dynamic_table(
            "//tmp/p1/m/t3",
            external_cell_tag=13,
            optimize_for="scan",
            account="tmp",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
        assert get("//tmp/p1/m/t3/@account") == "tmp"
        sync_create_cells(1)
        sync_mount_table("//tmp/p1/m/t3")
        insert_rows("//tmp/p1/m/t3", [{"key": 42, "value": "hello"}])
        sync_freeze_table("//tmp/p1/m/t3")

        if in_tx:
            tx = start_transaction()
        else:
            tx = "0-0-0-0"

        copy("//tmp/p1/m", "//tmp/p2/m", preserve_account=True, tx=tx)

        assert get("//tmp/p2/m/@key", tx=tx) == "m"
        assert get("//tmp/p2/m/@type", tx=tx) == "map_node"
        assert get("//tmp/p2/m/@account", tx=tx) == "a"
        assert get("//tmp/p2/m/d/@type", tx=tx) == "document"
        assert get("//tmp/p2/m/d", tx=tx) == {"hello": "world"}
        assert get("//tmp/p2/m/l/@type", tx=tx) == "list_node"
        assert get("//tmp/p2/m/l", tx=tx) == [123, 345, 3.14, "test"]

        assert get("//tmp/p2/m/f/@type", tx=tx) == "file"
        assert get("//tmp/p2/m/f/@resource_usage", tx=tx) == get("//tmp/p1/m/f/@resource_usage")
        assert get("//tmp/p2/m/f/@uncompressed_data_size", tx=tx) == len(FILE_PAYLOAD)
        assert read_file("//tmp/p2/m/f", tx=tx) == FILE_PAYLOAD
        assert read_table("//tmp/p2/m/t1", tx=tx) == TABLE_PAYLOAD
        assert get("//tmp/p2/m/t2/@dynamic", tx=tx)
        assert get("//tmp/p2/m/t3/@dynamic", tx=tx)

        assert get("//sys/accounts/a/@resource_usage/chunk_count") == 1

        if in_tx:
            commit_transaction(tx)

        sync_mount_table("//tmp/p2/m/t2")
        assert lookup_rows("//tmp/p2/m/t2", [{"key": 42}]) == [{"key": 42, "value": "hello"}]
        sync_mount_table("//tmp/p2/m/t3")
        assert lookup_rows("//tmp/p2/m/t3", [{"key": 42}]) == [{"key": 42, "value": "hello"}]

        create_account("b")
        set("//tmp/p2/m/f/@account", "b")
        wait(
            lambda: get("//sys/accounts/a/@resource_usage/chunk_count") == 1
            and get("//sys/accounts/b/@resource_usage/chunk_count") == 1
        )

        chunk_ids = get("//tmp/p2/m/f/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert_items_equal(get("#{}/@owning_nodes".format(chunk_id)), ["//tmp/p1/m/f", "//tmp/p2/m/f"])

        remove("//tmp/p1/m/f")
        remove("//tmp/p2/m/f")

        wait(lambda: not exists("#" + chunk_id))

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")
        remove("//tmp/p2")

    @authors("babenko")
    @pytest.mark.parametrize("in_tx", [False, True])
    def test_cross_cell_move(self, in_tx):
        create_account("a")
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        create("file", "//tmp/p1/f", attributes={"external_cell_tag": 13, "account": "a"})
        FILE_PAYLOAD = b"PAYLOAD"
        write_file("//tmp/p1/f", FILE_PAYLOAD)

        create_dynamic_table(
            "//tmp/p1/t",
            external_cell_tag=13,
            optimize_for="scan",
            account="tmp",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
        sync_create_cells(1)
        sync_mount_table("//tmp/p1/t")
        insert_rows("//tmp/p1/t", [{"key": 42, "value": "hello"}])
        sync_unmount_table("//tmp/p1/t")

        if in_tx:
            tx = start_transaction()
        else:
            tx = "0-0-0-0"

        move("//tmp/p1/f", "//tmp/p2/f", tx=tx, preserve_account=True)
        move("//tmp/p1/t", "//tmp/p2/t", tx=tx, preserve_account=True)

        assert not exists("//tmp/p1/f", tx=tx)
        assert exists("//tmp/p2/f", tx=tx)

        if in_tx:
            assert exists("//tmp/p1/f")
            assert not exists("//tmp/p2/f")

        assert read_file("//tmp/p2/f", tx=tx) == FILE_PAYLOAD

        if in_tx:
            commit_transaction(tx)

        sync_mount_table("//tmp/p2/t")
        assert lookup_rows("//tmp/p2/t", [{"key": 42}]) == [{"key": 42, "value": "hello"}]

        assert get("//tmp/p2/f/@account") == "a"

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")
        remove("//tmp/p2")

    @authors("shakurov")
    def test_portal_entrance_always_opaque(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        assert not get("//tmp/p1/@opaque")
        assert get("//tmp/p1&/@opaque")

        set("//tmp/p1/@opaque", True)
        assert get("//tmp/p1/@opaque")
        with pytest.raises(YtError):
            set("//tmp/p1&/@opaque", False)

    @authors("shakurov")
    def test_cross_cell_move_opaque_with_user_attribute(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})

        create(
            "table",
            "//tmp/d1/d2/d3/t",
            recursive=True,
            attributes={"external_cell_tag": 13},
        )
        set("//tmp/d1/d2/d3/t/@opaque", True)
        set("//tmp/d1/d2/d3/t/@some_user_attr", "some_value")
        set("//tmp/d1/d2/d3/@opaque", True)
        set("//tmp/d1/d2/d3/@some_user_attr", "some_value")
        set("//tmp/d1/d2/@opaque", True)
        set("//tmp/d1/d2/@some_user_attr", "some_value")
        set("//tmp/d1/@opaque", True)
        set("//tmp/d1/@some_user_attr", "some_value")

        move("//tmp/d1/d2", "//tmp/p1/d2")

        assert not exists("//tmp/d1/d2/d3/t")
        assert exists("//tmp/p1/d2/d3/t")

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")

    @authors("babenko")
    def test_cross_cell_copy_removed_account(self):
        create_account("a")
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        create("file", "//tmp/p1/f", attributes={"external_cell_tag": 13, "account": "a"})

        remove("//sys/accounts/a")
        wait(lambda: get("//sys/accounts/a/@life_stage") == "removal_pre_committed")

        with pytest.raises(YtError):
            copy("//tmp/p1/f", "//tmp/p2/f", preserve_account=True)

        remove("//tmp/p1/f")
        wait(lambda: not exists("//sys/accounts/a"))

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")
        remove("//tmp/p2")

    @authors("babenko")
    def test_cross_cell_copy_removed_bundle(self):
        create_tablet_cell_bundle("b")
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        create(
            "table",
            "//tmp/p1/t",
            attributes={"external_cell_tag": 13, "tablet_cell_bundle": "b"},
        )

        remove_tablet_cell_bundle("b")
        wait(lambda: get("//sys/tablet_cell_bundles/b/@life_stage") == "removal_pre_committed")

        with pytest.raises(YtError):
            copy("//tmp/p1/t", "//tmp/p2/t")

        remove("//tmp/p1/t")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b"))

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")
        remove("//tmp/p2")

    @authors("babenko")
    def test_create_portal_in_tx_commit(self):
        tx = start_transaction()

        entrance_id = create("portal_entrance", "//tmp/p", tx=tx, attributes={"exit_cell_tag": 11})
        exit_id = get("//tmp/p&/@exit_node_id", tx=tx)

        assert ls("//tmp/p", tx=tx) == []
        assert exists("//sys/portal_entrances/{}".format(entrance_id))
        assert exists("//sys/portal_exits/{}".format(exit_id), driver=get_driver(1))

        TABLE_PAYLOAD = [{"key": "value"}]
        create("table", "//tmp/p/t", tx=tx)
        write_table("//tmp/p/t", TABLE_PAYLOAD, tx=tx)

        assert ls("//tmp/p", tx=tx) == ["t"]
        assert read_table("//tmp/p/t", tx=tx) == TABLE_PAYLOAD

        commit_transaction(tx)

        assert ls("//tmp/p") == ["t"]
        assert exists("//sys/portal_entrances/{}".format(entrance_id))
        assert exists("//sys/portal_exits/{}".format(exit_id), driver=get_driver(1))
        assert read_table("//tmp/p/t") == TABLE_PAYLOAD

    @authors("babenko")
    def test_create_portal_in_tx_abort(self):
        tx = start_transaction()

        entrance_id = create("portal_entrance", "//tmp/p", tx=tx, attributes={"exit_cell_tag": 11})
        exit_id = get("//tmp/p&/@exit_node_id", tx=tx)

        assert ls("//tmp/p", tx=tx) == []
        assert exists("//sys/portal_entrances/{}".format(entrance_id))
        assert exists("//sys/portal_exits/{}".format(exit_id), driver=get_driver(1))

        TABLE_PAYLOAD = [{"key": "value"}]
        create("table", "//tmp/p/t", tx=tx)
        write_table("//tmp/p/t", TABLE_PAYLOAD, tx=tx)

        assert ls("//tmp/p", tx=tx) == ["t"]
        assert read_table("//tmp/p/t", tx=tx) == TABLE_PAYLOAD

        abort_transaction(tx)

        assert ls("//tmp") == []
        wait(lambda: not exists("//sys/portal_entrances/{}".format(entrance_id)))
        wait(lambda: not exists("//sys/portal_exits/{}".format(exit_id), driver=get_driver(1)))

    @authors("babenko")
    def test_mutation_id1(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

        mutation_id = generate_uuid()

        create("table", "//tmp/p/t", mutation_id=mutation_id)
        remove("//tmp/p/t", mutation_id=mutation_id, retry=True)
        assert exists("//tmp/p/t")

    @authors("babenko")
    def test_mutation_id2(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

        mutation_id = generate_uuid()

        create("table", "//tmp/p/t", mutation_id=mutation_id)
        with pytest.raises(YtError):
            remove("//tmp/p/t", mutation_id=mutation_id)

    @authors("babenko")
    def test_externalize_node(self):
        create_account("a")
        set("//sys/accounts/a/@resource_limits/tablet_count", 10)
        create_account("b")
        create_user("u")

        create(
            "map_node",
            "//tmp/m",
            attributes={"attr": "value", "acl": [make_ace("allow", "u", "write")]},
        )

        TABLE_PAYLOAD = [{"key": "value"}]
        create(
            "table",
            "//tmp/m/t1",
            attributes={
                "external": True,
                "external_cell_tag": 13,
                "account": "a",
                "attr": "t1",
            },
        )
        write_table("//tmp/m/t1", TABLE_PAYLOAD)

        create_dynamic_table(
            "//tmp/m/t2",
            external_cell_tag=13,
            optimize_for="scan",
            account="a",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            attr="t2"
        )
        sync_create_cells(1)
        sync_mount_table("//tmp/m/t2")
        insert_rows("//tmp/m/t2", [{"key": 42, "value": "hello"}])
        sync_unmount_table("//tmp/m/t2")

        FILE_PAYLOAD = b"PAYLOAD"
        create(
            "file",
            "//tmp/m/f",
            attributes={
                "external": True,
                "external_cell_tag": 13,
                "account": "b",
                "attr": "f",
            },
        )
        write_file("//tmp/m/f", FILE_PAYLOAD)

        create("document", "//tmp/m/d", attributes={"value": {"hello": "world"}})
        ct = get("//tmp/m/d/@creation_time")
        mt = get("//tmp/m/d/@modification_time")

        create(
            "map_node",
            "//tmp/m/m",
            attributes={"account": "a", "compression_codec": "brotli_8"},
        )

        create(
            "table",
            "//tmp/m/et",
            attributes={
                "external_cell_tag": 13,
                "expiration_time": "2100-01-01T00:00:00.000000Z",
            },
        )

        create(
            "map_node",
            "//tmp/m/acl1",
            attributes={"inherit_acl": True, "acl": [make_ace("deny", "u", "read")]},
        )
        create(
            "map_node",
            "//tmp/m/acl2",
            attributes={"inherit_acl": False, "acl": [make_ace("deny", "u", "read")]},
        )

        root_acl = get("//tmp/m/@effective_acl")
        explicit_acl = get("//tmp/m/@acl")
        acl1 = get("//tmp/m/acl1/@acl")
        acl2 = get("//tmp/m/acl2/@acl")

        ORCHID_MANIFEST = {"address": "someaddress"}
        create("orchid", "//tmp/m/orchid", attributes={"manifest": ORCHID_MANIFEST})

        externalize("//tmp/m", 11)

        assert get("//tmp/m/@inherit_acl")
        assert get("//tmp/m/@acl") == explicit_acl
        assert get("//tmp/m/@effective_acl") == root_acl

        assert get("//tmp/m/acl1/@inherit_acl")
        assert get("//tmp/m/acl1/@acl") == acl1

        assert not get("//tmp/m/acl2/@inherit_acl")
        assert get("//tmp/m/acl2/@acl") == acl2

        assert get("//tmp/m/@type") == "portal_exit"
        assert get("//tmp/m/@attr") == "value"

        assert read_table("//tmp/m/t1") == TABLE_PAYLOAD
        assert get("//tmp/m/t1/@account") == "a"
        assert get("//tmp/m/t1/@attr") == "t1"

        sync_mount_table("//tmp/m/t2")
        assert lookup_rows("//tmp/m/t2", [{"key": 42}]) == [{"key": 42, "value": "hello"}]
        assert get("//tmp/m/t2/@account") == "a"
        assert get("//tmp/m/t2/@attr") == "t2"

        assert read_file("//tmp/m/f") == FILE_PAYLOAD
        assert get("//tmp/m/f/@account") == "b"
        assert get("//tmp/m/f/@attr") == "f"

        assert get("//tmp/m/d") == {"hello": "world"}
        assert get("//tmp/m/d/@creation_time") == ct
        assert get("//tmp/m/d/@modification_time") == mt

        assert get("//tmp/m/m/@account") == "a"
        assert get("//tmp/m/m/@compression_codec") == "brotli_8"

        assert get("//tmp/m/et/@expiration_time") == "2100-01-01T00:00:00.000000Z"

        assert get("//tmp/m/orchid/@type") == "orchid"
        assert get("//tmp/m/orchid/@manifest") == ORCHID_MANIFEST

        shard_id = get("//tmp/m/@shard_id")
        assert get("//tmp/m/t1/@shard_id") == shard_id
        assert get("//sys/cypress_shards/{}/@account_statistics/tmp/node_count".format(shard_id)) == 6

        remove("//tmp/m")
        wait(lambda: "m" not in ls("//tmp"))
        assert not exists("//tmp/m")

    @authors("babenko")
    def test_externalize_copy_externalize(self):
        create("map_node", "//tmp/m1")

        TABLE_PAYLOAD = [{"key": "value"}]
        create("table", "//tmp/m1/t1", attributes={"external": True, "external_cell_tag": 13})
        write_table("//tmp/m1/t1", TABLE_PAYLOAD)

        create_dynamic_table(
            "//tmp/m1/t2",
            external_cell_tag=13,
            optimize_for="scan",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
        )
        sync_create_cells(1)
        sync_mount_table("//tmp/m1/t2")
        insert_rows("//tmp/m1/t2", [{"key": 42, "value": "hello"}])
        sync_unmount_table("//tmp/m1/t2")

        FILE_PAYLOAD = b"PAYLOAD"
        create("file", "//tmp/m1/f", attributes={"external": True, "external_cell_tag": 13})
        write_file("//tmp/m1/f", FILE_PAYLOAD)

        externalize("//tmp/m1", 11)

        shard_id1 = get("//tmp/m1/@shard_id")
        assert get("//tmp/m1/t1/@shard_id") == shard_id1
        assert get("//tmp/m1/t2/@shard_id") == shard_id1
        assert get("//tmp/m1/f/@shard_id") == shard_id1

        assert read_table("//tmp/m1/t1") == TABLE_PAYLOAD
        sync_mount_table("//tmp/m1/t2")
        assert lookup_rows("//tmp/m1/t2", [{"key": 42}]) == [{"key": 42, "value": "hello"}]
        sync_unmount_table("//tmp/m1/t2")
        assert read_file("//tmp/m1/f") == FILE_PAYLOAD

        copy("//tmp/m1", "//tmp/m2")
        assert get("//tmp/m2/@type") == "map_node"
        assert_items_equal(ls("//tmp"), ["m1", "m2"])
        assert get("//tmp/m1/@key") == "m1"
        assert get("//tmp/m2/@key") == "m2"

        externalize("//tmp/m2", 12)

        shard_id2 = get("//tmp/m2/@shard_id")
        assert shard_id1 != shard_id2
        assert get("//tmp/m2/t1/@shard_id") == shard_id2
        assert get("//tmp/m2/t2/@shard_id") == shard_id2
        assert get("//tmp/m2/f/@shard_id") == shard_id2

        assert read_table("//tmp/m2/t1") == TABLE_PAYLOAD
        sync_mount_table("//tmp/m2/t2")
        assert lookup_rows("//tmp/m2/t2", [{"key": 42}]) == [{"key": 42, "value": "hello"}]
        sync_unmount_table("//tmp/m2/t2")
        assert read_file("//tmp/m2/f") == FILE_PAYLOAD

    @authors("babenko")
    def test_internalize_node(self):
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        create_account("a")
        create_account("b")
        set("//sys/accounts/b/@resource_limits/tablet_count", 10)
        create_account("c")
        create_user("u")

        create("portal_entrance", "//tmp/m", attributes={"exit_cell_tag": 11})
        set("//tmp/m/@attr", "value")
        set("//tmp/m&/@acl", [make_ace("allow", "u", "write")])
        shard_id = get("//tmp/m/@shard_id")

        TABLE_PAYLOAD = [{"key": "value"}]
        create(
            "table",
            "//tmp/m/t1",
            attributes={
                "external": True,
                "external_cell_tag": 13,
                "account": "a",
                "attr": "t1",
            },
        )
        write_table("//tmp/m/t1", TABLE_PAYLOAD)

        create_dynamic_table(
            "//tmp/m/t2",
            external_cell_tag=13,
            optimize_for="scan",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            account="b",
            attr="t2",
        )
        sync_create_cells(1)
        sync_mount_table("//tmp/m/t2")
        insert_rows("//tmp/m/t2", [{"key": 42, "value": "hello"}])
        sync_unmount_table("//tmp/m/t2")

        FILE_PAYLOAD = b"PAYLOAD"
        create(
            "file",
            "//tmp/m/f",
            attributes={
                "external": True,
                "external_cell_tag": 13,
                "account": "c",
                "attr": "f",
            },
        )
        write_file("//tmp/m/f", FILE_PAYLOAD)

        create("document", "//tmp/m/d", attributes={"value": {"hello": "world"}})
        ct = get("//tmp/m/d/@creation_time")
        mt = get("//tmp/m/d/@modification_time")

        create(
            "map_node",
            "//tmp/m/m",
            attributes={"account": "a", "compression_codec": "brotli_8"},
        )

        create(
            "table",
            "//tmp/m/et",
            attributes={
                "external_cell_tag": 13,
                "expiration_time": "2100-01-01T00:00:00.000000Z",
            },
        )

        create(
            "map_node",
            "//tmp/m/acl1",
            attributes={"inherit_acl": True, "acl": [make_ace("deny", "u", "read")]},
        )
        create(
            "map_node",
            "//tmp/m/acl2",
            attributes={"inherit_acl": False, "acl": [make_ace("deny", "u", "read")]},
        )

        explicit_acl = get("//tmp/m/@acl")
        # NB: Getting @acl actually occur on primary cell, but for @effective_acl we need to wait for synchronization.
        wait(lambda: get("//tmp/m/@effective_acl") == get("//tmp/m&/@effective_acl"))
        root_acl = get("//tmp/m/@effective_acl")
        acl1 = get("//tmp/m/acl1/@acl")
        acl2 = get("//tmp/m/acl2/@acl")

        ORCHID_MANIFEST = {"address": "someaddress"}
        create("orchid", "//tmp/m/orchid", attributes={"manifest": ORCHID_MANIFEST})

        internalize("//tmp/m")

        wait(lambda: not exists("#" + shard_id))

        assert get("//tmp/m/@inherit_acl")
        assert get("//tmp/m/@effective_acl") == root_acl
        assert get("//tmp/m/@acl") == explicit_acl

        assert get("//tmp/m/acl1/@inherit_acl")
        assert get("//tmp/m/acl1/@acl") == acl1

        assert not get("//tmp/m/acl2/@inherit_acl")
        assert get("//tmp/m/acl2/@acl") == acl2

        assert get("//tmp/m/@type") == "map_node"
        assert get("//tmp/m/@attr") == "value"

        assert read_table("//tmp/m/t1") == TABLE_PAYLOAD
        assert get("//tmp/m/t1/@account") == "a"
        assert get("//tmp/m/t1/@attr") == "t1"

        sync_mount_table("//tmp/m/t2")
        assert lookup_rows("//tmp/m/t2", [{"key": 42}]) == [{"key": 42, "value": "hello"}]
        sync_unmount_table("//tmp/m/t2")
        assert get("//tmp/m/t2/@account") == "b"
        assert get("//tmp/m/t2/@attr") == "t2"

        assert read_file("//tmp/m/f") == FILE_PAYLOAD
        assert get("//tmp/m/f/@account") == "c"
        assert get("//tmp/m/f/@attr") == "f"

        assert get("//tmp/m/d") == {"hello": "world"}
        assert get("//tmp/m/d/@creation_time") == ct
        assert get("//tmp/m/d/@modification_time") == mt

        assert get("//tmp/m/m/@account") == "a"
        assert get("//tmp/m/m/@compression_codec") == "brotli_8"

        assert get("//tmp/m/et/@expiration_time") == "2100-01-01T00:00:00.000000Z"

        assert get("//tmp/m/orchid/@type") == "orchid"
        assert get("//tmp/m/orchid/@manifest") == ORCHID_MANIFEST

        assert get("//tmp/m/t1/@shard_id") == get("//tmp/@shard_id")
        assert get("//tmp/m/t2/@shard_id") == get("//tmp/@shard_id")

    @authors("babenko")
    def test_bulk_insert_yt_11194(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

        sync_create_cells(1)
        create(
            "table",
            "//tmp/p/target",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "external": False,
            },
        )
        sync_mount_table("//tmp/p/target")

        create(
            "table",
            "//tmp/p/source",
            attributes={"external": True, "external_cell_tag": 12},
        )

        PAYLOAD = [{"key": 1, "value": "blablabla"}]
        write_table("//tmp/p/source", PAYLOAD)

        map(in_="//tmp/p/source", out="<append=%true>//tmp/p/target", command="cat")

        assert select_rows("* from [//tmp/p/target]") == PAYLOAD

    @authors("babenko")
    def test_root_shard_names(self):
        root_shard_count = 0
        for shard in ls("//sys/cypress_shards", attributes=["name"]):
            if shard.attributes["name"].startswith("root:"):
                root_shard_count += 1
        assert root_shard_count == 1 + get("//sys/secondary_masters/@count")

    @authors("babenko")
    def test_portal_shard_name(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        shard_id = get("//tmp/p/@shard_id")
        assert get("#{}/@name".format(shard_id)) == "portal://tmp/p"

    @authors("babenko")
    def test_externalize_shard_name(self):
        create("map_node", "//tmp/m")
        externalize("//tmp/m", 11)
        shard_id = get("//tmp/m/@shard_id")
        assert get("#{}/@name".format(shard_id)) == "portal://tmp/m"

    @authors("babenko")
    def test_portal_get_set_shard_name(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        shard_id = get("//tmp/p/@shard_id")
        set("#{}/@name".format(shard_id), "shard_name")
        assert get("#{}/@name".format(shard_id)) == "shard_name"

    @authors("babenko")
    def test_concat_in_tx(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        tx = start_transaction()
        create("table", "//tmp/p/t1")
        write_table("//tmp/p/t1", [{"key": "value1"}], tx=tx)
        create("table", "//tmp/p/t2")
        write_table("//tmp/p/t2", [{"key": "value2"}], tx=tx)
        create("table", "//tmp/p/t3")
        concatenate(["//tmp/p/t1", "//tmp/p/t2"], "//tmp/p/t3", tx=tx)
        commit_transaction(tx)
        assert read_table("//tmp/p/t3") == [{"key": "value1"}, {"key": "value2"}]

    @authors("gritukan")
    def test_granular_externalize(self):
        create("map_node", "//tmp/m1", attributes={"opaque": True})
        create("map_node", "//tmp/m1/m21", attributes={"opaque": True})
        create("map_node", "//tmp/m1/m22", attributes={"opaque": True})
        create("map_node", "//tmp/m1/m21/m31", attributes={"opaque": True})
        create("map_node", "//tmp/m1/m21/m32", attributes={"opaque": True})
        create("map_node", "//tmp/m1/m22/m33", attributes={"opaque": True})
        create("map_node", "//tmp/m1/m22/m34", attributes={"opaque": True})

        for table_directory in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            table_path = "//tmp/m1" + table_directory + "/table"
            create(
                "table",
                table_path,
                attributes={"external": True, "external_cell_tag": 11},
            )
            write_table(table_path, [{"key": table_directory}])

        externalize("//tmp/m1", 12)

        for table_directory in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            table_path = "//tmp/m1" + table_directory + "/table"
            assert read_table(table_path) == [{"key": table_directory}]

    @authors("gritukan")
    def test_granular_internalize(self):
        create(
            "portal_entrance",
            "//tmp/p1",
            attributes={"exit_cell_tag": 11, "opaque": True},
        )
        create("map_node", "//tmp/p1/m21", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m22", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m21/m31", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m21/m32", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m22/m33", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m22/m34", attributes={"opaque": True})

        for table_directory in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            table_path = "//tmp/p1" + table_directory + "/table"
            create(
                "table",
                table_path,
                attributes={"external": True, "external_cell_tag": 12},
            )
            write_table(table_path, [{"key": table_directory}])

        internalize("//tmp/p1")

        for table_directory in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            table_path = "//tmp/p1" + table_directory + "/table"
            assert read_table(table_path) == [{"key": table_directory}]

    @authors("gritukan")
    def test_granular_cross_cell_copy(self):
        create(
            "portal_entrance",
            "//tmp/p1",
            attributes={"exit_cell_tag": 11, "opaque": True},
        )
        create(
            "portal_entrance",
            "//tmp/p2",
            attributes={"exit_cell_tag": 12, "opaque": True},
        )

        create("map_node", "//tmp/p1/m1", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m1/m21", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m1/m22", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m1/m21/m31", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m1/m21/m32", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m1/m22/m33", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m1/m22/m34", attributes={"opaque": True})

        for document_dir in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            document_path = "//tmp/p1/m1" + document_dir + "/doc"
            create("document", document_path, attributes={"value": document_dir})

        copy("//tmp/p1/m1", "//tmp/p2/m1")

        for document_dir in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            document_path = "//tmp/p1/m1" + document_dir + "/doc"
            assert get(document_path) == document_dir

    @authors("shakurov")
    def test_link_not_externalizable(self):
        create("map_node", "//tmp/m")
        link("//tmp/m", "//tmp/l")
        with pytest.raises(YtError):
            externalize("//tmp/l", 11)

    @authors("shakurov")
    def test_link_not_internalizable(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        link("//tmp/p", "//tmp/l")
        with pytest.raises(YtError):
            internalize("//tmp/l")

    @authors("shakurov")
    @pytest.mark.parametrize("remove_source", [False, True])
    def test_cross_shard_copy_link1(self, remove_source):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p/t", attributes={"external_cell_tag": 12})
        link("//tmp/p/t", "//tmp/l1")
        link("//tmp/p/t", "//tmp/p/l2")

        copier = move if remove_source else copy

        with pytest.raises(YtError):
            copier("//tmp/l1", "//tmp/l1_copy")

        copier("//tmp/p/l2", "//tmp/l2_copy")

        get("//tmp/l2_copy&/@type") == "table"
        assert exists("//tmp/p/t&") == (not remove_source)
        assert get("//tmp/p/l2&/@target_path") == "//tmp/p/t"
        assert get("//tmp/p/l2&/@broken") == remove_source

    @authors("shakurov")
    @pytest.mark.parametrize("remove_source", [False, True])
    def test_cross_shard_copy_link2(self, remove_source):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/t", attributes={"external_cell_tag": 12})
        link("//tmp/t", "//tmp/l1")
        link("//tmp/t", "//tmp/p/l2")

        copier = move if remove_source else copy

        copier("//tmp/l1", "//tmp/p/l1_copy")

        assert get("//tmp/p/l1_copy&/@type") == "table"

        assert exists("//tmp/t&") == (not remove_source)
        assert get("//tmp/l1&/@target_path") == "//tmp/t"
        assert get("//tmp/l1&/@broken") == remove_source

        with pytest.raises(YtError):
            copier("//tmp/p/l2", "//tmp/p/l2_copy")

    @authors("shakurov")
    def test_link_to_portal_not_broken(self):
        create("map_node", "//tmp/m")
        link("//tmp/m", "//tmp/l")

        externalize("//tmp/m", 11)

        assert not get("//tmp/l&/@broken")

    @authors("shakurov")
    def test_remove_portal_not_permitted_by_acl(self):
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        create_user("u")
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

        assert get("//tmp/p/@inherit_acl")
        # NB: denying removal for portal exit only!
        set("//tmp/p&/@acl", [make_ace("deny", "u", "remove", "object_and_descendants")])
        wait(lambda: get("//tmp/p&/@effective_acl") == get("//tmp/p/@effective_acl"))

        assert check_permission("u", "remove", "//tmp/p")["action"] == "deny"

        with pytest.raises(YtError):
            remove("//tmp/p", authenticated_user="u")

    @authors("shakurov")
    def test_cross_shard_copy_src_tx_yt_13015(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})

        create("table", "//tmp/t", attributes={"external_cell_tag": 12})
        write_table("//tmp/t", {"a": "b"})

        tx = start_transaction()
        write_table("//tmp/t", {"c": "d"}, tx=tx)

        copy("//tmp/t", "//tmp/p1/t", tx=tx)
        commit_transaction(tx)

        assert read_table("//tmp/p1/t") == [{"c": "d"}]

    @authors("shakurov")
    def test_cross_shard_copy_dynamic_table_attrs_on_static_table(self):
        attributes = {
            "atomicity": "full",
            "commit_ordering": "weak",
            "in_memory_mode": "uncompressed",
            "enable_dynamic_store_read": True,
            "profiling_mode": "path",
            "profiling_tag": "some_tag",
            "compression_codec": "lzma_1",
            "erasure_codec": "reed_solomon_3_3",
            "hunk_erasure_codec": "reed_solomon_6_3",
            "external_cell_tag": 12  # To be removed below.
        }
        create("table", "//tmp/t", attributes=attributes)
        del attributes["external_cell_tag"]

        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})

        # Must not crash.
        copy("//tmp/t", "//tmp/p1/t")

        copy_attributes = get("//tmp/p1/t/@")
        for k, v in attributes.items():
            assert copy_attributes[k] == v

    @authors("shakurov")
    def test_recursive_resource_usage_portal(self):
        create("map_node", "//tmp/d")
        create("portal_entrance", "//tmp/d/p1", attributes={"exit_cell_tag": 11})

        create("table", "//tmp/d/t", attributes={"external_cell_tag": 12})
        write_table("//tmp/d/t", {"a": "b"})
        table_usage = get("//tmp/d/t/@resource_usage")

        create("table", "//tmp/d/p1/t", attributes={"external_cell_tag": 12})
        write_table("//tmp/d/p1/t", {"a": "b"})

        expected_usage = {
            "node_count": 5,  # two tables + one map node + portal entrance and exit
            "disk_space": 2 * table_usage["disk_space"],
            "disk_space_per_medium": {"default": 2 * table_usage["disk_space_per_medium"]["default"]},
            "chunk_count": 2 * table_usage["chunk_count"],
        }
        total_usage = get("//tmp/d/@recursive_resource_usage")
        assert cluster_resources_equal(total_usage, expected_usage)

    @authors("babenko")
    def test_id_path_resolve(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("map_node", "//tmp/p/a")
        create("map_node", "//tmp/p/b")
        create("document", "//tmp/p/b/x", attributes={"value": {"hello": "worldx"}})
        create("document", "//tmp/p/b/y", attributes={"value": {"hello": "worldy"}})
        id = get("//tmp/p/b/@id")
        assert get("#{}/x".format(id)) == {"hello": "worldx"}

    @authors("s-v-m")
    def test_create_map_node_with_ignore_existing(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("map_node", "//tmp/p", ignore_existing=True)
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            create("map_node", "//tmp/t", ignore_existing=True)

    @authors("kvk1920")
    def test_cross_cell_copy_with_prerequisite_transaction_ids(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/t", attributes={"external_cell_tag": 12})

        write_table("//tmp/t", {"a": "b"})
        tx = start_transaction()
        copy("//tmp/t", "//tmp/p/t", prerequisite_transaction_ids=[tx])
        commit_transaction(tx)

        assert exists("//tmp/p/t")
        assert read_table("//tmp/p/t") == [{"a": "b"}]

        with raises_yt_error("Prerequisite check failed"):
            copy("//tmp/t", "//tmp/p/t2", prerequisite_transaction_ids=[tx])

        assert not exists("//tmp/p/t2")

    @authors("kvk1920")
    def test_inheritable_attributes_synchronization(self):
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        set("//sys/@config/cypress_manager/enable_portal_exit_effective_inherited_attributes", True)
        create("map_node", "//tmp/dir_with_attrs")
        create("portal_entrance", "//tmp/dir_with_attrs/p", attributes={"exit_cell_tag": 11})

        def check_inherited_codec():
            create("table", "//tmp/dir_with_attrs/p/t")
            result = get("//tmp/dir_with_attrs/p/t/@compression_codec") == get("//tmp/dir_with_attrs/@compression_codec")
            remove("//tmp/dir_with_attrs/p/t")
            return result

        set("//tmp/dir_with_attrs/@compression_codec", "lz4")
        wait(check_inherited_codec)
        set("//tmp/dir_with_attrs/@compression_codec", "zstd_17")
        wait(check_inherited_codec)

    @authors("kvk1920")
    def test_smart_pointer_in_inheritable_attributes(self):
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        set("//sys/@config/cypress_manager/enable_portal_exit_effective_inherited_attributes", True)
        create("map_node", "//tmp/d")
        create("portal_entrance", "//tmp/d/p", attributes={"exit_cell_tag": 11})
        create_tablet_cell_bundle("buggy_bundle")
        set("//tmp/d/@tablet_cell_bundle", "buggy_bundle")

        def check_inherited_bundle():
            create("table", "//tmp/d/p/t")
            result = get("//tmp/d/p/t/@tablet_cell_bundle") == "buggy_bundle"
            remove("//tmp/d/p/t")
            return result

        wait(check_inherited_bundle)

    @authors("kvk1920")
    def test_effective_acl_synchronization(self):
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        create_user("dog")
        create_user("cat")
        create_user("rat")
        folder = "//tmp/dir_with_acl"
        create(
            "map_node",
            folder,
            attributes={
                "acl": [
                    make_ace("allow", "dog", "write"),
                    make_ace("allow", "dog", "read"),
                    make_ace("deny", "cat", "read"),
                ],
            })
        portal_exit = folder + "/p"
        portal_entrance = portal_exit + "&"
        create("portal_entrance", portal_exit, attributes={
            "inherit_acl": True,
            "exit_cell_tag": 11,
            "acl": [make_ace("allow", "rat", "read")],
        })
        get(portal_entrance + "/@acl", authenticated_user="dog")
        assert get(portal_entrance + "/@inherit_acl")
        assert "allow" == check_permission("dog", "read", portal_entrance)["action"]
        assert "allow" == check_permission("dog", "write", portal_entrance)["action"]
        assert "deny" == check_permission("cat", "read", portal_exit)["action"]
        assert "allow" == check_permission("dog", "read", portal_exit)["action"]
        table = portal_exit + "/t"
        create("table", table, attributes={"inherit_acl": True})
        assert "allow" == check_permission("dog", "write", table)["action"]
        assert "deny" == check_permission("cat", "read", table)["action"]
        assert "allow" == check_permission("rat", "read", table)["action"]

        set(portal_entrance + "/@inherit_acl", False)
        wait(lambda: not get(f"{portal_exit}/@inherit_acl"))
        assert "deny" == check_permission("dog", "write", table)["action"]
        assert "deny" == check_permission("cat", "read", table)["action"]
        assert "allow" == check_permission("rat", "read", table)["action"]

        set(folder + "/@acl", [
            make_ace("deny", "dog", "read"),
            make_ace("allow", "cat", "write"),
        ])
        set(portal_entrance + "/@acl", [make_ace("deny", "rat", "read")])
        set(portal_entrance + "/@inherit_acl", True)
        wait(lambda: get(portal_exit + "/@inherit_acl"))
        assert "deny" == check_permission("dog", "read", table)["action"]
        assert "allow" == check_permission("cat", "write", table)["action"]
        assert "deny" == check_permission("rat", "read", table)["action"]

    @authors("kvk1920")
    def test_empty_annotation(self):
        remove("//sys/@config/cypress_manager/portal_synchronization_period")
        create("portal_entrance", "//tmp/p", attributes={
            "exit_cell_tag": 11,
        })
        assert get("//tmp/p/@annotation") == yson.YsonEntity()
        assert get("//tmp/p/@annotation_path") == yson.YsonEntity()

    @authors("kvk1920")
    def test_annotation_synchronization(self):
        remove("//sys/@config/cypress_manager/portal_synchronization_period")
        create("map_node", "//tmp/d", attributes={"annotation": "qwerty"})
        create("portal_entrance", "//tmp/d/p", attributes={"annotation": None, "exit_cell_tag": 11})
        assert "qwerty" == get("//tmp/d/p/@annotation")
        assert "//tmp/d" == get("//tmp/d/p/@annotation_path")
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        remove("//tmp/d/@annotation")
        wait(lambda: yson.YsonEntity() == get("//tmp/d/@annotation"))
        wait(lambda: yson.YsonEntity() == get("//tmp/d/p/@annotation_path"))
        set("//tmp/d/p&/@annotation", "abc")

        wait(lambda: "abc" == get("//tmp/d/p/@annotation"))
        assert "//tmp/d/p" == get("//tmp/d/p/@annotation_path")

    @authors("kvk1920")
    def test_portal_creation_under_nested_transaction_is_forbidden(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)
        with raises_yt_error('Portal creation under nested transaction is forbidden'):
            create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11}, tx=tx2)
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11}, tx=tx1)

##################################################################


class TestPortalsCypressProxy(TestPortals):
    NUM_CYPRESS_PROXIES = 1


##################################################################

class TestResolveCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("babenko")
    def test_cache_populated_on_resolve(self):
        create("map_node", "//tmp/dir1/dir2", recursive=True)
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 11})
        assert not get("//tmp/dir1/@resolve_cached")
        assert not get("//tmp/dir1/dir2/@resolve_cached")
        assert not get("//tmp/dir1/dir2/p&/@resolve_cached")

        create("table", "//tmp/dir1/dir2/p/t")
        assert get("//@resolve_cached")
        assert get("//tmp/@resolve_cached")
        assert get("//tmp/dir1/@resolve_cached")
        assert get("//tmp/dir1/dir2/@resolve_cached")
        assert get("//tmp/dir1/dir2/p&/@resolve_cached")

    @authors("babenko")
    def test_cache_populated_on_resolve_with_link(self):
        create("map_node", "//tmp/dir1/dir2", recursive=True)
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 11})
        assert not get("//tmp/dir1/@resolve_cached")
        assert not get("//tmp/dir1/dir2/@resolve_cached")
        assert not get("//tmp/dir1/dir2/p&/@resolve_cached")

        create("table", "//tmp/dir1/dir2/p/t")
        assert get("//tmp/dir1/dir2/p&/@resolve_cached")

        link("//tmp/dir1/dir2", "//tmp/l")
        assert not get("//tmp/l&/@resolve_cached")
        assert get("//tmp/l/p/t/@type") == "table"
        assert get("//tmp/l&/@resolve_cached")

    @authors("babenko")
    def test_cache_purged_on_lock(self):
        create("map_node", "//tmp/dir1/dir2", recursive=True)
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/dir1/dir2/p/t")
        assert get("//tmp/dir1/dir2/p&/@resolve_cached")
        tx = start_transaction()
        lock("//tmp/dir1", tx=tx)
        assert not get("//tmp/dir1/@resolve_cached")
        assert not get("//tmp/dir1/dir2/@resolve_cached")
        assert not get("//tmp/dir1/dir2/p&/@resolve_cached")

    @authors("babenko")
    def test_cache_purged_on_remove(self):
        create("map_node", "//tmp/dir1/dir2", recursive=True)
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/dir1/dir2/p/t")
        assert get("//tmp/dir1/dir2/p&/@resolve_cached")
        remove("//tmp/dir1/dir2/p")
        wait(lambda: not get("//tmp/dir1/@resolve_cached"))

    @authors("babenko")
    def test_cache_trimmed(self):
        create("map_node", "//tmp/dir1/dir2a", recursive=True)
        create("map_node", "//tmp/dir1/dir2b", recursive=True)
        create("portal_entrance", "//tmp/dir1/dir2a/p", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/dir1/dir2b/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/dir1/dir2a/p/t")
        create("table", "//tmp/dir1/dir2b/p/t")
        assert get("//tmp/dir1/dir2a/p&/@resolve_cached")
        assert get("//tmp/dir1/dir2b/p&/@resolve_cached")
        tx = start_transaction()
        lock("//tmp/dir1/dir2a", tx=tx)
        assert get("//@resolve_cached")
        assert get("//tmp/@resolve_cached")
        assert get("//tmp/dir1/@resolve_cached")
        assert not get("//tmp/dir1/dir2a/@resolve_cached")
        assert not get("//tmp/dir1/dir2a/p&/@resolve_cached")
        assert get("//tmp/dir1/dir2b/@resolve_cached")
        assert get("//tmp/dir1/dir2b/p&/@resolve_cached")

    @authors("s-v-m")
    def test_link_through_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p/t")
        set("//tmp/p/t-non-existing", 1)
        link("//tmp/p/t-non-existing", "//tmp/l-bad")
        remove("//tmp/p/t-non-existing")
        assert get("//tmp/l-bad&/@broken")
        link("//tmp/p/t", "//tmp/l-ok")
        assert not get("//tmp/l-ok&/@broken")

    @authors("shakurov")
    def test_link_resolution_by_resolve_cache_yt_13909(self):
        create("portal_entrance", "//tmp/d/p", recursive=True, attributes={"exit_cell_tag": 11})
        link("//tmp/d", "//tmp/d1")

        # Make sure paths are cached.
        get("//tmp/d/p/@id")
        get("//tmp/d1/p/@id")

        assert get("//tmp/d/@resolve_cached")
        assert get("//tmp/d1/@resolve_cached")
        assert get("//tmp/d1&/@resolve_cached")

        assert get("//tmp/d/@type") == "map_node"
        assert get("//tmp/d1/@type") == "map_node"  # Must not throw.
        assert get("//tmp/d1&/@type") == "link"

    @authors("gritukan")
    def test_nested_portals_are_forbidden(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p/q", attributes={"exit_cell_tag": 12})
