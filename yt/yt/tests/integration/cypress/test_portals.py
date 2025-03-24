from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, raises_yt_error, wait, create, ls, get, set, copy, move, remove, link,
    exists, concatenate, create_account, create_user, create_tablet_cell_bundle,
    remove_tablet_cell_bundle, make_ace, check_permission, start_transaction,
    abort_transaction, commit_transaction, lock, externalize, internalize, select_rows,
    read_file, write_file, read_table, write_table, map, sync_create_cells,
    create_dynamic_table, insert_rows, lookup_rows, sync_mount_table, sync_unmount_table,
    sync_freeze_table, get_singular_chunk_id, cluster_resources_equal, get_driver,
    generate_uuid, abort_all_transactions)

import yt_error_codes

from yt_helpers import get_current_time

from yt.common import YtError
from yt.test_helpers import assert_items_equal
from flaky import flaky
import yt.yson as yson

from datetime import timedelta
import pytest
import random

import time

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

    @authors("nadya02")
    def test_disable_cross_cell_copying(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        create("file", "//tmp/p1/f", attributes={"external_cell_tag": 13})

        with raises_yt_error("Cross-cell \"copy\"/\"move\" command is explicitly disabled by request options"):
            copy("//tmp/p1/f", "//tmp/p2/f", enable_cross_cell_copying=False)

        with raises_yt_error("Cross-cell \"copy\"/\"move\" command is explicitly disabled by request options"):
            move("//tmp/p1/f", "//tmp/p2/f2", enable_cross_cell_copying=False)

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
    def test_cross_cell_links_forbidden(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        with pytest.raises(YtError):
            link("//tmp", "//tmp/p/l")

    @authors("shakurov")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_intra_cell_cross_shard_links(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 11})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p1")
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p2")
        table_id = create("table", "//tmp/p2/t")
        link("//tmp/p2/t", "//tmp/p1/l")
        assert get("//tmp/p1/l/@id") == table_id

    @authors("shakurov")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_intra_cell_cross_shard_links_disabled(self, purge_resolve_cache):
        set("//sys/@config/cypress_manager/enable_intra_cell_cross_shard_links", False)
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 11})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p1")
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p2")
        create("table", "//tmp/p2/t")
        link("//tmp/p2/t", "//tmp/p1/l")
        with raises_yt_error('Link target path must start with //tmp/p1'):
            get("//tmp/p1/l/@id")

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

    @authors("h0pless")
    def test_cross_cell_copy_banned_for_list_nodes(self):
        create_account("a")
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        create("map_node", "//tmp/p1/m", attributes={"account": "a"})
        set("//sys/@config/cypress_manager/forbid_list_node_creation", False)
        create("list_node", "//tmp/p1/m/l")
        set("//sys/@config/cypress_manager/forbid_list_node_creation", True)

        with pytest.raises(YtError):
            copy("//tmp/p1/m", "//tmp/p2/m", preserve_account=True)

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

    # This test seems broken
    @authors("shakurov", "h0pless")
    def test_cross_cell_move_opaque_with_user_attribute(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})

        create(
            "table",
            "//tmp/d1/d2/d3/t",
            recursive=True,
            attributes={
                "external_cell_tag": 13,
                "schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]},
        )
        set("//tmp/d1/d2/d3/t/@opaque", True)
        set("//tmp/d1/d2/d3/t/@some_user_attr", "some_value")
        set("//tmp/d1/d2/d3/@opaque", True)
        set("//tmp/d1/d2/d3/@some_user_attr", "some_value")
        set("//tmp/d1/d2/@opaque", True)
        set("//tmp/d1/d2/@some_user_attr", "some_value")
        set("//tmp/d1/@opaque", True)
        set("//tmp/d1/@some_user_attr", "some_value")

        assert get("//tmp/d1/d2/d3/t/@schema_mode") == "strong"

        move("//tmp/d1/d2", "//tmp/p1/d2")

        assert not exists("//tmp/d1/d2/d3/t")
        assert exists("//tmp/p1/d2/d3/t")
        assert get("//tmp/p1/d2/d3/t/@schema_mode") == "strong"

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")

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

    @authors("koloshmet")
    @flaky(max_runs=3)
    def test_force_link_through_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})

        create("map_node", "//tmp/p/d")
        create("map_node", "//tmp/p/d1")
        create("map_node", "//tmp/d")

        set("//sys/@config/object_manager/gc_sweep_period", 15000)

        start = time.time()

        time.sleep(2)

        link("//tmp/p/d", "//tmp/d/l")

        for i in range(10):
            get("//tmp/d/l/@")
        assert get("//tmp/d/@resolve_cached") and get("//tmp/d/l&/@resolve_cached")

        link("//tmp/p/d1", "//tmp/d/l", force=True)
        assert get("//tmp/d/l&/@target_path") == "//tmp/p/d1"

        for i in range(10):
            get("//tmp/d/l/@")
        assert get("//tmp/d/@resolve_cached") and get("//tmp/d/l&/@resolve_cached")

        assert time.time() - start < 15

    @authors("koloshmet")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    @pytest.mark.parametrize("target_on_primary", [False, True])
    def test_cross_cell_links_resolve(self, purge_resolve_cache, target_on_primary):
        set("//sys/@config/cypress_manager/enable_cross_cell_links", True)

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("map_node", "//tmp/p/d")
        create("table", "//tmp/p/d/p")

        target = "//tmp" if target_on_primary else "//tmp/p/d"
        link(target, "//tmp/p/l")
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p/l&")

        assert ls("//tmp/p/l") == ["p"]

    @authors("koloshmet")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_cross_cell_links_resolve_multiportal(self, purge_resolve_cache):
        set("//sys/@config/cypress_manager/enable_cross_cell_links", True)

        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        link("//tmp", "//tmp/p1/l")
        link("//tmp/p1/l", "//tmp/p2/l")
        link("//tmp/p2/l", "//tmp/p1/l2")
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p1/l&")
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p1/l2&")
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p2/l&")

        assert sorted(ls("//tmp/p1/l2")) == ["p1", "p2"]

    @authors("koloshmet")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    @pytest.mark.parametrize("target_on_primary", [False, True])
    def test_cross_cell_links_resolve_cycle(self, purge_resolve_cache, target_on_primary):
        set("//sys/@config/cypress_manager/enable_cross_cell_links", True)

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("map_node", "//tmp/p/d")

        target_dir = "//tmp" if target_on_primary else "//tmp/p/d"
        target_link = target_dir + "/l"

        link(target_dir, "//tmp/p/l")
        link("//tmp/p/l", target_link)

        if not target_on_primary:
            with raises_yt_error("link is cyclic"):
                link(target_link, "//tmp/p/l", force=True)
            return

        link(target_link, "//tmp/p/l", force=True)

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p/l&")
        _maybe_purge_resolve_cache(purge_resolve_cache, target_link + '&')

        with raises_yt_error("exceeds resolve depth limit"):
            ls("//tmp/p/l")

    @authors("koloshmet")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_cross_cell_links_resolve_broken(self, purge_resolve_cache):
        set("//sys/@config/cypress_manager/enable_cross_cell_links", True)

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/t")
        link("//tmp/t", "//tmp/p/l")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p/l&")

        assert not get("//tmp/p/l&/@broken")

        move("//tmp/t", "//tmp/t1")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p/l&")

        assert get("//tmp/p/l&/@broken")
        assert get("//tmp/p/l&/@target_path") == "//tmp/t"

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

    @authors("shakurov")
    def test_link_not_externalizable(self):
        create("map_node", "//tmp/m")
        link("//tmp/m", "//tmp/l")
        with raises_yt_error("A link node cannot be externalized; consider externalizing its target instead"):
            externalize("//tmp/l", 11)

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
        # NB: Denying removal for portal exit only!
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
        create_user("bat")
        set("//tmp/@inherit_acl", False)
        set("//tmp/@acl", [])
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
        assert "deny" == check_permission("bat", "read", portal_exit)["action"]  # No matching ACE.
        table = portal_exit + "/t"
        create("table", table, attributes={"inherit_acl": True})
        assert "allow" == check_permission("dog", "write", table)["action"]
        assert "deny" == check_permission("cat", "read", table)["action"]
        assert "allow" == check_permission("rat", "read", table)["action"]
        assert "deny" == check_permission("bat", "read", table)["action"]  # No matching ACE.

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

    @authors("h0pless")
    def test_internalize_deprication(self):
        create("portal_entrance", "//tmp/portal", attributes={"exit_cell_tag": 11})
        with raises_yt_error("Node internalization is deprecated and is no longer possible."):
            internalize("//tmp/m")
        remove("//tmp/portal")


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


################################################################################

class TestCrossCellCopy(YTEnvSetup):
    NUM_TEST_PARTITIONS = 2

    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True
    NUM_SCHEDULERS = 1

    FILE_PAYLOAD = b"FILE PAYLOAD SOME BYTES AND STUFF"
    TABLE_PAYLOAD = [{"key": 42, "value": "the answer"}]

    COMMAND = "copy"

    AVAILABLE_ACCOUNTS = [
        "george",
        "geoff",
        "geronimo",
        "golem",
    ]

    AVAILABLE_USERS = [
        "john",
        "jim",
        "jason",
        "jolem",
    ]

    PRESERVABLE_ATTRIBUTES = {
        "account": False,
        "creation_time": False,
        "modification_time": False,
        "expiration_time": False,
        "expiration_timeout": False,
        "owner": False,
        "acl": False,
    }

    # These attributes can change or be the same depending on the context.
    CONTEXT_DEPENDENT_ATTRIBUTES = [
        "access_counter",
        "actual_tablet_state",
        "expected_tablet_state",
        "tablet_state",
        "unflushed_timestamp",
    ]

    # These attributes have to change when cross-cell copying.
    EXPECTED_ATTRIBUTE_CHANGES = [
        "access_time",

        # Changed due to cell change:
        "id",
        "native_cell_tag",
        "parent_id",
        "shard_id",
        "schema_id",
        "cow_cookie",

        # Changed due to test design:
        "revision",
        "attribute_revision",
        "native_content_revision",
        "content_revision",
    ]

    def setup_method(self, method):
        super(TestCrossCellCopy, self).setup_method(method)

        # Setting defaults for meta state.
        self.PRESERVABLE_ATTRIBUTES = {
            "account": False,
            "creation_time": False,
            "modification_time": False,
            "expiration_time": False,
            "expiration_timeout": False,
            "owner": False,
            "acl": False,
        }
        self.SRC_ATTRIBUTES = {}

        for account in self.AVAILABLE_ACCOUNTS:
            create_account(account, ignore_existing=True)
        for user in self.AVAILABLE_USERS:
            create_user(user, ignore_existing=True)
        create("portal_entrance", "//tmp/portal", attributes={"exit_cell_tag": 11})

    def teardown_method(self, method):
        abort_all_transactions()
        # XXX(babenko): cleanup is weird
        remove("//tmp/portal")
        super(TestCrossCellCopy, self).teardown_method(method)

    def _validate_attribute_consistency_for_node(self, src_path, dst_path, tx):
        src_attributes = get(f"{src_path}/@", tx=tx) if self.COMMAND == "copy" else self.SRC_ATTRIBUTES[src_path]
        dst_attributes = get(f"{dst_path}/@", tx=tx)

        for attribute_key in src_attributes.keys():
            # Preservable attributes should be tested directly.
            if attribute_key in self.CONTEXT_DEPENDENT_ATTRIBUTES or attribute_key in self.PRESERVABLE_ATTRIBUTES.keys():
                # This if was added for move command, because pre-move we get data from under a tx, but post-move we look at it without a tx.
                # Maybe it can be avoided. Think about it.
                if attribute_key in dst_attributes:
                    del dst_attributes[attribute_key]
                continue

            src_attribute_value = src_attributes[attribute_key]
            dst_attribute_value = dst_attributes[attribute_key]

            assert (src_attribute_value != dst_attribute_value) == (attribute_key in self.EXPECTED_ATTRIBUTE_CHANGES)
            del dst_attributes[attribute_key]

        assert len(dst_attributes) == 0

    def _validate_preservable_attributes(self, src_path, dst_path, tx):
        src_attributes = get(f"{src_path}/@", tx=tx) if self.COMMAND == "copy" else self.SRC_ATTRIBUTES[src_path]
        dst_attributes = get(f"{dst_path}/@", tx=tx)

        for attribute_key, equality_expected in self.PRESERVABLE_ATTRIBUTES.items():
            if equality_expected:
                if attribute_key not in src_attributes.keys():
                    assert attribute_key not in dst_attributes.keys()
                else:
                    assert src_attributes[attribute_key] == dst_attributes[attribute_key]
            elif attribute_key in dst_attributes.keys():
                assert src_attributes[attribute_key] != dst_attributes[attribute_key]

    def _populate_preservable_attributes(self, path, tx):
        set(f"{path}/@account", random.choice(self.AVAILABLE_ACCOUNTS))
        user = random.choice(self.AVAILABLE_USERS)
        set(f"{path}/@owner", user)
        set(f"{path}/@acl", [
            make_ace("allow", user, "read"),
            make_ace("allow", user, "write"),
            ])
        set(f"{path}/@expiration_time", f"{random.randint(2050, 2150)}-01-01T00:00:00.000000Z", tx=tx)
        set(f"{path}/@expiration_timeout", random.randint(2800000, 3200000), tx=tx)
        # Creation and modification difference is easy to spot without setting anything.

    def create_map_node(self, path, tx="0-0-0-0"):
        create(
            "map_node",
            path,
            attributes={"account": random.choice(self.AVAILABLE_ACCOUNTS)},
            tx=tx)

    def create_file(self, path, tx="0-0-0-0"):
        create(
            "file",
            path,
            attributes={
                "account": random.choice(self.AVAILABLE_ACCOUNTS),
                "external_cell_tag": 13
            },
            tx=tx)
        write_file(path, self.FILE_PAYLOAD, tx=tx)

    def create_table(self, path, tx="0-0-0-0"):
        create(
            "table",
            path,
            attributes={
                "external_cell_tag": 13,
                "optimize_for": "scan",
                "account": random.choice(self.AVAILABLE_ACCOUNTS),
            },
            tx=tx)
        write_table(path, self.TABLE_PAYLOAD, tx=tx)

    def create_document(self, path, tx="0-0-0-0"):
        create(
            "document",
            path,
            attributes={
                "account": random.choice(self.AVAILABLE_ACCOUNTS),
                "value": {"hello": "world", "greetings": "sun"}
            },
            tx=tx)

    def create_unmounted_table(self, path, tx="0-0-0-0"):
        create_dynamic_table(
            path,
            external_cell_tag=13,
            optimize_for="scan",
            account="tmp",  # This is the account with enough quota.
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
        sync_create_cells(1)
        sync_mount_table(path)
        insert_rows(path, self.TABLE_PAYLOAD)
        sync_unmount_table(path)

    def create_frozen_table(self, path, tx="0-0-0-0"):
        create_dynamic_table(
            path,
            external_cell_tag=13,
            optimize_for="scan",
            account="tmp",  # This is the account with enough quota.
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
        sync_create_cells(1)
        sync_mount_table(path)
        insert_rows(path, self.TABLE_PAYLOAD)
        sync_freeze_table(path)

    def create_subtree(self, path, tx="0-0-0-0"):
        # starting_path
        # |-- map_node
        # |   |-- table
        # |   |-- file
        # |   |-- document
        # |   `-- nested_map_node
        # |       `-- other_table
        # `-- top_level_table

        create("map_node", path, force=True, tx=tx)  # Ensure starting_path is created and empty.

        self.create_map_node(f"{path}/map_node", tx=tx)
        self.create_table(f"{path}/map_node/table", tx=tx)
        self.create_file(f"{path}/map_node/file", tx=tx)
        self.create_document(f"{path}/map_node/document", tx=tx)
        self.create_map_node(f"{path}/map_node/nested_map_node", tx=tx)
        self.create_table(f"{path}/map_node/nested_map_node/other_table", tx=tx)
        self.create_table(f"{path}/top_level_table", tx=tx)

    def populate_preservable_attributes_subtree(self, path, tx):
        self._populate_preservable_attributes(path, tx=tx)
        self._populate_preservable_attributes(f"{path}/map_node", tx=tx)
        for node in ls(f"{path}/map_node", tx=tx):
            self._populate_preservable_attributes(f"{path}/map_node/{node}", tx=tx)
        self._populate_preservable_attributes(f"{path}/map_node/nested_map_node/other_table", tx=tx)
        self._populate_preservable_attributes(f"{path}/top_level_table", tx=tx)

    def _iterate_over_subtree_and_validate_attributes(
            self,
            src_path,
            dst_path,
            validation_function,
            tx="0-0-0-0"):
        paths_to_check = [[src_path, dst_path]]

        # This is copy + paste, maybe improve this later? But it requires a lot of python magic.
        while len(paths_to_check) > 0:
            current_src_path, current_dst_path = paths_to_check.pop(0)
            validation_function(current_src_path, current_dst_path, tx=tx)

            # Document supports "ls" command, but we don't actually want to check it's contents with it.
            if get(f"{current_dst_path}/@type", tx=tx) == "document":
                continue

            # Some other nodes don't support "ls" command.
            try:
                for child_node in ls(current_dst_path, tx=tx):
                    next_src_path = f"{current_src_path}/{child_node}"
                    next_dst_path = f"{current_dst_path}/{child_node}"
                    paths_to_check.append([next_src_path, next_dst_path])
            except YtError as err:
                if err.contains_code(yt_error_codes.NoSuchMethod):
                    continue
                else:
                    raise err

    def validate_subtree_attribute_consistency(self, src_path, dst_path, tx="0-0-0-0"):
        self._iterate_over_subtree_and_validate_attributes(
            src_path,
            dst_path,
            self._validate_attribute_consistency_for_node,
            tx=tx)

    def validate_subtree_preservable_attribute_consistency(self, src_path, dst_path, tx):
        self._iterate_over_subtree_and_validate_attributes(
            src_path,
            dst_path,
            self._validate_preservable_attributes,
            tx=tx)

    def validate_copy_base(self, src_path, dst_path, tx="0-0-0-0"):
        # TODO(h0pless): Fix this!
        if self.COMMAND == "copy":
            assert get(src_path, tx=tx) == get(dst_path, tx=tx)

    def validate_map_node_copy(self, path, tx="0-0-0-0"):
        return

    def validate_file_copy(self, path, tx="0-0-0-0"):
        assert read_file(path, tx=tx) == self.FILE_PAYLOAD

    def validate_table_copy(self, path, tx="0-0-0-0"):
        assert read_table(path, tx=tx) == self.TABLE_PAYLOAD

    def populate_preservable_attributes_table(self, path, tx):
        self._populate_preservable_attributes(path, tx=tx)

    def validate_document_copy(self, path, tx="0-0-0-0"):
        assert get(path, tx=tx) == {"hello": "world", "greetings": "sun"}

    def validate_unmounted_table_copy(self, path, tx="0-0-0-0"):
        sync_mount_table(path)
        assert lookup_rows(path, [{"key": 42}]) == self.TABLE_PAYLOAD

    def validate_frozen_table_copy(self, path, tx="0-0-0-0"):
        sync_mount_table(path)
        assert lookup_rows(path, [{"key": 42}]) == self.TABLE_PAYLOAD

    def write_user_attribute(self, path, tx="0-0-0-0"):
        set(f"{path}/@my_personal_attribute", "is_here", tx=tx)

    def _preserve_src_state(self, src_path, tx):
        paths_to_check = [src_path]

        while len(paths_to_check) > 0:
            next_path = paths_to_check.pop(0)
            attributes = get(f"{next_path}/@", tx=tx)
            self.SRC_ATTRIBUTES[next_path] = attributes

            # Document supports "ls" command, but we don't actually want to check it's contents with it.
            if attributes["type"] == "document":
                continue

            # Some other nodes don't support "ls" command.
            try:
                for child_node in ls(next_path, tx=tx):
                    paths_to_check.append(f"{next_path}/{child_node}")
            except YtError as err:
                if err.contains_code(yt_error_codes.NoSuchMethod):
                    continue
                else:
                    raise err

    def execute_command(self, src_path, dst_path, tx="0-0-0-0", **kwargs):
        if self.COMMAND == "move":
            self._preserve_src_state(src_path, tx=tx)
            move(src_path, dst_path, tx=tx, **kwargs)
        else:
            copy(src_path, dst_path, tx=tx, **kwargs)

    @authors("h0pless")
    def test_subtree_size_flag(self):
        set("//sys/@config/cypress_manager/cross_cell_copy_max_subtree_size", 1)
        self.create_subtree("//tmp/dir")

        with raises_yt_error("Subtree is too large for cross-cell copy"):
            copy("//tmp/dir", "//tmp/portal/dir")

    @authors("h0pless")
    @pytest.mark.parametrize("node_type", ["map_node", "file", "table", "document", "unmounted_table", "frozen_table"])
    def test_single_node(self, node_type):
        if node_type == "frozen_table" and self.COMMAND == "move":
            pytest.skip()

        src_path = f"//tmp/{node_type}"
        dst_path = f"//tmp/portal/{node_type}"

        create_node = getattr(self, f"create_{node_type}")
        create_node(src_path)
        self.write_user_attribute(src_path)

        self.execute_command(src_path, dst_path)

        self.validate_copy_base(src_path, dst_path)
        validate_node_copy = getattr(self, f"validate_{node_type}_copy")
        validate_node_copy(dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("use_redundant_flags", [True, False])
    @pytest.mark.parametrize("use_tx", [True, False])
    def test_subtree(self, use_redundant_flags, use_tx):
        create("portal_entrance", "//tmp/other_portal", attributes={"exit_cell_tag": 12})
        src_path = "//tmp/portal/dir"
        dst_path = "//tmp/other_portal/dir"

        tx = "0-0-0-0"
        if use_tx:
            tx = start_transaction()
            self.CONTEXT_DEPENDENT_ATTRIBUTES.append("ref_counter")
            self.CONTEXT_DEPENDENT_ATTRIBUTES.append("update_mode")

        self.create_subtree(src_path, tx=tx)
        self.execute_command(
            src_path,
            dst_path,
            ignore_existing=use_redundant_flags,
            recursive=use_redundant_flags,
            tx=tx)

        self.validate_copy_base(src_path, dst_path, tx=tx)
        self.validate_subtree_attribute_consistency(src_path, dst_path, tx=tx)

        if use_tx:
            if self.COMMAND == "copy":
                assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "update_mode"
                assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "ref_counter"
            else:
                self.CONTEXT_DEPENDENT_ATTRIBUTES.append("versioned_resource_usage")

            commit_transaction(tx)

            self.validate_copy_base(src_path, dst_path)
            self.validate_subtree_attribute_consistency(src_path, dst_path)

            if self.COMMAND != "copy":
                assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "versioned_resource_usage"

        # XXX(babenko): cleanup is weird
        remove("//tmp/other_portal")

    @authors("h0pless")
    @pytest.mark.parametrize("subtree_type", ["subtree", "table"])
    @pytest.mark.parametrize("use_tx", [True, False])
    @pytest.mark.parametrize("should_preserve", [True, False])
    def test_preservable_attributes(self, subtree_type, use_tx, should_preserve):
        path = "//tmp/parent"
        dst_path = "//tmp/portal/parent"

        create_subtree = getattr(self, f"create_{subtree_type}")
        create_subtree(path)

        tx = start_transaction() if use_tx else "0-0-0-0"
        populate_attributes = getattr(self, f"populate_preservable_attributes_{subtree_type}")
        populate_attributes(path, tx=tx)

        create_user("not_john")
        user = "not_john"
        # Preserving is trickier than not preserving.
        if should_preserve:
            # In order to preserve ACL user has to have "administer" permission for the parent node.
            administer_permission = make_ace("allow", user, "administer")
            set("//sys/@config/cypress_manager/portal_synchronization_period", 100)
            set("//tmp/portal&/@acl", [
                administer_permission,
            ])
            wait(lambda: administer_permission in get("//tmp/portal/@acl"))

            # In order to preserve account user has to have "use" permission for the account.
            for account in self.AVAILABLE_ACCOUNTS:
                set(f"//sys/accounts/{account}/@acl/end", make_ace("allow", user, "use"))

        self.execute_command(
            path,
            dst_path,
            tx=tx,
            authenticated_user=user,
            preserve_account=should_preserve,
            preserve_creation_time=should_preserve,
            preserve_modification_time=should_preserve,
            preserve_expiration_time=should_preserve,
            preserve_expiration_timeout=should_preserve,
            preserve_owner=should_preserve,
            preserve_acl=should_preserve)

        self.PRESERVABLE_ATTRIBUTES = {
            "account": should_preserve,
            "creation_time": should_preserve,
            "modification_time": should_preserve,
            "expiration_time": should_preserve,
            "expiration_timeout": should_preserve,
            "owner": should_preserve,
            "acl": should_preserve,
        }
        self.validate_subtree_preservable_attribute_consistency(path, dst_path, tx=tx)

        if use_tx:
            commit_transaction(tx)
            self.validate_subtree_preservable_attribute_consistency(path, dst_path, tx="0-0-0-0")

    # Maybe move set to the test below to save up on time?
    @authors("h0pless")
    def test_opaque_subtree(self):
        src_path = "//tmp/subtree"
        dst_path = "//tmp/portal/subtree"

        self.create_subtree(src_path)
        set(f"{src_path}/map_node/@opaque", True)
        self.execute_command(src_path, dst_path)

        self.validate_copy_base(src_path, dst_path)
        self.validate_subtree_attribute_consistency(src_path, dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("is_redundant", [True, False])
    def test_copy_recursive(self, is_redundant):
        src_path = "//tmp/subtree"
        if is_redundant:
            dst_path = "//tmp/portal/subtree"
        else:
            dst_path = "//tmp/portal/some/arbitrary/long/path/that/has/to/be/created/during/copy/of/the/aforementioned/subtree"

        self.create_subtree(src_path)
        self.execute_command(src_path, dst_path, recursive=True)

        self.validate_copy_base(src_path, dst_path)
        self.validate_subtree_attribute_consistency(src_path, dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("is_redundant", [True, False])
    def test_copy_force(self, is_redundant):
        src_path = "//tmp/subtree"
        dst_path = "//tmp/portal/subtree"

        if not is_redundant:
            self.create_table(dst_path)

        self.create_subtree(src_path)
        self.execute_command(src_path, dst_path, force=True)

        self.validate_copy_base(src_path, dst_path)
        self.validate_subtree_attribute_consistency(src_path, dst_path)

    @authors("h0pless")
    def test_ignore_existing(self):
        src_path = "//tmp/file"
        self.create_file(src_path)

        dst_path = "//tmp/portal/table"
        self.create_table(dst_path)

        if self.COMMAND == "move":
            with raises_yt_error("Node //tmp/portal/table already exists"):
                self.execute_command(src_path, dst_path, ignore_existing=True)
            return
        else:
            self.execute_command(src_path, dst_path, ignore_existing=True)

        # This actually checks that table wasn't overwritten by a copy of the file.
        self.validate_table_copy(dst_path)

    @authors("h0pless")
    def test_lock_existing(self):
        src_path = "//tmp/file"
        self.create_file(src_path)

        dst_path = "//tmp/portal/table"
        self.create_table(dst_path)

        # Lock existing doesn't make sense without a transaction.
        tx = start_transaction()
        if self.COMMAND == "move":
            with raises_yt_error("Node //tmp/portal/table already exists"):
                self.execute_command(src_path, dst_path, ignore_existing=True, lock_existing=True, tx=tx)
            return
        else:
            self.execute_command(src_path, dst_path, ignore_existing=True, lock_existing=True, tx=tx)

        # This actually checks that table wasn't overwritten by a copy of the file.
        self.validate_table_copy(dst_path, tx=tx)
        assert get(f"{dst_path}/@lock_count", tx=tx) == 1

    @authors("h0pless")
    def test_accounting(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/table"
        self.create_table(src_path)
        account = get(f"{src_path}/@account")
        wait(lambda: get(f"//sys/accounts/{account}/@resource_usage/chunk_count") == 1)

        self.execute_command(src_path, dst_path, preserve_account=True)
        assert get(f"//sys/accounts/{account}/@resource_usage/chunk_count") == 1

        create_account("jack")
        set(f"{dst_path}/@account", "jack")
        original_account_expected_usage = 1 if self.COMMAND == "copy" else 0
        wait(
            lambda: get(f"//sys/accounts/{account}/@resource_usage/chunk_count") == original_account_expected_usage and
            get("//sys/accounts/jack/@resource_usage/chunk_count") == 1
        )

        chunk_ids = get("//tmp/portal/table/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        expected_owning_nodes = [dst_path]
        if self.COMMAND == "copy":
            expected_owning_nodes.append(src_path)
        assert_items_equal(get(f"#{chunk_id}/@owning_nodes"), expected_owning_nodes)

        remove(src_path, force=True)
        remove(dst_path)
        wait(lambda: not exists("#" + chunk_id))

    @authors("kvk1920")
    def test_prerequisite_transaction_ids(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/table"
        self.create_table(src_path)

        tx = start_transaction()
        abort_transaction(tx)
        with raises_yt_error("Prerequisite check failed"):
            self.execute_command(src_path, dst_path, prerequisite_transaction_ids=[tx])

        tx = start_transaction()
        self.execute_command(src_path, dst_path, prerequisite_transaction_ids=[tx])
        assert exists(dst_path)
        self.validate_table_copy(dst_path)
        remove(dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("enable_inheritance", [True, False])
    @pytest.mark.parametrize("use_tx", [True, False])
    def test_inheritable_attributes(self, enable_inheritance, use_tx):
        #                                                 portal_exit               attribute = A
        # starting_node         attribute = NONE          `-- starting_node         attribute = NONE
        # |-- map_node          attribute = B                 |-- map_node          attribute = B
        # |   |-- table_keep_b  attribute = B      COPY       |   |-- table_keep_b  attribute = B
        # |   `-- table_c_to_b  attribute = C       ->        |   `-- table_c_to_b  attribute = B
        # |-- table_c_to_a      attribute = C                 |-- table_c_to_a      attribute = A
        # `-- table_none_to_a   attribute = NONE              `-- table_none_to_a   attribute = A

        # This permutation does not test anything meaningful, it's fine to just skip it.
        if use_tx and not enable_inheritance:
            pytest.skip()

        # For the sake of simplicity I used "chunk_merger_mode". Later this test can be expanded.
        attribute_not_found = "Attribute \"chunk_merger_mode\" is not found"
        set("//sys/@config/cypress_manager/enable_inherit_attributes_during_copy", enable_inheritance)

        src_path = "//tmp/starting_node"
        dst_parent = "//tmp/portal"
        dst_path = f"{dst_parent}/starting_node"

        A = "auto"
        B = "deep"
        C = "shallow"

        tx = start_transaction() if use_tx else "0-0-0-0"

        set(f"{dst_parent}/@chunk_merger_mode", A)

        self.create_map_node(src_path)
        with raises_yt_error(attribute_not_found):
            get(f"{src_path}/@chunk_merger_mode")

        self.create_map_node(f"{src_path}/map_node")
        set(f"{src_path}/map_node/@chunk_merger_mode", B, tx=tx)

        self.create_table(f"{src_path}/map_node/table_keep_b", tx=tx)
        assert get(f"{src_path}/map_node/table_keep_b/@chunk_merger_mode", tx=tx) == B

        self.create_table(f"{src_path}/map_node/table_c_to_b", tx=tx)
        set(f"{src_path}/map_node/table_c_to_b/@chunk_merger_mode", C, tx=tx)

        self.create_table(f"{src_path}/table_c_to_a")
        set(f"{src_path}/table_c_to_a/@chunk_merger_mode", C, tx=tx)

        self.create_table(f"{src_path}/table_none_to_a")
        assert get(f"{src_path}/table_none_to_a/@chunk_merger_mode") == "none"

        self.execute_command(src_path, dst_path, tx=tx)

        if self.COMMAND == "copy":
            # Validate source hasn't changed.
            with raises_yt_error(attribute_not_found):
                get(f"{src_path}/@chunk_merger_mode", tx=tx)
            assert get(f"{src_path}/map_node/@chunk_merger_mode", tx=tx) == B
            assert get(f"{src_path}/map_node/table_keep_b/@chunk_merger_mode", tx=tx) == B
            assert get(f"{src_path}/map_node/table_c_to_b/@chunk_merger_mode", tx=tx) == C
            assert get(f"{src_path}/table_c_to_a/@chunk_merger_mode", tx=tx) == C
            assert get(f"{src_path}/table_none_to_a/@chunk_merger_mode", tx=tx) == "none"

        # Validate destination is correct.
        # These should have the same attribute value.
        assert get(f"{dst_parent}/@chunk_merger_mode", tx=tx) == A
        with raises_yt_error(attribute_not_found):
            get(f"{dst_path}/@chunk_merger_mode", tx=tx)
        assert get(f"{dst_path}/map_node/@chunk_merger_mode", tx=tx) == B
        assert get(f"{dst_path}/map_node/table_keep_b/@chunk_merger_mode", tx=tx) == B
        # These might change, depending on config.
        expected_value = B if enable_inheritance else C
        assert get(f"{dst_path}/map_node/table_c_to_b/@chunk_merger_mode", tx=tx) == expected_value
        expected_value = A if enable_inheritance else C
        assert get(f"{dst_path}/table_c_to_a/@chunk_merger_mode", tx=tx) == expected_value
        expected_value = A if enable_inheritance else "none"
        assert get(f"{dst_path}/table_none_to_a/@chunk_merger_mode", tx=tx) == expected_value

    @authors("h0pless")
    def test_many_transactions_in_subtree(self):
        # Transaction hierarchy:
        # topmost_tx
        # |-- grandparent_tx
        # |   `-- parent_tx < -- intentionally unused
        # |       `-- child_tx
        # |           `-- grandchild_tx
        # `-- great_uncle_tx

        topmost_tx = start_transaction()
        grandparent_tx = start_transaction(tx=topmost_tx)
        parent_tx = start_transaction(tx=grandparent_tx)
        child_tx = start_transaction(tx=parent_tx)
        grandchild_tx = start_transaction(tx=child_tx)
        great_uncle_tx = start_transaction(tx=topmost_tx)

        # Tree that should be copied / moved.
        # trunk_map_node
        # |-- grandparent_tx_map_node
        # |   |-- grandparent_tx_table
        # |   `-- child_tx_table
        # |-- other_trunk_map_node
        # |   `-- topmost_tx_table
        # `-- great_uncle_table

        src_path = "//tmp/trunk_map_node"
        dst_path = "//tmp/portal/trunk_map_node"

        create("map_node", src_path)
        create("map_node", f"{src_path}/grandparent_tx_map_node", tx=grandparent_tx)
        create("map_node", f"{src_path}/grandparent_tx_map_node/grandparent_tx_table", tx=grandparent_tx)
        create("map_node", f"{src_path}/grandparent_tx_map_node/child_tx_table", tx=child_tx)
        create("map_node", f"{src_path}/other_trunk_map_node")
        create("map_node", f"{src_path}/other_trunk_map_node/topmost_tx_table", tx=topmost_tx)
        create("map_node", f"{src_path}/great_uncle_table", tx=great_uncle_tx)

        if self.COMMAND != "copy":
            abort_transaction(great_uncle_tx)  # This leads to a lock conflict, which is reasonable.

        self.execute_command(src_path, dst_path, tx=grandchild_tx)

        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("ref_counter")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("update_mode")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_count")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_mode")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("resource_usage")

        self.validate_copy_base(src_path, dst_path, tx=grandchild_tx)
        self.validate_subtree_attribute_consistency(src_path, dst_path, tx=grandchild_tx)

        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "resource_usage"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "lock_mode"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "lock_count"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "update_mode"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "ref_counter"

    # Maybe these two tests are redundant, considering the preservable attributes test.
    @authors("shakurov")
    def test_expiration_time(self):
        expiration_time = str(get_current_time() + timedelta(seconds=3))
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/table"
        self.create_table(src_path)
        set(f"{src_path}/@expiration_time", expiration_time)
        self.execute_command(src_path, dst_path, preserve_expiration_time=True)
        wait(
            lambda: not exists(dst_path),
            sleep_backoff=0.5,
            timeout=5)

    @authors("shakurov")
    def test_expiration_timeout(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/table"
        self.create_table(src_path)
        set(f"{src_path}/@expiration_timeout", 3000)
        self.execute_command(src_path, dst_path, preserve_expiration_timeout=True)

        wait(
            lambda: not exists(dst_path, suppress_expiration_timeout_renewal=True),
            sleep_backoff=0.5,
            timeout=5)

    @authors("babenko")
    def test_removed_account(self):
        src_path = "//tmp/file"
        dst_parent_path = "//tmp/portal"
        dst_path = f"{dst_parent_path}/file"

        self.create_file(src_path)
        account = get(f"{src_path}/@account")

        remove(f"//sys/accounts/{account}")
        wait(lambda: get(f"//sys/accounts/{account}/@life_stage") == "removal_started")

        with raises_yt_error("cannot be used since it is in \"removal_pre_committed\" life stage"):
            self.execute_command(src_path, dst_path, preserve_account=True)

        self.execute_command(src_path, dst_path)
        assert get(f"{dst_parent_path}/@account") == get(f"{dst_path}/@account")

        remove(src_path, force=True)
        wait(lambda: not exists(f"//sys/accounts/{account}"))

    # IMPROPER USES
    @authors("h0pless")
    def test_force_not_set(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/existing_node"

        tx = start_transaction()

        self.create_table(src_path)
        self.create_map_node(dst_path, tx=tx)

        # Conflict because of locks.
        with raises_yt_error("Cannot take lock for child \"existing_node\" of node //tmp/portal since this child is locked by concurrent transaction"):
            self.execute_command(src_path, dst_path)

        # Force was not used.
        with raises_yt_error(f"Node {dst_path} already exists"):
            self.execute_command(src_path, dst_path, tx=tx)

        commit_transaction(tx)
        # Force was not used.
        with raises_yt_error(f"Node {dst_path} already exists"):
            self.execute_command(src_path, dst_path)

    @authors("h0pless")
    def test_recursive_not_set(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/listen/its/hard/to/come/up/with/funny/paths/every/time/table"

        self.create_table(src_path)

        with raises_yt_error("Node //tmp/portal has no child with key \"listen\""):
            self.execute_command(src_path, dst_path)

    @authors("h0pless")
    def test_non_external_table(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/table"

        tabl_id = create("table", src_path, attributes={"external_cell_tag": 11})

        with raises_yt_error(f"Cannot copy node {tabl_id} to cell 11 since the latter is its external cell"):
            self.execute_command(src_path, dst_path)

    @authors("h0pless")
    def test_cant_copy_to_root(self):
        src_path = "//tmp/portal/table"
        self.create_table(src_path)
        with raises_yt_error("Node / cannot be replaced"):
            self.execute_command(src_path, "/", force=True)

    @authors("h0pless")
    def test_cant_copy_subtree_with_portal(self):
        with raises_yt_error("Cannot clone a portal"):
            self.execute_command("//tmp", "//home/other")

    @authors("h0pless")
    def test_ignore_existing_error(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/table"

        self.create_table(src_path)

        if self.COMMAND == "copy":
            with raises_yt_error("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously"):
                self.execute_command(src_path, dst_path, ignore_existing=True, force=True)
        else:
            self.execute_command(src_path, dst_path, lock_existing=True)

    @authors("h0pless")
    def test_lock_existing_error(self):
        src_path = "//tmp/table"
        dst_path = "//tmp/portal/table"

        self.create_table(src_path)

        if self.COMMAND == "copy":
            with raises_yt_error("Cannot specify \"lock_existing\" without \"ignore_existing\""):
                self.execute_command(src_path, dst_path, lock_existing=True)
        else:
            self.execute_command(src_path, dst_path, lock_existing=True)


################################################################################


class TestCrossCellMove(TestCrossCellCopy):
    COMMAND = "move"

    # These attributes can change or be the same depending on the context.
    CONTEXT_DEPENDENT_ATTRIBUTES = [
        "tablet_state",
        "actual_tablet_state",
        "expected_tablet_state",
        "unflushed_timestamp",

        "access_counter",
        "lock_count",
        "lock_mode",

        "schema_duplicate_count",  # Figure out if this is ok.
    ]

    # These attributes have to change when cross-cell copying.
    EXPECTED_ATTRIBUTE_CHANGES = [
        "access_time",

        # Changed due to cell change:
        "id",
        "native_cell_tag",
        "parent_id",
        "shard_id",
        "schema_id",
        "cow_cookie",

        # Changed due to test design:
        "revision",
        "attribute_revision",
        "native_content_revision",
        "content_revision",
    ]


################################################################################


# NB: CheckInvariants() complexity is at least O(|Cypress nodes|) which is too
# slow in this case.
class TestPortalsWithoutInvariantChecking(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0
    NUM_SECONDARY_MASTER_CELLS = 3

    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "invariants_check_probability": None,
        }
    }

    @authors("h0pless")
    @pytest.mark.timeout(150)
    def test_copy_large_subtree(self):
        set("//sys/accounts/tmp/@resource_limits/node_count", 10000000)
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 11})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 12})

        for node_count in [1000, 5000, 10000]:
            subtree = {str(i): {} for i in range(node_count)}
            set(f"//tmp/p1/large_subtree_{node_count}", subtree, force=True)

            copy(f"//tmp/p1/large_subtree_{node_count}", f"//tmp/p2/large_subtree_{node_count}")

            time.sleep(5)  # Just don't crash...

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")
        remove("//tmp/p2")
