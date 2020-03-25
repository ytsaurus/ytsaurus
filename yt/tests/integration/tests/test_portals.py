import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt.common import YtError

from yt.test_helpers import assert_items_equal, are_almost_equal

from dateutil.tz import tzlocal

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
            create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 0})

    @authors("babenko")
    def test_cannot_create_portal_to_primary2(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p/q", attributes={"exit_cell_tag": 0})

    @authors("babenko")
    def test_validate_cypress_node_host_cell_role(self):
        set("//sys/@config/multicell_manager/cell_roles", {"1": ["chunk_host"]})
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})

    @authors("babenko")
    def test_need_exit_cell_tag_on_create(self):
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p")

    @authors("babenko")
    def test_create_portal(self):
        entrance_id = create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        assert get("//tmp/p&/@type") == "portal_entrance"
        acl = get("//tmp/@effective_acl")
        assert get("//tmp/p&/@path") == "//tmp/p"

        assert exists("//sys/portal_entrances/{}".format(entrance_id))

        exit_id = get("//tmp/p&/@exit_node_id")
        assert get("#{}/@type".format(exit_id), driver=get_driver(1)) == "portal_exit"
        assert get("#{}/@entrance_node_id".format(exit_id), driver=get_driver(1)) == entrance_id
        assert not get("#{}/@inherit_acl".format(exit_id), driver=get_driver(1))
        assert get("#{}/@acl".format(exit_id), driver=get_driver(1)) == acl
        assert get("#{}/@path".format(exit_id), driver=get_driver(1)) == "//tmp/p"

        assert exists("//sys/portal_exits/{}".format(exit_id), driver=get_driver(1))

    @authors("babenko")
    def test_cannot_enable_acl_inheritance(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        exit_id = get("//tmp/p&/@exit_node_id")
        with pytest.raises(YtError):
            set("//tmp/p/@inherit_acl", True, driver=get_driver(1))

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_portal_reads(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("table", "//tmp/p/t")

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert get("//tmp/p") == {"t": yson.YsonEntity()}

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_remove_portal(self, purge_resolve_cache):
        entrance_id = create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        exit_id = get("//tmp/p&/@exit_node_id")
        table_id = create("table", "//tmp/p/t")
        shard_id = get("//tmp/p/t/@shard_id")

        for i in xrange(10):
            create("map_node", "//tmp/p/m" + str(i))

        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        remove("//tmp/p")

        wait(lambda: not exists("#{}".format(exit_id)) and \
                     not exists("#{}".format(entrance_id), driver=get_driver(1)) and \
                     not exists("#{}".format(table_id), driver=get_driver(1)) and \
                     not exists("#{}".format(shard_id)))

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_remove_all_portal_children(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        remove("//tmp/p/*")

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_portal_set(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        set("//tmp/p/key", "value", force=True)
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert get("//tmp/p/key") == "value"
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        set("//tmp/p/map/key", "value", force=True, recursive=True)
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        assert get("//tmp/p/map/key") == "value"

    @pytest.mark.parametrize("with_outer_tx,external_cell_tag,purge_resolve_cache",
                             [(with_outer_tx, external_cell_tag, purge_resolve_cache)
                             for with_outer_tx in [False, True]
                             for external_cell_tag in [1, 2]
                             for purge_resolve_cache in [False, True]])
    @authors("babenko")
    def test_read_write_table_in_portal(self, with_outer_tx, external_cell_tag, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("map_node", "//tmp/p/m")

        PAYLOAD = [{"key": "value"}]

        if with_outer_tx:
            tx = start_transaction()
        else:
            tx = "0-0-0-0"

        create("table", "//tmp/p/m/t", attributes={"external": True, "external_cell_tag": external_cell_tag}, tx=tx)
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

    @pytest.mark.parametrize("with_outer_tx,external_cell_tag,purge_resolve_cache",
                             [(with_outer_tx, external_cell_tag, purge_resolve_cache)
                             for with_outer_tx in [False, True]
                             for external_cell_tag in [1, 2]
                             for purge_resolve_cache in [False, True]])
    @authors("babenko")
    def test_read_write_file_in_portal(self, with_outer_tx, external_cell_tag, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("map_node", "//tmp/p/m")

        PAYLOAD = "a" *  100

        if with_outer_tx:
            tx = start_transaction()
        else:
            tx = "0-0-0-0"

        create("file", "//tmp/p/m/f", attributes={"external": True, "external_cell_tag": external_cell_tag}, tx=tx)
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
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 2})
        create("table", "//tmp/p1/t", attributes={"account": "a", "external": True, "external_cell_tag": 1})
        create("table", "//tmp/p2/t", attributes={"account": "a", "external": True, "external_cell_tag": 2})
        remove("//sys/accounts/a")
        assert get("//sys/accounts/a/@life_stage") == "removal_pre_committed"
        wait(lambda: get("//sys/accounts/a/@life_stage", driver=get_driver(1)) == "removal_started")
        wait(lambda: get("//sys/accounts/a/@life_stage", driver=get_driver(2)) == "removal_started")
        remove("//tmp/p1/t")
        wait(lambda: get("//sys/accounts/a/@life_stage", driver=get_driver(1)) == "removal_pre_committed")
        assert get("//sys/accounts/a/@life_stage", driver=get_driver(2)) == "removal_started"
        remove("//tmp/p2/t")
        wait(lambda: not exists("//sys/accounts/a"))

    def _now(self):
        return datetime.now(tzlocal())

    @authors("babenko")
    def test_expiration_time(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("table", "//tmp/p/t", attributes={"expiration_time": str(self._now())})
        wait(lambda: not exists("//tmp/p/t"))

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_remove_table_in_portal(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        table_id = create("table", "//tmp/p/t", attributes={"external": True, "external_cell_tag": 2})
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
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        assert get("//tmp/p/@key") == "p"
        assert get("//tmp/p/@parent_id") == get("//tmp/@id")

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    @pytest.mark.skipif("True", reason="YT-11197")
    def test_cross_shard_links_forbidden(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        _maybe_purge_resolve_cache(purge_resolve_cache, "//tmp/p")
        with pytest.raises(YtError):
            link("//tmp", "//tmp/p/l")

    @authors("babenko")
    @pytest.mark.parametrize("purge_resolve_cache", [False, True])
    def test_intra_shard_links(self, purge_resolve_cache):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 2})

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

        create("file", "//tmp/p1/m/f", attributes={"external_cell_tag": 3})
        assert get("//tmp/p1/m/f/@account") == "a"
        FILE_PAYLOAD = "PAYLOAD"
        write_file("//tmp/p1/m/f", FILE_PAYLOAD)

        wait(lambda: get("//sys/accounts/a/@resource_usage/chunk_count") == 1)

        create("table", "//tmp/p1/m/t", attributes={"external_cell_tag": 3, "optimize_for": "scan", "account": "tmp"})
        assert get("//tmp/p1/m/t/@account") == "tmp"
        TABLE_PAYLOAD = [{"key": "value"}]
        write_table("//tmp/p1/m/t", TABLE_PAYLOAD)

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
        assert read_table("//tmp/p2/m/t", tx=tx) == TABLE_PAYLOAD

        assert get("//sys/accounts/a/@resource_usage/chunk_count") == 1

        if in_tx:
            commit_transaction(tx)

        create_account("b")
        set("//tmp/p2/m/f/@account", "b")
        wait(lambda: get("//sys/accounts/a/@resource_usage/chunk_count") == 1 and \
                     get("//sys/accounts/b/@resource_usage/chunk_count") == 1)

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
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 2})

        create("file", "//tmp/p1/f", attributes={"external_cell_tag": 3, "account": "a"})
        FILE_PAYLOAD = "PAYLOAD"
        write_file("//tmp/p1/f", FILE_PAYLOAD)

        if in_tx:
            tx = start_transaction()
        else:
            tx = "0-0-0-0"

        move("//tmp/p1/f", "//tmp/p2/f", tx=tx, preserve_account=True)

        assert not exists("//tmp/p1/f", tx=tx)
        assert exists("//tmp/p2/f", tx=tx)

        if in_tx:
            assert exists("//tmp/p1/f")
            assert not exists("//tmp/p2/f")

        assert read_file("//tmp/p2/f", tx=tx) == FILE_PAYLOAD

        if in_tx:
            commit_transaction(tx)

        assert get("//tmp/p2/f/@account") == "a"

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")
        remove("//tmp/p2")

    @authors("shakurov")
    def test_cross_cell_move_opaque_with_user_attribute(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1})

        create("table", "//tmp/d1/d2/d3/t", recursive=True, attributes={"external_cell_tag": 3})
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
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 2})

        create("file", "//tmp/p1/f", attributes={"external_cell_tag": 3, "account": "a"})

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
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 2})

        create("table", "//tmp/p1/t", attributes={"external_cell_tag": 3, "tablet_cell_bundle": "b"})

        remove("//sys/tablet_cell_bundles/b")
        wait(lambda: get("//sys/tablet_cell_bundles/b/@life_stage") == "removal_pre_committed")

        with pytest.raises(YtError):
            copy("//tmp/p1/t", "//tmp/p2/t")

        remove("//tmp/p1/t")
        wait(lambda: not exists("//sys/tablet_cell_bundles/b"))

        # XXX(babenko): cleanup is weird
        remove("//tmp/p1")
        remove("//tmp/p2")

    @authors("babenko")
    def test_portal_inside_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//tmp/p/q", attributes={"exit_cell_tag": 2})

        TABLE_PAYLOAD = [{"key": "value"}]
        create("table", "//tmp/t", attributes={"external_cell_tag": 3})
        write_table("//tmp/t", TABLE_PAYLOAD)

        move("//tmp/t", "//tmp/p/q/t")
        assert read_table("//tmp/p/q/t") == TABLE_PAYLOAD

    @authors("babenko")
    def test_create_portal_in_tx_commit(self):
        tx = start_transaction()

        entrance_id = create("portal_entrance", "//tmp/p", tx=tx, attributes={"exit_cell_tag": 1})
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

        entrance_id = create("portal_entrance", "//tmp/p", tx=tx, attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})

        mutation_id = generate_uuid()

        create("table", "//tmp/p/t", mutation_id=mutation_id)
        remove("//tmp/p/t", mutation_id=mutation_id, retry=True)
        assert exists("//tmp/p/t")

    @authors("babenko")
    def test_mutation_id2(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})

        mutation_id = generate_uuid()

        create("table", "//tmp/p/t", mutation_id=mutation_id)
        with pytest.raises(YtError):
            remove("//tmp/p/t", mutation_id=mutation_id)

    @authors("babenko")
    def test_externalize_node(self):
        create_account("a")
        create_account("b")
        create_user("u")

        create("map_node", "//tmp/m", attributes={"attr": "value", "acl": [make_ace("allow", "u", "write")]})

        TABLE_PAYLOAD = [{"key": "value"}]
        create("table", "//tmp/m/t", attributes={"external": True, "external_cell_tag": 3, "account": "a", "attr": "t"})
        write_table("//tmp/m/t", TABLE_PAYLOAD)

        FILE_PAYLOAD = "PAYLOAD"
        create("file", "//tmp/m/f", attributes={"external": True, "external_cell_tag": 3, "account": "b", "attr": "f"})
        write_file("//tmp/m/f", FILE_PAYLOAD)

        create("document", "//tmp/m/d", attributes={"value": {"hello": "world"}})
        ct = get("//tmp/m/d/@creation_time")
        mt = get("//tmp/m/d/@modification_time")

        create("map_node", "//tmp/m/m", attributes={"account": "a", "compression_codec": "brotli_8"})

        create("table", "//tmp/m/et", attributes={"external_cell_tag": 3, "expiration_time": "2100-01-01T00:00:00.000000Z"})

        create("map_node", "//tmp/m/acl1", attributes={"inherit_acl": True,  "acl": [make_ace("deny", "u", "read")]})
        create("map_node", "//tmp/m/acl2", attributes={"inherit_acl": False, "acl": [make_ace("deny", "u", "read")]})

        root_acl = get("//tmp/m/@effective_acl")
        acl1 = get("//tmp/m/acl1/@acl")
        acl2 = get("//tmp/m/acl2/@acl")

        ORCHID_MANIFEST = {"address": "someaddress"}
        create("orchid", "//tmp/m/orchid", attributes={"manifest": ORCHID_MANIFEST})

        externalize("//tmp/m", 1)

        assert not get("//tmp/m/@inherit_acl")
        assert get("//tmp/m/@acl") == root_acl

        assert get("//tmp/m/acl1/@inherit_acl")
        assert get("//tmp/m/acl1/@acl") == acl1

        assert not get("//tmp/m/acl2/@inherit_acl")
        assert get("//tmp/m/acl2/@acl") == acl2

        assert get("//tmp/m/@type") == "portal_exit"
        assert get("//tmp/m/@attr") == "value"

        assert read_table("//tmp/m/t") == TABLE_PAYLOAD
        assert get("//tmp/m/t/@account") == "a"
        assert get("//tmp/m/t/@attr") == "t"

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
        assert get("//tmp/m/t/@shard_id") == shard_id
        assert get("//sys/cypress_shards/{}/@account_statistics/tmp/node_count".format(shard_id)) == 6

        remove("//tmp/m")
        wait(lambda: "m" not in ls("//tmp"))
        assert not exists("//tmp/m")

    @authors("babenko")
    def test_externalize_copy_externalize(self):
        create("map_node", "//tmp/m1")

        TABLE_PAYLOAD = [{"key": "value"}]
        create("table", "//tmp/m1/t", attributes={"external": True, "external_cell_tag": 3})
        write_table("//tmp/m1/t", TABLE_PAYLOAD)

        FILE_PAYLOAD = "PAYLOAD"
        create("file", "//tmp/m1/f", attributes={"external": True, "external_cell_tag": 3})
        write_file("//tmp/m1/f", FILE_PAYLOAD)

        externalize("//tmp/m1", 1)

        shard_id1 = get("//tmp/m1/@shard_id")
        assert get("//tmp/m1/t/@shard_id") == shard_id1
        assert get("//tmp/m1/f/@shard_id") == shard_id1

        assert read_table("//tmp/m1/t") == TABLE_PAYLOAD
        assert read_file("//tmp/m1/f") == FILE_PAYLOAD

        copy("//tmp/m1", "//tmp/m2")
        assert get("//tmp/m2/@type") == "map_node"
        assert_items_equal(ls("//tmp"), ["m1", "m2"]) 
        assert get("//tmp/m1/@key") == "m1"
        assert get("//tmp/m2/@key") == "m2"

        externalize("//tmp/m2", 2)

        shard_id2 = get("//tmp/m2/@shard_id")
        assert shard_id1 != shard_id2
        assert get("//tmp/m2/t/@shard_id") == shard_id2
        assert get("//tmp/m2/f/@shard_id") == shard_id2

        assert read_table("//tmp/m2/t") == TABLE_PAYLOAD
        assert read_file("//tmp/m2/f") == FILE_PAYLOAD

    @authors("babenko")
    def test_internalize_node(self):
        create_account("a")
        create_account("b")
        create_user("u")

        create("portal_entrance", "//tmp/m", attributes={"exit_cell_tag": 1})
        set("//tmp/m/@attr", "value")
        set("//tmp/m/@acl", [make_ace("allow", "u", "write")])
        shard_id = get("//tmp/m/@shard_id")

        TABLE_PAYLOAD = [{"key": "value"}]
        create("table", "//tmp/m/t", attributes={"external": True, "external_cell_tag": 3, "account": "a", "attr": "t"})
        write_table("//tmp/m/t", TABLE_PAYLOAD)

        FILE_PAYLOAD = "PAYLOAD"
        create("file", "//tmp/m/f", attributes={"external": True, "external_cell_tag": 3, "account": "b", "attr": "f"})
        write_file("//tmp/m/f", FILE_PAYLOAD)

        create("document", "//tmp/m/d", attributes={"value": {"hello": "world"}})
        ct = get("//tmp/m/d/@creation_time")
        mt = get("//tmp/m/d/@modification_time")

        create("map_node", "//tmp/m/m", attributes={"account": "a", "compression_codec": "brotli_8"})

        create("table", "//tmp/m/et", attributes={"external_cell_tag": 3, "expiration_time": "2100-01-01T00:00:00.000000Z"})

        create("map_node", "//tmp/m/acl1", attributes={"inherit_acl": True,  "acl": [make_ace("deny", "u", "read")]})
        create("map_node", "//tmp/m/acl2", attributes={"inherit_acl": False, "acl": [make_ace("deny", "u", "read")]})

        root_acl = get("//tmp/m/@effective_acl")
        acl1 = get("//tmp/m/acl1/@acl")
        acl2 = get("//tmp/m/acl2/@acl")

        ORCHID_MANIFEST = {"address": "someaddress"}
        create("orchid", "//tmp/m/orchid", attributes={"manifest": ORCHID_MANIFEST})

        internalize("//tmp/m")

        wait(lambda: not exists("#" + shard_id))

        assert not get("//tmp/m/@inherit_acl")
        assert get("//tmp/m/@acl") == root_acl

        assert get("//tmp/m/acl1/@inherit_acl")
        assert get("//tmp/m/acl1/@acl") == acl1

        assert not get("//tmp/m/acl2/@inherit_acl")
        assert get("//tmp/m/acl2/@acl") == acl2

        assert get("//tmp/m/@type") == "map_node"
        assert get("//tmp/m/@attr") == "value"

        assert read_table("//tmp/m/t") == TABLE_PAYLOAD
        assert get("//tmp/m/t/@account") == "a"
        assert get("//tmp/m/t/@attr") == "t"

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

        assert get("//tmp/m/t/@shard_id") == get("//tmp/@shard_id")

    @authors("babenko")
    def test_bulk_insert_yt_11194(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})

        sync_create_cells(1)
        create("table", "//tmp/p/target", attributes={
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ],
            "external": False
        })
        sync_mount_table("//tmp/p/target")

        create("table", "//tmp/p/source", attributes={
            "external": True,
            "external_cell_tag": 2
        })

        PAYLOAD = [{"key": 1, "value": "blablabla"}]
        write_table("//tmp/p/source", PAYLOAD)

        map(
            in_="//tmp/p/source",
            out="<append=%true>//tmp/p/target",
            command="cat")

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
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        shard_id = get("//tmp/p/@shard_id")
        assert get("#{}/@name".format(shard_id)) == "portal://tmp/p"

    @authors("babenko")
    def test_externalize_shard_name(self):
        create("map_node", "//tmp/m")
        externalize("//tmp/m", 1)
        shard_id = get("//tmp/m/@shard_id")
        assert get("#{}/@name".format(shard_id)) == "portal://tmp/m"

    @authors("babenko")
    def test_portal_get_set_shard_name(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        shard_id = get("//tmp/p/@shard_id")
        set("#{}/@name".format(shard_id), "shard_name")
        assert get("#{}/@name".format(shard_id)) == "shard_name"

    @authors("babenko")
    def test_concat_in_tx(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
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
            create("table", table_path, attributes={"external": True, "external_cell_tag": 1})
            write_table(table_path, [{"key": table_directory}])

        externalize("//tmp/m1", 2)

        for table_directory in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            table_path = "//tmp/m1" + table_directory + "/table"
            assert read_table(table_path) == [{"key": table_directory}]

    @authors("gritukan")
    def test_granular_internalize(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1, "opaque": True})
        create("map_node", "//tmp/p1/m21", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m22", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m21/m31", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m21/m32", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m22/m33", attributes={"opaque": True})
        create("map_node", "//tmp/p1/m22/m34", attributes={"opaque": True})

        for table_directory in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            table_path = "//tmp/p1" + table_directory + "/table"
            create("table", table_path, attributes={"external": True, "external_cell_tag": 2})
            write_table(table_path, [{"key": table_directory}])

        internalize("//tmp/p1")

        for table_directory in ["/m21/m31", "/m21/m32", "/m22/m33", "/m22/m34"]:
            table_path = "//tmp/p1" + table_directory + "/table"
            assert read_table(table_path) == [{"key": table_directory}]

    @authors("gritukan")
    def test_granular_cross_cell_copy(self):
        create("portal_entrance", "//tmp/p1", attributes={"exit_cell_tag": 1, "opaque": True})
        create("portal_entrance", "//tmp/p2", attributes={"exit_cell_tag": 2, "opaque": True})

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
        create("link", "//tmp/l", attributes={"target_path": "//tmp/m"})
        with pytest.raises(YtError):
            externalize("//tmp/l", 1)

    @authors("shakurov")
    def test_link_to_portal_not_broken(self):
        create("map_node", "//tmp/m")
        create("link", "//tmp/l", attributes={"target_path": "//tmp/m"})

        externalize("//tmp/m", 1)

        assert not get("//tmp/l&/@broken")

    @authors("shakurov")
    def test_remove_portal_not_permitted_by_acl(self):
        create_user("u")
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})

        assert not get("//tmp/p/@inherit_acl")
        # NB: denying removal for portal exit only!
        set("//tmp/p/@acl", [make_ace("deny", "u", "remove", "object_and_descendants")])

        assert check_permission("u", "remove", "//tmp/p")["action"] == "deny"

        with pytest.raises(YtError):
            remove("//tmp/p", authenticated_user="u")
##################################################################

class TestResolveCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("babenko")
    def test_cache_populated_on_resolve(self):
        create("map_node", "//tmp/dir1/dir2", recursive=True)
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 1})
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
        create("portal_entrance", "//tmp/dir1/dir2/p", attributes={"exit_cell_tag": 1})
        create("table", "//tmp/dir1/dir2/p/t")
        assert get("//tmp/dir1/dir2/p&/@resolve_cached")
        remove("//tmp/dir1/dir2/p")
        wait(lambda: not get("//tmp/dir1/@resolve_cached"))

    @authors("babenko")
    def test_cache_trimmed(self):
        create("map_node", "//tmp/dir1/dir2a", recursive=True)
        create("map_node", "//tmp/dir1/dir2b", recursive=True)
        create("portal_entrance", "//tmp/dir1/dir2a/p", attributes={"exit_cell_tag": 1})
        create("portal_entrance", "//tmp/dir1/dir2b/p", attributes={"exit_cell_tag": 1})
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


