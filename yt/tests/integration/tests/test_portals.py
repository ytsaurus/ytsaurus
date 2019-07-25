import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt.common import YtError

from dateutil.tz import tzlocal

##################################################################

class TestPortals(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    def test_need_exit_cell_tag_on_create(self):
        with pytest.raises(YtError):
            create("portal_entrance", "//tmp/p")

    def test_create_portal(self):
        entrance_id = create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        assert get("//tmp/p&/@type") == "portal_entrance"
        assert not get("//tmp/p&/@inherit_acl")
        acl = get("//tmp/@effective_acl")
        assert get("//tmp/p&/@acl") == acl
        assert get("//tmp/p&/@path") == "//tmp/p"

        assert exists("//sys/portal_entrances/{}".format(entrance_id))

        exit_id = get("//tmp/p&/@exit_node_id")
        assert get("#{}/@type".format(exit_id), driver=get_driver(1)) == "portal_exit"
        assert get("#{}/@entrance_node_id".format(exit_id), driver=get_driver(1)) == entrance_id
        assert not get("#{}/@inherit_acl".format(exit_id), driver=get_driver(1))
        assert get("#{}/@acl".format(exit_id), driver=get_driver(1)) == acl
        assert get("#{}/@path".format(exit_id), driver=get_driver(1)) == "//tmp/p"

        assert exists("//sys/portal_exits/{}".format(exit_id), driver=get_driver(1))

    def test_cannot_enable_acl_inheritance(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        exit_id = get("//tmp/p&/@exit_node_id")
        with pytest.raises(YtError):
            set("//tmp/p&/@inherit_acl", True)
        with pytest.raises(YtError):
            set("#{}/@inherit_acl".format(exit_id), True, driver=get_driver(1))

    def test_portal_reads(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        exit_id = get("//tmp/p&/@exit_node_id")

        assert get("//tmp/p") == {}
        assert get("//tmp/p/@type") == "portal_exit"
        assert get("//tmp/p/@id") == exit_id

        create("table", "#{}/t".format(exit_id), driver=get_driver(1))
        assert get("//tmp/p") == {"t": yson.YsonEntity()}

    def test_portal_writes(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("table", "//tmp/p/t")

        assert get("//tmp/p") == {"t": yson.YsonEntity()}

    def test_remove_portal(self):
        entrance_id = create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        exit_id = get("//tmp/p&/@exit_node_id")
        table_id = create("table", "//tmp/p/t")

        remove("//tmp/p")
        wait(lambda: not exists("#{}".format(exit_id)) and \
                     not exists("#{}".format(entrance_id), driver=get_driver(1)) and \
                     not exists("#{}".format(table_id), driver=get_driver(1)))

    def test_remove_all_portal_children(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        remove("//tmp/p/*")
al
    def test_portal_set(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        set("//tmp/p/key", "value", force=True)
        assert get("//tmp/p/key") == "value"
        set("//tmp/p/map/key", "value", force=True, recursive=True)
        assert get("//tmp/p/map/key") == "value"

    @pytest.mark.parametrize("external_cell_tag", [1, 2])
    def test_table_in_portal(self, external_cell_tag):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("table", "//tmp/p/t", attributes={"external": True, "external_cell_tag": external_cell_tag})
        PAYLOAD = [{"key": "value"}]
        write_table("//tmp/p/t", PAYLOAD)
        assert get("//tmp/p/t/@row_count") == len(PAYLOAD)
        assert get("//tmp/p/t/@chunk_count") == 1
        chunk_ids = get("//tmp/p/t/@chunk_ids")
        assert len(chunk_ids) == 1
        assert get("#{}/@owning_nodes".format(chunk_ids[0])) == ["//tmp/p/t"]
        assert read_table("//tmp/p/t") == PAYLOAD

    @pytest.mark.parametrize("external_cell_tag", [1, 2])
    def test_file_in_portal(self, external_cell_tag):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("file", "//tmp/p/f", attributes={"external": True, "external_cell_tag": external_cell_tag})
        PAYLOAD = "a" *  100
        write_file("//tmp/p/f", PAYLOAD)
        assert get("//tmp/p/f/@uncompressed_data_size") == len(PAYLOAD)
        assert get("//tmp/p/f/@chunk_count") == 1
        chunk_ids = get("//tmp/p/f/@chunk_ids")
        assert len(chunk_ids) == 1
        assert get("#{}/@owning_nodes".format(chunk_ids[0])) == ["//tmp/p/f"]
        assert read_file("//tmp/p/f") == PAYLOAD

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
    
    def test_expiration_time(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        create("table", "//tmp/p/t", attributes={"expiration_time": str(self._now())})
        wait(lambda: not exists("//tmp/p/t"))

    def test_remove_table_in_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 1})
        table_id = create("table", "//tmp/p/t", attributes={"external": True, "external_cell_tag": 2})
        wait(lambda: exists("#{}".format(table_id), driver=get_driver(2)))
        remove("//tmp/p/t")
        wait(lambda: not exists("#{}".format(table_id), driver=get_driver(2)))

    def test_root_shard(self):
        shard_id = get("//@shard_id")
        assert exists("//sys/cypress_shards/{}".format(shard_id))
        assert get("//@id") == get("#{}/@root_node_id".format(shard_id))
        assert get("#{}/@account_statistics/sys/node_count".format(shard_id)) > 0
 
    def test_shard_statistics(self):
        shard_id = get("//@shard_id")
        create_account("a")
        create_account("b")
        assert not exists("#{}/@account_statistics/a".format(shard_id))
        create("table", "//tmp/t1", attributes={"account": "a"})
        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 1
        create("table", "//tmp/t2", attributes={"account": "a"})
        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 2
        set("//tmp/t2/@account", "b")
        assert get("#{}/@account_statistics/a/node_count".format(shard_id)) == 1
        assert get("#{}/@account_statistics/b/node_count".format(shard_id)) == 1
        remove("//tmp/*")
        wait(lambda: not exists("#{}/@account_statistics/a".format(shard_id)))

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

