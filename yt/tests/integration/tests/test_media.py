import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import copy
import __builtin__

from time import sleep

from yt.environment.helpers import assert_items_equal

################################################################################

class TestMedia(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 10

    REPLICATOR_REACTION_TIME = 3.5
    NON_DEFAULT_MEDIUM = "hdd2"

    @classmethod
    def setup_class(cls, delayed_secondary_cells_start=False):
        super(TestMedia, cls).setup_class()
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, TestMedia.NON_DEFAULT_MEDIUM)

    @classmethod
    def modify_node_config(cls, config):
        # Add second location with non-default medium to each node.
        assert len(config["data_node"]["store_locations"]) == 1
        location_prototype = config["data_node"]["store_locations"][0]

        default_location = copy.deepcopy(location_prototype)
        default_location["path"] += "_0";
        default_location["medium_name"] = "default"

        non_default_location = copy.deepcopy(location_prototype)
        non_default_location["path"] += "_1";
        non_default_location["medium_name"] = cls.NON_DEFAULT_MEDIUM

        config["data_node"]["store_locations"] = []
        config["data_node"]["store_locations"].append(default_location)
        config["data_node"]["store_locations"].append(non_default_location)

    @classmethod
    def on_masters_started(cls):
        create_medium(cls.NON_DEFAULT_MEDIUM)
        medium_count = len(get_media())
        while (medium_count < 7):
            create_medium("hdd" + str(medium_count))
            medium_count += 1

    def _assert_all_chunks_on_medium(self, tbl, medium):
        chunk_ids = get("//tmp/{0}/@chunk_ids".format(tbl))
        for chunk_id in chunk_ids:
            replicas = get("#" + chunk_id + "/@stored_replicas")
            for replica in replicas:
                assert replica.attributes["medium"] == medium

    def _count_chunks_on_medium(self, tbl, medium):
        chunk_count = 0
        chunk_ids = get("//tmp/{0}/@chunk_ids".format(tbl))
        for chunk_id in chunk_ids:
            replicas = get("#" + chunk_id + "/@stored_replicas")
            for replica in replicas:
                if replica.attributes["medium"] == medium:
                    chunk_count += 1

        return chunk_count

    def _assert_account_and_table_usage_equal(self, tbl):
        multicell_sleep()
        account_usage = get("//sys/accounts/tmp/@resource_usage/disk_space_per_medium")
        table_usage = get("//tmp/{0}/@resource_usage/disk_space_per_medium".format(tbl))
        assert account_usage == table_usage

    def test_default_store_medium_name(self):
        assert get("//sys/media/default/@name") == "default"

    def test_default_store_medium_index(self):
        assert get("//sys/media/default/@index") == 0

    def test_default_cache_medium_name(self):
        assert get("//sys/media/cache/@name") == "cache"

    def test_default_cache_medium_index(self):
        assert get("//sys/media/cache/@index") == 1

    def test_create(self):
        assert get("//sys/media/hdd3/@name") == "hdd3"
        assert get("//sys/media/hdd3/@index") > 0

    def test_create_too_many_fails(self):
        with pytest.raises(YtError) :
            create_medium("excess_medium")

    def test_rename(self):
        hdd3_index = get("//sys/media/hdd3/@index")

        set("//sys/media/hdd3/@name", "hdd33")
        assert exists("//sys/media/hdd33")
        assert get("//sys/media/hdd33/@name") == "hdd33"
        assert get("//sys/media/hdd33/@index") == hdd3_index
        assert not exists("//sys/media/hdd3")

    def test_rename_duplicate_name_fails(self):
        with pytest.raises(YtError): set("//sys/media/hdd4/@name", "hdd5")

    def test_rename_default_fails(self):
        with pytest.raises(YtError): set("//sys/media/default/@name", "new_default")
        with pytest.raises(YtError): set("//sys/media/cache/@name", "new_cache")

    def test_create_empty_name_fails(self):
        with pytest.raises(YtError): create_medium("")

    def test_create_duplicate_name_fails(self):
        with pytest.raises(YtError): create_medium(TestMedia.NON_DEFAULT_MEDIUM)

    def test_new_table_attributes(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a" : "b"})

        replication_factor = get("//tmp/t1/@replication_factor")
        assert replication_factor > 0
        assert get("//tmp/t1/@vital")

        assert get("//tmp/t1/@primary_medium") == "default"

        tbl_media = get("//tmp/t1/@media")
        assert tbl_media["default"]["replication_factor"] == replication_factor
        assert not tbl_media["default"]["data_parts_only"]
        self._assert_account_and_table_usage_equal("t1")

    def test_assign_additional_medium(self):
        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"a" : "b"})

        tbl_media = get("//tmp/t2/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {"replication_factor": 7, "data_parts_only": False}

        set("//tmp/t2/@media", tbl_media)

        tbl_media = get("//tmp/t2/@media")
        assert tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["replication_factor"] == 7
        assert not tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["data_parts_only"]

        set("//tmp/t2/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)

        assert get("//tmp/t2/@primary_medium") == TestMedia.NON_DEFAULT_MEDIUM
        assert get("//tmp/t2/@replication_factor") == 7

        sleep(TestMedia.REPLICATOR_REACTION_TIME)

        assert self._count_chunks_on_medium("t2", "default") == 3
        assert self._count_chunks_on_medium("t2", TestMedia.NON_DEFAULT_MEDIUM) == 7
        self._assert_account_and_table_usage_equal("t2")

    def test_move_between_media(self):
        create("table", "//tmp/t3")
        write_table("//tmp/t3", {"a" : "b"})

        tbl_media = get("//tmp/t3/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {"replication_factor": 3, "data_parts_only": False}

        set("//tmp/t3/@media", tbl_media)
        set("//tmp/t3/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)

        tbl_media["default"] = {"replication_factor": 0, "data_parts_only": False}
        set("//tmp/t3/@media", tbl_media)

        tbl_media = get("//tmp/t3/@media")
        assert "default" not in tbl_media
        assert tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["replication_factor"] == 3
        assert not tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["data_parts_only"]

        assert get("//tmp/t3/@primary_medium") == TestMedia.NON_DEFAULT_MEDIUM
        assert get("//tmp/t3/@replication_factor") == 3

        sleep(TestMedia.REPLICATOR_REACTION_TIME)

        self._assert_all_chunks_on_medium("t3", TestMedia.NON_DEFAULT_MEDIUM)
        self._assert_account_and_table_usage_equal("t3")

    def test_move_between_media_shortcut(self):
        create("table", "//tmp/t4")
        write_table("//tmp/t4", {"a" : "b"})

        # Shortcut: making a previously disabled medium primary *moves* the table to that medium.
        set("//tmp/t4/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)

        tbl_media = get("//tmp/t4/@media")
        assert "default" not in tbl_media
        assert tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["replication_factor"] == 3
        assert not tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["data_parts_only"]

        assert get("//tmp/t4/@primary_medium") == TestMedia.NON_DEFAULT_MEDIUM
        assert get("//tmp/t4/@replication_factor") == 3

        sleep(TestMedia.REPLICATOR_REACTION_TIME)

        self._assert_all_chunks_on_medium("t4", TestMedia.NON_DEFAULT_MEDIUM)
        self._assert_account_and_table_usage_equal("t4")

    def test_assign_empty_medium_fails(self):
        create("table", "//tmp/t5")
        write_table("//tmp/t5", {"a" : "b"})

        empty_tbl_media = {"default": {"replication_factor": 0, "data_parts_only": False}}
        with pytest.raises(YtError): set("//tmp/t5/@media", empty_tbl_media)

        partity_loss_tbl_media = {"default": {"replication_factor": 3, "data_parts_only": True}}
        with pytest.raises(YtError): set("//tmp/t5/@media", partity_loss_tbl_media)

    def test_write_to_non_default_medium(self):
        create("table", "//tmp/t6")
        # Move table into non-default medium.
        set("//tmp/t6/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)
        write_table("<append=true>//tmp/t6", {"a" : "b"}, table_writer={"medium_name": TestMedia.NON_DEFAULT_MEDIUM})
        self._assert_all_chunks_on_medium("t6", TestMedia.NON_DEFAULT_MEDIUM)
        self._assert_account_and_table_usage_equal("t6")

    def test_chunks_inherit_properties(self):
        create("table", "//tmp/t7")
        write_table("//tmp/t7", {"a" : "b"})

        tbl_media_1 = get("//tmp/t7/@media")
        tbl_vital_1 = get("//tmp/t7/@vital")

        chunk_ids = get("//tmp/t7/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        chunk_media_1 = copy.deepcopy(tbl_media_1)
        chunk_vital_1 = tbl_vital_1

        assert chunk_media_1 == get("#" + chunk_id + "/@media")
        assert chunk_vital_1 == get("#" + chunk_id + "/@vital")

        # Modify table properties in some way.
        tbl_media_2 = copy.deepcopy(tbl_media_1)
        tbl_vital_2 = not tbl_vital_1
        tbl_media_2["hdd6"] = {"replication_factor": 7, "data_parts_only": True}

        set("//tmp/t7/@media", tbl_media_2)
        set("//tmp/t7/@vital", tbl_vital_2)

        assert tbl_media_2 == get("//tmp/t7/@media")
        assert tbl_vital_2 == get("//tmp/t7/@vital")

        chunk_media_2 = copy.deepcopy(tbl_media_2)
        chunk_vital_2 = tbl_vital_2

        sleep(2.0) # Wait for chunk update to take effect.

        assert chunk_media_2 == get("#" + chunk_id + "/@media")
        assert chunk_vital_2 == get("#" + chunk_id + "/@vital")

    def test_no_create_cache_media(self):
        with pytest.raises(YtError): create_medium("new_cache", attributes={"cache": True})

    def test_chunks_intersecting_by_nodes(self):
        create("table", "//tmp/t8")
        write_table("//tmp/t8", {"a" : "b"})

        tbl_media = get("//tmp/t8/@media")
        # Forcing chunks to be placed twice on one node (once for each medium) by making
        # replication factor equal to the number of nodes.
        tbl_media["default"]["replication_factor"] = TestMedia.NUM_NODES
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {"replication_factor": TestMedia.NUM_NODES, "data_parts_only": False}

        set("//tmp/t8/@media", tbl_media)

        assert get("//tmp/t8/@replication_factor") == TestMedia.NUM_NODES
        assert get("//tmp/t8/@media/default/replication_factor") == TestMedia.NUM_NODES
        assert get("//tmp/t8/@media/{}/replication_factor".format(TestMedia.NON_DEFAULT_MEDIUM)) == TestMedia.NUM_NODES

        sleep(self.REPLICATOR_REACTION_TIME)

        assert self._count_chunks_on_medium("t8", "default") == TestMedia.NUM_NODES
        assert self._count_chunks_on_medium("t8", TestMedia.NON_DEFAULT_MEDIUM) == TestMedia.NUM_NODES

    def test_default_media_priorities(self):
        assert get("//sys/media/default/@priority") == 0
        assert get("//sys/media/cache/@priority") == 0

    def test_new_medium_default_priority(self):
        assert get("//sys/media/{}/@priority".format(TestMedia.NON_DEFAULT_MEDIUM)) == 0

    def test_set_medium_priority(self):
        assert get("//sys/media/hdd4/@priority") == 0
        set("//sys/media/hdd4/@priority", 7)
        assert get("//sys/media/hdd4/@priority") == 7
        set("//sys/media/hdd4/@priority", 10)
        assert get("//sys/media/hdd4/@priority") == 10
        set("//sys/media/hdd4/@priority", 0)
        assert get("//sys/media/hdd4/@priority") == 0

    def test_set_incorrect_medium_priority(self):
        assert get("//sys/media/hdd5/@priority") == 0
        with pytest.raises(YtError): set("//sys/media/hdd5/@priority", 11)
        with pytest.raises(YtError): set("//sys/media/hdd5/@priority", -1)

    def test_journal_medium(self):
        create("journal", "//tmp/j", attributes={"primary_medium": self.NON_DEFAULT_MEDIUM})
        assert exists("//tmp/j/@media/{0}".format(self.NON_DEFAULT_MEDIUM))
        data = [{"data" : "payload" + str(i)} for i in xrange(0, 10)]
        write_journal("//tmp/j", data)
        self.wait_until_sealed("//tmp/j")
        chunk_ids = get("//tmp/j/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        for replica in get("#{0}/@stored_replicas".format(chunk_id)):
            assert replica.attributes["medium"] == self.NON_DEFAULT_MEDIUM
        
    def test_file_medium(self):
        create("file", "//tmp/f", attributes={"primary_medium": self.NON_DEFAULT_MEDIUM})
        assert exists("//tmp/f/@media/{0}".format(self.NON_DEFAULT_MEDIUM))
        write_file("//tmp/f", "payload")
        chunk_ids = get("//tmp/f/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        for replica in get("#{0}/@stored_replicas".format(chunk_id)):
            assert replica.attributes["medium"] == self.NON_DEFAULT_MEDIUM

    def test_table_medium(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.NON_DEFAULT_MEDIUM})
        assert exists("//tmp/t/@media/{0}".format(self.NON_DEFAULT_MEDIUM))
        write_table("//tmp/t", [{"key": "value"}])
        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        for replica in get("#{0}/@stored_replicas".format(chunk_id)):
            assert replica.attributes["medium"] == self.NON_DEFAULT_MEDIUM

    def test_chunk_statuses_1_media(self):
        codec = "reed_solomon_6_3"
        codec_replica_count = 9

        create("table", "//tmp/t9")
        set("//tmp/t9/@erasure_codec", codec)
        write_table("//tmp/t9", {"a" : "b"})

        # Actually replicating multiple data pieces takes time -
        # single REPLICATOR_REACTION_TIME is not enough.
        time.sleep(2*TestMedia.REPLICATOR_REACTION_TIME)

        chunk_ids = get("//tmp/t9/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        self._assert_chunk_ok(True, chunk_id, {"default"})

        stored_replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(stored_replicas) == codec_replica_count

        replicas = __builtin__.set()
        replica_media = __builtin__.set()

        for r in stored_replicas:
            replicas.add(str(r)) # str() is for attribute stripping.
            replica_media.add(r.attributes["medium"])

        assert {"default"} == replica_media

        self._ban_nodes(replicas)

        time.sleep(TestMedia.REPLICATOR_REACTION_TIME)

        self._assert_chunk_ok(False, chunk_id, {"default"})

    def test_chunk_statuses_2_media(self):
        codec = "reed_solomon_6_3"
        codec_replica_count = 9

        create("table", "//tmp/t9")
        set("//tmp/t9/@erasure_codec", codec)
        write_table("//tmp/t9", {"a" : "b"})

        tbl_media = get("//tmp/t9/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {"replication_factor": 3, "data_parts_only": False}
        set("//tmp/t9/@media", tbl_media)

        relevant_media = {"default", TestMedia.NON_DEFAULT_MEDIUM}

        # Actually replicating multiple data pieces takes time -
        # single REPLICATOR_REACTION_TIME is not enough.
        time.sleep(2*TestMedia.REPLICATOR_REACTION_TIME)

        chunk_ids = get("//tmp/t9/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        self._assert_chunk_ok(True, chunk_id, relevant_media)

        stored_replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(stored_replicas) == len(relevant_media) * codec_replica_count

        replicas = __builtin__.set()
        replica_media = __builtin__.set()

        for r in stored_replicas:
            replicas.add(str(r)) # str() is for attribute stripping.
            replica_media.add(r.attributes["medium"])

        assert relevant_media == replica_media

        self._ban_nodes(replicas)

        time.sleep(TestMedia.REPLICATOR_REACTION_TIME)

        self._assert_chunk_ok(False, chunk_id, relevant_media)

    def _assert_chunk_ok(self, ok, chunk_id, media):
        available = get("#%s/@available" % chunk_id)
        replication_status = get("#%s/@replication_status" % chunk_id)
        lvc = ls("//sys/lost_vital_chunks")
        for medium in media:
            if ok:
                assert available
                assert not replication_status[medium]["underreplicated"]
                assert not replication_status[medium]["lost"]
                assert not replication_status[medium]["data_missing"]
                assert not replication_status[medium]["parity_missing"]
                len(lvc) == 0
            else:
                assert not available
                assert replication_status[medium]["lost"]
                assert replication_status[medium]["data_missing"]
                assert replication_status[medium]["parity_missing"]
                len(lvc) != 0

    def _ban_nodes(self, nodes):
        banned = False
        for node in ls("//sys/nodes"):
            if node in nodes:
                set("//sys/nodes/{0}/@banned".format(node), True)
                banned = True
        assert banned

    def test_create_with_invalid_attrs_yt_7093(self):
        with pytest.raises(YtError): create_medium("x", attributes={"priority": "hello"})
        assert not exists("//sys/media/x")

################################################################################

class TestMediaMulticell(TestMedia):
    NUM_SECONDARY_MASTER_CELLS = 2
