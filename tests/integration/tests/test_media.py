import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import copy
import random

from time import sleep

################################################################################

class TestMedia(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20

    REPLICATOR_REACTION_TIME = 3.5

    NON_DEFAULT_MEDIUM = "hdd2"

    @classmethod
    def modify_node_config(cls, config):
        # Patch half of nodes to be configured with something other than default medium.
        assert len(config["data_node"]["store_locations"]) == 1
        config["data_node"]["store_locations"][0]["medium_name"] = random.choice(("default", cls.NON_DEFAULT_MEDIUM))

    @classmethod
    def on_masters_started(cls):
        create_medium(cls.NON_DEFAULT_MEDIUM)

    def _ensure_max_num_of_media_created(self):
        medium_count = len(get_media())
        while (medium_count < 7):
            create_medium("hdd" + str(medium_count))
            medium_count += 1

    def _assert_all_chunks_on_nodes_with_medium(self, tbl, medium):
        chunk_ids = get("//tmp/{0}/@chunk_ids".format(tbl))
        for chunk_id in chunk_ids:
            replicas = get("#" + chunk_id + "/@stored_replicas")
            for replica in replicas:
                assert replica.attributes["medium"] == medium

    def test_default_store_medium_name(self):
        assert get("//sys/media/default/@name") == "default"

    def test_default_store_medium_index(self):
        assert get("//sys/media/default/@index") == 0

    def test_default_cache_medium_name(self):
        assert get("//sys/media/cache/@name") == "cache"

    def test_default_cache_medium_index(self):
        assert get("//sys/media/cache/@index") == 1

    def test_create(self):
        self._ensure_max_num_of_media_created()

        assert get("//sys/media/hdd3/@name") == "hdd3"
        assert get("//sys/media/hdd3/@index") > 0

    def test_create_too_many_fails(self):
        self._ensure_max_num_of_media_created()
        with pytest.raises(YtError) :
            create_medium("excess_medium")

    def test_rename(self):
        self._ensure_max_num_of_media_created()

        hdd3_index = get("//sys/media/hdd3/@index")

        set("//sys/media/hdd3/@name", "hdd33")
        assert exists("//sys/media/hdd33")
        assert get("//sys/media/hdd33/@name") == "hdd33"
        assert get("//sys/media/hdd33/@index") == hdd3_index
        assert not exists("//sys/media/hdd3")

    def test_rename_duplicate_name_fails(self):
        self._ensure_max_num_of_media_created()
        with pytest.raises(YtError): set("//sys/media/hdd4/@name", "hdd5")

    def test_rename_default_fails(self):
        with pytest.raises(YtError): set("//sys/media/default/@name", "new_default")
        with pytest.raises(YtError): set("//sys/media/cache/@name", "new_cache")

    def test_create_empty_name_fails(self):
        with pytest.raises(YtError): create_medium("")

    def test_create_duplicate_name_fails(self):
        self._ensure_max_num_of_media_created()
        with pytest.raises(YtError): create_medium(TestMedia.NON_DEFAULT_MEDIUM)

    def test_new_table_attributes(self):
        self._ensure_max_num_of_media_created()

        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a" : "b"})

        replication_factor = get("//tmp/t1/@replication_factor")
        assert replication_factor > 0
        assert get("//tmp/t1/@vital")

        assert get("//tmp/t1/@primary_medium") == "default"

        tbl_media = get("//tmp/t1/@media")
        assert tbl_media["default"]["replication_factor"] == replication_factor
        assert not tbl_media["default"]["data_parts_only"]

    def test_assign_additional_medium(self):
        self._ensure_max_num_of_media_created()

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"a" : "b"})

        tbl_media = get("//tmp/t2/@media")
        tbl_media["hdd6"] = {"replication_factor": 7, "data_parts_only": False}

        set("//tmp/t2/@media", tbl_media)

        tbl_media = get("//tmp/t2/@media")
        assert tbl_media["hdd6"]["replication_factor"] == 7
        assert not tbl_media["hdd6"]["data_parts_only"]

        set("//tmp/t2/@primary_medium", "hdd6")

        assert get("//tmp/t2/@primary_medium") == "hdd6"
        assert get("//tmp/t2/@replication_factor") == 7

    def test_move_between_media(self):
        self._ensure_max_num_of_media_created()

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

        self._assert_all_chunks_on_nodes_with_medium("t3", TestMedia.NON_DEFAULT_MEDIUM)

    def test_move_between_media_shortcut(self):
        self._ensure_max_num_of_media_created()

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

        self._assert_all_chunks_on_nodes_with_medium("t4", TestMedia.NON_DEFAULT_MEDIUM)

    def test_assign_empty_medium_fails(self):
        create("table", "//tmp/t5")
        write_table("//tmp/t5", {"a" : "b"})

        empty_tbl_media = {"default": {"replication_factor": 0, "data_parts_only": False}}
        with pytest.raises(YtError): set("//tmp/t5/@media", empty_tbl_media)

        partity_loss_tbl_media = {"default": {"replication_factor": 3, "data_parts_only": True}}
        with pytest.raises(YtError): set("//tmp/t5/@media", partity_loss_tbl_media)

    # def test_write_to_non_default_medium(self):
    #     create("table", "//tmp/t6")
    #     # Move table into non-default medium.
    #     set("//tmp/t6/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)
    #     write_table("<append=true>//tmp/t6", {"a" : "b"}, table_writer={"medium_name": TestMedia.NON_DEFAULT_MEDIUM})
    #     self._assert_all_chunks_on_nodes_with_medium("t6", TestMedia.NON_DEFAULT_MEDIUM)

    def test_chunks_inherit_properties(self):
        self._ensure_max_num_of_media_created()

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"a" : "b"})

        tbl_media_1 = get("//tmp/t2/@media")
        tbl_vital_1 = get("//tmp/t2/@vital")

        chunk_ids = get("//tmp/t2/@chunk_ids")
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

        set("//tmp/t2/@media", tbl_media_2)
        set("//tmp/t2/@vital", tbl_vital_2)

        assert tbl_media_2 == get("//tmp/t2/@media")
        assert tbl_vital_2 == get("//tmp/t2/@vital")

        chunk_media_2 = copy.deepcopy(tbl_media_2)
        chunk_vital_2 = tbl_vital_2

        sleep(2.0) # Wait for chunk update to take effect.

        assert chunk_media_2 == get("#" + chunk_id + "/@media")
        assert chunk_vital_2 == get("#" + chunk_id + "/@vital")

    def test_no_create_cache_media(self):
        with pytest.raises(YtError): create_medium("new_cache", attributes={"cache": True})

################################################################################

class TestMediaMulticell(TestMedia):
    NUM_SECONDARY_MASTER_CELLS = 2
