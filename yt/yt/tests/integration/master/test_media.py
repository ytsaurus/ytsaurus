from py import builtin
from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait, create, ls, get, set, exists, remove,
    create_domestic_medium, create_s3_medium, write_file,
    read_table, write_table, write_journal, wait_until_sealed,
    get_singular_chunk_id, set_account_disk_space_limit, get_account_disk_space_limit,
    get_media, set_node_banned, set_all_nodes_banned, create_rack,
    make_random_string, map, print_debug, update_nodes_dynamic_config)

from yt.common import YtError

import pytest
from flaky import flaky

from copy import deepcopy
import builtins

import hashlib

################################################################################


class TestMediaBase(YTEnvSetup):
    def _check_all_chunks_on_medium(self, tbl, medium):
        chunk_ids = get("//tmp/{0}/@chunk_ids".format(tbl))
        for chunk_id in chunk_ids:
            replicas = get("#" + chunk_id + "/@stored_replicas")
            for replica in replicas:
                if replica.attributes["medium"] != medium:
                    return False
        return True
    
    def _count_chunks_on_medium(self, tbl, medium):
        chunk_count = 0
        chunk_ids = get("//tmp/{0}/@chunk_ids".format(tbl))
        for chunk_id in chunk_ids:
            replicas = get("#" + chunk_id + "/@stored_replicas")
            for replica in replicas:
                if replica.attributes["medium"] == medium:
                    chunk_count += 1

        return chunk_count
    
    def _remove_zeros(self, d):
        for k in list(d.keys()):
            if d[k] == 0:
                del d[k]
    
    def _check_account_and_table_usage_equal(self, tbl):
        account_usage = self._remove_zeros(get("//sys/accounts/tmp/@resource_usage/disk_space_per_medium"))
        table_usage = self._remove_zeros(get("//tmp/{0}/@resource_usage/disk_space_per_medium".format(tbl)))
        return account_usage == table_usage

    def _check_chunk_ok(self, ok, chunk_id, media, print_progress=False):
        available = get("#%s/@available" % chunk_id)
        replication_status = get("#%s/@replication_status" % chunk_id)
        lvc = ls("//sys/lost_vital_chunks")
        media_in_unexpected_state = 0
        for medium in media:
            if ok:
                if (
                        not available
                        or medium not in replication_status
                        or replication_status[medium]["underreplicated"]
                        or replication_status[medium]["lost"]
                        or replication_status[medium]["data_missing"]
                        or replication_status[medium]["parity_missing"]
                        or len(lvc) != 0
                ):
                    media_in_unexpected_state += 1
            else:
                if (
                        available
                        or medium not in replication_status
                        or not replication_status[medium]["lost"]
                        or not replication_status[medium]["data_missing"]
                        or not replication_status[medium]["parity_missing"]
                        or len(lvc) == 0
                ):
                    media_in_unexpected_state += 1
        if print_progress and media_in_unexpected_state > 0:
            print_debug(f"Chunk {chunk_id} is {'not ok' if ok else 'ok'} on {media_in_unexpected_state} media")
        return media_in_unexpected_state == 0

    def _get_chunk_replica_nodes(self, chunk_id):
        return builtins.set(str(r) for r in get("#{0}/@stored_replicas".format(chunk_id)))

    def _get_chunk_replica_media(self, chunk_id):
        return builtins.set(r.attributes["medium"] for r in get("#{0}/@stored_replicas".format(chunk_id)))

    def _ban_nodes(self, nodes):
        banned = False
        for node in ls("//sys/cluster_nodes"):
            if node in nodes:
                set_node_banned(node, True)
                banned = True
        assert banned

    def _get_locations(self, node):
        return {
            location["location_uuid"]: location
            for location in get(node + "/@statistics/locations")
        }

    def _get_non_existent_location(self, node):
        locations = self._get_locations(node)
        for i in range(2**10):
            location_uuid = "1-1-1-{}".format(hex(i)[2:])
            if location_uuid not in locations:
                return location_uuid
        assert False, "BINGO!!!"

    def _create_domestic_medium(self, medium):
        if not exists(f"//sys/media/{medium}"):
            create_domestic_medium(medium)

    def _sync_set_medium_overrides(self, overrides):
        for location, medium in overrides:
            path = f"//sys/chunk_locations/{location}/@medium_override"
            set(path, medium)
        for location, medium in overrides:
            wait(lambda: get(f"//sys/chunk_locations/{location}/@statistics/medium_name") == medium)

    def _validate_empty_medium_overrides(self):
        for location in ls("//sys/chunk_locations"):
            remove(f"//sys/chunk_locations/{location}/@medium_override")

        def medium_overrides_applied():
            for location in ls("//sys/chunk_locations"):
                if get(f"//sys/chunk_locations/{location}/@statistics/medium_name") != "default":
                    return False
            return True

        wait(medium_overrides_applied)

    def _set_medium_override_and_wait(self, location, medium):
        set(f"//sys/chunk_locations/{location}/@medium_override", medium)
        wait(lambda: get(f"//sys/chunk_locations/{location}/@statistics/medium_name") == medium)


class TestMedia(TestMediaBase):
    NUM_MASTERS = 1
    NUM_NODES = 10
    STORE_LOCATION_COUNT = 3

    NON_DEFAULT_MEDIUM = "hdd1"
    NON_DEFAULT_TRANSIENT_MEDIUM = "hdd2"

    @classmethod
    def setup_class(cls):
        super(TestMedia, cls).setup_class()
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, TestMedia.NON_DEFAULT_MEDIUM)
        set_account_disk_space_limit("tmp", disk_space_limit, TestMedia.NON_DEFAULT_TRANSIENT_MEDIUM)

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 3

        config["data_node"]["store_locations"][0]["medium_name"] = "default"
        config["data_node"]["store_locations"][1]["medium_name"] = cls.NON_DEFAULT_MEDIUM
        config["data_node"]["store_locations"][2]["medium_name"] = cls.NON_DEFAULT_TRANSIENT_MEDIUM

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium(cls.NON_DEFAULT_MEDIUM)
        create_domestic_medium(cls.NON_DEFAULT_TRANSIENT_MEDIUM, attributes={"transient": True})
        medium_count = len(get_media())
        while medium_count < 119:
            create_domestic_medium("hdd" + str(medium_count))
            medium_count += 1
        create_s3_medium("s3", {
            "url": "http://yt.s3.amazonaws.com",
            "region": "us‑east‑2",
            "bucket": "yt",
            "access_key_id": "lol",
            "secret_access_key": "nope",
        })

    @authors()
    def test_default_store_medium_name(self):
        assert get("//sys/media/default/@name") == "default"

    @authors()
    def test_default_store_medium_index(self):
        assert get("//sys/media/default/@index") == 0

    @authors("gritukan")
    def test_default_store_medium_domestic(self):
        assert get("//sys/media/default/@domestic")

    @authors("shakurov")
    def test_create_simple(self):
        assert get("//sys/media/hdd4/@name") == "hdd4"
        assert get("//sys/media/hdd4/@index") > 0

    @authors("aozeritsky", "shakurov")
    def test_rename(self):
        hdd4_index = get("//sys/media/hdd4/@index")

        set("//sys/media/hdd4/@name", "hdd144")
        assert exists("//sys/media/hdd144")
        assert get("//sys/media/hdd144/@name") == "hdd144"
        assert get("//sys/media/hdd144/@index") == hdd4_index
        assert not exists("//sys/media/hdd4")
        set("//sys/media/hdd144/@name", "hdd4")

    @authors()
    def test_rename_duplicate_name_fails(self):
        with pytest.raises(YtError):
            set("//sys/media/hdd4/@name", "hdd5")

    @authors()
    def test_rename_default_fails(self):
        with pytest.raises(YtError):
            set("//sys/media/default/@name", "new_default")

    @authors()
    def test_create_empty_name_fails(self):
        with pytest.raises(YtError):
            create_domestic_medium("")

    @authors()
    def test_create_duplicate_name_fails(self):
        with pytest.raises(YtError):
            create_domestic_medium(TestMedia.NON_DEFAULT_MEDIUM)

    @authors("babenko")
    def test_new_table_attributes(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a": "b"})

        replication_factor = get("//tmp/t1/@replication_factor")
        assert replication_factor > 0
        assert get("//tmp/t1/@vital")

        assert get("//tmp/t1/@primary_medium") == "default"

        tbl_media = get("//tmp/t1/@media")
        assert tbl_media["default"]["replication_factor"] == replication_factor
        assert not tbl_media["default"]["data_parts_only"]
        wait(lambda: self._check_account_and_table_usage_equal("t1"))

    @authors("shakurov", "babenko")
    def test_assign_additional_medium(self):
        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"a": "b"})

        tbl_media = get("//tmp/t2/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {
            "replication_factor": 7,
            "data_parts_only": False,
        }

        set("//tmp/t2/@media", tbl_media)

        tbl_media = get("//tmp/t2/@media")
        assert tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["replication_factor"] == 7
        assert not tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["data_parts_only"]

        set("//tmp/t2/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)

        assert get("//tmp/t2/@primary_medium") == TestMedia.NON_DEFAULT_MEDIUM
        assert get("//tmp/t2/@replication_factor") == 7

        wait(
            lambda:
                self._count_chunks_on_medium("t2", "default") == 3
                and self._count_chunks_on_medium("t2", TestMedia.NON_DEFAULT_MEDIUM) == 7
                and self._check_account_and_table_usage_equal("t2"))

    @authors("babenko")
    def test_move_between_media(self):
        create("table", "//tmp/t3")
        write_table("//tmp/t3", {"a": "b"})

        assert get("//tmp/t3/@chunk_media_statistics/default/chunk_count") == 1

        tbl_media = get("//tmp/t3/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {
            "replication_factor": 3,
            "data_parts_only": False,
        }

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

        wait(
            lambda:
                self._check_all_chunks_on_medium("t3", TestMedia.NON_DEFAULT_MEDIUM)
                and self._check_account_and_table_usage_equal("t3"))

        assert get(f"//tmp/t3/@chunk_media_statistics/{TestMedia.NON_DEFAULT_MEDIUM}/chunk_count") == 1
        assert not exists("//tmp/t3/@chunk_media_statistics/default")

    @authors("babenko")
    def test_move_between_media_shortcut(self):
        create("table", "//tmp/t4")
        write_table("//tmp/t4", {"a": "b"})

        # Shortcut: making a previously disabled medium primary *moves* the table to that medium.
        set("//tmp/t4/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)

        tbl_media = get("//tmp/t4/@media")
        assert "default" not in tbl_media
        assert tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["replication_factor"] == 3
        assert not tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["data_parts_only"]

        assert get("//tmp/t4/@primary_medium") == TestMedia.NON_DEFAULT_MEDIUM
        assert get("//tmp/t4/@replication_factor") == 3

        wait(
            lambda:
                self._check_all_chunks_on_medium("t4", TestMedia.NON_DEFAULT_MEDIUM)
                and self._check_account_and_table_usage_equal("t4"))

    @authors("shakurov")
    def test_assign_empty_medium_fails(self):
        create("table", "//tmp/t5")
        write_table("//tmp/t5", {"a": "b"})

        empty_tbl_media = {"default": {"replication_factor": 0, "data_parts_only": False}}
        with pytest.raises(YtError):
            set("//tmp/t5/@media", empty_tbl_media)

        parity_loss_tbl_media = {"default": {"replication_factor": 3, "data_parts_only": True}}
        with pytest.raises(YtError):
            set("//tmp/t5/@media", parity_loss_tbl_media)

    @authors("babenko", "shakurov")
    def test_write_to_non_default_medium(self):
        create("table", "//tmp/t6")
        # Move table into non-default medium.
        set("//tmp/t6/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)
        write_table(
            "<append=true>//tmp/t6",
            {"a": "b"},
            table_writer={"medium_name": TestMedia.NON_DEFAULT_MEDIUM},
        )

        wait(
            lambda:
                self._check_all_chunks_on_medium("t6", TestMedia.NON_DEFAULT_MEDIUM)
                and self._check_account_and_table_usage_equal("t6"))

    @authors("shakurov", "babenko")
    def test_chunks_inherit_properties(self):
        create("table", "//tmp/t7")
        write_table("//tmp/t7", {"a": "b"})

        tbl_media_1 = get("//tmp/t7/@media")
        tbl_vital_1 = get("//tmp/t7/@vital")

        chunk_id = get_singular_chunk_id("//tmp/t7")

        chunk_media_1 = deepcopy(tbl_media_1)
        chunk_vital_1 = tbl_vital_1

        assert chunk_media_1 == get("#" + chunk_id + "/@media")
        assert chunk_vital_1 == get("#" + chunk_id + "/@vital")

        # Modify table properties in some way.
        tbl_media_2 = deepcopy(tbl_media_1)
        tbl_vital_2 = not tbl_vital_1
        tbl_media_2["hdd6"] = {"replication_factor": 7, "data_parts_only": True}

        set("//tmp/t7/@media", tbl_media_2)
        set("//tmp/t7/@vital", tbl_vital_2)

        assert tbl_media_2 == get("//tmp/t7/@media")
        assert tbl_vital_2 == get("//tmp/t7/@vital")

        chunk_media_2 = deepcopy(tbl_media_2)
        chunk_vital_2 = tbl_vital_2

        wait(
            lambda:
                chunk_media_2 == get("#" + chunk_id + "/@media")
                and chunk_vital_2 == get("#" + chunk_id + "/@vital"))

    @authors("shakurov")
    def test_chunks_intersecting_by_nodes(self):
        create("table", "//tmp/t8")
        write_table("//tmp/t8", {"a": "b"})

        tbl_media = get("//tmp/t8/@media")
        # Forcing chunks to be placed twice on one node (once for each medium) by making
        # replication factor equal to the number of nodes.
        tbl_media["default"]["replication_factor"] = TestMedia.NUM_NODES
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {
            "replication_factor": TestMedia.NUM_NODES,
            "data_parts_only": False,
        }

        set("//tmp/t8/@media", tbl_media)

        assert get("//tmp/t8/@replication_factor") == TestMedia.NUM_NODES
        assert get("//tmp/t8/@media/default/replication_factor") == TestMedia.NUM_NODES
        assert get("//tmp/t8/@media/{}/replication_factor".format(TestMedia.NON_DEFAULT_MEDIUM)) == TestMedia.NUM_NODES

        wait(
            lambda:
                self._count_chunks_on_medium("t8", "default") == TestMedia.NUM_NODES
                and self._count_chunks_on_medium("t8", TestMedia.NON_DEFAULT_MEDIUM) == TestMedia.NUM_NODES)

    @authors("shakurov")
    def test_default_media_priorities(self):
        assert get("//sys/media/default/@priority") == 0

    @authors("shakurov")
    def test_new_medium_default_priority(self):
        assert get("//sys/media/{}/@priority".format(TestMedia.NON_DEFAULT_MEDIUM)) == 0

    @authors("shakurov")
    def test_set_medium_priority(self):
        assert get("//sys/media/hdd4/@priority") == 0
        set("//sys/media/hdd4/@priority", 7)
        assert get("//sys/media/hdd4/@priority") == 7
        set("//sys/media/hdd4/@priority", 10)
        assert get("//sys/media/hdd4/@priority") == 10
        set("//sys/media/hdd4/@priority", 0)
        assert get("//sys/media/hdd4/@priority") == 0

    @authors("shakurov")
    def test_set_incorrect_medium_priority(self):
        assert get("//sys/media/hdd5/@priority") == 0
        with pytest.raises(YtError):
            set("//sys/media/hdd5/@priority", 11)
        with pytest.raises(YtError):
            set("//sys/media/hdd5/@priority", -1)

    @authors("babenko")
    def test_journal_medium(self):
        create("journal", "//tmp/j", attributes={"primary_medium": self.NON_DEFAULT_MEDIUM})
        assert exists("//tmp/j/@media/{0}".format(self.NON_DEFAULT_MEDIUM))
        data = [{"payload": "payload" + str(i)} for i in range(0, 10)]
        write_journal("//tmp/j", data, enable_chunk_preallocation=False)
        wait_until_sealed("//tmp/j")
        chunk_id = get_singular_chunk_id("//tmp/j")
        for replica in get("#{0}/@stored_replicas".format(chunk_id)):
            assert replica.attributes["medium"] == self.NON_DEFAULT_MEDIUM

    @authors("babenko")
    def test_file_medium(self):
        create("file", "//tmp/f", attributes={"primary_medium": self.NON_DEFAULT_MEDIUM})
        assert exists("//tmp/f/@media/{0}".format(self.NON_DEFAULT_MEDIUM))
        write_file("//tmp/f", b"payload")
        chunk_id = get_singular_chunk_id("//tmp/f")
        for replica in get("#{0}/@stored_replicas".format(chunk_id)):
            assert replica.attributes["medium"] == self.NON_DEFAULT_MEDIUM

    @authors("babenko")
    def test_table_medium(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.NON_DEFAULT_MEDIUM})
        assert exists("//tmp/t/@media/{0}".format(self.NON_DEFAULT_MEDIUM))
        write_table("//tmp/t", [{"key": "value"}])
        chunk_id = get_singular_chunk_id("//tmp/t")
        for replica in get("#{0}/@stored_replicas".format(chunk_id)):
            assert replica.attributes["medium"] == self.NON_DEFAULT_MEDIUM

    @authors("babenko", "shakurov")
    def test_chunk_statuses_1_media(self):
        codec = "isa_reed_solomon_6_3"
        codec_replica_count = 9

        create("table", "//tmp/t9")
        set("//tmp/t9/@erasure_codec", codec)
        write_table("//tmp/t9", {"a": "b"})

        assert get("//tmp/t9/@chunk_count") == 1
        chunk_id = get_singular_chunk_id("//tmp/t9")

        wait(
            lambda:
                self._check_chunk_ok(True, chunk_id, {"default"})
                and len(get("#{0}/@stored_replicas".format(chunk_id))) == codec_replica_count
                and self._get_chunk_replica_media(chunk_id) == {"default"})

        self._ban_nodes(self._get_chunk_replica_nodes(chunk_id))

        wait(lambda: self._check_chunk_ok(False, chunk_id, {"default"}))

    @authors("shakurov")
    def test_chunk_statuses_2_media(self):
        codec = "isa_reed_solomon_6_3"
        codec_replica_count = 9

        create("table", "//tmp/t9")
        set("//tmp/t9/@erasure_codec", codec)
        write_table("//tmp/t9", {"a": "b"})

        tbl_media = get("//tmp/t9/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {
            "replication_factor": 3,
            "data_parts_only": False,
        }
        set("//tmp/t9/@media", tbl_media)

        relevant_media = {"default", TestMedia.NON_DEFAULT_MEDIUM}

        assert get("//tmp/t9/@chunk_count") == 1
        chunk_id = get_singular_chunk_id("//tmp/t9")

        wait(
            lambda:
                self._check_chunk_ok(True, chunk_id, relevant_media)
                and len(get("#{0}/@stored_replicas".format(chunk_id))) == len(relevant_media) * codec_replica_count
                and self._get_chunk_replica_media(chunk_id) == relevant_media)

        self._ban_nodes(self._get_chunk_replica_nodes(chunk_id))

        wait(lambda: self._check_chunk_ok(False, chunk_id, relevant_media))

    @authors("babenko")
    def test_create_with_invalid_attrs_yt_7093(self):
        with pytest.raises(YtError):
            create_domestic_medium("x", attributes={"priority": "hello"})
        assert not exists("//sys/media/x")

    @authors("shakurov")
    def test_transient_only_chunks_not_vital_yt_9133(self):
        create(
            "table",
            "//tmp/t10_persistent",
            attributes={
                "primary_medium": self.NON_DEFAULT_MEDIUM,
                "media": {
                    self.NON_DEFAULT_MEDIUM: {
                        "replication_factor": 2,
                        "data_parts_only": False,
                    }
                },
            },
        )
        create(
            "table",
            "//tmp/t10_transient",
            attributes={
                "primary_medium": self.NON_DEFAULT_TRANSIENT_MEDIUM,
                "media": {
                    self.NON_DEFAULT_TRANSIENT_MEDIUM: {
                        "replication_factor": 2,
                        "data_parts_only": False,
                    }
                },
            },
        )
        write_table("//tmp/t10_persistent", {"a": "b"})
        write_table("//tmp/t10_transient", {"a": "b"})

        chunk1 = get_singular_chunk_id("//tmp/t10_persistent")
        chunk2 = get_singular_chunk_id("//tmp/t10_transient")

        wait(
            lambda:
                len(get("#{0}/@stored_replicas".format(chunk1))) == 2
                and len(get("#{0}/@stored_replicas".format(chunk2))) == 2)

        set("//sys/@config/chunk_manager/enable_chunk_replicator", False, recursive=True)
        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

        # NB: banning just the nodes with replicas is sufficient but racy: there
        # could be a straggler replication job. It's possible to account for it
        # but it's simpler to just ban everything.
        set_all_nodes_banned(True, wait_for_master=False)

        def persistent_chunk_is_lost_vital():
            lvc = ls("//sys/lost_vital_chunks")
            return chunk1 in lvc

        def transient_chunk_is_lost_but_not_vital():
            lc = ls("//sys/lost_chunks")
            lvc = ls("//sys/lost_vital_chunks")
            return chunk2 in lc and chunk2 not in lvc

        wait(persistent_chunk_is_lost_vital)
        wait(transient_chunk_is_lost_but_not_vital)

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_merge_chunks_for_nondefault_medium(self, merge_mode):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        primary_medium = TestMedia.NON_DEFAULT_MEDIUM

        create("table", "//tmp/t", attributes={"primary_medium": primary_medium})

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        info = read_table("//tmp/t")

        set("//tmp/t/@chunk_merger_mode", merge_mode)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == info

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@requisition/0/medium".format(chunk_ids[0])) == primary_medium

    @authors("aleksandra-zh")
    @pytest.mark.parametrize("merge_mode", ["deep", "shallow"])
    def test_merge_chunks_change_medium(self, merge_mode):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        primary_medium = TestMedia.NON_DEFAULT_MEDIUM

        create("table", "//tmp/t", attributes={"primary_medium": primary_medium})

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        info = read_table("//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@requisition/0/medium".format(chunk_ids[0])) == primary_medium

        another_primary_medium = TestMedia.NON_DEFAULT_TRANSIENT_MEDIUM
        set("//tmp/t/@primary_medium", another_primary_medium)

        set("//tmp/t/@chunk_merger_mode", merge_mode)
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)
        assert read_table("//tmp/t") == info

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert get("#{0}/@requisition/0/medium".format(chunk_ids[0])) == another_primary_medium

    @authors("aleksandra-zh")
    def test_merge_chunks_with_two_media(self):
        set("//sys/@config/chunk_manager/chunk_merger/enable", True)

        create("table", "//tmp/t")

        tbl_media = get("//tmp/t/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {
            "replication_factor": 2,
            "data_parts_only": False
        }
        set("//tmp/t/@media", tbl_media)

        write_table("<append=true>//tmp/t", {"a": "b"})
        write_table("<append=true>//tmp/t", {"a": "c"})
        write_table("<append=true>//tmp/t", {"a": "d"})

        set("//tmp/t/@chunk_merger_mode", "deep")
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        wait(lambda: get("//tmp/t/@resource_usage/chunk_count") == 1)

        chunk_ids = get("//tmp/t/@chunk_ids")

        def are_requisitions_correct():
            requisitions = get("#{0}/@requisition".format(chunk_ids[0]))
            return [r["medium"] for r in requisitions] == ["default", TestMedia.NON_DEFAULT_MEDIUM]
        wait(are_requisitions_correct)

    @authors("gritukan")
    def test_s3_medium(self):
        assert not get("//sys/media/s3/@domestic")
        assert get("//sys/media/s3/@config/bucket") == "yt"
        set("//sys/media/s3/@config/bucket", "ytsaurus")
        assert get("//sys/media/s3/@config/bucket") == "ytsaurus"

    @authors("danilalexeev")
    def test_zero_replicas_needed(self):
        create("table", "//tmp/t2", attributes={"erasure_codec": "isa_reed_solomon_6_3"})
        write_table("//tmp/t2", {"a": "b"})

        tbl_media = get("//tmp/t2/@media")
        tbl_media[TestMedia.NON_DEFAULT_MEDIUM] = {
            "replication_factor": 1,
            "data_parts_only": True,
        }

        set("//tmp/t2/@media", tbl_media)
        wait(
            lambda:
                self._count_chunks_on_medium("t2", "default") == 9
                and self._count_chunks_on_medium("t2", TestMedia.NON_DEFAULT_MEDIUM) == 3
                and self._check_account_and_table_usage_equal("t2"))

        # Replica count:
        # 1 1 1 1 1 1 1 1 1 default <- primary
        # 1 1 1 0 0 0 0 0 0 hdd1

        set("//sys/@config/chunk_manager/enable_chunk_refresh", False)

        tbl_media[TestMedia.NON_DEFAULT_MEDIUM]["data_parts_only"] = False
        set("//tmp/t2/@media", tbl_media)

        set("//tmp/t2/@primary_medium", TestMedia.NON_DEFAULT_MEDIUM)
        tbl_media["default"] = {"replication_factor": 0, "data_parts_only": False}
        set("//tmp/t2/@media", tbl_media)

        set(f"//sys/media/{TestMedia.NON_DEFAULT_MEDIUM}/@config/max_replicas_per_rack", 1)

        create_rack("r0")
        chunk_id = get_singular_chunk_id("//tmp/t2")
        nodes = get(f"#{chunk_id}/@stored_replicas")
        for node in nodes:
            if node.attributes["medium"] == TestMedia.NON_DEFAULT_MEDIUM:
                set("//sys/cluster_nodes/" + node + "/@rack", "r0")

        # Replica count:
        # 1 1 1 1 1 1 1 1 1 test-medium <- primary

        set("//sys/@config/chunk_manager/enable_chunk_refresh", True)

        wait(
            lambda:
                self._check_all_chunks_on_medium("t2", TestMedia.NON_DEFAULT_MEDIUM)
                and self._check_account_and_table_usage_equal("t2"))


################################################################################


class TestMediaMulticell(TestMedia):
    NUM_SECONDARY_MASTER_CELLS = 2


################################################################################


class TestDynamicMedia(TestMediaBase):
    NUM_MASTERS = 1
    NUM_NODES = 1
    STORE_LOCATION_COUNT = 2

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100,
        },
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 10
            }
        }
    }

    def teardown_method(self, method):
        for node in ls("//sys/data_nodes"):
            for location in self._get_locations(f"//sys/data_nodes/{node}"):
                remove(f"//sys/chunk_locations/{location}/@medium_override")

        super(TestDynamicMedia, self).teardown_method(method)

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 2

        config["data_node"]["store_locations"][0]["medium_name"] = "default"
        config["data_node"]["store_locations"][1]["medium_name"] = "default"

    @authors("kvk1920")
    def test_medium_change_simple(self):
        self._validate_empty_medium_overrides()
        node = "//sys/data_nodes/" + ls("//sys/data_nodes")[0]

        medium_name = "testmedium"
        if not exists("//sys/media/" + medium_name):
            create_domestic_medium(medium_name)

        table = "//tmp/t"
        create("table", table, attributes={"replication_factor": 1})
        write_table(table, {"foo": "bar"}, table_writer={"upload_replication_factor": 1})

        location1, location2 = self._get_locations(node).keys()

        wait(lambda: sum([location["chunk_count"] for location in self._get_locations(node).values()]) == 1)

        set("//sys/chunk_locations/{}/@medium_override".format(location1), medium_name)

        wait(lambda: self._get_locations(node)[location1]["medium_name"] == medium_name)
        wait(lambda: self._get_locations(node)[location2]["medium_name"] == "default")

        wait(lambda: self._get_locations(node)[location1]["chunk_count"] == 0)
        assert self._get_locations(node)[location2]["chunk_count"] >= 1
        wait(lambda: self._get_locations(node)[location2]["chunk_count"] == 1)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        assert self._get_locations(node)[location1]["medium_name"] == medium_name
        assert self._get_locations(node)[location2]["medium_name"] == "default"

        remove("//sys/chunk_locations/{}/@medium_override".format(location1))
        set("//sys/chunk_locations/{}/@medium_override".format(location2), medium_name)

        wait(lambda: self._get_locations(node)[location1]["medium_name"] == "default")
        wait(lambda: self._get_locations(node)[location2]["medium_name"] == medium_name)

        wait(lambda: self._get_locations(node)[location2]["chunk_count"] == 0)
        assert self._get_locations(node)[location1]["chunk_count"] >= 1
        wait(lambda: self._get_locations(node)[location1]["chunk_count"] == 1)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        assert self._get_locations(node)[location1]["medium_name"] == "default"
        assert self._get_locations(node)[location2]["medium_name"] == medium_name

    @authors("kvk1920")
    def test_medium_override_removal(self):
        self._validate_empty_medium_overrides()
        medium = "testmedium"
        if not exists(f"//sys/media/{medium}"):
            create_domestic_medium(medium)

        location = ls("//sys/chunk_locations")[0]
        assert get(f"//sys/chunk_locations/{location}/@statistics/medium_name") == "default"
        set(f"//sys/chunk_locations/{location}/@medium_override", medium)
        wait(lambda: get(f"//sys/chunk_locations/{location}/@statistics/medium_name") == medium)
        remove(f"//sys/chunk_locations/{location}/@medium_override")
        wait(lambda: get(f"//sys/chunk_locations/{location}/@statistics/medium_name") == "default")

    @authors("kvk1920")
    def test_replica_collision1(self):
        self._validate_empty_medium_overrides()
        medium = "testmedium"
        self._create_domestic_medium("testmedium")
        tombstone = "tombstone"
        self._create_domestic_medium("tombstone")

        locations = ls("//sys/chunk_locations")
        location = locations[0]
        self._sync_set_medium_overrides([(location, medium)])
        create("table", "//tmp/t", attributes={"media": {
            "default": {"replication_factor": 1, "data_parts_only": False},
            medium: {"replication_factor": 1, "data_parts_only": False},
        }})
        write_table("//tmp/t", {"foo": "bar"})
        chunk = get_singular_chunk_id("//tmp/t")
        wait(lambda: len(get(f"#{chunk}/@stored_replicas")) == 2)

        def get_used_space():
            return [
                get(f"//sys/chunk_locations/{loc}/@statistics/used_space")
                for loc in locations
            ]
        used_space = get_used_space()

        remove(f"//sys/chunk_locations/{location}/@medium_override")
        set("//tmp/t/@media", {"default": {"replication_factor": 1, "data_parts_only": False}})
        wait(lambda: get(f"//sys/chunk_locations/{location}/@statistics/medium_name") == "default")
        wait(lambda: len(get(f"#{chunk}/@stored_replicas")) == 1)
        assert read_table("//tmp/t") == [{"foo": "bar"}]

        set("//tmp/t/@media", {
            "default": {"replication_factor": 1, "data_parts_only": False},
            medium: {"replication_factor": 1, "data_parts_only": False},
        })
        self._sync_set_medium_overrides([(location, medium)])
        self._sync_set_medium_overrides([(locations[1], tombstone)])
        self._sync_set_medium_overrides([(locations[1], "default")])
        wait(lambda: len(get(f"#{chunk}/@stored_replicas")) == 2)
        assert read_table("//tmp/t") == [{"foo": "bar"}]
        # NB: Total used space should not change.
        wait(lambda: get_used_space() == used_space)

    @authors("kvk1920")
    @flaky(max_runs=2)
    def test_replica_collision2(self):
        self._validate_empty_medium_overrides()
        m1, m2 = "m1", "m2"
        self._create_domestic_medium(m1)
        self._create_domestic_medium(m2)
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        for m in (m1, m2):
            set_account_disk_space_limit("tmp", disk_space_limit, m)
        loc1, loc2 = ls("//sys/chunk_locations")[:2]
        self._sync_set_medium_overrides([(loc1, m1), (loc2, m2)])
        t = "//tmp/t"
        create("table", t, attributes={
            "media": {
                m1: {"replication_factor": 1, "data_parts_only": False},
                m2: {"replication_factor": 1, "data_parts_only": False},
            },
            "primary_medium": m1
        })
        content = [{"foo": 1, "bar": "123"}, {"foo": 42, "bar": "321"}]
        write_table(t, content)
        chunk = get_singular_chunk_id(t)
        wait(lambda: len(get(f"#{chunk}/@stored_replicas")) == 2)

        def get_used_space():
            return [
                get(f"//sys/chunk_locations/{loc}/@statistics/used_space")
                for loc in (loc1, loc2)
            ]

        used_space = get_used_space()
        self._sync_set_medium_overrides([(loc1, m1), (loc2, m1)])
        replicas = get(f"#{chunk}/@stored_replicas")
        assert len(replicas) == 2
        assert all([replica.attributes["medium"] == m1 for replica in replicas])
        assert used_space == get_used_space()
        assert read_table(t) == content

        set(f"{t}/@media", {m1: {"replication_factor": 1, "data_parts_only": False}})
        wait(lambda: len(get(f"#{chunk}/@stored_replicas")) == 1)


################################################################################


# As media cannot be deleted, for now, it is best to use a separate test suite for tests with a significant number of them.
# NB: Some new media are created during the runtime of the tests, be careful when writing new ones!
class TestManyMedia(TestMediaBase):
    NON_DEFAULT_MEDIUM_COUNT = 300
    NON_DEFAULT_MEDIA = [f"hdd{i}" for i in range(NON_DEFAULT_MEDIUM_COUNT)]

    NUM_MASTERS = 1
    NUM_NODES = 10
    # Each non-default medium will have a single location assigned to it.
    # Default medium will have one location on each node.
    STORE_LOCATION_COUNT = NON_DEFAULT_MEDIUM_COUNT // NUM_NODES + 1

    # The periods below are set to extremely low values to speed up replication.
    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "chunk_refresh_period": 2,
            "chunk_requisition_update_period": 2,
        },
    }

    # The periods below are set to extremely low values to speed up replication.
    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 2,
        },
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 10,
            }
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "resource_limits": {
                "overrides": {
                    "replication_slots": 64,
                },
            },
        },
    }

    # It is impossible to modify data node configurations non-uniformely, so let's use a sledge-hammer to crack a nut.
    # TODO(achulkov2): It is better to pass node index to modify_node_config, but that warrants a separate refactoring PR.
    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index, cluster_path):
        super(TestManyMedia, cls).apply_config_patches(configs, ytserver_version, cluster_index, cluster_path)

        for node_index, config in enumerate(configs["node"]):
            locations = config["data_node"]["store_locations"]
            assert len(locations) == cls.STORE_LOCATION_COUNT

            locations[0]["medium_name"] = "default"

            for i in range(1, cls.STORE_LOCATION_COUNT):
                medium = cls.NON_DEFAULT_MEDIA[node_index * (cls.STORE_LOCATION_COUNT - 1) + (i - 1)]
                locations[i]["medium_name"] = medium

    @classmethod
    def on_masters_started(cls):
        for medium in cls.NON_DEFAULT_MEDIA:
            create_domestic_medium(medium)

    @classmethod
    def setup_class(cls, *args, **kwargs):
        super(TestManyMedia, cls).setup_class(*args, **kwargs)
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        for medium in cls.NON_DEFAULT_MEDIA:
            set_account_disk_space_limit("tmp", disk_space_limit, medium)

    # Returns a deterministic sample based on the given seed.
    @staticmethod
    def get_deterministic_sample(objects, sample_size, seed):
        assert sample_size <= len(objects)

        seed_bytes = str(seed).encode("utf-8")

        def key(obj):
            return hashlib.sha256(seed_bytes + str(obj).encode("utf-8")).digest(), obj

        shuffled = sorted(objects, key=key)

        return shuffled[:sample_size]

    # Returns a deterministic sample of non-default media based on the given seed.
    @classmethod
    def get_nondefault_media_sample(cls, sample_size, seed):
        return cls.get_deterministic_sample(cls.NON_DEFAULT_MEDIA, sample_size, seed)
    
    @classmethod
    def enumerate_nondefault_media_sample(cls, sample_size, seed):
        indexes = cls.get_deterministic_sample(range(len(cls.NON_DEFAULT_MEDIA)), sample_size, seed)
        for i in indexes:
            yield i, cls.NON_DEFAULT_MEDIA[i]

    @authors("achulkov2")
    def test_medium_index_assignment(self):
        assert get("//sys/media/default/@index") == 0

        for i, medium in self.enumerate_nondefault_media_sample(sample_size=30, seed=42):
            # Indexes 126 and 127 are historical sentinels.
            expected_medium_index = i + 1 if i + 1 < 126 else i + 3
            assert get(f"//sys/media/{medium}/@index") == expected_medium_index

        with pytest.raises(YtError):
            create_domestic_medium("excess_medium_same_index", attributes={"index": 42})

        # TODO(achulkov2): Test out of bounds index.

        create_domestic_medium("excess_medium_666", attributes={"index": 666})
        assert get("//sys/media/excess_medium_666/@index") == 666

        create_domestic_medium("excess_medium_get_mex")
        # Default medium + 2 sentinels + 300 non-default media => first free index is 303.
        assert get("//sys/media/excess_medium_get_mex/@index") == 1 + 2 + 300

    @authors("achulkov2")
    def test_chunk_movement(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        write_table("//tmp/t", [{"a": 1}])

        assert self._check_all_chunks_on_medium("t", "default")

        for medium in self.get_nondefault_media_sample(sample_size=10, seed=1543):
            print_debug(f"Moving table to medium {medium}")
            set("//tmp/t/@primary_medium", medium)
            wait(lambda: self._check_all_chunks_on_medium("t", medium) and self._check_account_and_table_usage_equal("t"))

        set("//tmp/t/@primary_medium", "default")
        wait(lambda: self._check_all_chunks_on_medium("t", "default") and self._check_account_and_table_usage_equal("t"))

    @authors("achulkov2")
    def test_many_replicas_with_removal(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        write_table("//tmp/t", [{"a": 1}])

        tbl_media = get("//tmp/t/@media")

        # This is a reasonably large number of replicas across newly indexed media,
        # but small enough for us to check both replication and removal.
        media_sample = self.get_nondefault_media_sample(sample_size=33, seed=179)

        for medium in media_sample:
            tbl_media[medium] = {"replication_factor": 1, "data_parts_only": False}

        set("//tmp/t/@media", tbl_media)

        chunk_id = get_singular_chunk_id("//tmp/t")
        expected_chunk_media = builtin.set(media_sample) | {"default"}

        # This takes around 15 seconds on a local machine, so should be fine.
        wait(
            lambda:
                self._check_chunk_ok(True, chunk_id, media_sample, print_progress=True)
                and self._get_chunk_replica_media(chunk_id) == expected_chunk_media)
        
        chunk_replica_nodes = self._get_chunk_replica_nodes(chunk_id)

        # We stop chunks from being removed to be able to test removal job scheduling.
        for node in chunk_replica_nodes:
            set(f"//sys/cluster_nodes/{node}/@resource_limits_overrides/removal_slots", 0)

        remove("//tmp/t")

        # Removal jobs should be scheduled properly.
        wait(lambda: all(get(f"//sys/cluster_nodes/{node}/@destroyed_chunk_replica_count") > 0 for node in chunk_replica_nodes))

        for node in chunk_replica_nodes:
            remove(f"//sys/cluster_nodes/{node}/@resource_limits_overrides/removal_slots")

        # We somewhat rely on nothing being removed at the same time as this test is running.
        # If that turns out not to be the case, there are ways to improve, but let's keep it simple for now.
        wait(lambda: all(get(f"//sys/cluster_nodes/{node}/@destroyed_chunk_replica_count") == 0 for node in chunk_replica_nodes))

    @authors("achulkov2")
    @pytest.mark.timeout(300)
    def test_extreme_replica_count(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        write_table("//tmp/t", [{"a": 1}])

        tbl_media = get("//tmp/t/@media")

        # We want to test a number of replicas larger than 8 bits.
        # This takes a while, as we only schedule one replication job at once,
        # and there is no easy way (or reason) to change that.
        media_sample = self.get_nondefault_media_sample(sample_size=257, seed=179)

        for medium in media_sample:
            tbl_media[medium] = {"replication_factor": 1, "data_parts_only": False}

        set("//tmp/t/@media", tbl_media)

        chunk_id = get_singular_chunk_id("//tmp/t")
        expected_chunk_media = builtin.set(media_sample) | {"default"}

        # NB: This large wait is impossible to avoid with such number of replicas.
        # It takes around 130 seconds on local machine, so we give some margin.
        wait(
            lambda:
                self._check_chunk_ok(True, chunk_id, media_sample, print_progress=True)
                and self._get_chunk_replica_media(chunk_id) == expected_chunk_media,
            timeout=180)
        
    # TODO(achulkov2): Finish this test.
        
    # @authors("achulkov2")
    # def test_medium_overrides(self):
    #     media_sample = self.get_nondefault_media_sample(sample_size=29, seed=2007)

    #     create("table", "//tmp/t", attributes={"replication_factor": 1, "primary_medium": media_sample[0]})

    #     # TODO(achulkov2): Create helper for this.
    #     tbl_media = get("//tmp/t/@media")
    #     for medium in media_sample[1:]:
    #         tbl_media[medium] = {"replication_factor": 1, "data_parts_only": False}

    #     set("//tmp/t/@media", tbl_media)

    #     write_table("//tmp/t", [{"a": 1}])

    #     chunk_id = get_singular_chunk_id("//tmp/t")

    #     wait(
    #         lambda:
    #             self._check_chunk_ok(True, chunk_id, media_sample, print_progress=True)
    #             and self._get_chunk_replica_media(chunk_id) == builtin.set(media_sample))

    #     location_to_node = {}

    #     for node in ls("//sys/data_nodes"):
    #         for location in self._get_locations(node).keys():
    #             location_to_node[location] = node
        
    #     medium_overrides = []
    #     new_nodes_for_medium = {}

    #     shuffled_nondefault_media = self.get_nondefault_media_sample(sample_size=len(self.NON_DEFAULT_MEDIA), seed=1329)

    #     next_medium_index = 0
    #     for location in ls("//sys/chunk_locations", attributes=["statistics", "medium_override"]):
    #         assert "medium_override" not in location.attributes

    #         medium = get(f"//sys/chunk_locations/{location}/@statistics/medium_name")

    #         if medium == "default":
    #             continue

    #         new_medium = shuffled_nondefault_media[next_medium_index]
    #         next_medium_index += 1

    #         medium_overrides.append((str(location), new_medium))




    #     nodes = ls("//sys/data_nodes")
        
    #     for replica in get(f"#{chunk_id}/@stored_replicas"):
    #         node = str(replica)
    #         medium = replica.attributes["medium"]
    #         location = replica.attributes["location_uuid"]

    #         new_node = self.get_deterministic_sample(
    #             [node for node in nodes if node != node],
    #             sample_size=1,
    #             seed=hashlib.sha256(f"{node}-{medium}-{location}".encode()).hexdigest())[0]
    #         medium_overrides.append((location, new_node))

        # Test outline:
        #   1. Create a table on a sample of media.
        #   2. Shuffle these media among nodes by setting medium overrides.
        #   3. Expect replicator to move replicas.