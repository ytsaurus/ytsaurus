import pytest

from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE
from yt_commands import *
import yt.yson
import random

##################################################################

class TestJournals(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5

    DATA = [
        {"data" : "payload-" + str(i) + "-" + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(i * i + random.randrange(10)))}
        for i in xrange(0, 10)
    ]

    def _write_and_wait_until_sealed(self, path, data, **kwargs):
        write_journal(path, data, **kwargs)
        wait_until_sealed(path)

    def _wait_until_last_chunk_sealed(self, path):
        chunk_ids = get(path + "/@chunk_ids")
        chunk_id = chunk_ids[-1]
        wait(lambda: all(r.attributes["state"] == "sealed" for r in get("#{}/@stored_replicas".format(chunk_id))))

    def _truncate_and_check(self, path, row_count, expected_row_count):
        truncate_journal(path, row_count)

        assert get(path + "/@sealed")
        assert get(path + "/@quorum_row_count") == expected_row_count


    @authors("babenko")
    def test_explicit_compression_codec_forbidden(self):
        with pytest.raises(YtError): create("journal", "//tmp/j", attributes={"compression_codec": "lz4"})

    @authors("babenko")
    def test_inherited_compression_codec_allowed(self):
        create("map_node", "//tmp/m", attributes={"compression_codec": "lz4"})
        create("journal", "//tmp/m/j")
        assert get("//tmp/m/j/@compression_codec") == "none"

    @authors("ignat")
    def test_create_regular_success(self):
        create("journal", "//tmp/j")
        assert get("//tmp/j/@erasure_codec") == "none"
        assert get("//tmp/j/@replication_factor") == 3
        assert get("//tmp/j/@read_quorum") == 2
        assert get("//tmp/j/@write_quorum") == 2
        assert get_chunk_owner_disk_space("//tmp/j") == 0
        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 0
        assert get("//tmp/j/@chunk_ids") == []

    @authors("babenko")
    def test_create_erasure_success(self):
        create("journal", "//tmp/j", attributes={
            "erasure_codec": "isa_lrc_12_2_2",
            "replication_factor": 1,
            "read_quorum": 14,
            "write_quorum": 15
        })
        assert get("//tmp/j/@erasure_codec") == "isa_lrc_12_2_2"
        assert get("//tmp/j/@replication_factor") == 1
        assert get("//tmp/j/@read_quorum") == 14
        assert get("//tmp/j/@write_quorum") == 15
        assert get_chunk_owner_disk_space("//tmp/j") == 0
        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 0
        assert get("//tmp/j/@chunk_ids") == []

    @authors("babenko", "ignat")
    def test_create_regular_failure(self):
        BAD_ATTRIBUTES = [
            {"replication_factor": 1},
            {"read_quorum": 4},
            {"write_quorum": 4},
            {"replication_factor": 4}
        ]
        for attributes in BAD_ATTRIBUTES:
            with pytest.raises(YtError): create("journal", "//tmp/j", attributes=attributes)

    @authors("babenko")
    def test_create_erasure_failure(self):
        BAD_ATTRIBUTES = [
            {"erasure_codec": "isa_lrc_12_2_2", "replication_factor": 2},
            {"erasure_codec": "isa_lrc_12_2_2", "read_quorum": 17},
            {"erasure_codec": "isa_lrc_12_2_2", "write_quorum": 17},
            {"erasure_codec": "isa_lrc_12_2_2", "read_quorum": 14, "write_quorum": 14}
        ]
        for attributes in BAD_ATTRIBUTES:
            with pytest.raises(YtError): create("journal", "//tmp/j", attributes=attributes)

    @authors("babenko", "ignat")
    def test_read_write(self):
        create("journal", "//tmp/j")
        for i in xrange(0, 10):
            self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 100
        assert get("//tmp/j/@chunk_count") == 10

        for i in xrange(0, 10):
            assert read_journal("//tmp/j[#" + str(i * 10) + ":]") == self.DATA * (10 - i)

        for i in xrange(0, 9):
            assert read_journal("//tmp/j[#" + str(i * 10 + 5) + ":]") == (self.DATA * (10 - i))[5:]

        assert read_journal("//tmp/j[#200:]") == []

    @authors("aleksandra-zh")
    def test_truncate1(self):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 10

        self._truncate_and_check("//tmp/j", 3, 3)
        self._truncate_and_check("//tmp/j", 10, 3)

    @authors("aleksandra-zh")
    def test_truncate2(self):
        create("journal", "//tmp/j")
        for i in xrange(0, 10):
            self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 100

        self._truncate_and_check("//tmp/j", 73, 73)
        self._truncate_and_check("//tmp/j", 2, 2)
        self._truncate_and_check("//tmp/j", 0, 0)

        for i in xrange(0, 10):
            self._write_and_wait_until_sealed("//tmp/j", self.DATA)
            truncate_journal("//tmp/j", (i + 1) * 3)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == 30

    @authors("aleksandra-zh")
    def test_truncate_unsealed(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False, recursive=True)

        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.DATA, journal_writer={"ignore_closing": True})

        with pytest.raises(YtError): truncate_journal("//tmp/j", 1)

    @authors("babenko")
    def test_resource_usage(self):
        wait(lambda: get_account_committed_disk_space("tmp") == 0)

        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        chunk_id = get_singular_chunk_id("//tmp/j")

        wait(lambda: get("#" + chunk_id + "/@sealed"))

        get("#" + chunk_id + "/@owning_nodes")
        disk_space_delta = get_chunk_owner_disk_space("//tmp/j")
        assert disk_space_delta > 0

        get("//sys/accounts/tmp/@")

        wait(lambda: get_account_committed_disk_space("tmp") == disk_space_delta and \
                     get_account_disk_space("tmp") == disk_space_delta)

        remove("//tmp/j")

        wait(lambda: get_account_committed_disk_space("tmp") == 0 and \
                     get_account_disk_space("tmp") == 0)

    @authors("babenko")
    def test_no_copy(self):
        create("journal", "//tmp/j1")
        with pytest.raises(YtError): copy("//tmp/j1", "//tmp/j2")

    @authors("babenko")
    def test_move(self):
        create("journal", "//tmp/j1")
        self._write_and_wait_until_sealed("//tmp/j1", self.DATA)

        move('//tmp/j1', '//tmp/j2')
        assert read_journal("//tmp/j2") == self.DATA

    @authors("babenko")
    def test_no_storage_change_after_creation(self):
        create("journal", "//tmp/j", attributes={"replication_factor": 5, "read_quorum": 3, "write_quorum": 3})
        with pytest.raises(YtError): set("//tmp/j/@replication_factor", 6)
        with pytest.raises(YtError): set("//tmp/j/@vital", False)
        with pytest.raises(YtError): set("//tmp/j/@primary_medium", "default")

    @authors("kiselyovp")
    def test_write_future_semantics(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False, recursive=True)

        create("journal", "//tmp/j1")
        write_journal("//tmp/j1", self.DATA, journal_writer={"ignore_closing": True})

        assert(read_journal("//tmp/j1") == self.DATA)

    @authors("ifsmirnov")
    def test_data_node_orchid(self):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", self.DATA)
        chunk_id = get("//tmp/j/@chunk_ids/0")
        replica = get("#{}/@last_seen_replicas/0".format(chunk_id))
        orchid = get("//sys/cluster_nodes/{}/orchid/stored_chunks/{}".format(replica, chunk_id))
        assert "location" in orchid
        assert "disk_space" in orchid

class TestJournalsMulticell(TestJournals):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestJournalsPortal(TestJournalsMulticell):
    ENABLE_TMP_PORTAL = True

class TestJournalsRpcProxy(TestJournals):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

##################################################################

class TestJournalsChangeMedia(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5

    DATA = [{"data" : "payload" + str(i)} for i in xrange(0, 10)]

    @authors("ilpauzner", "shakurov")
    def test_journal_replica_changes_medium_yt_8669(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False, recursive=True)

        create("journal", "//tmp/j1")
        write_journal("//tmp/j1", self.DATA, journal_writer={"ignore_closing": True})

        assert not get("//tmp/j1/@sealed")
        chunk_id = get_singular_chunk_id("//tmp/j1")
        assert not get("#{0}/@sealed".format(chunk_id))
        node_to_patch = str(get("#{0}/@stored_replicas".format(chunk_id))[0])

        with Restarter(self.Env, NODES_SERVICE):
            create_medium("ssd")

            patched_node_config = False
            for i in xrange(0, len(self.Env.configs["node"])):
                config = self.Env.configs["node"][i]

                node_address ="{0}:{1}".format(config["address_resolver"]["localhost_fqdn"], config["rpc_port"])

                if node_address == node_to_patch:
                    location = config["data_node"]["store_locations"][0]

                    assert "medium_name" not in location or location["medium_name"] == "default"
                    location["medium_name"] = "ssd"

                    config_path = self.Env.config_paths["node"][i]
                    with open(config_path, "w") as fout:
                        yson.dump(config, fout)
                    patched_node_config = True
                    break

            assert patched_node_config

        set("//sys/@config/chunk_manager/enable_chunk_sealer", True, recursive=True)

        wait_until_sealed("//tmp/j1")

        def replicator_has_done_well():
            try:
                stored_replicas = get("#{0}/@stored_replicas".format(chunk_id))
                if len(stored_replicas) != 3:
                    return False

                for replica in stored_replicas:
                    if replica.attributes["medium"] != "default":
                        return False

                return True
            except YtError:
                return False


        wait(replicator_has_done_well)

##################################################################

class TestErasureJournals(TestJournals):
    NUM_NODES = 20

    JOURNAL_ATTRIBUTES = {
        "none": {
            "erasure_codec": "none",
            "replication_factor": 3,
            "read_quorum": 2,
            "write_quorum": 2
        },
        "isa_lrc_12_2_2": {
            "erasure_codec": "isa_lrc_12_2_2",
            "replication_factor": 1,
            "read_quorum": 14,
            "write_quorum": 15
        },
        "isa_reed_solomon_3_3": {
            "erasure_codec": "isa_reed_solomon_3_3",
            "replication_factor": 1,
            "read_quorum": 4,
            "write_quorum": 5
        }
    }

    @pytest.mark.parametrize("erasure_codec", ["none", "isa_lrc_12_2_2", "isa_reed_solomon_3_3"])
    @authors("babenko", "ignat")
    def test_seal_abruptly_closed_journal(self, erasure_codec):
        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES[erasure_codec])
        N = 3
        for i in xrange(N):
            self._write_and_wait_until_sealed("//tmp/j", self.DATA, journal_writer={"ignore_closing": True})
            self._wait_until_last_chunk_sealed("//tmp/j")

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == len(self.DATA) * N
        assert get("//tmp/j/@chunk_count") == N
        assert read_journal("//tmp/j") == self.DATA * N

    @pytest.mark.parametrize("erasure_codec", ["isa_lrc_12_2_2", "isa_reed_solomon_3_3"])
    @authors("babenko")
    def test_repair_jobs(self, erasure_codec):
        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES[erasure_codec])
        write_journal("//tmp/j", self.DATA)

        chunk_ids = get("//tmp/j/@chunk_ids")
        chunk_id = chunk_ids[-1]
        replica_count = len(get("#{}/@stored_replicas".format(chunk_id)))

        def _check_all_replicas_ok():
            replicas = get("#{}/@stored_replicas".format(chunk_id))
            return len(replicas) == replica_count and all(r.attributes["state"] == "sealed" for r in replicas)

        for i in xrange(10):
            wait(_check_all_replicas_ok)

            replicas = get("#{}/@stored_replicas".format(chunk_id))
            random.shuffle(replicas)
            nodes_to_ban = [str(x) for x in replicas[:3]]
    
            for node in nodes_to_ban:
                set_node_banned(node, True)

            wait(_check_all_replicas_ok)

            assert read_journal("//tmp/j") == self.DATA

            for node in nodes_to_ban:
                set_node_banned(node, False)

    def _test_critical_erasure_state(self, state, n):
        set("//sys/@config/chunk_manager/enable_chunk_replicator", False)
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False)
        
        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES["isa_lrc_12_2_2"])
        write_journal("//tmp/j", self.DATA, journal_writer={"ignore_closing": True})

        chunk_ids = get("//tmp/j/@chunk_ids")
        chunk_id = chunk_ids[-1]
        check_path = "//sys/{}_chunks/{}".format(state, chunk_id)

        assert not exists(check_path)
        replicas = get("#{}/@stored_replicas".format(chunk_id))
        for r in replicas[:n]:
            set_node_banned(str(r), True)
        wait(lambda: exists(check_path))
        for r in replicas[:n]:
            set_node_banned(str(r), False)
        wait(lambda: not exists(check_path))

    @authors("babenko")
    def test_erasure_quorum_missing(self):
        self._test_critical_erasure_state("quorum_missing", 3)

    @authors("babenko")
    def test_erasure_lost(self):
        self._test_critical_erasure_state("lost_vital", 5)

    @pytest.mark.parametrize("erasure_codec", ["none", "isa_lrc_12_2_2", "isa_reed_solomon_3_3"])
    @authors("babenko", "ignat")
    def test_read_with_repair(self, erasure_codec):
        create("journal", "//tmp/j", attributes=self.JOURNAL_ATTRIBUTES[erasure_codec])
        self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@quorum_row_count") == len(self.DATA)
        assert get("//tmp/j/@chunk_count") == 1

        def check():
            for i in xrange(0, len(self.DATA)):
                assert read_journal("//tmp/j[#" + str(i) + ":#" + str(i + 1) + "]") == [self.DATA[i]]
            for i in xrange(0, len(self.DATA)):
                assert read_journal("//tmp/j[#" + str(i) + ":]") == self.DATA[i:]
            for i in xrange(0, len(self.DATA)):
                assert read_journal("//tmp/j[:#" + str(i) + "]") == self.DATA[:i]

        check()

        chunk_ids = get("//tmp/j/@chunk_ids")
        chunk_id = chunk_ids[-1]
        replicas = get("#{}/@stored_replicas".format(chunk_id))
        for replica in replicas:
            set_node_banned(replica, True)
            check()
            set_node_banned(replica, False)


class TestErasureJournalsRpcProxy(TestErasureJournals):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

##################################################################


