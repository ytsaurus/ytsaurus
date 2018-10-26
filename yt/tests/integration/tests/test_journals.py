import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
import yt.yson
from io import TextIOBase, UnsupportedOperation

##################################################################

class TestJournals(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

    DATA = [{"data" : "payload" + str(i)} for i in xrange(0, 10)]

    def _write_and_wait_until_sealed(self, path, data):
        write_journal(path, data)
        wait_until_sealed(path)

    def test_create_success(self):
        create("journal", "//tmp/j")
        assert get("//tmp/j/@replication_factor") == 3
        assert get("//tmp/j/@read_quorum") == 2
        assert get("//tmp/j/@write_quorum") == 2
        assert get_chunk_owner_disk_space("//tmp/j") == 0
        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@row_count") == 0
        assert get("//tmp/j/@chunk_ids") == []

    def test_create_failure(self):
        with pytest.raises(YtError): create("journal", "//tmp/j", attributes={"replication_factor": 1})
        with pytest.raises(YtError): create("journal", "//tmp/j", attributes={"read_quorum": 4})
        with pytest.raises(YtError): create("journal", "//tmp/j", attributes={"write_quorum": 4})
        with pytest.raises(YtError): create("journal", "//tmp/j", attributes={"replication_factor": 4})

    def test_readwrite1(self):
        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@row_count") == 10
        assert get("//tmp/j/@chunk_count") == 1

        for i in xrange(0, len(self.DATA)):
            assert read_journal("//tmp/j[#" + str(i) + ":#" + str(i + 1) + "]") == [{"data" : "payload" + str(i)}]

    def test_readwrite2(self):
        create("journal", "//tmp/j")
        for i in xrange(0, 10):
            self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        assert get("//tmp/j/@sealed")
        assert get("//tmp/j/@row_count") == 100
        assert get("//tmp/j/@chunk_count") == 10

        for i in xrange(0, 10):
            assert read_journal("//tmp/j[#" + str(i * 10) + ":]") == self.DATA * (10 - i)

        for i in xrange(0, 9):
            assert read_journal("//tmp/j[#" + str(i * 10 + 5) + ":]") == (self.DATA * (10 - i))[5:]

        assert read_journal("//tmp/j[#200:]") == []

    def test_resource_usage(self):
        wait(lambda: get_account_committed_disk_space("tmp") == 0)

        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        chunk_ids = get("//tmp/j/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

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

    def test_no_copy(self):
        create("journal", "//tmp/j1")
        with pytest.raises(YtError): copy("//tmp/j1", "//tmp/j2")

    def test_move(self):
        create("journal", "//tmp/j1")
        self._write_and_wait_until_sealed("//tmp/j1", self.DATA)

        move('//tmp/j1', '//tmp/j2')
        assert read_journal("//tmp/j2") == self.DATA

    def test_no_storage_change_after_creation(self):
        create("journal", "//tmp/j", attributes={"replication_factor": 5, "read_quorum": 3, "write_quorum": 3})
        with pytest.raises(YtError): set("//tmp/j/@replication_factor", 6)
        with pytest.raises(YtError): set("//tmp/j/@vital", False)
        with pytest.raises(YtError): set("//tmp/j/@primary_medium", "default")

    def test_journal_replica_changes_medium_yt_8669(self):
        set("//sys/@config/chunk_manager/enable_chunk_sealer", False, recursive=True)

        class JournalDataStream(TextIOBase):
            def __init__(self, owner):
                self._i = 0
                self._owner = owner

            def read(self, size):
                if size < 0:
                    raise ValueError()

                self._i += 1

                if self._i == 50:
                    self._owner.Env.kill_nodes()
                    raise "" # Throw to abort the journal writer without sealing the chunk.

                # NB: size is hackishly ignored for simplicity.
                return '{{"data"="payload{0}";}};'.format(self._i)

        create("journal", "//tmp/j1")
        with pytest.raises(YtError):
            write_journal("//tmp/j1", None, raw=False, input_stream=JournalDataStream(self))

        assert not get("//tmp/j1/@sealed")

        chunk_ids = get("//tmp/j1/@chunk_ids")
        unsealed_chunk_id = None
        for chunk_id in chunk_ids:
            if not get("#{0}/@sealed".format(chunk_id)):
                unsealed_chunk_id = chunk_id
                break
        assert unsealed_chunk_id is not None

        node_to_patch = str(get("#{0}/@stored_replicas".format(unsealed_chunk_id))[0])

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

        self.Env.start_nodes()

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

class TestJournalsMulticell(TestJournals):
    NUM_SECONDARY_MASTER_CELLS = 2
