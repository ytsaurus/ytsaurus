import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep


##################################################################

class TestJournals(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5

    DATA = [{"data" : "payload" + str(i)} for i in xrange(0, 10)]

    REPLICATOR_REACTION_TIME = 3.5

    def _replicator_sleep(self):
        sleep(self.REPLICATOR_REACTION_TIME)

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
        multicell_sleep()
        assert get_account_committed_disk_space("tmp") == 0

        create("journal", "//tmp/j")
        self._write_and_wait_until_sealed("//tmp/j", self.DATA)

        chunk_ids = get("//tmp/j/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        # wait for chunk to become sealed
        while True:
            if get("#" + chunk_id + "/@sealed"):
                break
            sleep(1)

        get("#" + chunk_id + "/@owning_nodes")
        disk_space_delta = get_chunk_owner_disk_space("//tmp/j")
        assert disk_space_delta > 0

        get("//sys/accounts/tmp/@")

        self._replicator_sleep()
        assert get_account_committed_disk_space("tmp") == disk_space_delta
        assert get_account_disk_space("tmp") == disk_space_delta

        remove("//tmp/j")

        gc_collect() # wait for account stats to be updated
        self._replicator_sleep()
        assert get_account_committed_disk_space("tmp") == 0
        assert get_account_disk_space("tmp") == 0

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

##################################################################

class TestJournalsMulticell(TestJournals):
    NUM_SECONDARY_MASTER_CELLS = 2
