import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.yson import to_yson_type
from time import sleep

##################################################################

class TestChunkServer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20

    def test_owning_nodes1(self):
        create("table", "//tmp/t")
        write("//tmp/t", {"a" : "b"})
        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@owning_nodes") == ["//tmp/t"]

    def test_owning_nodes2(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        write("//tmp/t", {"a" : "b"}, tx=tx)
        chunk_ids = get("//tmp/t/@chunk_ids", tx=tx)
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@owning_nodes") == \
            [to_yson_type("//tmp/t", attributes = {"transaction_id" : tx})]

    def test_replication(self):
        create("table", "//tmp/t")
        write("//tmp/t", {"a" : "b"})

        assert get("//tmp/t/@replication_factor") == 3

        sleep(2) # wait for background replication

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        nodes = get("#%s/@stored_replicas" % chunk_id)
        assert len(nodes) == 3

    def _test_decommission(self, erasure_codec, replica_count):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", erasure_codec)
        write("//tmp/t", {"a" : "b"})

        sleep(2) # wait for background replication

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        nodes = get("#%s/@stored_replicas" % chunk_id)
        assert len(nodes) == replica_count

        node_to_decommission = nodes[0]
        assert get("//sys/nodes/%s/@stored_replica_count" % node_to_decommission) == 1

        set("//sys/nodes/%s/@decommissioned" % node_to_decommission, True)

        sleep(2) # wait for background replication

        assert get("//sys/nodes/%s/@stored_replica_count" % node_to_decommission) == 0
        assert len(get("#%s/@stored_replicas" % chunk_id)) == replica_count

    def test_decommission_regular(self):
        self._test_decommission("none", 3)

    def test_decommission_erasure(self):
        self._test_decommission("lrc_12_2_2", 16)
