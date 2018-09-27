from yt_env_setup import YTEnvSetup
from yt_commands import *

from yt.yson import to_yson_type
from yt.environment.helpers import assert_items_equal, wait

import json
from time import sleep

##################################################################

class TestChunkServer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20
    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "safe_online_node_count": 3
        },
        "node_tracker": {
            "full_node_states_gossip_period": 1000
        }
    }

    def test_owning_nodes1(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@owning_nodes") == ["//tmp/t"]

    def test_owning_nodes2(self):
        create("table", "//tmp/t")
        tx = start_transaction()
        write_table("//tmp/t", {"a" : "b"}, tx=tx)
        chunk_ids = get("//tmp/t/@chunk_ids", tx=tx)
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert get("#" + chunk_id + "/@owning_nodes") == \
            [to_yson_type("//tmp/t", attributes = {"transaction_id" : tx})]

    def test_replication(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        assert get("//tmp/t/@replication_factor") == 3

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        wait(lambda: len(get("#%s/@stored_replicas" % chunk_id)) == 3)

    def _test_decommission(self, path, replica_count, node_to_decommission_count=1):
        assert replica_count >= node_to_decommission_count

        chunk_ids = get(path + "/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        nodes_to_decommission = self._decommission_chunk_replicas(chunk_id, replica_count, node_to_decommission_count)

        wait(lambda: not self._nodes_have_chunk(nodes_to_decommission, chunk_id) and
                     len(get("#%s/@stored_replicas" % chunk_id)) == replica_count)

    def _decommission_chunk_replicas(self, chunk_id, replica_count, node_to_decommission_count):
        wait(lambda: len(get("#%s/@stored_replicas" % chunk_id)) == replica_count)

        nodes_to_decommission = get("#%s/@stored_replicas" % chunk_id)
        assert len(nodes_to_decommission) == replica_count
        nodes_to_decommission = nodes_to_decommission[:node_to_decommission_count]
        assert self._nodes_have_chunk(nodes_to_decommission, chunk_id)

        for node in nodes_to_decommission:
            set_node_decommissioned(node, True)

        return nodes_to_decommission

    def _nodes_have_chunk(self, nodes, id):
        def id_to_hash(id):
            return id.split('-')[3]

        for node in nodes:
            if not (id_to_hash(id) in [id_to_hash(id_) for id_ in ls("//sys/nodes/%s/orchid/stored_chunks" % node)]):
                return False
        return True

    def test_decommission_regular(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        self._test_decommission("//tmp/t", 3)

    def test_decommission_regular2(self):
        create("table", "//tmp/t", attributes={"replication_factor": 4})
        write_table("//tmp/t", {"a" : "b"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        self._decommission_chunk_replicas(chunk_id, 4, 2)
        set("//tmp/t/@replication_factor", 3)
        # Now 2 replicas are decommissioned and 2 aren't.
        # The chunk should be both under- and overreplicated.

        wait(lambda: len(get("#%s/@stored_replicas" % chunk_id)) == 3)
        nodes = get("#%s/@stored_replicas" % chunk_id)
        for node in nodes:
            assert not get("//sys/nodes/%s/@decommissioned" % node)

    def test_decommission_erasure(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t", {"a" : "b"})
        self._test_decommission("//tmp/t", 16)

    def test_decommission_erasure2(self):
        create("table", "//tmp/t")
        set("//tmp/t/@erasure_codec", "lrc_12_2_2")
        write_table("//tmp/t", {"a" : "b"})
        self._test_decommission("//tmp/t", 16, 4)

    def test_decommission_journal(self):
        create("journal", "//tmp/j")
        write_journal("//tmp/j", [{"data" : "payload" + str(i)} for i in xrange(0, 10)])
        self._test_decommission("//tmp/j", 3)

    def test_list_chunk_owners(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"a": "b"}])
        ls("//sys/chunks", attributes=["owning_nodes"])

    def _check_replicator(self, expected):
        assert get("//sys/@chunk_replicator_enabled") == expected

    def test_disable_replicator_when_few_nodes_are_online(self):
        nodes = ls("//sys/nodes")
        assert len(nodes) == 20

        assert get("//sys/@chunk_replicator_enabled")

        for i in xrange(18):
            set("//sys/nodes/%s/@banned" % nodes[i], True)

        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

    def test_disable_replicator_when_explicitly_requested_so(self):
        assert get("//sys/@chunk_replicator_enabled")

        set("//sys/@config/chunk_manager/enable_chunk_replicator", False, recursive=True)

        wait(lambda: not get("//sys/@chunk_replicator_enabled"))

    def test_hide_chunk_attrs(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})
        chunks = ls("//sys/chunks")
        for c in chunks:
            assert len(c.attributes) == 0

        chunks_json = execute_command("list", {"path": "//sys/chunks", "output_format": "json"})
        for c in json.loads(chunks_json):
            assert isinstance(c, basestring)

    def test_chunk_requisition_registry_orchid(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        master = ls("//sys/primary_masters")[0]
        master_orchid_path = "//sys/primary_masters/{0}/orchid/chunk_manager/requisition_registry".format(master)

        known_requisition_indexes = frozenset(ls(master_orchid_path))
        set("//tmp/t/@replication_factor", 4)
        sleep(0.3)
        new_requisition_indexes = frozenset(ls(master_orchid_path)) - known_requisition_indexes
        assert len(new_requisition_indexes) == 1
        new_requisition_index = next(iter(new_requisition_indexes))

        new_requisition = get("{0}/{1}".format(master_orchid_path, new_requisition_index))
        assert new_requisition["ref_counter"] == 2 # one for 'local', one for 'aggregated' requisition
        assert new_requisition["vital"]
        assert len(new_requisition["entries"]) == 1
        assert new_requisition["entries"][0]["replication_policy"]["replication_factor"] == 4

    def test_node_chunk_replica_count(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        wait(lambda: len(get("#{0}/@stored_replicas".format(chunk_id))) == 3)

        def check_replica_count():
            nodes = get("#{0}/@stored_replicas".format(chunk_id))
            for node in nodes:
                if not (get("//sys/nodes/{0}/@chunk_replica_count/cache".format(node)) == 0 and
                        get("//sys/nodes/{0}/@chunk_replica_count/default".format(node)) == 1):
                    return False
            return True

        wait(check_replica_count, sleep_backoff=1.0)

##################################################################

class TestChunkServerMulticell(TestChunkServer):
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_SCHEDULERS = 1

    def test_owning_nodes3(self):
        create("table", "//tmp/t0", attributes={"external": False})
        create("table", "//tmp/t1", attributes={"external_cell_tag": 1})
        create("table", "//tmp/t2", attributes={"external_cell_tag": 2})

        write_table("//tmp/t1", {"a" : "b"})

        merge(mode="ordered", in_="//tmp/t1", out="//tmp/t0")
        merge(mode="ordered", in_="//tmp/t1", out="//tmp/t2")

        chunk_ids0 = get("//tmp/t0/@chunk_ids")
        chunk_ids1 = get("//tmp/t1/@chunk_ids")
        chunk_ids2 = get("//tmp/t2/@chunk_ids")

        assert chunk_ids0 == chunk_ids1
        assert chunk_ids1 == chunk_ids2
        chunk_ids = chunk_ids0
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert_items_equal(
            get("#" + chunk_id + "/@owning_nodes"),
            ["//tmp/t0", "//tmp/t1", "//tmp/t2"])

    def test_chunk_requisition_registry_orchid(self):
        pass

##################################################################

class TestMultipleErasurePartsPerNode(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    def test_allow_multiple_erasure_parts_per_node(self):
        create("table", "//tmp/t", attributes={"erasure_codec": "lrc_12_2_2"})
        rows = [{"a": "b"}]
        write_table("//tmp/t", rows)
        assert read_table("//tmp/t") == rows

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        status = get("#" + chunk_id + "/@replication_status/default")
        assert not status["data_missing"]
        assert not status["parity_missing"]
        assert not status["overreplicated"]
        assert not status["underreplicated"]
