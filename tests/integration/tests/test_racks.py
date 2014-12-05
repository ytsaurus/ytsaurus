import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time
import __builtin__

##################################################################

class TestRacks(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20

    JOURNAL_DATA = [{"data" : "payload" + str(i)} for i in xrange(0, 10)]
    FILE_DATA = "payload"


    def _get_replica_nodes(self, chunk_id):
        return list(str(x) for x in get("#" + chunk_id + "/@stored_replicas"))

    def _wait_for_safely_placed(self, chunk_id, replica_count):
        ok = False
        for i in xrange(60):
            if not get("#" + chunk_id + "/@unsafely_placed") and len(self._get_replica_nodes(chunk_id)) == replica_count:
                ok = True
                break
            time.sleep(1.0)
        assert ok

    def _set_rack(self, node, rack):
        set("//sys/nodes/" + node + "/@rack", rack)

    def _reset_rack(self, node):
        remove("//sys/nodes/" + node + "/@rack")

    def _get_rack(self, node):
        return get("//sys/nodes/" + node + "/@rack")

    def _has_rack(self, node):
        return "rack" in ls("//sys/nodes/" + node + "/@")

    def _init_n_racks(self, n):
        nodes = get_nodes()
        index = 0
        created_indexes = __builtin__.set()
        for node in nodes:
            rack = "r" + str(index)
            if not index in created_indexes:
                create_rack(rack)
                created_indexes.add(index)
            self._set_rack(node, rack)
            index = (index + 1) % n

    def _reset_all_racks(self):
        nodes = get_nodes()
        for node in nodes:
            self._reset_rack(node)

    def _set_rack_map(self, map):
        racks = __builtin__.set(map[node] for node in map)
        for rack in racks:
            create_rack(rack)
        for node in map:
            set("//sys/nodes/" + node + "/@rack", map[node])

    def _get_max_replicas_per_rack(self, map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        rack_to_counter = {}
        for replica in replicas:
            rack = map[replica]
            if not rack in rack_to_counter:
                rack_to_counter[rack] = 0
            rack_to_counter[rack] += 1
        return max(rack_to_counter[rack] for rack in rack_to_counter)


    def test_create(self):
        create_rack("r")
        assert get_racks() == ["r"]
        assert get("//sys/racks/r/@name") == "r"

    def test_empty_name_fail(self):
        with pytest.raises(YtError): create_rack("")

    def test_duplicate_name_fail(self):
        create_rack("r")
        with pytest.raises(YtError): create_rack("r")

    def test_rename_success(self):
        create_rack("r1")
        n = get_nodes()[0]
        self._set_rack(n, "r1")
        
        set("//sys/racks/r1/@name", "r2")
        assert get("//sys/racks/r2/@name") == "r2"
        assert self._get_rack(n) == "r2"

    def test_rename_fail(self):
        create_rack("r1")
        create_rack("r2")
        with pytest.raises(YtError): set("//sys/racks/r1/@name", "r2")

    def test_assign_success1(self):
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        assert self._get_rack(n) == "r"

    def test_assign_success2(self):
        create_rack("r")
        nodes = get_nodes()
        for node in nodes:
            self._set_rack(node, "r")
        for node in nodes:
            assert self._get_rack(node) == "r"
        self.assertItemsEqual(get("//sys/racks/r/@nodes"), nodes)
        
    def test_remove(self):
        create_rack("r")
        nodes = get_nodes()
        for node in nodes:
            self._set_rack(node, "r")
        remove_rack("r")
        for node in nodes:
            assert not self._has_rack(node) 

    def test_assign_fail(self):
        n = get_nodes()[0]
        with pytest.raises(YtError): self._set_rack(n, "r")


    def test_regular_not_enough_racks(self):
        self._init_n_racks(1)
        create("file", "//tmp/file")
        with pytest.raises(YtError): upload("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})
        
    def test_regular_enough_racks(self):
        self._init_n_racks(2)
        create("file", "//tmp/file")
        upload("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

        
    def test_erasure_not_enough_racks(self):
        self._init_n_racks(5)
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        with pytest.raises(YtError): upload("//tmp/file", self.FILE_DATA)
        
    def test_erasure_enough_racks(self):
        self._init_n_racks(6)
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        upload("//tmp/file", self.FILE_DATA)


    def test_journal_not_enough_racks(self):
        self._init_n_racks(2)
        create("journal", "//tmp/j")
        with pytest.raises(YtError): write_journal("//tmp/j", self.JOURNAL_DATA)

    def test_journal_enough_racks(self):
        self._init_n_racks(3)
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)

        
    def test_unsafely_placed(self):
        create("file", "//tmp/file", file_writer={"upload_replication_factor": 3})
        upload("//tmp/file", self.FILE_DATA)
        
        chunk_ids = get("//tmp/file/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert not get("#" + chunk_id + "/@unsafely_placed")

        self._init_n_racks(1)

        time.sleep(1.0)
        assert get("#" + chunk_id + "/@unsafely_placed")

        self._reset_all_racks()

        time.sleep(1.0)
        assert not get("#" + chunk_id + "/@unsafely_placed")


    def test_regular_move_to_safe_place(self):
        create("file", "//tmp/file")
        upload("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})
        
        chunk_ids = get("//tmp/file/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert not get("#" + chunk_id + "/@unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 3

        map = {}
        nodes = get_nodes()
        for node in nodes:
            if node in replicas:
                map[node] = "r1"
            else:
                map[node] = "r2"
        self._set_rack_map(map)

        self._wait_for_safely_placed(chunk_id, 3)
        
        assert self._get_max_replicas_per_rack(map, chunk_id) <= 2

    def test_erasure_move_to_safe_place(self):
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        upload("//tmp/file", self.FILE_DATA)
        
        chunk_ids = get("//tmp/file/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert not get("#" + chunk_id + "/@unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 16

        # put unlisted nodes to the end
        nodes = get_nodes()
        replicas_plus_nodes = replicas
        for node in nodes:
            if not node in replicas:
                replicas_plus_nodes.append(node)

        assert len(replicas_plus_nodes) == 20
        map = {
            replicas_plus_nodes[ 0] : "r1",
            replicas_plus_nodes[ 1] : "r1",
            replicas_plus_nodes[ 2] : "r1",
            replicas_plus_nodes[ 3] : "r1",
            replicas_plus_nodes[ 4] : "r1",
            replicas_plus_nodes[ 5] : "r2",
            replicas_plus_nodes[ 6] : "r2",
            replicas_plus_nodes[ 7] : "r2",
            replicas_plus_nodes[ 8] : "r3",
            replicas_plus_nodes[ 9] : "r3",
            replicas_plus_nodes[10] : "r3",
            replicas_plus_nodes[11] : "r4",
            replicas_plus_nodes[12] : "r4",
            replicas_plus_nodes[13] : "r4",
            replicas_plus_nodes[14] : "r5",
            replicas_plus_nodes[15] : "r5",
            replicas_plus_nodes[16] : "r5",
            replicas_plus_nodes[17] : "r6",
            replicas_plus_nodes[18] : "r6",
            replicas_plus_nodes[19] : "r6",
        }        
        self._set_rack_map(map)

        self._wait_for_safely_placed(chunk_id, 16)
        
        assert self._get_max_replicas_per_rack(map, chunk_id) <= 3

    def test_journal_move_to_safe_place(self):
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)
        assert get("//tmp/j/@sealed")
        
        chunk_ids = get("//tmp/j/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        assert not get("#" + chunk_id + "/@unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 3

        map = {}
        nodes = get_nodes()
        for i in xrange(len(nodes)):
            map[nodes[i]] = "r" + str(i % 3)
        for node in replicas:
            map[node] = "r0"
        self._set_rack_map(map)

        self._wait_for_safely_placed(chunk_id, 3)
        
        assert self._get_max_replicas_per_rack(map, chunk_id) <= 1


