import pytest

from yt_env_setup import YTEnvSetup
from yt.environment.helpers import assert_items_equal
from yt_commands import *

import time
import __builtin__

##################################################################

class TestDataCenters(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20

    JOURNAL_DATA = [{"data" : "payload" + str(i)} for i in xrange(0, 10)]
    FILE_DATA = "payload"


    def _get_replica_nodes(self, chunk_id):
        return list(str(x) for x in get("#" + chunk_id + "/@stored_replicas"))

    def _wait_for_safely_placed(self, chunk_id, replica_count):
        ok = False
        for i in xrange(60):
            if not get("#" + chunk_id + "/@replication_status/unsafely_placed") and len(self._get_replica_nodes(chunk_id)) == replica_count:
                ok = True
                break
            time.sleep(1.0)
        assert ok

    def _set_rack(self, node, rack):
        set("//sys/nodes/" + node + "/@rack", rack)

    def _set_data_center(self, rack, dc):
        set("//sys/racks/" + rack + "/@data_center", dc)

    def _unset_data_center(self, rack):
        remove("//sys/racks/" + rack + "/@data_center")

    def _reset_rack(self, node):
        remove("//sys/nodes/" + node + "/@rack")

    def _reset_data_center(self, rack):
        remove("//sys/racks/" + rack + "/@data_center")


    def _get_rack(self, node):
        return get("//sys/nodes/" + node + "/@rack")

    def _get_data_center(self, rack):
        return get("//sys/racks/" + rack + "/@data_center")


    def _has_rack(self, node):
        return "rack" in ls("//sys/nodes/" + node + "/@")

    def _has_data_center(self, rack):
        return "data_center" in ls("//sys/racks/" + rack + "/@")


    def _init_n_racks(self, n):
        node_to_rack_map = {}
        nodes = get_nodes()
        index = 0
        created_indexes = __builtin__.set()
        for node in nodes:
            rack = "r" + str(index)
            if not index in created_indexes:
                create_rack(rack)
                created_indexes.add(index)
            self._set_rack(node, rack)
            node_to_rack_map[node] = rack
            index = (index + 1) % n
        return node_to_rack_map

    def _init_n_data_centers(self, n):
        rack_to_dc_map = {}
        racks = get_racks()
        index = 0
        created_indexes = __builtin__.set()
        for rack in racks:
            dc = "d" + str(index)
            if not index in created_indexes:
                create_data_center(dc)
                created_indexes.add(index)
            self._set_data_center(rack, dc)
            rack_to_dc_map[rack] = dc
            index = (index + 1) % n
        return rack_to_dc_map


    def _reset_all_racks(self):
        nodes = get_nodes()
        for node in nodes:
            self._reset_rack(node)

    def _reset_all_data_centers(self):
        racks = get_racks()
        for rak in racks:
            self._reset_data_center(rack)

    def _set_rack_map(self, node_to_rack_map):
        racks = frozenset(node_to_rack_map.itervalues())
        for rack in racks:
            create_rack(rack)
        for node, rack in node_to_rack_map.iteritems():
            set("//sys/nodes/" + node + "/@rack", rack)

    def _set_data_center_map(self, rack_to_dc_map):
        dcs = frozenset(rack_to_dc_map.itervalues())
        for dc in dcs:
            create_data_center(dc)
        for rack, dc in rack_to_dc_map.iteritems():
            set("//sys/racks/" + rack + "/@data_center", dc)


    def _get_max_replicas_per_rack(self, node_to_rack_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        rack_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            rack_to_counter.setdefault(rack, 0)
            rack_to_counter[rack] += 1
        return max(rack_to_counter.itervalues())

    def _get_max_replicas_per_data_center(self, node_to_rack_map, rack_to_dc_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        dc_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            dc = rack_to_dc_map[rack]
            dc_to_counter.setdefault(dc, 0)
            dc_to_counter[rack] += 1
        return max(dc_to_counter.itervalues())


    def test_create(self):
        create_data_center("d")
        assert get_data_centers() == ["d"]
        assert get("//sys/data_centers/d/@name") == "d"

    def test_empty_name_fail(self):
        with pytest.raises(YtError): create_data_center("")

    def test_duplicate_name_fail(self):
        create_data_center("d")
        with pytest.raises(YtError): create_data_center("d")

    def test_rename_success(self):
        create_data_center("d1")
        create_rack("r1")
        create_rack("r2")

        nodes = get_nodes()
        n1 = nodes[0]
        n2 = nodes[1]
        self._set_rack(n1, "r1")
        self._set_rack(n2, "r2")

        self._set_data_center("r1", "d1")
        # r2 is kept out of d1 deliberately.

        set("//sys/data_centers/d1/@name", "d2")
        assert get("//sys/data_centers/d2/@name") == "d2"
        assert self._get_data_center("r1") == "d2"

    def test_rename_fail(self):
        create_data_center("d1")
        create_data_center("d2")
        with pytest.raises(YtError): set("//sys/data_centers/d1/@name", "d2")

    def test_assign_success1(self):
        create_data_center("d")
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        self._set_data_center("r" , "d")
        assert self._get_data_center("r") == "d"
        assert self._get_data_center(self._get_rack(n)) == "d"

    def test_assign_success2(self):
        self._init_n_racks(10)
        self._init_n_data_centers(1)
        nodes = get_nodes()
        for node in nodes:
            assert self._get_data_center(self._get_rack(node)) == "d0"
        assert_items_equal(get("//sys/data_centers/d0/@racks"), get_racks())

    def test_unassign(self):
        create_data_center("d")
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        self._set_data_center("r" , "d")
        self._unset_data_center("r")
        assert not self._has_data_center("r")

    def test_tags(self):
        n = get_nodes()[0]

        tags = get("//sys/nodes/{0}/@tags".format(n))
        assert "r" not in tags
        assert "d" not in tags

        create_data_center("d")
        create_rack("r")
        self._set_rack(n, "r")

        tags = get("//sys/nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" not in tags

        self._set_data_center("r" , "d")
        tags = get("//sys/nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" in tags

        self._unset_data_center("r")

        tags = get("//sys/nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" not in tags

    def test_remove(self):
        self._init_n_racks(10)
        self._init_n_data_centers(1)
        remove_data_center("d0")
        racks = get_racks()
        for rack in racks:
            assert not self._has_data_center(rack)

    def test_assign_fail(self):
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        with pytest.raises(YtError): self._set_data_center("r", "d")

    def test_write_regular(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

    def test_write_erasure(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        write_file("//tmp/file", self.FILE_DATA)

    def test_write_journal(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)

    def test_journals_with_degraded_data_centers(self):
        self._init_n_racks(10)
        self._init_n_data_centers(2)

        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)
        wait_until_sealed("//tmp/j")

        assert read_journal("//tmp/j") == self.JOURNAL_DATA

    def test_data_center_count_limit(self):
        for i in xrange(16):
            create_data_center("d" + str(i))
        with pytest.raises(YtError): create_data_center("too_many")

##################################################################

class TestDataCentersMulticell(TestDataCenters):
    NUM_SECONDARY_MASTER_CELLS = 2
