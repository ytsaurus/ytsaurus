from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, ls, get, set, exists, remove, create_data_center, create_rack,
    remove_data_center, write_file, wait, sync_control_chunk_replicator,
    read_journal, write_journal, write_table, wait_until_sealed,
    get_nodes, get_racks, get_data_centers, get_singular_chunk_id)

from yt.environment.helpers import assert_items_equal

from yt.common import YtError

import pytest

import builtins

##################################################################


class TestDataCenters(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 20

    JOURNAL_DATA = [{"payload": "payload" + str(i)} for i in range(0, 10)]
    FILE_DATA = b"payload"

    def _get_replica_nodes(self, chunk_id):
        return list(str(x) for x in get("#" + chunk_id + "/@stored_replicas"))

    def _get_replica_racks(self, chunk_id):
        return [get("//sys/cluster_nodes/{}/@rack".format(n)) for n in self._get_replica_nodes(chunk_id)]

    def _get_replica_data_centers(self, chunk_id):
        return [get("//sys/cluster_nodes/{}/@data_center".format(n)) for n in self._get_replica_nodes(chunk_id)]

    def _wait_for_safely_placed(self, chunk_id):
        def check():
            stat = get("#{0}/@replication_status/default".format(chunk_id))
            return not stat["unsafely_placed"] and not stat["overreplicated"]

        wait(lambda: check())

    def _set_rack(self, node, rack):
        set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _set_data_center(self, rack, dc):
        set("//sys/racks/" + rack + "/@data_center", dc)

    def _unset_data_center(self, rack):
        remove("//sys/racks/" + rack + "/@data_center")

    def _reset_rack(self, node):
        remove("//sys/cluster_nodes/" + node + "/@rack")

    def _reset_data_center(self, rack):
        remove("//sys/racks/" + rack + "/@data_center")

    def _get_rack(self, node):
        return get("//sys/cluster_nodes/" + node + "/@rack")

    def _get_data_center(self, rack):
        return get("//sys/racks/" + rack + "/@data_center")

    def _has_rack(self, node):
        return "rack" in ls("//sys/cluster_nodes/" + node + "/@")

    def _has_data_center(self, rack):
        return "data_center" in ls("//sys/racks/" + rack + "/@")

    def _init_n_racks(self, n):
        node_to_rack_map = {}
        nodes = get_nodes()
        index = 0
        created_indexes = builtins.set()
        for node in nodes:
            rack = "r" + str(index)
            if index not in created_indexes:
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
        created_indexes = builtins.set()
        for rack in racks:
            dc = "d" + str(index)
            if index not in created_indexes:
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
        for rack in racks:
            self._reset_data_center(rack)

    def _set_rack_map(self, node_to_rack_map):
        racks = frozenset(node_to_rack_map.values())
        for rack in racks:
            create_rack(rack)
        for node, rack in node_to_rack_map.items():
            set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _set_data_center_map(self, rack_to_dc_map):
        dcs = frozenset(rack_to_dc_map.values())
        for dc in dcs:
            create_data_center(dc)
        for rack, dc in rack_to_dc_map.items():
            set("//sys/racks/" + rack + "/@data_center", dc)

    def _get_max_replicas_per_rack(self, node_to_rack_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        rack_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            rack_to_counter.setdefault(rack, 0)
            rack_to_counter[rack] += 1
        return max(rack_to_counter.values())

    def _get_max_replicas_per_data_center(self, node_to_rack_map, rack_to_dc_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        dc_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            dc = rack_to_dc_map[rack]
            dc_to_counter.setdefault(dc, 0)
            dc_to_counter[rack] += 1
        return max(dc_to_counter.values())

    def _create_chunk(self, replication_factor=None, erasure_codec=None):
        if exists("//tmp/t"):
            remove("//tmp/t")
        if replication_factor:
            create("table", "//tmp/t", attributes={"replication_factor": replication_factor})
        else:
            create("table", "//tmp/t", attributes={"erasure_codec": erasure_codec})
        write_table("//tmp/t", [{"x": 42}])
        chunk_id = get_singular_chunk_id("//tmp/t")
        if replication_factor:
            wait(lambda: len(self._get_replica_nodes(chunk_id)) == replication_factor)
        return chunk_id

    def _get_rack_counters(self, chunk_id):
        counters = {}
        for rack in self._get_replica_racks(chunk_id):
            if rack in counters:
                counters[rack] += 1
            else:
                counters[rack] = 1
        return list(counters.values())

    def _get_data_center_counters(self, chunk_id):
        counters = {}
        for data_center in self._get_replica_data_centers(chunk_id):
            if data_center in counters:
                counters[data_center] += 1
            else:
                counters[data_center] = 1
        return list(counters.values())

    def _init_data_center_aware_replicator(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        set("//sys/@config/chunk_manager/use_data_center_aware_replicator", True)
        set("//sys/@config/chunk_manager/storage_data_centers", ["d0", "d1", "d2"])

    def _ban_data_centers(self, data_centers):
        set("//sys/@config/chunk_manager/banned_storage_data_centers", data_centers)

    @authors("shakurov")
    def test_create(self):
        create_data_center("d")
        assert get_data_centers() == ["d"]
        assert get("//sys/data_centers/d/@name") == "d"

    @authors("shakurov")
    def test_empty_name_fail(self):
        with pytest.raises(YtError):
            create_data_center("")

    @authors("shakurov")
    def test_duplicate_name_fail(self):
        create_data_center("d")
        with pytest.raises(YtError):
            create_data_center("d")

    @authors("shakurov")
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

    @authors("shakurov")
    def test_rename_fail(self):
        create_data_center("d1")
        create_data_center("d2")
        with pytest.raises(YtError):
            set("//sys/data_centers/d1/@name", "d2")

    @authors("shakurov")
    def test_assign_success1(self):
        create_data_center("d")
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        self._set_data_center("r", "d")
        assert self._get_data_center("r") == "d"
        assert self._get_data_center(self._get_rack(n)) == "d"

    @authors("shakurov")
    def test_assign_success2(self):
        self._init_n_racks(10)
        self._init_n_data_centers(1)
        nodes = get_nodes()
        for node in nodes:
            assert self._get_data_center(self._get_rack(node)) == "d0"
        assert_items_equal(get("//sys/data_centers/d0/@racks"), get_racks())

    @authors("shakurov")
    def test_unassign(self):
        create_data_center("d")
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        self._set_data_center("r", "d")
        self._unset_data_center("r")
        assert not self._has_data_center("r")

    @authors("shakurov")
    def test_tags(self):
        n = get_nodes()[0]

        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" not in tags
        assert "d" not in tags

        create_data_center("d")
        create_rack("r")
        self._set_rack(n, "r")

        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" not in tags

        self._set_data_center("r", "d")
        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" in tags

        self._unset_data_center("r")

        tags = get("//sys/cluster_nodes/{0}/@tags".format(n))
        assert "r" in tags
        assert "d" not in tags

    @authors("shakurov")
    def test_remove(self):
        self._init_n_racks(10)
        self._init_n_data_centers(1)
        remove_data_center("d0")
        racks = get_racks()
        for rack in racks:
            assert not self._has_data_center(rack)

    @authors("shakurov")
    def test_assign_fail(self):
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        with pytest.raises(YtError):
            self._set_data_center("r", "d")

    @authors("shakurov")
    def test_write_regular(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

    @authors("shakurov")
    def test_write_erasure(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        write_file("//tmp/file", self.FILE_DATA)

    @authors("shakurov")
    def test_write_journal(self):
        self._init_n_racks(10)
        self._init_n_data_centers(3)
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)

    @authors("shakurov")
    def test_journals_with_degraded_data_centers(self):
        self._init_n_racks(10)
        self._init_n_data_centers(2)

        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)
        wait_until_sealed("//tmp/j")

        assert read_journal("//tmp/j") == self.JOURNAL_DATA

    @authors("shakurov")
    def test_data_center_count_limit(self):
        for i in range(16):
            create_data_center("d" + str(i))
        with pytest.raises(YtError):
            create_data_center("too_many")

    @authors("gritukan")
    def test_replica_uniformity(self):
        self._init_data_center_aware_replicator()

        chunk_id = self._create_chunk(replication_factor=1)
        assert sorted(self._get_data_center_counters(chunk_id)) == [1]

        chunk_id = self._create_chunk(replication_factor=3)
        assert sorted(self._get_data_center_counters(chunk_id)) == [1, 1, 1]

        chunk_id = self._create_chunk(replication_factor=5)
        assert sorted(self._get_data_center_counters(chunk_id)) == [1, 2, 2]

        chunk_id = self._create_chunk(erasure_codec="isa_reed_solomon_3_3")
        assert sorted(self._get_data_center_counters(chunk_id)) == [2, 2, 2]

        chunk_id = self._create_chunk(erasure_codec="isa_reed_solomon_6_3")
        assert sorted(self._get_data_center_counters(chunk_id)) == [3, 3, 3]

    @authors("gritukan")
    @pytest.mark.parametrize("erasure", [False, True])
    def test_ban_data_center(self, erasure):
        self._init_data_center_aware_replicator()

        def check_no_ban(chunk_id):
            expected_counters = [2, 2, 2] if erasure else [1, 1, 1]
            return sorted(self._get_data_center_counters(chunk_id)) == expected_counters

        def check_ban(chunk_id, data_center):
            expected_counters = [3, 3] if erasure else [1, 2]
            if sorted(self._get_data_center_counters(chunk_id)) != expected_counters:
                return False
            return data_center not in self._get_replica_data_centers(chunk_id)

        if erasure:
            chunk_id = self._create_chunk(erasure_codec="isa_reed_solomon_3_3")
        else:
            chunk_id = self._create_chunk(replication_factor=3)
        assert check_no_ban(chunk_id)

        self._ban_data_centers(["d0"])
        wait(lambda: check_ban(chunk_id, "d0"))

        self._ban_data_centers([])
        wait(lambda: check_no_ban(chunk_id))

        self._ban_data_centers(["d2"])
        wait(lambda: check_ban(chunk_id, "d2"))

        if erasure:
            chunk_id = self._create_chunk(erasure_codec="isa_reed_solomon_3_3")
        else:
            chunk_id = self._create_chunk(replication_factor=3)
        assert check_ban(chunk_id, "d2")

    @authors("gritukan")
    def test_ban_too_many_data_centers(self):
        self._init_data_center_aware_replicator()

        self._ban_data_centers(["d0", "d1"])
        chunk_id = self._create_chunk(erasure_codec="isa_reed_solomon_3_3")
        assert self._get_replica_data_centers(chunk_id) == ["d2"] * 6

        assert not get("#{}/@replication_status/default/unsafely_placed".format(chunk_id))

    @authors("gritukan")
    def test_ban_all_data_centers(self):
        self._init_data_center_aware_replicator()

        self._ban_data_centers(["d0", "d1", "d2"])
        with pytest.raises(YtError):
            self._create_chunk(erasure_codec="isa_reed_solomon_3_3")

    @authors("gritukan")
    def test_invalid_data_center_sets(self):
        self._init_data_center_aware_replicator()

        def has_alert(msg):
            for alert in get("//sys/@master_alerts"):
                if msg in str(alert):
                    return True
            return False

        set("//sys/@config/chunk_manager/storage_data_centers", ["d"])
        wait(lambda: has_alert("Storage data center \"d\" is unknown"))

        set("//sys/@config/chunk_manager/storage_data_centers", ["d0", "d1", "d2"])
        wait(lambda: not has_alert("Storage data center \"d\" is unknown"))

        set("//sys/@config/chunk_manager/banned_storage_data_centers", ["d"])
        wait(lambda: has_alert("Banned data center \"d\" is unknown"))

        set("//sys/@config/chunk_manager/banned_storage_data_centers", [])
        wait(lambda: not has_alert("Banned data center \"d\" is unknown"))

        set("//sys/@config/chunk_manager/storage_data_centers", ["d1", "d2"])
        set("//sys/@config/chunk_manager/banned_storage_data_centers", ["d0"])
        wait(lambda: has_alert("Banned data center \"d0\" is not a storage data center"))

    @authors("gritukan")
    @pytest.mark.parametrize("erasure", [False, True])
    def test_replicate_unsafely_placed_chunk(self, erasure):
        self._init_data_center_aware_replicator()

        if erasure:
            chunk_id = self._create_chunk(erasure_codec="isa_reed_solomon_3_3")
        else:
            chunk_id = self._create_chunk(replication_factor=6)
        replicas = builtins.set(self._get_replica_nodes(chunk_id))

        sync_control_chunk_replicator(False)

        set("//sys/media/default/@config/max_replicas_per_rack", 1)

        node_to_dc = {}
        for node in ls("//sys/cluster_nodes"):
            node_to_dc[node] = get("//sys/cluster_nodes/{}/@data_center".format(node))

        solid_racks = {"d0": "r0", "d1": "r1", "d2": "r2"}
        hollow_racks = {"d0": "r3", "d1": "r4", "d2": "r5"}

        for dc, rack in solid_racks.items():
            set("//sys/racks/{}/@data_center".format(rack), dc)
        for dc, rack in hollow_racks.items():
            set("//sys/racks/{}/@data_center".format(rack), dc)
        for node in ls("//sys/cluster_nodes"):
            dc = node_to_dc[node]
            if node in replicas:
                rack = solid_racks[dc]
            else:
                rack = hollow_racks[dc]
            set("//sys/cluster_nodes/{}/@rack".format(node), rack)

        wait(lambda: get("#{}/@replication_status/default/unsafely_placed".format(chunk_id)))

        sync_control_chunk_replicator(True)

        wait(lambda: not get("#{}/@replication_status/default/unsafely_placed".format(chunk_id)))


##################################################################


class TestDataCentersMulticell(TestDataCenters):
    NUM_SECONDARY_MASTER_CELLS = 2
