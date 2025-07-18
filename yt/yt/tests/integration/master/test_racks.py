from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, create, ls, get, set, remove, create_rack,
    remove_rack, write_file,
    read_journal, write_journal, wait_until_sealed,
    get_singular_chunk_id, set_node_decommissioned, get_nodes, get_racks)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import builtins

##################################################################


@pytest.mark.enabled_multidaemon
class TestRacks(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 20

    JOURNAL_DATA = [{"payload": "payload" + str(i)} for i in range(0, 10)]
    FILE_DATA = b"payload"

    def _get_replica_nodes(self, chunk_id):
        return list(str(x) for x in get("#{0}/@stored_replicas".format(chunk_id)))

    def _wait_for_safely_placed(self, chunk_id):
        def check():
            stat = get("#{0}/@replication_status/default".format(chunk_id))
            return not stat["unsafely_placed"] and not stat["overreplicated"]

        wait(lambda: check())

    def _set_rack(self, node, rack):
        set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _reset_rack(self, node):
        remove("//sys/cluster_nodes/" + node + "/@rack")

    def _get_rack(self, node):
        return get("//sys/cluster_nodes/" + node + "/@rack")

    def _has_rack(self, node):
        return "rack" in ls("//sys/cluster_nodes/" + node + "/@")

    def _init_n_racks(self, n):
        nodes = get_nodes()
        index = 0
        created_indexes = builtins.set()
        for node in nodes:
            rack = "r" + str(index)
            if index not in created_indexes:
                create_rack(rack)
                created_indexes.add(index)
            self._set_rack(node, rack)
            index = (index + 1) % n

    def _reset_all_racks(self):
        nodes = get_nodes()
        for node in nodes:
            self._reset_rack(node)

    def _set_rack_map(self, node_to_rack_map):
        racks = frozenset(node_to_rack_map.values())
        for rack in racks:
            create_rack(rack)
        for node, rack in node_to_rack_map.items():
            set("//sys/cluster_nodes/" + node + "/@rack", rack)

    def _get_max_replicas_per_rack(self, node_to_rack_map, chunk_id):
        replicas = self._get_replica_nodes(chunk_id)
        rack_to_counter = {}
        for replica in replicas:
            rack = node_to_rack_map[replica]
            rack_to_counter.setdefault(rack, 0)
            rack_to_counter[rack] += 1
        return max(rack_to_counter.values())

    @authors("babenko", "ignat")
    def test_create(self):
        create_rack("r")
        assert get_racks() == ["r"]
        assert get("//sys/racks/r/@name") == "r"

    @authors("babenko", "ignat")
    def test_empty_name_fail(self):
        with pytest.raises(YtError):
            create_rack("")

    @authors("babenko", "ignat")
    def test_duplicate_name_fail(self):
        create_rack("r")
        with pytest.raises(YtError):
            create_rack("r")

    @authors("babenko", "ignat")
    def test_rename_success(self):
        create_rack("r1")
        n = get_nodes()[0]
        self._set_rack(n, "r1")

        set("//sys/racks/r1/@name", "r2")
        assert get("//sys/racks/r2/@name") == "r2"
        assert self._get_rack(n) == "r2"

    @authors("babenko", "ignat")
    def test_rename_fail(self):
        create_rack("r1")
        create_rack("r2")
        with pytest.raises(YtError):
            set("//sys/racks/r1/@name", "r2")

    @authors("babenko", "ignat")
    def test_assign_success1(self):
        create_rack("r")
        n = get_nodes()[0]
        self._set_rack(n, "r")
        assert self._get_rack(n) == "r"

    @authors("babenko", "ignat")
    def test_assign_success2(self):
        create_rack("r")
        nodes = get_nodes()
        for node in nodes:
            self._set_rack(node, "r")
        for node in nodes:
            assert self._get_rack(node) == "r"
        assert_items_equal(get("//sys/racks/r/@nodes"), nodes)

    @authors("babenko", "ignat")
    def test_remove(self):
        create_rack("r")
        nodes = get_nodes()
        for node in nodes:
            self._set_rack(node, "r")
        remove_rack("r")
        for node in nodes:
            assert not self._has_rack(node)

    @authors("babenko")
    def test_assign_fail(self):
        n = get_nodes()[0]
        with pytest.raises(YtError):
            self._set_rack(n, "r")

    @authors("babenko", "ignat")
    def test_write_regular(self):
        self._init_n_racks(2)
        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

    @authors("babenko", "ignat")
    def test_write_erasure(self):
        self._init_n_racks(6)
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        write_file("//tmp/file", self.FILE_DATA)

    @authors("babenko", "ignat")
    def test_write_journal(self):
        self._init_n_racks(3)
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA)

    @authors("babenko")
    def test_unsafely_placed(self):
        create_rack("r1")
        create_rack("r2")

        nodes = ls("//sys/cluster_nodes")
        self._set_rack(nodes[0], "r1")
        for i in range(1, len(nodes)):
            self._set_rack(nodes[i], "r2")

        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/file")

        self._set_rack(nodes[0], "r2")
        wait(lambda: get("#" + chunk_id + "/@replication_status/default/unsafely_placed"))

        self._reset_all_racks()
        wait(lambda: not get("#" + chunk_id + "/@replication_status/default/unsafely_placed"))

    @authors("babenko")
    def test_regular_move_to_safe_place(self):
        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/file")
        assert not get("#" + chunk_id + "/@replication_status/default/unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 3

        mapping = {}
        nodes = get_nodes()
        for node in nodes:
            if node in replicas:
                mapping[node] = "r1"
            else:
                mapping[node] = "r2"
        self._set_rack_map(mapping)

        self._wait_for_safely_placed(chunk_id)

        assert self._get_max_replicas_per_rack(mapping, chunk_id) <= 2

    @authors("babenko", "ignat")
    def test_erasure_move_to_safe_place(self):
        create("file", "//tmp/file", attributes={"erasure_codec": "lrc_12_2_2"})
        write_file("//tmp/file", self.FILE_DATA)

        chunk_id = get_singular_chunk_id("//tmp/file")
        assert not get("#" + chunk_id + "/@replication_status/default/unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 16

        # put unlisted nodes to the end
        nodes = get_nodes()
        replicas_plus_nodes = replicas
        for node in nodes:
            if node not in replicas:
                replicas_plus_nodes.append(node)

        assert len(replicas_plus_nodes) == TestRacks.NUM_NODES
        mapping = {
            replicas_plus_nodes[0]: "r1",
            replicas_plus_nodes[1]: "r1",
            replicas_plus_nodes[2]: "r1",
            replicas_plus_nodes[3]: "r1",
            replicas_plus_nodes[4]: "r1",
            replicas_plus_nodes[5]: "r2",
            replicas_plus_nodes[6]: "r2",
            replicas_plus_nodes[7]: "r2",
            replicas_plus_nodes[8]: "r3",
            replicas_plus_nodes[9]: "r3",
            replicas_plus_nodes[10]: "r3",
            replicas_plus_nodes[11]: "r4",
            replicas_plus_nodes[12]: "r4",
            replicas_plus_nodes[13]: "r4",
            replicas_plus_nodes[14]: "r5",
            replicas_plus_nodes[15]: "r5",
            replicas_plus_nodes[16]: "r5",
            replicas_plus_nodes[17]: "r6",
            replicas_plus_nodes[18]: "r6",
            replicas_plus_nodes[19]: "r6",
        }
        self._set_rack_map(mapping)

        self._wait_for_safely_placed(chunk_id)

        assert self._get_max_replicas_per_rack(mapping, chunk_id) <= 3

    @authors("shakurov")
    def test_decommission_with_3_racks_yt_9720(self):
        set("//sys/media/default/@config/max_regular_replicas_per_rack", 1)

        create_rack("r0")
        create_rack("r1")
        create_rack("r2")

        nodes = ls("//sys/cluster_nodes")
        for i in range(len(nodes)):
            rack = "r" + str(i % 3)
            self._set_rack(nodes[i], rack)

        create("file", "//tmp/file")
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 3})

        chunk_id = get_singular_chunk_id("//tmp/file")

        replicas = get("#" + chunk_id + "/@stored_replicas")

        assert len(replicas) == 3
        node_to_decommission = replicas[0]
        set_node_decommissioned(node_to_decommission, True)

        def decommission_successful():
            replicas = get("#" + chunk_id + "/@stored_replicas")
            return len(replicas) == 3 and (node_to_decommission not in replicas)

        wait(decommission_successful)

        set("//sys/media/default/@config/max_regular_replicas_per_rack", 64)

    @authors("babenko")
    def test_journal_move_to_safe_place(self):
        create("journal", "//tmp/j")
        write_journal("//tmp/j", self.JOURNAL_DATA, enable_chunk_preallocation=False)
        wait_until_sealed("//tmp/j")

        chunk_id = get_singular_chunk_id("//tmp/j")
        assert not get("#" + chunk_id + "/@replication_status/default/unsafely_placed")

        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == 3

        mapping = {}
        nodes = get_nodes()
        for i in range(len(nodes)):
            mapping[nodes[i]] = "r" + str(i % 3)
        for node in replicas:
            mapping[node] = "r0"
        self._set_rack_map(mapping)

        self._wait_for_safely_placed(chunk_id)

        assert self._get_max_replicas_per_rack(mapping, chunk_id) <= 1

    @authors("danilalexeev")
    def test_journals_with_degraded_racks(self):
        set("//sys/media/default/@config/max_replicas_per_rack", 2)

        mapping = {}
        nodes = get_nodes()
        for i in range(len(nodes)):
            mapping[nodes[i]] = "r" + str(i % 2)
        self._set_rack_map(mapping)

        set("//sys/@config/chunk_manager/force_rack_awareness_for_erasure_parts", True)

        create("journal", "//tmp/j", attributes={
            "erasure_codec": "isa_lrc_12_2_2",
            "replication_factor": 1,
            "read_quorum": 14,
            "write_quorum": 15,
        })

        with pytest.raises(YtError, match="retry count limit exceeded"):
            write_journal("//tmp/j", self.JOURNAL_DATA, journal_writer={
                "open_session_backoff_time": 100,
            })

        wait_until_sealed("//tmp/j")

        set("//sys/@config/chunk_manager/force_rack_awareness_for_erasure_parts", False)

        write_journal("//tmp/j", self.JOURNAL_DATA)
        wait_until_sealed("//tmp/j")

        assert read_journal("//tmp/j") == self.JOURNAL_DATA

        # reset to the initial value
        set("//sys/media/default/@config/max_replicas_per_rack", 64)

    @authors("babenko")
    def test_rack_count_limit(self):
        for i in range(255):
            create_rack("r" + str(i))
        with pytest.raises(YtError):
            create_rack("too_many")

    @authors("shakurov")
    def test_max_replication_factor(self):
        set("//sys/media/default/@config/max_regular_replicas_per_rack", 1)
        self._init_n_racks(6)

        create("file", "//tmp/file", attributes={"replication_factor": 10})
        write_file("//tmp/file", self.FILE_DATA, file_writer={"upload_replication_factor": 10})

        chunk_id = get_singular_chunk_id("//tmp/file")

        replication_status = get("#{0}/@replication_status/default".format(chunk_id))
        assert replication_status["underreplicated"] or replication_status["unsafely_placed"]

        set("//sys/media/default/@config/max_replication_factor", 6)

        def chunk_is_ok():
            replication_status = get("#{0}/@replication_status/default".format(chunk_id))
            return not replication_status["underreplicated"] and not replication_status["unsafely_placed"]

        wait(lambda: chunk_is_ok)

        set("//sys/media/default/@config/max_regular_replicas_per_rack", 64)


##################################################################


@pytest.mark.enabled_multidaemon
class TestRacksMulticell(TestRacks):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2
