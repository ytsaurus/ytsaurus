from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, create, ls, get, set, exists, write_table,
    get_singular_chunk_id, set_node_banned)


import pytest

from time import sleep

import builtins

##################################################################


class TestAllyReplicas(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 7

    GENERIC_REPLICA_INDEX = 16
    CHUNK_OBJECT_TYPE = "64"
    ERASURE_CHUNK_OBJECT_TYPE = "66"
    FIRST_ERASURE_PART_OBJECT_TYPE = "67"

    def setup_method(self, method):
        super(TestAllyReplicas, self).setup_method(method)
        set("//sys/@config/chunk_manager/ally_replica_manager/enable_ally_replica_announcement", True)
        set("//sys/@config/chunk_manager/ally_replica_manager/enable_endorsements", True)

    def _create_table(self, path, erasure_codec="none"):
        attrs = {}
        if erasure_codec == "none":
            attrs["replication_factor"] = 3
        else:
            attrs["erasure_codec"] = erasure_codec
        create("table", path, attributes=attrs)

    def _get_object_type(self, object_id):
        return object_id.split("-")[2][-4:].lstrip("0")

    def _get_cell_tag(self, object_id):
        return object_id.split("-")[2][:-4] or "0"

    def _replace_type_in_id(self, object_id, object_type):
        parts = object_id.split("-")
        cell_tag = parts[2][:-4]
        if cell_tag:
            object_type = object_type.rjust(4, "0")
        parts[2] = cell_tag + object_type
        return "-".join(parts)

    def _encode_chunk_id(self, chunk_id, replica_index):
        object_type = self._get_object_type(chunk_id)
        if object_type == self.CHUNK_OBJECT_TYPE:
            assert replica_index == self.GENERIC_REPLICA_INDEX
            return chunk_id

        assert object_type == self.ERASURE_CHUNK_OBJECT_TYPE
        new_type = "{:x}".format(int(self.FIRST_ERASURE_PART_OBJECT_TYPE, 16) + replica_index)
        return self._replace_type_in_id(chunk_id, new_type)

    def _check_ally_replicas_consistency(self, chunk_id):
        def _get_replica_index(replica):
            return replica.attributes.get("index", self.GENERIC_REPLICA_INDEX)

        def _replica_sets_equal(lhs, rhs):
            lhs = [(str(x), _get_replica_index(x)) for x in lhs]
            rhs = [(str(x), _get_replica_index(x)) for x in rhs]
            return sorted(lhs) == sorted(rhs)

        stored_replicas = get("//sys/chunks/{}/@stored_replicas".format(chunk_id))
        for r in stored_replicas:
            orchid = "//sys/cluster_nodes/{}/orchid/data_node/stored_chunks".format(r)
            encoded_chunk_id = self._encode_chunk_id(chunk_id, _get_replica_index(r))
            if not exists(orchid + "/" + encoded_chunk_id):
                return False
            ally_replicas = get(orchid + "/" + encoded_chunk_id + "/ally_replicas")
            if not _replica_sets_equal(stored_replicas, ally_replicas):
                return False

        return True

    def _wait_ally_replicas_consistency(self, chunk_id):
        wait(lambda: self._check_ally_replicas_consistency(chunk_id))

    def _get_endorsement_count(self, chunk_id):
        cell_tag = self._get_cell_tag(chunk_id)
        if cell_tag == "0":
            master = ls("//sys/primary_masters")[0]
            master_orchid_path = "//sys/primary_masters/{}/orchid".format(master)
        else:
            master = ls("//sys/secondary_masters/" + cell_tag)[0]
            master_orchid_path = "//sys/secondary_masters/{}/{}/orchid".format(cell_tag, master)
        return get(master_orchid_path + "/chunk_manager/endorsement_count")

    def _set_node_banned_and_wait_for_replicas(self, node, banned, chunk_id):
        set_node_banned(node, banned)
        rf = 3 if self._get_object_type(chunk_id) == self.CHUNK_OBJECT_TYPE else 6
        wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == rf)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("erasure_codec", ["none", "reed_solomon_3_3"])
    def test_ally_replicas(self, erasure_codec):
        self._create_table("//tmp/t", erasure_codec)
        rows = [{"a": "b"}]
        write_table("//tmp/t", rows)

        chunk_id = get_singular_chunk_id("//tmp/t")
        rf = 3 if erasure_codec == "none" else 6
        wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == rf)
        self._wait_ally_replicas_consistency(chunk_id)

        for node in ls("//sys/cluster_nodes"):
            set_node_banned(node, True)
            wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == rf)
            self._wait_ally_replicas_consistency(chunk_id)
            set_node_banned(node, False)

    @authors("ifsmirnov")
    def test_ally_replicas_disabled(self):
        pytest.skip("YT-15097")
        self._create_table("//tmp/t")
        rows = [{"a": "b"}]
        write_table("//tmp/t", rows)

        chunk_id = get_singular_chunk_id("//tmp/t")
        rf = 3
        wait(lambda: len(get("//sys/chunks/{}/@stored_replicas".format(chunk_id))) == rf)
        self._wait_ally_replicas_consistency(chunk_id)

        set("//sys/@config/chunk_manager/ally_replica_manager/safe_online_node_count", 6)

        banned_nodes = builtins.set()

        def _get_any_replica(chunk_id):
            return get("//sys/chunks/{}/@stored_replicas/0".format(chunk_id))

        def _ban_any_replica(chunk_id):
            node = str(_get_any_replica(chunk_id))
            self._set_node_banned_and_wait_for_replicas(node, True, chunk_id)
            banned_nodes.add(node)

        def _unban_any_node(chunk_id):
            node = banned_nodes.pop()
            self._set_node_banned_and_wait_for_replicas(node, False, chunk_id)

        _ban_any_replica(chunk_id)
        self._wait_ally_replicas_consistency(chunk_id)

        _ban_any_replica(chunk_id)
        sleep(1)
        # There are < 6 online nodes, no replica announcements.
        assert not self._check_ally_replicas_consistency(chunk_id)
        # Endorsements are kept.
        assert self._get_endorsement_count(chunk_id) > 0

        _unban_any_node(chunk_id)
        # There are >= 6 online nodes, old endorsements are sent.
        self._wait_ally_replicas_consistency(chunk_id)

        _ban_any_replica(chunk_id)
        assert not self._check_ally_replicas_consistency(chunk_id)
        assert self._get_endorsement_count(chunk_id) > 0

        set("//sys/@config/chunk_manager/ally_replica_manager/safe_online_node_count", 0)
        # Enough online nodes, endorsements are sent.
        wait(lambda: self._get_endorsement_count(chunk_id) == 0)
        self._wait_ally_replicas_consistency(chunk_id)

        set("//sys/@config/chunk_manager/ally_replica_manager/safe_online_node_count", 6)
        _unban_any_node(chunk_id)
        _ban_any_replica(chunk_id)
        assert not self._check_ally_replicas_consistency(chunk_id)
        assert self._get_endorsement_count(chunk_id) > 0

        set("//sys/@config/chunk_manager/ally_replica_manager", {
            "enable_ally_replica_announcement": True,
            "enable_endorsements": False,
            "safe_online_node_count": 5})
        # There are enough online nodes, but endorsements are not sent but discarded.
        wait(lambda: self._get_endorsement_count(chunk_id) == 0)
        assert not self._check_ally_replicas_consistency(chunk_id)

        _unban_any_node(chunk_id)
        _unban_any_node(chunk_id)

        assert not banned_nodes


##################################################################


class TestAllyReplicasMulticell(TestAllyReplicas):
    NUM_SECONDARY_MASTER_CELLS = 2
