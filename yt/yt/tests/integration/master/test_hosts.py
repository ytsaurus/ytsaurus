
from time import sleep
from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, get_singular_chunk_id, ls, get, set, remove, create_rack, exists, create_host, create, remove_host, get_driver, set_node_banned, set_nodes_banned, wait, write_table)

from yt.common import YtError

from copy import deepcopy

from yt import yson

import pytest

##################################################################


class TestHostsBase(YTEnvSetup):
    def teardown_method(self, method):
        for node in ls("//sys/cluster_nodes"):
            self._set_host(node, node)

        for host in ls("//sys/hosts"):
            try:
                remove_host(host)
            except YtError:
                pass
        super(TestHostsBase, self).teardown_method(method)

    def _get_custom_hosts(self):
        nodes = ls("//sys/cluster_nodes")
        hosts = ls("//sys/hosts")
        for node in nodes:
            assert node in hosts
        return [host for host in hosts if host not in nodes]

    def _get_host(self, node):
        return get("//sys/cluster_nodes/{}/@host".format(node))

    def _set_host(self, node, host):
        set("//sys/cluster_nodes/{}/@host".format(node), host)

    def _get_nodes(self, host):
        return get("//sys/hosts/{}/@nodes".format(host))

    def _get_rack(self, host):
        return get("//sys/hosts/{}/@rack".format(host))

    def _set_rack(self, host, rack):
        set("//sys/hosts/{}/@rack".format(host), rack)

    def _reset_rack(self, host):
        remove("//sys/hosts/{}/@rack".format(host))

    def _get_hosts(self, rack):
        return get("//sys/racks/{}/@hosts".format(rack))


@pytest.mark.enabled_multidaemon
class TestHosts(TestHostsBase):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("gritukan")
    def test_create(self):
        create_host("h")
        assert self._get_custom_hosts() == ["h"]
        assert get("//sys/hosts/h/@name") == "h"
        assert get("//sys/hosts/h/@nodes") == []
        assert not exists("//sys/hosts/h/@rack")

    @authors("gritukan")
    def test_empty_name_fail(self):
        with pytest.raises(YtError):
            create_host("")
        assert self._get_custom_hosts() == []

    @authors("gritukan")
    def test_duplicate_name_fail(self):
        create_host("h")
        with pytest.raises(YtError):
            create_host("h")
        assert self._get_custom_hosts() == ["h"]

    @authors("gritukan")
    def test_rename_forbidden(self):
        create_host("h")
        with pytest.raises(YtError):
            set("//sys/hosts/h/@name", "g")
        assert self._get_custom_hosts() == ["h"]

    @authors("gritukan")
    def test_default_node_host(self):
        for node in ls("//sys/cluster_nodes"):
            assert self._get_host(node) == node
            assert self._get_nodes(node) == [node]

    @authors("gritukan")
    def test_set_host(self):
        node = ls("//sys/cluster_nodes")[0]
        create_host("h")

        assert self._get_host(node) == node
        assert self._get_nodes(node) == [node]
        assert self._get_nodes("h") == []
        self._set_host(node, "h")
        assert self._get_host(node) == "h"
        assert self._get_nodes(node) == []
        assert self._get_nodes("h") == [node]

    @authors("gritukan")
    def test_set_invalid_host(self):
        node = ls("//sys/cluster_nodes")[0]
        with pytest.raises(YtError):
            self._set_host(node, "h")
        assert self._get_host(node) == node
        assert self._get_nodes(node) == [node]

    @authors("gritukan")
    def test_remove_host(self):
        create_host("h")
        remove_host("h")

    @authors("gritukan")
    def test_remove_forbidden(self):
        create_host("h")
        node = ls("//sys/cluster_nodes")[0]
        self._set_host(node, "h")
        with pytest.raises(YtError):
            remove_host("h")

    @authors("gritukan")
    def test_set_rack(self):
        create_host("h")
        create_rack("r")

        assert self._get_hosts("r") == []
        self._set_rack("h", "r")
        assert self._get_rack("h") == "r"
        assert self._get_hosts("r") == ["h"]
        self._reset_rack("h")
        assert not exists("//sys/hosts/h/@rack")
        assert self._get_hosts("r") == []

    @authors("gritukan")
    def test_set_rack_fail(self):
        create_host("h")
        with pytest.raises(YtError):
            self._set_rack("h", "r")


##################################################################


class TestHostAwareReplication(TestHostsBase):
    ENABLE_MULTIDAEMON = False  # There are components restart.
    NUM_MASTERS = 1
    NUM_NODES = 32

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "use_host_aware_replicator": True,
        }
    }

    def _get_replica_nodes(self, chunk_id):
        return list(str(x) for x in get(f"#{chunk_id}/@stored_replicas"))

    def _is_chunk_status_ok(self, chunk_id):
        status = get(f"#{chunk_id}/@replication_status/default")
        return not status["underreplicated"] and not status["overreplicated"] and not status["unsafely_placed"]

    def _prepare_different_node_hosts(self):
        nodes = ls("//sys/cluster_nodes")
        for i, node in enumerate(nodes):
            create_host(f"h{i}")
            self._set_host(node, f"h{i}")
            wait(lambda: node in self._get_nodes(f"h{i}"))
        return nodes

    @authors("grphil")
    @pytest.mark.parametrize("storage_parameters", [(3, {"replication_factor": 3}), (16, {"erasure_codec": "lrc_12_2_2"})])
    def test_limited_replication(self, storage_parameters):
        num_replicas, table_creation_attributes = storage_parameters

        nodes = self._prepare_different_node_hosts()
        set_nodes_banned(nodes[:-num_replicas], True)
        create("table", "//tmp/t", attributes=table_creation_attributes)

        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")

        wait(lambda: self._is_chunk_status_ok(chunk_id))
        stored_replicas = self._get_replica_nodes(chunk_id)
        for node in nodes[:-num_replicas]:
            assert node not in stored_replicas
        for node in nodes[-num_replicas:]:
            assert node in stored_replicas

        # Now last node host is same as previous node host.
        self._set_host(nodes[-1], f"h{len(nodes) - 2}")

        wait(lambda: not self._is_chunk_status_ok(chunk_id))

        set_node_banned(nodes[0], False)
        wait(lambda: self._is_chunk_status_ok(chunk_id))
        wait(lambda: nodes[0] in self._get_replica_nodes(chunk_id))

        def check_chunk_after_host_change():
            assert self._is_chunk_status_ok(chunk_id)
            replicas = self._get_replica_nodes(chunk_id)
            for node in nodes[1:-num_replicas]:
                assert node not in replicas
            for node in nodes[-num_replicas:-2]:
                assert node in replicas
            assert nodes[-2] in replicas or nodes[-1] in replicas

        check_chunk_after_host_change()
        sleep(0.5)
        check_chunk_after_host_change()

    def _check_chunk_is_ok(self, chunk_id, num_replicas):
        wait(lambda: self._is_chunk_status_ok(chunk_id))
        replicas = self._get_replica_nodes(chunk_id)
        assert len(replicas) == num_replicas
        assert len({self._get_host(node) for node in replicas}) == num_replicas

    @authors("grphil")
    @pytest.mark.parametrize("erasure_codec", ["none", "lrc_12_2_2"])
    def test_replication_after_host_changes(self, erasure_codec):
        self._prepare_different_node_hosts()

        if erasure_codec == "none":
            num_replicas = 3
            create("table", "//tmp/t", attributes={"replication_factor": 3})
        else:
            num_replicas = 16
            create("table", "//tmp/t", attributes={"erasure_codec": "lrc_12_2_2"})

        write_table("//tmp/t", {"a": "b"})
        chunk_id = get_singular_chunk_id("//tmp/t")
        self._check_chunk_is_ok(chunk_id, num_replicas)

        replicas = self._get_replica_nodes(chunk_id)
        for i in range(1, 10):
            host = self._get_host(replicas[0])
            self._set_host(replicas[0], host)
            self._check_chunk_is_ok(chunk_id, num_replicas)

##################################################################


@pytest.mark.enabled_multidaemon
class TestHostsMulticell(TestHosts):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

##################################################################


class TestPreserveRackForNewHost(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are components restart.
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("gritukan")
    def test_preserve_rack_for_new_host(self):
        node = ls("//sys/cluster_nodes")[0]
        assert get("//sys/cluster_nodes/{}/@host".format(node)) == node

        create_rack("r")
        set("//sys/cluster_nodes/{}/@rack".format(node), "r")

        with Restarter(self.Env, NODES_SERVICE):
            for i, node_config in enumerate(self.Env.configs["node"]):
                config = deepcopy(node_config)
                config["host_name"] = "sas1-2345"
                config_path = self.Env.config_paths["node"][i]
                with open(config_path, "wb") as fout:
                    yson.dump(config, fout)

        assert get("//sys/cluster_nodes/{}/@host".format(node)) == "sas1-2345"
        assert get("//sys/cluster_nodes/{}/@rack".format(node)) == "r"
        assert get("//sys/cluster_nodes/{}/@rack".format(node), driver=get_driver(1)) == "r"
