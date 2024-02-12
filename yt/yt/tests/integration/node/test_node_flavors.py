from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, insert_rows, with_breakpoint, wait_breakpoint, release_breakpoint,
    create, get, ls, write_table, wait, remove, exists, update_nodes_dynamic_config,
    get_data_nodes, get_exec_nodes, get_tablet_nodes, get_chaos_nodes, get_singular_chunk_id,
    sync_mount_table, sync_unmount_table, run_test_vanilla, sync_create_cells,
    set_node_banned)

from yt.environment.helpers import assert_items_equal

import pytest

#################################################################


class TestNodeFlavors(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1
    # TODO(gritukan): Chaos Node has all flavors.
    NUM_CHAOS_NODES = 1

    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 2000,
            "lease_transaction_ping_period": 1000,
        },
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        node_flavors = [
            ["data"],
            ["exec"],
            ["tablet"],
            ["data", "exec"],
            ["data", "tablet"],
            ["exec", "tablet"],
        ]

        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        config["flavors"] = node_flavors[cls.node_counter]
        cls.node_counter = (cls.node_counter + 1) % cls.NUM_NODES

    def _find_node_with_flavors(self, required_flavors):
        for node in get("//sys/cluster_nodes"):
            flavors = get("//sys/cluster_nodes/{}/@flavors".format(node))
            found = True
            for flavor in required_flavors:
                if flavor not in flavors:
                    found = False
                    break

            if found:
                return node
        return None

    @authors("gritukan")
    def test_data_nodes(self):
        data_nodes = get_data_nodes()
        assert len(data_nodes) == 3

        exec_nodes = get_exec_nodes()
        assert len(exec_nodes) == 3

        create("table", "//tmp/t")
        for i in range(5):
            write_table("//tmp/t", [{"x": i}])
        for chunk_id in get("//tmp/t/@chunk_ids"):
            for replica in get("#{}/@stored_replicas".format(chunk_id)):
                replica_address = str(replica)
                assert replica_address in data_nodes or replica_address in exec_nodes

    @authors("gritukan")
    def test_exec_nodes(self):
        exec_nodes = get_exec_nodes()
        assert len(exec_nodes) == 3

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=2)
        job_ids = wait_breakpoint(job_count=2)
        for job_id in job_ids:
            get(op.get_path() + "/controller_orchid/running_jobs/{}/address".format(job_id)) in exec_nodes
        release_breakpoint()

    @authors("gritukan")
    def test_tablet_nodes(self):
        tablet_nodes = get_tablet_nodes()
        assert len(tablet_nodes) == 3

        sync_create_cells(2)
        for cell_id in ls("//sys/tablet_cells"):
            peers = get("#" + cell_id + "/@peers")
            assert len(peers) == 1
            assert peers[0]["address"] in tablet_nodes

    @authors("gritukan")
    def test_chaos_nodes(self):
        chaos_nodes = get_chaos_nodes()
        assert len(chaos_nodes) == 1
        # TODO(gritukan, savrus): Create chaos cell here.

    @authors("gritukan")
    @pytest.mark.parametrize("flavor", ["data", "exec", "tablet", "chaos"])
    def test_per_flavor_node_maps(self, flavor):
        expected = []

        for node in get("//sys/cluster_nodes"):
            if flavor in get("//sys/cluster_nodes/{}/@flavors".format(node)):
                expected.append(node)

        actual = []
        for node in get("//sys/{}_nodes".format(flavor)):
            actual.append(node)

        assert_items_equal(expected, actual)

    @authors("gritukan")
    def test_ban_multi_flavor_node(self):
        node = self._find_node_with_flavors(["data", "exec"])
        assert node is not None

        def check_banned(expected):
            assert get("//sys/cluster_nodes/{}/@banned".format(node)) == expected
            assert get("//sys/data_nodes/{}/@banned".format(node)) == expected
            assert get("//sys/exec_nodes/{}/@banned".format(node)) == expected

        check_banned(False)
        set_node_banned(str(node), True, wait_for_master=False)
        check_banned(True)
        set_node_banned(str(node), False, wait_for_master=False)
        check_banned(False)

    @authors("gritukan")
    def test_remove_multi_flavored_node(self):
        node = None
        for n in get("//sys/cluster_nodes"):
            flavors = get("//sys/cluster_nodes/{}/@flavors".format(n))
            # TODO(gritukan, savrus): Chaos node is not restarted via Restarter.
            if "data" in flavors and "exec" in flavors and "chaos" not in flavors:
                node = n
        assert node is not None

        def check_exists(expected):
            assert (node in ls("//sys/cluster_nodes")) == expected
            assert (node in ls("//sys/data_nodes")) == expected
            assert (node in ls("//sys/exec_nodes")) == expected

        check_exists(True)
        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
            remove("//sys/cluster_nodes/{}".format(node))
            check_exists(False)

        wait(lambda: exists("//sys/cluster_nodes/{}".format(node)))
        wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "online")
        check_exists(True)


##################################################################


class TestNodeFlavorsMulticell(TestNodeFlavors):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestNodeFlavorsExecNodeIsNotDataNode(TestNodeFlavors):
    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 2000,
            "lease_transaction_ping_period": 1000,
        },
        "exec_node_is_not_data_node": True,
    }


class TestNodeFlavorsExecNodeIsNotDataNodeMulticell(TestNodeFlavors):
    NUM_SECONDARY_MASTER_CELLS = 2

    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 2000,
            "lease_transaction_ping_period": 1000,
        },
        "exec_node_is_not_data_node": True,
    }

##################################################################


class TestDataAndTabletNodesCollocation(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        if cls.node_counter == 0:
            config["flavors"] = ["tablet"]
        else:
            config["flavors"] = ["data"]
        if cls.node_counter <= 1:
            config["host_name"] = "h"
        cls.node_counter = (cls.node_counter + 1) % cls.NUM_NODES

    @authors("gritukan")
    def test_flushed_chunk_local_replica(self):
        update_nodes_dynamic_config({
            "master_connector": {
                "use_host_objects": True,
            },
        })

        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        create(
            "table",
            "//tmp/t",
            attributes={
                "dynamic": True,
                "replication_factor": 1,
                "schema": schema,
            })
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 42, "value": "foo"}])
        sync_unmount_table("//tmp/t")

        data_node = None
        for node in get("//sys/hosts/h/@nodes"):
            if get("//sys/cluster_nodes/{}/@flavors".format(node)) == ["data"]:
                data_node = node

        chunk_id = get_singular_chunk_id("//tmp/t")
        replicas = get("#{}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1
        assert str(replicas[0]) == data_node
