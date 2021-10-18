from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import (
    authors, get, print_debug, build_master_snapshots, exists, ls)


from original_tests.yt.yt.tests.integration.tests.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import os
import pytest
import yatest.common

##################################################################


def check_foo():
    assert not exists("//sys/@supports_virtual_mutations")

    yield

    assert get("//sys/@supports_virtual_mutations")


def check_per_flavor_maps():
    nodes = ls("//sys/cluster_nodes")

    cluster_node_map_id = get("//sys/cluster_nodes/@id")
    assert get("//sys/cluster_nodes/@type") == "cluster_node_map"

    def get_lease_txs():
        return [get("//sys/cluster_nodes/{}/@lease_transaction_id".format(node)) for node in nodes]
    lease_txs = get_lease_txs()

    yield

    for map_name in ["data", "exec", "tablet", "cluster"]:
        map_nodes = set(ls("//sys/{}_nodes".format(map_name)))
        assert set(nodes) == set(map_nodes)

    # Cluster node map should be recreated
    assert get("//sys/cluster_nodes/@id") != cluster_node_map_id
    assert get("//sys/cluster_nodes/@type") == "cluster_node_map"

    # Nodes should not reregister
    assert get_lease_txs() == lease_txs


class TestMasterSnapshotsCompatibility(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    ARTIFACT_COMPONENTS = {
        "21_2": ["master"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    @authors("gritukan")
    def test(self):
        CHECKER_LIST = [
            check_foo,
            check_per_flavor_maps,
        ] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_master_snapshots(set_read_only=True)

        with Restarter(self.Env, MASTERS_SERVICE):
            master_path = os.path.join(self.bin_path, "ytserver-master")
            ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/server/all/ytserver-all")
            print_debug("Removing {}".format(master_path))
            os.remove(master_path)
            print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, master_path))
            os.symlink(ytserver_all_trunk_path, master_path)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
