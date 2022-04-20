from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE
from yt_commands import (
    ls, exists, get, authors, print_debug, build_master_snapshots, create, start_transaction,
    commit_transaction, sync_create_cells, wait_for_cells, create_user, make_ace, wait, set)

from original_tests.yt.yt.tests.integration.tests.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

from yt.test_helpers import assert_items_equal

import os
import pytest
import yatest.common
import builtins

##################################################################


def check_chunk_locations():
    node_address = ls("//sys/cluster_nodes")[0]

    location_uuids = list(location["location_uuid"] for location in get("//sys/cluster_nodes/{}/@statistics/locations".format(node_address)))
    assert len(location_uuids) > 0

    yield

    assert_items_equal(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys(), location_uuids)
    for location_uuid in location_uuids:
        assert exists("//sys/chunk_locations/{}".format(location_uuid))
        assert get("//sys/chunk_locations/{}/@uuid".format(location_uuid)) == location_uuid
        assert get("//sys/chunk_locations/{}/@node_address".format(location_uuid)) == node_address


def check_queue_list():
    create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
    create("table", "//tmp/qq", attributes={"dynamic": False, "schema": [{"name": "data", "type": "string"}]})
    create("table", "//tmp/qqq", attributes={"dynamic": True,
                                             "schema": [{"name": "data", "type": "string", "sort_order": "ascending"},
                                                        {"name": "payload", "type": "string"}]})
    create("map_node", "//tmp/mn")
    tx = start_transaction(timeout=60000)
    create("table", "//tmp/qqqq", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]}, tx=tx)

    yield

    assert builtins.set(get("//sys/@queue_agent_object_revisions")["queues"].keys()) == {"//tmp/q"}
    commit_transaction(tx)
    assert builtins.set(get("//sys/@queue_agent_object_revisions")["queues"].keys()) == {"//tmp/q", "//tmp/qqqq"}


def check_portal_entrance_validation():
    set("//sys/@config/cypress_manager/portal_synchronization_period", 1000)
    create_user("u")
    create("map_node", "//tmp/portal_d")
    set("//tmp/portal_d/@acl", [make_ace("deny", ["u"], ["read"])])
    create("portal_entrance", "//tmp/portal_d/p", attributes={"exit_cell_tag": 13})
    create_user("v")
    set("//tmp/portal_d/p/@acl", [make_ace("deny", ["v"], ["read"])])
    portal_exit_effective_acl = get("//tmp/portal_d/p/@effective_acl")
    portal_entrance_effective_acl = get("//tmp/portal_d/p&/@effective_acl")

    assert portal_entrance_effective_acl != portal_exit_effective_acl

    yield

    wait(lambda: get("//tmp/portal_d/p/@annotation_path") == get("//tmp/portal_d/p&/@annotation_path"))
    wait(lambda: get("//tmp/portal_d/p/@effective_acl") == portal_entrance_effective_acl)


class TestMasterSnapshotsCompatibility(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100,
        },
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 1
            }
        }
    }

    ARTIFACT_COMPONENTS = {
        "22_1": ["master"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    def teardown_method(self, method):
        master_path = os.path.join(self.bin_path, "ytserver-master")
        if os.path.exists(master_path + "__BACKUP"):
            print_debug("Removing symlink {}".format(master_path))
            os.remove(master_path)
            print_debug("Renaming {} to {}".format(master_path + "__BACKUP", master_path))
            os.rename(master_path + "__BACKUP", master_path)
        super(TestMasterSnapshotsCompatibility, self).teardown_method(method)

    @authors("gritukan", "kvk1920")
    def test(self):
        CHECKER_LIST = [
            check_chunk_locations,
            check_queue_list,
            check_portal_entrance_validation,
        ] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        build_master_snapshots(set_read_only=True)

        with Restarter(self.Env, MASTERS_SERVICE):
            master_path = os.path.join(self.bin_path, "ytserver-master")
            ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/packages/tests_package/ytserver-all")
            print_debug("Renaming {} to {}".format(master_path, master_path + "__BACKUP"))
            os.rename(master_path, master_path + "__BACKUP")
            print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, master_path))
            os.symlink(ytserver_all_trunk_path, master_path)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)


class TestTabletCellsSnapshotsCompatibility(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True

    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": False,
        },
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100,
        },
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 1
            }
        }
    }

    ARTIFACT_COMPONENTS = {
        "22_1": ["master", "node"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy", "exec", "tools"],
    }

    def teardown_method(self, method):
        node_path = os.path.join(self.bin_path, "ytserver-node")
        if os.path.exists(node_path + "__BACKUP"):
            print_debug("Removing symlink {}".format(node_path))
            os.remove(node_path)
            print_debug("Renaming {} to {}".format(node_path + "__BACKUP", node_path))
            os.rename(node_path + "__BACKUP", node_path)
        super(TestTabletCellsSnapshotsCompatibility, self).teardown_method(method)

    @authors("aleksandra-zh")
    def test(self):
        cell_ids = sync_create_cells(1)

        with Restarter(self.Env, NODES_SERVICE):
            nodes_path = os.path.join(self.bin_path, "ytserver-node")
            ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/packages/tests_package/ytserver-all")
            print_debug("Renaming {} to {}".format(nodes_path, nodes_path + "__BACKUP"))
            os.rename(nodes_path, nodes_path + "__BACKUP")
            print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, nodes_path))
            os.symlink(ytserver_all_trunk_path, nodes_path)

        wait_for_cells(cell_ids)
