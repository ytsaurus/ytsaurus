from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import (
    ls, exists, get, set, raises_yt_error, authors, print_debug, build_master_snapshots, create, start_transaction,
    commit_transaction)

from original_tests.yt.yt.tests.integration.tests.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

from yt.test_helpers import assert_items_equal

import os
import pytest
import yatest.common
import builtins

##################################################################


def check_cluster_connection_simple():
    set("//sys/@cluster_connection", {"default_input_row_limit": "abacaba"})

    yield

    assert get("//sys/@cluster_connection") == {"default_input_row_limit": "abacaba"}
    with raises_yt_error("Cannot parse"):
        set("//sys/@cluster_connection", {"default_input_row_limit": "abacaba"})


def check_cluster_name_simple():
    set("//sys/@cluster_name", "a" * 130)

    yield

    assert get("//sys/@cluster_name") == "a" * 130
    with raises_yt_error("too long"):
        set("//sys/@cluster_name", "a" * 130)


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
        "21_3": ["master"],
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
            check_cluster_connection_simple,
            check_cluster_name_simple,
            check_chunk_locations,
            check_queue_list
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
