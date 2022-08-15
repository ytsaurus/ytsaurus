from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE
from yt_commands import (authors, print_debug, build_master_snapshots, sync_create_cells, wait_for_cells)

from original_tests.yt.yt.tests.integration.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import os
import pytest
import yatest.common

##################################################################


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
        "22_2": ["master"],
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
    @pytest.mark.timeout(150)
    def test(self):
        CHECKER_LIST = [
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
        "22_2": ["master", "node"],
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
