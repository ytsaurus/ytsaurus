from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE
from yt_commands import (
    ls, get, set, authors, print_debug, build_master_snapshots, create,
    sync_create_cells, wait_for_cells,
    write_table, read_table, get_singular_chunk_id)

from original_tests.yt.yt.tests.integration.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import yatest.common

import os
import pytest

##################################################################


def check_portal_synchronization_config():
    path = "//sys/@config/cypress_manager/portal_synchronization_period"
    set(path, 1111)

    yield

    assert get(path) == 1111

##################################################################


class MasterSnapshotsCompatibilityBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True
    TEST_LOCATION_AWARE_REPLICATOR = True

    DELTA_MASTER_CONFIG = {
        "logging": {
            "abort_on_alert": True,
        },
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
        super(MasterSnapshotsCompatibilityBase, self).teardown_method(method)

    def restart_with_update(self, service, build_snapshots=True):
        if build_snapshots:
            build_master_snapshots(set_read_only=True)

        with Restarter(self.Env, service):
            master_path = os.path.join(self.bin_path, "ytserver-master")
            ytserver_all_trunk_path = yatest.common.binary_path("yt/yt/packages/tests_package/ytserver-all")
            print_debug("Renaming {} to {}".format(master_path, master_path + "__BACKUP"))
            os.rename(master_path, master_path + "__BACKUP")
            print_debug("Symlinking {} to {}".format(ytserver_all_trunk_path, master_path))
            os.symlink(ytserver_all_trunk_path, master_path)


class TestMasterSnapshotsCompatibility(MasterSnapshotsCompatibilityBase):
    @authors("gritukan", "kvk1920")
    @pytest.mark.timeout(150)
    def test(self):
        CHECKER_LIST = [
            check_portal_synchronization_config,
        ] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        self.restart_with_update(MASTERS_SERVICE)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)


class TestTabletCellsSnapshotsCompatibility(MasterSnapshotsCompatibilityBase):
    ARTIFACT_COMPONENTS = {
        "22_2": ["master", "node"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy", "exec", "tools"],
    }

    @authors("aleksandra-zh")
    def test(self):
        cell_ids = sync_create_cells(1)

        self.restart_with_update(NODES_SERVICE, build_snapshots=False)

        wait_for_cells(cell_ids)


class TestImaginaryChunkLocations(MasterSnapshotsCompatibilityBase):
    NUM_NODES = 1

    @authors("kvk1920")
    def test(self):
        create("table", "//tmp/t", attributes={
            "chunk_writer": {"upload_replication_factor": 1},
            "replication_factor": 1,
        })
        write_table("//tmp/t", {"a": 1}, upload_replication_factor=1)
        chunk_id = get_singular_chunk_id("//tmp/t")

        stored_replicas = get(f"#{chunk_id}/@stored_replicas")

        def check():
            assert get(f"#{chunk_id}/@stored_replicas") == stored_replicas
            assert read_table("//tmp/t") == [{"a": 1}]

        self.restart_with_update(MASTERS_SERVICE)
        assert not get("//sys/@config/node_tracker/enable_real_chunk_locations")

        node = ls("//sys/data_nodes")[0]
        assert get(f"//sys/data_nodes/{node}/@use_imaginary_chunk_locations")
        check()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        assert get(f"//sys/data_nodes/{node}/@use_imaginary_chunk_locations")
        check()

        with Restarter(self.Env, NODES_SERVICE):
            set("//sys/@config/node_tracker/enable_real_chunk_locations", True)

        assert not get(f"//sys/data_nodes/{node}/@use_imaginary_chunk_locations")
        check()
