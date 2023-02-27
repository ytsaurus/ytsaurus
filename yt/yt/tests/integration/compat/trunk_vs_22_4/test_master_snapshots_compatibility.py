from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE
from yt_commands import (
    authors, create_tablet_cell_bundle, print_debug, build_master_snapshots, sync_create_cells, wait_for_cells,
    ls, get, set, retry)

from original_tests.yt.yt.tests.integration.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import yatest.common

import os
import pytest

##################################################################


class MasterSnapshotsCompatibilityBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True
    TEST_LOCATION_AWARE_REPLICATOR = True

    DELTA_MASTER_CONFIG = {
        "logging": {
            # COMPAT(gritukan): EMasterReign::FixAccountResourceUsageCharge
            "abort_on_alert": False,
        },
        "security_manager": {
            "alert_on_ref_counter_mismatch": False,
        },
    }

    ARTIFACT_COMPONENTS = {
        "22_4": ["master"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "node", "job-proxy", "exec", "tools"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

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


##################################################################


def check_maintenance_flags():
    node = ls("//sys/cluster_nodes")[0]
    set(f"//sys/cluster_nodes/{node}/@banned", True)

    yield

    assert get(f"//sys/cluster_nodes/{node}/@banned")
    maintenances = get(f"//sys/cluster_nodes/{node}/@maintenance_requests")
    assert list(map(lambda req: req["maintenance_type"], maintenances.values())) == ["ban"]
    set(f"//sys/cluster_nodes/{node}/@banned", False)
    assert not get(f"//sys/cluster_nodes/{node}/@banned")
    assert not get(f"//sys/cluster_nodes/{node}/@maintenance_requests")


class TestMasterSnapshotsCompatibility(MasterSnapshotsCompatibilityBase):
    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False
    TEST_MAINTENANCE_FLAGS = True

    @authors("gritukan", "kvk1920")
    @pytest.mark.timeout(150)
    def test(self):
        CHECKER_LIST = [check_maintenance_flags] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        self.restart_with_update(MASTERS_SERVICE)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)


##################################################################


class TestTabletCellsSnapshotsCompatibility(MasterSnapshotsCompatibilityBase):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "node", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy", "exec"],
    }

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

    @authors("aleksandra-zh")
    def test(self):
        cell_ids = sync_create_cells(1)

        self.restart_with_update(NODES_SERVICE, build_snapshots=False)

        wait_for_cells(cell_ids)


class TestBundleControllerAttribute(MasterSnapshotsCompatibilityBase):
    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False

    @authors("capone212")
    def test(self):
        create_tablet_cell_bundle("bundle212")
        bundle_controller_config = {
            "tablet_node_count" : 10,
        }
        bundle_path = "//sys/tablet_cell_bundles/bundle212"
        config_path = "{}/@bundle_controller_target_config".format(bundle_path)

        set(config_path, bundle_controller_config)
        assert bundle_controller_config == get(config_path)

        retry(lambda: self.restart_with_update(MASTERS_SERVICE))

        assert bundle_controller_config == get(config_path)
        assert "bundle_controller_target_config" not in get("{}/@user_attribute_keys".format(bundle_path))

        bundle_controller_config["tablet_node_count"] = 11
        set(config_path, bundle_controller_config)

        retry(lambda: self.restart_with_update(MASTERS_SERVICE))

        assert bundle_controller_config == get(config_path)
