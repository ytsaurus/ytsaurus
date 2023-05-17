from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE
from yt_commands import (
    authors, create_tablet_cell_bundle, print_debug, build_master_snapshots, sync_create_cells, wait_for_cells,
    ls, get, set, retry, start_transaction, commit_transaction, create, exists, wait, write_table)

from yt_helpers import profiler_factory

from original_tests.yt.yt.tests.integration.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import yatest.common

import builtins
import datetime
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
        "22_4": ["master", "node", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy"],
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
    assert list(map(lambda req: req["type"], maintenances.values())) == ["ban"]
    set(f"//sys/cluster_nodes/{node}/@banned", False)
    assert not get(f"//sys/cluster_nodes/{node}/@banned")
    assert not get(f"//sys/cluster_nodes/{node}/@maintenance_requests")


def check_cypress_transactions():
    old_tx = start_transaction()
    create("map_node", "//tmp/old", tx=old_tx)

    yield

    new_tx = start_transaction()
    create("map_node", "//tmp/new", tx=new_tx)

    assert not get(f"//sys/transactions/{old_tx}/@cypress_transaction")
    assert get(f"//sys/transactions/{new_tx}/@cypress_transaction")

    commit_transaction(old_tx)
    commit_transaction(new_tx)

    assert exists("//tmp/old")
    assert exists("//tmp/new")


def check_chunk_creation_time_histogram():
    create("table", "//tmp/chunk_creation_time")
    write_table("//tmp/chunk_creation_time", {"a": "b"})
    write_table("<append=%true>//tmp/chunk_creation_time", {"a": "b"})

    yield

    def check_histogram(self):
        def parse_time(t):
            return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000

        bounds = sorted(map(parse_time, get("//sys/@config/chunk_manager/global_chunk_statistics_collector/creation_time_histogram_bucket_bounds")))
        chunks = ls("//sys/chunks", attributes=["shard_index"])
        creation_times = [
            (parse_time(get(f"//sys/estimated_creation_time/{chunk}")["min"]), chunk.attributes["shard_index"])
            for chunk in chunks
        ]

        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)
        shards = [
            profiler.histogram(
                "chunk_server/histograms/chunk_creation_time_histogram",
                fixed_tags={"shard_index": str(shard_index)}).get_bins()
            for shard_index in range(60)
        ]

        true_bins = [[0] * (len(bounds) + 1) for _ in range(60)]
        for creation_time, shard_index in creation_times:
            bin_index = 0
            while bin_index < len(bounds) and creation_time >= bounds[bin_index]:
                bin_index += 1
            true_bins[shard_index][bin_index] += 1

        sizes = builtins.set(map(len, shards))
        if len(sizes) != 1:
            print_debug(f"Shards have different sizes: {sizes}")
            return False

        histogram = [[bin["count"] for bin in shard] for shard in shards]

        return histogram == true_bins

    wait(check_histogram)


class TestMasterSnapshotsCompatibility(MasterSnapshotsCompatibilityBase):
    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False
    TEST_MAINTENANCE_FLAGS = True

    @authors("gritukan", "kvk1920")
    @pytest.mark.timeout(150)
    def test(self):
        CHECKER_LIST = [
            check_maintenance_flags,
            check_cypress_transactions,
        ] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

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
        "22_4": ["master", "node", "tools", "exec"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy"],
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
