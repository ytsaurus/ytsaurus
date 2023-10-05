from yt_chaos_test_base import ChaosTestBase
from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE, RPC_PROXIES_SERVICE
from yt_commands import (
    authors, create_tablet_cell_bundle, print_debug, build_master_snapshots, sync_create_cells, wait_for_cells,
    ls, get, set, retry, create, wait, write_table, remove, exists)

from yt_helpers import profiler_factory, master_exit_read_only_sync

from original_tests.yt.yt.tests.integration.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import yatest.common

import datetime

from time import sleep

import os
import pytest

##################################################################


class MasterSnapshotsCompatibilityBase(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    NUM_NODES = 5
    USE_DYNAMIC_TABLES = True
    TEST_LOCATION_AWARE_REPLICATOR = True

    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy"],
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

        master_exit_read_only_sync()


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


def check_chunk_creation_time_histogram():
    create("table", "//tmp/chunk_creation_time")
    write_table("//tmp/chunk_creation_time", {"a": "b"})
    write_table("<append=%true>//tmp/chunk_creation_time", {"a": "b"})

    yield

    set("//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/chunk_scan_period", 250)
    set("//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/max_skipped_chunks_per_scan", 10)
    set("//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/max_visited_chunk_lists_per_scan", 500)

    def check_histogram():
        def parse_time(t):
            return datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000

        bounds = sorted(map(parse_time, get("//sys/@config/chunk_manager/master_cell_chunk_statistics_collector/creation_time_histogram_bucket_bounds")))
        chunks = ls("//sys/chunks", attributes=["estimated_creation_time", "type"])
        chunks = [chunk for chunk in chunks if chunk.attributes["type"] != "journal_chunk"]

        master_addresses = [ls("//sys/primary_masters")[0]]

        secondary_cell_tags = ls("//sys/secondary_masters")
        for cell_tag in secondary_cell_tags:
            master_addresses.append((cell_tag, ls(f"//sys/secondary_masters/{cell_tag}")[0]))

        profilers = [profiler_factory().at_primary_master(master_addresses[0])] + [
            profiler_factory().at_secondary_master(cell_tag, address)
            for cell_tag, address in master_addresses[1:]
        ]

        def profilers_ready():
            for profiler in profilers:
                if not profilers[0].histogram("chunk_server/histograms/chunk_creation_time_histogram").get_bins():
                    return False
            return True
        wait(profilers_ready)

        histogram = [
            bin["count"]
            for bin in profilers[0].histogram("chunk_server/histograms/chunk_creation_time_histogram").get_bins()
        ]

        for profiler in profilers[1:]:
            for i, bin in enumerate(profiler.histogram("chunk_server/histograms/chunk_creation_time_histogram").get_bins()):
                histogram[i] += bin["count"]

        true_histogram = [0] * (len(bounds) + 1)
        verbose_true_histogram = [[] for _ in range(len(bounds) + 1)]
        for chunk in chunks:
            creation_time = parse_time(chunk.attributes["estimated_creation_time"]["min"])
            bin_index = 0
            while bin_index < len(bounds) and creation_time >= bounds[bin_index]:
                bin_index += 1
            true_histogram[bin_index] += 1
            verbose_true_histogram[bin_index].append(str(chunk))

        if histogram != true_histogram:
            print_debug(f"actual:   {histogram}")
            print_debug(f"expected: {true_histogram}")
            print_debug(f"verbose:  {verbose_true_histogram}")

        return histogram == true_histogram

    wait(check_histogram)


def do_check_proxy_maintenance_requests(test_suite):
    http = ls("//sys/http_proxies")[0]
    set(f"//sys/http_proxies/{http}/@banned", True)

    rpc = ls("//sys/rpc_proxies")[0]
    set(f"//sys/rpc_proxies/{rpc}/@banned", True)

    yield

    wait(lambda: get(f"//sys/http_proxies/{http}/@type") == "cluster_proxy_node")
    maintenance_requests = list(get(f"//sys/http_proxies/{http}/@maintenance_requests").values())
    assert len(maintenance_requests) == 1
    assert maintenance_requests[0]["type"] == "ban"
    assert get(f"//sys/http_proxies/{http}/@banned")

    with Restarter(test_suite.Env, RPC_PROXIES_SERVICE):
        for rpc in ls("//sys/rpc_proxies"):
            remove(f"//sys/rpc_proxies/{rpc}")

    wait(lambda: exists(f"//sys/rpc_proxies/{rpc}"))
    assert get(f"//sys/rpc_proxies/{rpc}/@type") == "cluster_proxy_node"


class TestMasterSnapshotsCompatibility(ChaosTestBase, MasterSnapshotsCompatibilityBase):
    TEST_MAINTENANCE_FLAGS = True
    NUM_RPC_PROXIES = 2
    ENABLE_RPC_PROXY = True
    NUM_HTTP_PROXIES = 2
    ENABLE_HTTP_PROXY = True

    @authors("gritukan", "kvk1920")
    @pytest.mark.timeout(150)
    def test(self):
        # Preparation cell bundle for chaos table.
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        CHECKER_LIST = [
            check_maintenance_flags,
            check_chunk_creation_time_histogram,
        ] + MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

        checker_state_list = [iter(c()) for c in CHECKER_LIST] + [
            iter(do_check_proxy_maintenance_requests(self))
        ]
        for s in checker_state_list:
            next(s)

        self.restart_with_update(MASTERS_SERVICE)

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)


##################################################################


class TestTabletCellsSnapshotsCompatibility(MasterSnapshotsCompatibilityBase):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "tools", "exec"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy"],
    }

    @authors("aleksandra-zh")
    def test(self):
        cell_ids = sync_create_cells(1)

        self.restart_with_update(NODES_SERVICE, build_snapshots=False)

        wait_for_cells(cell_ids)


class TestBundleControllerAttribute(MasterSnapshotsCompatibilityBase):
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

        sleep(0.5)

        retry(lambda: self.restart_with_update(MASTERS_SERVICE))

        assert bundle_controller_config == get(config_path)
