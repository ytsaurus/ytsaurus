from yt_chaos_test_base import ChaosTestBase
from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE, CHAOS_NODES_SERVICE, RPC_PROXIES_SERVICE
from yt_commands import (
    authors, create_tablet_cell_bundle, print_debug, build_master_snapshots, sync_create_cells, wait_for_cells,
    ls, get, set, retry, create, wait, write_table, remove, exists, create_dynamic_table, sync_mount_table,
    start_transaction, commit_transaction, insert_rows, delete_rows, lock_rows, build_snapshot, abort_transaction)

from yt_helpers import profiler_factory, master_exit_read_only_sync

from original_tests.yt.yt.tests.integration.master.test_master_snapshots \
    import MASTER_SNAPSHOT_COMPATIBILITY_CHECKER_LIST

import yatest.common

from yt.common import YtError

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

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "cell_hydra_persistence_synchronizer": {
                "use_hydra_persistence_directory": False,
                "migrate_to_virtual_cell_maps": False,
            },
        }
    }

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
                if not profiler.histogram("chunk_server/histograms/chunk_creation_time_histogram").get_bins():
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
            print_debug(f"Actual:   {histogram}")
            print_debug(f"Expected: {true_histogram}")
            print_debug(f"Verbose:  {verbose_true_histogram}")

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

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,
            "peer_revocation_timeout": 3000,
            "cell_hydra_persistence_synchronizer": {
                "use_hydra_persistence_directory": False,
                "migrate_to_virtual_cell_maps": False,
            },
        }
    }

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

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("build_tablet_snapshots", [True, False])
    @pytest.mark.parametrize("modification", ["insert", "delete", "lock_shared_strong", "lock_exclusive"])
    @pytest.mark.parametrize("commit_wrt_restart", ["before", "after"])
    def test_transactions(self, build_tablet_snapshots, modification, commit_wrt_restart):
        cell_ids = sync_create_cells(1)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64", "lock": "value_lock"}
        ]

        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        tx = start_transaction(type="tablet")

        m_tx = start_transaction(type="tablet")

        if modification == "insert":
            insert_rows("//tmp/t", [{"key": 0, "value": 0}], tx=m_tx)
        elif modification == "delete":
            delete_rows("//tmp/t", [{"key": 0}], tx=m_tx)
        elif modification == "lock_shared_strong":
            lock_rows("//tmp/t", [{"key": 0}], locks=["value_lock"], lock_type="shared_strong", tx=m_tx)
        elif modification == "lock_exclusive":
            lock_rows("//tmp/t", [{"key": 0}], locks=["value_lock"], lock_type="exclusive", tx=m_tx)
        else:
            assert False

        if commit_wrt_restart == "before":
            commit_transaction(m_tx)

        if build_tablet_snapshots:
            build_snapshot(cell_id=cell_ids[0])

        self.restart_with_update(NODES_SERVICE, build_snapshots=False)
        wait_for_cells(cell_ids)

        if commit_wrt_restart == "after":
            commit_transaction(m_tx)

        insert_rows("//tmp/t", [{"key": 0, "value": 0}], tx=tx)

        expected_error = (
            "Row lock conflict" if modification != "lock_shared_strong"
            else "Write failed due to concurrent read lock"
        )

        with pytest.raises(YtError, match=expected_error):
            commit_transaction(tx)


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


##################################################################


class CellsHydraPersistenceMigrationBase(ChaosTestBase, MasterSnapshotsCompatibilityBase):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master"],
        "trunk": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000,
        }
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,
            "peer_revocation_timeout": 3000,
            "max_snapshot_count_to_keep": 3,
            "tablet_cells_cleanup_period": 1000,
            "cell_hydra_persistence_synchronizer": {
                "use_hydra_persistence_directory": False,
                "migrate_to_virtual_cell_maps": False,
            },
        }
    }

    def execute_cells_hydra_presistence_migration(self, chaos):
        cell_id = None
        if chaos:
            cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
            set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        else:
            cell_id = sync_create_cells(1)[0]

        map_name = "{0}_cells".format("chaos" if chaos else "tablet")
        peer_segment = "/0" if chaos else ""
        prmiary_path = f"//sys/hydra_persistence/{map_name}/{cell_id}{peer_segment}/snapshots"
        secondary_path = f"//sys/{map_name}/{cell_id}{peer_segment}/snapshots"

        self.restart_with_update(MASTERS_SERVICE)

        snapshot_count = get("//sys/@config/tablet_manager/max_snapshot_count_to_keep")
        snapshot_id = None
        for _ in range(snapshot_count):
            snapshot_id = build_snapshot(cell_id=cell_id)
        assert len(ls(secondary_path)) == snapshot_count

        # Phase one
        set("//sys/@config/tablet_manager/cell_hydra_persistence_synchronizer/use_hydra_persistence_directory", True)
        wait(lambda: get(f"#{cell_id}/@registered_in_cypress"), sleep_backoff=1.0)

        for index in range(1, snapshot_count + 1):
            assert snapshot_id + index == build_snapshot(cell_id=cell_id)
            wait(lambda: len(ls(secondary_path)) == snapshot_count - index
                 and len(ls(prmiary_path)) == index)

        # Phase two
        set("//sys/@config/tablet_manager/cell_hydra_persistence_synchronizer/migrate_to_virtual_cell_maps", True)

        with Restarter(self.Env, [CHAOS_NODES_SERVICE, NODES_SERVICE]):
            tx = get(f"#{cell_id}/@peers/0/prerequisite_transaction") if chaos \
                else get(f"#{cell_id}/@prerequisite_transaction_id")
            abort_transaction(tx)
            remove("//sys/tablet_cells", force=True)
            remove("//sys/chaos_cells", force=True)
            create("virtual_tablet_cell_map", "//sys/tablet_cells", ignore_existing=True)
            create("virtual_chaos_cell_map", "//sys/chaos_cells", ignore_existing=True)

        assert ls(f"//sys/{map_name}") == [cell_id]
        assert len(ls(secondary_path)) == snapshot_count


class TestTabletCellsHydraPersistenceMigration(CellsHydraPersistenceMigrationBase):
    @authors("danilalexeev")
    def test(self):
        self.execute_cells_hydra_presistence_migration(False)


class TestChaosCellsHydraPersistenceMigration(CellsHydraPersistenceMigrationBase):
    @authors("danilalexeev")
    def test(self):
        self.execute_cells_hydra_presistence_migration(True)
