from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE
from yt_commands import (
    authors, create_tablet_cell_bundle, print_debug, build_master_snapshots, sync_create_cells, wait_for_cells,
    ls, get, set, retry, start_transaction, create, wait, write_table, get_driver, lock)

from yt_helpers import profiler_factory

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
        "23_1": ["master", "node", "exec", "tools"],
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


class TestMasterSnapshotsCompatibility(MasterSnapshotsCompatibilityBase):
    TEST_MAINTENANCE_FLAGS = True

    @authors("gritukan", "kvk1920")
    @pytest.mark.timeout(150)
    def test(self):
        CHECKER_LIST = [
            check_maintenance_flags,
            check_chunk_creation_time_histogram,
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
        "23_1": ["master", "node", "tools", "exec"],
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


##################################################################


class TestSchemaMigration(MasterSnapshotsCompatibilityBase):
    def _check_table_schemas(self):
        def make_externalized_transaction_id(tx, cell_tag):
            parts = list(str(tx).split("-"))
            tmp = "0000" + parts[3][-4:]
            parts[3] = parts[2][:-4] + tmp[-4:]
            parts[2] = f"{cell_tag:0x}" + "0005"
            return "-".join(parts)

        table_path = "//tmp/empty_schema_holder_primary1"
        assert self.empty_schema_primary_id == get("{}/@schema_id".format(table_path))
        assert self.empty_schema_content == get("{}/@schema".format(table_path))

        table_path = "//tmp/empty_schema_holder_primary2"
        assert self.empty_schema_primary_id == get("{}/@schema_id".format(table_path))
        assert self.empty_schema_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.empty_schema_primary_id == get("#{}/@schema_id".format(table_id), driver=get_driver(1))
        assert self.empty_schema_content == get("#{}/@schema".format(table_id), driver=get_driver(1))

        table_path = "//tmp/empty_schema_holder_primary3"
        assert self.empty_schema_primary_id == get("{}/@schema_id".format(table_path))
        assert self.empty_schema_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.empty_schema_primary_id == get("#{}/@schema_id".format(table_id), driver=get_driver(2))
        assert self.empty_schema_content == get("#{}/@schema".format(table_id), driver=get_driver(2))

        table_path = "//tmp/schema1_holder_primary"
        assert self.schema1_primary_id == get("{}/@schema_id".format(table_path))
        assert self.schema1_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.schema1_primary_id == get("#{}/@schema_id".format(table_id), driver=get_driver(1))
        assert self.schema1_content == get("#{}/@schema".format(table_id), driver=get_driver(1))

        table_path = "//tmp/schema2_holder_primary"
        assert self.schema2_primary_id == get("{}/@schema_id".format(table_path))
        assert self.schema2_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.schema2_primary_id == get("#{}/@schema_id".format(table_id), driver=get_driver(2))
        assert self.schema2_content == get("#{}/@schema".format(table_id), driver=get_driver(2))

        table_path = "//tmp/schema4_holder_primary"
        assert self.schema4_primary_id == get("{}/@schema_id".format(table_path))
        assert self.schema4_content == get("{}/@schema".format(table_path))

        table_path = "//tmp/empty_schema_holder_primary3_tx"
        assert self.empty_schema_primary_id == get("{}/@schema_id".format(table_path), tx=self.tx)
        assert self.empty_schema_content == get("{}/@schema".format(table_path), tx=self.tx)
        table_id = get("{}/@id".format(table_path))
        assert self.empty_schema_primary_id == get("#{}/@schema_id".format(table_id), driver=get_driver(2), tx=self.tx)
        assert self.empty_schema_content == get("#{}/@schema".format(table_id), driver=get_driver(2), tx=self.tx)

        table_path = "//tmp/schema1_holder_primary_tx"
        assert self.schema1_primary_id == get("{}/@schema_id".format(table_path), tx=self.tx)
        assert self.schema1_content == get("{}/@schema".format(table_path), tx=self.tx)
        table_id = get("{}/@id".format(table_path))
        assert self.schema1_primary_id == get("#{}/@schema_id".format(table_id), driver=get_driver(1), tx=self.tx)
        assert self.schema1_content == get("#{}/@schema".format(table_id), driver=get_driver(1), tx=self.tx)

        table_path = "//tmp/portal_to_11/empty_schema_holder_secondary1"
        assert self.empty_schema_primary_id == get("{}/@schema_id".format(table_path))
        assert self.empty_schema_content == get("{}/@schema".format(table_path))

        table_path = "//tmp/portal_to_11/empty_schema_holder_secondary2"
        assert self.empty_schema_primary_id == get("{}/@schema_id".format(table_path))
        assert self.empty_schema_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.empty_schema_primary_id == get("#{}/@schema_id".format(table_id), driver=get_driver(2))
        assert self.empty_schema_content == get("#{}/@schema".format(table_id), driver=get_driver(2))

        table_path = "//tmp/portal_to_11/schema1_holder_secondary1"
        assert self.schema1_11_id == get("{}/@schema_id".format(table_path))
        assert self.schema1_content == get("{}/@schema".format(table_path))

        table_path = "//tmp/portal_to_11/schema1_holder_secondary2"
        assert self.schema1_11_id == get("{}/@schema_id".format(table_path))
        assert self.schema1_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.schema1_11_id == get("#{}/@schema_id".format(table_id), driver=get_driver(2))
        assert self.schema1_content == get("#{}/@schema".format(table_id), driver=get_driver(2))

        table_path = "//tmp/portal_to_11/schema1_holder_secondary3"
        assert self.schema1_11_id == get("{}/@schema_id".format(table_path))
        assert self.schema1_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.schema1_11_id == get("#{}/@schema_id".format(table_id), driver=get_driver(3))
        assert self.schema1_content == get("#{}/@schema".format(table_id), driver=get_driver(3))

        table_path = "//tmp/portal_to_11/schema3_holder_secondary"
        assert self.schema3_11_id == get("{}/@schema_id".format(table_path))
        assert self.schema3_content == get("{}/@schema".format(table_path))
        table_id = get("{}/@id".format(table_path))
        assert self.schema3_11_id == get("#{}/@schema_id".format(table_id), driver=get_driver(2))
        assert self.schema3_content == get("#{}/@schema".format(table_id), driver=get_driver(2))

        table_path = "//tmp/portal_to_11/schema1_holder_secondary3_tx"
        assert self.schema1_11_id == get("{}/@schema_id".format(table_path), tx=self.tx)
        assert self.schema1_content == get("{}/@schema".format(table_path), tx=self.tx)
        table_id = get("{}/@id".format(table_path))
        externalized_tx = make_externalized_transaction_id(self.tx, 11)
        assert self.schema1_11_id == get("#{}/@schema_id".format(table_id), driver=get_driver(3), tx=externalized_tx)
        assert self.schema1_content == get("#{}/@schema".format(table_id), driver=get_driver(3), tx=externalized_tx)

    @authors("h0pless")
    def test(self):
        create("portal_entrance", "//tmp/portal_to_11", attributes={"exit_cell_tag": 11})

        schema1 = [{"name": "id", "type": "int64"}]
        schema2 = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "int64"}]
        schema3 = [{"name": "whoami", "type": "string"}]

        schema4 = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "subkey", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"}]

        self.tx = start_transaction()

        # Primary cell native schemas
        create("table", "//tmp/empty_schema_holder_primary1", attributes={"external": False})
        create("table", "//tmp/empty_schema_holder_primary2", attributes={"external_cell_tag": 11})
        create("table", "//tmp/empty_schema_holder_primary3", attributes={"external_cell_tag": 12})
        create("table", "//tmp/schema1_holder_primary", attributes={"schema": schema1, "external_cell_tag": 11})
        create("table", "//tmp/schema2_holder_primary", attributes={"schema": schema2, "external_cell_tag": 12})
        create("table", "//tmp/schema4_holder_primary", attributes={"schema": schema4, "external": False})
        # Testing branched nodes
        create("table", "//tmp/empty_schema_holder_primary3_tx", attributes={"external_cell_tag": 12})
        create("table", "//tmp/schema1_holder_primary_tx", attributes={"schema": schema1, "external_cell_tag": 11})

        lock("//tmp/empty_schema_holder_primary3_tx", mode="snapshot", tx=self.tx)
        lock("//tmp/schema1_holder_primary_tx", mode="exclusive", tx=self.tx)

        # Secondary cell native schemas
        create("table", "//tmp/portal_to_11/empty_schema_holder_secondary1", attributes={"external": False})
        create("table", "//tmp/portal_to_11/empty_schema_holder_secondary2", attributes={"external_cell_tag": 12})
        create("table", "//tmp/portal_to_11/schema1_holder_secondary1", attributes={"schema": schema1, "external": False})
        create("table", "//tmp/portal_to_11/schema1_holder_secondary2", attributes={"schema": schema1, "external_cell_tag": 12})
        create("table", "//tmp/portal_to_11/schema1_holder_secondary3", attributes={"schema": schema1, "external_cell_tag": 13})
        create("table", "//tmp/portal_to_11/schema3_holder_secondary", attributes={"schema": schema3, "external_cell_tag": 12})
        # Testing branched nodes
        create("table", "//tmp/portal_to_11/schema1_holder_secondary3_tx", attributes={"schema": schema1, "external_cell_tag": 13})

        lock("//tmp/portal_to_11/schema1_holder_secondary3_tx", mode="shared", tx=self.tx)

        # Schema content
        self.empty_schema_content = get("//tmp/empty_schema_holder_primary1/@schema")
        self.schema1_content = get("//tmp/schema1_holder_primary/@schema")
        self.schema2_content = get("//tmp/schema2_holder_primary/@schema")
        self.schema3_content = get("//tmp/portal_to_11/schema3_holder_secondary/@schema")
        self.schema4_content = get("//tmp/schema4_holder_primary/@schema")

        # Ids on primary cell
        self.empty_schema_primary_id = get("//tmp/empty_schema_holder_primary1/@schema_id")
        self.schema1_primary_id = get("//tmp/schema1_holder_primary/@schema_id")
        self.schema2_primary_id = get("//tmp/schema2_holder_primary/@schema_id")
        self.schema4_primary_id = get("//tmp/schema4_holder_primary/@schema_id")

        # Ids on secondary native cell
        self.schema1_11_id = get("//tmp/portal_to_11/schema1_holder_secondary1/@schema_id")
        self.schema3_11_id = get("//tmp/portal_to_11/schema3_holder_secondary/@schema_id")

        self.restart_with_update(MASTERS_SERVICE)

        # To make sure mutations have enough time to apply before looking attributes up.
        sleep(1)

        self._check_table_schemas()

        self.restart_with_update(MASTERS_SERVICE)

        self._check_table_schemas()
