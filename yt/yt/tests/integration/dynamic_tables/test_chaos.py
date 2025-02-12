from yt_chaos_test_base import ChaosTestBase

from yt_env_setup import (
    Restarter,
    NODES_SERVICE,
    CHAOS_NODES_SERVICE,
    RPC_PROXIES_SERVICE,
)

from yt_commands import (
    authors, print_debug, wait, execute_command, get_driver, create_user, make_ace, check_permission,
    get, set, ls, create, exists, remove, copy, move, start_transaction, commit_transaction,
    sync_create_cells, sync_mount_table, sync_unmount_table, sync_flush_table,
    suspend_coordinator, resume_coordinator, reshard_table, alter_table, remount_table,
    insert_rows, delete_rows, lookup_rows, select_rows, pull_rows, trim_rows, lock_rows,
    create_replication_card, alter_table_replica, abort_transaction,
    build_snapshot, wait_for_cells, wait_for_chaos_cell, create_chaos_area,
    sync_create_chaos_cell, create_chaos_cell_bundle, generate_chaos_cell_id,
    align_chaos_cell_tag, migrate_replication_cards, alter_replication_card,
    get_in_sync_replicas, generate_timestamp, MaxTimestamp, raises_yt_error,
    create_table_replica, sync_enable_table_replica, get_tablet_infos, get_table_mount_info, set_node_banned,
    suspend_chaos_cells, resume_chaos_cells, merge, add_maintenance, remove_maintenance,
    sync_freeze_table, lock, get_tablet_errors, create_tablet_cell_bundle, create_area, link,
    execute_batch, make_batch_request)

from yt_type_helpers import make_schema

from yt.environment.helpers import assert_items_equal, are_items_equal
from yt.common import YtError, YtResponseError, WaitFailed

import yt.yson as yson

import yt_error_codes

from yt_driver_bindings import Driver

import pytest
import time
from copy import deepcopy
from itertools import zip_longest

import builtins

##################################################################

MAX_KEY = [yson.to_yson_type(None, attributes={"type": "max"})]


class TestChaos(ChaosTestBase):
    # TODO(nadya73): split this test suite.
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_REMOTE_CLUSTERS = 2
    NUM_TEST_PARTITIONS = 30
    NUM_SCHEDULERS = 1

    DELTA_DRIVER_CONFIG = {
        "enable_distributed_replication_collocation_attachment": True
    }

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "enable_distributed_replication_collocation_attachment": True
        }
    }

    def setup_method(self, method):
        super(TestChaos, self).setup_method(method)

        primary_cell_tag = get("//sys/@primary_cell_tag")
        for driver in self._get_drivers():
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", primary_cell_tag, driver=driver)

    @authors("savrus")
    def test_virtual_maps(self):
        tablet_cell_id = sync_create_cells(1)[0]
        tablet_bundle_id = get("//sys/tablet_cell_bundles/default/@id")

        chaos_bundle_ids = self._create_chaos_cell_bundle(name="default")

        assert tablet_bundle_id not in chaos_bundle_ids
        assert get("//sys/chaos_cell_bundles/default/@id") in chaos_bundle_ids

        chaos_cell_id = generate_chaos_cell_id()
        sync_create_chaos_cell("default", chaos_cell_id, self.get_cluster_names())

        assert_items_equal(get("//sys/chaos_cell_bundles"), ["default"])
        assert_items_equal(get("//sys/tablet_cell_bundles"), ["default", "sequoia"])
        assert_items_equal(get("//sys/tablet_cells"), [tablet_cell_id])
        assert_items_equal(get("//sys/chaos_cells"), [chaos_cell_id])

    @authors("savrus")
    def test_bundle_bad_options(self):
        params = {
            "type": "chaos_cell_bundle",
            "attributes": {
                "name": "chaos_bundle",
            }
        }
        with pytest.raises(YtError):
            execute_command("create", params)
        params["attributes"]["chaos_options"] = {"peers": [{"remote": False}]}
        with pytest.raises(YtError):
            execute_command("create", params)
        params["attributes"]["options"] = {
            "peer_count": 1,
            "changelog_account": "sys",
            "snapshot_account": "sys",
        }
        execute_command("create", params)

    @authors("babenko")
    def test_create_chaos_cell_with_duplicate_id(self):
        self._create_chaos_cell_bundle()

        cell_id = generate_chaos_cell_id()
        sync_create_chaos_cell("c", cell_id, self.get_cluster_names())

        with pytest.raises(YtError):
            sync_create_chaos_cell("c", cell_id, self.get_cluster_names())

    @authors("babenko")
    def test_create_chaos_cell_with_duplicate_tag(self):
        self._create_chaos_cell_bundle()

        cell_id = generate_chaos_cell_id()
        sync_create_chaos_cell("c", cell_id, self.get_cluster_names())

        cell_id_parts = cell_id.split("-")
        cell_id_parts[0], cell_id_parts[1] = cell_id_parts[1], cell_id_parts[0]
        another_cell_id = "-".join(cell_id_parts)

        with pytest.raises(YtError):
            sync_create_chaos_cell("c", another_cell_id, self.get_cluster_names())

    @authors("babenko")
    def test_create_chaos_cell_with_malformed_id(self):
        self._create_chaos_cell_bundle()
        with pytest.raises(YtError):
            sync_create_chaos_cell("c", "abcdabcd-fedcfedc-4204b1-12345678", self.get_cluster_names())

    @authors("savrus")
    def test_chaos_cells(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        def _check(peers, local_index=0):
            assert len(peers) == 3
            assert all(peer["address"] for peer in peers)
            assert sum("alien" in peer for peer in peers) == 2
            assert "alien" not in peers[local_index]

        _check(get("#{0}/@peers".format(cell_id)))

        for index, driver in enumerate(self._get_drivers()):
            _check(get("#{0}/@peers".format(cell_id), driver=driver), index)

    @authors("savrus")
    def test_remove_chaos_cell(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        remove("#{0}".format(cell_id))
        wait(lambda: not exists("#{0}".format(cell_id)))

        _, remote_driver0, _ = self._get_drivers()
        wait(lambda: not exists("#{0}/@peers/0/address".format(cell_id), driver=remote_driver0))

    @authors("savrus")
    def test_resurrect_chaos_cell(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        for driver in self._get_drivers():
            remove("#{0}".format(cell_id), driver=driver)
            wait(lambda: not exists("#{0}".format(cell_id), driver=driver))

        _, remote_driver0, _ = self._get_drivers()
        sync_unmount_table("//tmp/t")
        sync_unmount_table("//tmp/r0", driver=remote_driver0)

        peer_cluster_names = self.get_cluster_names()
        sync_create_chaos_cell("c", cell_id, peer_cluster_names)
        card_id = create_replication_card(chaos_cell_id=cell_id)
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, create_replica_tables=False, sync_replication_era=False)

        alter_table("//tmp/t", upstream_replica_id=replica_ids[0])
        alter_table("//tmp/r0", upstream_replica_id=replica_ids[1], driver=remote_driver0)
        sync_mount_table("//tmp/t")
        sync_mount_table("//tmp/r0", driver=remote_driver0)

        self._sync_replication_era(card_id, replicas)

        values = [{"key": 0, "value": "1"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

    @authors("savrus")
    def test_chaos_cell_update_acl(self):
        self._create_chaos_cell_bundle(name="chaos_bundle")
        create_user("u")
        set("//sys/chaos_cell_bundles/chaos_bundle/@options/snapshot_acl", [make_ace("allow", "u", "read")])
        cell_id = self._sync_create_chaos_cell(name="chaos_bundle")
        assert check_permission("u", "read", f"//sys/hydra_persistence/chaos_cells/{cell_id}/0/snapshots")["action"] == "allow"

    @authors("savrus")
    def test_replication_card(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        card_id = create_replication_card(chaos_cell_id=cell_id)
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "replica_path": "//tmp/r1"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, replication_card_id=card_id)

        card = get("#{0}/@".format(card_id))
        card_replicas = [{key: r[key] for key in list(replicas[0].keys())} for r in card["replicas"].values()]
        assert_items_equal(card_replicas, replicas)
        assert card["id"] == card_id
        assert card["type"] == "replication_card"
        card_replicas = [{key: r[key] for key in list(replicas[0].keys())} for r in card["replicas"].values()]
        assert_items_equal(card_replicas, replicas)

        assert get("#{0}/@id".format(card_id)) == card_id
        assert get("#{0}/@type".format(card_id)) == "replication_card"

        card_attributes = ls("#{0}/@".format(card_id))
        assert "id" in card_attributes
        assert "type" in card_attributes
        assert "era" in card_attributes
        assert "replicas" in card_attributes
        assert "coordinator_cell_ids" in card_attributes

        for replica_index, replica_id in enumerate(replica_ids):
            replica = get("#{0}/@".format(replica_id))
            assert replica["id"] == replica_id
            assert replica["type"] == "chaos_table_replica"
            assert replica["replication_card_id"] == card_id
            assert replica["cluster_name"] == replicas[replica_index]["cluster_name"]
            assert replica["replica_path"] == replicas[replica_index]["replica_path"]

        assert exists("#{0}".format(card_id))
        assert exists("#{0}".format(replica_id))
        assert exists("#{0}/@id".format(replica_id))
        assert not exists("#{0}/@nonexisting".format(replica_id))

        remove("#{0}".format(card_id))
        assert not exists("#{0}".format(card_id))

    @authors("savrus")
    def test_chaos_table(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(cell_id, replicas)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values)

    @authors("osidorkin")
    def test_read_write_chaos_table_via_symlink_to_replica(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(cell_id, replicas)
        link("//tmp/t", "//tmp/l")
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/l", values)

        assert lookup_rows("//tmp/l", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values)

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("mount_before_alter", [True, False])
    def test_invalid_alter_replication_progress(self, mount_before_alter):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(
            cell_id,
            replicas,
            mount_tables=mount_before_alter,
            sync_replication_era=mount_before_alter
        )

        if not mount_before_alter:
            sync_mount_table("//tmp/r0", driver=get_driver(cluster="remote_0"))

        if mount_before_alter:
            sync_unmount_table("//tmp/t")

        with pytest.raises(YtError, match="Replication progress should fully cover key space"):
            alter_table(
                "//tmp/t",
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": ["#<Max>"],
                },
            )

        with pytest.raises(YtError, match="Invalid replication progress"):
            alter_table(
                "//tmp/t",
                replication_progress={
                    "segments": [
                        {"lower_key": [], "timestamp": 0},
                        {"lower_key": ["2"], "timestamp": 0},
                        {"lower_key": ["1"], "timestamp": 0},
                    ],
                    "upper_key": MAX_KEY,
                },
            )

        alter_table(
            "//tmp/t",
            replication_progress={
                "segments": [{"lower_key": [], "timestamp": 1}],
                "upper_key": MAX_KEY,
            },
        )

        sync_mount_table("//tmp/t")
        self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=True)
        self._sync_replication_era(card_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

    @authors("savrus")
    def test_pull_rows(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": False, "replica_path": "//tmp/t"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(progress_timestamp):
            return pull_rows(
                "//tmp/q",
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": progress_timestamp}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_ids[0])

        def _sync_pull_rows(progress_timestamp):
            wait(lambda: len(_pull_rows(progress_timestamp)) == 1)
            result = _pull_rows(progress_timestamp)
            assert len(result) == 1
            return result[0]

        def _sync_check(progress_timestamp, expected_row):
            row = _sync_pull_rows(progress_timestamp)
            assert row["key"] == expected_row["key"]
            assert str(row["value"][0]) == expected_row["value"]
            return row.attributes["write_timestamps"][0]

        insert_rows("//tmp/q", [{"key": 0, "value": "0"}])
        timestamp1 = _sync_check(0, {"key": 0, "value": "0"})

        insert_rows("//tmp/q", [{"key": 1, "value": "1"}])
        timestamp2 = _sync_check(timestamp1, {"key": 1, "value": "1"})

        delete_rows("//tmp/q", [{"key": 0}])
        row = _sync_pull_rows(timestamp2)
        assert row["key"] == 0
        assert len(row.attributes["write_timestamps"]) == 0
        assert len(row.attributes["delete_timestamps"]) == 1

    @authors("savrus")
    def test_serialized_pull_rows(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": False, "replica_path": "//tmp/t"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)
        reshard_table("//tmp/q", [[], [1], [2], [3], [4]])
        sync_mount_table("//tmp/q")
        self._sync_replication_era(card_id, replicas[:1])

        for i in (1, 2, 0, 4, 3):
            insert_rows("//tmp/q", [{"key": i, "value": str(i)}])

        def _pull_rows():
            return pull_rows(
                "//tmp/q",
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_ids[0],
                order_rows_by_timestamp=True)

        wait(lambda: len(_pull_rows()) == 5)

        result = _pull_rows()
        timestamps = [row.attributes["write_timestamps"][0] for row in result]
        import logging
        logger = logging.getLogger()
        logger.debug("Write timestamps: {0}".format(timestamps))

        for i in range(len(timestamps) - 1):
            assert timestamps[i] < timestamps[i+1], "Write timestamp order mismatch for positions {0} and {1}: {2} > {3}".format(i, i+1, timestamps[i], timestamps[i+1])

    @authors("savrus")
    @pytest.mark.parametrize("reshard", [True, False])
    def test_advanced_pull_rows(self, reshard):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": False, "replica_path": "//tmp/t"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)
        if reshard:
            reshard_table("//tmp/q", [[], [1], [2]])
        sync_mount_table("//tmp/q")
        self._sync_replication_era(card_id, replicas[:1])

        for i in (0, 1, 2):
            insert_rows("//tmp/q", [{"key": i, "value": str(i)}])

        sync_unmount_table("//tmp/q")
        set("//tmp/q/@enable_replication_progress_advance_to_barrier", False)
        sync_mount_table("//tmp/q")

        timestamp = generate_timestamp()
        replication_progress = {
            "segments": [
                {"lower_key": [0], "timestamp": timestamp},
                {"lower_key": [1], "timestamp": 0},
                {"lower_key": [2], "timestamp": timestamp},
            ],
            "upper_key": MAX_KEY,
        }

        def _pull_rows(response_parameters=None):
            return pull_rows(
                "//tmp/q",
                replication_progress=replication_progress,
                upstream_replica_id=replica_ids[0],
                response_parameters=response_parameters)
        wait(lambda: len(_pull_rows()) == 1)

        response_parameters = {}
        rows = _pull_rows(response_parameters)
        print_debug(response_parameters)

        row = rows[0]
        assert row["key"] == 1
        assert str(row["value"][0]) == "1"
        assert len(row.attributes["write_timestamps"]) == 1
        assert row.attributes["write_timestamps"][0] > 0

        progress = response_parameters["replication_progress"]
        assert progress["segments"][0] == replication_progress["segments"][0]
        assert progress["segments"][2] == replication_progress["segments"][2]
        assert yson.dumps(progress["upper_key"]) == yson.dumps(replication_progress["upper_key"])
        assert progress["segments"][1]["timestamp"] >= row.attributes["write_timestamps"][0]

    @authors("osidorkin")
    def test_chaos_table_move_under_transaction(self):
        cb_name = "cb"
        table_name = "//tmp/chaos_table"
        table_name2 = "//tmp/chaos_table2"
        cell_id = self._sync_create_chaos_bundle_and_cell(name=cb_name)
        set(f"//sys/chaos_cell_bundles/{cb_name}/@metadata_cell_id", cell_id)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]

        create("chaos_replicated_table",
               table_name,
               attributes={"chaos_cell_bundle": cb_name, "schema": schema})

        tx = start_transaction()
        move(table_name, table_name2, tx=tx)
        commit_transaction(tx)

        assert exists(table_name2)

    @authors("osidorkin")
    def test_replication_via_async_queue(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/qs"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/qa"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/ts"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/ta"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/tae"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        create("chaos_replicated_table", "//tmp/crt", attributes={
            "replication_card_id": card_id,
            "chaos_cell_bundle": "c"
        })

        insert_rows("//tmp/ts", [{"key": 0, "value": "0"}])
        insert_rows("//tmp/ts", [{"key": 1, "value": "1"}])
        delete_rows("//tmp/ts", [{"key": 0}])
        delete_rows("//tmp/ts", [{"key": 1}])

        timestamp = generate_timestamp()
        wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[1]}/replication_lag_timestamp") > timestamp)

        assert lookup_rows("//tmp/ts", [{"key": 0}]) == []
        assert lookup_rows("//tmp/ts", [{"key": 1}]) == []

        def _pull_rows(path, replica_id, upper_timestamp=0, response_parameters=None):
            return pull_rows(
                path,
                upstream_replica_id=replica_id,
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upper_timestamp=upper_timestamp,
                response_parameters=response_parameters)

        sync_queue_rows = _pull_rows("//tmp/qs", replica_ids[0], timestamp, {})
        async_queue_rows = _pull_rows("//tmp/qa", replica_ids[1], timestamp, {})

        assert sync_queue_rows == async_queue_rows

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, mode="sync")
        self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=False)
        self._sync_alter_replica(card_id, replicas, replica_ids, 3, enabled=True)

        # Now we can only fetch from async replica
        timestamp = generate_timestamp()
        wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[3]}/replication_lag_timestamp") > timestamp)

        assert lookup_rows("//tmp/ta", [{"key": 0}]) == []
        assert lookup_rows("//tmp/ta", [{"key": 1}]) == []

    @authors("savrus")
    def test_end_replication_row_index(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": False, "replica_path": "//tmp/t"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(upper_timestamp=0, response_parameters=None):
            return pull_rows(
                "//tmp/q",
                upstream_replica_id=replica_ids[0],
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upper_timestamp=upper_timestamp,
                response_parameters=response_parameters)

        timestamp = generate_timestamp()
        insert_rows("//tmp/q", [{"key": 0, "value": "0"}])
        wait(lambda: len(_pull_rows()) == 1)

        def _check(timestamp, expected_row_count, expected_end_index, expected_progress):
            response_parameters = {}
            rows = _pull_rows(timestamp, response_parameters)
            print_debug(response_parameters)
            end_indexes = list(response_parameters["end_replication_row_indexes"].values())

            assert len(rows) == expected_row_count
            assert len(end_indexes) == 1
            assert end_indexes[0] == expected_end_index

            segments = response_parameters["replication_progress"]["segments"]
            assert len(segments) == 1
            if expected_progress:
                assert segments[0]["timestamp"] == expected_progress

        _check(0, 1, 1, None)
        _check(timestamp, 0, 0, timestamp)

    @authors("savrus")
    def test_delete_rows_replication(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        delete_rows("//tmp/t", [{"key": 0}])
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == [])

        rows = lookup_rows("//tmp/t", [{"key": 0}], versioned=True)
        row = rows[0]
        assert len(row.attributes["write_timestamps"]) == 1
        assert len(row.attributes["delete_timestamps"]) == 1

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replica_enable(self, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": mode, "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "disabled"
        assert orchid["write_mode"] == "pull"

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == []
        alter_table_replica(replica_ids[0], enabled=True)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)
        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "enabled"
        assert orchid["write_mode"] == "pull" if mode == "async" else "direct"

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replica_disable(self, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "enabled"
        assert orchid["write_mode"] == "pull" if mode == "async" else "direct"

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=False)

        values = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "disabled"
        assert orchid["write_mode"] == "pull"
        assert lookup_rows("//tmp/t", [{"key": 1}]) == []

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replica_mode_switch(self, mode):
        if mode == "sync":
            old_mode, new_mode, old_write_mode, new_write_mode = "sync", "async", "direct", "pull"
        else:
            old_mode, new_mode, old_write_mode, new_write_mode = "async", "sync", "pull", "direct"

        def _check_lookup(keys, values, mode):
            if mode == "sync":
                assert lookup_rows("//tmp/t", keys) == values
            else:
                assert lookup_rows("//tmp/t", keys) == []

        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": old_mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == old_mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "enabled"
        assert orchid["write_mode"] == old_write_mode

        values = [{"key": i, "value": str(i)} for i in range(2)]
        insert_rows("//tmp/t", values[:1])

        _check_lookup([{"key": 0}], values[:1], old_mode)
        self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode=new_mode)

        self._insistent_insert_rows("//tmp/t", values[1:])
        _check_lookup([{"key": 1}], values[1:], new_mode)

        orchid = self._get_table_orchids("//tmp/t")[0]
        replica_id = replica_ids[0]
        assert orchid["replication_card"]["replicas"][replica_id]["mode"] == new_mode
        assert orchid["replication_card"]["replicas"][replica_id]["state"] == "enabled"
        assert orchid["write_mode"] == new_write_mode

    @authors("savrus")
    def test_queue_replica_mode_stuck_in_cataclysm(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        alter_table_replica(replica_ids[1], mode="async")
        wait(lambda: get("#{0}/@coordinator_cell_ids".format(card_id)) == [])
        assert get("#{0}/@mode".format(replica_ids[1])) == "sync_to_async"

        with self.CellsDisabled(clusters=self.get_cluster_names(), chaos_bundles=["c"]):
            pass

        assert get("#{0}/@mode".format(replica_ids[1])) == "sync_to_async"
        assert get("#{0}/@coordinator_cell_ids".format(card_id)) == []

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, mode="sync")

        values = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == values

    @authors("savrus")
    def test_queue_replica_state_stuck_in_cataclysm(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        alter_table_replica(replica_ids[1], enabled=False)
        wait(lambda: get("#{0}/@coordinator_cell_ids".format(card_id)) == [])
        assert get("#{0}/@state".format(replica_ids[1])) == "disabling"

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)

        values = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == values

    @authors("savrus")
    def test_replication_progress(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        # Default replication factor is 3 and there are only 3 nodes in this configuration one of which is banned in the test.
        set(
            "//sys/tablet_cell_bundles/default/@options",
            {
                "snapshot_account": "sys",
                "changelog_account": "sys",
                "changelog_replication_factor": 2,
                "changelog_write_quorum": 2,
                "changelog_read_quorum": 1,
            },
        )

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=False)

        orchid = get("#{0}/orchid".format(tablet_id))
        progress = orchid["replication_progress"]

        sync_unmount_table("//tmp/t")
        assert get("#{0}/@replication_progress".format(tablet_id)) == progress

        sync_mount_table("//tmp/t")
        orchid = get("#{0}/orchid".format(tablet_id))
        assert orchid["replication_progress"] == progress

        cell_id = get("#{0}/@cell_id".format(tablet_id))
        build_snapshot(cell_id=cell_id)
        peer = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set_node_banned(peer, True)
        wait_for_cells([cell_id], decommissioned_addresses=[peer])

        orchid = get("#{0}/orchid".format(tablet_id))
        assert orchid["replication_progress"] == progress

    @authors("savrus")
    def test_async_queue_replica(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(replica_index, versioned=False):
            rows = pull_rows(
                replicas[replica_index]["replica_path"],
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster_name"]))
            if versioned:
                return rows
            return [{"key": row["key"], "value": str(row["value"][0])} for row in rows]

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/q", values)
        wait(lambda: _pull_rows(2) == values)
        wait(lambda: _pull_rows(1) == values)

        async_rows = _pull_rows(1, True)
        sync_rows = _pull_rows(2, True)
        assert async_rows == sync_rows

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_queue_replica_enable_disable(self, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": mode, "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(replica_index):
            rows = pull_rows(
                replicas[replica_index]["replica_path"],
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster_name"]))
            return [{"key": row["key"], "value": str(row["value"][0])} for row in rows]

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)
        wait(lambda: _pull_rows(2) == values0)
        wait(lambda: _pull_rows(1) == values0)

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=False)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        wait(lambda: _pull_rows(2) == values0 + values1)
        assert _pull_rows(1) == values0

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)

        wait(lambda: _pull_rows(1) == values0 + values1)

        values2 = [{"key": 2, "value": "2"}]
        insert_rows("//tmp/t", values2)
        values = values0 + values1 + values2
        wait(lambda: _pull_rows(2) == values)
        wait(lambda: _pull_rows(1) == values)

    @authors("osidorkin")
    def test_replica_move(self):
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t0"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t1"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/ta"}
        ]

        cell_id = self._sync_create_chaos_bundle_and_cell()
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        create("chaos_replicated_table", "//tmp/crt", attributes={
            "replication_card_id": card_id,
            "chaos_cell_bundle": "c"
        })

        values = [{"key": 0, "value": "0"}]
        insert_rows(replicas[0]["replica_path"], values)
        assert lookup_rows(replicas[1]["replica_path"], [{"key": 0}]) == values

        def _move_replica(new_replica_path: str, replica_index: int, driver):
            is_sync = replicas[replica_index]["mode"] == "sync"
            if is_sync:
                self._sync_alter_replica(card_id, replicas, replica_ids, replica_index, mode="async")

            sync_unmount_table(replicas[replica_index]["replica_path"], driver=driver)

            alter_table_replica(replica_ids[replica_index], replica_path=new_replica_path)
            move(replicas[replica_index]["replica_path"], new_replica_path, driver=driver)
            replicas[replica_index]["replica_path"] = new_replica_path

            sync_mount_table(replicas[replica_index]["replica_path"], driver=driver)
            if is_sync:
                self._sync_alter_replica(card_id, replicas, replica_ids, replica_index, mode="sync")

        primary_driver = get_driver(cluster="primary")
        remove_0_driver = get_driver(cluster="remote_0")

        # Moving data replica.
        _move_replica("//tmp/t2", 1, primary_driver)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows(replicas[0]["replica_path"], values1)
        assert lookup_rows(replicas[0]["replica_path"], [{"key": 1}]) == values1
        assert lookup_rows(replicas[1]["replica_path"], [{"key": 1}]) == values1
        assert lookup_rows(replicas[1]["replica_path"], [{"key": 0}]) == values

        # Duplicated paths in replication card are prohibited.
        with pytest.raises(YtResponseError):
            alter_table_replica(replica_ids[1], replica_path=replicas[0]["replica_path"])

        # Moving queue replicas.
        self._sync_alter_replica(card_id, replicas, replica_ids, 3, mode="sync")
        _move_replica("//tmp/r2", 2, remove_0_driver)
        self._sync_alter_replica(card_id, replicas, replica_ids, 3, mode="async")
        _move_replica("//tmp/r3", 3, remove_0_driver)

        values2 = [{"key": 2, "value": "2"}]
        insert_rows(replicas[0]["replica_path"], values2)
        timestamp = generate_timestamp()

        assert lookup_rows(replicas[0]["replica_path"], [{"key": 2}]) == values2
        assert lookup_rows(replicas[1]["replica_path"], [{"key": 2}]) == values2
        assert lookup_rows(replicas[0]["replica_path"], [{"key": 1}]) == values1
        assert lookup_rows(replicas[1]["replica_path"], [{"key": 1}]) == values1
        assert lookup_rows(replicas[1]["replica_path"], [{"key": 0}]) == values

        wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[4]}/replication_lag_timestamp") > timestamp)
        assert lookup_rows(replicas[4]["replica_path"], [{"key": 2}]) == values2
        assert lookup_rows(replicas[4]["replica_path"], [{"key": 1}]) == values1
        assert lookup_rows(replicas[4]["replica_path"], [{"key": 0}]) == values

    @authors("savrus")
    @pytest.mark.parametrize("content", ["data", "queue", "both"])
    def test_resharded_replication(self, content):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)

        if content in ["data", "both"]:
            reshard_table("//tmp/t", [[], [1]])
        if content in ["queue", "both"]:
            reshard_table("//tmp/q", [[], [1]], driver=get_driver(cluster="remote_0"))

        for replica in replicas:
            sync_mount_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
        self._sync_replication_era(card_id, replicas)

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        wait(lambda: lookup_rows("//tmp/t", keys) == rows)

        rows = [{"key": i, "value": str(i+2)} for i in range(2)]
        for i in reversed(list(range(2))):
            insert_rows("//tmp/t", [rows[i]])
        wait(lambda: lookup_rows("//tmp/t", keys) == rows)

    @authors("savrus")
    def test_resharded_queue_pull(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q1"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)

        reshard_table("//tmp/q0", [[], [1]], driver=get_driver(cluster="remote_0"))

        for replica in replicas:
            sync_mount_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
        self._sync_replication_era(card_id, replicas)

        def _pull_rows(replica_index, timestamp):
            rows = pull_rows(
                replicas[replica_index]["replica_path"],
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": timestamp}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster_name"]))
            return [{"key": row["key"], "value": str(row["value"][0])} for row in rows]

        values0 = [{"key": i, "value": str(i)} for i in range(2)]
        insert_rows("//tmp/t", values0)
        wait(lambda: _pull_rows(1, 0) == values0)
        wait(lambda: _pull_rows(2, 0) == values0)

        timestamp = generate_timestamp()
        values1 = [{"key": i, "value": str(i+10)} for i in range(2)]
        insert_rows("//tmp/t", values1)
        wait(lambda: _pull_rows(1, timestamp) == values1)
        wait(lambda: _pull_rows(2, timestamp) == values1)

    @authors("ponasenko-rs")
    def test_reshard_ordered_on_at_a_time(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        remote_driver0 = get_driver(cluster="remote_0")

        card_id, _ = self._create_chaos_tables(cell_id, replicas, ordered=True)

        sync_unmount_table("//tmp/t")
        reshard_table("//tmp/t", 2)
        sync_mount_table("//tmp/t")

        def _pull_failed():
            tablet_infos = get_tablet_infos("//tmp/t", [1], request_errors=True)
            errors = tablet_infos["tablets"][0]["tablet_errors"]

            if len(errors) != 1 or errors[0]["attributes"]["background_activity"] != "pull":
                return False

            return any(
                error["message"].startswith("Target queue has no corresponding tablet")
                for error in errors[0]["inner_errors"]
            )

        wait(_pull_failed)

        sync_unmount_table("//tmp/q", driver=remote_driver0)
        reshard_table("//tmp/q", 2, driver=remote_driver0)
        sync_mount_table("//tmp/q", driver=remote_driver0)

        self._sync_replication_card(card_id)

        tablet_count = 2
        values = [{"$tablet_index": j, "key": i, "value": str(i + j)} for i in range(2) for j in range(tablet_count)]
        data_values = [[{"key": i, "value": str(i + j)} for i in range(2)] for j in range(tablet_count)]
        insert_rows("//tmp/t", values)

        for j in range(tablet_count):
            wait(lambda: select_rows(f"key, value from [//tmp/t] where [$tablet_index] = {j}") == data_values[j])
            assert select_rows(f"key, value from [//tmp/q] where [$tablet_index] = {j}", driver=remote_driver0) == data_values[j]

        def _no_pull_errors():
            def _no_tablet_errors(table, driver=None):
                errors = get_tablet_errors(table, driver=driver)
                return len(errors["tablet_errors"]) == 0

            return _no_tablet_errors("//tmp/t") and _no_tablet_errors("//tmp/q", driver=remote_driver0)

        wait(_no_pull_errors)

    @authors("babenko")
    def test_chaos_replicated_table_requires_valid_card_id(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        with pytest.raises(YtError, match="Malformed replication card id"):
            create(
                "chaos_replicated_table",
                "//tmp/crt",
                attributes={"chaos_cell_bundle": "c", "replication_card_id": "1-2-3-4"}
            )

    @authors("savrus")
    def test_chaos_replicated_table_requires_bundle_use(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        card_id = create_replication_card(chaos_cell_id=cell_id)
        create_user("u")

        def _create(path, card_id=None):
            attributes = {"chaos_cell_bundle": "c"}
            if card_id:
                attributes["replication_card_id"] = card_id
            create("chaos_replicated_table", path, authenticated_user="u", attributes=attributes)

        with pytest.raises(YtError):
            _create("//tmp/crt", card_id)

        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)
        with pytest.raises(YtError):
            _create("//tmp/crt")

        set("//sys/chaos_cell_bundles/c/@acl/end", make_ace("allow", "u", "use"))
        _create("//tmp/crt", card_id)
        _create("//tmp/crt2")

    @authors("babenko")
    def test_chaos_replicated_table_with_explicit_card_id(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        card_id = create_replication_card(chaos_cell_id=cell_id)

        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "c",
            "replication_card_id": card_id
        })
        assert get("//tmp/crt/@type") == "chaos_replicated_table"
        assert get("//tmp/crt/@replication_card_id") == card_id
        assert get("//tmp/crt/@owns_replication_card")

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, replication_card_id=card_id)
        assert len(replica_ids) == 2

        wait(lambda: get("#{0}/@era".format(card_id)) == 1)

        assert get("//tmp/crt/@era") == get("#{0}/@era".format(card_id))
        wait(lambda: get("//tmp/crt/@coordinator_cell_ids") == get("#{0}/@coordinator_cell_ids".format(card_id)))
        card = get("#{0}/@".format(card_id))

        crt_replicas = get("//tmp/crt/@replicas")
        assert len(crt_replicas) == 2

        def _assert_replicas_equal(lhs, rhs):
            return all(lhs[key] == rhs[key] for key in ("cluster_name", "replica_path", "mode", "state", "content_type"))

        for replica_id in replica_ids:
            replica = card["replicas"][replica_id]
            del replica["history"]
            del replica["replication_progress"]
            _assert_replicas_equal(replica, crt_replicas[replica_id])

    @authors("babenko")
    def test_chaos_replicated_table_replication_card_ownership(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        card_id = create_replication_card(chaos_cell_id=cell_id)

        create("chaos_replicated_table", "//tmp/crt", attributes={
            "replication_card_id": card_id,
            "chaos_cell_bundle": "c"
        })
        create("chaos_replicated_table", "//tmp/crt_view", attributes={
            "replication_card_id": card_id,
            "owns_replication_card": False,
            "chaos_cell_bundle": "c"
        })

        assert get("//tmp/crt/@owns_replication_card")
        assert not get("//tmp/crt_view/@owns_replication_card")

        assert exists("#{0}".format(card_id))
        remove("//tmp/crt_view")
        time.sleep(1)
        assert exists("#{0}".format(card_id))

        tx = start_transaction()
        set("//tmp/crt/@attr", "value", tx=tx)
        commit_transaction(tx)
        time.sleep(1)
        assert exists("#{0}".format(card_id))

        remove("//tmp/crt")
        wait(lambda: not exists("#{0}".format(card_id)))

    @authors("savrus")
    @pytest.mark.parametrize("all_keys", [True, False])
    def test_get_in_sync_replicas(self, all_keys):
        def _get_in_sync_replicas(path):
            if all_keys:
                return get_in_sync_replicas(path, [], all_keys=True, timestamp=MaxTimestamp)
            else:
                return get_in_sync_replicas(path, [{"key": 0}], timestamp=MaxTimestamp)

        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])

        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "chaos_bundle",
            "schema": schema,
        })

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, schema=schema)
        card_id = get("//tmp/crt/@replication_card_id")

        assert len(_get_in_sync_replicas("//tmp/t")) == 0
        assert len(_get_in_sync_replicas("//tmp/crt")) == 0

        self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode="sync")

        assert len(_get_in_sync_replicas("//tmp/t")) == 1
        assert len(_get_in_sync_replicas("//tmp/crt")) == 1

        self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=False)

        rows = [{"key": 0, "value": "0"}]
        keys = [{"key": 0}]
        insert_rows("//tmp/crt", rows)

        sync_unmount_table("//tmp/t")
        alter_table_replica(replica_ids[0], enabled=True)

        assert len(_get_in_sync_replicas("//tmp/t")) == 0
        assert len(_get_in_sync_replicas("//tmp/crt")) == 0

        sync_mount_table("//tmp/t")
        wait(lambda: lookup_rows("//tmp/t", keys) == rows)

        def _check_progress():
            card = get("#{0}/@".format(card_id))
            replica = card["replicas"][replica_ids[0]]
            if replica["state"] != "enabled":
                return False
            if replica["replication_progress"]["segments"][0]["timestamp"] < replica["history"][-1]["timestamp"]:
                return False
            return True

        wait(_check_progress)

        assert len(_get_in_sync_replicas("//tmp/t")) == 1
        assert len(_get_in_sync_replicas("//tmp/crt")) == 1

    @authors("savrus")
    def test_async_get_in_sync_replicas(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        sync_unmount_table("//tmp/t")
        reshard_table("//tmp/t", [[], [1]])

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t", rows)
        timestamp0 = generate_timestamp()

        def _check(keys, expected):
            sync_replicas = get_in_sync_replicas("//tmp/t", keys, timestamp=timestamp0)
            assert len(sync_replicas) == expected

        _check(keys[:1], 0)
        _check(keys[1:], 0)
        _check(keys, 0)

        def _check_progress(segment_index=0):
            card = get("#{0}/@".format(card_id))
            replica = card["replicas"][replica_ids[0]]
            # Segment index may not be strict since segments with the same timestamp are glued into one.
            segment_index = min(segment_index, len(replica["replication_progress"]["segments"]) - 1)

            if replica["state"] != "enabled":
                return False
            if replica["replication_progress"]["segments"][segment_index]["timestamp"] < timestamp0:
                return False
            return True

        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)
        wait(lambda: lookup_rows("//tmp/t", keys[:1]) == rows[:1])
        wait(lambda: _check_progress(0))

        _check(keys[:1], 1)
        _check(keys[1:], 0)
        _check(keys, 0)

        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        wait(lambda: lookup_rows("//tmp/t", keys[1:]) == rows[1:])
        wait(lambda: _check_progress(1))

        _check(keys[:1], 1)
        _check(keys[1:], 1)
        _check(keys, 1)

    @authors("babenko")
    def test_replication_card_attributes(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        TABLE_ID = "1-2-4b6-3"
        TABLE_PATH = "//path/to/table"
        TABLE_CLUSTER_NAME = "remote_0"

        card_id = create_replication_card(chaos_cell_id=cell_id, attributes={
            "table_id": TABLE_ID,
            "table_path": TABLE_PATH,
            "table_cluster_name": TABLE_CLUSTER_NAME
        })

        attributes = get("#{0}/@".format(card_id))
        assert attributes["table_id"] == TABLE_ID
        assert attributes["table_path"] == TABLE_PATH
        assert attributes["table_cluster_name"] == TABLE_CLUSTER_NAME

    @authors("savrus")
    def test_metadata_chaos_cell(self):
        self._create_chaos_cell_bundle(name="c1")
        self._create_chaos_cell_bundle(name="c2")

        assert not exists("//sys/chaos_cell_bundles/c1/@metadata_cell_id")

        with pytest.raises(YtError, match="No chaos cell with id .* is known"):
            set("//sys/chaos_cell_bundles/c1/@metadata_cell_id", "1-2-3-4")

        cell_id1 = self._sync_create_chaos_cell(name="c1")
        set("//sys/chaos_cell_bundles/c1/@metadata_cell_id", cell_id1)
        assert get("#{0}/@ref_counter".format(cell_id1)) == 1

        cell_id2 = self._sync_create_chaos_cell(name="c1")
        set("//sys/chaos_cell_bundles/c1/@metadata_cell_id", cell_id2)
        assert get("#{0}/@ref_counter".format(cell_id2)) == 1

        remove("//sys/chaos_cell_bundles/c1/@metadata_cell_id")
        assert not exists("//sys/chaos_cell_bundles/c1/@metadata_cell_id")

        cell_id3 = self._sync_create_chaos_cell(name="c2")
        with pytest.raises(YtError, match="Cell .* belongs to a different bundle"):
            set("//sys/chaos_cell_bundles/c1/@metadata_cell_id", cell_id3)

    @authors("babenko")
    def test_chaos_replicated_table_failure(self):
        with pytest.raises(YtError, match=".* is neither specified nor inherited.*"):
            create("chaos_replicated_table", "//tmp/crt")

        self._create_chaos_cell_bundle(name="chaos_bundle")
        with pytest.raises(YtError, match="Chaos cell bundle .* has no associated metadata chaos cell"):
            create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle"})

    @authors("babenko")
    def test_chaos_replicated_table_with_implicit_card_id(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        table_id = create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle"})
        assert get("//tmp/crt/@type") == "chaos_replicated_table"
        assert get("//tmp/crt/@owns_replication_card")

        card_id = get("//tmp/crt/@replication_card_id")
        assert exists("#{0}".format(card_id))

        card = get("#{0}/@".format(card_id))
        assert card["table_id"] == table_id
        assert card["table_path"] == "//tmp/crt"
        assert card["table_cluster_name"] == "primary"

        remove("//tmp/crt")
        wait(lambda: not exists("#{0}".format(card_id)))

    @authors("babenko")
    def test_create_chaos_table_replica_for_chaos_replicated_table(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        card_id = create_replication_card(chaos_cell_id=cell_id)

        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "c",
            "replication_card_id": card_id
        })

        replica = {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "replica_path": "//tmp/r0"}
        replica_id = self._create_chaos_table_replica(replica, table_path="//tmp/crt")

        attributes = get("#{0}/@".format(replica_id))
        assert attributes["type"] == "chaos_table_replica"
        assert attributes["id"] == replica_id
        assert attributes["replication_card_id"] == card_id
        assert attributes["replica_path"] == "//tmp/r0"
        assert attributes["cluster_name"] == "remote_0"

    @authors("h0pless")
    def test_chaos_replicated_table_node_type_handler(self):
        def branch_my_table():
            tx = start_transaction()
            lock("//tmp/table", mode="exclusive", tx=tx)
            assert expected_new_schema == get("//tmp/table/@schema", tx=tx)
            return tx

        def alter_my_table(tx):
            alter_table("//tmp/table", schema=original_schema, tx=tx)
            assert expected_original_schema == get("//tmp/table/@schema", tx=tx)

        original_schema = make_schema(
            [
                {"name": "headline", "type": "string"},
                {"name": "something_something_pineapple", "type": "string"},
                {"name": "coolness_factor", "type": "int64"},
            ]
        )
        create("table", "//tmp/original_schema_holder", attributes={"schema": original_schema, "external": False})
        expected_original_schema = get("//tmp/original_schema_holder/@schema")

        new_schema = make_schema(
            [
                {"name": "weird_id", "type": "int64"},
                {"name": "something_something_pomegranate", "type": "string"},
                {"name": "normal_id", "type": "int64"},
            ]
        )
        create("table", "//tmp/new_schema_holder", attributes={"schema": new_schema, "external": False})
        expected_new_schema = get("//tmp/new_schema_holder/@schema")

        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        create("chaos_replicated_table", "//tmp/table", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": original_schema})

        # Test that alter still works
        alter_table("//tmp/table", schema=new_schema)
        assert expected_new_schema == get("//tmp/table/@schema")

        # Check that nothing changes from branching
        tx = branch_my_table()
        commit_transaction(tx)
        assert expected_new_schema == get("//tmp/table/@schema")

        # Check that nothing changes if transaction is aborted
        tx = branch_my_table()
        alter_my_table(tx)
        abort_transaction(tx)
        assert expected_new_schema == get("//tmp/table/@schema")

        # Check that changes made inside a transaction actually apply
        tx = branch_my_table()
        alter_my_table(tx)
        commit_transaction(tx)
        assert expected_original_schema == get("//tmp/table/@schema")

        # Cross shard copy if portals are enabled
        if self.ENABLE_TMP_PORTAL:
            create("portal_entrance", "//non_tmp", attributes={"exit_cell_tag": 12})
            copy("//tmp/table", "//non_tmp/table_copy")
            assert get("//tmp/table/@schema") == get("//non_tmp/table_copy/@schema")

    @authors("savrus")
    def test_chaos_table_alter(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

        def _validate_schema(schema):
            actual_schema = get("//tmp/crt/@schema")
            assert actual_schema.attributes["unique_keys"]
            for column, actual_column in zip_longest(schema, actual_schema):
                for name, value in column.items():
                    assert actual_column[name] == value
        _validate_schema(schema)

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
            {"name": "new_value", "type": "string"},
        ])
        alter_table("//tmp/crt", schema=schema)
        assert get("//tmp/crt/@dynamic")
        _validate_schema(schema)

    @authors("ashishkin")
    def test_chaos_table_add_column(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        # create chaos tables federation
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

        def _validate_schema(schema, path, driver=None):
            actual_schema = get(f"{path}/@schema", driver=driver)
            assert actual_schema.attributes["unique_keys"]
            for column, actual_column in zip_longest(schema, actual_schema):
                for name, value in column.items():
                    assert actual_column[name] == value
        _validate_schema(schema, "//tmp/crt")

        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q.new"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas[:2], table_path="//tmp/crt")
        self._create_replica_tables(replicas[:2], replica_ids, schema=schema)

        _, remote_driver0, _ = self._get_drivers()
        _validate_schema(schema, "//tmp/t", driver=remote_driver0)
        _validate_schema(schema, "//tmp/q", driver=remote_driver0)

        card_id = get("//tmp/crt/@replication_card_id")
        self._sync_replication_era(card_id, replicas[:2])

        # check data can be written
        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/crt", values0)
        wait(lambda: lookup_rows("//tmp/crt", [{"key": 0}]) == values0)
        assert select_rows("* from [//tmp/crt]") == values0

        # create replication_log with new schema
        new_schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
            {"name": "new_value", "type": "string"},
        ])
        new_replica_id = self._create_chaos_table_replica(replicas[2], table_path="//tmp/crt", catchup=False)
        self._create_replica_tables(replicas[2:], [new_replica_id], schema=new_schema)
        _validate_schema(new_schema, "//tmp/q.new", remote_driver0)

        # wait until all data replicated
        timestamp = generate_timestamp()
        wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[0]}/replication_lag_timestamp") > timestamp)

        # alter schema of data table
        sync_unmount_table("//tmp/t", driver=remote_driver0)
        alter_table("//tmp/t", schema=new_schema, driver=remote_driver0)
        sync_mount_table("//tmp/t", driver=remote_driver0)
        _validate_schema(new_schema, "//tmp/t", driver=remote_driver0)

        # remove old replication_log
        remove("//tmp/q", driver=remote_driver0)
        replica_id_to_delete = replica_ids[1]
        alter_table_replica(replica_id_to_delete, enabled=False)
        wait(lambda: get("#{0}/@state".format(replica_id_to_delete)) == "disabled")
        remove("#{}".format(replica_id_to_delete))

        # alter chaos_replicated_table schema
        alter_table("//tmp/crt", schema=new_schema)
        _validate_schema(new_schema, "//tmp/crt")

        # check data with new schema can be written
        values1 = [{"key": 1, "value": "1", "new_value": "n1"}]
        insert_rows("//tmp/crt", values1)
        wait(lambda: lookup_rows("//tmp/crt", [{"key": 1}]) == values1)

        values0[0]["new_value"] = None
        assert select_rows("* from [//tmp/crt]") == values0 + values1

    @authors("ashishkin")
    def test_chaos_table_remove_column(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        # create chaos tables federation
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
            {"name": "value2", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

        def _validate_schema(schema, path, driver=None):
            actual_schema = get(f"{path}/@schema", driver=driver)
            assert actual_schema.attributes["unique_keys"]
            for column, actual_column in zip_longest(schema, actual_schema):
                for name, value in column.items():
                    assert actual_column[name] == value
        _validate_schema(schema, "//tmp/crt")

        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, schema=schema)

        _, remote_driver0, remote_driver1 = self._get_drivers()
        _validate_schema(schema, "//tmp/t", driver=remote_driver0)
        _validate_schema(schema, "//tmp/q", driver=remote_driver0)
        _validate_schema(schema, "//tmp/t", driver=remote_driver1)
        _validate_schema(schema, "//tmp/q", driver=remote_driver1)

        card_id = get("//tmp/crt/@replication_card_id")
        self._sync_replication_era(card_id, replicas)

        def _filter_rows(rows, schema):
            columns = builtins.set([c["name"] for c in schema])
            for row in rows:
                row_columns = list(row.keys())
                for column in row_columns:
                    if column not in columns:
                        del row[column]
            return rows

        def _check_write(values_row, schema):
            values = [values_row]
            keys = [{"key": values_row["key"]}]
            insert_rows("//tmp/crt", values, update=True)
            wait(lambda: _filter_rows(lookup_rows("//tmp/crt", keys), schema) == values)

        def _check_read_replica(values_row, replica_path, remote_driver, schema):
            values = _filter_rows([values_row], schema)
            keys = [{"key": values_row["key"]}]
            wait(lambda: _filter_rows(lookup_rows(replica_path, keys, driver=remote_driver), schema) == values)

        # check data written and can be read
        values0 = {"key": 0, "value": "0", "value2": "10"}
        _check_write(values0, schema)
        _check_read_replica(values0, "//tmp/t", remote_driver1, schema)

        new_schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        # alter chaos_replicated_table schema
        alter_table("//tmp/crt", schema=new_schema)
        _validate_schema(new_schema, "//tmp/crt")

        new_replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t.new", "catchup": True},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q.new", "catchup": False},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t.new", "catchup": True},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q.new", "catchup": False},
        ]
        new_replica_ids = [None] * len(new_replicas)

        # create queue replicas
        for i, r in enumerate(new_replicas):
            if r["content_type"] == "data":
                continue
            new_replica_ids[i] = self._create_chaos_table_replica(new_replicas[i], table_path="//tmp/crt")
            self._create_replica_tables([new_replicas[i]], [new_replica_ids[i]], schema=new_schema)
            _validate_schema(new_schema, "//tmp/q.new", get_driver(cluster=r["cluster_name"]))

        def _remove_replica(replica_id, driver):
            alter_table_replica(replica_id, enabled=False, driver=driver)
            wait(lambda: get(f"#{replica_id}/@state", driver=driver) == "disabled")
            remove(f"#{replica_id}", driver=driver)

        def _remove_table(path, driver):
            sync_unmount_table(path, driver=driver)
            remove(path, driver=driver)

        def _get_replica_indexes(cluster_name, replicas):
            data_replica_index = -1
            queue_replica_index = -1
            for i, r in enumerate(replicas):
                if r["cluster_name"] == cluster_name:
                    if r["content_type"] == "data":
                        data_replica_index = i
                    elif r["content_type"] == "queue":
                        queue_replica_index = i
            assert data_replica_index != -1 and queue_replica_index != -1
            return (data_replica_index, queue_replica_index)

        def _migrate_replica_cluster(cluster_name):
            data_replica_index, queue_replica_index = _get_replica_indexes(cluster_name, replicas)
            new_replica = new_replicas[data_replica_index]
            driver = get_driver(cluster=cluster_name)

            create(
                "table",
                "//tmp/t.new",
                schema=make_schema(new_schema, unique_keys=True),
                driver=driver,
            )

            timestamp = generate_timestamp(driver=driver)
            wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[data_replica_index]}/replication_lag_timestamp") > timestamp)
            sync_unmount_table("//tmp/t", driver=driver)
            merge(
                in_="//tmp/t",
                out=f"<output_timestamp={timestamp}>//tmp/t.new",
                mode="ordered",
                schema_inference_mode="from_output",
                driver=driver
            )
            replication_progress = get("//tmp/t/@replication_progress", driver=driver)
            new_replica_ids[data_replica_index] = self._create_chaos_table_replica(
                new_replica,
                table_path="//tmp/crt",
                replication_progress=replication_progress
            )

            alter_table(
                "//tmp/t.new",
                dynamic=True,
                upstream_replica_id=new_replica_ids[data_replica_index],
                replication_progress=replication_progress,
                driver=driver
            )
            sync_mount_table("//tmp/t.new", driver=driver)

            # remove old replicas and tables
            for i in (data_replica_index, queue_replica_index):
                _remove_replica(replica_ids[i], driver)
            for t in ("//tmp/t", "//tmp/q"):
                _remove_table(t, driver)

            # check old data is present in new data replica
            _check_read_replica(values0, "//tmp/t.new", driver, new_schema)

        _migrate_replica_cluster("remote_1")

        # check read/write new data
        values1 = {"key": 1, "value": "1"}
        _check_write(values1, new_schema)
        _check_read_replica(values1, "//tmp/t.new", remote_driver1, new_schema)

        def _switch_cluster_mode(cluster_name, replicas, replica_ids, mode):
            data_replica_index, queue_replica_index = _get_replica_indexes(cluster_name, replicas)
            alter_table_replica(replica_ids[data_replica_index], mode=mode)
            alter_table_replica(replica_ids[queue_replica_index], mode=mode)

            if mode == "sync":
                replica_id = replica_ids[data_replica_index]
                path = replicas[data_replica_index]["replica_path"]
                driver = get_driver(cluster=cluster_name)

                wait(lambda: replica_id in frozenset(get_in_sync_replicas(path, data=None, timestamp=MaxTimestamp, driver=driver)))

        _switch_cluster_mode("remote_1", new_replicas, new_replica_ids, "sync")
        _switch_cluster_mode("remote_0", replicas, replica_ids, "async")
        self._sync_replication_era(card_id)
        _migrate_replica_cluster("remote_0")

        # check previously written
        _check_read_replica(values1, "//tmp/t.new", remote_driver0, new_schema)

        # check read/write new data
        values2 = {"key": 2, "value": "2"}
        _check_write(values2, new_schema)
        _check_read_replica(values2, "//tmp/t.new", remote_driver0, new_schema)
        _check_read_replica(values2, "//tmp/t.new", remote_driver1, new_schema)

    @authors("savrus")
    def test_invalid_chaos_table_data_access(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle"})

        with pytest.raises(YtError, match="Table schema is not specified"):
            insert_rows("//tmp/crt", [{"key": 0, "value": "0"}])
        with pytest.raises(YtError, match="Table schema is not specified"):
            lookup_rows("//tmp/crt", [{"key": 0}])

    @authors("ponasenko-rs")
    def test_transaction_locks(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "int64", "lock": "lock_a"},
            {"name": "b", "type": "int64", "lock": "lock_b"},
            {"name": "c", "type": "int64", "lock": "lock_c"},
        ])

        create(
            "chaos_replicated_table",
            "//tmp/crt",
            attributes={
                "chaos_cell_bundle": "chaos_bundle",
                "schema": schema,
            },
        )

        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, schema=schema)

        card_id = get("//tmp/crt/@replication_card_id")
        self._sync_replication_era(card_id, replicas)

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/crt", [{"key": 1, "a": 1}], update=True, tx=tx1)
        lock_rows("//tmp/crt", [{"key": 1}], locks=["lock_a", "lock_c"], tx=tx1, lock_type="shared_weak")
        insert_rows("//tmp/crt", [{"key": 1, "b": 2}], update=True, tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        assert lookup_rows("//tmp/crt", [{"key": 1}], column_names=["key", "a", "b"]) == [{"key": 1, "a": 1, "b": 2}]

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")
        tx3 = start_transaction(type="tablet")

        insert_rows("//tmp/crt", [{"key": 2, "a": 1}], update=True, tx=tx1)
        lock_rows("//tmp/crt", [{"key": 2}], locks=["lock_a", "lock_c"], tx=tx1, lock_type="shared_weak")

        insert_rows("//tmp/crt", [{"key": 2, "b": 2}], update=True, tx=tx2)
        lock_rows("//tmp/crt", [{"key": 2}], locks=["lock_c"], tx=tx2, lock_type="shared_weak")

        lock_rows("//tmp/crt", [{"key": 2}], locks=["lock_a"], tx=tx3, lock_type="shared_weak")

        commit_transaction(tx1)
        commit_transaction(tx2)

        with pytest.raises(YtError):
            commit_transaction(tx3)

        assert lookup_rows("//tmp/crt", [{"key": 2}], column_names=["key", "a", "b"]) == [{"key": 2, "a": 1, "b": 2}]

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        lock_rows("//tmp/crt", [{"key": 3}], locks=["lock_a"], tx=tx1, lock_type="shared_weak")
        insert_rows("//tmp/crt", [{"key": 3, "a": 1}], update=True, tx=tx2)

        commit_transaction(tx2)

        with pytest.raises(YtError):
            commit_transaction(tx1)

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        lock_rows("//tmp/crt", [{"key": 3}], locks=["lock_a"], tx=tx1, lock_type="shared_strong")
        insert_rows("//tmp/crt", [{"key": 3, "a": 1}], update=True, tx=tx2)

        commit_transaction(tx1)

        with pytest.raises(YtError):
            commit_transaction(tx2)

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/crt", [{"key": 1, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        insert_rows("//tmp/crt", [{"key": 1, "a": 2}], update=True, lock_type="shared_write", tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        wait(lambda: lookup_rows("//tmp/crt", [{"key": 1}], column_names=["key", "a"]) == [{"key": 1, "a": 2}])

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")
        tx3 = start_transaction(type="tablet")

        insert_rows("//tmp/crt", [{"key": 2, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        insert_rows("//tmp/crt", [{"key": 2, "a": 2}], update=True, tx=tx2)
        insert_rows("//tmp/crt", [{"key": 2, "a": 3}], update=True, lock_type="shared_write", tx=tx3)

        commit_transaction(tx1)
        with pytest.raises(YtError):
            commit_transaction(tx2)
        commit_transaction(tx3)

        wait(lambda: lookup_rows("//tmp/crt", [{"key": 2}], column_names=["key", "a"]) == [{"key": 2, "a": 3}])

    @authors("savrus")
    def test_chaos_table_data_access(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle"})

        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids)

        card_id = get("//tmp/crt/@replication_card_id")
        self._sync_replication_era(card_id, replicas)

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        alter_table("//tmp/crt", schema=schema)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/crt", values)
        wait(lambda: lookup_rows("//tmp/crt", [{"key": 0}]) == values)
        assert select_rows("* from [//tmp/crt]") == values

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_chaos_table_and_dynamic_table(self, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids)
        card_id = get("//tmp/crt/@replication_card_id")
        self._sync_replication_era(card_id, replicas)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/d")
        sync_mount_table("//tmp/d")

        values = [{"key": 0, "value": "0"}]
        tx = start_transaction(type="tablet")
        insert_rows("//tmp/crt", values, tx=tx)
        insert_rows("//tmp/d",  [{"key": 0, "value": "1"}], tx=tx)
        commit_transaction(tx)

        row = lookup_rows("//tmp/d", [{"key": 0}], versioned=True)
        assert str(row[0]["value"][0]) == "1"
        ts = row[0].attributes["write_timestamps"][0]

        if mode == "async":
            def _insistent_lookup_rows(versioned):
                result = None

                def try_lookup_rows():
                    try:
                        nonlocal result
                        result = lookup_rows("//tmp/crt", [{"key": 0}], timestamp=ts, versioned=versioned)
                    except YtError as err:
                        if err.is_no_in_sync_replicas():
                            return False
                        raise err
                    return True

                wait(try_lookup_rows)
                return result

            assert _insistent_lookup_rows(versioned=False) == values
            # Since we have 2 proxies, the second request could be done to another proxy with older replication card
            row = _insistent_lookup_rows(versioned=True)
        else:
            row = lookup_rows("//tmp/crt", [{"key": 0}], versioned=True)

        assert str(row[0]["value"][0]) == "0"
        assert row[0].attributes["write_timestamps"][0] == ts

    @authors("ponasenko-rs")
    def test_migrate_replicated_table_to_chaos_replicated_table(self):
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])

        def _create_replica_with_table(path, cluster_name, mode, table):
            replica_id = create_table_replica(
                table,
                cluster_name,
                path,
                attributes={"mode": mode}
            )

            replica_driver = get_driver(cluster=cluster_name)
            create("table", path, driver=replica_driver, attributes={
                "schema": schema,
                "dynamic": True,
                "upstream_replica_id": replica_id
            })
            sync_mount_table(path, driver=replica_driver)

            sync_enable_table_replica(replica_id)
            return replica_id

        _keys = [{"key": i} for i in range(15)]
        _values = [{"key": key["key"], "value": ""} for key in _keys]

        def _checked_insert_values(path, iteration):
            def gen_values(iteration):
                assert iteration < len(_keys)
                return [
                    {"key": _keys[i]["key"], "value": str(_keys[i]["key"] * iteration)}
                    for i in range(0, len(_keys), iteration)
                ]

            def update_values(values, new_values):
                for new_value in new_values:
                    for value in values:
                        if value["key"] == new_value["key"]:
                            value["value"] = new_value["value"]

            def _check(keys, values):
                for replica in replicas:
                    driver = get_driver(cluster=replica["cluster_name"])
                    path = replica["replica_path"]
                    if replica["mode"] == "sync":
                        assert lookup_rows(path, keys, driver=driver) == values
                    else:
                        wait(lambda: lookup_rows(path, keys, driver=driver) == values)

            new_values = gen_values(iteration=iteration)
            update_values(_values, new_values)
            insert_rows(path, new_values)
            assert lookup_rows(path, _keys) == _values
            _check(_keys, _values)

        def _in_sync(path, replica_ids):
            timestamp = generate_timestamp()

            def _checkable():
                return are_items_equal(
                    get_in_sync_replicas(path, [], all_keys=True, timestamp=timestamp),
                    replica_ids
                )
            return _checkable

        def _switch(from_replica_ids, to_replica_ids):
            for replica_id in from_replica_ids:
                alter_table_replica(replica_id, enabled=False)

            for i, replica_id in enumerate(to_replica_ids):
                replica = replicas[i]
                path = replica["replica_path"]
                driver = get_driver(cluster=replica["cluster_name"])
                sync_unmount_table(path, driver=driver)
                alter_table(
                    path,
                    upstream_replica_id=replica_id,
                    driver=driver
                )
                sync_mount_table(path, driver=driver)
                alter_table_replica(replica_id, enabled=True)

        # Setup environment.
        for driver in self._get_drivers():
            sync_create_cells(1, driver=driver)

        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        # Replicated table initialization.
        create("replicated_table", "//tmp/rt", attributes={"schema": schema, "dynamic": True})

        sync_mount_table("//tmp/rt")
        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": False, "replica_path": "//tmp/r1"},
        ]

        rt_replica_ids = [
            _create_replica_with_table(
                replica["replica_path"],
                replica["cluster_name"],
                replica["mode"],
                "//tmp/rt"
            )
            for replica in replicas
        ]

        _checked_insert_values("//tmp/rt", 1)

        # Chaos initialization.
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})
        card_id = get("//tmp/crt/@replication_card_id")

        queues = [
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
        ]
        crt_replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        queues_replica_ids = self._create_chaos_table_replicas(queues, table_path="//tmp/crt")
        self._create_replica_tables(queues, queues_replica_ids, schema=schema)

        # Switch to chaos.
        _checked_insert_values("//tmp/rt", 2)
        sync_freeze_table("//tmp/rt")
        wait(_in_sync("//tmp/rt", rt_replica_ids))
        sync_unmount_table("//tmp/rt")

        _switch(rt_replica_ids, crt_replica_ids)

        self._sync_replication_era(card_id, replicas)
        wait(_in_sync("//tmp/crt", crt_replica_ids))

        _checked_insert_values("//tmp/crt", 3)

        # Switch back.
        sync_mount_table("//tmp/rt")
        _switch(crt_replica_ids, rt_replica_ids)
        wait(_in_sync("//tmp/rt", rt_replica_ids))

        _checked_insert_values("//tmp/rt", 4)
        wait(_in_sync("//tmp/rt", rt_replica_ids))

    @authors("savrus")
    @pytest.mark.parametrize("disable_data", [True, False])
    @pytest.mark.parametrize("wait_alter", [True, False])
    def test_trim_replica_history_items(self, disable_data, wait_alter):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _alter_table_replica(replica_id, mode):
            alter_table_replica(replica_id, mode=mode)
            if wait_alter:
                wait(lambda: get("#{0}/@mode".format(replica_id)) == mode)

        _alter_table_replica(replica_ids[2], "sync")
        _alter_table_replica(replica_ids[1], "async")
        if disable_data:
            self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=False)
        _alter_table_replica(replica_ids[1], "sync")
        _alter_table_replica(replica_ids[2], "async")
        _alter_table_replica(replica_ids[2], "sync")
        _alter_table_replica(replica_ids[1], "async")
        if disable_data:
            self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=True)

        wait(lambda: all(replica["mode"] in ["sync", "async"] for replica in get("#{0}/@replicas".format(card_id)).values()))
        self._sync_replication_era(card_id)

        def _check():
            card = get("#{0}/@".format(card_id))
            if any(len(replica["history"]) > 1 for replica in card["replicas"].values()):
                return False
            return True
        wait(_check)

        rows = [{"key": 1, "value": "1"}]
        keys = [{"key": 1}]
        self._insistent_insert_rows("//tmp/t", rows)
        wait(lambda: lookup_rows("//tmp/t", keys) == rows)

    @authors("savrus")
    def test_queue_trimming(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        def _check(value, row_count):
            rows = [{"key": 1, "value": value}]
            keys = [{"key": 1}]
            insert_rows("//tmp/t", rows)
            wait(lambda: lookup_rows("//tmp/t", keys) == rows)
            wait(lambda: get("//tmp/q/@tablets/0/trimmed_row_count", driver=remote_driver0) == row_count)
            sync_flush_table("//tmp/q", driver=remote_driver0)
            wait(lambda: len(get("//tmp/q/@chunk_ids", driver=remote_driver0)) == 0)

        _check("1", 1)
        _check("2", 2)

    @authors("savrus")
    def test_initially_disabled_replica(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/r1"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        assert get("#{0}/@".format(card_id))["replicas"][replica_ids[2]]["state"] == "disabled"

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("content_type", ["data", "queue"])
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_copy_replica(self, content_type, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        # Keep one replica disabled to prevent trimming.
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/data"},
            {"cluster_name": "primary", "content_type": content_type, "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/queue"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/disabled"},
        ]
        _, (_, t_replica_id, _, _) = self._create_chaos_tables(cell_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        def _get_rows_from_queue(path):
            rows = pull_rows(
                path,
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=t_replica_id,
            )

            encountered_keys = builtins.set()
            result_rows = []
            for row in rows:
                key = row["key"]
                assert key not in encountered_keys
                encountered_keys.add(key)

                assert len(row["value"]) == 1
                result_rows.append({"key": row["key"], "value": str(row["value"][0])})

            return sorted(result_rows, key=lambda row: row["key"])

        def _lookup_over_pull_rows(path, key):
            for row in _get_rows_from_queue(path):
                if row["key"] == key:
                    return row

            return None

        if content_type == "data":
            wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)
        else:
            wait(lambda: _lookup_over_pull_rows("//tmp/t", 0) == values[0])

        sync_unmount_table("//tmp/t")

        copy("//tmp/t", "//tmp/t-copy")

        sync_mount_table("//tmp/t")
        sync_mount_table("//tmp/t-copy")

        assert get("//tmp/t-copy/@upstream_replica_id") == get("//tmp/t/@upstream_replica_id")

        values = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values)

        if content_type == "data":
            wait(lambda: lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "value": "1"}])
        else:
            wait(lambda: _lookup_over_pull_rows("//tmp/t", 1) == {"key": 1, "value": "1"})

        with pytest.raises(WaitFailed):
            if content_type == "data":
                wait(lambda: lookup_rows("//tmp/t-copy", [{"key": 1}]) == [{"key": 1, "value": "1"}], timeout=15)
            else:
                wait(lambda: _lookup_over_pull_rows("//tmp/t-copy", 1) == {"key": 1, "value": "1"}, timeout=15)

        if content_type == "data":
            wait(lambda: select_rows("* from [//tmp/t] LIMIT 3") == [{"key": 0, "value": "0"}, {"key": 1, "value": "1"}])
            wait(lambda: select_rows("* from [//tmp/t-copy]") == [{"key": 0, "value": "0"}])
        else:
            wait(lambda: _get_rows_from_queue("//tmp/t") == [{"key": 0, "value": "0"}, {"key": 1, "value": "1"}])
            wait(lambda: _get_rows_from_queue("//tmp/t-copy") == [{"key": 0, "value": "0"}])

        def _check():
            tablet_infos = get_tablet_infos("//tmp/t-copy", [0], request_errors=True)
            errors = tablet_infos["tablets"][0]["tablet_errors"]

            if len(errors) != 1 or errors[0]["attributes"]["background_activity"] != "pull":
                return False

            return any(
                error["message"] == "Upstream replica id corresponds to another table"
                for error in errors[0]["inner_errors"]
            )

        wait(_check)

        values = [{"key": 2, "value": "2"}]
        with pytest.raises(YtError, match="Table uses upstream_replica_id of other replica"):
            insert_rows("//tmp/t-copy", values)

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("content_type", ["data", "queue"])
    @pytest.mark.parametrize("mode", ["sync", "async"])
    @pytest.mark.parametrize("reset_type", ["recreate", "reset_progress"])
    def test_change_replica_id(self, content_type, mode, reset_type):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        queue_mode = "async" if mode == "sync" and content_type == "queue" else "sync"

        # Keep one replica disabled to prevent trimming.
        replicas1 = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/data-1"},
            {"cluster_name": "primary", "content_type": content_type, "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": False, "replica_path": "//tmp/catchup_queue-1"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": queue_mode, "enabled": True, "replica_path": "//tmp/q-1"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/disabled-1"},
        ]
        replicas2 = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/data-2"},
            {"cluster_name": "primary", "content_type": content_type, "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": queue_mode, "enabled": True, "replica_path": "//tmp/q-2"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/disabled-2"},
        ]

        card_id1, replica_ids1 = self._create_chaos_tables(cell_id, replicas1)

        card_id2 = create_replication_card(chaos_cell_id=cell_id)
        replica_ids2 = self._create_chaos_table_replicas(replicas2, replication_card_id=card_id2)
        self._create_replica_tables(replicas2[:1] + replicas2[2:], replica_ids2[:1] + replica_ids2[2:])
        self._sync_replication_era(card_id2, replicas2[:1] + replicas2[2:])

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        def _get_rows_from_queue(path, replica_id):
            rows = pull_rows(
                path,
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_id,
            )

            encountered_keys = builtins.set()
            result_rows = []
            for row in rows:
                key = row["key"]
                assert key not in encountered_keys
                encountered_keys.add(key)

                assert len(row["value"]) == 1
                result_rows.append({"key": row["key"], "value": str(row["value"][0])})

            return sorted(result_rows, key=lambda row: row["key"])

        def _lookup_over_pull_rows(path, replica_id, key):
            for row in _get_rows_from_queue(path, replica_id):
                if row["key"] == key:
                    return row

            return None

        if content_type == "data":
            wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)
        else:
            wait(lambda: _lookup_over_pull_rows("//tmp/t", replica_ids1[1], 0) == values[0])

        if queue_mode == "async":
            with pytest.raises(YtError, match=f"Mismatched upstream replica: expected {replica_ids1[1]}, got {replica_ids2[1]}"):
                insert_rows("//tmp/data-2", values)

        sync_unmount_table("//tmp/t")

        if reset_type == "recreate":
            remove("//tmp/t")
            self._create_replica_tables(replicas2[1:2], replica_ids2[1:2])
        else:
            alter_table(
                "//tmp/t",
                upstream_replica_id=replica_ids2[1],
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 1}],
                    "upper_key": MAX_KEY,
                },
            )

        sync_mount_table("//tmp/t")

        values = [{"key": 10, "value": "10"}]
        insert_rows("//tmp/t", values)

        if content_type == "data":
            wait(lambda: lookup_rows("//tmp/t", [{"key": 10}]) == [{"key": 10, "value": "10"}])
        else:
            wait(lambda: _lookup_over_pull_rows("//tmp/t", replica_ids2[1], 10) == {"key": 10, "value": "10"})

        wait(lambda: lookup_rows("//tmp/data-2", [{"key": 10}]) == [{"key": 10, "value": "10"}])
        with pytest.raises(WaitFailed):
            wait(lambda: lookup_rows("//tmp/data-1", [{"key": 10}]) == [{"key": 10, "value": "10"}], timeout=15)

        if queue_mode == "async":
            alter_table_replica(replica_ids1[2], enabled=True)

            def _check():
                tablet_infos = get_tablet_infos("//tmp/catchup_queue-1", [0], request_errors=True)
                errors = tablet_infos["tablets"][0]["tablet_errors"]

                if len(errors) != 1 or errors[0]["attributes"]["background_activity"] != "pull":
                    return False

                def _is_mismatched_replica_error(error):
                    return (
                        error["message"] == "All pull rows subrequests failed" and
                        len(error["inner_errors"]) == 1 and
                        error["inner_errors"][0]["message"] == f"Mismatched upstream replica: expected {replica_ids2[1]}, got {replica_ids1[1]}"
                    )

                return any(_is_mismatched_replica_error(error) for error in errors[0]["inner_errors"])

            wait(_check)
        else:
            self._sync_alter_replica(card_id1, replicas1[:1] + replicas1[2:], replica_ids1[:1] + replica_ids1[2:], 1, enabled=True)

        expected = [{"key": 0, "value": "0"}, {"key": 10, "value": "10"}] if reset_type == "reset_progress" else [{"key": 10, "value": "10"}]
        if content_type == "data":
            wait(lambda: select_rows("* from [//tmp/t] LIMIT 3") == expected)
        else:
            wait(lambda: _get_rows_from_queue("//tmp/t", replica_ids2[1]) == expected)

        wait(lambda: select_rows("* from [//tmp/data-1] LIMIT 2") == [{"key": 0, "value": "0"}])
        expected = [{"key": 0, "value": "0"}, {"key": 10, "value": "10"}] if queue_mode == "async" and reset_type == "reset_progress" else [{"key": 10, "value": "10"}]
        wait(lambda: select_rows("* from [//tmp/data-2] LIMIT 2") == expected)

    @authors("savrus")
    def test_new_data_replica(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas[:2])

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)

        replica_ids.append(self._create_chaos_table_replica(replicas[2], replication_card_id=card_id, catchup=False))
        self._create_replica_tables(replicas[2:], replica_ids[2:])
        self._sync_replication_era(card_id, replicas)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        wait(lambda: select_rows("* from [//tmp/t]") == values0 + values1)
        assert select_rows("* from [//tmp/r]", driver=get_driver(cluster="remote_0")) == values1

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_new_data_replica_catchup(self, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas[:2])

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)

        replica_ids.append(self._create_chaos_table_replica(replicas[2], replication_card_id=card_id, catchup=True))
        self._create_replica_tables(replicas[2:], replica_ids[2:])
        self._sync_replication_era(card_id, replicas)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)
        wait(lambda: select_rows("* from [//tmp/r]", driver=get_driver(cluster="remote_0")) == values0 + values1)

    @authors("savrus")
    @pytest.mark.parametrize("enabled", [True, False])
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_new_data_replica_with_progress_and_wo_catchup(self, mode, enabled):
        # NB: Bug reproduce, do not create replica with explicit progress and catchup=False.
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": mode, "enabled": enabled, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas[:2])
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)

        replica_ids.append(self._create_chaos_table_replica(replicas[2], replication_card_id=card_id, catchup=False))
        self._create_replica_tables(replicas[2:], replica_ids[2:], mount_tables=False)

        sync_unmount_table("//tmp/t")
        replication_progress = get("//tmp/t/@replication_progress")
        sync_mount_table("//tmp/t")
        alter_table("//tmp/r", replication_progress=replication_progress, driver=remote_driver0)
        sync_mount_table("//tmp/r", driver=remote_driver0)

        self._sync_replication_era(card_id, replicas)

        if not enabled:
            self._sync_alter_replica(card_id, replicas, replica_ids, 2, enabled=True)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        if mode == "sync":
            assert select_rows("* from [//tmp/r]", driver=get_driver(cluster="remote_0")) == values1
        else:
            wait(lambda: select_rows("* from [//tmp/r]", driver=get_driver(cluster="remote_0")) == values1)

    @authors("savrus")
    @pytest.mark.parametrize("catchup", [True, False])
    def test_new_queue_replica(self, catchup):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas[:2])

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)

        replica_ids.append(self._create_chaos_table_replica(replicas[2], replication_card_id=card_id, catchup=catchup))
        self._create_replica_tables(replicas[2:], replica_ids[2:])
        self._sync_replication_era(card_id, replicas)

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=False)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=True)

        expected = values0 + values1 if catchup else []
        wait(lambda: select_rows("* from [//tmp/t]") == expected)

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)
        wait(lambda: select_rows("* from [//tmp/t]") == values0 + values1)

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    @pytest.mark.parametrize("method", ["replica", "table"])
    def test_new_replica_with_progress(self, mode, method):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        drivers = self._get_drivers()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas[:2])

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)

        timestamp = generate_timestamp()
        replication_progress = {
            "segments": [{
                "lower_key": [],
                "timestamp": timestamp,
            }],
            "upper_key": MAX_KEY,
        }
        print_debug("Creating new replica with progress:", replication_progress)

        if method == "replica":
            # set replication progress in replica creation
            replica_ids.append(self._create_chaos_table_replica(
                replicas[2],
                replication_card_id=card_id,
                replication_progress=replication_progress
            ))
            self._create_replica_tables(replicas[2:], replica_ids[2:])
        else:
            # set replication progress in table creation
            replica_ids.append(self._create_chaos_table_replica(replicas[2], replication_card_id=card_id))

            self._create_sorted_table(
                "//tmp/r",
                dynamic=True,
                upstream_replica_id=replica_ids[2],
                replication_progress=replication_progress,
                driver=drivers[1]
            )

            sync_mount_table("//tmp/r", driver=drivers[1])

        self._sync_replication_era(card_id, replicas)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=True)
        wait(lambda: select_rows("* from [//tmp/t]") == values0 + values1)

        if mode == "sync":
            assert select_rows("* from [//tmp/r]", driver=drivers[1]) == values1
        else:
            wait(lambda: select_rows("* from [//tmp/r]", driver=drivers[1]) == values1)

    @authors("savrus")
    def test_coordinator_suspension(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        coordinator_cell_id = self._sync_create_chaos_cell()
        assert len(get("//sys/chaos_cells")) == 2

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas[:2])

        def _get_orchid(cell_id, path):
            address = get("#{0}/@peers/0/address".format(cell_id))
            return get("//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}{2}".format(address, cell_id, path))

        def _get_shortcuts(cell_id):
            return _get_orchid(cell_id, "/coordinator_manager/shortcuts")

        assert list(_get_shortcuts(cell_id).keys()) == [card_id]
        assert list(_get_shortcuts(coordinator_cell_id).keys()) == [card_id]

        chaos_cell_ids = [cell_id, coordinator_cell_id]
        assert_items_equal(get("#{0}/@coordinator_cell_ids".format(card_id)), chaos_cell_ids)

        suspend_coordinator(coordinator_cell_id)
        # NB: Chaos cell orchid reads from follower do not sync with upstream.
        wait(lambda: _get_orchid(coordinator_cell_id, "/coordinator_manager/internal/suspended"))
        wait(lambda: get("#{0}/@coordinator_cell_ids".format(card_id)) == [cell_id])

        resume_coordinator(coordinator_cell_id)
        wait(lambda: not _get_orchid(coordinator_cell_id, "/coordinator_manager/internal/suspended"))
        wait(lambda: sorted(get("#{0}/@coordinator_cell_ids".format(card_id))) == sorted(chaos_cell_ids))

    @authors("savrus")
    def test_nodes_restart(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values0)

        with Restarter(self.Env, NODES_SERVICE):
            pass
        with Restarter(self.Env, CHAOS_NODES_SERVICE):
            pass

        wait_for_chaos_cell(cell_id, self.get_cluster_names())
        for driver in self._get_drivers():
            cell_ids = get("//sys/tablet_cell_bundles/default/@tablet_cell_ids", driver=driver)
            wait_for_cells(cell_ids, driver=driver)

        assert lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values0
        self._sync_replication_era(card_id, replicas)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 1}], driver=remote_driver1) == values1)

    @authors("savrus")
    def test_replication_progress_attribute(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": False, "replica_path": "//tmp/r"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        reshard_table("//tmp/t", [[], [1]])
        reshard_table("//tmp/q", [[], [1]], driver=remote_driver0)
        reshard_table("//tmp/r", [[], [1]], driver=remote_driver1)

        def _sync_mount_tables(replicas):
            for replica in replicas:
                sync_mount_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
            self._sync_replication_era(card_id, replicas)
        _sync_mount_tables(replicas[:2])

        def _insert_and_check(key, value):
            rows = [{"key": key, "value": value}]
            insert_rows("//tmp/t", rows)
            wait(lambda: lookup_rows("//tmp/t", [{"key": key}]) == rows)

        _insert_and_check(0, "0")
        sync_unmount_table("//tmp/q", first_tablet_index=0, last_tablet_index=0, driver=remote_driver0)
        _insert_and_check(1, "1")

        sync_unmount_table("//tmp/t")
        progress = get("//tmp/t/@replication_progress")
        assert len(progress["segments"]) > 1
        alter_table("//tmp/r", replication_progress=progress, driver=remote_driver1)
        reshard_table("//tmp/t", [[], [2]])

        _sync_mount_tables(replicas)
        self._sync_alter_replica(card_id, replicas, replica_ids, 2, enabled=True)

        _insert_and_check(2, "2")
        assert lookup_rows("//tmp/r", [{"key": 2}], driver=remote_driver1) == [{"key": 2, "value": "2"}]
        assert lookup_rows("//tmp/r", [{"key": i} for i in range(2)], driver=remote_driver1) == []
        _insert_and_check(1, "3")
        assert lookup_rows("//tmp/r", [{"key": 1}], driver=remote_driver1) == [{"key": 1, "value": "3"}]

    @authors("savrus")
    def test_copy_chaos_tables(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values0)

        wait(lambda: get("//tmp/q0/@tablets/0/trimmed_row_count", driver=remote_driver0) == 1)
        wait(lambda: get("//tmp/q1/@tablets/0/trimmed_row_count", driver=remote_driver0) == 1)

        sync_unmount_table("//tmp/t")
        sync_unmount_table("//tmp/q0", driver=remote_driver0)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        sync_unmount_table("//tmp/q1", driver=remote_driver0)
        sync_unmount_table("//tmp/r", driver=remote_driver1)

        copy_replicas = []
        for replica in replicas:
            driver = get_driver(cluster=replica["cluster_name"])
            copy_replica = deepcopy(replica)
            copy_replica["replica_path"] = replica["replica_path"] + "-copy"
            replication_progress = get("{0}/@replication_progress".format(replica["replica_path"]), driver=driver)
            copy_replica["replication_progress"] = replication_progress
            copy_replicas.append(copy_replica)
            copy(replica["replica_path"], copy_replica["replica_path"], driver=driver)

        copy_card_id, copy_replica_ids = self._create_chaos_tables(
            cell_id,
            copy_replicas,
            create_replica_tables=False,
            create_tablet_cells=False,
            mount_tables=False,
            sync_replication_era=False)
        self._prepare_replica_tables(copy_replicas, copy_replica_ids)
        self._sync_replication_era(copy_card_id, copy_replicas)

        assert lookup_rows("//tmp/t-copy", [{"key": 0}]) == values0
        wait(lambda: lookup_rows("//tmp/t-copy", [{"key": 1}]) == values1)

        values2 = [{"key": 2, "value": "2"}]
        insert_rows("//tmp/t-copy", values2)
        assert lookup_rows("//tmp/r-copy", [{"key": 2}], driver=remote_driver1) == values2
        wait(lambda: lookup_rows("//tmp/t-copy", [{"key": 2}]) == values2)

    @authors("savrus")
    def test_lagging_queue(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        reshard_table("//tmp/q0", [[], [1]], driver=remote_driver0)
        reshard_table("//tmp/q1", [[], [1]], driver=remote_driver1)

        for replica in replicas:
            sync_mount_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
        self._sync_replication_era(card_id, replicas)

        sync_unmount_table("//tmp/q1", driver=remote_driver1)

        timestamp = generate_timestamp()

        def _get_card_progress_timestamp():
            segments = get("#{0}/@replicas/{1}/replication_progress/segments".format(card_id, replica_ids[0]))
            return min(segment["timestamp"] for segment in segments)
        wait(lambda: _get_card_progress_timestamp() > timestamp)

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/q1", first_tablet_index=0, last_tablet_index=0, driver=remote_driver1)

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/q0", values0, driver=remote_driver0)
        sync_unmount_table("//tmp/q0", driver=remote_driver0)

        set("//tmp/q1/@enable_replication_progress_advance_to_barrier", False, driver=remote_driver1)
        sync_mount_table("//tmp/q1", driver=remote_driver1)
        sync_mount_table("//tmp/t")
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values0)

        sync_mount_table("//tmp/q0", driver=remote_driver0)
        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/q0", values1, driver=remote_driver0)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 1}]) == values1)

    @authors("savrus")
    def test_replica_data_access(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        values = [{"key": 0, "value": "0"}]
        keys = [{"key": 0}]
        insert_rows("//tmp/t", values)
        assert lookup_rows("//tmp/t", keys, replica_consistency="sync") == values
        assert select_rows("* from [//tmp/t]", replica_consistency="sync") == values
        assert lookup_rows("//tmp/t", keys) == []
        assert select_rows("* from [//tmp/t]") == []

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_dual_replica_placement(self, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        amode = {"sync": "async", "async": "sync"}[mode]
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": mode, "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": amode, "enabled": True, "replica_path": "//tmp/r"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": amode, "enabled": True, "replica_path": "//tmp/s"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        values = [{"key": i, "value": str(i)} for i in range(10)]
        keys = [{"key": i} for i in range(10)]

        tx = start_transaction(type="tablet")
        for i in range(10):
            insert_rows("//tmp/t", values[i:i+1], tx=tx)
            time.sleep(1)
        commit_transaction(tx)

        assert lookup_rows("//tmp/t", keys, replica_consistency="sync") == values
        assert select_rows("* from [//tmp/t]", replica_consistency="sync") == values
        versioned_rows = lookup_rows("//tmp/t", keys, replica_consistency="sync", versioned=True)
        rows = [{"key": row["key"], "value": str(row["value"][0])} for row in versioned_rows]
        assert rows == values
        ts = versioned_rows[0].attributes["write_timestamps"][0]
        assert all(row.attributes["write_timestamps"][0] == ts for row in versioned_rows)

    @authors("shakurov")
    @pytest.mark.timeout(180)
    def test_chaos_cell_peer_snapshot_loss(self):
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(cell_id, replicas)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values)

        build_snapshot(cell_id=cell_id)
        wait(lambda: len(ls("//sys/chaos_cells/{}/0/snapshots".format(cell_id))) != 0)
        wait(lambda: len(ls("//sys/chaos_cells/{}/1/snapshots".format(cell_id), driver=remote_driver0)) != 0)
        wait(lambda: len(ls("//sys/chaos_cells/{}/2/snapshots".format(cell_id), driver=remote_driver1)) != 0)

        assert get("//sys/chaos_cells/{}/@health".format(cell_id)) == "good"

        def get_alive_non_alien_peer_count():
            peers = get("//sys/chaos_cells/{}/@peers".format(cell_id), driver=remote_driver1)
            return len([peer for peer in peers if not peer.get("alien", False) and peer["state"] != "none"])

        chaos_nodes = ls("//sys/chaos_nodes", driver=remote_driver1)
        assert len(chaos_nodes) == 1
        maintenance_id = add_maintenance("cluster_node", chaos_nodes[0], "ban", comment="", driver=remote_driver1)[chaos_nodes[0]]

        set("//sys/chaos_cell_bundles/chaos_bundle/@node_tag_filter", "empty_set_of_nodes", driver=remote_driver1)
        wait(lambda: get("//sys/chaos_cells/{}/@local_health".format(cell_id), driver=remote_driver1) != "good")
        wait(lambda: get_alive_non_alien_peer_count() == 0)

        remove("//sys/chaos_cells/{}/2/snapshots/*".format(cell_id), driver=remote_driver1)

        set("//sys/chaos_cell_bundles/chaos_bundle/@node_tag_filter", "", driver=remote_driver1)
        remove_maintenance("cluster_node", chaos_nodes[0], id=maintenance_id, driver=remote_driver1)
        wait(lambda: get("//sys/chaos_cells/{}/@local_health".format(cell_id), driver=remote_driver1) == "good")
        wait(lambda: get("//sys/chaos_cells/{}/@health".format(cell_id)) == "good")
        wait(lambda: len(ls(f"//sys/chaos_cells/{cell_id}/2/snapshots", driver=remote_driver1)) != 0)

    @authors("savrus")
    @pytest.mark.parametrize("tablet_count", [1, 2])
    def test_ordered_chaos_table_pull(self, tablet_count):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False, ordered=True)

        reshard_table("//tmp/t", tablet_count)

        for replica in replicas:
            sync_mount_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
        self._sync_replication_era(card_id, replicas)

        def _pull_rows(replica_index, tablet_index, timestamp):
            rows = pull_rows(
                replicas[replica_index]["replica_path"],
                replication_progress={
                    "segments": [{"lower_key": [tablet_index], "timestamp": timestamp}],
                    "upper_key": [tablet_index + 1]
                },
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster_name"]))
            return [{"key": row["key"], "value": row["value"]} for row in rows]

        values = [{"$tablet_index": j, "key": i, "value": str(i + j)} for i in range(2) for j in range(tablet_count)]
        data_values = [[{"key": i, "value": str(i + j)} for i in range(2)] for j in range(tablet_count)]
        insert_rows("//tmp/t", values)

        for j in range(tablet_count):
            wait(lambda: select_rows("key, value from [//tmp/t] where [$tablet_index] = {0}".format(j)) == data_values[j])
            assert _pull_rows(0, j, 0) == data_values[j]

    @authors("savrus")
    @pytest.mark.parametrize("tablet_count", [1, 2])
    @pytest.mark.parametrize(
        "schema",
        [
            None,
            [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
                {"name": "$timestamp", "type": "uint64"},
            ]
        ]
    )
    def test_ordered_chaos_table(self, tablet_count, schema):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/r"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/q"},
        ]

        card_id, replica_ids = self._create_chaos_tables(
            cell_id,
            replicas,
            sync_replication_era=False,
            mount_tables=False,
            ordered=True,
            schema=schema
        )
        _, remote_driver0, remote_driver1 = self._get_drivers()

        for replica in replicas:
            path = replica["replica_path"]
            driver = get_driver(cluster=replica["cluster_name"])
            reshard_table(path, tablet_count, driver=driver)
            sync_mount_table(path, driver=driver)
        self._sync_replication_era(card_id, replicas)

        values = [{"$tablet_index": j, "key": i, "value": str(i + j)} for i in range(2) for j in range(tablet_count)]
        data_values = [[{"key": i, "value": str(i + j)} for i in range(2)] for j in range(tablet_count)]
        insert_rows("//tmp/t", values)

        for j in range(tablet_count):
            wait(lambda: select_rows("key, value from [//tmp/t] where [$tablet_index] = {0}".format(j)) == data_values[j])
            wait(lambda: select_rows("key, value from [//tmp/r] where [$tablet_index] = {0}".format(j), driver=remote_driver0) == data_values[j])
            assert select_rows("key, value from [//tmp/q] where [$tablet_index] = {0}".format(j), replica_consistency="sync", driver=remote_driver1) == data_values[j]

        sync_unmount_table("//tmp/t")
        progress = get("//tmp/t/@replication_progress")
        assert len(progress["segments"]) <= tablet_count

    @authors("savrus")
    def test_invalid_ordered_chaos_table(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, create_replica_tables=False, sync_replication_era=False, mount_tables=False, ordered=True)

        self._create_ordered_table(
            "//tmp/t",
            upstream_replica_id=replica_ids[0],
            schema=[
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
            ]
        )
        self._prepare_replica_tables(replicas, replica_ids, mount_tables=False)
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t")

        alter_table("//tmp/t", schema=[
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
            {"name": "$timestamp", "type": "uint64"},
        ])
        sync_mount_table("//tmp/t")

        with raises_yt_error("Invalid input row for chaos ordered table //tmp/t: \"$tablet_index\" column is not provided"):
            insert_rows("//tmp/t", [{"key": 0, "value": str(0)}])

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@commit_ordering", "weak")
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t")

    @authors("savrus")
    def test_invalid_replication_log_table(self):
        with raises_yt_error("Table of type \"replication_log_table\" must be dynamic"):
            create("replication_log_table", "//tmp/r")

        with raises_yt_error("Could not create unsorted replication log table"):
            attributes = {
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64"},
                    {"name": "value", "type": "string"}],
            }
            create("replication_log_table", "//tmp/r", attributes=attributes)

    @authors("savrus")
    def test_ordered_chaos_table_trim(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, ordered=True)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        data_values = [{"key": i, "value": str(i)} for i in range(3)]
        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(1)]
        insert_rows("//tmp/t", values)

        def _try_trim_rows():
            with raises_yt_error("trim tablet since some replicas may not be replicated up to this point"):
                trim_rows("//tmp/t", 0, 1)

        wait(lambda: select_rows("key, value from [//tmp/t]") == data_values[:1])
        _try_trim_rows()

        sync_flush_table("//tmp/t")
        _try_trim_rows()

        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(1, 2)]
        insert_rows("//tmp/t", values)
        _try_trim_rows()

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)
        wait(lambda: select_rows("key, value from [//tmp/r]", driver=remote_driver0) == data_values[:2])

        def _insistent_trim_rows(table, driver=None):
            try:
                trim_rows(table, 0, 1, driver=driver)
                return True
            except YtError as err:
                print_debug("Table {0} trim failed: ".format(table), err)
                return False

        wait(lambda: _insistent_trim_rows("//tmp/t"))
        assert select_rows("key, value from [//tmp/t]") == data_values[1:2]

        sync_flush_table("//tmp/r", driver=remote_driver0)
        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(2, 3)]
        insert_rows("//tmp/t", values)
        wait(lambda: _insistent_trim_rows("//tmp/r", driver=remote_driver0))

        # Ensure all replication is complete
        self._sync_alter_replica(card_id, replicas, replica_ids, 1, mode="sync")

        assert select_rows("key, value from [//tmp/r]", driver=remote_driver0) == data_values[1:]

    @authors("osidorkin")
    def test_ordered_chaos_table_trim_without_flush(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        queue_schema = self._get_schemas_by_name(["ordered_simple"])[0]
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "c", "schema": queue_schema})

        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q2"},
        ]

        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, ordered=True, schema=queue_schema)
        self._sync_replication_era(card_id)

        trim_rows("//tmp/q0", 0, 0)

        data_values = [{"key": i, "value": str(i)} for i in range(3)]
        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(3)]

        tx = start_transaction(type="tablet")
        insert_rows("//tmp/q0", values, tx=tx)
        commit_res = commit_transaction(tx)
        print_debug("Commit results: ", commit_res)

        wait(lambda: select_rows("key, value from [//tmp/q0]") == data_values)

        ts = generate_timestamp()
        for replica_id in replica_ids:
            wait(lambda: get(f"//tmp/crt/@replicas/{replica_id}/replication_lag_timestamp") > ts)

        ts = generate_timestamp()

        def _wait_replica_card_ts(replica):
            orchid = self._get_table_orchids(
                replica["replica_path"],
                driver=get_driver(cluster=replica["cluster_name"]))[0]
            return orchid["replication_card"]["current_timestamp"] > ts

        wait(lambda: _wait_replica_card_ts(replicas[0]))

        trim_rows("//tmp/q0", 0, 1)

        assert select_rows("key, value from [//tmp/q0]") == data_values[1:]

    @authors("apachee", "osidorkin")
    @pytest.mark.parametrize("rows_to_insert", [1, 2])
    def test_ordered_chaos_table_trim_one_row_for_different_row_count(self, rows_to_insert):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/r"},
        ]
        self._create_chaos_tables(cell_id, replicas, ordered=True)
        remote_driver0 = self._get_drivers()[1]

        data_values = [{"key": i, "value": f"{i}"} for i in range(rows_to_insert)]
        values = [{"$tablet_index": 0, "key": i, "value": f"{i}"} for i in range(rows_to_insert)]

        for i in range(len(values)):
            insert_rows("//tmp/t", values[i:(i + 1)])

            wait(lambda: select_rows("key, value from [//tmp/t]") == data_values[:(i + 1)])
            wait(lambda: select_rows("key, value from [//tmp/r]", driver=remote_driver0) == data_values[:(i + 1)])

            sync_flush_table("//tmp/t")
            sync_flush_table("//tmp/r", driver=remote_driver0)

        trim_rows("//tmp/t", 0, 1)
        trim_rows("//tmp/r", 0, 1, driver=remote_driver0)

        assert select_rows("key, value from [//tmp/t]") == data_values[1:]
        assert select_rows("key, value from [//tmp/r]", driver=remote_driver0) == data_values[1:]

    @authors("savrus")
    def test_ordered_chaos_table_auto_trim(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, ordered=True)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        set("//tmp/t/@min_data_ttl", 0)
        set("//tmp/t/@min_data_versions", 0)
        set("//tmp/t/@max_data_versions", 0)
        remount_table("//tmp/t")

        data_values = [{"key": i, "value": str(i)} for i in range(2)]
        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(1)]
        insert_rows("//tmp/t", values)
        sync_flush_table("//tmp/t")

        assert select_rows("key, value from [//tmp/t]") == data_values[:1]

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)

        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(1, 2)]
        insert_rows("//tmp/t", values)
        wait(lambda: select_rows("key, value from [//tmp/t]") == data_values[1:])

    @authors("savrus")
    def test_ordered_start_replication_row_index(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, ordered=True)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        data_values = [{"key": i, "value": str(i)} for i in range(3)]
        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(3)]
        insert_rows("//tmp/t", values[:1])
        wait(lambda: select_rows("key, value from [//tmp/r]", driver=remote_driver0) == data_values[:1])

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, mode="sync")
        insert_rows("//tmp/t", values[1:2])
        wait(lambda: select_rows("key, value from [//tmp/r]", driver=remote_driver0) == data_values[:2])

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, mode="async")
        insert_rows("//tmp/t", values[2:3])
        wait(lambda: select_rows("key, value from [//tmp/r]", driver=remote_driver0) == data_values)

    @authors("osidorkin")
    def test_ordered_chaos_table_in_transaction(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/t"},
        ]

        card_id, replica_ids = self._create_chaos_tables(
            cell_id,
            replicas,
            sync_replication_era=False,
            mount_tables=False,
            ordered=True
        )
        primary_driver, remote_driver0, remote_driver1 = self._get_drivers()

        for replica in replicas:
            path = replica["replica_path"]
            driver = get_driver(cluster=replica["cluster_name"])
            reshard_table(path, 1, driver=driver)
            sync_mount_table(path, driver=driver)
        self._sync_replication_era(card_id, replicas)

        batch_size = 2
        data_values = []
        prev_commit_timestamp = 0

        for idx, tx_driver in enumerate(self._get_drivers()):
            values = [{"$tablet_index": 0, "key": i + batch_size * idx, "value": str(i)} for i in range(batch_size)]
            data_values.extend(({"key": i + batch_size * idx, "value": str(i)} for i in range(batch_size)))

            tx = start_transaction(type="tablet", driver=tx_driver)
            insert_rows("//tmp/t", values, tx=tx, driver=tx_driver)
            commit_res = commit_transaction(tx, driver=tx_driver)
            print_debug("Commit results: ", commit_res, ", idx: ", idx)
            primary_commit_timestamp = commit_res["primary_commit_timestamp"]
            assert primary_commit_timestamp != 0

            wait(lambda: select_rows("key, value from [//tmp/t] where [$tablet_index] = 0") == data_values)
            wait(lambda: select_rows("key, value from [//tmp/t] where [$tablet_index] = 0", driver=remote_driver0) == data_values)
            assert select_rows("key, value from [//tmp/t] where [$tablet_index] = 0", replica_consistency="sync", driver=remote_driver1) == data_values

            rows = pull_rows(
                "//tmp/t",
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": prev_commit_timestamp}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_ids[0],
                driver=primary_driver
            )
            assert rows == [v | {"$timestamp": primary_commit_timestamp} for v in values]
            prev_commit_timestamp = primary_commit_timestamp

        sync_unmount_table("//tmp/t")
        progress = get("//tmp/t/@replication_progress")
        assert len(progress["segments"]) <= 1

    @authors("savrus")
    @pytest.mark.parametrize("with_data", [True, False])
    @pytest.mark.parametrize("write_target", ["t", "q"])
    def test_locks_replication(self, with_data, write_target):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "string", "lock": "a"},
            {"name": "b", "type": "string", "lock": "b"},
            {"name": "c", "type": "string", "lock": "c"},
        ]

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": False, "replica_path": "//tmp/r"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/s"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, schema=schema)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        path = "//tmp/" + write_target
        if with_data:
            insert_rows(path, [{"key": 1, "a": "1"}], update=True, tx=tx1)
        lock_rows(path, [{"key": 1}], locks=["a", "c"], tx=tx1, lock_type="shared_weak")
        insert_rows(path, [{"key": 1, "b": "2"}], update=True, tx=tx2)

        commit_transaction(tx1)
        commit_transaction(tx2)

        def _pull_rows():
            rows = pull_rows(
                replicas[3]["replica_path"],
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": 0}],
                    "upper_key": MAX_KEY,
                },
                upstream_replica_id=replica_ids[3],
                driver=get_driver(cluster=replicas[3]["cluster_name"]))

            def _unversion(row):
                result = {"key": row["key"]}
                for value in "a", "b":
                    if value in row:
                        result[value] = str(row[value][0])
                return result
            return [_unversion(row) for row in rows]

        def _check():
            rows = _pull_rows()
            return len(rows) > 0 and "b" in rows[-1]
        wait(_check)

        self._sync_alter_replica(card_id, replicas, replica_ids, 3, mode="sync")
        self._sync_alter_replica(card_id, replicas, replica_ids, 2, enabled=False)
        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)

        expected = [{"key": 1, "a": "1", "b": "2", "c": None}] if with_data else [{"key": 1, "a": None, "b": "2", "c": None}]
        wait(lambda: lookup_rows("//tmp/r", [{"key": 1}]) == expected)

    @authors("savrus")
    def test_replicated_table_tracker(self):
        self._init_replicated_table_tracker()
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        replicated_table_options = {
            "enable_replicated_table_tracker": True,
            "min_sync_replica_count": 1,
            "tablet_cell_bundle_name_ttl": 1000,
            "tablet_cell_bundle_name_failure_interval": 100,
        }
        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "c",
            "replicated_table_options": replicated_table_options
        })
        card_id = get("//tmp/crt/@replication_card_id")
        options = get("//tmp/crt/@replicated_table_options")
        assert options["enable_replicated_table_tracker"]
        assert options["min_sync_replica_count"] == 1

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q1"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids)

        cypress_replicas = get("//tmp/crt/@replicas")
        assert all(cypress_replicas[replica_id]["replicated_table_tracker_enabled"] for replica_id in replica_ids)

        # Wait for table_puller or advance_replication_progress iteration. Then timestamp=1 will be unable to misguide RTT
        timestamp = generate_timestamp()
        for replica_id in replica_ids:
            wait(lambda: get(f"//tmp/crt/@replicas/{replica_id}/replication_lag_timestamp") > timestamp)

        wait(lambda: get("#{0}/@mode".format(replica_ids[3])) == "sync")
        self._sync_replication_era(card_id, replicas)

        with self.CellsDisabled(clusters=["primary"], tablet_bundles=["default"]):
            wait(lambda: get("#{0}/@mode".format(replica_ids[0])) == "async")
            wait(lambda: get("#{0}/@mode".format(replica_ids[2])) == "async")
            wait(lambda: get("#{0}/@mode".format(replica_ids[1])) == "sync")

            alter_table_replica(replica_ids[0], enable_replicated_table_tracker=False)
            assert not get("//tmp/crt/@replicas/{0}/replicated_table_tracker_enabled".format(replica_ids[0]))

            values = [{"key": 0, "value": "0"}]
            keys = [{"key": 0}]
            self._insistent_insert_rows("//tmp/t", values)
            assert lookup_rows("//tmp/t", keys, replica_consistency="sync") == values

        alter_replication_card(card_id, enable_replicated_table_tracker=False)
        assert not get("//tmp/crt/@replicated_table_options/enable_replicated_table_tracker")
        replicated_table_options["min_sync_replica_count"] = 2
        alter_replication_card(card_id, replicated_table_options=replicated_table_options)
        options = get("//tmp/crt/@replicated_table_options")
        assert options["enable_replicated_table_tracker"]
        assert options["min_sync_replica_count"] == 2
        wait(lambda: get("#{0}/@mode".format(replica_ids[2])) == "sync")
        assert get("#{0}/@mode".format(replica_ids[1])) == "sync"
        assert get("#{0}/@mode".format(replica_ids[3])) == "sync"

        assert get("#{0}/@mode".format(replica_ids[0])) == "async"
        alter_table_replica(replica_ids[0], enable_replicated_table_tracker=True)
        wait(lambda: get("#{0}/@mode".format(replica_ids[0])) == "sync")

    @authors("akozhikhov")
    def test_banned_replica_cluster(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        replicated_table_options = {
            "enable_replicated_table_tracker": True,
            "tablet_cell_bundle_name_ttl": 1000,
            "tablet_cell_bundle_name_failure_interval": 100,
            "preferred_sync_replica_clusters": ["primary"],
        }
        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "c",
            "replicated_table_options": replicated_table_options
        })
        card_id = get("//tmp/crt/@replication_card_id")
        options = get("//tmp/crt/@replicated_table_options")
        assert options["enable_replicated_table_tracker"]

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q0"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids)

        for driver in self._get_drivers():
            set("//sys/@config/tablet_manager/replicated_table_tracker/use_new_replicated_table_tracker", True, driver=driver)

        self._sync_replication_era(card_id, replicas)

        for driver in self._get_drivers():
            set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", ["primary"], driver=driver)

        wait(lambda: get("#{0}/@mode".format(replica_ids[0])) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_ids[1])) == "sync")
        wait(lambda: get("#{0}/@mode".format(replica_ids[2])) == "sync")

    @authors("savrus")
    def test_ordered_replicated_table_tracker(self):
        self._init_replicated_table_tracker()
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        replicated_table_options = {
            "enable_replicated_table_tracker": True,
            "tablet_cell_bundle_name_ttl": 1000,
            "tablet_cell_bundle_name_failure_interval": 100,
        }
        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "c",
            "replicated_table_options": replicated_table_options
        })
        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/r"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, ordered=True)

        for driver in self._get_drivers():
            set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", ["primary"], driver=driver)
        wait(lambda: get("#{0}/@mode".format(replica_ids[0])) == "async")
        wait(lambda: get("#{0}/@mode".format(replica_ids[1])) == "sync")
        wait(lambda: get("#{0}/@mode".format(replica_ids[2])) == "sync")
        self._sync_replication_era(card_id)

        data_values = [{"key": i, "value": str(i)} for i in range(1)]
        values = [{"$tablet_index": 0, "key": i, "value": str(i)} for i in range(1)]
        insert_rows("//tmp/t", values)

        for replica in replicas:
            wait(lambda: select_rows("key, value from [{0}]".format(replica["replica_path"]), driver=get_driver(cluster=replica["cluster_name"])) == data_values)

    @authors("savrus", "akozhikhov")
    @pytest.mark.parametrize("snapshotting", ["none", "snapshot"])
    @pytest.mark.parametrize("migration", ["none", "migrate"])
    @pytest.mark.parametrize("max_sync", [1, 2])
    def test_replication_card_collocation(self, max_sync, migration, snapshotting):
        self._init_replicated_table_tracker()
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)
        cells = [cell_id]

        replicated_table_options = {
            "enable_replicated_table_tracker": False,
            "min_sync_replica_count": 1,
            "max_sync_replica_count": max_sync,
            "tablet_cell_bundle_name_ttl": 1000,
            "tablet_cell_bundle_name_failure_interval": 100,
        }

        def _create_supertable(prefix, clusters, sync_cluster):
            return self._create_chaos_supertable(prefix, clusters, sync_cluster, "c", replicated_table_options)

        clusters = self.get_cluster_names()
        crt1, card1, replicas1, replica_ids1 = _create_supertable("//tmp/a", clusters, clusters[0])
        crt2, card2, replicas2, replica_ids2 = _create_supertable("//tmp/b", clusters, clusters[1])

        for crt in [crt1, crt2]:
            assert get("{0}/@collocated_replication_card_ids".format(crt)) == []

        collocation_id = create("replication_card_collocation", None, attributes={
            "type": "replication",
            "table_paths": [crt1, crt2]
        })

        cards = sorted([card1, card2])
        for crt in [crt1, crt2]:
            assert get("{0}/@replication_collocation_id".format(crt)) == collocation_id
            assert sorted(get("{0}/@collocated_replication_card_ids".format(crt))) == cards

        def _get_orchid(cell_id, path):
            address = get("#{0}/@peers/0/address".format(cell_id))
            return get("//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}{2}".format(address, cell_id, path))
        collocation = _get_orchid(cell_id, "/chaos_manager/replication_card_collocations/{0}".format(collocation_id))
        assert collocation["state"] == "normal"
        assert collocation["size"] == 2

        if migration == "migrate":
            dst_cell_id = self._sync_create_chaos_cell()
            cells.append(dst_cell_id)

            with raises_yt_error("Trying to move incomplete collocation"):
                migrate_replication_cards(cell_id, [card1], destination_cell_id=dst_cell_id)

            self._sync_migrate_replication_cards(cell_id, [card1, card2], dst_cell_id)

            self._sync_replication_era(card1, replicas1)
            self._sync_replication_era(card2, replicas2)

            options = get("{0}/@replicated_table_options".format(crt1))
            assert not options["enable_replicated_table_tracker"]
            assert options["max_sync_replica_count"] == max_sync

            replicas = get("{0}/@replicas".format(crt1))
            assert all(replica["replicated_table_tracker_enabled"] for replica in replicas.values())

        alter_replication_card(card1, enable_replicated_table_tracker=True)
        alter_replication_card(card2, enable_replicated_table_tracker=True)

        if snapshotting == "snapshot":
            _, remote_driver0, remote_driver1 = self._get_drivers()
            for cell in cells:
                def _get_snapshots(cell):
                    return [
                        len(ls("//sys/chaos_cells/{0}/{1}/snapshots".format(cell, index), driver=driver))
                        for index, driver in enumerate(self._get_drivers())
                    ]
                snapshots = _get_snapshots(cell)
                build_snapshot(cell_id=cell)
                wait(lambda: all(new > old for new, old in zip(_get_snapshots(cell), snapshots)))
            with self.CellsDisabled(clusters=self.get_cluster_names(), chaos_bundles=["c"]):
                pass

        def _get_sync_replica_clusters(crt, content_type):
            replicas = get("{0}/@replicas".format(crt))

            def valid(replica):
                return replica["mode"] == "sync" and replica["content_type"] == content_type
            return list(builtins.set(replica["cluster_name"] for replica in replicas.values() if valid(replica)))

        def _check(content_type):
            sync1 = _get_sync_replica_clusters(crt1, content_type)
            sync2 = _get_sync_replica_clusters(crt2, content_type)
            sync1.sort()
            sync2.sort()
            import logging
            logger = logging.getLogger()
            logger.debug("Comparing sync {0} replicas: {1} vs {2}".format(content_type, sync1, sync2))
            return sync1 == sync2 and len(sync1) == [2, max_sync][content_type == "data"]
        wait(lambda: _check("data"))
        wait(lambda: _check("queue"))

        sync_clusters = _get_sync_replica_clusters(crt1, "data")
        async_cluster = [c for c in clusters if c not in sync_clusters][0]
        alter_replication_card(card1, collocation_options={"preferred_sync_replica_clusters": [async_cluster]})

        def _check2(crt, content_type):
            sync_clusters = _get_sync_replica_clusters(crt, content_type)
            return async_cluster in sync_clusters

        wait(lambda: _check2(crt1, "data"))
        wait(lambda: _check2(crt2, "data"))
        wait(lambda: _check2(crt1, "queue"))
        wait(lambda: _check2(crt2, "queue"))

        for content_type in ["data", "queue"]:
            sync_clusters = _get_sync_replica_clusters(crt1, content_type)
            with self.CellsDisabled(clusters=[sync_clusters[0]], tablet_bundles=["default"]):
                wait(lambda: _get_sync_replica_clusters(crt1, content_type) != sync_clusters)
                wait(lambda: _check(content_type))

    @authors("savrus")
    def test_alter_replication_card_collocation(self):
        self._init_replicated_table_tracker()
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        replicated_table_options = {
            "enable_replicated_table_tracker": False,
            "min_sync_replica_count": 1,
            "max_sync_replica_count": 1,
            "tablet_cell_bundle_name_ttl": 1000,
            "tablet_cell_bundle_name_failure_interval": 100,
        }

        def _create_supertable(prefix, clusters, sync_cluster):
            return self._create_chaos_supertable(prefix, clusters, sync_cluster, "c", replicated_table_options)

        clusters = self.get_cluster_names()
        crt1, card1, replicas1, replica_ids1 = _create_supertable("//tmp/a", clusters, clusters[0])
        crt2, card2, replicas2, replica_ids2 = _create_supertable("//tmp/b", clusters, clusters[1])
        crt3, card3, replicas3, replica_ids3 = _create_supertable("//tmp/c", clusters, clusters[2])

        collocation_options = {
            "preferred_sync_replica_clusters": [clusters[0]]
        }

        collocation_id = create("replication_card_collocation", None, attributes={
            "type": "replication",
            "table_paths": [crt1],
            "options": collocation_options
        })

        dst_cell_id = self._sync_create_chaos_cell()
        self._sync_migrate_replication_cards(cell_id, [card1], dst_cell_id)

        address = get("#{0}/@peers/0/address".format(dst_cell_id))
        collocation_path = f"//sys/cluster_nodes/{address}/orchid/chaos_cells/{dst_cell_id}/chaos_manager/replication_card_collocations/{collocation_id}"
        collocation = get(collocation_path)
        assert_items_equal(collocation["options"], collocation_options)

        execute_batch(
            [
                make_batch_request(
                    "alter_replication_card",
                    replication_card_id=card,
                    replication_card_collocation_id=collocation_id
                ) for card in [card2, card3]
            ]
        )

        assert get(f"#{card2}/@replication_card_collocation_id") == collocation_id
        assert get(f"#{card3}/@replication_card_collocation_id") == collocation_id
        wait(lambda: card2 in get(f"#{collocation_id}/@replication_card_ids"))
        wait(lambda: card3 in get(f"#{collocation_id}/@replication_card_ids"))

        collocation = get(collocation_path)
        assert collocation["state"] == "normal"
        assert collocation["size"] == 3
        assert card1 in collocation["replication_card_ids"]
        assert card2 in collocation["replication_card_ids"]
        assert card3 in collocation["replication_card_ids"]

        # Workaround for YT-22791.
        alter_replication_card(card1, enable_replicated_table_tracker=True)
        alter_replication_card(card2, enable_replicated_table_tracker=True)
        alter_replication_card(card3, enable_replicated_table_tracker=True)

        def _get_sync_replica_clusters(crt):
            def valid(replica):
                return replica["mode"] == "sync" and replica["content_type"] == "data"
            return list(builtins.set(replica["cluster_name"] for replica in get("{0}/@replicas".format(crt)).values() if valid(replica)))

        wait(lambda: _get_sync_replica_clusters("//tmp/a-crt") == _get_sync_replica_clusters("//tmp/b-crt"))
        wait(lambda: _get_sync_replica_clusters("//tmp/a-crt") == _get_sync_replica_clusters("//tmp/c-crt"))
        assert _get_sync_replica_clusters("//tmp/b-crt") == [clusters[0]]
        assert _get_sync_replica_clusters("//tmp/c-crt") == [clusters[0]]

    @authors("savrus")
    @pytest.mark.parametrize("method", ["alter", "remove"])
    def test_replication_card_collocation_removed(self, method):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        def _create(prefix):
            crt = "{0}-crt".format(prefix)
            create("chaos_replicated_table", crt, attributes={"chaos_cell_bundle": "c"})
            card_id = get("{0}/@replication_card_id".format(crt))
            return crt, card_id

        crt1, card1 = _create("//tmp/a")
        crt2, card2 = _create("//tmp/b")

        collocation_id = create("replication_card_collocation", None, attributes={
            "type": "replication",
            "table_paths": [crt1, crt2]
        })

        def _get_orchid_path(cell_id, path):
            address = get("#{0}/@peers/0/address".format(cell_id))
            return "//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}{2}".format(address, cell_id, path)
        collocation_path = _get_orchid_path(cell_id, "/chaos_manager/replication_card_collocations")
        assert len(get("{0}/{1}/replication_card_ids".format(collocation_path, collocation_id))) == 2

        def _unbind(crt, card):
            if method == "remove":
                remove(crt)
            else:
                alter_replication_card(card, replication_card_collocation_id="0-0-0-0")

        _unbind(crt1, card1)
        wait(lambda: get("{0}/{1}/replication_card_ids".format(collocation_path, collocation_id)) == [card2])
        _unbind(crt2, card2)
        wait(lambda: len(get(collocation_path)) == 0)

    @authors("savrus")
    @pytest.mark.parametrize("reshard", [True, False])
    @pytest.mark.parametrize("first", ["chaos", "replicated"])
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_switch_to_replicated_table(self, mode, first, reshard):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "c", "schema": schema})
        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")

        data_replicas = []
        data_replica_ids = []
        for replica, replica_id in zip(replicas, replica_ids):
            if replica["content_type"] == "data":
                data_replicas.append(replica)
                data_replica_ids.append(replica_id)

        sync_create_cells(1)
        create("replicated_table", "//tmp/rt", attributes={"schema": schema, "dynamic": True})
        if reshard:
            reshard_table("//tmp/rt", [[], [3], [6]])
        sync_mount_table("//tmp/rt")
        rt_replica_ids = []
        for replica in data_replicas:
            attributes = {"mode": replica["mode"]}
            replica_id = create_table_replica("//tmp/rt", replica["cluster_name"], replica["replica_path"], attributes=attributes)
            sync_enable_table_replica(replica_id)
            rt_replica_ids.append(replica_id)

        self._create_replica_tables(replicas, replica_ids, mount_tables=False)
        for replica in replicas:
            driver = get_driver(cluster=replica["cluster_name"])
            path = replica["replica_path"]
            if reshard:
                reshard_table(path, [[], [5]], driver=driver)
            if replica["content_type"] == "queue":
                sync_mount_table(path, driver=driver)

        def _check(keys, values):
            for replica in data_replicas:
                driver = get_driver(cluster=replica["cluster_name"])
                path = replica["replica_path"]
                if replica["mode"] == "sync":
                    assert lookup_rows(path, keys, driver=driver) == values
                else:
                    wait(lambda: lookup_rows(path, keys, driver=driver) == values)

        def _switch(new_replica_ids):
            for replica, new_id in zip(data_replicas, new_replica_ids):
                driver = get_driver(cluster=replica["cluster_name"])
                path = replica["replica_path"]
                sync_unmount_table(path, driver=driver)
                alter_table(path, upstream_replica_id=new_id, driver=driver)
                sync_mount_table(path, driver=driver)

        values = None
        keys = [{"key": i} for i in range(10)]

        for iteration in range(3):
            begin = 0 if first == "chaos" else 1
            if iteration % 2 == begin:
                _switch(data_replica_ids)
                self._sync_replication_era(card_id, replicas)
                table = "//tmp/crt"
            else:
                _switch(rt_replica_ids)
                table = "//tmp/rt"

            new_values = [{"key": i, "value": str(i + iteration)} for i in range(0, 10, iteration + 1)]
            if values is None:
                values = new_values
            else:
                for new_value in new_values:
                    for value in values:
                        if value["key"] == new_value["key"]:
                            value["value"] = new_value["value"]

            insert_rows(table, new_values)
            _check(keys, values)

    @authors("savrus")
    @pytest.mark.parametrize("tablet_count", [1, 2])
    @pytest.mark.parametrize("first", ["chaos", "replicated"])
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_ordered_switch_to_replicated_table(self, mode, first, tablet_count):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)
        schema = yson.YsonList([
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "c", "schema": schema})
        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "remote_0", "content_type": "queue", "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")

        sync_create_cells(1)
        create("replicated_table", "//tmp/rt", attributes={"schema": schema, "dynamic": True, "preserve_tablet_index": True})
        if tablet_count > 1:
            reshard_table("//tmp/rt", tablet_count)
        sync_mount_table("//tmp/rt")
        rt_replica_ids = []
        for replica in replicas:
            attributes = {"mode": replica["mode"]}
            replica_id = create_table_replica("//tmp/rt", replica["cluster_name"], replica["replica_path"], attributes=attributes)
            sync_enable_table_replica(replica_id)
            rt_replica_ids.append(replica_id)

        self._create_replica_tables(replicas, replica_ids, mount_tables=False, ordered=True)
        if tablet_count > 1:
            for replica in replicas:
                driver = get_driver(cluster=replica["cluster_name"])
                path = replica["replica_path"]
                reshard_table(path, tablet_count, driver=driver)

        def _check(tablet_index, values):
            import logging
            logger = logging.getLogger()
            logger.debug("Expected: {0}".format(values))

            for replica in replicas:
                driver = get_driver(cluster=replica["cluster_name"])
                path = replica["replica_path"]
                query = "key, value from [{0}] where [$tablet_index] = {1}". format(path, tablet_index)
                wait(lambda: select_rows(query, driver=driver) == values)

        def _switch(new_replica_ids, progress=None):
            for replica, new_id in zip(replicas, new_replica_ids):
                driver = get_driver(cluster=replica["cluster_name"])
                path = replica["replica_path"]
                sync_unmount_table(path, driver=driver)
                alter_table(path, upstream_replica_id=new_id, driver=driver)
                if progress:
                    alter_table(path, replication_progress=progress, driver=driver)
                sync_mount_table(path, driver=driver)

        values = [[] for i in range(tablet_count)]

        for iteration in range(3):
            if (iteration % 2 == 0) == (first == "chaos"):
                timestamp = generate_timestamp()
                progress = {
                    "segments": [{"lower_key": [], "timestamp": timestamp}],
                    "upper_key": MAX_KEY,
                }
                _switch(replica_ids, progress)
                self._sync_replication_era(card_id, replicas)
                table = "//tmp/crt"
            else:
                _switch(rt_replica_ids)
                table = "//tmp/rt"

            new_values = [{"$tablet_index": i % tablet_count, "key": i, "value": str(i + iteration)} for i in range(0, 10, iteration + 1)]
            for value in new_values:
                values[value["$tablet_index"]].append({"key": value["key"], "value": value["value"]})

            import logging
            logger = logging.getLogger()
            logger.debug("Inserting: {0}".format(new_values))

            insert_rows(table, new_values)
            for tablet_index in range(tablet_count):
                _check(tablet_index, values[tablet_index])

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("mode", ["data/ordered", "ordered/replication_log"])
    def test_content_type_incompatibility(self, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        content_type = "queue" if mode == "ordered/replication_log" else "data"

        replicas = [
            {"cluster_name": "primary", "content_type": content_type, "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, create_replica_tables=False, sync_replication_era=False)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        if mode == "ordered/replication_log":
            self._create_replica_tables(replicas[:1], replica_ids[:1], ordered=True)
            self._create_replica_tables(replicas[1:], replica_ids[1:])
        else:
            self._create_replica_tables(replicas[:1], replica_ids[:1])
            self._create_replica_tables(replicas[1:], replica_ids[1:], ordered=True)

        self._sync_replication_era(card_id, replicas)

        def _values():
            if mode == "ordered/replication_log":
                return [{"key": 0, "value": "0"}]
            else:
                return [{"key": 0, "value": "0", "$tablet_index": 0}]

        insert_rows("//tmp/q", _values())

        def _check():
            tablet_infos = get_tablet_infos("//tmp/t", [0], request_errors=True)
            errors = tablet_infos["tablets"][0]["tablet_errors"]

            if len(errors) != 1 or errors[0]["attributes"]["background_activity"] != "pull":
                return False

            return any(
                error["message"].startswith("Table schemas are incompatible")
                for error in errors[0]["inner_errors"]
            )

        wait(_check)

    @authors("savrus")
    @pytest.mark.parametrize("schemas", [
        ("sorted_simple", "sorted_value2"),
        ("sorted_simple", "sorted_key2"),
    ])
    @pytest.mark.parametrize("tablet_count", [1, 3])
    @pytest.mark.parametrize("mode", ["sync", "async", "async_queue"])
    def test_schema_compatibility(self, schemas, tablet_count, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        enabled = mode != "async_queue"
        replica_mode = "sync" if mode == "sync" else "async"

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": replica_mode, "enabled": enabled, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": replica_mode, "enabled": False, "replica_path": "//tmp/q"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, create_replica_tables=False, sync_replication_era=False)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        pivot_keys = [[]] + [[i] for i in range(1, tablet_count)]
        schema1, schema2 = self._get_schemas_by_name(schemas)
        self._create_replica_tables(replicas[:2], replica_ids[:2], schema=schema2, pivot_keys=pivot_keys)
        self._create_replica_tables(replicas[2:], replica_ids[2:], schema=schema1, pivot_keys=pivot_keys)

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)

        values = [{"key": i, "value": str(i)} for i in range(4)]
        keys = [{"key": i} for i in range(4)]
        insert_rows("//tmp/r", values, driver=remote_driver1, allow_missing_key_columns=True)

        extra_columns = list(builtins.set([c["name"] for c in schema2]) - builtins.set([c["name"] for c in schema1]))
        expected = deepcopy(values)
        for value in expected:
            value.update({c: None for c in extra_columns})

        if mode == "async_queue":
            self._sync_alter_replica(card_id, replicas, replica_ids, 1, mode="sync")
            self._sync_alter_replica(card_id, replicas, replica_ids, 2, enabled=False)
            self._sync_alter_replica(card_id, replicas, replica_ids, 0, enabled=True)

        wait(lambda: lookup_rows("//tmp/t", keys) == expected)

    @authors("savrus")
    @pytest.mark.parametrize("schemas", [
        ("sorted_value2", "sorted_simple"),
        ("sorted_key2", "sorted_simple"),
        ("sorted_simple", "sorted_hash"),
        ("sorted_hash", "sorted_simple"),
        ("sorted_key2", "sorted_key2_inverted")
    ])
    def test_schema_incompatibility(self, schemas):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, create_replica_tables=False, sync_replication_era=False)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        schema1, schema2 = self._get_schemas_by_name(schemas)
        self._create_replica_tables(replicas[:1], replica_ids[:1], schema=schema2)
        self._create_replica_tables(replicas[1:], replica_ids[1:], schema=schema1)
        self._sync_replication_era(card_id, replicas)

        def _create_data(schema_name):
            if schema_name == "sorted_key2":
                return [{"key": 0, "key2": 0, "value": str(0)}], [{"key": 0, "key2": 0}]
            else:
                return [{"key": 0, "value": str(0)}], [{"key": 0}]

        values, keys = _create_data(schemas[0])
        insert_rows("//tmp/r", values, driver=remote_driver1)

        def _check():
            tablet_infos = get_tablet_infos("//tmp/t", [0], request_errors=True)
            errors = tablet_infos["tablets"][0]["tablet_errors"]

            if len(errors) != 1 or errors[0]["attributes"]["background_activity"] != "pull":
                return False

            return any(
                error["message"].startswith("Table schemas are incompatible")
                for error in errors[0]["inner_errors"]
            )

        wait(_check)

        def are_keys_compatible():
            for index in range(len(schema2)):
                c2 = schema2[index]
                if "sort_order" not in c2:
                    break
                if index == len(schema1):
                    return False
                c1 = schema1[index]
                return c1["name"] == c2["name"] and c1["type"] == c2["type"]

        if not are_keys_compatible():
            with raises_yt_error("Table schemas are incompatible"):
                lookup_rows("//tmp/t", keys, replica_consistency="sync")
            with raises_yt_error("Table schemas are incompatible"):
                select_rows("* from [//tmp/t] where key = 0", replica_consistency="sync")

    @authors("savrus")
    @pytest.mark.parametrize("schemas", [
        ("ordered_value2", "ordered_simple"),
        ("ordered_simple", "ordered_simple_int"),
    ])
    def test_ordered_schema_incompatibility(self, schemas):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, create_replica_tables=False, sync_replication_era=False)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        schema1, schema2 = self._get_schemas_by_name(schemas)
        self._create_replica_tables(replicas[:1], replica_ids[:1], schema=schema2, ordered=True)
        self._create_replica_tables(replicas[1:], replica_ids[1:], schema=schema1, ordered=True)
        self._sync_replication_era(card_id, replicas)

        values = [{"$tablet_index": 0, "key": 0, "value": str(0)}]
        insert_rows("//tmp/r", values, driver=remote_driver1)

        def _check():
            tablet_infos = get_tablet_infos("//tmp/t", [0], request_errors=True)
            errors = tablet_infos["tablets"][0]["tablet_errors"]

            if len(errors) != 1 or errors[0]["attributes"]["background_activity"] != "pull":
                return False

            return any(
                error["message"].startswith("Table schemas are incompatible")
                for error in errors[0]["inner_errors"]
            )

        wait(_check)

    @authors("savrus")
    @pytest.mark.parametrize("schemas", [
        ("sorted_simple", "sorted_value2"),
        ("sorted_simple", "sorted_key2"),
        ("sorted_value2", "sorted_simple"),
        ("sorted_key2", "sorted_simple"),
        ("sorted_simple", "sorted_hash"),
        ("sorted_hash", "sorted_simple"),
        ("sorted_key2", "sorted_key2_inverted")
    ])
    @pytest.mark.parametrize("tablet_count", [1, 3])
    @pytest.mark.parametrize("mode", ["sync", "async", "async_queue"])
    def test_schema_coexistence(self, schemas, tablet_count, mode):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        enabled = mode != "async_queue"
        replica_mode = "sync" if mode == "sync" else "async"

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": replica_mode, "enabled": enabled, "replica_path": "//tmp/pd"},
            {"cluster_name": "primary", "content_type": "queue", "mode": replica_mode, "enabled": True, "replica_path": "//tmp/pqa"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/pqs"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/rqs"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": replica_mode, "enabled": enabled, "replica_path": "//tmp/rd"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": replica_mode, "enabled": True, "replica_path": "//tmp/rqa"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas[:3], create_replica_tables=False, sync_replication_era=False)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        def _create_pivots(schema):
            if schema[0]["type"] == "uint64":
                return [[]] + [[yson.YsonUint64(i)] for i in range(1, tablet_count)]
            else:
                return [[]] + [[i] for i in range(1, tablet_count)]

        schema1, schema2 = self._get_schemas_by_name(schemas)
        self._create_replica_tables(replicas[:3], replica_ids[:3], schema=schema2, pivot_keys=_create_pivots(schema2))

        replica_ids.append(self._create_chaos_table_replica(replicas[3], replication_card_id=card_id, catchup=False))
        self._create_replica_tables(replicas[3:4], replica_ids[3:4], schema=schema1, pivot_keys=_create_pivots(schema1))
        self._sync_replication_era(card_id)

        sync_unmount_table("//tmp/rqs", driver=remote_driver0)
        progress = get("#{0}/@replication_progress".format(replica_ids[3]))
        replica_ids.append(self._create_chaos_table_replica(replicas[4], replication_card_id=card_id, replication_progress=progress))
        replica_ids.append(self._create_chaos_table_replica(replicas[5], replication_card_id=card_id, replication_progress=progress))
        sync_mount_table("//tmp/rqs", driver=remote_driver0)
        self._create_replica_tables(replicas[4:], replica_ids[4:], schema=schema1, pivot_keys=_create_pivots(schema1), replication_progress=progress)
        self._sync_replication_era(card_id)

        assert get("//tmp/pd/@tablet_count") == tablet_count
        assert get("//tmp/pqs/@tablet_count") == tablet_count

        def _create_expected_data(schema_name):
            if schema_name in ["sorted_key2", "sorted_key2_inverted"]:
                return [{"key": 0, "key2": None, "value": str(0)}], [{"key": 0, "key2": None}]
            elif schema_name == "sorted_value2":
                return [{"key": 0, "value": str(0), "value2": None}], [{"key": 0}]
            elif schema_name == "sorted_hash":
                return [{"hash": yson.YsonUint64(3315701238936582721), "key": 0, "value": str(0)}], [{"key": 0}]
            else:
                return [{"key": 0, "value": str(0)}], [{"key": 0}]

        values = [{"key": 0, "value": str(0)}]
        insert_rows("//tmp/rd", values, driver=remote_driver0, allow_missing_key_columns=True, update=True)
        ts = generate_timestamp()

        if mode == "async_queue":
            alter_table_replica(replica_ids[1], mode="sync")
            alter_table_replica(replica_ids[5], mode="sync")
            self._sync_replication_era(card_id)
            alter_table_replica(replica_ids[2], enabled=False)
            alter_table_replica(replica_ids[3], enabled=False)
            alter_table_replica(replica_ids[0], enabled=True)
            alter_table_replica(replica_ids[4], enabled=True)
            self._sync_replication_era(card_id)

        def _filter(rows, schema):
            columns = builtins.set([c["name"] for c in schema])
            for row in rows:
                row_columns = list(row.keys())
                for column in row_columns:
                    if column not in columns:
                        assert row[column] == yson.to_yson_type(None)
                        del row[column]
            return rows

        def _check(lookup_call):
            if mode == "sync":
                assert lookup_call()
            else:
                def _do():
                    try:
                        return lookup_call()
                    except YtError as err:
                        print_debug("Lookup failed: ", err)
                        if err.is_no_in_sync_replicas() or err.contains_text("No single cluster contains in-sync replicas for all involved tables"):
                            return False
                        else:
                            raise err
                wait(_do)

        values, keys = _create_expected_data(schemas[0])
        wait(lambda: lookup_rows("//tmp/rd", keys, driver=remote_driver0) == values)
        _check(lambda: _filter(lookup_rows("//tmp/rd", keys, timestamp=ts, replica_consistency="sync", driver=remote_driver0), schema1) == values)
        _check(lambda: _filter(select_rows("* from [//tmp/rd]", timestamp=ts, replica_consistency="sync", driver=remote_driver0), schema1) == values)

        values, keys = _create_expected_data(schemas[1])
        wait(lambda: lookup_rows("//tmp/pd", keys) == values)
        _check(lambda: _filter(lookup_rows("//tmp/pd", keys, timestamp=ts, replica_consistency="sync"), schema2) == values)
        _check(lambda: _filter(select_rows("* from [//tmp/pd]", timestamp=ts, replica_consistency="sync"), schema2) == values)

    @authors("savrus")
    def test_schema_coexistence_in_join(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        def _create(prefix, schemas):
            data_schema, hash_schema = self._get_schemas_by_name(schemas)
            create("chaos_replicated_table", f"{prefix}.crt", attributes={"chaos_cell_bundle": "c", "schema": data_schema})
            card_id = get(f"{prefix}.crt/@replication_card_id")

            replicas = [
                {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": f"{prefix}.queue"},
                {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": f"{prefix}.data"},
                {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": f"{prefix}.queue_incompatible", "catchup": False},
                {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": f"{prefix}.data_incompatible", "catchup": False},
            ]
            replica_ids = self._create_chaos_table_replicas(replicas, table_path=f"{prefix}.crt")

            self._create_replica_tables(replicas[:2], replica_ids[:2], schema=data_schema)
            self._create_replica_tables(replicas[2:], replica_ids[2:], schema=hash_schema)
            self._sync_replication_era(card_id, replicas)

        _create("//tmp/t1", ["sorted_simple", "sorted_hash"])
        _create("//tmp/t2", ["sorted_value1", "sorted_hash_value1"])

        insert_rows("//tmp/t1.crt", [{"key": 0, "value": str(0)}])
        insert_rows("//tmp/t2.crt", [{"key": 0, "value1": str(1)}])

        assert select_rows("* from [//tmp/t1.crt] join [//tmp/t2.crt] using key") == [{"key": 0, "value": str(0), "value1": str(1)}]

    @authors("akozhikhov")
    def test_cumulative_data_weight_computation(self):
        self._init_replicated_table_tracker()
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        schema = yson.YsonList([
            {"name": "value", "type": "string"},
            {"name": "$timestamp", "type": "uint64"},
            {"name": "$cumulative_data_weight", "type": "int64"},
        ])

        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "c",
            "schema": schema,
        })
        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/q"},
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, ordered=True, schema=schema)

        self._sync_replication_era(card_id)

        values = [{"$tablet_index": 0, "value": str(i)} for i in range(1)]
        insert_rows("//tmp/t", values)

        def _check(replica, expected):
            driver = get_driver(cluster=replica["cluster_name"])
            select_result = select_rows("[$cumulative_data_weight] from [{0}]".format(
                replica["replica_path"]),
                driver=driver)
            if not select_result:
                return False
            return select_result[-1]["$cumulative_data_weight"] == expected

        for replica in replicas:
            wait(lambda: _check(replica, 18))

        replicas.append({
            "cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/q1",
        })
        replicas.append({
            "cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/q2",
        })

        new_replica_ids = [
            self._create_chaos_table_replica(replicas[-2], table_path="//tmp/crt", catchup=False),
            self._create_chaos_table_replica(replicas[-1], table_path="//tmp/crt", catchup=False),
        ]
        self._create_replica_tables(replicas[-2:], new_replica_ids, ordered=True, schema=schema)

        self._sync_replication_era(card_id)

        values = [{"$tablet_index": 0, "value": str(i)} for i in range(1, 2)]
        insert_rows("//tmp/t", values)

        for replica in replicas[:-2]:
            wait(lambda: _check(replica, 36))
        for replica in replicas[-2:]:
            wait(lambda: _check(replica, 18))

    @authors("osidorkin")
    def test_crt_creation_under_transaction(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        data_schema = self._get_schemas_by_name(["sorted_simple"])[0]

        tx = start_transaction(type="master")
        with pytest.raises(YtError, match="Replicated table cannot be created inside a transaction"):
            create("chaos_replicated_table", "//tmp/t1.crt", attributes={"chaos_cell_bundle": "c", "schema": data_schema}, tx=tx)
        abort_transaction(tx)

        create("chaos_replicated_table", "//tmp/t1.crt", attributes={"chaos_cell_bundle": "c", "schema": data_schema})

    @authors("osidorkin")
    def test_crt_tablets_count(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        queue_schema = self._get_schemas_by_name(["ordered_simple"])[0]
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "c", "schema": queue_schema})

        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q2"},
        ]

        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, ordered=True, schema=queue_schema)

        self._sync_replication_era(card_id)

        assert get("//tmp/crt/@tablet_count") == 1
        assert get_table_mount_info("//tmp/crt")["upper_cap_bound"][0] == 1
        primary_driver, remote_driver0, remote_driver1 = self._get_drivers()

        def _reshard_table(path: str, driver) -> None:
            sync_unmount_table(path, driver=driver)
            reshard_table(path, 5, driver=driver)
            sync_mount_table(path, driver=driver)

        _reshard_table("//tmp/q1", remote_driver0)
        assert get("//tmp/crt/@tablet_count") == 1
        assert get_table_mount_info("//tmp/crt")["upper_cap_bound"][0] == 1

        _reshard_table("//tmp/q2", remote_driver1)
        assert get("//tmp/crt/@tablet_count") == 5
        assert get_table_mount_info("//tmp/crt")["upper_cap_bound"][0] == 5

        _reshard_table("//tmp/q0", primary_driver)
        assert get("//tmp/crt/@tablet_count") == 5
        assert get_table_mount_info("//tmp/crt")["upper_cap_bound"][0] == 5

    @authors("osidorkin")
    def test_crt_tablets_info(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        queue_schema = self._get_schemas_by_name(["ordered_simple"])[0]
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "c", "schema": queue_schema})

        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q2"},
        ]

        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, ordered=True, schema=queue_schema)

        self._sync_replication_era(card_id)
        assert get("//tmp/crt/@tablet_count") == 1

        values = [{"$tablet_index": 0, "key": 0, "value": str(0)}]
        tx = start_transaction(type="tablet")
        insert_rows("//tmp/crt", values, tx=tx)
        commit_res = commit_transaction(tx)
        primary_commit_timestamp = commit_res["primary_commit_timestamp"]

        # Waitng for transaction serialization so total row count is updated
        wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[1]}/replication_lag_timestamp") >= primary_commit_timestamp)
        wait(lambda: get(f"//tmp/crt/@replicas/{replica_ids[2]}/replication_lag_timestamp") >= primary_commit_timestamp)

        tablet_infos = get_tablet_infos("//tmp/crt", [0])["tablets"]
        assert len(tablet_infos) == 1
        assert tablet_infos[0]["last_write_timestamp"] >= primary_commit_timestamp
        assert tablet_infos[0]["total_row_count"] == 1
        assert tablet_infos[0]["trimmed_row_count"] == 0

        # Prevent trimmig errors due to replication lag
        self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode="sync")

        trim_rows("//tmp/q0", 0, 1)
        trim_rows("//tmp/q1", 0, 1)
        trim_rows("//tmp/q2", 0, 1)

        tablet_infos = get_tablet_infos("//tmp/crt", [0])["tablets"]
        assert len(tablet_infos) == 1
        assert tablet_infos[0]["total_row_count"] == 1
        assert tablet_infos[0]["trimmed_row_count"] == 1

    @authors("sabdenovch")
    def test_chaos_async_hint(self):
        remote_driver = get_driver(cluster="remote_0")
        sync_create_cells(1)
        sync_create_cells(1, driver=remote_driver)

        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ])

        for mode in ("sync", "async"):
            path = "//tmp/chaos_" + mode
            create(
                "chaos_replicated_table",
                path,
                attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

            replicas = [
                {
                    "cluster_name": "remote_0",
                    "content_type": "data",
                    "mode": mode,
                    "enabled": True,
                    "replica_path": f"{path}_data",
                },
                {
                    "cluster_name": "remote_0",
                    "content_type": "queue",
                    "mode": "sync",
                    "enabled": True,
                    "replica_path": f"{path}_queue",
                },
            ]
            replica_ids = self._create_chaos_table_replicas(replicas, table_path=path)
            self._create_replica_tables(replicas, replica_ids, schema=schema)

            card_id = get(f"{path}/@replication_card_id")
            self._sync_replication_era(card_id, replicas)

            path = "//tmp/regular_" + mode
            create("replicated_table", path, attributes={"schema": schema, "dynamic": True})
            replica_id = create_table_replica(path, "remote_0", f"{path}_replica", attributes={"mode": mode})
            create(
                "table",
                f"{path}_replica",
                attributes={"dynamic": True, "schema": schema, "upstream_replica_id": replica_id},
                driver=remote_driver)
            sync_enable_table_replica(replica_id)
            sync_mount_table(path)
            sync_mount_table(f"{path}_replica", driver=remote_driver)

        for kind in ("chaos", "regular"):
            for mode in ("sync", "async"):
                path = f"//tmp/{kind}_{mode}"
                insert_rows(path, [{"key": 0, "value": 1}, {"key": 1, "value": 10}], require_sync_replica=False)

        hint = "with hint \"{require_sync_replica=%false;}\""

        for left_mode in ("sync", "async"):
            for right_mode in ("sync", "async"):
                for left_kind in ("chaos", "regular"):
                    for right_kind in ("chaos", "regular"):
                        left_path = f"//tmp/{left_kind}_{left_mode}"
                        right_path = f"//tmp/{right_kind}_{right_mode}"
                        left_hint = hint if left_mode == "async" else ""
                        right_hint = hint if right_mode == "async" else ""

                        result = select_rows(f"""
                            T.value + D.value as s from [{left_path}] AS T {left_hint}
                            join [{right_path}] AS D {right_hint}
                            on T.key + 1 = D.key""")

                        assert len(result) in (0, 1)
                        if result:
                            assert result[0]["s"] == 11

                        select_rows(f"* from [{left_path}] {hint} join [{right_path}] {hint} using key, value")

                        if left_mode == "async" or right_mode == "async":
                            with raises_yt_error(yt_error_codes.NoInSyncReplicas):
                                select_rows(f"* from [{left_path}] join [{right_path}] using key, value")

    @authors("sabdenovch")
    def test_chaos_async_preference(self):
        _, remote_driver_0, _ = self._get_drivers()
        cell_id = self._sync_create_chaos_bundle_and_cell(name="chaos_bundle")
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ])
        path = "//tmp/t"
        create(
            "chaos_replicated_table",
            path,
            attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

        replicas = [
            {
                "cluster_name": "remote_0",
                "content_type": "data",
                "mode": "async",
                "enabled": True,
                "replica_path": f"{path}_data_abel",
            },
            {
                "cluster_name": "remote_1",
                "content_type": "data",
                "mode": "async",
                "enabled": False,
                "replica_path": f"{path}_data_cain",
            },
            {
                "cluster_name": "remote_0",
                "content_type": "queue",
                "mode": "sync",
                "enabled": True,
                "replica_path": f"{path}_queue",
            },
        ]

        replica_ids = self._create_chaos_table_replicas(replicas, table_path=path)
        self._create_replica_tables(replicas, replica_ids, schema=schema)

        card_id = get(f"{path}/@replication_card_id")
        self._sync_replication_era(card_id, replicas)

        insert_rows(path, [{"key": 0, "value": 0}], require_sync_replica=False)
        wait(lambda: lookup_rows(f"{path}_data_abel", [{"key": 0}], driver=remote_driver_0))

        self._sync_alter_replica(card_id, replicas, replica_ids, 1, enabled=True)

        hint = "\"{require_sync_replica=%false;}\""
        assert select_rows(f"T.value AS v from [{path}] AS T with hint {hint}") == [{"v": 0}]


##################################################################


class TestChaosRpcProxy(TestChaos):
    ENABLE_MULTIDAEMON = False  # There are components restarts.
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    DELTA_RPC_DRIVER_CONFIG = {
        "table_mount_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
    }

    @authors("savrus")
    def test_insert_first(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        _, remote_driver0, remote_driver1 = self._get_drivers()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(cell_id, replicas)

        with Restarter(self.Env, RPC_PROXIES_SERVICE):
            pass

        def _check():
            try:
                return exists("/")
            except YtError as err:
                print_debug(err)
                return False

        wait(_check)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values)


##################################################################


@pytest.mark.enabled_multidaemon
class TestChaosNativeProxy(ChaosTestBase):
    ENABLE_MULTIDAEMON = True
    NUM_REMOTE_CLUSTERS = 0
    NUM_NODES = 5

    @authors("osidorkin")
    def test_partial_pull_rows(self):
        metadata_cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", metadata_cell_id)

        create_tablet_cell_bundle("b")
        custom_bundle_id = get("//sys/tablet_cell_bundles/b/@id")

        b_area_id = create_area("b_area", cell_bundle_id=custom_bundle_id)
        create_area("a_area", cell_bundle_id=custom_bundle_id)

        cell_ids = sync_create_cells(3, tablet_cell_bundle="b", area="a_area")
        custom_area_cell_id = sync_create_cells(1, tablet_cell_bundle="b", area="b_area")[0]

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/pds"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/pqs"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": False, "replica_path": "//tmp/pqa"},
        ]

        card_id, replica_ids = self._create_chaos_tables(metadata_cell_id, replicas, create_tablet_cells=False, create_replica_tables=False, sync_replication_era=False)

        self._create_replica_tables([replicas[0]], [replica_ids[0]], create_tablet_cells=False, mount_tables=False, tablet_cell_bundle="b")
        sync_mount_table(replicas[0]["replica_path"], driver=get_driver(cluster=replicas[0]["cluster_name"]), target_cell_ids=[cell_ids[0]])

        self._create_replica_tables([replicas[2]], [replica_ids[2]], create_tablet_cells=False, mount_tables=False, tablet_cell_bundle="b")
        sync_mount_table(replicas[2]["replica_path"], driver=get_driver(cluster=replicas[2]["cluster_name"]), target_cell_ids=[cell_ids[1]])

        self._create_replica_tables([replicas[1]], [replica_ids[1]], pivot_keys=[[], [1]], create_tablet_cells=False, mount_tables=False, tablet_cell_bundle="b")
        sync_mount_table(
            replicas[1]["replica_path"],
            driver=get_driver(cluster=replicas[1]["cluster_name"]),
            target_cell_ids=[custom_area_cell_id, cell_ids[2]]
        )

        self._sync_replication_era(card_id, replicas)

        row1 = {"key": 0, "value": "0"}
        row2 = {"key": 2, "value": "2"}

        insert_rows("//tmp/pds", [row1, row2])

        def _pull_rows(progress_timestamp, response_parameters):
            return pull_rows(
                "//tmp/pqs",
                replication_progress={
                    "segments": [{"lower_key": [], "timestamp": progress_timestamp}],
                    "upper_key": [yson.to_yson_type(None, attributes={"type": "max"})]
                },
                upstream_replica_id=replica_ids[1],
                response_parameters=response_parameters)

        def _sync_pull_rows(progress_timestamp, expected_rows, response_parameters):
            wait(lambda: len(_pull_rows(progress_timestamp, None)) >= len(expected_rows))

            rows = _pull_rows(progress_timestamp, response_parameters)

            print_debug("Replication progress:", response_parameters)

            for row, expected_row in zip(rows, expected_rows):
                assert row["key"] == expected_row["key"]
                assert str(row["value"][0]) == expected_row["value"]

        response_parameters = {}
        _sync_pull_rows(0, [row1, row2], response_parameters)

        assert len(response_parameters["replication_progress"]["segments"]) > 0
        assert response_parameters["replication_progress"]["segments"][0]["lower_key"] == []
        for segment in response_parameters["replication_progress"]["segments"]:
            assert segment["timestamp"] > 0

        response_parameters = {}

        with self.CellsDisabled(clusters=["primary"], area_ids=[b_area_id]):
            _sync_pull_rows(0, [row2], response_parameters)

        assert len(response_parameters["replication_progress"]["segments"]) == 2
        assert response_parameters["replication_progress"]["segments"][0]["lower_key"] == []
        assert response_parameters["replication_progress"]["segments"][0]["timestamp"] == 0
        assert response_parameters["replication_progress"]["segments"][1]["timestamp"] > 0


##################################################################


class TestChaosRpcProxyWithReplicationCardCache(ChaosTestBase):
    NUM_REMOTE_CLUSTERS = 1
    NUM_SCHEDULERS = 1

    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    DELTA_RPC_DRIVER_CONFIG = {
        "table_mount_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
    }
    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "replication_card_cache": {
                "expire_after_successful_update_time": 60000,
                "expire_after_failed_update_time": 60000,
                "expire_after_access_time": 60000,
                "refresh_time": 10000,
                "soft_backoff_time": 10000,
                "hard_backoff_time":  10000,
                "enable_watching": True,
            },
        },
    }
    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "replication_card_cache": {
                "expire_after_successful_update_time": 60000,
                "expire_after_failed_update_time": 60000,
                "expire_after_access_time": 60000,
                "refresh_time": 10000,
                "soft_backoff_time": 10000,
                "hard_backoff_time":  10000,
                "enable_watching": True,
            },
        },
    }

    def setup_method(self, method):
        super().setup_method(method)

        primary_cell_tag = get("//sys/@primary_cell_tag")
        for driver in self._get_drivers():
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", primary_cell_tag, driver=driver)

    @authors("osidorkin")
    def test_multitable_transactions(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        remote_driver0 = self._get_drivers()[1]

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])

        replicas1 = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/ts1"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/ta1"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"}
        ]

        replicas2 = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/ts2"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/ta2"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q2"}
        ]

        create("chaos_replicated_table", "//tmp/crt1", attributes={"chaos_cell_bundle": "c", "schema": schema})
        create("chaos_replicated_table", "//tmp/crt2", attributes={"chaos_cell_bundle": "c", "schema": schema})

        replica_ids1 = self._create_chaos_table_replicas(replicas1, table_path="//tmp/crt1")
        replica_ids2 = self._create_chaos_table_replicas(replicas2, table_path="//tmp/crt2")

        self._create_replica_tables(replicas1, replica_ids1)
        self._create_replica_tables(replicas2, replica_ids2)

        self._sync_replication_era(get("//tmp/crt1/@replication_card_id"), replicas1)
        self._sync_replication_era(get("//tmp/crt2/@replication_card_id"), replicas2)

        values = [{"key": 0, "value": "0"}]
        values1 = [{"key": 1, "value": "1"}]
        values2 = [{"key": 2, "value": "2"}]

        def _insert_rows_with_retries(v, replica_ids_to_alter=None):
            need_to_alter = replica_ids_to_alter is not None
            for _ in range(1, 100):
                try:
                    tx = start_transaction(type="tablet")
                    # Altering AFTER transaction is started is essential
                    if need_to_alter:
                        alter_table_replica(replica_ids_to_alter[1], mode="sync", driver=remote_driver0)
                        need_to_alter = False

                    insert_rows("//tmp/ts2", v, tx=tx)
                    insert_rows("//tmp/ts1", v, tx=tx)

                    commit_transaction(tx)
                    break

                except (YtResponseError):
                    pass

            else:
                raise Exception("Failed to insert rows")

        _insert_rows_with_retries(values)
        assert lookup_rows("//tmp/ts2", [{"key": 0}]) == values

        _insert_rows_with_retries(values1, replica_ids_to_alter=replica_ids2)
        assert lookup_rows("//tmp/ts2", [{"key": 1}]) == values1
        assert lookup_rows("//tmp/ta2", [{"key": 1}], driver=remote_driver0) == values1

        _insert_rows_with_retries(values2, replica_ids_to_alter=replica_ids1)
        assert lookup_rows("//tmp/ta1", [{"key": 2}], driver=remote_driver0) == values2
        assert lookup_rows("//tmp/ta2", [{"key": 2}], driver=remote_driver0) == values2


##################################################################


class TestChaosMulticell(TestChaos):
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


class TestChaosMetaCluster(ChaosTestBase):
    NUM_REMOTE_CLUSTERS = 3
    NUM_CHAOS_NODES = 2

    DELTA_CHAOS_NODE_CONFIG = {
        "chaos_node": {
            "chaos_manager": {
                "foreign_migrated_replication_card_remover": {
                    "remove_period": 1000,
                    "replication_card_keep_alive_period": 0,
                },
                "leftover_migration_period": 5,
            },
        },
    }

    def _create_dedicated_areas_and_cells(self, name="c"):
        align_chaos_cell_tag()

        cluster_names = self.get_cluster_names()
        meta_cluster_names = cluster_names[:-2] + cluster_names[-1:]
        peer_cluster_names = cluster_names[-2:-1]
        create_chaos_cell_bundle(
            name,
            peer_cluster_names,
            meta_cluster_names=meta_cluster_names,
            independent_peers=False)
        default_cell_id = self._sync_create_chaos_cell(
            name=name,
            peer_cluster_names=peer_cluster_names,
            meta_cluster_names=meta_cluster_names)

        meta_cluster_names = cluster_names[:-1]
        peer_cluster_names = cluster_names[-1:]
        create_chaos_area(
            "beta",
            name,
            peer_cluster_names,
            meta_cluster_names=meta_cluster_names)
        beta_cell_id = self._sync_create_chaos_cell(
            name=name,
            peer_cluster_names=peer_cluster_names,
            meta_cluster_names=meta_cluster_names,
            area="beta")

        return [default_cell_id, beta_cell_id]

    @authors("babenko")
    def test_meta_cluster(self):
        cluster_names = self.get_cluster_names()

        peer_cluster_names = cluster_names[1:]
        meta_cluster_names = [cluster_names[0]]

        cell_id = self._sync_create_chaos_bundle_and_cell(peer_cluster_names=peer_cluster_names, meta_cluster_names=meta_cluster_names)

        card_id = create_replication_card(chaos_cell_id=cell_id)
        replicas = [
            {"cluster_name": "remote_0", "content_type": "data", "mode": "sync", "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "replica_path": "//tmp/r1"},
            {"cluster_name": "remote_2", "content_type": "data", "mode": "async", "replica_path": "//tmp/r2"}
        ]
        self._create_chaos_table_replicas(replicas, replication_card_id=card_id)

        card = get("#{0}/@".format(card_id))
        assert card["type"] == "replication_card"
        assert card["id"] == card_id
        assert len(card["replicas"]) == 3

    @authors("ponasenko-rs")
    def test_metadata_cell_ids_rotate(self):
        [alpha_cell, beta_cell] = self._create_dedicated_areas_and_cells()
        remote_driver2 = get_driver(cluster="remote_2")

        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", alpha_cell)
        assert get("//sys/chaos_cell_bundles/c/@metadata_cell_id") == alpha_cell
        assert get("//sys/chaos_cell_bundles/c/@metadata_cell_ids") == [alpha_cell]

        set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", [beta_cell, alpha_cell])
        assert get("//sys/chaos_cell_bundles/c/@metadata_cell_id") == beta_cell
        assert get("//sys/chaos_cell_bundles/c/@metadata_cell_ids") == [beta_cell, alpha_cell]

        set("//sys/@config/chaos_manager/enable_metadata_cells", False, driver=remote_driver2)
        wait(lambda: get("//sys/chaos_cell_bundles/c/@metadata_cell_id") == alpha_cell)
        assert get("//sys/chaos_cell_bundles/c/@metadata_cell_ids") == [beta_cell, alpha_cell]

        set("//sys/@config/chaos_manager/enable_metadata_cells", True, driver=remote_driver2)
        wait(lambda: get("//sys/chaos_cell_bundles/c/@metadata_cell_id") == beta_cell)
        assert get("//sys/chaos_cell_bundles/c/@metadata_cell_ids") == [beta_cell, alpha_cell]

    @authors("ponasenko-rs")
    def test_metadata_cell_ids_use(self):
        [alpha_cell, beta_cell] = self._create_dedicated_areas_and_cells()

        with pytest.raises(YtError):
            set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", ["surely not guid"])

        with pytest.raises(YtError):
            set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", ["1-2-3-4"])

        with pytest.raises(YtError):
            set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", [alpha_cell, beta_cell, alpha_cell])

        # use the same bundle and area as were created in _create_dedicated_areas_and_cells
        cluster_names = self.get_cluster_names()
        alpha_meta_cluster_names = cluster_names[:-2] + cluster_names[-1:]
        alpha_peer_cluster_names = cluster_names[-2:-1]

        another_alpha_cell = self._sync_create_chaos_cell(
            meta_cluster_names=alpha_meta_cluster_names,
            peer_cluster_names=alpha_peer_cluster_names
        )

        with pytest.raises(YtError):
            set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", [beta_cell, another_alpha_cell])

        set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", [alpha_cell])
        set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", [beta_cell])
        set("//sys/chaos_cell_bundles/c/@metadata_cell_ids", [alpha_cell, beta_cell])

    @authors("savrus")
    def test_dedicated_areas(self):
        cells = self._create_dedicated_areas_and_cells()
        drivers = self._get_drivers()

        def _is_alien(cell, driver):
            return "alien" in get("//sys/chaos_cells/{0}/@peers/0".format(cell), driver=driver)

        assert _is_alien(cells[0], drivers[-1])
        assert _is_alien(cells[1], drivers[-2])
        assert not _is_alien(cells[0], drivers[-2])
        assert not _is_alien(cells[1], drivers[-1])

        areas = get("//sys/chaos_cell_bundles/c/@areas")
        assert len(areas) == 2
        assert all(area["cell_count"] == 1 for area in areas.values())

        def _get_coordinators(cell, driver):
            peer = get("//sys/chaos_cells/{0}/@peers/0/address".format(cell), driver=driver)
            return get("//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}/chaos_manager/coordinators".format(peer, cell), driver=driver)

        wait(lambda: len(_get_coordinators(cells[0], drivers[-2])) == 2)
        wait(lambda: len(_get_coordinators(cells[1], drivers[-1])) == 2)

    @authors("savrus")
    def test_dedicated_chaos_table(self):
        cells = self._create_dedicated_areas_and_cells()
        drivers = self._get_drivers()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(cells[0], replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=drivers[2]) == values)

    @authors("savrus")
    @pytest.mark.parametrize("method", ["migrate", "suspend"])
    def test_replication_card_migration(self, method):
        cells = self._create_dedicated_areas_and_cells()
        drivers = self._get_drivers()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cells[0], replicas)

        def _get_orchid_path(cell_id, driver=None):
            address = get("#{0}/@peers/0/address".format(cell_id), driver=driver)
            return "//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}".format(address, cell_id)

        def _migrate(cell_id, card_ids, driver):
            if method == "migrate":
                migrate_replication_cards(cell_id, card_ids)
            else:
                suspend_chaos_cells([cell_id])
                suspended_path = "{0}/chaos_manager/internal/suspended".format(_get_orchid_path(cell_id, driver=driver))
                wait(lambda: get(suspended_path, driver=driver))
                resume_chaos_cells([cell_id])
                assert not get(suspended_path, driver=driver)

        def _migrated(migrated_card_path, origin, driver=None):
            def _checkable():
                try:
                    return get("{0}/state".format(migrated_card_path), driver=driver) == "migrated"
                except YtError as err:
                    if not origin and err.contains_text("Node has no child with key \"{0}\"".format(card_id)):
                        return True
                    raise err

            return _checkable

        _migrate(cells[0], [card_id], drivers[-2])

        migration_path = "{0}/chaos_manager/replication_cards/{1}".format(_get_orchid_path(cells[0], driver=drivers[-2]), card_id)
        wait(_migrated(migration_path, origin=True, driver=drivers[-2]))

        migrated_card_path = "{0}/chaos_manager/replication_cards/{1}".format(_get_orchid_path(cells[1], driver=drivers[-1]), card_id)
        wait(lambda: exists(migrated_card_path, driver=drivers[-1]))

        self._sync_replication_era(card_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=drivers[2]) == values)

        _migrate(cells[1], [card_id], drivers[-1])
        wait(lambda: get("{0}/state".format(migration_path), driver=drivers[-2]) == "normal")
        wait(_migrated(migrated_card_path, origin=False, driver=drivers[-1]))
        wait(lambda: not exists(migrated_card_path, driver=drivers[-1]))

        self._sync_replication_era(card_id, replicas)

        values = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 1}], driver=drivers[2]) == values)

    @authors("ponasenko-rs")
    def test_leftovers_migration(self):
        [alpha_cell, beta_cell] = self._create_dedicated_areas_and_cells()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", alpha_cell)

        _, _, remote_driver1, remote_driver2 = self._get_drivers()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(alpha_cell, replicas)

        def _get_orchid_path(cell_id, driver=None):
            address = get("#{0}/@peers/0/address".format(cell_id), driver=driver)
            return "//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}".format(address, cell_id)

        orchids_paths = {
            cell_id: _get_orchid_path(cell_id, driver=driver)
            for cell_id, driver in zip([alpha_cell, beta_cell], [remote_driver1, remote_driver2])
        }

        suspend_chaos_cells([alpha_cell])
        suspended_path = f"{orchids_paths[alpha_cell]}/chaos_manager/internal/suspended"
        wait(lambda: get(suspended_path, driver=remote_driver1))

        with pytest.raises(YtError):
            create_replication_card(chaos_cell_id=alpha_cell)
        create_replication_card(chaos_cell_id=alpha_cell, attributes={'bypass_suspended': True})

        wait(lambda: get(suspended_path, driver=remote_driver1))

        migration_replication_cards_path = f"{orchids_paths[alpha_cell]}/chaos_manager/replication_cards"
        assert all(
            get(f"{migration_replication_cards_path}/{replication_card_id}", driver=remote_driver1)["state"] == "migrated"
            for replication_card_id in get(migration_replication_cards_path, driver=remote_driver1)
        )

        migrated_replication_cards_path = f"{orchids_paths[beta_cell]}/chaos_manager/replication_cards"
        assert all(
            get(f"{migrated_replication_cards_path}/{replication_card_id}", driver=remote_driver2)["state"]
            in
            ('normal', 'generating_timestamp_for_new_era')
            for replication_card_id in get(migrated_replication_cards_path, driver=remote_driver2)
        )

        # Test internal orchid.
        beta_cell_states = get(f"{orchids_paths[beta_cell]}/chaos_manager/internal/replication_card_states", driver=remote_driver2)
        assert beta_cell_states["normal"] == 1 and beta_cell_states["generating_timestamp_for_new_era"] == 1
        assert get(f"{orchids_paths[alpha_cell]}/chaos_manager/internal/replication_card_states", driver=remote_driver1)["migrated"] == 2

        resume_chaos_cells([alpha_cell])

        suspend_chaos_cells([beta_cell])

        suspended_path = f"{orchids_paths[beta_cell]}/chaos_manager/internal/suspended"
        wait(lambda: get(suspended_path, driver=remote_driver2))
        alpha_cell_states = get(f"{orchids_paths[alpha_cell]}/chaos_manager/internal/replication_card_states", driver=remote_driver1)
        beta_cell_states = get(f"{orchids_paths[beta_cell]}/chaos_manager/internal/replication_card_states", driver=remote_driver2)
        assert alpha_cell_states["normal"] == 1 and alpha_cell_states["generating_timestamp_for_new_era"] == 1
        # NB: Foreign migrated replication cards could be removed.
        assert beta_cell_states["migrated"] == sum(beta_cell_states.values()) and beta_cell_states["migrated"] <= 2

    @authors("savrus")
    def test_remove_migrated_replication_card(self):
        cells = self._create_dedicated_areas_and_cells()
        drivers = self._get_drivers()
        cluster_names = self.get_cluster_names()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cells[0], replicas, create_replica_tables=False, sync_replication_era=False)
        self._sync_replication_card(card_id)

        migrate_replication_cards(cells[0], [card_id])

        def _get_orchid_path(cell_id, driver=None):
            address = get("#{0}/@peers/0/address".format(cell_id), driver=driver)
            return "//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}".format(address, cell_id)

        migration_path = "{0}/chaos_manager/replication_cards/{1}/state".format(_get_orchid_path(cells[0], driver=drivers[-2]), card_id)
        wait(lambda: get(migration_path, driver=drivers[-2]) == "migrated")

        migrated_card_path = "{0}/chaos_manager/replication_cards/{1}".format(_get_orchid_path(cells[1], driver=drivers[-1]), card_id)
        wait(lambda: exists(migrated_card_path, driver=drivers[-1]))

        area_id = get("#{0}/@area_id".format(cells[0]), driver=drivers[-2])
        with self.CellsDisabled(clusters=cluster_names[-2:-1], area_ids=[area_id]):
            remove("#{0}".format(card_id))
            with pytest.raises(YtError):
                exists("#{0}".format(card_id))

        wait_for_chaos_cell(cells[0], cluster_names[-2:-1])

        def _check():
            try:
                return not exists("#{0}".format(card_id))
            except YtError as err:
                print_debug("Checking if replication card {0} exists failed".format(card_id), err)
                return False
        wait(_check)

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("migrate", ["migrate", "stay"])
    def test_remove_crt_with_migrated_replication_card(self, migrate):
        cells = self._create_dedicated_areas_and_cells()
        drivers = self._get_drivers()

        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        card_id = create_replication_card(chaos_cell_id=cells[0])

        create(
            "chaos_replicated_table",
            "//tmp/crt",
            attributes={"chaos_cell_bundle": "c", "schema": schema, "replication_card_id": card_id}
        )

        if migrate == "migrate":
            self._sync_migrate_replication_cards(
                cells[0],
                [card_id],
                cells[1],
                origin_driver=drivers[-2],
                destination_driver=drivers[-1]
            )

        remove("//tmp/crt")

        def _check():
            try:
                return not exists(f"#{card_id}")
            except YtError as err:
                print_debug(f"Replication card {card_id} exists failed", err)
                return False
        wait(_check)

    @authors("savrus")
    @pytest.mark.parametrize("alter", ["alter", "noalter"])
    def test_alter_replica_during_coordinator_suspension(self, alter):
        cells = self._create_dedicated_areas_and_cells()
        drivers = self._get_drivers()
        cluster_names = self.get_cluster_names()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cells[1], replicas)
        assert_items_equal(get("#{0}/@coordinator_cell_ids".format(card_id)), cells)

        def _check(value):
            values = [{"key": 0, "value": str(value)}]
            insert_rows("//tmp/t", values)
            wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        def _alter(mode):
            if alter == "alter":
                self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode=mode)

        _check(0)

        suspend_coordinator(cells[0])
        wait(lambda: get("#{0}/@coordinator_cell_ids".format(card_id)) == [cells[1]])

        area_id = get("#{0}/@area_id".format(cells[0]), driver=drivers[-2])
        with self.CellsDisabled(clusters=cluster_names[-2:-1], area_ids=[area_id]):
            _alter("async")
            _check(1)

        resume_coordinator(cells[0])
        wait(lambda: cells[0] in get("#{0}/@coordinator_cell_ids".format(card_id)))
        _alter("sync")
        _check(2)

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("action", ["migration", "write"])
    def test_client_keeps_connection_to_chaos_cell(self, action):
        [alpha_cell, beta_cell] = self._create_dedicated_areas_and_cells()
        remote_driver2 = get_driver(cluster="remote_2")

        nodes = ls("//sys/chaos_nodes", driver=remote_driver2)
        assert len(nodes) == 2

        beta_area_id = get("//sys/chaos_cell_bundles/c/@areas/beta/id", driver=remote_driver2)

        set(f"//sys/chaos_nodes/{nodes[0]}/@user_tags", ["custom"], driver=remote_driver2)
        set(f"#{beta_area_id}/@node_tag_filter", "custom", driver=remote_driver2)

        def cell_located_on_node(cell, node, driver=None):
            def get_peer():
                peers = get(f"//sys/chaos_cells/{cell}/@peers", driver=driver)
                assert len(peers) == 1
                peer = peers[0]
                assert "alien" not in peer
                return peer

            def checkable():
                peer = get_peer()
                return "address" in peer and peer["address"] == node
            return checkable

        wait(cell_located_on_node(beta_cell, nodes[0], driver=remote_driver2))

        card_id1 = create_replication_card(chaos_cell_id=beta_cell)

        def values(key: int):
            return [{"key": key, "value": str(key)}]

        if action == "migration":
            migrate_replication_cards(beta_cell, [card_id1], destination_cell_id=alpha_cell)

            card_id2 = create_replication_card(chaos_cell_id=beta_cell)
            card_id3 = create_replication_card(chaos_cell_id=beta_cell)
        else:
            replicas = [
                {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
                {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/r"},
                {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"},
            ]
            replica_ids = self._create_chaos_table_replicas(replicas, replication_card_id=card_id1)
            self._create_replica_tables(replicas, replica_ids)
            self._sync_replication_era(card_id1, replicas)

            insert_rows("//tmp/t", values(0))

        set(f"#{beta_area_id}/@node_tag_filter", "!custom", driver=remote_driver2)
        wait(cell_located_on_node(beta_cell, nodes[1], driver=remote_driver2))

        assert exists(f"#{card_id1}")

        if action == "migration":
            assert exists(f"#{card_id2}")
            assert exists(f"#{card_id3}")

            migrate_replication_cards(beta_cell, [card_id2], destination_cell_id=alpha_cell)

            remove(f"#{card_id2}")
            assert not exists(f"#{card_id2}")
            remove(f"#{card_id3}")
            assert not exists(f"#{card_id2}")
        else:
            wait(lambda: lookup_rows("//tmp/r", [{"key": 0}]) == values(0))
            insert_rows("//tmp/t", values(1))
            wait(lambda: lookup_rows("//tmp/r", [{"key": 1}]) == values(1))

        remove(f"#{card_id1}")
        assert not exists(f"#{card_id1}")

    @authors("ponasenko-rs")
    def test_alien_cell_health(self):
        [alpha_cell, _] = self._create_dedicated_areas_and_cells()
        _, _, remote_driver1, remote_driver2 = self._get_drivers()

        def all_cells_are_healthy(driver=None):
            cells = ls("//sys/chaos_cells", attributes=["health"], driver=driver)
            assert len(cells) == 2
            return all(cell.attributes["health"] == "good" for cell in cells)

        for cluster in ("remote_1", "remote_2"):
            driver = get_driver(cluster=cluster)
            wait(lambda: all_cells_are_healthy(driver=driver))

        cluster_names = self.get_cluster_names()
        area_id = get(f"#{alpha_cell}/@area_id", driver=remote_driver1)
        with self.CellsDisabled(clusters=cluster_names[-2:-1], area_ids=[area_id]):
            wait(lambda: get(f"//sys/chaos_cells/{alpha_cell}/@health", driver=remote_driver1) == "failed")
            wait(lambda: get(f"//sys/chaos_cells/{alpha_cell}/@health", driver=remote_driver2) == "good")


##################################################################


class TestChaosMetaClusterNativeProxy(TestChaosMetaCluster):
    @authors("osidorkin")
    def test_forsake_revoking_coordinator(self):
        cluster_names = self.get_cluster_names()
        peer_cluster_names = [cluster_names[0]]

        cell_id = self._sync_create_chaos_bundle_and_cell(peer_cluster_names=peer_cluster_names)
        cell_id1 = self._sync_create_chaos_cell(peer_cluster_names=peer_cluster_names)
        cell_id2 = self._sync_create_chaos_cell(peer_cluster_names=peer_cluster_names)
        driver0 = self._get_drivers()[0]

        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        sorted_schema = self._get_schemas_by_name(["sorted_simple"])[0]
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "c", "schema": sorted_schema})

        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q2"},
        ]

        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, schema=sorted_schema)

        self._sync_replication_era(card_id)

        def _get_replication_card(cell, card_id, driver):
            peer = get(f"//sys/chaos_cells/{cell}/@peers/0/address", driver=driver)
            return get(
                f"//sys/cluster_nodes/{peer}/orchid/chaos_cells/{cell}/chaos_manager/replication_cards/{card_id}",
                driver=driver
            )

        replication_card = _get_replication_card(cell_id, card_id, driver0)
        assert len(replication_card["coordinators"]) == 3
        assert cell_id1 in replication_card["coordinators"]

        replication_card = _get_replication_card(cell_id, card_id, driver0)
        assert len(replication_card["coordinators"]) == 3
        assert cell_id1 in replication_card["coordinators"]

        # Trying to remove an alive coordinator
        with pytest.raises(YtError):
            execute_command(
                "forsake_chaos_coordinator",
                parameters={
                    "chaos_cell_id": cell_id,
                    "coordinator_cell_id": cell_id1
                }
            )

        replication_card = _get_replication_card(cell_id, card_id, driver0)
        assert len(replication_card["coordinators"]) == 3
        assert cell_id1 in replication_card["coordinators"]

        remove(f"#{cell_id1}")

        def _check():
            try:
                get(f"#{cell_id1}/@")
                return False
            except Exception:
                return True

        wait(_check)

        migrate_replication_cards(cell_id, [card_id], destination_cell_id=cell_id2)

        execute_command(
            "forsake_chaos_coordinator",
            parameters={
                "chaos_cell_id": cell_id,
                "coordinator_cell_id": cell_id1
            }
        )

        wait(lambda: len(_get_replication_card(cell_id2, card_id, driver0)["coordinators"]) == 2, ignore_exceptions=True)
        wait(lambda: _get_replication_card(cell_id2, card_id, driver0)["coordinators"][cell_id] == "granted")
        wait(lambda: _get_replication_card(cell_id2, card_id, driver0)["coordinators"][cell_id2] == "granted")
        self._sync_replication_era(card_id)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/crt", values)
        wait(lambda: lookup_rows("//tmp/q0", [{"key": 0}]) == values)

    @authors("osidorkin")
    def test_forsake_granted_coordinator(self):
        cluster_names = self.get_cluster_names()
        peer_cluster_names = [cluster_names[0]]

        cell_id = self._sync_create_chaos_bundle_and_cell(peer_cluster_names=peer_cluster_names)
        cell_id1 = self._sync_create_chaos_cell(peer_cluster_names=peer_cluster_names)
        cell_id2 = self._sync_create_chaos_cell(peer_cluster_names=peer_cluster_names)
        driver0 = self._get_drivers()[0]

        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        sorted_schema = self._get_schemas_by_name(["sorted_simple"])[0]
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "c", "schema": sorted_schema})

        card_id = get("//tmp/crt/@replication_card_id")

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/q0"},
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/q1"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q2"},
        ]

        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, schema=sorted_schema)

        self._sync_replication_era(card_id)

        def _get_replication_card(cell, card_id, driver):
            peer = get(f"//sys/chaos_cells/{cell}/@peers/0/address", driver=driver)
            return get(
                f"//sys/cluster_nodes/{peer}/orchid/chaos_cells/{cell}/chaos_manager/replication_cards/{card_id}",
                driver=driver
            )

        replication_card = _get_replication_card(cell_id, card_id, driver0)
        assert len(replication_card["coordinators"]) == 3
        assert cell_id1 in replication_card["coordinators"]

        replication_card = _get_replication_card(cell_id, card_id, driver0)
        assert len(replication_card["coordinators"]) == 3
        assert cell_id1 in replication_card["coordinators"]

        remove(f"#{cell_id1}")

        def _check():
            try:
                get(f"#{cell_id1}/@")
                return False
            except Exception:
                return True

        wait(_check)

        execute_command(
            "forsake_chaos_coordinator",
            parameters={
                "chaos_cell_id": cell_id,
                "coordinator_cell_id": cell_id1
            }
        )

        self._sync_replication_era(card_id)

        assert len(_get_replication_card(cell_id, card_id, driver0)["coordinators"]) == 2
        assert _get_replication_card(cell_id, card_id, driver0)["coordinators"][cell_id] == "granted"
        assert _get_replication_card(cell_id, card_id, driver0)["coordinators"][cell_id2] == "granted"

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/crt", values)
        wait(lambda: lookup_rows("//tmp/q0", [{"key": 0}]) == values)


##################################################################


class TestChaosMetaClusterRpcProxy(TestChaosMetaCluster):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


##################################################################


class ChaosClockBase(ChaosTestBase):
    NUM_REMOTE_CLUSTERS = 1
    NUM_TIMESTAMP_PROVIDERS = 1
    USE_PRIMARY_CLOCKS = False

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "transaction_manager": {
                "reject_incorrect_clock_cluster_tag": True
            }
        }
    }

    @classmethod
    def add_alien_clocks_to_ts_configs(
        cls,
        timestamp_providers_configs,
        self_clock_cluster_tag,
        alien_clock_cluster_tag,
        alien_clock_configs
    ):
        alien_clock_configs = alien_clock_configs[alien_clock_configs["cell_tag"]]
        alien_clock_addresses = [f"localhost:{clock_config['rpc_port']}" for clock_config in alien_clock_configs]

        for timestamp_providers_config in timestamp_providers_configs:
            timestamp_providers_config["alien_timestamp_providers"] = [
                {
                    "clock_cluster_tag": alien_clock_cluster_tag,
                    "timestamp_provider": {
                        "addresses": alien_clock_addresses
                    },
                }
            ]
            timestamp_providers_config["clock_cluster_tag"] = self_clock_cluster_tag

    @classmethod
    def modify_timestamp_providers_configs(cls, timestamp_providers_configs, clock_configs, yt_configs):
        cls.add_alien_clocks_to_ts_configs(
            timestamp_providers_configs[0],
            yt_configs[0].primary_cell_tag,
            yt_configs[1].primary_cell_tag,
            clock_configs[1]
        )

        cls.add_alien_clocks_to_ts_configs(
            timestamp_providers_configs[1],
            yt_configs[1].primary_cell_tag,
            yt_configs[0].primary_cell_tag,
            clock_configs[0]
        )

        return True

    def _create_single_peer_chaos_cell(self, name="c", clock_cluster_tag=None):
        cluster_names = self.get_cluster_names()
        peer_cluster_names = cluster_names[:1]
        meta_cluster_names = cluster_names[1:]
        cell_id = self._sync_create_chaos_bundle_and_cell(
            name=name,
            peer_cluster_names=peer_cluster_names,
            meta_cluster_names=meta_cluster_names,
            clock_cluster_tag=clock_cluster_tag)
        return cell_id


##################################################################


class TestChaosClock(ChaosClockBase):
    @authors("savrus")
    def test_invalid_clock(self):
        drivers = self._get_drivers()
        for driver in drivers:
            clock_cluster_tag = get("//sys/@primary_cell_tag", driver=driver)
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell()
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        self._create_chaos_tables(cell_id, replicas)

        with pytest.raises(YtError, match="Transaction timestamp is generated from unexpected clock"):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    @pytest.mark.parametrize("primary", [True, False])
    def test_different_clock(self, primary, mode):
        drivers = self._get_drivers()
        clock_cluster_tag = get("//sys/@primary_cell_tag", driver=drivers[0 if primary else 1])
        for driver in drivers:
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell(clock_cluster_tag=clock_cluster_tag)
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _run_iterations():
            total_iterations = 10
            for iteration in range(total_iterations):
                rows = [{"key": 1, "value": str(iteration)}]
                keys = [{"key": 1}]
                if primary:
                    insert_rows("//tmp/t", rows)
                else:
                    insert_rows("//tmp/q", rows, driver=drivers[1])
                wait(lambda: lookup_rows("//tmp/t", keys) == rows)

                if iteration < total_iterations - 1:
                    mode = ["sync", "async"][iteration % 2]
                    self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode=mode)
        _run_iterations()

        # Check that master transactions are working.
        sync_flush_table("//tmp/t")
        sync_flush_table("//tmp/q", driver=drivers[1])
        _run_iterations()


##################################################################


class TestChaosClockRpcProxy(ChaosClockBase):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    def _set_proxies_clock_cluster_tag(self, clock_cluster_tag, driver=None):
        config = {
            "cluster_connection": {
                "clock_manager": {
                    "clock_cluster_tag": clock_cluster_tag,
                },
                # FIXME(savrus) Workaround for YT-16713.
                "table_mount_cache": {
                    "expire_after_successful_update_time": 0,
                    "expire_after_failed_update_time": 0,
                    "expire_after_access_time": 0,
                    "refresh_time": 0,
                },
            },
        }

        set("//sys/rpc_proxies/@config", config, driver=driver)

        proxies = ls("//sys/rpc_proxies")

        def _check_config():
            orchid_path = "orchid/dynamic_config_manager/effective_config/cluster_connection/clock_manager/clock_cluster_tag"
            for proxy in proxies:
                path = "//sys/rpc_proxies/{0}/{1}".format(proxy, orchid_path)
                if not exists(path) or get(path) != clock_cluster_tag:
                    return False
            return True
        wait(_check_config)

    @authors("savrus")
    def test_invalid_clock_source(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        clock_cluster_tag = get("//sys/@primary_cell_tag")
        set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag)

        self._create_sorted_table("//tmp/t")
        sync_create_cells(1)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError, match="Transaction origin clock source differs from coordinator clock source"):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        with pytest.raises(YtError):
            generate_timestamp()

    @authors("savrus")
    def test_chaos_write_with_client_clock_tag(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        for driver in drivers:
            clock_cluster_tag = get("//sys/@primary_cell_tag", driver=driver)
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell(clock_cluster_tag=remote_clock_tag)
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        self._create_chaos_tables(cell_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

    @authors("savrus")
    def test_chaos_replicated_table_with_different_clock_tag(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        for driver in drivers:
            clock_cluster_tag = get("//sys/@primary_cell_tag", driver=driver)
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell(name="chaos_bundle", clock_cluster_tag=remote_clock_tag)
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids)
        card_id = get("//tmp/crt/@replication_card_id")
        self._sync_replication_era(card_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/crt", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

    @authors("savrus")
    def test_generate_timestamps(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        with raises_yt_error("Unable to generate timestamps: clock source is configured to non-native clock"):
            generate_timestamp()

        config = deepcopy(self.Env.configs["rpc_driver"])
        config["clock_cluster_tag"] = remote_clock_tag
        config["api_version"] = 3

        rpc_driver = Driver(config=config)
        generate_timestamp(driver=rpc_driver)


##################################################################


class TestChaosSingleCluster(ChaosTestBase):
    NUM_REMOTE_CLUSTERS = 0
    NUM_CHAOS_NODES = 1

    @authors("osidorkin")
    def test_multiple_chaos_slots_on_single_node(self):
        nodes = ls("//sys/chaos_nodes")
        assert len(nodes) >= 1

        # It's essential for all chaos bundles coordinators to be on the same chaos node
        set(f"//sys/chaos_nodes/{nodes[0]}/@user_tags", ["custom"])

        cell_id = self._sync_create_chaos_bundle_and_cell(name="c", node_tag_filter="custom")
        assert get("//sys/chaos_cell_bundles/c/@node_tag_filter") == "custom"

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        self._create_chaos_tables(cell_id, replicas)

        self._sync_create_chaos_bundle_and_cell(name="trolling_bundle", node_tag_filter="custom")
        assert get("//sys/chaos_cell_bundles/trolling_bundle/@node_tag_filter") == "custom"

        values = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 1}], replica_consistency="sync") == values)
