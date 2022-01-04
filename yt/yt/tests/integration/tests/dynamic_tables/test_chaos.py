from test_dynamic_tables import DynamicTablesBase

from yt_commands import (
    authors, wait, execute_command, get_driver, get, set, ls, create,
    sync_create_cells, sync_mount_table, sync_unmount_table, reshard_table, alter_table,
    insert_rows, delete_rows, lookup_rows, pull_rows, build_snapshot, wait_for_cells,
    create_replication_card, get_replication_card, create_replication_card_replica, alter_replication_card_replica)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import yt.yson as yson

import pytest

import __builtin__
from copy import deepcopy

##################################################################


class TestChaos(DynamicTablesBase):
    NUM_REMOTE_CLUSTERS = 2
    NUM_CLOCKS = 1
    NUM_MASTER_CACHES=1
    NUM_NODES = 5
    NUM_CHAOS_NODES = 1

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "peer_revocation_timeout": 10000,
        },
    }

    def _get_clusters(self):
        return [self.get_cluster_name(cluster_index) for cluster_index in xrange(self.NUM_REMOTE_CLUSTERS + 1)]

    def _get_drivers(self):
        return [get_driver(cluster=cluster_name) for cluster_name in self._get_clusters()]

    def _create_chaos_cell_bundle(self, name):
        clusters = self._get_clusters()
        params_pattern = {
            "type": "chaos_cell_bundle",
            "attributes": {
                "name": name,
                "chaos_options": {
                    "peers": [{"remote": True, "alien_cluster": cluster} for cluster in clusters],
                },
                "options": {
                    "changelog_account": "sys",
                    "snapshot_account": "sys",
                    "peer_count": len(clusters),
                    "independent_peers": True,
                }
            }
        }

        bundle_ids = []
        for peer_id, driver in enumerate(self._get_drivers()):
            params = deepcopy(params_pattern)
            params["attributes"]["chaos_options"]["peers"][peer_id] = {}
            params["driver"] = driver
            result = execute_command("create", params)
            bundle_ids.append(yson.loads(result)["object_id"])

        return bundle_ids

    def _create_chaos_cell(self, cell_bundle):
        drivers = self._get_drivers()
        params = {
            "type": "chaos_cell",
            "attributes": {
                "cell_bundle": cell_bundle,
            },
            "driver": drivers[0],
        }

        result = execute_command("create", params)
        cell_id = yson.loads(result)["object_id"]
        params["attributes"]["chaos_cell_id"] = cell_id

        for driver in drivers[1:]:
            params["driver"] = driver
            execute_command("create", params)
        return cell_id

    def _wait_for_chaos_cell(self, cell_id):
        drivers = self._get_drivers()
        def _check():
            for driver in drivers:
                get("#{0}/@peers".format(cell_id), driver=driver)
                get("#{0}/@health".format(cell_id), driver=driver)
            for driver in drivers:
                if get("#{0}/@health".format(cell_id), driver=driver) != "good":
                    return False
            return True
        wait(_check)

    def _sync_create_chaos_cell(self, cell_bundle):
        cell_id = self._create_chaos_cell(cell_bundle)
        self._wait_for_chaos_cell(cell_id)
        return cell_id

    def _list_chaos_nodes(self, driver=None):
        nodes = ls("//sys/cluster_nodes", attributes=["state", "flavors"], driver=driver)
        return [node for node in nodes if ("chaos" in node.attributes["flavors"])]

    def _get_table_orchids(self, path, driver=None):
        tablets = get("{0}/@tablets".format(path), driver=driver)
        tablet_ids = [tablet["tablet_id"] for tablet in tablets]
        orchids = [get("#{0}/orchid".format(tablet_id), driver=driver) for tablet_id in tablet_ids]
        return orchids

    def _wait_for_era(self, path, era=1, check_write=False, driver=None):
        def _check():
            for orchid in self._get_table_orchids(path, driver=driver):
                if not orchid["replication_card"] or orchid["replication_card"]["era"] != era:
                    return False
                if check_write and orchid["write_mode"] != "direct":
                    return False
            return True
        wait(_check)

    def _sync_replication_card(self, cell_id, card_id):
        def _check():
            card = get_replication_card(chaos_cell_id=cell_id, replication_card_id=card_id)
            return all(replica["state"] in ["enabled", "disabled"] for replica in card["replicas"])
        wait(_check)
        return get_replication_card(chaos_cell_id=cell_id, replication_card_id=card_id)

    def _create_replication_card_replicas(self, cell_id, card_id, replicas):
        replica_ids = []
        for replica in replicas:
            replica_id = create_replication_card_replica(
                chaos_cell_id=cell_id,
                replication_card_id=card_id,
                replica_info=replica)
            replica_ids.append(replica_id)
        return replica_ids

    def _create_replica_tables(self, replication_card_token, replicas, replica_ids, create_tablet_cells=True, mount_tables=True):
        for replica, replica_id in zip(replicas, replica_ids):
            path = replica["table_path"]
            driver = get_driver(cluster=replica["cluster"])
            create_table = self._create_queue_table if replica["content_type"] == "queue" else self._create_sorted_table
            create_table(path, driver=driver, replication_card_token=replication_card_token)
            alter_table(path, upstream_replica_id=replica_id, driver=driver)
            if create_tablet_cells:
                sync_create_cells(1, driver=driver)
            if mount_tables:
                sync_mount_table(path, driver=driver)

    def _sync_replication_era(self, cell_id, card_id, replicas):
        repliation_card = self._sync_replication_card(cell_id, card_id)
        for replica in replicas:
            path = replica["table_path"]
            driver = get_driver(cluster=replica["cluster"])
            check_write = replica["mode"] == "sync" and replica["state"] == "enabled"
            self._wait_for_era(path, era=repliation_card["era"], check_write=check_write, driver=driver)

    def _create_chaos_tables(self, cell_id, replicas, sync_replication_era=True, create_tablet_cells=True, mount_tables=True):
        card_id = create_replication_card(chaos_cell_id=cell_id)
        replication_card_token = {"chaos_cell_id": cell_id, "replication_card_id": card_id}
        replica_ids = self._create_replication_card_replicas(cell_id, card_id, replicas)
        self._create_replica_tables(replication_card_token, replicas, replica_ids, create_tablet_cells, mount_tables)
        if sync_replication_era:
            self._sync_replication_era(cell_id, card_id, replicas)
        return card_id, replica_ids

    def _sync_alter_replication_card_replica(self, cell_id, card_id, replicas, replica_ids, replica_index, **kwargs):
        enabled = kwargs.get("enabled", None)
        mode = kwargs.get("mode", None)

        alter_replication_card_replica(
            chaos_cell_id=cell_id,
            replication_card_id=card_id,
            replica_id=replica_ids[replica_index],
            **kwargs)

        def _replica_checker(replica_info):
            if enabled is not None and replica_info["state"] != ("enabled" if enabled else "disabled"):
                return False
            if mode is not None and replica_info["mode"] != mode:
                return False
            return True

        def _check():
            replica = replicas[replica_index]
            orchids = self._get_table_orchids(replica["table_path"], driver=get_driver(cluster=replica["cluster"]))
            if not all(_replica_checker(orchid["replication_card"]["replicas"][replica_index]) for orchid in orchids):
                return False
            if len(__builtin__.set(orchid["replication_card"]["era"] for orchid in orchids)) > 1:
                return False
            replica_info = orchids[0]["replication_card"]["replicas"][replica_index]
            if replica_info["mode"] == "sync" and replica_info["state"] == "enabled":
                if not all(orchid["write_mode"] == "direct" for orchid in orchids):
                    return False
            return True

        wait(_check)
        era = self._get_table_orchids("//tmp/t")[0]["replication_card"]["era"]

        # Include coordinators to use same replication card cache key as insert_rows does.
        wait(lambda: get_replication_card(chaos_cell_id=cell_id, replication_card_id=card_id, include_coordinators=True)["era"] == era)
        replication_card = get_replication_card(chaos_cell_id=cell_id, replication_card_id=card_id, include_coordinators=True)
        assert replication_card["era"] == era

        def _check_sync():
            for replica in replication_card["replicas"]:
                if replica["mode"] != "sync" or replica["state"] != "enabled":
                    continue
                orchids = self._get_table_orchids(replica["table_path"], driver=get_driver(cluster=replica["cluster"]))
                if not all(orchid["replication_card"]["era"] == era for orchid in orchids):
                    return False
            return True

        wait(_check_sync)

    def setup_method(self, method):
        super(TestChaos, self).setup_method(method)

        for driver in self._get_drivers():
            synchronizer_config = {
                "enable": True,
                "sync_period": 100,
            }
            set("//sys/@config/chaos_manager/alien_cell_synchronizer", synchronizer_config, driver=driver)

            discovery_config = {
                "peer_count": 1,
                "update_period": 100,
                "node_tag_filter": "master_cache"
            }
            set("//sys/@config/node_tracker/master_cache_manager", discovery_config, driver=driver)

            chaos_nodes = self._list_chaos_nodes(driver)
            for chaos_node in chaos_nodes:
                set("//sys/cluster_nodes/{0}/@user_tags/end".format(chaos_node), "chaos_cache", driver=driver)

    @authors("savrus")
    def test_virtual_maps(self):
        tablet_cell_id = sync_create_cells(1)[0]
        tablet_bundle_id = get("//sys/tablet_cell_bundles/default/@id")

        result = execute_command("create", {
            "type": "chaos_cell_bundle",
            "attributes": {
                "name": "default",
                "chaos_options": {"peers": [{"remote": False}]},
                "options": {"changelog_account": "sys", "snapshot_account": "sys", "peer_count": 1, "independent_peers": True}
            }
        })
        chaos_bundle_id = yson.loads(result)["object_id"]

        assert chaos_bundle_id != tablet_bundle_id
        assert get("//sys/chaos_cell_bundles/default/@id") == chaos_bundle_id

        result = execute_command("create", {
            "type": "chaos_cell",
            "attributes": {
                "cell_bundle": "default",
            }
        })
        chaos_cell_id = yson.loads(result)["object_id"]

        assert_items_equal(get("//sys/chaos_cell_bundles"), ["default"])
        assert_items_equal(get("//sys/tablet_cell_bundles"), ["default"])
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
        with pytest.raises(YtError):
            set("//sys/chaos_cell_bundles/chaos_bundle/@options/independent_peers", False)

    @authors("savrus")
    def test_chaos_cells(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        def _check(peers, local_index=0):
            assert len(peers) == 3
            assert all(peer["address"] for peer in peers)
            assert sum("alien" in peer for peer in peers) == 2
            assert "alien" not in peers[local_index]

        _check(get("#{0}/@peers".format(cell_id)))

        for index, driver in enumerate(self._get_drivers()):
            _check(get("#{0}/@peers".format(cell_id), driver=driver), index)

    @authors("savrus")
    def test_replication_card(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        chaos_node = get("#{0}/@peers/0/address".format(cell_id))
        set("//sys/cluster_nodes/{0}/@user_tags/end".format(chaos_node), "chaos_node")

        card_id = create_replication_card(chaos_cell_id=cell_id)
        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": "sync", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "table_path": "//tmp/r0"},
            {"cluster": "remote_1", "content_type": "data", "mode": "async", "table_path": "//tmp/r1"}
        ]
        for replica in replicas:
            create_replication_card_replica(
                chaos_cell_id=cell_id,
                replication_card_id=card_id,
                replica_info=replica)
        card = get_replication_card(chaos_cell_id=cell_id, replication_card_id=card_id)
        assert len(card["replicas"]) == 3
        card_replicas = [{key: r[key] for key in replicas[0].keys()} for r in card["replicas"]]
        assert_items_equal(card_replicas, replicas)

    def _create_queue_table(self, path, **attributes):
        attributes.update({"dynamic": True})
        if "schema" not in attributes:
            attributes.update({
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
                })
        driver = attributes.pop("driver", None)
        create("replicated_table", path, attributes=attributes, driver=driver)

    @authors("savrus")
    def test_chaos_table(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": "sync", "state": "enabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
            {"cluster": "remote_1", "content_type": "data", "mode": "async", "state": "enabled", "table_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(cell_id, replicas)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values)

    @authors("savrus")
    def test_pull_rows(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/q"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(progress_timestamp):
            return pull_rows(
                "//tmp/q",
                replication_progress={"segments": [{"lower_key": [], "timestamp": progress_timestamp}], "upper_key": ["#<Max>"]},
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
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/q"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)
        reshard_table("//tmp/q", [[], [1], [2], [3], [4]])
        sync_mount_table("//tmp/q")
        self._sync_replication_era(cell_id, card_id, replicas)

        for i in (1, 2, 0, 4, 3):
            insert_rows("//tmp/q", [{"key": i, "value": str(i)}])

        def _pull_rows():
            return pull_rows(
                "//tmp/q",
                replication_progress={"segments": [{"lower_key": [], "timestamp": 0}], "upper_key": ["#<Max>"]},
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
    def test_delete_rows_replication(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": "async", "state": "enabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
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
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": mode, "state": "disabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][0]["mode"] == mode
        assert orchid["replication_card"]["replicas"][0]["state"] == "disabled"
        assert orchid["write_mode"] == "pull"

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == []
        alter_replication_card_replica(
            chaos_cell_id=cell_id,
            replication_card_id=card_id,
            replica_id=replica_ids[0],
            enabled=True)

        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)
        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][0]["mode"] == mode
        assert orchid["replication_card"]["replicas"][0]["state"] == "enabled"
        assert orchid["write_mode"] == "pull" if mode == "async" else "direct"

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replica_disable(self, mode):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": mode, "state": "enabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][0]["mode"] == mode
        assert orchid["replication_card"]["replicas"][0]["state"] == "enabled"
        assert orchid["write_mode"] == "pull" if mode == "async" else "direct"

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        self._sync_alter_replication_card_replica(cell_id, card_id, replicas, replica_ids, 0, enabled=False)

        values = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][0]["mode"] == mode
        assert orchid["replication_card"]["replicas"][0]["state"] == "disabled"
        assert orchid["write_mode"] == "pull"
        assert lookup_rows("//tmp/t", [{"key": 1}]) == []

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_replica_mode_switch(self, mode):
        if mode == "sync":
            old_mode, new_mode, old_write_mode, new_wirte_mode = "sync", "async", "direct", "pull"
        else:
            old_mode, new_mode, old_write_mode, new_wirte_mode = "async", "sync", "pull", "direct"

        def _check_lookup(keys, values, mode):
            if mode == "sync":
                assert lookup_rows("//tmp/t", keys) == values
            else:
                assert lookup_rows("//tmp/t", keys) == []

        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": old_mode, "state": "enabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][0]["mode"] == old_mode
        assert orchid["replication_card"]["replicas"][0]["state"] == "enabled"
        assert orchid["write_mode"] == old_write_mode

        values = [{"key": i, "value": str(i)} for i in range(2)]
        insert_rows("//tmp/t", values[:1])

        _check_lookup([{"key": 0}], values[:1], old_mode)
        self._sync_alter_replication_card_replica(cell_id, card_id, replicas, replica_ids, 0, mode=new_mode)

        def _insistent_insert_rows():
            try:
                insert_rows("//tmp/t", values[1:])
                return True
            except YtError:
                return False

        wait(_insistent_insert_rows)
        _check_lookup([{"key": 1}], values[1:], new_mode)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][0]["mode"] == new_mode
        assert orchid["replication_card"]["replicas"][0]["state"] == "enabled"
        assert orchid["write_mode"] == new_wirte_mode

    @authors("savrus")
    def test_replication_progress(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": "async", "state": "enabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        self._sync_alter_replication_card_replica(cell_id, card_id, replicas, replica_ids, 0, enabled=False)

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
        set("//sys/cluster_nodes/{}/@banned".format(peer), True)
        wait_for_cells([cell_id])

        orchid = get("#{0}/orchid".format(tablet_id))
        assert orchid["replication_progress"] == progress

    @authors("savrus")
    def test_async_queue_replica(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "queue", "mode": "async", "state": "enabled", "table_path": "//tmp/q"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(replica_index, versioned=False):
            rows = pull_rows(
                replicas[replica_index]["table_path"],
                replication_progress={"segments": [{"lower_key": [], "timestamp": 0}], "upper_key": ["#<Max>"]},
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster"]))
            if versioned:
                return rows
            return [{"key": row["key"], "value": str(row["value"][0])} for row in rows]

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/q", values)
        wait(lambda: _pull_rows(1) == values)
        wait(lambda: _pull_rows(0) == values)

        async_rows = _pull_rows(0, True)
        sync_rows = _pull_rows(1, True)
        assert async_rows == sync_rows

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    def test_queue_replica_enable_disable(self, mode):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        replicas = [
            {"cluster": "primary", "content_type": "queue", "mode": mode, "state": "enabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(replica_index):
            rows = pull_rows(
                replicas[replica_index]["table_path"],
                replication_progress={"segments": [{"lower_key": [], "timestamp": 0}], "upper_key": ["#<Max>"]},
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster"]))
            return [{"key": row["key"], "value": str(row["value"][0])} for row in rows]

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)
        wait(lambda: _pull_rows(replica_index=1) == values0)
        wait(lambda: _pull_rows(replica_index=0) == values0)

        self._sync_alter_replication_card_replica(cell_id, card_id, replicas, replica_ids, 0, enabled=False)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        wait(lambda: _pull_rows(replica_index=1) == values0 + values1)
        assert _pull_rows(replica_index=0) == values0

        self._sync_alter_replication_card_replica(cell_id, card_id, replicas, replica_ids, 0, enabled=True)

        wait(lambda: _pull_rows(replica_index=0) == values0 + values1)

        values2 = [{"key": 2, "value": "2"}]
        insert_rows("//tmp/t", values2)
        values = values0 + values1 + values2
        wait(lambda: _pull_rows(replica_index=1) == values)
        wait(lambda: _pull_rows(replica_index=0) == values)
