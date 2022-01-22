from test_dynamic_tables import DynamicTablesBase

from yt_commands import (
    authors, wait, execute_command, get_driver, get, set, ls, create,
    sync_create_cells, sync_mount_table, sync_unmount_table, reshard_table, alter_table,
    insert_rows, delete_rows, lookup_rows, pull_rows, build_snapshot, wait_for_cells,
    create_replication_card, get_replication_card, create_replication_card_replica, alter_replication_card_replica,
    sync_create_chaos_cell, create_chaos_cell_bundle, generate_chaos_cell_id)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import __builtin__

##################################################################


class TestChaos(DynamicTablesBase):
    NUM_REMOTE_CLUSTERS = 2
    NUM_CLOCKS = 1
    NUM_MASTER_CACHES=1
    NUM_NODES = 5
    NUM_CHAOS_NODES = 1
    NUM_TEST_PARTITIONS = 2

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "peer_revocation_timeout": 10000,
        },
    }

    def _get_drivers(self):
        return [get_driver(cluster=cluster_name) for cluster_name in self.get_cluster_names()]

    def _create_chaos_cell_bundle(self, name="c"):
        return create_chaos_cell_bundle(name, self.get_cluster_names())

    def _sync_create_chaos_bundle_and_cell(self):
        self._create_chaos_cell_bundle()
        cell_id = generate_chaos_cell_id()
        sync_create_chaos_cell("c", cell_id, self.get_cluster_names())
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

    def _sync_replication_card(self, card_id):
        def _check():
            card = get_replication_card(card_id)
            return all(replica["state"] in ["enabled", "disabled"] for replica in card["replicas"].itervalues())
        wait(_check)
        return get_replication_card(card_id)

    def _create_replication_card_replicas(self, card_id, replicas):
        return [create_replication_card_replica(card_id, replica) for replica in replicas]

    def _create_replica_tables(self, replication_card_id, replicas, replica_ids, create_tablet_cells=True, mount_tables=True):
        for replica, replica_id in zip(replicas, replica_ids):
            path = replica["replica_path"]
            driver = get_driver(cluster=replica["cluster_name"])
            create_table = self._create_queue_table if replica["content_type"] == "queue" else self._create_sorted_table
            create_table(path, driver=driver, upstream_replica_id=replica_id)
            alter_table(path, upstream_replica_id=replica_id, driver=driver)
            if create_tablet_cells:
                sync_create_cells(1, driver=driver)
            if mount_tables:
                sync_mount_table(path, driver=driver)

    def _sync_replication_era(self, card_id, replicas):
        repliation_card = self._sync_replication_card(card_id)
        for replica in replicas:
            path = replica["replica_path"]
            driver = get_driver(cluster=replica["cluster_name"])
            check_write = replica["mode"] == "sync" and replica["state"] == "enabled"
            self._wait_for_era(path, era=repliation_card["era"], check_write=check_write, driver=driver)

    def _create_chaos_tables(self, cell_id, replicas, sync_replication_era=True, create_tablet_cells=True, mount_tables=True):
        card_id = create_replication_card(chaos_cell_id=cell_id)
        replica_ids = self._create_replication_card_replicas(card_id, replicas)
        self._create_replica_tables(card_id, replicas, replica_ids, create_tablet_cells, mount_tables)
        if sync_replication_era:
            self._sync_replication_era(card_id, replicas)
        return card_id, replica_ids

    def _sync_alter_replication_card_replica(self, card_id, replicas, replica_ids, replica_index, **kwargs):
        replica_id = replica_ids[replica_index]
        alter_replication_card_replica(card_id, replica_id, **kwargs)

        enabled = kwargs.get("enabled", None)
        mode = kwargs.get("mode", None)
        def _replica_checker(replica_info):
            if enabled is not None and replica_info["state"] != ("enabled" if enabled else "disabled"):
                return False
            if mode is not None and replica_info["mode"] != mode:
                return False
            return True

        def _check():
            replica = replicas[replica_index]
            orchids = self._get_table_orchids(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
            if not all(_replica_checker(orchid["replication_card"]["replicas"][replica_id]) for orchid in orchids):
                return False
            if len(__builtin__.set(orchid["replication_card"]["era"] for orchid in orchids)) > 1:
                return False
            replica_info = orchids[0]["replication_card"]["replicas"][replica_id]
            if replica_info["mode"] == "sync" and replica_info["state"] == "enabled":
                if not all(orchid["write_mode"] == "direct" for orchid in orchids):
                    return False
            return True

        wait(_check)
        era = self._get_table_orchids("//tmp/t")[0]["replication_card"]["era"]

        # Include coordinators to use same replication card cache key as insert_rows does.
        wait(lambda: get_replication_card(card_id, include_coordinators=True)["era"] == era)
        replication_card = get_replication_card(card_id, include_coordinators=True)
        assert replication_card["era"] == era

        def _check_sync():
            for replica in replication_card["replicas"].itervalues():
                if replica["mode"] != "sync" or replica["state"] != "enabled":
                    continue
                orchids = self._get_table_orchids(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
                if not all(orchid["replication_card"]["era"] == era for orchid in orchids):
                    return False
            return True

        wait(_check_sync)

    def setup_method(self, method):
        super(TestChaos, self).setup_method(method)

        # TODO(babenko): consider moving to yt_env_setup.py
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

        chaos_bundle_ids = self._create_chaos_cell_bundle(name="default")

        assert tablet_bundle_id not in chaos_bundle_ids
        assert get("//sys/chaos_cell_bundles/default/@id") in chaos_bundle_ids

        chaos_cell_id = generate_chaos_cell_id()
        sync_create_chaos_cell("default", chaos_cell_id, self.get_cluster_names())

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
    def test_replication_card(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        chaos_node = get("#{0}/@peers/0/address".format(cell_id))
        set("//sys/cluster_nodes/{0}/@user_tags/end".format(chaos_node), "chaos_node")

        card_id = create_replication_card(chaos_cell_id=cell_id)
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "replica_path": "//tmp/r1"}
        ]
        replica_ids = self._create_replication_card_replicas(card_id, replicas)

        card = get_replication_card(card_id)
        card_replicas = [{key: r[key] for key in replicas[0].keys()} for r in card["replicas"].itervalues()]
        assert_items_equal(card_replicas, replicas)

        card = get("#{0}/@".format(card_id))
        assert card["id"] == card_id
        assert card["type"] == "replication_card"
        card_replicas = [{key: r[key] for key in replicas[0].keys()} for r in card["replicas"].itervalues()]
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
            assert replica["type"] == "replication_card_replica"
            assert replica["cluster_name"] == replicas[replica_index]["cluster_name"]
            assert replica["replica_path"] == replicas[replica_index]["replica_path"]

    def _create_queue_table(self, path, **attributes):
        attributes.update({"dynamic": True})
        if "schema" not in attributes:
            attributes.update({
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
                })
        driver = attributes.pop("driver", None)
        create("replication_log_table", path, attributes=attributes, driver=driver)

    @authors("savrus")
    def test_chaos_table(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "sync", "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "state": "enabled", "replica_path": "//tmp/r1"}
        ]
        self._create_chaos_tables(cell_id, replicas)
        _, remote_driver0, remote_driver1 = self._get_drivers()

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values)

    @authors("savrus")
    def test_pull_rows(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/q"}
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
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/q"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)
        reshard_table("//tmp/q", [[], [1], [2], [3], [4]])
        sync_mount_table("//tmp/q")
        self._sync_replication_era(card_id, replicas)

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
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
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
            {"cluster_name": "primary", "content_type": "data", "mode": mode, "state": "disabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "disabled"
        assert orchid["write_mode"] == "pull"

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == []
        alter_replication_card_replica(card_id, replica_ids[0], enabled=True)

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
            {"cluster_name": "primary", "content_type": "data", "mode": mode, "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "enabled"
        assert orchid["write_mode"] == "pull" if mode == "async" else "direct"

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        self._sync_alter_replication_card_replica(card_id, replicas, replica_ids, 0, enabled=False)

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
            old_mode, new_mode, old_write_mode, new_wirte_mode = "sync", "async", "direct", "pull"
        else:
            old_mode, new_mode, old_write_mode, new_wirte_mode = "async", "sync", "pull", "direct"

        def _check_lookup(keys, values, mode):
            if mode == "sync":
                assert lookup_rows("//tmp/t", keys) == values
            else:
                assert lookup_rows("//tmp/t", keys) == []

        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": old_mode, "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        orchid = self._get_table_orchids("//tmp/t")[0]
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["mode"] == old_mode
        assert orchid["replication_card"]["replicas"][replica_ids[0]]["state"] == "enabled"
        assert orchid["write_mode"] == old_write_mode

        values = [{"key": i, "value": str(i)} for i in range(2)]
        insert_rows("//tmp/t", values[:1])

        _check_lookup([{"key": 0}], values[:1], old_mode)
        self._sync_alter_replication_card_replica(card_id, replicas, replica_ids, 0, mode=new_mode)

        def _insistent_insert_rows():
            try:
                insert_rows("//tmp/t", values[1:])
                return True
            except YtError:
                return False

        wait(_insistent_insert_rows)
        _check_lookup([{"key": 1}], values[1:], new_mode)

        orchid = self._get_table_orchids("//tmp/t")[0]
        replica_id = replica_ids[0]
        assert orchid["replication_card"]["replicas"][replica_id]["mode"] == new_mode
        assert orchid["replication_card"]["replicas"][replica_id]["state"] == "enabled"
        assert orchid["write_mode"] == new_wirte_mode

    @authors("savrus")
    def test_replication_progress(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

        self._sync_alter_replication_card_replica(card_id, replicas, replica_ids, 0, enabled=False)

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
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "state": "enabled", "replica_path": "//tmp/q"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(replica_index, versioned=False):
            rows = pull_rows(
                replicas[replica_index]["replica_path"],
                replication_progress={"segments": [{"lower_key": [], "timestamp": 0}], "upper_key": ["#<Max>"]},
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster_name"]))
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
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": mode, "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/r0"},
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _pull_rows(replica_index):
            rows = pull_rows(
                replicas[replica_index]["replica_path"],
                replication_progress={"segments": [{"lower_key": [], "timestamp": 0}], "upper_key": ["#<Max>"]},
                upstream_replica_id=replica_ids[replica_index],
                driver=get_driver(cluster=replicas[replica_index]["cluster_name"]))
            return [{"key": row["key"], "value": str(row["value"][0])} for row in rows]

        values0 = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values0)
        wait(lambda: _pull_rows(1) == values0)
        wait(lambda: _pull_rows(0) == values0)

        self._sync_alter_replication_card_replica(card_id, replicas, replica_ids, 0, enabled=False)

        values1 = [{"key": 1, "value": "1"}]
        insert_rows("//tmp/t", values1)

        wait(lambda: _pull_rows(1) == values0 + values1)
        assert _pull_rows(0) == values0

        self._sync_alter_replication_card_replica(card_id, replicas, replica_ids, 0, enabled=True)

        wait(lambda: _pull_rows(0) == values0 + values1)

        values2 = [{"key": 2, "value": "2"}]
        insert_rows("//tmp/t", values2)
        values = values0 + values1 + values2
        wait(lambda: _pull_rows(1) == values)
        wait(lambda: _pull_rows(0) == values)

    @authors("savrus")
    @pytest.mark.parametrize("content", ["data", "queue", "both"])
    def test_resharded_replication(self, content):
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/q"},
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
        for i in reversed(range(2)):
            insert_rows("//tmp/t", [rows[i]])
        wait(lambda: lookup_rows("//tmp/t", keys) == rows)

    @authors("babenko")
    def test_chaos_replicated_table_requires_valid_card_id(self):
        with pytest.raises(YtError):
            create("chaos_replicated_table", "//tmp/crt")
            create("chaos_replicated_table", "//tmp/crt", attributes={"replication_card_id": "1-2-3-4"})

    @authors("babenko")
    def test_chaos_replicated_table(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        card_id = create_replication_card(chaos_cell_id=cell_id)

        create("chaos_replicated_table", "//tmp/crt", attributes={"replication_card_id": card_id})
        assert get("//tmp/crt/@type") == "chaos_replicated_table"
        assert get("//tmp/crt/@replication_card_id") == card_id

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "state": "enabled", "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "replica_path": "//tmp/q"},
        ]
        replica_ids = self._create_replication_card_replicas(card_id, replicas)
        assert len(replica_ids) == 2

        wait(lambda: get_replication_card(card_id, bypass_cache=True)["era"] == 1)

        card = get_replication_card(card_id, include_coordinators=True, bypass_cache=True)
        assert get("//tmp/crt/@era") == card["era"]
        assert get("//tmp/crt/@coordinator_cell_ids") == card["coordinator_cell_ids"]

        crt_replicas = get("//tmp/crt/@replicas")
        assert len(crt_replicas) == 2

        for replica_id in replica_ids:
            assert card["replicas"][replica_id] == crt_replicas[replica_id]
