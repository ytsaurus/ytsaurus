from test_dynamic_tables import DynamicTablesBase

from yt_commands import (
    authors, wait, execute_command, get_driver, get, set, ls, create, get_tablet_leader_address,
    sync_create_cells, sync_mount_table, alter_table, insert_rows, lookup_rows, pull_rows,
    reshard_table,
    create_replication_card, get_replication_card, create_replication_card_replica)

from yt.common import YtError
import yt.yson as yson

import pytest

from copy import deepcopy

from yt.environment.helpers import assert_items_equal

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

    def _wait_for_era(self, path, era=1, check_write=False, driver=None):
        def _check():
            tablets = get("{0}/@tablets".format(path), driver=driver)
            tablet_ids = [tablet["tablet_id"] for tablet in tablets]
            for tablet_id in tablet_ids:
                orchid = self._find_tablet_orchid(get_tablet_leader_address(tablet_id, driver=driver), tablet_id, driver=driver)
                if not orchid["replication_card"] or orchid["replication_card"]["era"] != era:
                    return False
                if check_write and orchid["write_mode"] != "direct":
                    return False
            return True
        wait(_check)

    def _sync_replication_card(self, cell_id, card_id):
        def _check():
            card = get_replication_card(chaos_cell_id=cell_id, replication_card_id=card_id)
            return all(replica["state"] == "enabled" for replica in card["replicas"])
        wait(_check)
        return get_replication_card(chaos_cell_id=cell_id, replication_card_id=card_id)

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

        _, remote_driver0, remote_driver1 = self._get_drivers()

        self._create_sorted_table("//tmp/t")
        self._create_sorted_table("//tmp/r1", driver=remote_driver1)
        self._create_queue_table("//tmp/r0", driver=remote_driver0)

        card_id = create_replication_card(chaos_cell_id=cell_id)
        replicas = [
            {"cluster": "primary", "content_type": "data", "mode": "sync", "state": "enabled", "table_path": "//tmp/t"},
            {"cluster": "remote_0", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/r0"},
            {"cluster": "remote_1", "content_type": "data", "mode": "async", "state": "enabled", "table_path": "//tmp/r1"}
        ]
        replica_ids = []
        for replica in replicas:
            replica_id = create_replication_card_replica(
                chaos_cell_id=cell_id,
                replication_card_id=card_id,
                replica_info=replica)
            replica_ids.append(replica_id)

        repliation_card = self._sync_replication_card(cell_id, card_id)
        replication_card_token = {"chaos_cell_id": cell_id, "replication_card_id": card_id}

        for replica, replica_id in zip(replicas, replica_ids):
            path = replica["table_path"]
            driver = get_driver(cluster=replica["cluster"])
            set("{0}/@replication_card_token".format(path), replication_card_token, driver=driver)
            alter_table(path, upstream_replica_id=replica_id, driver=driver)
            sync_create_cells(1, driver=driver)
            sync_mount_table(path, driver=driver)

        self._wait_for_era("//tmp/t", era=repliation_card["era"])
        self._wait_for_era("//tmp/r0", era=repliation_card["era"], driver=remote_driver0)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)

        assert lookup_rows("//tmp/t", [{"key": 0}]) == values
        wait(lambda: lookup_rows("//tmp/r1", [{"key": 0}], driver=remote_driver1) == values)

    @authors("savrus")
    def test_pull_rows(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        self._create_queue_table("//tmp/q")

        card_id = create_replication_card(chaos_cell_id=cell_id)
        replica_info = {"cluster": "primary", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/q"}
        replica_id = create_replication_card_replica(
            chaos_cell_id=cell_id,
            replication_card_id=card_id,
            replica_info=replica_info)

        replication_card_token = {"chaos_cell_id": cell_id, "replication_card_id": card_id}
        set("//tmp/q/@replication_card_token", replication_card_token)
        alter_table("//tmp/q", upstream_replica_id=replica_id)
        sync_create_cells(1)
        sync_mount_table("//tmp/q")

        repliation_card = self._sync_replication_card(cell_id, card_id)
        self._wait_for_era("//tmp/q", era=repliation_card["era"])

        insert_rows("//tmp/q", [{"key": 0, "value": "0"}])

        def _pull_rows():
            return pull_rows(
                "//tmp/q",
                replication_progress={"segments": [{"lower_key": [], "timestamp": 0}], "upper_key": ["#<Max>"]},
                upstream_replica_id=replica_id)

        wait(lambda: len(_pull_rows()) == 1)

        result = _pull_rows()
        assert len(result) == 1
        row = result[0]
        assert row["key"] == 0
        assert str(row["value"][0]) == "0"

    @authors("savrus")
    def test_serialized_pull_rows(self):
        self._create_chaos_cell_bundle("c")
        cell_id = self._sync_create_chaos_cell("c")

        self._create_queue_table("//tmp/q")
        reshard_table("//tmp/q", [[], [1], [2], [3], [4]])

        card_id = create_replication_card(chaos_cell_id=cell_id)
        replica_info = {"cluster": "primary", "content_type": "queue", "mode": "sync", "state": "enabled", "table_path": "//tmp/q"}
        replica_id = create_replication_card_replica(
            chaos_cell_id=cell_id,
            replication_card_id=card_id,
            replica_info=replica_info)

        replication_card_token = {"chaos_cell_id": cell_id, "replication_card_id": card_id}
        set("//tmp/q/@replication_card_token", replication_card_token)
        alter_table("//tmp/q", upstream_replica_id=replica_id)
        sync_create_cells(1)
        sync_mount_table("//tmp/q")

        repliation_card = self._sync_replication_card(cell_id, card_id)
        self._wait_for_era("//tmp/q", era=repliation_card["era"])

        # Shuffle to mix timestamps.
        for i in (1, 2, 0, 4, 3):
            insert_rows("//tmp/q", [{"key": i, "value": str(i)}])

        def _pull_rows():
            return pull_rows(
                "//tmp/q",
                replication_progress={"segments": [{"lower_key": [], "timestamp": 0}], "upper_key": ["#<Max>"]},
                upstream_replica_id=replica_id,
                order_rows_by_timestamp=True)

        wait(lambda: len(_pull_rows()) == 5)

        result = _pull_rows()
        timestamps = [row.attributes["write_timestamps"][0] for row in result]
        import logging
        logger = logging.getLogger()
        logger.debug("Write timestamps: {0}".format(timestamps))

        for i in range(len(timestamps) - 1):
            assert timestamps[i] < timestamps[i+1], "Write timestamp order mismatch for positions {0} and {1}: {2} > {3}".format(i, i+1, timestamps[i], timestamps[i+1])
