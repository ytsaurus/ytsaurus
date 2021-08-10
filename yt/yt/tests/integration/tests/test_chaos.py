from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, execute_command, get_driver, sync_create_cells,
    get, set)

from yt.common import YtError
import yt.yson as yson

import pytest

from copy import deepcopy

from yt.environment.helpers import assert_items_equal

##################################################################


class TestChaos(YTEnvSetup):
    NUM_CLOCKS = 1
    NUM_NODES = 5
    NUM_REMOTE_CLUSTERS = 2
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
                "node_tag_filter": "chaos_node",
            }
            set("//sys/@config/node_tracker/chaos_cache_manager", discovery_config, driver=driver)

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
