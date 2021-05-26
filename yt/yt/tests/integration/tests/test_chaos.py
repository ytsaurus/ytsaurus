from copy import deepcopy

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################


class TestChaos(YTEnvSetup):
    NUM_CLOCKS = 1
    NUM_NODES = 5
    NUM_REMOTE_CLUSTERS = 2
    NUM_CHAOS_NODES = 1

    DELTA_MASTER_CONFIG = {
        "tablet_manager": {
            "peer_revocation_timeout" : 10000,
        },
    }

    @authors("savrus")
    def test_bundle_bad_options(self):
        create_parameters = {
            "type": "chaos_cell_bundle",
            "attributes": {
                "name": "chaos_bundle",
            }
        }
        with pytest.raises(YtError):
            execute_command("create", create_parameters)
        create_parameters["attributes"]["chaos_options"] = {"peers": [{"remote": False}]}
        with pytest.raises(YtError):
            execute_command("create", create_parameters)
        create_parameters["attributes"]["options"] = {
            "peer_count": 1,
            "changelog_account": "sys",
            "snapshot_account": "sys",
        }
        execute_command("create", create_parameters)
        with pytest.raises(YtError):
            set("//sys/tablet_cell_bundles/chaos_bundle/@options/independent_peers", False)

    @authors("savrus")
    def test_chaos_cells(self):
        primary_driver = get_driver(cluster="primary")
        remote_driver0 = get_driver(cluster="remote_0")
        remote_driver1 = get_driver(cluster="remote_1")
        drivers = (primary_driver, remote_driver0, remote_driver1)

        for driver in drivers:
            discovery_config = {
                "peer_count": 1,
                "update_period": 100,
                "node_tag_filter": "chaos_node",
            }
            set("//sys/@config/node_tracker/chaos_cache_manager", discovery_config, driver=driver)

        for driver in drivers:
            nodes = ls("//sys/cluster_nodes", driver=driver)
            chaos_node = None
            for node in nodes:
                cellars = get("//sys/cluster_nodes/{}/@cellars".format(node), driver=driver)
                if len(cellars.get("chaos", [])) > 0:
                    chaos_node = node
            assert chaos_node
            set("//sys/cluster_nodes/{}/@user_tags/end".format(chaos_node), "chaos", driver=driver)

        create_parameters_pattern = {
            "type": "chaos_cell_bundle",
            "attributes": {
                "name": "c",
                "chaos_options": {"peers": [
                    {"remote": True, "alien_cluster": "primary"},
                    {"remote": True, "alien_cluster": "remote_0"},
                    {"remote": True, "alien_cluster": "remote_1"}
                ]},
                "options": {"changelog_account": "sys", "snapshot_account": "sys", "peer_count": 3, "independent_peers": True}
            }
        }

        for peer, driver in enumerate(drivers):
            create_parameters = deepcopy(create_parameters_pattern)
            create_parameters["attributes"]["chaos_options"]["peers"][peer] = {}
            create_parameters["driver"] = driver
            execute_command("create", create_parameters)

        result = execute_command("create", {
            "type": "chaos_cell",
            "attributes": {
                "cell_bundle": "c",
            }
        })
        cell_id = yson.loads(result)["object_id"]

        for driver in (remote_driver0, remote_driver1):
            result = execute_command("create", {
                "type": "chaos_cell",
                "attributes": {
                    "cell_bundle": "c",
                    "chaos_cell_id": cell_id,
                },
                "driver": driver,
            })

        def _check():
            for driver in drivers:
                get("#{0}/@peers".format(cell_id), driver=driver)
                get("#{0}/@health".format(cell_id), driver=driver)
            for driver in drivers:
                if get("#{0}/@health".format(cell_id), driver=driver) != "good":
                    return False
            return True
        wait(_check)
