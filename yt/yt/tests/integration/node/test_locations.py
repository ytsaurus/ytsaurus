from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (authors, wait, ls, get)

import yt_error_codes


class TestLocationMisconfigured(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    STORE_LOCATION_COUNT = 2

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 2
        config["data_node"]["store_locations"][1]["medium_name"] = "test"

    @authors("don-dron")
    def test_location_medium_misconfigured(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1

        with Restarter(self.Env, NODES_SERVICE):
            pass

        get("//sys/cluster_nodes/{}/@".format(nodes[0]))

        def check_alerts():
            alerts = get("//sys/cluster_nodes/{}/@alerts".format(nodes[0]))
            return len(alerts) == 1 and alerts[0]["code"] == yt_error_codes.LocationMediumIsMisconfigured

        wait(lambda: check_alerts())
