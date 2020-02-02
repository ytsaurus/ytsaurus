from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE
from yt_commands import *
from yt_helpers import Metric

import os.path
import time

from sys import stderr

#################################################################

class TestMasterCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    MASTER_CACHE_CONFIG = {
        "enable_master_cache_discovery": True,
        "master_cache_discovery_period": 100,
        "addresses": [],
        "cell_id": "1-1-1-1"
    }

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "master_cache": MASTER_CACHE_CONFIG
        }
    }

    DELTA_DRIVER_CONFIG = {
        "master_cache": MASTER_CACHE_CONFIG
    }

    def set_master_cache_manager_node_tag_filter(self, filter):
        set("//sys/@config/node_tracker/master_cache_manager/node_tag_filter", filter)

    def set_master_cache_manager_update_period(self, period):
        set("//sys/@config/node_tracker/master_cache_manager/update_period", period)

    def select_node(self, id):
        return sorted(ls("//sys/cluster_nodes"))[id]

    def select_nodes(self, ids):
        nodes = sorted(ls("//sys/cluster_nodes"))
        return list(nodes[id] for id in ids)

    def set_node_tags(self, id, tags):
        set("//sys/cluster_nodes/{0}/@user_tags".format(self.select_node(id)), tags)

    def ban_node(self, id):
        set("//sys/cluster_nodes/{0}/@banned".format(self.select_node(id)), True)

    def get_node_cache_list(self, node):
        result = get("//sys/cluster_nodes/{}/orchid/cluster_connection/master_cache/addresses".format(node))
        return list(result)

    def get_master_cache_list(self):
        return get("//sys/cluster_nodes/@master_cache_nodes")

    def wait_for_cache_config_at_nodes(self, expected_nodes):
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: self.get_node_cache_list(node) == expected_nodes, "Node {} cannot update list of master caches".format(node))

    def wait_for_cache_requests(self, expected_node_ids):
        metrics = [
            Metric.at_node(
                node,
                "rpc/server/request_count",
                with_tags={"service": "ObjectService", "method": "Execute"},
                aggr_method="last")
            for node in sorted(ls("//sys/cluster_nodes"))
        ]

        def check():
            time.sleep(0.5)
            metrics_before = [metric.update().get(verbose=True) or 0 for metric in metrics]
            ls("//tmp", read_from="cache")
            time.sleep(0.5)
            metrics_after = [metric.update().get(verbose=True) or 0 for metric in metrics]

            for node_id in xrange(len(metrics_before)):
                actual_sign = 1 if metrics_after[node_id] > metrics_before[node_id] else 0
                expected_sign = 1 if node_id in expected_node_ids else 0
                if actual_sign != expected_sign:
                    return False
            return True

        wait(check, "Cache traffic goes through improper nodes")

    def wait_for_cache_config(self, expected_node_ids):
        expected_nodes = self.select_nodes(expected_node_ids)
        print>>stderr, "Expecting master caches:", expected_nodes

        wait(lambda: self.get_master_cache_list() == expected_nodes, "Master cannot update list of master caches")
        self.wait_for_cache_config_at_nodes(expected_nodes)
        self.wait_for_cache_requests(expected_node_ids)

    @authors("aleksandra-zh")
    def test_dynamic_discovery(self):
        self.set_node_tags(0, ["cache_here"])
        self.set_master_cache_manager_update_period(100)
        self.set_master_cache_manager_node_tag_filter("cache_here")

        self.wait_for_cache_config([0])

    @authors("aleksandra-zh")
    def test_dynamic_reconfig(self):
        self.set_node_tags(0, ["foo"])
        self.set_node_tags(1, ["bar"])
        self.set_master_cache_manager_update_period(100)
        self.set_master_cache_manager_node_tag_filter("foo")

        self.wait_for_cache_config([0])

        self.set_master_cache_manager_node_tag_filter("bar")

        self.wait_for_cache_config([1])

    @authors("aleksandra-zh")
    def test_no_available_caches(self):
        self.set_master_cache_manager_update_period(100)
        self.set_master_cache_manager_node_tag_filter("some_long_and_creepy_tag")

        self.wait_for_cache_config([])

    @authors("aleksandra-zh")
    def test_ban_node(self):
        self.set_node_tags(0, ["foo"])
        self.set_master_cache_manager_update_period(100)
        self.set_master_cache_manager_node_tag_filter("foo")

        self.wait_for_cache_config([0])

        self.ban_node(0)
        self.wait_for_cache_config([])
