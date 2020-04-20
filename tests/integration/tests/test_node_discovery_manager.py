from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE
from yt_commands import *
from yt_helpers import Metric

import os.path
import time

from sys import stderr

#################################################################

class Base:
    def select_node(self, id):
        return sorted(ls("//sys/cluster_nodes"))[id]

    def select_nodes(self, ids):
        nodes = sorted(ls("//sys/cluster_nodes"))
        return sorted(list(nodes[id] for id in ids))

    def set_node_tags(self, id, tags):
        set("//sys/cluster_nodes/{0}/@user_tags".format(self.select_node(id)), tags)

    def ban_node(self, id):
        set("//sys/cluster_nodes/{0}/@banned".format(self.select_node(id)), True)		

    def wait_for_config_at_nodes(self, expected_nodes):
        for node in ls("//sys/cluster_nodes"):		
            wait(lambda: self.get_node_addresses_list(node) == expected_nodes, "Node {} cannot update list of master caches".format(node))

    def wait_for_requests(self, expected_node_ids, metric_tags, make_request):
        metrics = [
            Metric.at_node(
                node,
                "rpc/server/request_count",
                with_tags=metric_tags,
                aggr_method="last")
            for node in sorted(ls("//sys/cluster_nodes"))
        ]

        def check():
            time.sleep(0.5)
            metrics_before = [metric.update().get(verbose=True) or 0 for metric in metrics]
            try:
                for i in range(len(metrics)):
                    make_request()
                make_request()
            except:
                return False
            time.sleep(0.5)
            metrics_after = [metric.update().get(verbose=True) or 0 for metric in metrics]

            for node_id in xrange(len(metrics)):
                actual_sign = 1 if metrics_after[node_id] > metrics_before[node_id] else 0
                expected_sign = 1 if node_id in expected_node_ids else 0
                if actual_sign != expected_sign:
                    return False
            return True

        wait(check, "Cache traffic goes through improper nodes")

    @authors("aleksandra-zh")
    def test_dynamic_discovery(self):
        self.set_node_tags(0, ["cache_here"])
        self.set_update_period(100)
        self.set_node_tag_filter("cache_here")

        self.wait_for_config([0])

    @authors("aleksandra-zh")
    def test_dynamic_reconfig(self):
        self.set_node_tags(0, ["foo"])
        self.set_node_tags(1, ["bar"])
        self.set_update_period(100)
        self.set_node_tag_filter("foo")

        self.wait_for_config([0])

        self.set_node_tag_filter("bar")

        self.wait_for_config([1])

    @authors("aleksandra-zh")
    def test_no_available_proveders(self):
        self.set_update_period(100)
        self.set_node_tag_filter("some_long_and_creepy_tag")

        self.wait_for_config([])

    @authors("aleksandra-zh")
    def test_ban_node(self):
        self.set_node_tags(0, ["foo"])
        self.set_update_period(100)
        self.set_node_tag_filter("foo")
        self.wait_for_config([0])

        self.ban_node(0)

        self.wait_for_config([])

    @authors("aleksandra-zh")
    def test_peer_count_change(self):
        self.set_peer_count(3)
        self.wait_for_config([0, 1, 2])

        self.set_peer_count(1)

        wait(lambda: len(self.get_discovered_node_list()) == 1, "Master cannot update node list")

    @authors("aleksandra-zh")
    def test_racks(self):
        expected_node_ids = [0, 1, 2]
        nodes = self.select_nodes(expected_node_ids)
        create_rack("r1")
        set("//sys/cluster_nodes/" + nodes[0] + "/@rack", "r1")
        create_rack("r2")
        set("//sys/cluster_nodes/" + nodes[1] + "/@rack", "r2")
        set("//sys/cluster_nodes/" + nodes[2] + "/@rack", "r2")

        self.set_max_peers_per_rack(1)
        self.set_peer_count(2)
        def check_discovered_node_list():
            node_list = self.get_discovered_node_list()
            return node_list == [0, 1] or node_list == [0, 2]
        wait(lambda: check_discovered_node_list, "Master cannot update node list")

        self.set_peer_count(3)
        self.wait_for_config([0, 1, 2])


class TestMasterCache(Base, YTEnvSetup):
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

    def get_discovered_node_list(self):
        return sorted(get("//sys/cluster_nodes/@master_cache_nodes"))

    def get_node_addresses_list(self, node):
         return sorted(list(get("//sys/cluster_nodes/{}/orchid/cluster_connection/master_cache/channel_attributes/addresses".format(node))))

    def wait_for_config(self, expected_node_ids):
        expected_nodes = self.select_nodes(expected_node_ids)
        print>>stderr, "Expecting master caches:", expected_nodes

        wait(lambda: self.get_discovered_node_list() == expected_nodes, "Master cannot update list of master caches")
        if expected_nodes:
            self.wait_for_config_at_nodes(expected_nodes)
        self.wait_for_requests(
            expected_node_ids,
            {"service": "ObjectService", "method": "Execute"},
            lambda: ls("//tmp", read_from="cache"))

    def set_peer_count(self, peer_count):
        set("//sys/@config/node_tracker/master_cache_manager/peer_count", peer_count)

    def set_max_peers_per_rack(self, max_peers_per_rack):
        set("//sys/@config/node_tracker/master_cache_manager/max_peers_per_rack", max_peers_per_rack)

    def set_update_period(self, update_period):
        set("//sys/@config/node_tracker/master_cache_manager/update_period", update_period)

    def set_node_tag_filter(self, node_tag_filter):
        set("//sys/@config/node_tracker/master_cache_manager/node_tag_filter", node_tag_filter)


class TestTimestampProvider(Base, YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    TIMESTAMP_PROVIDER_CONFIG = {
        "enable_timestamp_provider_discovery": True,
        "timestamp_provider_discovery_period": 100,
        "addresses": [],
        "cell_id": "1-1-1-1"
    }

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "timestamp_provider": TIMESTAMP_PROVIDER_CONFIG
        }
    }

    DELTA_DRIVER_CONFIG = {
        "timestamp_provider": TIMESTAMP_PROVIDER_CONFIG
    }

    def get_discovered_node_list(self):
        return sorted(get("//sys/cluster_nodes/@timestamp_provider_nodes"))

    def get_node_addresses_list(self, node):		
         return sorted(list(get("//sys/cluster_nodes/{}/orchid/cluster_connection/timestamp_provider/channel_attributes/addresses".format(node))))

    def wait_for_config(self, expected_node_ids):
        expected_nodes = self.select_nodes(expected_node_ids)
        print>>stderr, "Expecting timestamp providers:", expected_nodes

        wait(lambda: self.get_discovered_node_list() == expected_nodes, "Master cannot update list of timestamp providers")
        if expected_nodes:
            self.wait_for_config_at_nodes(expected_nodes)
        self.wait_for_requests(
            expected_node_ids,
            {"service": "TimestampService", "method": "GenerateTimestamps"},
            start_transaction)

    def set_update_period(self, update_period):
        set("//sys/@config/node_tracker/timestamp_provider_manager/update_period", update_period)

    def set_node_tag_filter(self, node_tag_filter):
        set("//sys/@config/node_tracker/timestamp_provider_manager/node_tag_filter", node_tag_filter)

    def set_peer_count(self, peer_count):
        set("//sys/@config/node_tracker/timestamp_provider_manager/peer_count", peer_count)

    def set_max_peers_per_rack(self, max_peers_per_rack):
        set("//sys/@config/node_tracker/timestamp_provider_manager/max_peers_per_rack", max_peers_per_rack)
