from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, update_nodes_dynamic_config, wait,
    get, set, ls, create, read_table, write_table,
    get_singular_chunk_id)

from yt_helpers import profiler_factory


import time
import threading


class TestP2P(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 4
    NUM_SCHEDULERS = 0

    DELTA_NODE_CONFIG = {
        "data_node": {
            "p2p_block_distributor": {
                "enabled": False,
            },
            "p2p": {
                "enabled": True,
                "node_refresh_period": 1000,
                "chunk_cooldown_timeout": 10000,
                "hot_block_threshold": 3,
                "second_hot_block_threshold": 3,
                "node_tag_filter": "!tag42",
            }
        }
    }

    def setup_method(self):
        self.write_table()

        eligible_nodes = profiler_factory().at_node(self.seed).gauge("data_node/p2p/eligible_nodes")

        def check():
            count = eligible_nodes.get()
            return count is not None and count > 0
        wait(check)

        create("table", "//tmp/sync_node_directory", force=True)
        set("//tmp/sync_node_directory/@replication_factor", len(ls("//sys/cluster_nodes")))
        write_table("//tmp/sync_node_directory", [{"a": 1}])
        read_table("//tmp/sync_node_directory")

    def teardown_method(self):
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{0}/@user_tags".format(node), [])

    def write_table(self):
        create("table", "//tmp/t", force=True)
        set("//tmp/t/@replication_factor", 1)
        write_table("//tmp/t", [{"a": 1}])
        chunk_id = get_singular_chunk_id("//tmp/t")

        # Node that is the seed for the only existing chunk.
        self.seed = str(get("#{0}/@stored_replicas/0".format(chunk_id)))
        self.nodes = ls("//sys/cluster_nodes")
        self.non_seeds = ls("//sys/cluster_nodes")
        self.non_seeds.remove(self.seed)
        assert len(self.non_seeds) == 3

    def access_table(self):
        read_table("//tmp/t")

    def seed_counter(self, path):
        return profiler_factory().at_node(self.seed).counter(path)

    def peer_counter(self, peer, path):
        return profiler_factory().at_node(peer).counter(path)

    @authors("prime")
    def test_no_distribution(self):
        throttled = self.seed_counter("data_node/p2p/throttled_bytes")

        for _ in range(2):
            self.access_table()
        time.sleep(2)

        assert throttled.get_delta() == 0

    @authors("prime")
    def test_simple_distribution(self):
        throttled = self.seed_counter("data_node/p2p/throttled_bytes")
        distributed = self.seed_counter("data_node/p2p/distributed_bytes")
        peer_hit = [self.peer_counter(peer, "data_node/p2p/hit_bytes") for peer in self.non_seeds]

        for _ in range(6):
            self.access_table()
        time.sleep(2)

        wait(lambda: throttled.get_delta() > 0)
        wait(lambda: distributed.get_delta() > 0)

        assert sum(c.get_delta() for c in peer_hit) > 0

    @authors("prime")
    def test_node_filter_tags(self):
        for non_seed in self.non_seeds:
            set("//sys/cluster_nodes/{0}/@user_tags".format(non_seed), ["tag42"])

        eligible_nodes = profiler_factory().at_node(self.seed).gauge("data_node/p2p/eligible_nodes")
        wait(lambda: eligible_nodes.get() == 0.0)

    @authors("prime")
    def test_chunk_cooldown(self):
        hot_chunk_count = profiler_factory().at_node(self.seed).gauge("data_node/p2p/hot_chunks")

        for _ in range(6):
            self.access_table()

        wait(lambda: hot_chunk_count.get() == 1)
        wait(lambda: hot_chunk_count.get() == 0)

    @authors("prime")
    def test_stress(self):
        def stress_table(i):
            path = "//tmp/t" + str(i)
            create("table", path, force=True)

            for i in range(16):
                write_table("<append=%true>" + path, [{"a": 1}])

            def run_reader():
                for _ in range(4):
                    read_table(path)

            readers = []
            for i in range(3):
                t = threading.Thread(target=run_reader)
                t.start()
                readers.append(t)

            for t in readers:
                t.join()

        threads = []
        for i in range(3):
            t = threading.Thread(target=stress_table, args=(i,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    @authors("prime")
    def test_dynamic_config(self):
        throttled = self.seed_counter("data_node/p2p/throttled_bytes")

        update_nodes_dynamic_config({
            "p2p": {"enabled": False},
        }, path="data_node", replace=True)

        for _ in range(10):
            self.access_table()
        time.sleep(2)

        assert throttled.get_delta() == 0

        update_nodes_dynamic_config({}, path="data_node", replace=True)

    @authors("prime")
    def test_last_seen_online(self):
        eligible_nodes = profiler_factory().at_node(self.seed).gauge("data_node/p2p/eligible_nodes")
        assert eligible_nodes.get() > 0

        update_nodes_dynamic_config({
            "p2p": {
                "enabled": True,
                "node_refresh_period": 1000,
                "node_staleness_timeout": 0,
            },
        }, path="data_node", replace=True)

        wait(lambda: eligible_nodes.get() == 0)

        update_nodes_dynamic_config({}, path="data_node", replace=True)
