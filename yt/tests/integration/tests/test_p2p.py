import pytest

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

from yt.environment.helpers import assert_items_equal

from sys import stderr

##################################################################

# This class provides effective means for getting information from YT
# profiling information exported via Orchid. Wrap some calculations into "with"
# section using it as a context manager, and then call `get` method of the
# remaining Profile object to get exactly the slice of given profiling path
# corresponding to the time spend in "with" section.
class Profile(object):
    def __init__(self, node, path):
        self.path = "//sys/nodes/{0}/orchid/profiling/{1}".format(node, path)

    def _get(self):
        try:
            return get(self.path)
        except YtError:
            return []

    def __enter__(self):
        self.len_on_enter = len(self._get())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # False return value makes python re-raise the exception happened inside "with" section.
            return False
        else:
            self.profile = self._get()

    def get(self):
        return self.profile[self.len_on_enter:]

    def total(self):
        return sum(event["value"] for event in self.get())

    def _up_to_moment(self, i):
        return self.profile[i - 1]["value"] if i > 0 else 0

    def differentiate(self):
        return self._up_to_moment(len(self.profile)) - self._up_to_moment(self.len_on_enter)

def clear_everything_after_test(func):
    def wrapped(*args, **kwargs):
        func(*args, **kwargs)
        self = args[0]
        self.Env.kill_nodes()
        self.Env.start_nodes()
        nodes = list(get("//sys/nodes"))
        for node in nodes:
            set("//sys/nodes/{0}/@user_tags".format(node), [])
    return wrapped

class TestBlockPeerDistributorSynthetic(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 4
    NUM_SCHEDULERS = 0

    DELTA_NODE_CONFIG = {
        "data_node": {
            "peer_block_distributor": {
                "iteration_period": 100, # 0.1 sec
                "window_length": 1000, # 1 sec,
                # In tests we are always trying to distribute something.
                "out_traffic_activation_threshold": 0,
                "node_tag_filter": "!tag42",
                "min_request_count": 3,
                "max_distribution_count": 1,
                "destination_node_count": 2,
            },
            "block_cache": {
                "compressed_data": {
                    "capacity": 256 * 1024 * 1024,
                }
            }
        }
    }

    def setup(self):
        create("table", "//tmp/t")
        set("//tmp/t/@replication_factor", 1)
        write_table("//tmp/t", [{"a": 1}])
        chunk_id = get("//tmp/t/@chunk_ids/0")
        # Node that is the seed for the only existing chunk.
        self.seed = str(get("#{0}/@stored_replicas/0".format(chunk_id)))
        self.nodes = ls("//sys/nodes")
        self.non_seeds = ls("//sys/nodes")
        self.non_seeds.remove(self.seed)
        assert len(self.non_seeds) == 3
        print >>sys.stderr, "Seed: ", self.seed
        print >>sys.stderr, "Non-seeds: ", self.non_seeds

    @classmethod
    def _access(cls):
        read_table("//tmp/t")

    @clear_everything_after_test
    def test_no_distribution(self):
        with Profile(self.seed, "data_node/p2p/distributed_block_size") as p:
            self._access()
            self._access()
            time.sleep(2)
        assert p.differentiate() == 0

    @clear_everything_after_test
    def test_simple_distribution(self):
        with Profile(self.seed, "data_node/p2p/distributed_block_size") as p:
            self._access()
            self._access()
            self._access()
            time.sleep(2)
        assert p.differentiate() > 0

    @clear_everything_after_test
    def test_node_filter_tags(self):
        for non_seed in self.non_seeds:
            set("//sys/nodes/{0}/@user_tags".format(non_seed), ["tag42"])
        # Wait for node directory to become updated.
        time.sleep(2)
        with Profile(self.seed, "data_node/p2p/distributed_block_size") as p:
            self._access()
            self._access()
            self._access()
            time.sleep(2)
        assert p.differentiate() == 0

class TestBlockPeerDistributorManyRequestsProduction(TestBlockPeerDistributorSynthetic):
    DELTA_NODE_CONFIG = {
        "data_node": {
            "peer_block_distributor": {
                "iteration_period": 100, # 0.1 sec
                "window_length": 1000, # 1 sec,
                # In tests we are always trying to distribute something.
                "out_traffic_activation_threshold": 0,
                "node_tag_filter": "!tag42",
                "min_request_count": 3,
                "max_distribution_count": 12, # As in production
                "destination_node_count": 2,
            },
            "block_cache": {
                "compressed_data": {
                    "capacity": 256 * 1024 * 1024,
                }
            }
        }
    }

    @clear_everything_after_test
    def test_wow_block_so_hot_such_much_requests(self):
        with Profile(self.seed, "data_node/block_cache/compressed_data/hit") as ps, \
            Profile(self.non_seeds[0], "data_node/block_cache/compressed_data/hit") as pns0, \
            Profile(self.non_seeds[1], "data_node/block_cache/compressed_data/hit") as pns1, \
            Profile(self.non_seeds[2], "data_node/block_cache/compressed_data/hit") as pns2:
            for i in range(300):
                self._access()
        assert ps.differentiate() > 0
        assert pns0.differentiate() > 0
        assert pns1.differentiate() > 0
        assert pns2.differentiate() > 0
