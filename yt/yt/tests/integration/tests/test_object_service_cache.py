from yt_env_setup import YTEnvSetup
from yt_commands import *

import time

##################################################################


class TestObjectServiceCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1

    MASTER_CACHE_CONFIG = {
        "enable_master_cache_discovery": True,
        "master_cache_discovery_period": 100,
        "addresses": [],
        "cell_id": "1-1-1-1",
    }

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "master_cache": MASTER_CACHE_CONFIG
        },
        "master_cache_service": {
            "top_entry_byte_rate_threshold": 1
        }
    }

    DELTA_DRIVER_CONFIG = {"master_cache": MASTER_CACHE_CONFIG}

    @authors("aleksandra-zh")
    def test_orchid(self):
        wait(
            lambda: len(get("//sys/cluster_nodes/@master_cache_nodes")) == 1,
            "Master cannot update list of master caches",
        )

        node = get("//sys/cluster_nodes/@master_cache_nodes")[0]
        for _ in range(15):
            ls("//tmp", read_from="cache")
            time.sleep(0.25)

        assert len(get("//sys/cluster_nodes/{0}/orchid/object_service_cache/top_requests".format(node))) > 0
