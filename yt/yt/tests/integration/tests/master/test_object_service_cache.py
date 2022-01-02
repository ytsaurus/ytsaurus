from yt_env_setup import YTEnvSetup
from yt_commands import authors, wait, get, ls

##################################################################


class TestObjectServiceCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1

    USE_MASTER_CACHE = True

    DELTA_NODE_CONFIG = {
        "master_cache_service": {
            "top_entry_byte_rate_threshold": 1
        }
    }

    @authors("aleksandra-zh")
    def test_orchid(self):
        wait(
            lambda: len(get("//sys/cluster_nodes/@master_cache_nodes")) == 1,
            "Master cannot update list of master caches",
        )

        node = get("//sys/cluster_nodes/@master_cache_nodes")[0]

        def check():
            ls("//tmp", read_from="cache")
            requests = get("//sys/cluster_nodes/{0}/orchid/object_service_cache/top_requests".format(node))
            return any(r["path"] == "//tmp" and r["method"] == "List" and r["service"] == "Node" for r in requests)
        wait(check)
