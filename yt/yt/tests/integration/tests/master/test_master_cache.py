from yt_env_setup import YTEnvSetup
from yt_commands import authors, get, set

##################################################################


class TestMasterCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_MASTER_CACHES = 1

    USE_MASTER_CACHE = True

    @authors("gritukan")
    def test_read_from_cache(self):
        set("//tmp/x", 123)
        assert get("//tmp/x", read_from="cache") == 123

##################################################################


class TestMasterCacheMulticell(TestMasterCache):
    NUM_SECONDARY_MASTER_CELLS = 2
