from yt_env_setup import YTEnvSetup
from yt_commands import authors, wait, get, set, exists, ls

##################################################################


class TestMasterCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_MASTER_CACHES = 1

    USE_MASTER_CACHE = True

    DELTA_MASTER_CACHE_CONFIG = {
        "cypress_registrar": {
            "update_period": 100,
        }
    }

    @authors("gritukan")
    def test_read_from_cache(self):
        set("//tmp/x", 123)
        assert get("//tmp/x", read_from="cache") == 123

    @authors("ifsmirnov")
    def test_cypress_registration(self):
        wait(lambda: exists("//sys/master_caches"))
        wait(lambda: get("//sys/master_caches/@count") > 0)
        address = ls("//sys/master_caches")[0]
        expiration_time = get(f"//sys/master_caches/{address}/@expiration_time")
        wait(lambda: get(f"//sys/master_caches/{address}/@expiration_time") != expiration_time)
        assert get(f"//sys/master_caches/{address}/orchid/service/name") == "master_cache"

##################################################################


class TestMasterCacheMulticell(TestMasterCache):
    NUM_SECONDARY_MASTER_CELLS = 2
