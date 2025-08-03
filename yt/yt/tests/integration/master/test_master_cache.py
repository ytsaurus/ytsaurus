from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, wait, get, set, exists, ls, generate_uuid)

import pytest

##################################################################


class MasterCacheTestBase(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("babenko")
    def test_read_from_cache(self):
        key = generate_uuid()
        set(f"//tmp/{key}", 123)
        assert get(f"//tmp/{key}", read_from="cache") == 123


@pytest.mark.enabled_multidaemon
class TestMasterCacheWithoutMasterCacheService(MasterCacheTestBase):
    pass


@pytest.mark.enabled_multidaemon
class TestMasterCacheWithMasterCacheService(MasterCacheTestBase):
    NUM_MASTER_CACHES = 1
    USE_MASTER_CACHE = True

    DELTA_MASTER_CACHE_CONFIG = {
        "cypress_registrar": {
            "update_period": 100,
        }
    }

    @authors("babenko")
    def test_read_from_cache_with_expire(self):
        key = generate_uuid()
        set(f"//tmp/{key}", 123)
        assert get(f"//tmp/{key}", read_from="cache", expire_after_successful_update_time=3000) == 123
        set(f"//tmp/{key}", 456)
        assert get(f"//tmp/{key}", read_from="cache", expire_after_successful_update_time=3000) == 123
        wait(lambda: get(f"//tmp/{key}", read_from="cache", expire_after_successful_update_time=3000) == 456)

    @authors("ifsmirnov")
    def test_cypress_registration(self):
        wait(lambda: exists("//sys/master_caches"))
        wait(lambda: get("//sys/master_caches/@count") > 0)
        address = ls("//sys/master_caches")[0]
        expiration_time = get(f"//sys/master_caches/{address}/@expiration_time")
        wait(lambda: get(f"//sys/master_caches/{address}/@expiration_time") != expiration_time)
        assert get(f"//sys/master_caches/{address}/orchid/service/name") == "master_cache"


@pytest.mark.enabled_multidaemon
class TestMasterCacheWithMasterCacheServiceMulticell(TestMasterCacheWithMasterCacheService):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2


@pytest.mark.enabled_multidaemon
class TestMasterCacheWithoutMasterCacheServiceSequoia(TestMasterCacheWithoutMasterCacheService):
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host", "sequoia_node_host"]},
        "12": {"roles": ["chunk_host"]},
    }


@pytest.mark.enabled_multidaemon
class TestMasterCacheWithMasterCacheServiceSequoia(TestMasterCacheWithMasterCacheService):
    NUM_MASTER_CACHES = 1
    USE_MASTER_CACHE = True
