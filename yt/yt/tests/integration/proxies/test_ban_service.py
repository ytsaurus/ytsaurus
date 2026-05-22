from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, get_user_banned, set_user_banned, list_banned_users,
    set, create, get_driver, wait,
)

import pytest

from time import sleep


##################################################################

@pytest.mark.enabled_multidaemon
class TestBanService(YTEnvSetup):
    NUM_MASTERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    NUM_CYPRESS_PROXIES = 1

    ENABLE_MULTIDAEMON = True

    NUM_REMOTE_CLUSTERS = 3
    DELTA_MASTER_CONFIG = {
        "object_service": {
            "timeout_backoff_lead_time": 100,
        },
    }
    DELTA_DRIVER_CONFIG = {
        "transaction_manager": {
            "ping_batcher": {
                "enable": True,
                "batch_size": 2,
                "batch_period": 1000,
            },
        },
    }
    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "dynamic_config_manager": {
            "update_period": 1000,
        },
        "ban_service": {
            "enable": False,
            "cache_refresh_period": 1000,
            "cross_cluster_replicated_state": {
                "replicas": [
                    {
                        "cluster_name": "remote_0",
                        "state_directory": "//tmp/banned_users",
                    },
                    {
                        "cluster_name": "remote_1",
                        "state_directory": "//tmp/banned_users",
                    },
                    {
                        "cluster_name": "remote_2",
                        "state_directory": "//tmp/banned_users",
                    },
                ],
            },
        },
    }

    @authors("koloshmet")
    def test(self):
        drivers = [
            get_driver(cluster="remote_0"),
            get_driver(cluster="remote_1"),
            get_driver(cluster="remote_2"),
        ]
        for d in drivers:
            create("map_node", "//tmp/banned_users", driver=d)

        set("//sys/cypress_proxies/@config/ban_service/enable", True)
        set("//sys/cypress_proxies/@config/ban_service/use_in_object_service", True)

        sleep(2)

        assert not get_user_banned("user")
        set_user_banned("user", True)
        assert get_user_banned("user")
        wait(lambda: list_banned_users() == ["user"])
