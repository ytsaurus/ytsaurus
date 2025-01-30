from yt_env_setup import YTEnvSetup

from yt_commands import (
    create_domestic_medium, get_account_disk_space_limit,
    set, set_account_disk_space_limit,
)

##################################################################


class TestFastIntermediateMediumBase(YTEnvSetup):
    STORE_LOCATION_COUNT = 2

    FAST_MEDIUM = "ssd_blobs"
    SLOW_MEDIUM = "default"

    INTERMEDIATE_ACCOUNT = "intermediate"
    FAST_INTERMEDIATE_MEDIUM_LIMIT = 1 << 30

    @classmethod
    def setup_class(cls):
        super(TestFastIntermediateMediumBase, cls).setup_class()
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, TestFastIntermediateMediumBase.FAST_MEDIUM)

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        assert len(config["data_node"]["store_locations"]) == 2

        config["data_node"]["store_locations"][0]["medium_name"] = cls.SLOW_MEDIUM
        config["data_node"]["store_locations"][1]["medium_name"] = cls.FAST_MEDIUM

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium(cls.FAST_MEDIUM)
        set(f"//sys/accounts/{cls.INTERMEDIATE_ACCOUNT}/@resource_limits/disk_space_per_medium/{cls.FAST_MEDIUM}", cls.FAST_INTERMEDIATE_MEDIUM_LIMIT)
