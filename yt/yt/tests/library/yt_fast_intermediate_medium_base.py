from yt_env_setup import YTEnvSetup

from yt_commands import (
    get_account_disk_space_limit,
    set, set_account_disk_space_limit,
)

##################################################################


class TestFastIntermediateMediumBase(YTEnvSetup):
    FAST_MEDIUM = "ssd_blobs"
    SLOW_MEDIUM = "default"

    MEDIUM_CONFIG = {
        SLOW_MEDIUM: {},
        FAST_MEDIUM: {},
    }

    INTERMEDIATE_ACCOUNT = "intermediate"
    FAST_INTERMEDIATE_MEDIUM_LIMIT = 1 << 30

    @classmethod
    def setup_class(cls):
        super(TestFastIntermediateMediumBase, cls).setup_class()
        disk_space_limit = get_account_disk_space_limit("tmp", "default")
        set_account_disk_space_limit("tmp", disk_space_limit, TestFastIntermediateMediumBase.FAST_MEDIUM)

    @classmethod
    def on_masters_started(cls):
        set(f"//sys/accounts/{cls.INTERMEDIATE_ACCOUNT}/@resource_limits/disk_space_per_medium/{cls.FAST_MEDIUM}", cls.FAST_INTERMEDIATE_MEDIUM_LIMIT)
