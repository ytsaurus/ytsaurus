from yt_commands import authors, generate_timestamp, get_driver
from yt_env_setup import YTEnvSetup
from yt.common import YtError

import pytest


class TClockTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_REMOTE_CLUSTERS = 1
    NUM_CLOCKS = 1
    NUM_TIMESTAMP_PROVIDERS = 1
    USE_PRIMARY_CLOCKS = False


class TestAlienTsProviders(TClockTestBase):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    @classmethod
    def modify_timestamp_providers_configs(cls, timestamp_providers_configs, clock_configs, yt_configs):
        primary_timestamp_provider_configs = timestamp_providers_configs[0]
        remote_0_clock_config_by_cell_tag = clock_configs[1]
        remote_0_clock_configs = remote_0_clock_config_by_cell_tag[remote_0_clock_config_by_cell_tag["cell_tag"]]

        remote_0_clock_addresses = [f"localhost:{clock_config['rpc_port']}" for clock_config in remote_0_clock_configs]

        for primary_timestamp_provider_config in primary_timestamp_provider_configs:
            primary_timestamp_provider_config["alien_timestamp_providers"] = [
                {
                    "clock_cluster_tag": yt_configs[1].primary_cell_tag,
                    "timestamp_provider": {
                        "addresses": remote_0_clock_addresses
                    },
                }
            ]
            primary_timestamp_provider_config["clock_cluster_tag"] = yt_configs[0].primary_cell_tag

        return True

    @authors("osidorkin")
    def test_timestamp_generation(self):
        driver = get_driver(cluster=self.get_cluster_name(0))
        generate_timestamp(driver=driver)
        generate_timestamp(clock_cluster_tag=10, driver=driver)
        generate_timestamp(clock_cluster_tag=20, driver=driver)
        with pytest.raises(YtError):
            generate_timestamp(clock_cluster_tag=3, driver=driver)


class TestClockWithClusterTag(TestAlienTsProviders):
    @classmethod
    def modify_clock_config(cls, config, cluster_index, master_cell_tag):
        config["clock_cluster_tag"] = int(master_cell_tag)


class TestClockMisconfiguration(TClockTestBase):
    @classmethod
    def modify_clock_config(cls, config, cluster_index, master_cell_tag):
        config["clock_cluster_tag"] = int(master_cell_tag) + 1

    @classmethod
    def modify_timestamp_providers_configs(cls, timestamp_providers_configs, clock_configs, yt_configs):
        primary_timestamp_provider_configs = timestamp_providers_configs[0]
        remote_0_clock_config_by_cell_tag = clock_configs[1]
        remote_0_clock_configs = remote_0_clock_config_by_cell_tag[remote_0_clock_config_by_cell_tag["cell_tag"]]

        remote_0_clock_addresses = [f"localhost:{clock_config['rpc_port']}" for clock_config in remote_0_clock_configs]

        for primary_timestamp_provider_config in primary_timestamp_provider_configs:
            primary_timestamp_provider_config["alien_timestamp_providers"] = [
                {
                    "clock_cluster_tag": yt_configs[1].primary_cell_tag,
                    "timestamp_provider": {
                        "addresses": remote_0_clock_addresses
                    },
                }
            ]
            primary_timestamp_provider_config["clock_cluster_tag"] = yt_configs[0].primary_cell_tag

        return True

    @authors("osidorkin")
    def test_timestamp_generation(self):
        driver = get_driver(cluster=self.get_cluster_name(0))
        generate_timestamp(driver=driver)
        with pytest.raises(YtError):
            generate_timestamp(clock_cluster_tag=10, driver=driver)
        with pytest.raises(YtError):
            generate_timestamp(clock_cluster_tag=20, driver=driver)
        with pytest.raises(YtError):
            generate_timestamp(clock_cluster_tag=3, driver=driver)
