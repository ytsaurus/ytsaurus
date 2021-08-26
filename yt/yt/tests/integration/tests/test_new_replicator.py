from yt_env_setup import YTEnvSetup

from yt_commands import authors, set, create_medium


##################################################################


class TestNewReplicator(YTEnvSetup):
    DELTA_MASTER_CONFIG = {
        "use_new_replicator": True,
    }

    @authors("gritukan")
    def test_simple(self):
        pass

    @authors("gritukan")
    def test_dynamic_config_change(self):
        set("//sys/@config/chunk_manager/max_heavy_columns", 10)

    @authors("gritukan")
    def test_medium_update(self):
        create_medium("nvme")
        set("//sys/media/nvme/@name", "optane")
        set("//sys/media/optane/@config/max_replicas_per_rack", 3)


##################################################################


class TestNewReplicatorMulticell(TestNewReplicator):
    NUM_SECONDARY_MASTER_CELLS = 2
