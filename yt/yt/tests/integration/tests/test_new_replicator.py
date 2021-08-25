from yt_env_setup import YTEnvSetup

from yt_commands import authors


##################################################################


class TestNewReplicator(YTEnvSetup):
    DELTA_MASTER_CONFIG = {
        "use_new_replicator": True,
    }

    @authors("gritukan")
    def test_simple(self):
        pass


##################################################################


class TestNewReplicatorMulticell(TestNewReplicator):
    NUM_SECONDARY_MASTER_CELLS = 2
