from yt_env_setup import YTEnvSetup

from yt_commands import authors


##################################################################


class TestCellBalancer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_TABLET_BALANCERS = 3
    USE_DYNAMIC_TABLES = True

    @authors("alexelexa")
    def test_tablet_balancer_exists(self):
        pass
