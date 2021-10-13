from yt_env_setup import YTEnvSetup

from yt_commands import authors


##################################################################


class TestCellBalancer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_CELL_BALANCERS = 1

    @authors("alexkolodezny")
    def test_cell_balancer_exists(self):
        pass
