from yt_env_setup import YTEnvSetup

from yt_commands import authors, ls, get


##################################################################


class TestCellBalancer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_CELL_BALANCERS = 3
    USE_DYNAMIC_TABLES = True

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "enable_cell_tracker": False
        },
    }

    @authors("alexkolodezny")
    def test_sys_cell_balancer(self):
        assert len(ls("//sys/cell_balancers/instances")) == self.NUM_CELL_BALANCERS
        for cell_balancer in ls("//sys/cell_balancers/instances"):
            assert "monitoring" in get("//sys/cell_balancers/instances/{}/orchid".format(cell_balancer))

    @authors("alexkolodezny")
    def test_cell_balancer_election(self):
        assert len(ls("//sys/cell_balancers/instances")) == self.NUM_CELL_BALANCERS
        leader_count = 0
        for cell_balancer in ls("//sys/cell_balancers/instances"):
            leader_count += get(
                "//sys/cell_balancers/instances/{}/orchid/cell_balancer/service/connected".format(cell_balancer)
            )
        assert leader_count == 1
