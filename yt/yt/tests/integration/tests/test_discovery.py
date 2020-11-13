from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################


class TestDiscovery(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    @authors("aleksandra-zh")
    def test_orchid(self):
        master = ls("//sys/primary_masters")[0]
        assert exists("//sys/primary_masters/{0}/orchid/discovery_server".format(master))

        assert exists("//sys/discovery/primary_master_cell")
        cell = ls("//sys/secondary_masters")[0]
        assert exists("//sys/discovery/secondary_master_cells/{0}".format(cell))
