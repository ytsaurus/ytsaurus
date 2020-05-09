from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestDiscovery(YTEnvSetup):
    NUM_MASTERS = 1

    @authors("aleksandra-zh")
    def test_orchid(self):
        master = ls("//sys/primary_masters")[0]
        # assert exists("//sys/primary_masters/{0}/orchid/discovery_server".format(master))
