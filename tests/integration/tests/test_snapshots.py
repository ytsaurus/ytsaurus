from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestSnapshot(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def test(self):
    	set("//tmp/a", 42)

        build_snapshot()

        self.Env._kill_service("master")
        self.Env.start_masters("master")

        assert get("//tmp/a") == 42
