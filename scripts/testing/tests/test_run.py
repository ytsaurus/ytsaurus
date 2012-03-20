
from yt_env_setup import YTEnvSetup

class TestRunMaster(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    def test(self):
        assert True
