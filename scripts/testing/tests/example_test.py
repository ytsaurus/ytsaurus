
from yt_env_setup import YTEnvSetup

class TestSomething(YTEnvSetup):
    NUM_MASTERS = 0
    NUM_HOLDERS = 0
    SETUP_TIMEOUT = 0

    def test_1(self):
        print 'in test_1'
        assert True

    def test_2(self):
        print 'in test_2'
        assert True

