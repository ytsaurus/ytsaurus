from yt_env_setup import YTEnvSetup

##################################################################

class TestRunNothing(YTEnvSetup):
    NUM_MASTERS = 0
    NUM_NODES = 0

    def test(self):
        assert True

class TestRunMaster(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def test(self):
        assert True

class TestRunHolder(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1

    def test(self):
        assert True

class TestRunScheduler(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    START_SCHEDULER = True

    def test(self):
        assert True

class TestRunAll(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    START_SCHEDULER = True

    def test(self):
        assert True

