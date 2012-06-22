import unittest

from yt_env_setup import YTEnvSetup

##################################################################

class TestRunNothing(unittest.TestCase, YTEnvSetup):
    NUM_MASTERS = 0
    NUM_HOLDERS = 0

    def test(self):
        assert True

class TestRunMaster(unittest.TestCase, YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0

    def test(self):
        assert True

class TestRunHolder(unittest.TestCase, YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 1

    def test(self):
        assert True

class TestRunScheduler(unittest.TestCase, YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0
    NUM_SCHEDULERS = 1

    def test(self):
        assert True

