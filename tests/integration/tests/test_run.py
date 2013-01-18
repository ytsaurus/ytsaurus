from yt_env_setup import YTEnvSetup

import unittest

##################################################################

class TestRunNothing(YTEnvSetup, unittest.TestCase):
    NUM_MASTERS = 0
    NUM_NODES = 0

    def test(self):
        assert True

class TestRunMaster(YTEnvSetup, unittest.TestCase):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def test(self):
        assert True

class TestRunNode(YTEnvSetup, unittest.TestCase):
    NUM_MASTERS = 1
    NUM_NODES = 1

    def test(self):
        assert True

class TestRunScheduler(YTEnvSetup, unittest.TestCase):
    NUM_MASTERS = 1
    NUM_NODES = 0
    START_SCHEDULER = True

    def test(self):
        assert True

class TestRunAll(YTEnvSetup, unittest.TestCase):
    NUM_MASTERS = 1
    NUM_NODES = 1
    START_SCHEDULER = True

    def test(self):
        assert True

