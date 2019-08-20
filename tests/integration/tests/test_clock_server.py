import pytest
from time import sleep
from operator import itemgetter
from copy import deepcopy
from flaky import flaky

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

##################################################################

class TestClock(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_CLOCKS = 1
    NUM_NODES = 0
    NUM_SCHEDULERS = 0

    @authors("savrus")
    def test_clock(self):
        config = deepcopy(self.Env.configs["clock_driver"])
        config["api_version"] = 4
        driver = Driver(config=config)
        t1 = generate_timestamp()
        t2 = generate_timestamp(driver=driver)
        assert abs((t1 >> 30) - (t2 >> 30)) < 2
        pass

##################################################################

class TestClockMulticell(TestClock):
    NUM_SECONDARY_MASTER_CELLS = 1

