import pytest
from time import sleep
from operator import itemgetter
from copy import deepcopy
from flaky import flaky

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

##################################################################

class TestClockServer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_CLOCKS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    @authors("savrus")
    def test_generate_timestamp(self):
        config = deepcopy(self.Env.configs["clock_driver"])
        config["api_version"] = 4
        driver = Driver(config=config)
        t1 = generate_timestamp()
        t2 = generate_timestamp(driver=driver)
        assert abs((t1 >> 30) - (t2 >> 30)) < 2

    @authors("babenko")
    def test_tx(self):
        tx = start_transaction()
        commit_transaction(tx)

##################################################################

class TestClockServerMulticell(TestClockServer):
    NUM_SECONDARY_MASTER_CELLS = 1

