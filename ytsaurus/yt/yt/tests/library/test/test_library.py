from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait)

from time import sleep

import pytest


class TestYtTestLibrary(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    @authors("lukyan")
    @pytest.mark.timeout(2)
    @pytest.mark.xfail
    def test_timeout_plugin(self):
        sleep(5)

    @authors("lukyan")
    @pytest.mark.xfail
    def test_wait(self):
        def predicate():
            pytest.fail("Test is definitely failed. We do not want to wait.")
        wait(predicate, ignore_exceptions=True)
