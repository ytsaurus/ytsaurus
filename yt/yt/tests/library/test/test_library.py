from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait)

from time import sleep

import os
import pytest


class TestYtTestLibrary(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1

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

    @authors("ni-stoiko")
    @pytest.mark.skip(reason="This test should fail in teardown")
    def test_check_disabled_locations(self):
        location_key_lists = [
            ["data_node", "cache_locations"],
            ["data_node", "volume_manager", "layer_locations"],
            ["data_node", "store_locations"],
            ["exec_agent", "slot_manager", "locations"],
            ["exec_node", "slot_manager", "locations"],
        ]
        location_disabled = False
        for node_config in self.Env.configs["node"]:
            for location_key_list in location_key_lists:
                try:
                    locations = self._walk_dictionary(node_config, location_key_list)
                    if locations:
                        for location in locations:
                            path = location["path"]
                            if not os.path.exists(path):
                                os.mkdir(path)
                            with open(f"{path}/disabled", "w") as file:
                                file.flush()
                            location_disabled = True
                except KeyError:
                    pass
        assert location_disabled, "Some locations must be disabled"
