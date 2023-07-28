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
    @pytest.mark.skip(reason="This test must fails in teardown")
    def test_check_disabled_nodes(self):
        locations_paths = [
            ["data_node", "cache_locations"],
            ["data_node", "volume_manager", "layer_locations"],
            ["data_node", "store_locations"],
            ["exec_agent", "slot_manager", "locations"],
            ["exec_node", "slot_manager", "locations"],
        ]
        was_created = False
        for node_config in self.Env.configs["node"]:
            for location_paths in locations_paths:
                try:
                    locations = self._get_from_json_by_key_list(node_config, location_paths)
                    if locations:
                        for location in locations:
                            path = location["path"]
                            if not os.path.exists(path):
                                os.mkdir(path)
                            with open(f"{path}/disabled", "w+") as file:
                                file.flush()
                            was_created = True
                except KeyError:
                    pass
        assert was_created, "Some disables must be created"
