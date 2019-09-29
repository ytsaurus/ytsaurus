from yt.common import update

import yt.wrapper as yt

import os
import pytest

@pytest.mark.usefixtures("yt_env")
class TestConfig(object):
    @pytest.mark.usefixtures("config")
    def test_special_config_options(self, config):
        env_merge_option = os.environ.get("YT_MERGE_INSTEAD_WARNING", None)
        try:
            os.environ["YT_MERGE_INSTEAD_WARNING"] = "1"
            yt.config._update_from_env()
            assert yt.config["auto_merge_output"]["action"] == "merge"
            os.environ["YT_MERGE_INSTEAD_WARNING"] = "0"
            yt.config._update_from_env()
            assert yt.config["auto_merge_output"]["action"] == "log"
        finally:
            if env_merge_option is not None:
                os.environ["YT_MERGE_INSTEAD_WARNING"] = env_merge_option
            update(yt.config.config, config)

