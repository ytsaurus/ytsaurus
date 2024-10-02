from yt.common import YtResponseError

import pytest


class TestFullScan:
    EXAMPLE_MASTER_CONFIG = {
        "transaction_manager": {
            "full_scan_allowed_by_default": False,
        },
    }

    def test_allow_full_scan_by_config(self, example_env):
        config_patch = dict(
            transaction_manager=dict(
                full_scan_allowed_by_default=True,
            ),
        )

        with example_env.set_cypress_config_patch_in_context(config_patch):
            example_env.client.select_objects("book")

    def test_allow_full_scan_by_option(self, example_env):
        example_env.client.select_objects(
            "book",
            common_options=dict(allow_full_scan=True),
        )

    def test_disallow_full_scan(self, example_env):
        with pytest.raises(YtResponseError):
            example_env.client.select_objects("book")
