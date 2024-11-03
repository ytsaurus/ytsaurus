from yt_env_setup import YTEnvSetup

from yt_commands import authors, generate_uuid, get, create, raises_yt_error

from yt.common import YtError

import pytest

import time

################################################################################


class TestResponseKeeper(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 0

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "cell_master": {
            "response_keeper": {
                "eviction_period": 100,
                "expiration_timeout": 200,
            }
        }
    }

    @authors("shakurov")
    def test_excessively_long_retries(self):
        mutation_id = generate_uuid()
        create("table", "//tmp/t", mutation_id=mutation_id)
        table_id = get("//tmp/t/@id")

        time.sleep(1.6)

        # Denied by mutation idempotizer.
        with pytest.raises(YtError, match="Mutation is already applied"):
            create("table", "//tmp/t", mutation_id=mutation_id)

        # Denined by response keeper.
        with pytest.raises(YtError, match="Duplicate request is not marked"):
            create("table", "//tmp/t", mutation_id=mutation_id)

        # Replied from response keeper.
        with pytest.raises(YtError, match="Mutation is already applied"):
            create("table", "//tmp/t", mutation_id=mutation_id, retry=True)

        time.sleep(1.6)

        # Denied by mutation idempotizer again.
        with pytest.raises(YtError, match="Mutation is already applied"):
            create("table", "//tmp/t", mutation_id=mutation_id)

        assert get("//tmp/t/@id") == table_id

    @authors("achulkov2")
    def test_host_sanitization(self):
        create("table", "//tmp/t")

        with raises_yt_error() as err:
            create("table", "//tmp/t")

        # All host names are equal to "localhost", so the sanitized host name is also "localhost".
        assert err[0].inner_errors[0]["attributes"]["host"] == "localhost"


class TestResponseKeeperOldHydra(TestResponseKeeper):
    DELTA_MASTER_CONFIG = {
        "use_new_hydra": False
    }
