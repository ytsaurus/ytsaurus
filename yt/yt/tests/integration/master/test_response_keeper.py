from yt_env_setup import YTEnvSetup

from yt_commands import authors, generate_uuid, get, create, raises_yt_error

import time

################################################################################


class TestResponseKeeper(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
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
        with raises_yt_error("Mutation is already applied"):
            create("table", "//tmp/t", mutation_id=mutation_id)

        # Denined by response keeper.
        with raises_yt_error("Duplicate request is not marked"):
            create("table", "//tmp/t", mutation_id=mutation_id)

        # Replied from response keeper.
        with raises_yt_error("Mutation is already applied"):
            create("table", "//tmp/t", mutation_id=mutation_id, retry=True)

        time.sleep(1.6)

        # Denied by mutation idempotizer again.
        with raises_yt_error("Mutation is already applied"):
            create("table", "//tmp/t", mutation_id=mutation_id)

        assert get("//tmp/t/@id") == table_id

    @authors("achulkov2")
    def test_host_sanitization(self):
        create("table", "//tmp/t")

        with raises_yt_error() as err:
            create("table", "//tmp/t")

        # All host names are equal to "localhost", so the sanitized host name is also "localhost".
        assert err[0].inner_errors[0]["attributes"]["host"] == "localhost"


class TestSequoiaResponseKeeper(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    VALIDATE_SEQUOIA_TREE_CONSISTENCY = True
    NUM_CYPRESS_PROXIES = 1
    NUM_HTTP_PROXIES = 0
    NUM_RPC_PROXIES = 0

    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["sequoia_node_host"]},
    }

    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "object_service": {
            "allow_bypass_master_resolve": True,
        },
        "response_keeper": {
            "enable": True,
        },
    }

    @authors("cherepashka")
    def test_idempotent_create(self):
        mutation_id = generate_uuid()
        table_id = create("table", "//tmp/t", mutation_id=mutation_id)

        # Denined by Sequoia response keeper.
        with raises_yt_error("Duplicate request is not marked"):
            create("table", "//tmp/t", mutation_id=mutation_id)

        # Responded by Sequoia response keeper.
        assert table_id == create("table", "//tmp/t", mutation_id=mutation_id, retry=True)

        assert table_id == get("//tmp/t/@id")
