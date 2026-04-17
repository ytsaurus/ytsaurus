from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, remove, start_transaction, commit_transaction,
)

##################################################################


class TestSequoiaLatency(YTEnvSetup):
    ENABLE_MULTIDAEMON = False
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    NUM_SECONDARY_MASTER_CELLS = 4
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["cypress_node_host", "transaction_coordinator", "sequoia_node_host"]},
        "13": {"roles": ["chunk_host"]},
        "14": {"roles": ["sequoia_node_host"]},
    }

    DELTA_MASTER_CONFIG = {
        "object_service": {
            "enable_local_read_busy_wait": True,
        },
        "hive_manager": {
            "ping_period": 15000,
            "idle_post_period": 15000,
        },
        "hydra_manager": {
            "minimize_commit_latency": False,
            "leader_sync_delay": 10,
        },
        "transaction_supervisor": {
            "testing": {
                "prepared_transactions_barrier_delay": 150,
            },
        },
    }

    @authors("kvk1920")
    def test_latency(self):
        tx = start_transaction()
        node = create("table", "//tmp/t", tx=tx)
        remove(f"#{node}", tx=tx)
        commit_transaction(tx)
