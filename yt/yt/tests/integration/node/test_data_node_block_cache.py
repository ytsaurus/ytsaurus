from yt_env_setup import YTEnvSetup

from yt_helpers import profiler_factory

from yt_commands import authors, create, get, read_table, write_table

import time


@authors("jmvssnovikov")
class TestDataNodeBlockCache(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 0

    BLOCK_CACHE_CAPACITY = 64 * 1024

    DELTA_NODE_CONFIG = {
        "resource_limits": {
            "memory_limits": {
                "block_cache": {
                    "type": "static",
                    "value": BLOCK_CACHE_CAPACITY,
                },
            },
        },
        "data_node": {
            "block_cache": {
                "compressed_data": {
                    "capacity": BLOCK_CACHE_CAPACITY,
                },
                "uncompressed_data": {
                    "capacity": 0,
                },
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "data_node": {
                "fail_session_at_read_blocks_deadline": True,
                "return_blocks_if_session_fails": False,
                "testing_options": {
                    "block_read_timeout_fraction": 0.75,
                },
            },
        },
    }

    def test_session_deadline_cancelled_on_fast_complete(self):
        create("table", "//tmp/t", attributes={"replication_factor": 1})
        rows = [{"index": index, "payload": "x" * 1024} for index in range(1024)]
        write_table(
            "//tmp/t",
            rows,
            table_writer={"block_size": 4 * 1024, "desired_chunk_size": 2 * 1024 * 1024},
        )

        chunk_id = get("//tmp/t/@chunk_ids/0")
        assert get(f"#{chunk_id}/@max_block_size") <= 8 * 1024
        node = str(get(f"#{chunk_id}/@stored_replicas/0"))

        assert list(read_table("//tmp/t", table_reader={"node_rpc_timeout": 30000})) == rows
        time.sleep(0.1)

        block_cache_memory_used = (
            profiler_factory()
            .at_node(node)
            .gauge(
                "cluster_node/memory_usage/used",
                fixed_tags={"category": "block_cache"},
            )
        )
        assert block_cache_memory_used.get() <= self.BLOCK_CACHE_CAPACITY
