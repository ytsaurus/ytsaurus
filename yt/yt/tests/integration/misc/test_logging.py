from yt_env_setup import YTEnvSetup

from yt_commands import authors, wait, create, sync_mount_table, sync_create_cells, select_rows, start_transaction, set

import time

##################################################################


class TestDynamicTableLogger(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 1
    USE_DYNAMIC_TABLES = True

    DEBUG_LOG_TABLE = "//tmp/debug_log"
    ACCESS_LOG_TABLE = "//tmp/access_log"

    DELTA_MASTER_CONFIG = {
        "logging": {
            "flush_period": 100,
            "shutdown_busy_timeout": 1000,
            "rules": [
                {
                    "family": "plain_text",
                    "min_level": "debug",
                    "exclude_categories": ["Bus"],
                    "writers": ["debug_dynamic_table"],
                },
                {
                    "family": "structured",
                    "min_level": "debug",
                    "include_categories": ["Access"],
                    "writers": ["access_dynamic_table"]
                },
            ],
            "writers": {
                "debug_dynamic_table": {
                    "type": "dynamic_table",
                    "format": "yson",
                    "enable_host_field": True,
                    # We need to explicitly specify this for plain text yson logs.
                    "enable_system_messages": True,
                    # We need to specify this to format system messages correctly.
                    "system_message_family": "plain_text",
                    "table_path": DEBUG_LOG_TABLE,
                    "flush_period": 100,
                    # Helps skip messages logged while dynamic table is not created yet.
                    "write_backoff": {
                        "invocation_count": 1,
                    },
                },
                "access_dynamic_table": {
                    "type": "dynamic_table",
                    "format": "yson",
                    "enable_host_field": True,
                    "table_path": ACCESS_LOG_TABLE,
                    "common_fields": {
                        "cluster": "primary",
                    },
                    "flush_period": 400,
                },
            },
            # This is specified to test rate limits.
            "category_rate_limits": {
                # No concurrency logs for you!
                "Concurrency": 0,
            },
        }
    }

    def setup_method(self, method):
        super().setup_method(method)

        sync_create_cells(1)

        self._create_log_tables()

    @staticmethod
    def _create_log_tables():
        create("table", TestDynamicTableLogger.DEBUG_LOG_TABLE, attributes={"dynamic": True, "compression_codec": "zstd_3", "schema": [
            {"name": "message", "type": "string"},
            {"name": "instant", "type": "string"},
            {"name": "level", "type": "string"},
            {"name": "category", "type": "string"},
            {"name": "fiber_id", "type": "string"},
            {"name": "trace_id", "type": "string"},
            {"name": "source_file", "type": "string"},
            {"name": "host", "type": "string"},
        ]})
        sync_mount_table(TestDynamicTableLogger.DEBUG_LOG_TABLE)

        create("table", TestDynamicTableLogger.ACCESS_LOG_TABLE, attributes={"dynamic": True, "schema": [
            {"name": "instant", "type": "string", "sort_order": "ascending"},
            {"name": "user", "type": "string"},
            {"name": "method", "type": "string"},
            {"name": "type", "type": "string"},
            {"name": "id", "type": "string"},
            {"name": "path", "type": "string"},
            {"name": "original_path", "type": "string"},
            {"name": "destination_path", "type": "string"},
            {"name": "original_destination_path", "type": "string"},
            {"name": "mutation_id", "type": "string"},
            {"name": "transaction_info", "type": "any"},
            {"name": "revision_type", "type": "string"},
            {"name": "revision", "type": "string"},
            {"name": "mode", "type": "string"},
            {"name": "level", "type": "string"},
            {"name": "category", "type": "string"},
            {"name": "cluster", "type": "string"},
            {"name": "host", "type": "string"},
        ]})
        sync_mount_table(TestDynamicTableLogger.ACCESS_LOG_TABLE)

    @staticmethod
    def _find_log_event(rows, message_substrings):
        for row in rows:
            if all([substring in row["message"] for substring in message_substrings]):
                return row

        return None

    @authors("achulkov2")
    def test_debug_log(self):
        node_id = create("map_node", "//tmp/test_debug_log")
        set("//tmp/test_debug_log/@log_me", 1543)

        # Wait for flush.
        time.sleep(1)

        rows = list(select_rows(f"* from [{self.DEBUG_LOG_TABLE}]", verbose=False))

        log_event = self._find_log_event(rows, [node_id, "Node.Set /@log_me"])
        assert log_event is not None
        assert log_event["category"] == "ObjectServer"
        assert log_event["level"] == "debug"
        assert log_event["host"] == "localhost"

    @authors("achulkov2")
    def test_access_log(self):
        tx1 = start_transaction()
        tx2 = start_transaction(tx=tx1)

        table_id = create("table", "//tmp/test_access_log", tx=tx2)

        # Wait for flush.
        time.sleep(1)

        rows = list(select_rows(f"* from [{self.ACCESS_LOG_TABLE}] where path = \"//tmp/test_access_log\"", verbose=False))

        for row in rows:
            if row["method"] == "Create":
                assert row["id"] == table_id
                assert row["transaction_info"]["transaction_id"] == tx2
                assert row["transaction_info"]["parent"]["transaction_id"] == tx1
                assert row["host"] == "localhost"

    @authors("achulkov2")
    def test_rate_limits(self):
        # Logs go brrrrr.
        time.sleep(3)

        rows = list(select_rows(f"* from [{self.DEBUG_LOG_TABLE}]", verbose=False))
        log_event = self._find_log_event(rows, ["SkippedBy: Concurrency"])
        assert log_event is not None
        assert log_event["category"] == "Logging"
        assert log_event["level"] == "info"
