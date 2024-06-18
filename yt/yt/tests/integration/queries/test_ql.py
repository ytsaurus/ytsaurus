from yt_env_setup import YTEnvSetup

from yt_commands import (authors, raises_yt_error, create_dynamic_table,
                         sync_mount_table, insert_rows)

from yt_queries import start_query

from yt_type_helpers import decimal_type

from yt.test_helpers import assert_items_equal

from decimal_helpers import encode_decimal

from yt.wrapper import yson


class TestQueriesQL(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @staticmethod
    def _create_simple_dynamic_table(path, sort_order="ascending", **attributes):
        attributes["dynamic_store_auto_flush_period"] = yson.YsonEntity()
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    @authors("gudqeit")
    def test_simple_query(self, query_tracker):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i)} for i in range(2)]
        insert_rows("//tmp/t", rows)

        settings = {"cluster": "primary"}
        q = start_query("ql", "* from [//tmp/t]", settings=settings)
        q.track()

        info = q.get()
        assert info["result_count"] == 1
        assert_items_equal(q.read_result(0), rows)

    @authors("gudqeit")
    def test_query_error(self, query_tracker):
        settings = {"cluster": "primary"}
        q = start_query("ql", "* from [//tmp/t]", settings=settings)
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

    @authors("gudqeit")
    def test_types(self, query_tracker):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "str", "type": "string"},
            {"name": "list", "type_v3": {"type_name": "list", "item": "int32"}},
            {"name": "decimal32", "type_v3": decimal_type(9, 2)},
        ]
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True, schema=schema)
        sync_mount_table("//tmp/t")
        rows = [
            {
                "key": 0,
                "list": [1, 2, 3],
                "decimal32": encode_decimal("1.1", 9, 2),
                "str": "some_str",
            },
            {
                "key": 1,
                "list": [],
                "decimal32": encode_decimal("1.1", 9, 2),
                "str": None,
            }
        ]
        insert_rows("//tmp/t", rows)

        settings = {"cluster": "primary"}
        q = start_query("ql", "* from [//tmp/t]", settings=settings)
        q.track()

        query_info = q.get()
        assert query_info["result_count"] == 1

        result_info = q.get_result(0)
        for column in result_info["schema"]:
            del column["type_v3"]
            del column["required"]
        result_info["schema"] = list(result_info["schema"])
        expected_schema = [
            {'name': 'key', 'type': 'int64'},
            {'name': 'str', 'type': 'string'},
            {'name': 'list', 'type': 'any'},
            {'name': 'decimal32', 'type': 'string'},
        ]
        assert result_info["schema"] == expected_schema
        assert_items_equal(q.read_result(0), rows)


##################################################################


@authors("apollo1321")
class TestQueriesQLRpcProxy(TestQueriesQL):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
