from yt_env_setup import YTEnvSetup

from yt_commands import (authors, raises_yt_error, create_dynamic_table,
                         sync_mount_table, insert_rows, sync_create_cells)

from yt_queries import start_query

from yt_type_helpers import (
    decimal_type,
    make_column,
    make_sorted_column,
    optional_type,
    list_type,
)

from yt.test_helpers import assert_items_equal

from decimal_helpers import encode_decimal

from yt.wrapper import yson

import pytest


@pytest.mark.enabled_multidaemon
class TestQueriesQL(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    @staticmethod
    def _create_simple_dynamic_table(path, sort_order="ascending", **attributes):
        attributes["dynamic_store_auto_flush_period"] = yson.YsonEntity()
        if "schema" not in attributes:
            attributes["schema"] = [
                make_sorted_column("key", optional_type("int64")),
                make_column("value", optional_type("string")),
            ]
        create_dynamic_table(path, **attributes)

    @staticmethod
    def _assert_select_result(path, settings, rows, is_truncated):
        q = start_query("ql", f"* from [{path}]", settings=settings)
        q.track()
        assert q.get()["result_count"] == 1
        assert_items_equal(q.read_result(0), rows)
        assert q.get_result(0)["is_truncated"] == yson.YsonBoolean(is_truncated)

    @authors("gudqeit", "sabdenovch")
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

        settings["allow_full_scan"] = False
        q = start_query("ql", "* from [//tmp/t]", settings=settings)
        with raises_yt_error():
            q.track()

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

    @authors("aleksandr.gaev")
    @pytest.mark.timeout(300)
    def test_big_result(self, query_tracker):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")

        value_size = 1024 * 1024
        settings = {"cluster": "primary"}

        # 14 MB
        rows = [{"key": i, "value": ''.join(['a' for _ in range(value_size)])} for i in range(14)]
        insert_rows("//tmp/t", rows)
        self._assert_select_result("//tmp/t", settings, rows, False)

        # 15 MB
        new_rows = [{"key": i, "value": ''.join(['b' for _ in range(value_size)])} for i in range(14, 15)]
        insert_rows("//tmp/t", new_rows)
        rows += new_rows
        self._assert_select_result("//tmp/t", settings, rows, False)

        # 16 MB
        new_rows = [{"key": i, "value": ''.join(['c' for _ in range(value_size)])} for i in range(15, 16)]
        insert_rows("//tmp/t", new_rows)
        rows += new_rows
        self._assert_select_result("//tmp/t", settings, rows[:15], True)

        # 17 MB
        new_rows = [{"key": i, "value": ''.join(['d' for _ in range(value_size)])} for i in range(16, 17)]
        insert_rows("//tmp/t", new_rows)
        rows += new_rows
        self._assert_select_result("//tmp/t", settings, rows[:15], True)

    @authors("sabdenovch")
    def test_supports_syntax_and_expression_builder_versions(self, query_tracker):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True, schema=[
            make_sorted_column("key", optional_type("int64")),
            make_column("list_v", list_type("int64"))

        ])
        sync_mount_table("//tmp/t")
        rows = [{"key": i, "list_v": [i * j for j in range(3)]} for i in range(2)]
        insert_rows("//tmp/t", rows)

        settings = {"cluster": "primary", "syntax_version": 2, "expression_builder_version": 2}
        q = start_query("ql", "select (T.list_v[1] + 2u) as p from `//tmp/t` as T", settings=settings)
        q.track()
        assert_items_equal(q.read_result(0), [{"p": p + 2} for p in range(2)])

    @authors("abatovkin")
    def test_hyperloglog_functions(self, query_tracker):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]
        self._create_simple_dynamic_table("//tmp/t", schema=schema, enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")

        # 100 distinct values
        rows = [{"key": i, "value": i} for i in range(100)]
        insert_rows("//tmp/t", rows)

        settings = {"cluster": "primary"}

        q = start_query("ql", "SELECT hll_14(value) as cardinality FROM [//tmp/t] GROUP BY 1", settings=settings)
        q.track()
        info = q.get()
        assert info["result_count"] == 1
        result = q.read_result(0)
        assert len(result) == 1
        estimate = result[0]["cardinality"]
        # should be around 100, with some error tolerance (HLL standard error ~0.65% for p=14)
        assert abs(estimate - 100) < 20, f"Cardinality estimate {estimate} too far from 100"

        q7 = start_query("ql", "SELECT hll_7(value) as cardinality FROM [//tmp/t] GROUP BY 1", settings=settings)
        q7.track()
        result7 = q7.read_result(0)
        estimate7 = result7[0]["cardinality"]
        # tolerance larger for lower precision
        assert abs(estimate7 - 100) < 30, f"hll_7 estimate {estimate7} too far from 100"

        q_gen = start_query("ql", "SELECT cardinality(value) as cardinality FROM [//tmp/t] GROUP BY 1", settings=settings)
        q_gen.track()
        result_gen = q_gen.read_result(0)
        estimate_gen = result_gen[0]["cardinality"]
        assert abs(estimate_gen - 100) < 20

        # Test hll_14_state with grouping
        q2 = start_query("ql", """
                               SELECT
                                   key % 2 as group,
                                   hll_14_state(value) as state
                               FROM [//tmp/t]
                               GROUP BY key % 2
                               ORDER BY group
                               """, settings=settings)
        q2.track()
        result2 = q2.read_result(0)
        assert len(result2) == 2
        # Each group has ~50 distinct values
        for row in result2:
            # state is a binary string, ensure it's not empty
            state = row["state"]
            assert isinstance(state, bytes) or isinstance(state, str)
            assert len(state) > 0

        # Test hll_14_merge_state across groups (merge the two group states)
        # First, create a subquery that returns both states, then merge them
        q3 = start_query("ql", """
                               SELECT hll_14_merge_state(state) as merged_state
                               FROM (
                                        SELECT hll_14_state(value) as state
                                        FROM [//tmp/t]
                                        WHERE key % 2 = 0
                                        GROUP BY 1
                                        UNION ALL
                                        SELECT hll_14_state(value) as state
                                        FROM [//tmp/t]
                                        WHERE key % 2 = 1
                                        GROUP BY 1
                                    )
                               GROUP BY 1
                               """, settings=settings)
        q3.track()
        result3 = q3.read_result(0)
        merged_state = result3[0]["merged_state"]
        # Merged state should be a binary string
        assert isinstance(merged_state, bytes) or isinstance(merged_state, str)
        # Estimate cardinality from merged state using hll_14_merge
        q4 = start_query("ql", """
                               SELECT hll_14_merge(state) as merged_cardinality
                               FROM (
                                        SELECT hll_14_state(value) as state
                                        FROM [//tmp/t]
                                        WHERE key % 2 = 0
                                        GROUP BY 1
                                        UNION ALL
                                        SELECT hll_14_state(value) as state
                                        FROM [//tmp/t]
                                        WHERE key % 2 = 1
                                        GROUP BY 1
                                    )
                               GROUP BY 1
                               """, settings=settings)
        q4.track()
        result4 = q4.read_result(0)
        merged_estimate = result4[0]["merged_cardinality"]
        assert abs(merged_estimate - 100) < 20

        # Test cardinality_merge function (generic merge)
        q5 = start_query("ql", """
                               SELECT cardinality_merge(state) as total
                               FROM (
                                        SELECT hll_14_state(value) as state
                                        FROM [//tmp/t]
                                        WHERE key < 50
                                        GROUP BY 1
                                        UNION ALL
                                        SELECT hll_14_state(value) as state
                                        FROM [//tmp/t]
                                        WHERE key >= 50
                                        GROUP BY 1
                                    )
                               GROUP BY 1
                               """, settings=settings)
        q5.track()
        result5 = q5.read_result(0)
        total = result5[0]["total"]
        assert abs(total - 100) < 20

        # Test with duplicate values (should not increase cardinality)
        # Insert same 100 values again
        insert_rows("//tmp/t", rows)
        q6 = start_query("ql", "SELECT hll_14(value) as cardinality FROM [//tmp/t] GROUP BY 1", settings=settings)
        q6.track()
        result6 = q6.read_result(0)
        estimate_dup = result6[0]["cardinality"]
        # Should still be ~100, not 200
        assert abs(estimate_dup - 100) < 20, f"Duplicate values changed cardinality to {estimate_dup}"

        # Test with string values
        schema_str = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        self._create_simple_dynamic_table("//tmp/t_str", schema=schema_str, enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t_str")
        rows_str = [{"key": i, "value": f"value_{i}"} for i in range(50)]
        insert_rows("//tmp/t_str", rows_str)
        q7 = start_query("ql", "SELECT hll_14(value) as cardinality FROM [//tmp/t_str] GROUP BY 1", settings=settings)
        q7.track()
        result7 = q7.read_result(0)
        estimate_str = result7[0]["cardinality"]
        assert abs(estimate_str - 50) < 15


##################################################################


@authors("apollo1321")
@pytest.mark.enabled_multidaemon
class TestQueriesQLRpcProxy(TestQueriesQL):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
