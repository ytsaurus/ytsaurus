from yt_env_setup import YTEnvSetup

from yt_commands import (authors, raises_yt_error, create_dynamic_table,
                         sync_mount_table, insert_rows, create_user, get, ls, make_ace, set)

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


class TestQueriesQL(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1

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

    @authors("sinister")
    @pytest.mark.parametrize("role", [None, "default", "ql"])
    def test_rpc_proxy_role(self, query_tracker, role):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "one"}])

        proxy = ls("//sys/rpc_proxies")[0]
        role_path = f"//sys/rpc_proxies/{proxy}/@role"
        original_role = get(role_path, default="default")
        set(role_path, role or "default")
        try:
            settings = {"cluster": "primary"}
            if role is not None:
                settings["rpc_proxy_role"] = role
            q = start_query("ql", "* from [//tmp/t]", settings=settings)
            q.track()
            assert_items_equal(q.read_result(0), [{"key": 1, "value": "one"}])
        finally:
            set(role_path, original_role)

    @authors("sinister")
    def test_missing_rpc_proxy_role(self, query_tracker):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "one"}])

        q = start_query("ql", "* from [//tmp/t]", settings={
            "cluster": "primary",
            "rpc_proxy_role": "missing",
        })
        with raises_yt_error("No RPC proxies found"):
            q.track()
        assert q.get_state() == "failed"

    @authors("gudqeit")
    def test_query_error(self, query_tracker):
        settings = {"cluster": "primary"}
        q = start_query("ql", "* from [//tmp/t]", settings=settings)
        with raises_yt_error("Query .* failed"):
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


##################################################################


@authors("apollo1321")
class TestQueriesQLRpcProxy(TestQueriesQL):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1


class TestQueriesQLAuthentication(TestQueriesQL):
    DELTA_RPC_PROXY_CONFIG = {
        "enable_authentication": True,
        "cypress_token_authenticator": {"secure": True},
    }

    @authors("sinister")
    def test_user_permissions(self, query_tracker):
        create_user("ql_user")
        create_user("ql_user_without_access")
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "one"}])
        set("//tmp/t/@inherit_acl", False)
        set("//tmp/t/@acl", [make_ace("allow", "ql_user", "read")])

        q = start_query("ql", "* from [//tmp/t]", settings={"cluster": "primary"}, authenticated_user="ql_user")
        q.track()
        assert_items_equal(q.read_result(0), [{"key": 1, "value": "one"}])

        q = start_query("ql", "* from [//tmp/t]", settings={"cluster": "primary"}, authenticated_user="ql_user_without_access")
        with raises_yt_error("Access denied"):
            q.track()
        assert q.get_state() == "failed"
