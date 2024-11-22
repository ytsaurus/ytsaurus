from yt_queries import start_query

from yt.environment.helpers import assert_items_equal

from yt_commands import (authors, create, create_user, sync_mount_table,
                         write_table, insert_rows, alter_table, raises_yt_error,
                         write_file, create_pool, wait, get, set, ls)

from yt_env_setup import YTEnvSetup

import yt_error_codes

import pytest


class TestQueriesYqlBase(YTEnvSetup):
    NUM_YQL_AGENTS = 1
    NUM_MASTERS = 1
    NUM_QUERY_TRACKER = 1
    ENABLE_HTTP_PROXY = True
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    def _run_simple_query(self, query, **kwargs):
        query = start_query("yql", query, **kwargs)
        query.track()
        query_info = query.get()
        if query_info["result_count"] == 0:
            return None
        elif query_info["result_count"] == 1:
            return query.read_result(0)
        else:
            return [query.read_result(i) for i in range(query_info["result_count"])]

    def _test_simple_query(self, query, expected, **kwargs):
        result = self._run_simple_query(query, **kwargs)
        if expected is None:
            assert result is None
            return

        assert_items_equal(result, expected)


class TestSimpleQueriesYql(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4
    YQL_TEST_LIBRARY = """
            $my_sqr = ($x)->($x * $x);
            export $my_sqr;
        """

    @authors("max42")
    def test_simple(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t", rows)
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

    @authors("mpereskokova")
    def test_simple_insert(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}]

        self._test_simple_query("insert into primary.`//tmp/t`(a, b) values (42, 'foo')", None)
        self._test_simple_query("select * from primary.`//tmp/t`", rows)

    @authors("max42")
    def test_issues(self, query_tracker, yql_agent):
        with raises_yt_error(30000):
            self._run_simple_query("select * from primary.`//tmp/nonexistent`")

    @authors("max42")
    def test_schemaful_read(self, query_tracker, yql_agent):
        schema = [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "boolean"},
            {"name": "d", "type": "double"},
        ]
        schema_new = [
            {"name": "b", "type": "string"},
            {"name": "d", "type": "double"},
            {"name": "c", "type": "boolean"},
            {"name": "a", "type": "int64"},
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        rows = [
            {"a": 42, "b": "foo", "c": True, "d": 3.14},
        ]
        write_table("<append=%true>//tmp/t", rows)
        alter_table("//tmp/t", schema=schema_new)

        self._test_simple_query("""
            select * from primary.`//tmp/t`;
            select c, b from primary.`//tmp/t`
        """, [rows, [{"b": "foo", "c": True}]])

    @authors("mpereskokova")
    def test_calc(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select 1;
            select 1 + 1;
            select 2 as b, 1 as a
        """, [[{"column0": 1}], [{"column0": 2}], [{"a": 1, "b": 2}]])

    @authors("a-romanov")
    def test_libs(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select core::IndexOf([3,7,1], 7) as idx, test::my_sqr(3) as sqr;
        """, [{"idx": 1, "sqr": 9}])

    @authors("a-romanov")
    def test_datetime_types(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select
                Date("2024-11-24") as `Date`,
                Datetime("2024-11-24T11:20:59Z") as `Datetime`,
                Timestamp("2024-11-24T13:42:11.666Z") as `Timestamp`,
                -Interval("P2W") as `Interval`,
                Date32("1960-11-24") as `Date32`,
                Datetime64("1950-11-24T11:20:59Z") as `Datetime64`,
                Timestamp64("1940-11-24T13:42:11.666Z") as `Timestamp64`,
                -Interval64("PT42M") as `Interval64`,
        """,
        [{
            "Date": 20051,
            "Datetime": 1732447259,
            "Timestamp": 1732455731666000,
            "Interval":  -1209600000000,
            "Date32": -3325,
            "Datetime64": -602858341,
            "Timestamp64": -918382668334000,
            "Interval64":  -2520000000,

        }])

    @authors("a-romanov")
    def test_exotic_types(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select
                {} as `EmptyDict`,
                [] as `EmptyList`,
                Null as `Null`,
                <| Signed : -13, Unsigned : 42U, Y : '{"key" = 3.14}'y |> as Struct,
                ((-13, 42U, false), "foo", true, "bar"u) as Tuple,
                [-2.5f, 3.f] as `List`,
                {"one"u: 1, "two"u: 2} as `Dict`,
                {("One"u, 1UL), ("Two"u, 2UL)} as `Set`,
                AsVariant(88L, "var") as `Variant`,
                AsTagged(123, "tag") as `Tagged`,
                '[1, "text", 3.14]'j as `Json`,
                Just('[7u; "str"; -3.14]'y) as `Yson`,
        """,
        [{
            "EmptyDict": None,
            "EmptyList": None,
            "Null": None,
            "Struct": {"Signed": -13, "Unsigned": 42, "Y": {"key": 3.14}},
            "Tuple": [[-13, 42, False], "foo", True, "bar"],
            "List": [-2.5, 3.],
            "Dict": [["two", 2], ["one", 1]],
            "Set": [[["Two", 2], None], [["One", 1], None]],
            "Variant": ["var", 88],
            "Tagged": 123,
            "Json": '[1, "text", 3.14]',
            "Yson": [7, 'str', -3.14],
        }])


class TestYqlAgentBan(TestQueriesYqlBase):
    NUM_YQL_AGENTS = 1
    YQL_AGENT_DYNAMIC_CONFIG = {"state_check_period": 2000}

    def _test_query_fails():
        query = start_query("yql", 'select 1')
        query.track()
        return False

    def _test_query_completes():
        query = start_query("yql", 'select 1')
        try:
            query.track()
        except Exception:
            return False
        return True

    @authors("mpereskokova")
    def test_yql_agent_ban_on_new_queries(self, query_tracker, yql_agent):
        address = yql_agent.yql_agent.addresses[0]
        set(f"//sys/yql_agent/instances/{address}/@banned", True)

        with raises_yt_error(yt_error_codes.Unavailable) as err:
            wait(TestYqlAgentBan._test_query_fails)
        assert err[0].contains_text("No alive peers found")

        set(f"//sys/yql_agent/instances/{address}/@banned", False)

        wait(TestYqlAgentBan._test_query_completes)

    @authors("mpereskokova")
    def test_yql_agent_ban_on_existing_queries(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        long_query = start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        wait(lambda: long_query.get()["state"] == "running")

        address = yql_agent.yql_agent.addresses[0]
        set(f"//sys/yql_agent/instances/{address}/@banned", True)

        with raises_yt_error(yt_error_codes.Unavailable) as err:
            wait(TestYqlAgentBan._test_query_fails)
        assert err[0].contains_text("No alive peers found")

        set("//sys/pools/small/@resource_limits/user_slots", 1)
        long_query.track()


class TestComplexQueriesYql(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4

    @authors("mpereskokova")
    def test_count(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": -17}]
        write_table("//tmp/t", rows)

        self._test_simple_query("select count(*) from primary.`//tmp/t`", [{"column0": 2}])

    @authors("mpereskokova")
    def test_union(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t1", rows)

        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 43}]
        write_table("//tmp/t2", rows)

        self._test_simple_query(
            "select * from `//tmp/t1` union all select * from `//tmp/t2`",
            [{"a": 42}, {"a": 43}],
        )

    @authors("mpereskokova")
    @pytest.mark.timeout(150)
    def test_complex_query(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        self._test_simple_query("select sum(1) from (select * from `//tmp/t1`)", [{"column0": 2}])

    @authors("aleksandr.gaev")
    @pytest.mark.timeout(150)
    def test_sum(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        self._test_simple_query("select sum(a) from `//tmp/t1`", [{"column0": 85}])

    @authors("aleksandr.gaev")
    def test_zeros_in_settings(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 45}]
        write_table("//tmp/t1", rows)

        self._test_simple_query("select * from `//tmp/t1`", [{"a": 45}], settings={"random_attribute": 0})


class TestExecutionModesYql(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 16

    @authors("aleksandr.gaev")
    def test_validate(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["validate", 0]:
            query = start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": mode})
            query.track()
            result = query.get()
            assert result["result_count"] == 0
            assert result["progress"]["yql_plan"]["Basic"] == {'nodes': [{'id': 1, 'level': 1, 'name': 'Commit! on primary #1', 'type': 'op'}], 'links': []}

    @authors("aleksandr.gaev")
    def test_optimize(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["optimize", 1]:
            query = start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": mode})
            query.track()
            result = query.get()
            assert result["result_count"] == 0
            assert len(result["progress"]["yql_plan"]["Basic"]["nodes"]) > 2

    @authors("aleksandr.gaev")
    def test_run(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["run", 2]:
            self._test_simple_query("select * from `//tmp/t1`", rows, settings={"execution_mode": mode})

    @authors("aleksandr.gaev")
    def test_unknown_execution_modes(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        with raises_yt_error('Error parsing'):
            self._run_simple_query("select * from `//tmp/t1`", settings={"execution_mode": "unknown"})

        with raises_yt_error("Error casting"):
            self._run_simple_query("select * from `//tmp/t1`", settings={"execution_mode": 42})


class TestYqlPlugin(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4

    @authors("mpereskokova")
    def test_default_cluster_read(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        self._test_simple_query("select * from `//tmp/t`", rows)

    @authors("mpereskokova")
    def test_dyntable_read(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}, {"name": "c", "type": "string"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/t")

        rows = [{"a": 42, "b": 43, "c": "test"}]
        insert_rows("//tmp/t", rows)

        self._test_simple_query("""
            select * from `//tmp/t`;
            select c, a from `//tmp/t`
        """, [rows, [{"a": 42, "c": "test"}]])

    @authors("mpereskokova")
    def test_pragma_refselect(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}, {"name": "c", "type": "string"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/t")

        rows = [{"a": 42, "b": 43, "c": "test"}]
        insert_rows("//tmp/t", rows)

        self._test_simple_query("""
            pragma RefSelect;

            select * from `//tmp/t`;
            select c, a from `//tmp/t`
        """, [rows, [{"a": 42, "c": "test"}]])


class TestAllYqlAgentsOverload(TestQueriesYqlBase):
    YQL_AGENT_DYNAMIC_CONFIG = {"max_simultaneous_queries": 1}
    NUM_YQL_AGENTS = 1

    @authors("mpereskokova")
    def test_yql_agent_overload(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})

        q1 = start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        wait(lambda: q1.get()["state"] == "running")

        q2 = start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        wait(lambda: q2.get()["state"] == "running")
        wait(lambda: q2.get()["state"] == "pending")

        set("//sys/pools/small/@resource_limits/user_slots", 1)

        q1.track()
        q2.track()


class TestPartialYqlAgentsOverload(TestQueriesYqlBase):
    YQL_AGENT_DYNAMIC_CONFIG = {"max_simultaneous_queries": 1}
    NUM_YQL_AGENTS = 2

    @authors("mpereskokova")
    def test_yql_agent_overload(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})

        q1 = start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')
        q2 = start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')

        wait(lambda: q1.get()["state"] == "running")
        wait(lambda: q2.get()["state"] == "running")

        set("//sys/pools/small/@resource_limits/user_slots", 1)

        q1.track()
        q2.track()


class TestYqlAgent(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4

    @authors("mpereskokova")
    def test_progress(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)

        create_pool("small", attributes={"resource_limits": {"user_slots": 0}})
        query = start_query("yql", 'pragma yt.StaticPool = "small"; select a+1 as result from primary.`//tmp/t`')

        def existsPendingStage():
            queryInfo = query.get()
            if "progress" in queryInfo and "yql_progress" in queryInfo["progress"]:
                for stage in queryInfo["progress"]["yql_progress"].values():
                    if "pending" in stage and stage["pending"] > 0 :
                        return True
            return False
        wait(existsPendingStage)

        set("//sys/pools/small/@resource_limits/user_slots", 1)

        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"result": 43}])

    @authors("mpereskokova")
    def test_files(self, query_tracker, yql_agent):
        self._test_simple_query(
            "select FileContent(\"test_file_raw\") as column",
            [{"column": "test_content"}],
            files=[{"name": "test_file_raw", "content": "test_content", "type": "raw_inline_data"}],
        )

        # check downloading files by links
        create("file", "//tmp/test_file")
        write_file("//tmp/test_file", b"test_file_content")
        file_link = "http://" + self.Env.get_http_proxy_address() + "/api/v3/read_file?path=//tmp/test_file"

        self._test_simple_query(
            "select FileContent(\"long_link\"); select FileContent(\"short_link\")",
            [[{"column0": "test_file_content"}], [{"column0": "test_file_content"}]],
            files=[
                {"name": "long_link", "content": file_link, "type": "url"},
                {"name": "short_link", "content": "yt://primary/tmp/test_file", "type": "url"},
            ],
        )

    @authors("apollo1321")
    def test_config_defaults(self, query_tracker, yql_agent):
        instances = ls("//sys/yql_agent/instances")
        for instance in instances:
            config = get("//sys/yql_agent/instances/" + instance + "/orchid/config")

            gateway_config = config["yql_agent"]["gateway_config"]
            assert len(gateway_config["remote_file_patterns"]) == 1
            assert gateway_config["remote_file_patterns"][0]["pattern"] == "yt://([a-zA-Z0-9\\-_]+)/([^&@?]+)$"
            assert gateway_config["yt_log_level"] == "YL_DEBUG"
            assert not gateway_config["execute_udf_locally_if_possible"]
            assert len(gateway_config["cluster_mapping"]) == 1
            assert len(gateway_config["cluster_mapping"][0]["settings"]) == 2
            assert len(gateway_config["default_settings"]) == 58

            setting_found = False
            for setting in gateway_config["default_settings"]:
                if setting["name"] == "DefaultCalcMemoryLimit":
                    assert setting["value"] == "2G"
                    setting_found = True
            assert setting_found

            file_storage_config = config["yql_agent"]["file_storage_config"]
            assert file_storage_config["retry_count"] == 3
            assert file_storage_config["max_files"] == 1 << 13
            # The default should be overwritten from 1 << 14.
            assert file_storage_config["max_size_mb"] == 1 << 13


class TestQueriesYqlLimitedResult(TestQueriesYqlBase):
    QUERY_TRACKER_DYNAMIC_CONFIG = {"yql_engine": {"row_count_limit": 1}}

    @authors("mpereskokova")
    def test_rows_limit(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "sort_order": "ascending", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}, {"a": 44}]
        write_table("//tmp/t1", rows)

        query = start_query("yql", "select * from `//tmp/t1`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 42}])
        assert query.get_result(0)["is_truncated"]

        query = start_query("yql", "select * from `//tmp/t1` limit 1")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 42}])
        assert not query.get_result(0)["is_truncated"]


class TestQueriesYqlAuth(TestQueriesYqlBase):
    DELTA_PROXY_CONFIG = {
        "auth" : {"enable_authentication": True}
    }

    def setup_method(self, method):
        super().setup_method(method)
        create_user("denied_user")
        create_user("allowed_user")

        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}],
            "acl": [
                {"action": "deny", "subjects": ["denied_user"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["allowed_user"], "permissions": ["read"]},
            ]
        })
        write_table("//tmp/t", [{"a": 42}])

    @authors("mpereskokova")
    def test_yql_agent_impersonation_deny(self, query_tracker, yql_agent):
        with raises_yt_error("failed"):
            self._run_simple_query("select a + 1 as b from primary.`//tmp/t`;", authenticated_user="denied_user")

    @authors("mpereskokova")
    def test_yql_agent_impersonation_allow(self, query_tracker, yql_agent):
        self._test_simple_query("select a + 1 as b from primary.`//tmp/t`;", [{"b": 43}], authenticated_user="allowed_user")


class TestYqlColumnOrderAggregateWithAs(TestQueriesYqlBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.timeout(300)
    def test_aggregate_with_as(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        write_table("//tmp/t", [{"a": 42}])

        self._test_simple_query("""
            select
                sum(a) as c_a,
                sum(a) + 1 as c_b,
                sum(a) + 2 as c_c,
                sum(a) + 3 as c_d,
            from primary.`//tmp/t`
        """, [{"c_a": 42, "c_b": 43, "c_c": 44, "c_d": 45}])

        self._test_simple_query("""
            select
                sum(a) as c_d,
                sum(a) + 1 as c_c,
                sum(a) + 2 as c_b,
                sum(a) + 3 as c_a,
            from primary.`//tmp/t`
        """, [{"c_a": 45, "c_b": 44, "c_c": 43, "c_d": 42}])

        self._test_simple_query("""
            select
                sum(a) as c_d,
                sum(a) + 1 as c_b,
                sum(a) + 2 as c_c,
                sum(a) + 3 as c_a,
            from primary.`//tmp/t`
        """, [{"c_a": 45, "c_b": 43, "c_c": 44, "c_d": 42}])

        self._test_simple_query("""
            select
                sum(a) as c_c,
                sum(a) + 1 as c_a,
                sum(a) + 2 as c_d,
                sum(a) + 3 as c_b,
            from primary.`//tmp/t`
        """, [{"c_a": 43, "c_b": 45, "c_c": 42, "c_d": 44}])


class TestYqlColumnOrderIssue707(TestQueriesYqlBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.timeout(300)
    def test_issue_707(self, query_tracker, yql_agent):
        # https://github.com/ytsaurus/ytsaurus/issues/707
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}]
        })
        write_table("//tmp/t", [{"a": 42, "b": "foo", "c": 2.0}])

        self._test_simple_query("""
            select
                sum(a) as x,
                count(*) as y,
                sum(c) as z
            from primary.`//tmp/t`
        """, [{"x": 42, "y": 1, "z": 2.0}])
        self._test_simple_query("""
            select
                sum(a) as z,
                count(*) as x,
                sum(c) as y
            from primary.`//tmp/t`
        """, [{"x": 1, "y": 2.0, "z": 42}])


class TestYqlColumnOrderParametrize(TestQueriesYqlBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.parametrize("dynamic", [False, True])
    @pytest.mark.timeout(300)
    def test_select_table(self, query_tracker, yql_agent, dynamic):
        create("table", "//tmp/t", attributes={
            "schema": [
                {"name": "a", "type": "int64"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "float"},
            ],
            "dynamic": dynamic,
            "enable_dynamic_store_read": True,
        })
        if dynamic:
            sync_mount_table("//tmp/t")
            insert_rows("//tmp/t", [{"a": 42, "b": "foo", "c": 2.0}])
        else:
            write_table("//tmp/t", [{"a": 42, "b": "foo", "c": 2.0}])

        self._test_simple_query("""
            select *,
            from primary.`//tmp/t`
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select a, b, c,
            from primary.`//tmp/t`
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select c, a, b
            from primary.`//tmp/t`
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select a as b, b as c, c as a,
            from primary.`//tmp/t`
        """, [{"a": 2.0, "b": 42, "c": "foo"}])
        self._test_simple_query("""
            select c as b, b as a, a as c,
            from primary.`//tmp/t`
        """, [{"a": "foo", "b": 2.0, "c": 42}])

        # Multiple outputs
        self._test_simple_query("""
            select a, b, c
            from primary.`//tmp/t`;
            select a, b, c
            from primary.`//tmp/t`;
        """, [[{"a": 42, "b": "foo", "c": 2.0}], [{"a": 42, "b": "foo", "c": 2.0}]])
        self._test_simple_query("""
            select a, b, c
            from primary.`//tmp/t`;
            select c, a, b
            from primary.`//tmp/t`;
        """, [[{"a": 42, "b": "foo", "c": 2.0}], [{"a": 42, "b": "foo", "c": 2.0}]])


class TestYqlColumnOrderSelectScalars(TestQueriesYqlBase):
    @authors("gritukan", "mpereskokova")
    def test_select_scalars(self, query_tracker, yql_agent):
        self._test_simple_query("""
            select 42 as a, "foo" as b, 2.0 as c
        """, [{"a": 42, "b": "foo", "c": 2.0}])
        self._test_simple_query("""
            select 42 as c, 43 as a, 44 as b
        """, [{"a": 43, "b": 44, "c": 42}])
        self._test_simple_query("""
            select 42 as c, "foo" as a, 2.0 as b
        """, [{"a": "foo", "b": 2.0, "c": 42}])

        # Error cases
        with raises_yt_error("duplicate column names"):
            self._run_simple_query("select 42 as a, 42 as a")
        with raises_yt_error("duplicate column names"):
            self._run_simple_query("select 42 as a, 43 as a")
        with raises_yt_error("duplicate column names"):
            self._run_simple_query("select 42 as a, \"foo\" as a")

        # Multiple outputs
        self._test_simple_query("""
            select 42 as a, "foo" as b, 2.0 as c;
            select 42 as c, "foo" as a, 2.0 as b;
        """, [[{"a": 42, "b": "foo", "c": 2.0}], [{"a": "foo", "b": 2.0, "c": 42}]])


class TestYqlColumnOrderDifferentSources(TestQueriesYqlBase):
    @authors("gritukan", "mpereskokova")
    @pytest.mark.timeout(300)
    def test_different_sources(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
        })
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"a": 42, "b": "foo", "c": 2.0}])

        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}, {"name": "c", "type": "float"}],
            "enable_dynamic_store_read": True,
        })
        write_table("//tmp/t2", [{"a": 43, "b": "bar", "c": 3.0}])

        self._test_simple_query("""
            select a, b, c from primary.`//tmp/t2`
            union all
            select a, b, c from primary.`//tmp/t1`
        """, [{"a": 42, "b": "foo", "c": 2.0}, {"a": 43, "b": "bar", "c": 3.0}])
        self._test_simple_query("""
            select c, a, b from primary.`//tmp/t2`
            union all
            select c, a, b from primary.`//tmp/t1`
        """, [{"a": 42, "b": "foo", "c": 2.0}, {"a": 43, "b": "bar", "c": 3.0}])
