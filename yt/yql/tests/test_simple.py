from yt_queries import start_query

from yt.environment.helpers import assert_items_equal

from yt_commands import (authors, create, create_user, sync_mount_table,
                         write_table, insert_rows, alter_table, raises_yt_error,
                         write_file, create_pool, wait, get, set, ls)

from yt_env_setup import YTEnvSetup


class TestQueriesYqlBase(YTEnvSetup):
    NUM_YQL_AGENTS = 1
    NUM_QUERY_TRACKER = 1
    ENABLE_HTTP_PROXY = True
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }


class TestSimpleQueriesYql(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 16

    @authors("max42")
    def test_simple(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t", rows)
        query = start_query("yql", "select * from primary.`//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(rows, result)

    @authors("mpereskokova")
    def test_simple_insert(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}]

        query = start_query("yql", "insert into primary.`//tmp/t`(a, b) values (42, 'foo')")
        query.track()

        query_info = query.get()
        assert query_info["result_count"] == 0

        query = start_query("yql", "select * from primary.`//tmp/t`")
        query.track()

        result = query.read_result(0)
        assert_items_equal(result, rows)

    @authors("max42")
    def test_issues(self, query_tracker, yql_agent):
        query = start_query("yql", "select * from primary.`//tmp/nonexistent`")
        with raises_yt_error(30000):
            query.track()

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

        query = start_query("yql", "select * from primary.`//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, rows)

        query = start_query("yql", "select c, b from primary.`//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"b": "foo", "c": True}])

    @authors("mpereskokova")
    def test_calc(self, query_tracker, yql_agent):
        query = start_query("yql", "select 1")
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"column0": 1}], result)

        query = start_query("yql", "select 1 + 1")
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"column0": 2}], result)

        query = start_query("yql", "select 2 as b, 1 as a")
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"a": 1, "b": 2}], result)

    @authors("mpereskokova")
    def test_multiple_queries(self, query_tracker, yql_agent):
        query = start_query("yql", "select 1; select 2")
        query.track()

        result1 = query.read_result(0)
        assert_items_equal([{"column0": 1}], result1)
        result2 = query.read_result(1)
        assert_items_equal([{"column0": 2}], result2)

        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}]
        write_table("//tmp/t", rows)

        query = start_query("yql", "select a from `//tmp/t`; select b from `//tmp/t`; select 1;")
        query.track()

        result1 = query.read_result(0)
        assert_items_equal([{"a": 42}], result1)
        result2 = query.read_result(1)
        assert_items_equal([{"b": "foo"}], result2)
        result3 = query.read_result(2)
        assert_items_equal([{"column0": 1}], result3)

    @authors("mpereskokova")
    def test_count(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": -17}]
        write_table("//tmp/t", rows)

        query = start_query("yql", "select count(*) from `//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"column0": 2}], result)

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

        query = start_query("yql", "select * from `//tmp/t1` union all select * from `//tmp/t2`")
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"a": 42}, {"a": 43}], result)

    @authors("mpereskokova")
    def test_complex_query(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        query = start_query("yql", "select sum(1) from (select * from `//tmp/t1`)")
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"column0": 2}], result)

    @authors("aleksandr.gaev")
    def test_sum(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        query = start_query("yql", "select sum(a) from `//tmp/t1`")
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"column0": 85}], result)

    @authors("aleksandr.gaev")
    def test_zeros_in_settings(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 45}]
        write_table("//tmp/t1", rows)

        query = start_query("yql", "select * from `//tmp/t1`", settings={"random_attribute": 0})
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"a": 45}], result)


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
            query = start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": mode})
            query.track()
            query_info = query.get()
            assert query_info["result_count"] == 1
            query_result = query.read_result(0)
            assert_items_equal(rows, query_result)

    @authors("aleksandr.gaev")
    def test_unknown_execution_modes(self, query_tracker, yql_agent):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}, {"a": 43}]
        write_table("//tmp/t1", rows)

        for mode in ["Validate", "Optimize", "Run"]:
            query = start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": mode})
            with raises_yt_error('Enum value "' + str(mode) + '" is neither in a proper underscore case nor in a format'):
                query.track()

        query = start_query("yql", "select * from `//tmp/t1`", settings={"execution_mode": 42})
        with raises_yt_error('Invalid value 42 of enum type EExecuteMode'):
            query.track()


class TestYqlAgent(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 16

    @authors("max42")
    def test_udf(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [
            {"a": "there is"},
            {"a": "a meow"},
            {"a": "in a word"},
            {"a": "homeowner"},
        ])
        query = start_query("yql", 'select a from primary.`//tmp/t` where a like "%meow%"')
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": "a meow"}, {"a": "homeowner"}])

    @authors("mpereskokova")
    def test_default_cluster_read(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        write_table("//tmp/t", rows)
        query = start_query("yql", "select * from `//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(rows, result)

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

        query = start_query("yql", "select * from `//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, rows)

        query = start_query("yql", "select c, a from `//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 42, "c": "test"}])

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
        query = start_query("yql", "select FileContent(\"test_file_raw\") as column;", files=[{"name" : "test_file_raw", "content" : "test_content", "type" : "raw_inline_data"}])
        query.track()
        result = query.read_result(0)
        assert_items_equal([{"column": "test_content"}], result)

        # check downloading files by links
        create("file", "//tmp/test_file")
        write_file("//tmp/test_file", b"test_file_content")
        file_link = "http://" + self.Env.get_http_proxy_address() + "/api/v3/read_file?path=//tmp/test_file"

        query = start_query("yql", "select FileContent(\"long_link\"); select FileContent(\"short_link\");", files=[
            {"name" : "long_link", "content" : file_link, "type" : "url"},
            {"name" : "short_link", "content" : "yt://primary/tmp/test_file", "type" : "url"}])
        query.track()

        long_link_result = query.read_result(0)
        short_link_result = query.read_result(1)

        result = [{"column0": "test_file_content"}]
        assert_items_equal(result, long_link_result)
        assert_items_equal(result, short_link_result)

    @authors("apollo1321")
    def test_config_defaults(self, query_tracker, yql_agent):
        instances = ls("//sys/yql_agent/instances")
        for instance in instances:
            config = get("//sys/yql_agent/instances/" + instance + "/orchid/config")

            gateway_config = config["yql_agent"]["gateway_config"]
            assert len(gateway_config["remote_file_patterns"]) == 1
            assert gateway_config["remote_file_patterns"][0]["pattern"] == "yt://([a-zA-Z0-9\\-_]+)/([^&@?]+)$"
            assert gateway_config["yt_log_level"] == "YL_DEBUG"
            assert gateway_config["execute_udf_locally_if_possible"]
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

        query = start_query("yql", "pragma RefSelect; select * from `//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, rows)

        query = start_query("yql", "pragma RefSelect; select c, a from `//tmp/t`")
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 42, "c": "test"}])


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

    @authors("mpereskokova")
    def test_yql_agent_impersonation(self, query_tracker, yql_agent):
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

        q = start_query("yql", "select a + 1 as b from primary.`//tmp/t`;", authenticated_user="denied_user")
        with raises_yt_error("failed"):
            q.track()
        assert q.get_state() == "failed"

        q = start_query("yql", "select a + 1 as b from primary.`//tmp/t`;", authenticated_user="allowed_user")
        q.track()
        result = q.read_result(0)
        assert_items_equal([{"b": 43}], result)
