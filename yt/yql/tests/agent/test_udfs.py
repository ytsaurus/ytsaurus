from yt_queries import start_query

from yt.environment.helpers import assert_items_equal, wait_for_dynamic_config_update

from yt_commands import (authors, create, write_table, read_table, get, set)

from yt_env_setup import YTEnvSetup


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


class TestUdfs(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4

    @authors("max42")
    def test_simple_udf(self, query_tracker, yql_agent):
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

    @authors("max42")
    def test_folder(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
        })
        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/t", rows)
        query = start_query("yql", 'pragma yt.FolderInlineItemsLimit="0"; insert into `//tmp/output` select * from FOLDER("//tmp", "row_count")')
        query.track()
        result = read_table("//tmp/output")
        expected_result = [
            {
                "Path": "tmp/t",
                "Type": "table",
                "Attributes": {"row_count": 2},
            }
        ]
        assert_items_equal(result, expected_result)

    @authors("max42")
    def test_rename_members(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        expected_rows = [{"b": 42}]
        write_table("//tmp/t", rows)
        query = start_query("yql", "select * from (select RenameMembers(TableRow(), [('a', 'b')]) from `//tmp/t`) flatten columns;")
        query.track()
        result = query.read_result(0)
        assert_items_equal(expected_rows, result)

    @authors("lucius")
    def test_simple_python_udf(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "Int32"}]})
        write_table("//tmp/t", [
            {"a": -1},
            {"a": 0},
            {"a": 1},
            {"a": 2},
        ])
        yql_with_python = """
$python_code = @@
def f(x):
    return x > 0
@@;
$f = Python3::f(Callable<(Int32)->Bool>, $python_code);
select a from primary.`//tmp/t` where $f(unwrap(a));
"""
        query = start_query("yql", yql_with_python)
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 1}, {"a": 2}])

    @authors("a-romanov")
    def test_secure_param(self, query_tracker, yql_agent):
        yql_with_python = """
$get_secure_param = Python3::get_secure_param(
    Callable<(String)->Text>,
    @@#py
def get_secure_param(key):
    return get_secure_param._yql_secure_param(key)[0:5]
    @@
);

select $get_secure_param(SecureParam("token:default_yt")) as sp;
"""
        query = start_query("yql", yql_with_python)
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"sp": "ytct-"}])


class TestUdfsWithDynamicConfig(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4

    def _update_dyn_config(self, yql_agent, dyn_config):
        config = get("//sys/yql_agent/config")
        config["yql_agent"] = dyn_config
        set("//sys/yql_agent/config", config)
        wait_for_dynamic_config_update(yql_agent.yql_agent.client, config, "//sys/yql_agent/instances")

    @authors("lucius")
    def test_simple_udf_dyn_config(self, query_tracker, yql_agent):
        self._update_dyn_config(yql_agent, {
            "gateways_config": {
                "yt": {
                    "cluster_mapping": [
                    ],
                },
            },
        })
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

    @authors("lucius")
    def test_simple_python_udf_dyn_config(self, query_tracker, yql_agent):
        self._update_dyn_config(yql_agent, {
            "gateways_config": {
                "yt": {
                    "cluster_mapping": [
                    ],
                },
            },
        })
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "Int32"}]})
        write_table("//tmp/t", [
            {"a": -1},
            {"a": 0},
            {"a": 1},
            {"a": 2},
        ])
        yql_with_python = """
$python_code = @@
def f(x):
    return x > 0
@@;
$f = Python3::f(Callable<(Int32)->Bool>, $python_code);
select a from primary.`//tmp/t` where $f(unwrap(a));
"""
        query = start_query("yql", yql_with_python)
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 1}, {"a": 2}])
