from common import TestQueriesYqlBase

from yt.environment.helpers import assert_items_equal, wait_for_dynamic_config_update

from yt_commands import (authors, create, write_table, read_table, get, set)

import pytest


class TestUdfs(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4

    @authors("max42")
    @pytest.mark.timeout(120)
    def test_simple_udf(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [
            {"a": "there is"},
            {"a": "a meow"},
            {"a": "in a word"},
            {"a": "homeowner"},
        ])
        query = self.start_query("yql", 'select a from primary.`//tmp/t` where a like "%meow%"')
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": "a meow"}, {"a": "homeowner"}])

    @authors("max42")
    @pytest.mark.timeout(120)
    def test_folder(self, query_tracker, yql_agent):
        create(
            "table", "//tmp/folder/t",
            attributes={
                "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]
            },
            recursive=True
        )

        rows = [{"a": 42, "b": "foo"}, {"a": -17, "b": "bar"}]
        write_table("//tmp/folder/t", rows)
        query = self.start_query("yql", 'pragma yt.FolderInlineItemsLimit="0"; insert into `//tmp/output` select * from FOLDER("//tmp/folder", "row_count")')
        query.track()
        result = read_table("//tmp/output")
        expected_result = [
            {
                "Path": "tmp/folder/t",
                "Type": "table",
                "Attributes": {"row_count": 2},
            }
        ]
        assert_items_equal(result, expected_result)

    @authors("max42")
    @pytest.mark.timeout(120)
    def test_rename_members(self, query_tracker, yql_agent):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        rows = [{"a": 42}]
        expected_rows = [{"b": 42}]
        write_table("//tmp/t", rows)
        query = self.start_query("yql", "select * from (select RenameMembers(TableRow(), [('a', 'b')]) from `//tmp/t`) flatten columns;")
        query.track()
        result = query.read_result(0)
        assert_items_equal(expected_rows, result)


class TestPythonUdf(TestQueriesYqlBase):
    @authors("lucius")
    @pytest.mark.timeout(300)
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
        query = self.start_query("yql", yql_with_python)
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 1}, {"a": 2}])


class TestSecureParam(TestQueriesYqlBase):
    @authors("a-romanov")
    def test_secure_param(self, query_tracker, yql_agent):
        yql_with_python = """
$get_secure_param = Python3::get_secure_param(
    Callable<(Bytes)->Text>,
    @@#py
def get_secure_param(key):
    return get_secure_param._yql_secure_param(key)[0:5]
    @@
);

select $get_secure_param(SecureParam("token:default_yt")) as sp;
"""
        query = self.start_query("yql", yql_with_python)
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"sp": "ytct-"}])

    @authors("a-romanov")
    def test_custom_secret(self, query_tracker, yql_agent):
        yql_with_python = """
$get_secure_param = Python3::get_secure_param(
    Callable<(Bytes)->Text>,
    @@#py
def get_secure_param(key):
    return get_secure_param._yql_secure_param(key)
    @@
);

select $get_secure_param(SecureParam("token:geheim1")) as sp1, $get_secure_param(SecureParam("token:geheim2")) as sp2;
"""
        path = "//tmp/secret_path_to_secret_value"
        set(path, "test")
        query = self.start_query("yql", yql_with_python, secrets=[{"id": "geheim1", "ypath": path}, {"id": "geheim2", "ypath": f"primary:{path}"}])
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"sp1": "test", "sp2": "test"}])


class TestUdfsWithDynamicConfig(TestQueriesYqlBase):
    NUM_TEST_PARTITIONS = 4

    def _update_dyn_config(self, yql_agent, dyn_config):
        config = get("//sys/yql_agent/config")
        config["yql_agent"] = dyn_config
        set("//sys/yql_agent/config", config)
        wait_for_dynamic_config_update(yql_agent.yql_agent.client, config, "//sys/yql_agent/instances")

    @authors("lucius")
    @pytest.mark.timeout(300)
    def test_simple_udf_dyn_config(self, query_tracker, yql_agent):
        self._update_dyn_config(yql_agent, {
            "gateways": {
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
        query = self.start_query("yql", 'select a from primary.`//tmp/t` where a like "%meow%"')
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": "a meow"}, {"a": "homeowner"}])

    @authors("lucius")
    @pytest.mark.timeout(300)
    def test_simple_python_udf_dyn_config(self, query_tracker, yql_agent):
        self._update_dyn_config(yql_agent, {
            "gateways": {
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
        query = self.start_query("yql", yql_with_python)
        query.track()
        result = query.read_result(0)
        assert_items_equal(result, [{"a": 1}, {"a": 2}])


class TestUdfsWithDynamicConfigWithProcesses(TestUdfsWithDynamicConfig):
    YQL_SUBPROCESSES_COUNT = 8


class TestUdfsWithProcesses(TestUdfs):
    YQL_SUBPROCESSES_COUNT = 8
