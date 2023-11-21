from yt_queries import start_query

from yt.environment.helpers import assert_items_equal

from yt_commands import (authors, create, write_table, read_table)

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


class TestUdfs(TestQueriesYqlBase):
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
