from yt_commands import (authors, create_access_control_object_namespace,
                         create_access_control_object, create, write_table,
                         raises_yt_error)

from yt.test_helpers import assert_items_equal

from yt_type_helpers import decimal_type

from decimal_helpers import encode_decimal

from queries.environment import start_query

from base import ClickHouseTestBase, Clique

from yt.wrapper import yson


class TestQueriesChyt(ClickHouseTestBase):
    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    CONFIG_PATCH = {
        "yt": {
            "discovery": {
                "version": 2
            },
        }
    }

    def setup_method(self, method):
        super().setup_method(method)
        create_access_control_object_namespace(name="chyt")
        create_access_control_object(name="ch_alias", namespace="chyt")

    @authors("gudqeit")
    def test_simple_query(self, query_tracker):
        with Clique(1, config_patch=TestQueriesChyt.CONFIG_PATCH, alias="*ch_alias"):
            settings = {"clique": "ch_alias"}
            query = start_query("chyt", "select 1", settings=settings)
            query.track()

            query_info = query.get()
            assert query_info["result_count"] == 1
            assert_items_equal(query.read_result(0), [{"1": 1}])

    @authors("gudqeit")
    def test_read_table(self, query_tracker):
        table_schema = [{"name": "value", "type": "int64"}]
        create("table", "//tmp/test_table", attributes={"schema": table_schema})
        write_table("//tmp/test_table", [{"value": 1}])

        with Clique(1, config_patch=TestQueriesChyt.CONFIG_PATCH, alias="*ch_alias"):
            settings = {"clique": "ch_alias"}
            query = start_query("chyt", "select * from `//tmp/test_table`", settings=settings)
            query.track()

            query_info = query.get()
            assert query_info["result_count"] == 1

            result_info = query.get_result(0)
            for column in result_info["schema"]:
                del column["type_v3"]
                del column["required"]
            result_info["schema"] = list(result_info["schema"])
            assert result_info["schema"] == table_schema
            assert_items_equal(query.read_result(0), [{"value": 1}])

    @authors("gudqeit")
    def test_table_mutations(self, query_tracker):
        table_schema = [{"name": "value", "type": "int64"}]
        create("table", "//tmp/t1", attributes={"schema": table_schema})
        write_table("//tmp/t1", [{"value": 1}])

        with Clique(1, config_patch=TestQueriesChyt.CONFIG_PATCH, alias="*ch_alias"):
            settings = {"clique": "ch_alias"}
            query = "create table `//tmp/t2` engine=YtTable() as select * from `//tmp/t1`"
            query = start_query("chyt", query, settings=settings)
            query.track()

            query = "insert into `//tmp/t2` select * from `//tmp/t1`"
            query = start_query("chyt", query, settings=settings)
            query.track()

            query = start_query("chyt", "select * from `//tmp/t2`", settings=settings)
            query.track()

            query_info = query.get()
            assert query_info["result_count"] == 1
            assert_items_equal(query.read_result(0), [{"value": 1}, {"value": 1}])

    @authors("gudqeit")
    def test_query_settings(self, query_tracker):
        with Clique(1, config_patch=TestQueriesChyt.CONFIG_PATCH, alias="*ch_alias"):
            query_text = "select * from numbers(5)"
            expected = [{"number": 0}]
            settings = {
                "clique": "ch_alias",
                "query_settings": {
                    "limit": "1",
                },
            }
            query = start_query("chyt", query_text, settings=settings)
            query.track()
            query_info = query.get()
            assert query_info["result_count"] == 1
            assert_items_equal(query.read_result(0), expected)

            # Unrecognized settings are flattened and passed as query settings.
            settings = {
                "clique": "ch_alias",
                "limit": 1,
            }
            query = start_query("chyt", query_text, settings=settings)
            query.track()
            assert_items_equal(query.read_result(0), expected)

    @authors("gudqeit")
    def test_query_error(self, query_tracker):
        with Clique(1, config_patch=TestQueriesChyt.CONFIG_PATCH, alias="*ch_alias"):
            settings = {"clique": "ch_alias"}
            query = start_query("chyt", "select * from `//tmp/t`", settings=settings)
            with raises_yt_error("failed"):
                query.track()
            assert query.get_state() == "failed"

    @authors("gudqeit")
    def test_types(self, query_tracker):
        schema = [
            {
                "name": "list_int32",
                "type_v3": {
                    "type_name": "list",
                    "item": "int32",
                },
            },
            {
                "name": "decimal32",
                "type_v3": decimal_type(9, 2),
            },
            {
                "name": "string",
                "required": False,
                "type": "string",
            },
        ]
        create("table", "//tmp/t", attributes={"schema": schema})
        write_table("//tmp/t", [
            {
                "list_int32": [1, 2, 3],
                "decimal32": encode_decimal("1.1", 9, 2),
                "string": "some_str",
            },
            {
                "list_int32": [],
                "decimal32": encode_decimal("1.1", 9, 2),
                "string": None,
            }
        ])

        with Clique(1, config_patch=TestQueriesChyt.CONFIG_PATCH, alias="*ch_alias"):
            settings = {"clique": "ch_alias"}
            query = start_query("chyt", "select * from `//tmp/t`", settings=settings)
            query.track()

            assert_items_equal(query.read_result(0), [
                {
                    "list_int32": [1, 2, 3],
                    "decimal32": encode_decimal("1.1", 9, 2),
                    "string": "some_str",
                },
                {
                    "list_int32": [],
                    "decimal32": encode_decimal("1.1", 9, 2),
                    "string": yson.YsonEntity(),
                },
            ])
