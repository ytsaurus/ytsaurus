from yt_commands import (authors, create_access_control_object_namespace,
                         create_access_control_object, create, exists, read_table, write_table,
                         raises_yt_error, create_user, set, make_ace, wait)

from yt.test_helpers import assert_items_equal

from yt_type_helpers import decimal_type

from decimal_helpers import encode_decimal

from yt_queries import start_query

from base import ClickHouseTestBase, Clique

from yt.wrapper import yson


class TestQueriesChyt(ClickHouseTestBase):
    NUM_TEST_PARTITIONS = 4

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    def setup_method(self, method):
        super().setup_method(method)
        create_access_control_object_namespace(name="chyt")
        create_access_control_object(name="ch_alias", namespace="chyt")

    @authors("gudqeit")
    def test_simple_query(self, query_tracker):
        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
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

        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
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

        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
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
        with Clique(1, alias="*ch_alias"):
            query_text = "select * from numbers(5)"
            expected = [{"number": 0}]
            settings = {
                "cluster": "primary",
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
                "cluster": "primary",
                "clique": "ch_alias",
                "limit": 1,
            }
            query = start_query("chyt", query_text, settings=settings)
            query.track()
            assert_items_equal(query.read_result(0), expected)

    @authors("gudqeit")
    def test_query_error(self, query_tracker):
        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
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

        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
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

    @authors("gudqeit")
    def test_user_has_no_access_to_clique(self, query_tracker):
        create_user("u1")
        acl = [make_ace("deny", "u1", "use")]
        set("//sys/access_control_object_namespaces/chyt/ch_alias/principal/@acl", acl)

        table_schema = [{"name": "value", "type": "int64"}]
        create("table", "//tmp/test_table", attributes={"schema": table_schema})

        with Clique(1, alias="*ch_alias"):
            settings = {
                "clique": "ch_alias",
                "cluster": "primary",
            }

            query = start_query(
                "chyt", "select * from `//tmp/test_table`",
                settings=settings,
                authenticated_user="u1",
            )

            with raises_yt_error("User \"u1\" has no access to clique \"ch_alias\""):
                query.track()

    @authors("gudqeit")
    def test_user_has_no_access_to_data(self, query_tracker):
        create_user("u1")
        acl = [make_ace("allow", "u1", "use")]
        set("//sys/access_control_object_namespaces/chyt/ch_alias/principal/@acl", acl)

        acl = [make_ace("deny", "u1", "read")]
        table_schema = [{"name": "value", "type": "int64"}]
        create("table", "//tmp/test_table", attributes={"schema": table_schema, "acl": acl})

        with Clique(1, alias="*ch_alias"):
            settings = {
                "clique": "ch_alias",
                "cluster": "primary",
            }

            query = start_query(
                "chyt", "select * from `//tmp/test_table`",
                settings=settings,
                authenticated_user="u1",
            )

            with raises_yt_error("\"read\" permission for node //tmp/test_table is denied for \"u1\""):
                query.track()

    @authors("mpereskokova")
    def test_query_ids(self, query_tracker):
        with Clique(1, alias="*ch_alias") as clique:
            settings = {"clique": "ch_alias", "cluster": "primary"}
            query = start_query("chyt", "select 1", settings=settings)
            query.track()

            wait(lambda: any(query.id == log["query_id"] for log in clique.make_query("select query_id from system.query_log")))

    @authors("gudeqit", "dakovalkov")
    def test_conversion_for_const_columns(self, query_tracker):
        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}

            query = start_query("chyt", "select 1 as a from numbers(1000)", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": 1}] * 1000

            query = start_query("chyt", "select 'ab' as a from numbers(1000)", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": 'ab'}] * 1000

            query = start_query("chyt", "select [1, 2, 3] as a", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": [1, 2, 3]}]

            query = start_query("chyt", "select CAST(1, 'Nullable(UInt8)') as a", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": 1}]

            query = start_query("chyt", "select tuple(1, 'abc', 2) as a", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": [1, "abc", 2]}]

            query = start_query("chyt", "select CAST(123.23, 'Decimal(30, 2)') as a", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": encode_decimal("123.23", 30, 2)}]

    @authors("gudqeit")
    def test_conversion_for_enums(self, query_tracker):
        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
            query = start_query("chyt", "select CAST('a', 'Nullable(Enum8(\\'a\\' = 1))') as a", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": "a"}]

            query = start_query("chyt", "select CAST('a', 'Nullable(Enum16(\\'a\\' = 1))') as a", settings=settings)
            query.track()
            assert query.read_result(0) == [{"a": "a"}]

    @authors("gudqeit")
    def test_conversion_for_ip_addresses(self, query_tracker):
        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
            query = start_query("chyt", "select toIPv4('127.0.0.1') as ip", settings=settings)
            query.track()
            assert query.read_result(0) == [{"ip": "127.0.0.1"}]

            query = start_query("chyt", "select toIPv6('127.0.0.1') as ip", settings=settings)
            query.track()
            assert query.read_result(0) == [{"ip": "::ffff:127.0.0.1"}]

    @authors("dakovalkov")
    def test_system_query_log(self, query_tracker):
        with Clique(1, alias="*ch_alias") as clique:
            wait(lambda: len(clique.make_query("select * from system.query_log limit 1")) > 0)

            settings = {"clique": "ch_alias", "cluster": "primary"}
            query = start_query("chyt", "select * from system.query_log", settings=settings)
            query.track()

            assert len(query.read_result(0)) > 0

    @authors("dakovalkov")
    def test_unsupported_types(self, query_tracker):
        with Clique(1, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
            query = """
                select
                    toDateTime64('2019-01-01 00:00:00', 2) as dt,
                    CAST(1, 'Enum(\\'hello\\' = 1, \\'world\\' = 2)') as en
            """
            query = start_query("chyt", query, settings=settings)
            query.track()

            assert query.read_result(0) == [{
                "dt": "2019-01-01 00:00:00.00",
                "en": "hello",
            }]

    @authors("a-dyu")
    def test_multiquery(self, query_tracker):
        root_dir = "//tmp/exporter"
        create("map_node", root_dir)
        patch = {
            "yt": {
                "system_log_table_exporters": {
                    "cypress_root_directory": root_dir,
                    "default": {
                        "enabled": True,
                        "max_rows_to_keep": 100000,
                    },
                },
            }
        }
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])
        with Clique(1, config_patch=patch, alias="*ch_alias"):
            settings = {"clique": "ch_alias", "cluster": "primary"}
            query = start_query(
                "chyt",
                """
                                select 1; // some comment
                                 set param_x=42; select {x:UInt32} as result; /* block comment */
                                 select a from '//tmp/t';
                                """,
                settings=settings,
            )
            query.track()

            query_info = query.get()
            assert query_info["result_count"] == 3
            assert_items_equal(query.read_result(0), [{"1": 1}])
            assert_items_equal(query.read_result(1), [{"result": 42}])
            assert_items_equal(query.read_result(2), [{"a": 1}])

            def match(row):
                return row["initial_query_id"] == query.id and row["type"] == "QueryFinish" and row["is_initial_query"]

            table_path = root_dir + "/query_log/0"
            wait(lambda: exists(table_path))
            wait(lambda: len([r for r in read_table(table_path) if match(r)]) > 0)
            rows = [r for r in read_table(table_path) if match(r)]
            assert len(rows) == 4
