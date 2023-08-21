from yt_commands import authors, raises_yt_error, create, get, remove, write_table, read_table, exists

from yt_type_helpers import make_schema, normalize_schema

from yt.test_helpers import assert_items_equal

from base import ClickHouseTestBase, QueryFailedError, Clique

from helpers import get_object_attribute_cache_config, get_schema_from_description

import time
import copy
import random


class TestMutations(ClickHouseTestBase):
    @authors("max42")
    def test_insert_values(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t"(i64) values (1), (-2)')
            clique.make_query('insert into "//tmp/t"(ui64) values (7), (8)')
            clique.make_query('insert into "//tmp/t"(str) values (2)')
            clique.make_query("insert into \"//tmp/t\"(i64, ui64, str, dbl, bool) values (-1, 1, 'abc', 3.14, 1)")
            clique.make_query('insert into "//tmp/t"(i64, ui64, str, dbl, bool) values (NULL, NULL, NULL, NULL, NULL)')
            with raises_yt_error(QueryFailedError):
                clique.make_query('insert into "//tmp/t"(bool) values (3)')
            with raises_yt_error(QueryFailedError):
                clique.make_query('insert into "//tmp/t"(bool) values (2.4)')
            assert read_table("//tmp/t") == [
                {"i64": 1, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": -2, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 7, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 8, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": None, "str": "2", "dbl": None, "bool": None},
                {"i64": -1, "ui64": 1, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": None, "ui64": None, "str": None, "dbl": None, "bool": None},
            ]
            assert get("//tmp/t/@chunk_count") == 5
            clique.make_query("insert into \"<append=%false>//tmp/t\" values (-2, 2, 'xyz', 2.71, 0)")
            assert read_table("//tmp/t") == [
                {"i64": -2, "ui64": 2, "str": "xyz", "dbl": 2.71, "bool": False},
            ]
            assert get("//tmp/t/@chunk_count") == 1

    @authors("max42")
    def test_insert_values_complex(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "list_i64", "type_v3": {"type_name": "list", "item": "int64"}},
                    {"name": "tuple_dbl_bool",
                     "type_v3": {"type_name": "tuple",
                                 "elements": [{"type": "double"}, {"type": "bool"}]}},
                    {"name": "struct_ui8_str",
                     "type_v3": {"type_name": "struct",
                                 "members": [{"name": "ui8", "type": "uint8"}, {"name": "str", "type": "string"}]}},
                    {"name": "list_optional_i32",
                     "type_v3": {"type_name": "list", "item": {"type_name": "optional", "item": "int32"}}},
                ]
            },
        )
        with Clique(1) as clique:
            clique.make_query("insert into `//tmp/t`(list_i64) values ([2,-3]), ([]), ([42])")
            clique.make_query("insert into `//tmp/t`(tuple_dbl_bool) values ((3.14,1)), ((-2.71,0)), ((0.0,1))")
            clique.make_query("insert into `//tmp/t`(struct_ui8_str) values ((42,'foo')), ((0,'bar')), ((255,'baz'))")
            clique.make_query("insert into `//tmp/t`(list_optional_i32) values ([23, NULL]), ([]), ([57])")
            clique.make_query("insert into `//tmp/t`(list_i64, tuple_dbl_bool, struct_ui8_str, list_optional_i32) "
                              "values ([9,8,7], (6.02,0), (17,'qux'), [NULL,57,NULL,18])")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(list_i64) values (42)")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(list_i64) values ('foo')")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(list_i64) values (('foo', 'bar'))")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(tuple_dbl_bool) values ((3.14))")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(tuple_dbl_bool) values ((3.14,1,'foo'))")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(tuple_dbl_bool) values (('foo',1))")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(struct_ui8_str) values ((42))")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(struct_ui8_str) values ((42,'foo',3.14))")
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into `//tmp/t`(struct_ui8_str) values (('foo',42))")
            # TODO(max42): this crashes due to CH bug: CHYT-535
            # with raises_yt_error(QueryFailedError):
            #     clique.make_query("insert into `//tmp/t`(struct_ui8_str) values ((NULL,'foo'))")

            def populate_with_defaults(rows):
                defaults = {"list_i64": [], "tuple_dbl_bool": [0.0, False], "struct_ui8_str": {"ui8": 0, "str": ""},
                            "list_optional_i32": []}
                result = []
                for row in rows:
                    cloned_row = copy.deepcopy(defaults)
                    cloned_row.update(row)
                    result.append(cloned_row)
                return result

            assert read_table("//tmp/t") == populate_with_defaults([
                {"list_i64": [2, -3]},
                {"list_i64": []},
                {"list_i64": [42]},
                {"tuple_dbl_bool": [3.14, True]},
                {"tuple_dbl_bool": [-2.71, False]},
                {"tuple_dbl_bool": [0.0, True]},
                {"struct_ui8_str": {"ui8": 42, "str": "foo"}},
                {"struct_ui8_str": {"ui8": 0, "str": "bar"}},
                {"struct_ui8_str": {"ui8": 255, "str": "baz"}},
                {"list_optional_i32": [23, None]},
                {"list_optional_i32": []},
                {"list_optional_i32": [57]},
                {"list_i64": [9, 8, 7], "tuple_dbl_bool": [6.02, False], "struct_ui8_str": {"ui8": 17, "str": "qux"},
                 "list_optional_i32": [None, 57, None, 18]}
            ])

    @authors("max42")
    def test_insert_select(self):
        create(
            "table",
            "//tmp/s1",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        write_table(
            "//tmp/s1",
            [
                {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ],
        )

        # Table with different order of columns.
        create(
            "table",
            "//tmp/s2",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        write_table(
            "//tmp/s2",
            [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
            ],
        )

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t" select * from "//tmp/s1"')
            assert read_table("//tmp/t") == [
                {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ]

            # Number of columns does not match.
            with raises_yt_error(QueryFailedError):
                clique.make_query('insert into "//tmp/t" select i64, ui64 from "//tmp/s1"')

            # Columns are matched according to positions. Values are best-effort casted due to CH logic.
            clique.make_query('insert into "<append=%false>//tmp/t" select * from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": None, "str": "12.3", "dbl": 9.0, "bool": False},
                {"i64": -5, "ui64": None, "str": "-3.1", "dbl": 5.0, "bool": True},
            ]

            clique.make_query('insert into "<append=%false>//tmp/t" select i64, ui64, str, dbl, bool from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
            ]

            clique.make_query('insert into "<append=%false>//tmp/t" select * from "//tmp/t"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
            ]

            clique.make_query('insert into "//tmp/t"(i64, ui64) select max(i64), min(ui64) from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
                {"i64": 4, "ui64": 5, "str": None, "dbl": None, "bool": None},
            ]

    @authors("max42")
    def test_distributed_insert_select(self):
        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": i} for i in range(100)]
        write_table("//tmp/t_in", rows, verbose=False)
        create("table", "//tmp/t_out", attributes={"schema": [{"name": "a", "type": "int64"}]})
        with Clique(5, config_patch={"clickhouse": {"settings": {"max_threads": 1}},
                                     "yt": {"subquery": {"min_data_weight_per_subquery": 1}}}) as clique:
            clique.make_query("insert into `//tmp/t_out` select * from `//tmp/t_in`")
            assert_items_equal(read_table("//tmp/t_out", verbose=False), rows)
            assert get("//tmp/t_out/@chunk_count") == 1

            # Distributed INSERT is suitable in cases below.
            write_table("//tmp/t_out", [])

            clique.make_query("insert into `//tmp/t_out` select * from `//tmp/t_in`",
                              settings={"parallel_distributed_insert_select": 1})
            assert_items_equal(read_table("//tmp/t_out", verbose=False), rows)
            assert get("//tmp/t_out/@chunk_count") == 5

            clique.make_query("insert into `//tmp/t_out` select * from `//tmp/t_in`",
                              settings={"parallel_distributed_insert_select": 1})
            assert_items_equal(read_table("//tmp/t_out", verbose=False), rows + rows)
            assert get("//tmp/t_out/@chunk_count") == 10

            clique.make_query("insert into `<append=%false>//tmp/t_out` select * from `//tmp/t_in`",
                              settings={"parallel_distributed_insert_select": 1})
            assert_items_equal(read_table("//tmp/t_out", verbose=False), rows)
            assert get("//tmp/t_out/@chunk_count") == 5

            clique.make_query("insert into `<append=%false>//tmp/t_in` select * from `//tmp/t_in`",
                              settings={"parallel_distributed_insert_select": 1})
            assert_items_equal(read_table("//tmp/t_in", verbose=False), rows)
            assert get("//tmp/t_in/@chunk_count") == 5

            # Distributed INSERT produces underaggregated result, but we did our best.
            write_table("//tmp/t_out", [])
            clique.make_query("insert into `//tmp/t_out` select sum(a) from `//tmp/t_in`",
                              settings={"parallel_distributed_insert_select": 1})
            aggregated_rows = read_table("//tmp/t_out")
            assert sum(row["a"] for row in aggregated_rows) == sum([row["a"] for row in rows])

            # Distributed INSERT is not suitable for cases below.
            with raises_yt_error(QueryFailedError):
                clique.make_query("insert into concatYtTables(`//tmp/t_out`, `//tmp/t_out`) select * from `//tmp/t_in`")

            clique.make_query("insert into `<append=%false>//tmp/t_out` select * from `//tmp/t_in` "
                              "union all select * from `//tmp/t_in`",
                              settings={"parallel_distributed_insert_select": 1})
            assert_items_equal(read_table("//tmp/t_out", verbose=False), rows + rows)
            assert get("//tmp/t_out/@chunk_count") == 1

            clique.make_query("insert into `<append=%false>//tmp/t_out` select 1 union all select 2 union all select 3")
            assert_items_equal(read_table("//tmp/t_out", verbose=False), [{"a": 1}, {"a": 2}, {"a": 3}])
            assert get("//tmp/t_out/@chunk_count") == 1

    @authors("gudqeit")
    def test_distributed_insert_error(self):
        schema = [{"name": "a", "type": "int64"}]
        create("table", "//tmp/t_in", attributes={"schema": schema})
        rows = [{"a": i} for i in range(100)]
        write_table("//tmp/t_in", rows, verbose=False)
        create("table", "//tmp/t_out", attributes={"schema": schema})

        config_patch = {
            "clickhouse": {
                "settings": {
                    "max_threads": 1,
                    "parallel_distributed_insert_select": 1,
                }
            },
            "yt": {
                "subquery": {
                    "min_data_weight_per_subquery": 1
                }
            }
        }
        with Clique(3, config_patch=config_patch) as clique:
            # Distributed insert as select.
            for _ in range(5):
                number = random.randint(0, 99)
                with raises_yt_error(QueryFailedError):
                    clique.make_query("insert into `//tmp/t_out` select throwIf(a = {}, 'Generate error') from `//tmp/t_in`".format(number))
                read_table("//tmp/t_out", verbose=False) == []
                assert get("//tmp/t_out/@chunk_count") == 0

            # Distributed create as select.
            for _ in range(5):
                number = random.randint(0, 99)
                query = 'create table "//tmp/t1" engine YtTable() as select throwIf(a = {}, "Generate error") from "//tmp/t_in"'.format(number)
                with raises_yt_error(QueryFailedError):
                    clique.make_query(query)
                assert not exists("//tmp/t1")

    @authors("max42")
    def test_create_table_simple(self):
        with Clique(1, config_patch={"yt": {"create_table_default_attributes": {"foo": 42}}}) as clique:
            clique.make_query(
                'create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, '
                'i32 Int32, dt Date, dtm DateTime) engine YtTable() order by (str, i64)'
            )
            assert normalize_schema(get("//tmp/t/@schema")) == make_schema(
                [
                    {"name": "str", "type": "string", "sort_order": "ascending", "required": True},
                    {"name": "i64", "type": "int64", "sort_order": "ascending", "required": True},
                    {"name": "ui64", "type": "uint64", "required": True},
                    {"name": "dbl", "type": "double", "required": True},
                    {"name": "i32", "type": "int32", "required": True},
                    {"name": "dt", "type": "date", "required": True},
                    {"name": "dtm", "type": "datetime", "required": True},
                ],
                strict=True,
                unique_keys=False,
            )

            # Table already exists.
            with raises_yt_error(QueryFailedError):
                clique.make_query(
                    'create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, i32 Int32) '
                    "engine YtTable() order by (str, i64)"
                )

            clique.make_query(
                'create table "//tmp/t_nullable"(i64 Nullable(Int64), ui64 Nullable(UInt64), str Nullable(String), '
                + "dbl Nullable(Float64), i32 Nullable(Int32), dt Nullable(Date), dtm Nullable(DateTime))"
                  ""
                  "engine YtTable() order by (str, i64)"
            )
            assert normalize_schema(get("//tmp/t_nullable/@schema")) == make_schema(
                [
                    {"name": "str", "type": "string", "sort_order": "ascending", "required": False},
                    {"name": "i64", "type": "int64", "sort_order": "ascending", "required": False},
                    {"name": "ui64", "type": "uint64", "required": False},
                    {"name": "dbl", "type": "double", "required": False},
                    {"name": "i32", "type": "int32", "required": False},
                    {"name": "dt", "type": "date", "required": False},
                    {"name": "dtm", "type": "datetime", "required": False},
                ],
                strict=True,
                unique_keys=False,
            )

            # No non-trivial expressions.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t2"(i64 Int64) engine YtTable() order by (i64 * i64)')

            # Missing key column.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t2"(i Int64) engine YtTable() order by j')

            clique.make_query("create table \"//tmp/t_snappy\"(i Int64) engine YtTable('{compression_codec=snappy}')")
            assert get("//tmp/t_snappy/@compression_codec") == "snappy"

            # Default optimize_for should be scan.
            assert get("//tmp/t_snappy/@optimize_for") == "scan"

            assert get("//tmp/t_snappy/@foo") == 42

            # Empty schema.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t2" engine YtTable()')

            # Underscore indicates that the columns should be ignored and schema from attributes should
            # be taken.
            clique.make_query("create table \"//tmp/t2\"(_ UInt8) engine YtTable('{schema=[{name=a;type=int64}]}')")
            assert get("//tmp/t2/@schema/0/name") == "a"

            # Column list has higher priority.
            clique.make_query("create table \"//tmp/t3\"(b String) engine YtTable('{schema=[{name=a;type=int64}]}')")
            assert get("//tmp/t3/@schema/0/name") == "b"

    @authors("gudqeit")
    def test_create_table_with_yql_operation_id(self):
        headers = {
            "X-YQL-Operation-Id": "6151ee26d2b70ca7f86498ef"
        }

        with Clique(1) as clique:
            clique.make_query(
                'create table "//tmp/t"(i64 Int64) engine YtTable()',
                headers=headers
            )
            assert get("//tmp/t/@_yql_op_id") == "6151ee26d2b70ca7f86498ef"

    @authors("gudqeit")
    def test_create_table_with_yql_operation_id_and_trash(self):
        with Clique(1) as clique:
            headers = {
                "X-YQL-Operation-Id": "6151ee26d2b70ca7f86498ef5"
            }
            clique.make_query(
                'create table "//tmp/t1"(i64 Int64) engine YtTable()',
                headers=headers
            )
            assert get("//tmp/t1/@_yql_op_id") == "6151ee26d2b70ca7f86498ef"
            assert len(get("//tmp/t1/@_yql_op_id")) == 24

            headers = {
                "X-YQL-Operation-Id": "6151ee26d2b70ca7f86498ef56"
            }
            clique.make_query(
                'create table "//tmp/t2"(i64 Int64) engine YtTable()',
                headers=headers
            )
            assert get("//tmp/t2/@_yql_op_id") == "6151ee26d2b70ca7f86498ef"
            assert len(get("//tmp/t2/@_yql_op_id")) == 24

            headers = {
                "X-YQL-Operation-Id": "6151ee26d2b"
            }
            clique.make_query(
                'create table "//tmp/t3"(i64 Int64) engine YtTable()',
                headers=headers
            )
            assert get("//tmp/t3/@_yql_op_id") == "6151ee26d2b"
            assert len(get("//tmp/t3/@_yql_op_id")) == 11

    @authors("max42")
    def test_create_table_as_select(self):
        create(
            "table",
            "//tmp/s1",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        write_table(
            "//tmp/s1",
            [
                {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": True},
                {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ],
        )

        with Clique(1) as clique:
            clique.make_query('create table "//tmp/t1" engine YtTable() order by i64 as select * from "//tmp/s1"')

            assert read_table("//tmp/t1") == [
                {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": 1},
                {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": 0},
            ]

    @authors("gudqeit")
    def test_create_table_as_select_error(self):
        create("table", "//tmp/s1", attributes={"schema": [{"name": "str", "type": "string"}]})
        write_table(
            "//tmp/s1",
            [
                {"str": "def"},
                {"str": "xyz"},
            ],
        )

        with Clique(1) as clique:
            query = 'create table "//tmp/t1" engine YtTable() order by str as select throwIf(str = "xyz", "Generate error") from "//tmp/s1"'
            with raises_yt_error(QueryFailedError):
                clique.make_query(query)
            assert not exists("//tmp/t1")

            query = 'create table "//tmp/t1" engine YtTable() as select * from "//tmp/t1"'
            with raises_yt_error(QueryFailedError):
                clique.make_query(query)
            assert not exists("//tmp/t1")

    @authors("max42")
    def test_create_table_as_table(self):
        schema = [
            {"name": "i64", "type": "int64", "required": False, "sort_order": "ascending"},
            {"name": "ui64", "type": "uint64", "required": False},
            {"name": "str", "type": "string", "required": False},
            {"name": "dbl", "type": "double", "required": False},
            {"name": "bool", "type": "boolean", "required": False},
        ]
        create("table", "//tmp/s1", attributes={"schema": schema, "compression_codec": "snappy"})

        with Clique(1) as clique:
            clique.make_query('show create table "//tmp/s1"')
            clique.make_query('create table "//tmp/s2" as "//tmp/s1" engine YtTable() order by i64')
            assert normalize_schema(get("//tmp/s2/@schema")) == make_schema(
                schema, strict=True, unique_keys=False
            )

            # This is wrong.
            # assert get("//tmp/s2/@compression_codec") == "snappy"

    @authors("dakovalkov")
    def test_create_table_with_intermediate_nodes(self):
        with Clique(1, config_patch={"yt": {"create_table_default_attributes": {"foo": 42}}}) as clique:
            clique.make_query(
                'create table "//tmp/n1/n2/n3/n4/t"(test Int64) engine YtTable()'
            )
            assert normalize_schema(get("//tmp/n1/n2/n3/n4/t/@schema")) == make_schema(
                [{"name": "test", "type": "int64", "required": True}],
                strict=True,
                unique_keys=False,
            )

    @authors("dakovalkov")
    def test_create_table_clear_cache(self):
        patch = get_object_attribute_cache_config(15000, 15000, 500)
        with Clique(1, config_patch=patch) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})

            # Load attributes into cache.
            assert get_schema_from_description(clique.make_query('describe "//tmp/t"')) == [
                {"name": "a", "type": "Nullable(Int64)"}
            ]

            remove("//tmp/t")

            # Wait for clearing cache.
            time.sleep(1)

            clique.make_query('create table "//tmp/t"(b String) engine YtTable()')

            assert get_schema_from_description(clique.make_query('describe "//tmp/t"')) == [
                {"name": "b", "type": "String"}
            ]

    @authors("dakvalkov")
    def test_automatic_distributed_insert_simple(self):
        schema = [
            {"name": "a", "type": "int64", "required": False},
        ]
        create("table", "//tmp/in", attributes={"schema": schema})
        create("table", "//tmp/out", attributes={"schema": schema})

        chunks = [
            [{"a": 1}, {"a": 1}, {"a": 2}],
            [{"a": 2}, {"a": 3}, {"a": 3}],
        ]
        for chunk in chunks:
            write_table("<append=%true>//tmp/in", chunk)

        patch = {
            "yt": {
                "subquery": {
                    "min_data_weight_per_thread": 1,
                },
            },
        }

        with Clique(2, config_patch=patch) as clique:
            def get_settings(distributed_insert_stage):
                settings = {
                    "parallel_distributed_insert_select": 1,
                    "chyt.execution.optimize_query_processing_stage": 1,
                    "chyt.execution.distributed_insert_stage": distributed_insert_stage,
                }
                return settings

            def check_output_table(expected_rows, expected_chunk_count):
                assert get("//tmp/out/@chunk_count") == expected_chunk_count
                rows = read_table("//tmp/out")
                assert_items_equal(rows, expected_rows)

            expected_rows = [{"a": 1}, {"a": 1}, {"a": 2}, {"a": 2}, {"a": 3}, {"a": 3}]
            expected_distinct_rows = [{"a": 1}, {"a": 2}, {"a": 3}]
            expected_partially_distinct_rows = [{"a": 1}, {"a": 2}, {"a": 2}, {"a": 3}]

            # Simple.
            simple_query = 'insert into "<append=%false>//tmp/out" select * from "//tmp/in"'

            clique.make_query(simple_query, settings=get_settings('none'))
            check_output_table(expected_rows, 1)

            clique.make_query(simple_query, settings=get_settings('complete'))
            check_output_table(expected_rows, 2)

            # Join.
            join_query = '''insert into "<append=%false>//tmp/out"
                select * from "//tmp/in" as x left any join (select * from "//tmp/in") as y using a'''

            clique.make_query(join_query, settings=get_settings('none'))
            check_output_table(expected_rows, 1)

            settings = get_settings('complete')
            settings["chyt.execution.distribute_only_global_and_sorted_join"] = 0
            clique.make_query(join_query, settings=settings)
            check_output_table(expected_rows, 2)

            # Order by.
            order_by_query = 'insert into "<append=%false>//tmp/out" select * from "//tmp/in" order by a'

            clique.make_query(order_by_query, settings=get_settings('complete'))
            check_output_table(expected_rows, 1)

            clique.make_query(order_by_query, settings=get_settings('after_aggregation'))
            check_output_table(expected_rows, 2)

            # Limit.
            limit_query = 'insert into "<append=%false>//tmp/out" select * from "//tmp/in" limit 10'

            clique.make_query(limit_query, settings=get_settings('complete'))
            check_output_table(expected_rows, 1)

            clique.make_query(limit_query, settings=get_settings('after_aggregation'))
            check_output_table(expected_rows, 2)

            # Distinct.
            distinct_query = 'insert into "<append=%false>//tmp/out" select distinct * from "//tmp/in"'

            clique.make_query(distinct_query, settings=get_settings('after_aggregation'))
            check_output_table(expected_distinct_rows, 1)

            clique.make_query(distinct_query, settings=get_settings('with_mergeable_state'))
            check_output_table(expected_partially_distinct_rows, 2)

            # Group by.
            group_by_query = 'insert into "<append=%false>//tmp/out" select * from "//tmp/in" group by *'

            clique.make_query(group_by_query, settings=get_settings('after_aggregation'))
            check_output_table(expected_distinct_rows, 1)

            clique.make_query(group_by_query, settings=get_settings('with_mergeable_state'))
            check_output_table(expected_partially_distinct_rows, 2)

            # Limit by.
            limit_by_query = 'insert into "<append=%false>//tmp/out" select * from "//tmp/in" limit 1 by *'

            clique.make_query(limit_by_query, settings=get_settings('after_aggregation'))
            check_output_table(expected_distinct_rows, 1)

            clique.make_query(limit_by_query, settings=get_settings('with_mergeable_state'))
            check_output_table(expected_partially_distinct_rows, 2)

            # Simple aggregation.
            simple_aggregation_query = 'insert into "<append=%false>//tmp/out" select sum(a) as a from "//tmp/in"'

            clique.make_query(simple_aggregation_query, settings=get_settings('after_aggregation'))
            check_output_table([{"a": 12}], 1)

            clique.make_query(simple_aggregation_query, settings=get_settings('with_mergeable_state'))
            check_output_table([{"a": 4}, {"a": 8}], 2)

    @authors("dakovalkov")
    def test_automatic_distributed_insert_ordered_table(self):
        input_schema = [
            {"name": "a", "type": "int64", "sort_order": "ascending", "required": False},
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False},
            # {"name": "c", "type": "int64", "sort_order": "ascending", "required": False},
            # {"name": "d", "type": "int64", "required": False},
        ]
        output_schema = [
            {"name": "a", "type": "int64", "required": False},
            # {"name": "b", "type": "int64", "required": False},
            # {"name": "c", "type": "int64", "required": False},
            # {"name": "d", "type": "int64", "required": False},
        ]
        create("table", "//tmp/in", attributes={"schema": input_schema})
        create("table", "//tmp/out", attributes={"schema": output_schema})

        chunks = [
            [{"a": 1, "b": 1}, {"a": 1, "b": 1}, {"a": 2, "b": 2}],
            [{"a": 2, "b": 2}, {"a": 3, "b": 3}, {"a": 3, "b": 3}],
        ]
        for chunk in chunks:
            write_table("<append=%true>//tmp/in", chunk)

        patch = {
            "yt": {
                "subquery": {
                    "min_data_weight_per_thread": 1,
                },
            },
        }

        with Clique(2, config_patch=patch) as clique:
            def get_settings(distributed_insert_stage):
                settings = {
                    "parallel_distributed_insert_select": 1,
                    "chyt.execution.optimize_query_processing_stage": 1,
                    "chyt.execution.allow_switch_to_sorted_pool": 1,
                    "chyt.execution.allow_key_truncating": 1,
                    "chyt.execution.distributed_insert_stage": distributed_insert_stage,
                }
                return settings

            def check_output_table(expected_rows, expected_chunk_count):
                assert get("//tmp/out/@chunk_count") == expected_chunk_count
                rows = read_table("//tmp/out")
                assert_items_equal(rows, expected_rows)

            expected_distinct_rows = [{"a": 1}, {"a": 2}, {"a": 3}]

            group_by_key_column_query = 'insert into "<append=%false>//tmp/out" select a from "//tmp/in" group by a'
            clique.make_query(group_by_key_column_query, settings=get_settings('complete'))
            check_output_table(expected_distinct_rows, 2)

            group_by_subkey_column_query = 'insert into "<append=%false>//tmp/out" select b as a from "//tmp/in" group by b'
            clique.make_query(group_by_subkey_column_query, settings=get_settings('complete'))
            check_output_table(expected_distinct_rows, 1)

            group_by_long_key_query = 'insert into "<append=%false>//tmp/out" select a from "//tmp/in" group by a, b'
            clique.make_query(group_by_long_key_query, settings=get_settings('complete'))
            check_output_table(expected_distinct_rows, 2)

            group_by_with_join_query = '''insert into "<append=%false>//tmp/out"
                select a from "//tmp/in" as x join "//tmp/in" as y using a, b
                group by a'''
            clique.make_query(group_by_with_join_query, settings=get_settings('complete'))
            check_output_table(expected_distinct_rows, 2)
