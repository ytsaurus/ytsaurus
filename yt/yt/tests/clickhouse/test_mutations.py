from yt_commands import (create, read_table, get, write_table, authors, raises_yt_error, normalize_schema, make_schema,
                         remove)

from base import ClickHouseTestBase, QueryFailedError, Clique

from helpers import get_object_attribute_cache_config, get_schema_from_description

import time


class TestMutations(ClickHouseTestBase):
    def setup(self):
        self._setup()

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

            clique.make_query('insert into "//tmp/t"(i64, ui64) select max(i64), min(ui64) from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
                {"i64": 4, "ui64": 5, "str": None, "dbl": None, "bool": None},
            ]

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
