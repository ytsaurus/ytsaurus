from base import ClickHouseTestBase, Clique, QueryFailedError

from yt_commands import (authors, create, write_table, raises_yt_error, make_schema)

from yt.wrapper import yson


class TestClickHouseSchema(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("evgenstf")
    def test_int_types(self):
        with Clique(5) as clique:
            create(
                "table",
                "//tmp/test_table",
                attributes={
                    "schema": [
                        {"name": "int64_value", "type": "int64"},
                        {"name": "int32_value", "type": "int32"},
                        {"name": "int16_value", "type": "int16"},
                        {"name": "int8_value", "type": "int8"},
                        {"name": "uint64_value", "type": "uint64"},
                        {"name": "uint32_value", "type": "uint32"},
                        {"name": "uint16_value", "type": "uint16"},
                        {"name": "uint8_value", "type": "uint8"},
                    ]
                },
            )
            name_to_expected_type = {
                "int64_value": "Nullable(Int64)",
                "int32_value": "Nullable(Int32)",
                "int16_value": "Nullable(Int16)",
                "int8_value": "Nullable(Int8)",
                "uint64_value": "Nullable(UInt64)",
                "uint32_value": "Nullable(UInt32)",
                "uint16_value": "Nullable(UInt16)",
                "uint8_value": "Nullable(UInt8)",
            }
            table_description = clique.make_query('describe "//tmp/test_table"')
            for column_description in table_description:
                assert name_to_expected_type[column_description["name"]] == column_description["type"]

    @authors("max42")
    def test_missing_schema(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key": 42, "value": "x"}])

        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('select * from "//tmp/t"')

    @staticmethod
    def _strip_description(rows):
        return [{key: value for key, value in row.iteritems() if key in ("name", "type")} for row in rows]

    @authors("max42", "dakovalkov")
    def test_common_schema_unsorted(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "d", "type": "double"},
                ]
            },
        )
        create("table", "//tmp/t3", attributes={"schema": [{"name": "a", "type": "string"}]})
        create("table", "//tmp/t4", attributes={"schema": [{"name": "a", "type": "string", "required": True}]})
        create("table", "//tmp/t5", attributes={"schema": [{"name": "b", "type": "string"}]})

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})
        write_table("//tmp/t3", {"a": "1"})
        write_table("//tmp/t4", {"a": "2"})
        write_table("//tmp/t5", {"b": "3"})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2")')) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "b", "type": "Nullable(String)"},
                {"name": "c", "type": "Nullable(Float64)"},
                {"name": "d", "type": "Nullable(Float64)"},
            ]
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") order by a') == [
                {"a": 17, "b": None, "c": None, "d": 2.71},
                {"a": 42, "b": "x", "c": 3.14, "d": None},
            ]

            settings = {"chyt.concat_tables.on_column_miss": "throw"}
            with raises_yt_error(QueryFailedError):
                clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2")', settings=settings)

            settings = {"chyt.concat_tables.on_column_miss": "drop"}
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2")', settings=settings)) == [
                {"name": "a", "type": "Nullable(Int64)"},
            ]
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") order by a', settings=settings) == [
                {"a": 17},
                {"a": 42},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query('describe concatYtTables("//tmp/t2", "//tmp/t3")')

            settings = {
                "chyt.concat_tables.on_incompatible_types": "drop",
                "chyt.concat_tables.allow_empty_schema_intersection": 1,
            }
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t2", "//tmp/t3")', settings=settings)) == [
                {"name": "d", "type": "Nullable(Float64)"},
            ]

            settings = {"chyt.concat_tables.on_incompatible_types": "read_as_any"}
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t2", "//tmp/t3")', settings=settings)) == [
                {"name": "a", "type": "Nullable(String)"},
                {"name": "d", "type": "Nullable(Float64)"},
            ]
            assert clique.make_query('select * from concatYtTables("//tmp/t2", "//tmp/t3") order by a', settings=settings) == [
                {"a": yson.dumps("1", yson_format='binary'), "d": None},
                {"a": yson.dumps(17, yson_format='binary'), "d": 2.71},
            ]

            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t3", "//tmp/t4")')) == [
                {"name": "a", "type": "Nullable(String)"}
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t4")')) == [
                {"name": "a", "type": "String"}
            ]

            assert sorted(clique.make_query('select * from concatYtTables("//tmp/t3", "//tmp/t4")')) == [
                {"a": "1"},
                {"a": "2"},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query('describe concatYtTables("//tmp/t3", "//tmp/t5")')

            settings = {"chyt.concat_tables.allow_empty_schema_intersection": 1}
            assert clique.make_query('select * from concatYtTables("//tmp/t3", "//tmp/t5") order by a', settings=settings) == [
                {"a": "1", "b": None},
                {"a": None, "b": "3"},
            ]

    @authors("max42", "dakovalkov")
    def test_common_schema_sorted(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "string", "sort_order": "ascending"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "d", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t3",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "c", "type": "double"},
                ]
            },
        )

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2")')) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "b", "type": "Nullable(String)"},
                {"name": "c", "type": "Nullable(Float64)"},
                {"name": "d", "type": "Nullable(Float64)"},
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t2", "//tmp/t1")')) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "d", "type": "Nullable(Float64)"},
                {"name": "b", "type": "Nullable(String)"},
                {"name": "c", "type": "Nullable(Float64)"},
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t3")')) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "b", "type": "Nullable(String)"},
                {"name": "c", "type": "Nullable(Float64)"},
            ]

    @authors("dakovalkov")
    def test_common_schema_non_strict(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
            }
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64"},
                        {"name": "b", "type": "int64"},
                        {"name": "c", "type": "int64"},
                    ],
                    strict=False),
            },
        )
        create("table", "//tmp/t3", attributes={"schema": make_schema([{"name": "a", "type": "int64"}], strict=False)})
        create("table", "//tmp/t4", attributes={"schema": make_schema([], strict=False)})

        write_table("//tmp/t1", {"a": 10, "b": 20})
        write_table("//tmp/t2", {"a": 20, "b": 30, "c": 40})
        write_table("//tmp/t3", {"a": 30, "b": "40"})
        write_table("//tmp/t4", {"a": 40})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query("describe concatYtTables('//tmp/t1', '//tmp/t2')")) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "b", "type": "Nullable(Int64)"},
                {"name": "c", "type": "Nullable(Int64)"},
            ]
            assert clique.make_query("select * from concatYtTables('//tmp/t1', '//tmp/t2') order by a") == [
                {"a": 10, "b": 20, "c": None},
                {"a": 20, "b": 30, "c": 40},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query("describe concatYtTables('//tmp/t1', '//tmp/t3')")

            settings = {"chyt.concat_tables.on_incompatible_types": "read_as_any"}
            assert self._strip_description(clique.make_query("describe concatYtTables('//tmp/t1', '//tmp/t3')", settings=settings)) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "b", "type": "Nullable(String)"},
            ]
            assert clique.make_query("select * from concatYtTables('//tmp/t1', '//tmp/t3') order by a", settings=settings) == [
                {"a": 10, "b": yson.dumps(20, yson_format="binary")},
                {"a": 30, "b": yson.dumps("40", yson_format="binary")},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query("describe concatYtTables('//tmp/t1', '//tmp/t4')")
            with raises_yt_error(QueryFailedError):
                clique.make_query("describe concatYtTables('//tmp/t1', '//tmp/t4')", settings=settings)

            settings = {
                "chyt.concat_tables.on_incompatible_types": "read_as_any",
                "chyt.concat_tables.allow_empty_schema_intersection": 1,
            }
            assert self._strip_description(clique.make_query("describe concatYtTables('//tmp/t1', '//tmp/t4')", settings=settings)) == [
                {"name": "a", "type": "Nullable(String)"},
                {"name": "b", "type": "Nullable(String)"},
            ]
            assert clique.make_query("select * from concatYtTables('//tmp/t1', '//tmp/t4') order by a", settings=settings) == [
                {"a": yson.dumps(10, yson_format="binary"), "b": yson.dumps(20, yson_format="binary")},
                {"a": yson.dumps(40, yson_format="binary"), "b": None},
            ]

    @authors("max42")
    def test_nulls_in_primary_key(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})

        content = [{"a": None}, {"a": -1}, {"a": 42}]
        write_table("//tmp/t", content)

        with Clique(1) as clique:
            for source in ['"//tmp/t"', "concatYtTables('//tmp/t')"]:
                assert clique.make_query("select * from {}".format(source)) == content
                assert clique.make_query("select * from {} where isNull(a)".format(source)) == [{"a": None}]
                assert clique.make_query("select * from {} where isNotNull(a)".format(source)) == [{"a": -1}, {"a": 42}]
