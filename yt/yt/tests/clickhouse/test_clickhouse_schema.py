from base import ClickHouseTestBase, Clique, QueryFailedError

from yt_commands import (authors, create, write_table, raises_yt_error)


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

    @authors("max42")
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

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})
        write_table("//tmp/t3", {"a": "1"})
        write_table("//tmp/t4", {"a": "2"})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2")')) == [
                {"name": "a", "type": "Nullable(Int64)"}
            ]
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") order by a') == [
                {"a": 17},
                {"a": 42},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2", "//tmp/t3")')

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

    @authors("max42")
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
                {"name": "a", "type": "Nullable(Int64)"}
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t2", "//tmp/t1")')) == [
                {"name": "a", "type": "Nullable(Int64)"}
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t3")')) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "c", "type": "Nullable(Float64)"},
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
