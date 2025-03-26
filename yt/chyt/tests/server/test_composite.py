from base import ClickHouseTestBase, Clique

from helpers import get_schema_from_description

from yt_commands import (write_table, authors, create, get, read_table, concatenate)

from yt_type_helpers import (decimal_type, optional_type, list_type, tuple_type, make_schema, normalize_schema_v3)

from decimal_helpers import encode_decimal

from yt.test_helpers import assert_items_equal

import yt.yson as yson

import pytest


class TestComposite(ClickHouseTestBase):
    @authors("max42", "buyval01")
    def test_dict(self):
        create("table", "//tmp/t", attributes={"schema": [
            {
                "name": "a",
                "type_v3": {
                    "type_name": "dict",
                    "key": "string",
                    "value": "string",
                },
            },
        ]})
        write_table("//tmp/t", [{"a": []}, {"a": [["k1", "v1"], ["k2", "v2"]]}])

        with Clique(1) as clique:
            assert get_schema_from_description(clique.make_query('describe `//tmp/t`')) == [
                {"name": "a", "type": "Map(String, String)"},
            ]
            assert clique.make_query("select toTypeName(a) as ta from `//tmp/t` limit 1") == [
                {"ta": "Map(String, String)"}
            ]
            assert clique.make_query("select * from `//tmp/t`") == [
                {"a": {}},
                {"a": {"k1": "v1", "k2": "v2"}}
            ]
            clique.make_query("insert into `//tmp/t`(a) values ({'k1': 'v2', 'k3': 'v3'}), (CAST((['k3', 'k4'], ['v3', 'v4']), 'Map(String, String)'))")
            assert_items_equal(clique.make_query("select mapKeys(a) as keys from `//tmp/t`"), [
                {"keys": []},
                {"keys": ["k1", "k2"]},
                {"keys": ["k1", "k3"]},
                {"keys": ["k3", "k4"]},
            ])
            assert_items_equal(clique.make_query("select mapValues(a) as values from `//tmp/t`"), [
                {"values": []},
                {"values": ["v1", "v2"]},
                {"values": ["v2", "v3"]},
                {"values": ["v3", "v4"]},
            ])
            assert_items_equal(clique.make_query("select a['k1'] as k1 from `//tmp/t`"), [
                {"k1": ""},
                {"k1": "v1"},
                {"k1": "v2"},
                {"k1": ""},
            ])
            clique.make_query('create table "//tmp/tt" engine YtTable() as select * from "//tmp/t"')
            assert clique.make_query("select toTypeName(a) as ta from `//tmp/tt` limit 1") == [
                {"ta": "Map(String, String)"}
            ]
            assert_items_equal(read_table("//tmp/t"), read_table("//tmp/tt"))

    @authors("max42")
    def test_struct(self):
        create("table", "//tmp/t", attributes={"schema": [
            {
                "name": "a",
                "type_v3": {
                    "type_name": "struct",
                    "members": [
                        {
                            "name": "s",
                            "type": "string",
                        },
                        {
                            "name": "i",
                            "type": "int64",
                        },
                    ],
                },
            },
        ]})
        write_table("//tmp/t", [{"a": {"s": "foo", "i": 42}}])

        with Clique(1) as clique:
            assert get_schema_from_description(clique.make_query('describe `//tmp/t`', settings={"print_pretty_type_names": 0})) == [
                {"name": "a", "type": "Tuple(s String, i Int64)"},
            ]
            assert clique.make_query("select toTypeName(a) as ta from `//tmp/t`", settings={"print_pretty_type_names": 0}) == [
                {"ta": "Tuple(s String, i Int64)"}
            ]
            assert clique.make_query("select * from `//tmp/t`") == [
                {"a": {"s": "foo", "i": 42}}
            ]
            assert clique.make_query("select a.1 as s, a.2 as i from `//tmp/t`") == [
                {"s": "foo", "i": 42}
            ]
            # TODO(max42): CHYT-529.
            # assert clique.make_query("select a.1, a.2 from `//tmp/t`") == [
            #     {"a.s": "foo", "a.i": 42}
            # ]

            # TODO(max42): commented until untuple behavior is fixed (or it is completely removed from CH).
            # assert clique.make_query("select untuple(a) from `//tmp/t`") == [
            #     {"s": "foo", "i": 42}
            # ]

    @authors("dakovalkov")
    def test_decimal(self):
        create("table", "//tmp/t", attributes={"schema": [
            {
                "name": "decimal32",
                "type_v3": decimal_type(9, 2),
            },
            {
                "name": "decimal64",
                "type_v3": decimal_type(15, 5),
            },
            {
                "name": "decimal128",
                "type_v3": decimal_type(30, 10),
            },
            {
                "name": "decimal128_no_yql",
                "type_v3": decimal_type(38, 10)
            },
            {
                "name": "decimal256",
                "type_v3": decimal_type(70, 10)
            }
        ]})

        row = {
            "decimal32": encode_decimal("1.1", 9, 2),
            "decimal64": encode_decimal("1234.1234", 15, 5),
            "decimal128": encode_decimal("123456789.123456789", 30, 10),
            "decimal128_no_yql": encode_decimal("123456789.123456789", 38, 10),
            "decimal256": encode_decimal("123456789.123456789", 70, 10),
        }
        write_table("//tmp/t", [row])

        with Clique(1) as clique:
            assert get_schema_from_description(clique.make_query('describe "//tmp/t"')) == [
                {"name": "decimal32", "type": "Decimal(9, 2)"},
                {"name": "decimal64", "type": "Decimal(15, 5)"},
                {"name": "decimal128", "type": "Decimal(30, 10)"},
                {"name": "decimal128_no_yql", "type": "Decimal(38, 10)"},
                {"name": "decimal256", "type": "Decimal(70, 10)"},
            ]

            assert clique.make_query('select * from "//tmp/t"') == [{
                "decimal32": 1.1,
                "decimal64": 1234.1234,
                "decimal128": 123456789.123456789,
                "decimal128_no_yql": 123456789.123456789,
                "decimal256": 123456789.123456789,
            }]

            create_query = '''
                create table "//tmp/tout" engine YtTable() as select
                    toDecimal32(10.5, 5) as decimal32,
                    toDecimal64(100.005, 10) as decimal64,
                    CAST(0.000005, 'Decimal(35, 15)') as decimal128,
                    toDecimal128(15.43, 2) as decimal128_no_yql,
                    toDecimal256(42.424242424242, 10) as decimal256'''

            clique.make_query(create_query)

            assert normalize_schema_v3(get("//tmp/tout/@schema")) == make_schema(
                [
                    {
                        "name": "decimal32",
                        "type_v3": decimal_type(9, 5),
                    },
                    {
                        "name": "decimal64",
                        "type_v3": decimal_type(18, 10),
                    },
                    {
                        "name": "decimal128",
                        "type_v3": decimal_type(35, 15),
                    },
                    {
                        "name": "decimal128_no_yql",
                        "type_v3": decimal_type(38, 2),
                    },
                    {
                        "name": "decimal256",
                        "type_v3": decimal_type(76, 10),
                    },
                ],
                strict=True,
                unique_keys=False,
            )

            assert read_table("//tmp/tout") == [{
                "decimal32": encode_decimal("10.5", 9, 5),
                "decimal64": encode_decimal("100.005", 18, 10),
                "decimal128": encode_decimal("0.000005", 35, 15),
                "decimal128_no_yql": encode_decimal("15.43", 38, 2),
                "decimal256": encode_decimal("42.4242424242", 76, 10),
            }]

    # CHYT-896
    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_optional_null(self, optimize_for):
        schema = [{"name": "a", "type_v3": "null"}]
        create("table", "//tmp/t1", attributes={"schema": schema, "optimize_for": optimize_for})
        write_table("//tmp/t1", [{"a": yson.YsonEntity()}])

        schema = [{"name": "a", "type_v3": optional_type("null")}]
        create("table", "//tmp/t2", attributes={"schema": schema})
        concatenate(["//tmp/t1"], "//tmp/t2")

        assert get("//tmp/t1/@chunk_ids") == get("//tmp/t2/@chunk_ids")

        write_table("<append=%true>//tmp/t2", [{"a": [yson.YsonEntity()]}])

        with Clique(1) as clique:
            assert clique.make_query('select a from "//tmp/t2"') == [{"a": yson.YsonEntity()}] * 2

    @authors("dakovalkov")
    def test_low_cardinality(self):
        with Clique(1) as clique:
            clique.make_query("""
                create table "//tmp/t0" engine YtTable() as
                select
                    toLowCardinality(str) as lc_str,
                    toLowCardinality(nullable_str) as lc_nullable_str,
                    toLowCardinality(int) as lc_int,
                    toLowCardinality(nullable_int) as lc_nullable_int
                from system.one
                array join ['a', 'bc', 'a', 'cde'] as str,
                    [Null, 'ab', 'b', 'ab'] as nullable_str,
                    [1, 2, 3, 1] as int,
                    [0, 1, 1, Null] as nullable_int
            """, settings={"allow_suspicious_low_cardinality_types": 1})

            assert normalize_schema_v3(get("//tmp/t0/@schema")) == make_schema(
                [
                    {
                        "name": "lc_str",
                        "type_v3": "string",
                    },
                    {
                        "name": "lc_nullable_str",
                        "type_v3": optional_type("string"),
                    },
                    {
                        "name": "lc_int",
                        "type_v3": "uint8",
                    },
                    {
                        "name": "lc_nullable_int",
                        "type_v3": optional_type("uint8"),
                    },
                ],
                strict=True,
                unique_keys=False,
            )
            assert read_table("//tmp/t0") == [
                {"lc_str": "a", "lc_nullable_str": None, "lc_int": 1, "lc_nullable_int": 0},
                {"lc_str": "bc", "lc_nullable_str": "ab", "lc_int": 2, "lc_nullable_int": 1},
                {"lc_str": "a", "lc_nullable_str": "b", "lc_int": 3, "lc_nullable_int": 1},
                {"lc_str": "cde", "lc_nullable_str": "ab", "lc_int": 1, "lc_nullable_int": None},
            ]

            clique.make_query("""
                create table "//tmp/t1" engine YtTable() as
                select
                    CAST(['abc', 'bcd'], 'Array(LowCardinality(String))') as lc_arr,
                    CAST(tuple(1, 'abcd'), 'Tuple(LowCardinality(UInt8), LowCardinality(String))') as lc_tup
            """, settings={"allow_suspicious_low_cardinality_types": 1})

            assert normalize_schema_v3(get("//tmp/t1/@schema")) == make_schema(
                [
                    {
                        "name": "lc_arr",
                        "type_v3": list_type("string"),
                    },
                    {
                        "name": "lc_tup",
                        "type_v3": tuple_type(["uint8", "string"]),
                    },
                ],
                strict=True,
                unique_keys=False,
            )
            assert read_table("//tmp/t1") == [{"lc_arr": ["abc", "bcd"], "lc_tup": [1, "abcd"]}]
