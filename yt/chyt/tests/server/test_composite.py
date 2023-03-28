from base import ClickHouseTestBase, Clique, QueryFailedError

from helpers import get_schema_from_description

from yt_commands import (write_table, authors, create, get, raises_yt_error, read_table, concatenate)

from yt_type_helpers import (decimal_type, optional_type, make_schema, normalize_schema_v3)

from decimal_helpers import encode_decimal

import yt.yson as yson

import pytest


class TestComposite(ClickHouseTestBase):
    @authors("max42")
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
                {"name": "a", "type": "Array(Tuple(key String, value String))"},
            ]
            assert clique.make_query("select toTypeName(a) as ta from `//tmp/t` limit 1") == [
                {"ta": "Array(Tuple(key String, value String))"}
            ]
            assert clique.make_query("select * from `//tmp/t`") == [
                {"a": []},
                {"a": [{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}]}
            ]
            assert clique.make_query(
                "select arrayMap(item -> toTypeName(item), a) as ti from `//tmp/t[#1]` limit 1") == [
                    {"ti": ["Tuple(key String, value String)"] * 2}
                ]
            assert clique.make_query(
                "select arrayMap(item -> tupleElement(item, 'value'), a) as values "
                "from `//tmp/t`") == [
                    {"values": []},
                    {"values": ["v1", "v2"]}
                ]
            assert clique.make_query(
                "select tupleElement(arrayFirst(item -> tupleElement(item, 'key') == 'k1', a), 'value') as v1 "
                "from `//tmp/t`") == [
                    {"v1": ""},
                    {"v1": "v1"}
                ]

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
            assert get_schema_from_description(clique.make_query('describe `//tmp/t`')) == [
                {"name": "a", "type": "Tuple(s String, i Int64)"},
            ]
            assert clique.make_query("select toTypeName(a) as ta from `//tmp/t`") == [
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
        ]})

        row = {
            "decimal32": encode_decimal("1.1", 9, 2),
            "decimal64": encode_decimal("1234.1234", 15, 5),
            "decimal128": encode_decimal("123456789.123456789", 30, 10),
        }
        write_table("//tmp/t", [row])

        with Clique(1) as clique:
            assert get_schema_from_description(clique.make_query('describe "//tmp/t"')) == [
                {"name": "decimal32", "type": "Decimal(9, 2)"},
                {"name": "decimal64", "type": "Decimal(15, 5)"},
                {"name": "decimal128", "type": "Decimal(30, 10)"},
            ]

            assert clique.make_query('select * from "//tmp/t"') == [{
                "decimal32": 1.1,
                "decimal64": 1234.1234,
                "decimal128": 123456789.123456789,
            }]

            create_query = '''
                create table "//tmp/tout" engine YtTable() as select
                    toDecimal32(10.5, 5) as decimal32,
                    toDecimal64(100.005, 10) as decimal64,
                    CAST(0.000005, 'Decimal(35, 15)') as decimal128'''

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
                ],
                strict=True,
                unique_keys=False,
            )

            assert read_table("//tmp/tout") == [{
                "decimal32": encode_decimal("10.5", 9, 5),
                "decimal64": encode_decimal("100.005", 18, 10),
                "decimal128": encode_decimal("0.000005", 35, 15),
            }]

            # Max precision for Decimal128 in YT is 35 (because of YQL-compatibility).
            # In CH it's 38, so not all values are acceptable.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t_bad" engine YtTable() as select toDecimal128(1.0, 10) as d128')

            # Decimal256 is not supported in YT.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t_bad" engine YtTable() as select toDecimal256(1.0, 10) as d256')

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
