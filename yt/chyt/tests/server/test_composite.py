from base import ClickHouseTestBase, Clique

from helpers import get_schema_from_description

from yt_commands import (write_table, authors, create)


class TestComposite(ClickHouseTestBase):
    def setup(self):
        self._setup()

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
                {"a": [["k1", "v1"], ["k2", "v2"]]}
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
                {"a": ["foo", 42]}
            ]
            assert clique.make_query("select a.1 as s, a.2 as i from `//tmp/t`") == [
                {"s": "foo", "i": 42}
            ]
            # TODO(max42): CHYT-529.
            # assert clique.make_query("select a.1, a.2 from `//tmp/t`") == [
            #     {"a.s": "foo", "a.i": 42}
            # ]
            assert clique.make_query("select untuple(a) from `//tmp/t`") == [
                {"s": "foo", "i": 42}
            ]
