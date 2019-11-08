from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait, skip_if_porto
from yt_commands import *

from yt.test_helpers import assert_items_equal, are_almost_equal

import pytest
import time

def with_name(skiff_type, name=None):
    if name is not None:
        skiff_type["name"] = name
    return skiff_type


def skiff_simple(wire_type, name=None):
    return with_name({
        "wire_type": wire_type,
    }, name)


def skiff_optional(inner, name=None):
    return with_name({
        "wire_type": "variant8",
        "children": [
            {
                "wire_type": "nothing",
            },
            inner
        ]
    }, name)


def skiff_tuple(children, name=None):
    return with_name({
        "wire_type": "tuple",
        "children": children,
    }, name)

def skiff_repeated_variant8(children, name=None):
    return with_name({
        "wire_type": "repeated_variant8",
        "children": children,
    }, name)


class TestSkiffFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("ermolovd")
    @unix_only
    def test_id_map(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [
            {
                "int64_column": -42,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 3.14,
                "boolean_column": True,
                "string32_column": "foo",
                "yson32_column": [110, "xxx", {"foo": "bar"}],
            },
            {
                "int64_column": -15,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 2.7,
                "boolean_column": False,
                "string32_column": "qux",
                "yson32_column": None,
            },
        ])

        create("table", "//tmp/t_out")

        format = yson.YsonString("skiff")
        format.attributes["table_skiff_schemas"] = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "int64_column",
                    },
                    {
                        "wire_type": "uint64",
                        "name": "uint64_column",
                    },
                    {
                        "wire_type": "double",
                        "name": "double_column",
                    },
                    {
                        "wire_type": "boolean",
                        "name": "boolean_column",
                    },
                    {
                        "wire_type": "string32",
                        "name": "string32_column",
                    },
                    {
                        "wire_type": "yson32",
                        "name": "yson32_column",
                    },
                ]
            }
        ]

        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "mapper":{
                    "format": format
                }
            })

        assert read_table("//tmp/t_in") == read_table("//tmp/t_out")

        format = yson.YsonString("skiff")
        format.attributes["table_skiff_schemas"] = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "yson32",
                        "name": "int64_column",
                    },
                    {
                        "wire_type": "yson32",
                        "name": "uint64_column",
                    },
                    {
                        "wire_type": "yson32",
                        "name": "double_column",
                    },
                    {
                        "wire_type": "yson32",
                        "name": "boolean_column",
                    },
                    {
                        "wire_type": "yson32",
                        "name": "string32_column",
                    },
                    {
                        "wire_type": "yson32",
                        "name": "yson32_column",
                    },
                ]
            }
        ]

        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "mapper": {
                    "format": format,
                }
            })

        assert read_table("//tmp/t_in") == read_table("//tmp/t_out")

        format = yson.YsonString("skiff")
        format.attributes["table_skiff_schemas"] = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "yson32",
                        "name": "$other_columns",
                    },
                ]
            }
        ]

        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "mapper": {
                    "format": format,
                }
            })

        assert read_table("//tmp/t_in") == read_table("//tmp/t_out")

    @authors("ermolovd")
    def test_id_map_complex_types(self):
        schema = [
            {
                "name": "list_of_strings",
                "type_v2": list_type("string"),
            },
            {
                "name": "optional_list_of_strings",
                "type_v2": optional_type(list_type("string")),
            },
            {
                "name": "optional_optional_boolean",
                "type_v2": optional_type(optional_type("boolean")),
            },
            {
                "name": "struct",
                "type_v2": struct_type([
                    ("key", "string"),
                    ("points", list_type(
                        struct_type([
                            ("x", "int64"),
                            ("y", "int64"),
                        ])
                    ))
                ]),
            },
        ]
        create("table", "//tmp/t_in", attributes={"schema": schema})
        write_table("//tmp/t_in", [
            {
                "list_of_strings": ["foo", "bar", "baz"],
                "optional_list_of_strings": None,
                "optional_optional_boolean": [False],
                "struct": {"key": "qux", "points": [{"x": 1, "y": 4}, {"x": 5, "y": 4}]},
            },
            {
                "list_of_strings": ["a", "bc"],
                "optional_list_of_strings": ["defg", "hi"],
                "optional_optional_boolean": [None],
                "struct": {"key": "lol", "points": []},
            }
        ])

        create("table", "//tmp/t_out", attributes={"schema": schema})

        format = yson.YsonString("skiff")
        format.attributes["table_skiff_schemas"] = [
            skiff_tuple([
                skiff_repeated_variant8([skiff_simple("string32")], name="list_of_strings"),
                skiff_optional(
                    skiff_repeated_variant8([skiff_simple("string32")]),
                    name="optional_list_of_strings"
                ),
                skiff_optional(skiff_optional(skiff_simple("boolean")), name="optional_optional_boolean"),
                skiff_tuple(
                    [
                        skiff_simple("string32", name="key"),
                        skiff_repeated_variant8([skiff_tuple([
                            skiff_simple("int64", name="x"),
                            skiff_simple("int64", name="y"),
                        ])], name="points")
                    ],
                    name="struct",
                )
            ])
        ]

        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="tee /dev/stderr",
            spec={
                "mapper": {
                    "format": format,
                }
            })

        assert read_table("//tmp/t_in") == read_table("//tmp/t_out")

    @authors("ermolovd")
    def test_read_complex_types(self):
        schema = [
            {
                "name": "list_of_strings",
                "type_v2": list_type("string"),
            },
            {
                "name": "optional_list_of_strings",
                "type_v2": optional_type(list_type("string")),
            },
            {
                "name": "optional_optional_boolean",
                "type_v2": optional_type(optional_type("boolean")),
            },
            {
                "name": "struct",
                "type_v2": struct_type([
                    ("key", "string"),
                    ("points", list_type(
                        struct_type([
                            ("x", "int64"),
                            ("y", "int64"),
                        ])
                    ))
                ]),
            },
        ]
        create("table", "//tmp/table1", attributes={"schema": schema})
        write_table("//tmp/table1", [
            {
                "list_of_strings": ["foo", "bar", "baz"],
                "optional_list_of_strings": None,
                "optional_optional_boolean": [False],
                "struct": {"key": "qux", "points": [{"x": 1, "y": 4}, {"x": 5, "y": 4}]},
            },
            {
                "list_of_strings": ["a", "bc"],
                "optional_list_of_strings": ["defg", "hi"],
                "optional_optional_boolean": [None],
                "struct": {"key": "lol", "points": []},
            }
        ])

        format = yson.YsonString("skiff")
        format.attributes["table_skiff_schemas"] = [
            skiff_tuple([
                skiff_repeated_variant8([skiff_simple("string32")], name="list_of_strings"),
                skiff_optional(
                    skiff_repeated_variant8([skiff_simple("string32")]),
                    name="optional_list_of_strings"
                ),
                skiff_optional(skiff_optional(skiff_simple("boolean")), name="optional_optional_boolean"),
                skiff_tuple(
                    [
                        skiff_simple("string32", name="key"),
                        skiff_repeated_variant8([skiff_tuple([
                            skiff_simple("int64", name="x"),
                            skiff_simple("int64", name="y"),
                        ])], name="points")
                    ],
                    name="struct",
                )
            ])
        ]
        skiff_dump = read_table("//tmp/table1", output_format=format)

        # Check that column name is not in our table dump.
        # It's simple check that read_table didn't return yson.
        assert schema[0]["name"] not in skiff_dump

        create("table", "//tmp/table2", attributes={"schema": schema})
        write_table("//tmp/table2", skiff_dump, is_raw=True, input_format=format)

        assert read_table("//tmp/table1") == read_table("//tmp/table2")
