from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

from yt.test_helpers import assert_items_equal, are_almost_equal

import pytest

class TestProtobufFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @unix_only
    def test_id_map(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [
            {
                "int64_column": -42,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 3.14,
                "bool_column": True,
                "string_column": "foo",
                "any_column": [110, "xxx", {"foo": "bar"}],
            },
            {
                "int64_column": -15,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 2.7,
                "bool_column": False,
                "string_column": "qux",
                "any_column": 234,
            },
        ])

        create("table", "//tmp/t_out")

        format = yson.YsonString("protobuf")
        format.attributes["tables"] = [
            {
                "columns": [
                    {
                        "name": "int64_column",
                        "field_number": 1,
                        "proto_type": "int64",
                    },
                    {
                        "name": "uint64_column",
                        "field_number": 2,
                        "proto_type": "uint64",
                    },
                    {
                        "name": "double_column",
                        "field_number": 3,
                        "proto_type": "double",
                    },
                    {
                        "name": "bool_column",
                        "field_number": 4,
                        "proto_type": "bool",
                    },
                    {
                        "name": "string_column",
                        "field_number": 5,
                        "proto_type": "string",
                    },
                    {
                        "name": "any_column",
                        "field_number": 6,
                        "proto_type": "any",
                    },
                ],
            },
        ]

        map(in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "mapper":{
                    "format": format
                }
            })

        protobuf_dump = read_table("//tmp/t_in", output_format=format)

        # Check that column name is not in our table dump.
        # It's simple check that read_table didn't return yson.
        assert format.attributes["tables"][0]["columns"][0]["name"] not in protobuf_dump

        assert read_table("//tmp/t_in") == read_table("//tmp/t_out")

        format = yson.YsonString("protobuf")
        format.attributes["tables"] = [
            {
                "columns": [
                    {
                        "name": "int64_column",
                        "field_number": 1,
                        "proto_type": "any",
                    },
                    {
                        "name": "uint64_column",
                        "field_number": 2,
                        "proto_type": "any",
                    },
                    {
                        "name": "double_column",
                        "field_number": 3,
                        "proto_type": "any",
                    },
                    {
                        "name": "bool_column",
                        "field_number": 4,
                        "proto_type": "any",
                    },
                    {
                        "name": "string_column",
                        "field_number": 5,
                        "proto_type": "any",
                    },
                    {
                        "name": "any_column",
                        "field_number": 6,
                        "proto_type": "any",
                    },
                ],
            },
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

        format = yson.YsonString("protobuf")
        format.attributes["tables"] = [
            {
                "columns": [
                    {
                        "name": "other_columns",
                        "field_number": 1,
                        "proto_type": "other_columns",
                    },
                ],
            },
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

    @unix_only
    def test_id_map_reduce(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [
            {
                "int64_column": -42,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 3.14,
                "bool_column": True,
                "string_column": "foo",
                "any_column": [110, "xxx", {"foo": "bar"}],
            },
            {
                "int64_column": -15,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 2.7,
                "bool_column": False,
                "string_column": "qux",
                "any_column": 234,
            },
        ])

        create("table", "//tmp/t_out")

        format = yson.YsonString("protobuf")
        format.attributes["tables"] = [
            {
                "columns": [
                    {
                        "name": "int64_column",
                        "field_number": 1,
                        "proto_type": "int64",
                    },
                    {
                        "name": "uint64_column",
                        "field_number": 2,
                        "proto_type": "uint64",
                    },
                    {
                        "name": "double_column",
                        "field_number": 3,
                        "proto_type": "double",
                    },
                    {
                        "name": "bool_column",
                        "field_number": 4,
                        "proto_type": "bool",
                    },
                    {
                        "name": "string_column",
                        "field_number": 5,
                        "proto_type": "string",
                    },
                    {
                        "name": "any_column",
                        "field_number": 6,
                        "proto_type": "any",
                    },
                ],
            },
        ]

        map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by=["int64_column"],
            sort_by=["int64_column"],
            spec={
                "mapper": {
                    "format": format,
                    "command": "cat",
                },
                "reducer": {
                    "format": format,
                    "command": "cat",
                }
            },
        )

        protobuf_dump = read_table("//tmp/t_in", output_format=format)

        # Check that column name is not in our table dump.
        # It's simple check that read_table didn't return yson.
        assert format.attributes["tables"][0]["columns"][0]["name"] not in protobuf_dump

        assert_items_equal(read_table("//tmp/t_in"), read_table("//tmp/t_out"))
