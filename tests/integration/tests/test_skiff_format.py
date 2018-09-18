from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait, skip_if_porto
from yt_commands import *

from yt.test_helpers import assert_items_equal, are_almost_equal

import pytest
import time

class TestSkiffFormat(YTEnvSetup):
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
            format=format)

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
            format=format)

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
            format=format)

        assert read_table("//tmp/t_in") == read_table("//tmp/t_out")
