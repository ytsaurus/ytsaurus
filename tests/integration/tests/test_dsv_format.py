#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yt_env_setup import YTEnvSetup, unix_only, patch_porto_env_only, wait, skip_if_porto
from yt_commands import *


class TestDsvFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("ermolovd")
    def test_ignore_unknown_types(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "int", "type_v3": "int64"},
                    {"name": "opt_string", "type_v3": optional_type("string")},
                    {"name": "list_string", "type_v3": list_type("string")},
                ]
            },
        )
        write_table("//tmp/t_in", [
            {
                "int": 53,
                "opt_string": "foobar",
                "list_string": ["a", "b", "c"],
            },
            {
                "int": 82,
                "opt_string": None,
                "list_string": ["foo", "bar"],
            },
        ])

        with raises_yt_error("are not supported by the chosen format"):
            read_table("//tmp/t_in", output_format="dsv")

        format = yson.YsonString("dsv")
        format.attributes["skip_unsupported_types"] = True
        dsv_dump = read_table("//tmp/t_in", output_format=format)
        assert "int=53\topt_string=foobar\t\nint=82\t\n" == dsv_dump


class TestYamredDsvFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("ermolovd")
    def test_ignore_unknown_types(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "int", "type_v3": "int64"},
                    {"name": "opt_string", "type_v3": optional_type("string")},
                    {"name": "list_string", "type_v3": list_type("string")},
                ]
            },
        )
        write_table("//tmp/t_in", [
            {
                "int": 53,
                "opt_string": "foobar",
                "list_string": ["a", "b", "c"],
            },
            {
                "int": 82,
                "opt_string": None,
                "list_string": ["foo", "bar"],
            },
        ])

        format = yson.YsonString("yamred_dsv")
        format.attributes["key_column_names"] = ["int"]
        with raises_yt_error("are not supported by the chosen format"):
            read_table("//tmp/t_in", output_format=format)

        format.attributes["skip_unsupported_types_in_value"] = True
        dsv_dump = read_table("//tmp/t_in", output_format=format)
        assert "53\topt_string=foobar\n82\t\n" == dsv_dump
