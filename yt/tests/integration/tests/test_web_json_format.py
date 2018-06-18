from yt_commands import *
from yt_env_setup import YTEnvSetup, unix_only
import yt.yson as yson

import __builtin__
import json

class TestWebJsonFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    @unix_only
    def test_read_table(self):
        ROWS = [
            {
                "string32_column": "abcdefghij",
                "yson32_column": [110, "xxx", {"foo": "bar"}],
                "int64_column": -42,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 3.14,
                "boolean_column": True,
            },
            {
                "string32_column": "abcd",
                "yson32_column": {"f": "b"},
                "int64_column": -42,
                "uint64_column": yson.YsonUint64(25),
                "double_column": 3.14,
                "boolean_column": True,
            }
        ]

        COLUMNS_COUNT_LIMIT = max(__builtin__.map(len, ROWS))
        assert COLUMNS_COUNT_LIMIT >= 1

        create("table", "//tmp/table")
        write_table("//tmp/table", ROWS)

        format_ = yson.YsonString("web_json")
        format_.attributes["field_weight_limit"] = 7

        # do not slice columns
        format_.attributes["column_limit"] = COLUMNS_COUNT_LIMIT
        output = json.loads(read_table("//tmp/table", output_format=format_))

        EXPECTED_OUTPUT = {
            "rows": [
                {
                    "double_column": {
                        "$type": "double",
                        "$value": "3.14"
                    },
                    "uint64_column": {
                        "$type": "uint64",
                        "$value": "25"
                    },
                    "string32_column": {
                        "$incomplete": True,
                        "$type": "string",
                        "$value": "abcdefg"
                    },
                    "yson32_column": {
                        "$incomplete": True,
                        "$type": "any",
                        "$value": ""
                    },
                    "int64_column": {
                        "$type": "int64",
                        "$value": "-42"
                    },
                    "boolean_column": {
                        "$type": "boolean",
                        "$value": "true"
                    }
                },
                {
                    "double_column": {
                        "$type": "double",
                        "$value": "3.14"
                    },
                    "uint64_column": {
                        "$type": "uint64",
                        "$value": "25"
                    },
                    "string32_column": {
                        "$type": "string",
                        "$value": "abcd"
                    },
                    "yson32_column": {
                        "$incomplete": True,
                        "$type": "any",
                        "$value": ""
                    },
                    "int64_column": {
                        "$type": "int64",
                        "$value": "-42"
                    },
                    "boolean_column": {
                        "$type": "boolean",
                        "$value": "true"
                    }
                }
            ],
            "incomplete_columns": {
                "$type": "boolean",
                "$value": "false"
            }
        }

        assert output == EXPECTED_OUTPUT

        # slice columns
        format_.attributes["column_limit"] = COLUMNS_COUNT_LIMIT - 1
        output = json.loads(read_table("//tmp/table", output_format=format_))

        EXPECTED_INCOMPLETE_COLUMNS = {
            "$type": "boolean",
            "$value": "true"
        }

        assert "incomplete_columns" in output and output["incomplete_columns"] == EXPECTED_INCOMPLETE_COLUMNS
