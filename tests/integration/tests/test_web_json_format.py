from yt_commands import *
from yt_env_setup import YTEnvSetup, unix_only
import yt.yson as yson

import __builtin__
import copy
import json


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

SCHEMA_BASE = [
    {
        "name": "string32_column",
        "type": "string",
    },
    {
        "name": "yson32_column",
        "type": "any",
    },
    {
        "name": "int64_column",
        "type": "int64",
    },
    {
        "name": "uint64_column",
        "type": "uint64",
    },
    {
        "name": "double_column",
        "type": "double",
    },
    {
        "name": "boolean_column",
        "type": "boolean",
    },
]

DYNAMIC_ORDERED_TABLE_SYSTEM_COLUMN_NAMES = [
    "$row_index",
    "$tablet_index",
]

EXPECTED_OUTPUT_BASE = {
    "rows": [
        {
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
            "uint64_column": {
                "$type": "uint64",
                "$value": "25"
            },
            "double_column": {
                "$type": "double",
                "$value": "3.14"
            },
            "boolean_column": {
                "$type": "boolean",
                "$value": "true"
            }
        },
        {
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
            "uint64_column": {
                "$type": "uint64",
                "$value": "25"
            },
            "double_column": {
                "$type": "double",
                "$value": "3.14"
            },
            "boolean_column": {
                "$type": "boolean",
                "$value": "true"
            }
        },
    ],
    "incomplete_columns": "false"
}

TABLE_PATH = "//tmp/table"


def get_column_names(dynamic_ordered):
    result = list(__builtin__.map(lambda c: c["name"], SCHEMA_BASE))
    if dynamic_ordered:
        result += DYNAMIC_ORDERED_TABLE_SYSTEM_COLUMN_NAMES
    return result

def get_schema(**kwargs):
    kwargs.setdefault("key_column_names", [])
    key_column_names = kwargs.pop("key_column_names")

    schema_base = copy.deepcopy(SCHEMA_BASE)
    for column in schema_base:
        if column["name"] in key_column_names:
            column["sort_order"] = "ascending"

    return make_schema(schema_base, **kwargs)

def get_expected_all_column_names(dynamic_ordered):
    result = get_column_names(dynamic_ordered)
    result.sort()
    return result

def get_web_json_format(field_weight_limit, column_limit):
    format_ = yson.YsonString("web_json")
    format_.attributes["field_weight_limit"] = field_weight_limit
    format_.attributes["column_limit"] = column_limit
    return format_

def get_dynamic_table_select_query(column_names, table_path):
    def wrap_system_column(column_name):
        if column_name.startswith("$"):
            column_name = "[" + column_name + "]"
        return column_name
    columns_selector = ", ".join(__builtin__.map(wrap_system_column, column_names))
    return "{} FROM [{}]".format(columns_selector, table_path)


class TestWebJsonFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    USE_DYNAMIC_TABLES = True

    @unix_only
    def test_read_table(self):
        create("table", TABLE_PATH)
        write_table(TABLE_PATH, ROWS)

        column_names = get_column_names(dynamic_ordered=False)
        assert len(column_names) > 0

        # Do not slice columns.
        format_ = get_web_json_format(7, len(column_names))
        output = json.loads(read_table(TABLE_PATH, output_format=format_))

        expected_output = copy.deepcopy(EXPECTED_OUTPUT_BASE)
        expected_output["all_column_names"] = get_expected_all_column_names(dynamic_ordered=False)
        assert output == expected_output

        # Slice columns.
        format_ = get_web_json_format(7, len(column_names) - 1)
        output = json.loads(read_table(TABLE_PATH, output_format=format_))

        assert "incomplete_columns" in output and output["incomplete_columns"] == "true"

    @unix_only
    def test_select_rows_from_sorted_dynamic_table(self):
        self.sync_create_cells(1)
        schema = get_schema(key_column_names=["string32_column"], unique_keys=True, strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})
        self.sync_mount_table(TABLE_PATH)

        insert_rows(TABLE_PATH, ROWS)

        column_names = get_column_names(dynamic_ordered=False)

        format_ = get_web_json_format(7, len(column_names))
        query = get_dynamic_table_select_query(column_names, TABLE_PATH)
        output = json.loads(select_rows(query, output_format=format_))

        expected_output = copy.deepcopy(EXPECTED_OUTPUT_BASE)
        expected_output["all_column_names"] = get_expected_all_column_names(dynamic_ordered=False)
        expected_output["rows"].sort(key=lambda c: c["string32_column"]["$value"])
        assert output == expected_output

        self.sync_unmount_table(TABLE_PATH)

    @unix_only
    def test_select_rows_from_ordered_dynamic_table(self):
        self.sync_create_cells(1)
        schema = get_schema(strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})

        rows = copy.deepcopy(ROWS)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = i
        reshard_table(TABLE_PATH, len(rows))

        self.sync_mount_table(TABLE_PATH)

        insert_rows(TABLE_PATH, rows)

        column_names = get_column_names(dynamic_ordered=True)

        format_ = get_web_json_format(7, len(column_names))
        query = get_dynamic_table_select_query(column_names, TABLE_PATH)
        output = json.loads(select_rows(query, output_format=format_))

        expected_output = copy.deepcopy(EXPECTED_OUTPUT_BASE)
        for i in range(len(rows)):
            expected_output["rows"][i]["$$tablet_index"] = {
                "$type": "int64",
                "$value": str(i),
            }
            expected_output["rows"][i]["$$row_index"] = {
                "$type": "int64",
                "$value": "0"
            }
        expected_output["all_column_names"] = get_expected_all_column_names(dynamic_ordered=True)
        output["rows"].sort(key=lambda column: (column["$$tablet_index"], column["$$row_index"]))
        assert output == expected_output

        self.sync_unmount_table(TABLE_PATH)
