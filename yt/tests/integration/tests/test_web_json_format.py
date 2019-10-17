from yt_commands import *
from yt_env_setup import YTEnvSetup, unix_only

import yt.yson as yson

import pytest

import __builtin__
import base64
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
        "optional_list_int64_column": [11, 12, 13],
        "variant_tuple_column": [0, "bar"],
        "struct_column": {"a": False, "b": yson.YsonUint64(1), "c": "flame", "d": yson.YsonEntity()},
        "non_utf_string_column": b"\xFF\xFE\xFD\xFC",
    },
    {
        "string32_column": "abcd",
        "yson32_column": {"f": "b"},
        "int64_column": -42,
        "uint64_column": yson.YsonUint64(25),
        "double_column": 3.14,
        "boolean_column": True,
        "optional_list_int64_column": yson.YsonEntity(),
        "variant_tuple_column": [1, [7, 8]],
        "struct_column": {"a": True, "b": yson.YsonUint64(12), "c": "hey", "d": [yson.YsonEntity()]},
        "non_utf_string_column": b"ab\xFAcd",
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
    {
        "name": "optional_list_int64_column",
        "type_v2": {
            "metatype": "optional",
            "element": {
                "metatype": "list",
                "element": "int64"
            }
        }
    },
    {
        "name": "variant_tuple_column",
        "type_v2": {
            "metatype": "variant_tuple",
            "elements": [
                "string",
                {
                    "metatype": "list",
                    "element": "int8"
                },
            ]
        }
    },
    {
        "name": "struct_column",
        "type_v2": {
            "metatype": "struct",
            "fields": [
                {
                    "name": "a",
                    "type": "boolean"
                },
                {
                    "name": "b",
                    "type": "uint64"
                },
                {
                    "name": "c",
                    "type": "string"
                },
                {
                    "name": "d",
                    "type": {
                        "metatype": "optional",
                        "element": "null"
                    }
                },
            ]
        }
    },
    {
        "name": "non_utf_string_column",
        "type": "string"
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
            },
            "optional_list_int64_column": {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            "variant_tuple_column": {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            "struct_column": {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            "non_utf_string_column": {
                "$type": "string",
                "$value": u"\xFF\xFE\xFD\xFC"
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
            },
            "optional_list_int64_column": yson.YsonEntity(),
            "variant_tuple_column": {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            "struct_column": {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            "non_utf_string_column": {
                "$type": "string",
                "$value": u"ab\xFAcd"
            }
        },
    ],
    "incomplete_columns": "false",
    "incomplete_all_column_names": "false"
}


# Instead of type indices this function returns types.
# Later they will be matched with actual rows using yql_type_registry.
#
# NB: field_weight_limit is currently ignored!
def get_output_yql_rows(value_format, field_weight_limit):
    def make_string_entry(string):
        assert value_format == "yql"
        return string

    def make_base64_encoded_string_entry(string):
        encoded = base64.b64encode(string)
        assert value_format == "yql"
        return [encoded]

    optional_list_int64_type = ["OptionalType", ["ListType", ["DataType", "Int64"]]]
    variant_tuple_type = ["VariantType", ["TupleType", [
        ["DataType", "String"],
        ["ListType", ["DataType", "Int8"]],
    ]]]
    struct_type = ["StructType", [
        ["a", ["DataType", "Boolean"]],
        ["b", ["DataType", "Uint64"]],
        ["c", ["DataType", "String"]],
        ["d", ["OptionalType", ["NullType"]]],
    ]]

    return [
        {
            "string32_column": [
                make_string_entry("abcdefghij"),
                ["OptionalType", ["DataType", "String"]]
            ],
            "yson32_column": [
                make_string_entry("[110;\"xxx\";{\"foo\"=\"bar\";};]"),
                ["OptionalType", ["DataType", "Yson"]]
            ],
            "int64_column": [
                "-42",
                ["OptionalType", ["DataType", "Int64"]]
            ],
            "uint64_column": [
                "25",
                ["OptionalType", ["DataType", "Uint64"]]
            ],
            "double_column": [
                "3.14",
                ["OptionalType", ["DataType", "Double"]]
            ],
            "boolean_column": [
                True,
                ["OptionalType", ["DataType", "Boolean"]]
            ],
            "optional_list_int64_column": [
                [["11", "12", "13"]],
                optional_list_int64_type
            ],
            "variant_tuple_column": [
                ["0", "bar"],
                variant_tuple_type
            ],
            "struct_column": [
                [False, "1", "flame", yson.YsonEntity()],
                struct_type
            ],
            "non_utf_string_column": [
                make_base64_encoded_string_entry(b"\xFF\xFE\xFD\xFC"),
                ["OptionalType", ["DataType", "String"]]
            ],
        },
        {
            "string32_column": [
                "abcd",
                ["OptionalType", ["DataType", "String"]]
            ],
            "yson32_column": [
                make_string_entry("{\"f\"=\"b\";}"),
                ["OptionalType", ["DataType", "Yson"]]
            ],
            "int64_column": [
                "-42",
                ["OptionalType", ["DataType", "Int64"]]
            ],
            "uint64_column": [
                "25",
                ["OptionalType", ["DataType", "Uint64"]]
            ],
            "double_column": [
                "3.14",
                ["OptionalType", ["DataType", "Double"]]
            ],
            "boolean_column": [
                True,
                ["OptionalType", ["DataType", "Boolean"]]
            ],
            "optional_list_int64_column": [
                yson.YsonEntity(),
                optional_list_int64_type
            ],
            "variant_tuple_column": [
                [
                    "1",
                    ["7", "8"]
                ],
                variant_tuple_type
            ],
            "struct_column": [
                [True, "12", "hey", [yson.YsonEntity()]],
                struct_type
            ],
            "non_utf_string_column": [
                make_base64_encoded_string_entry(b"ab\xFAcd"),
                ["OptionalType", ["DataType", "String"]]
            ],
        },
    ]

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

def get_web_json_format(field_weight_limit, column_limit, value_format=None):
    format_ = yson.YsonString("web_json")
    format_.attributes["field_weight_limit"] = field_weight_limit
    format_.attributes["max_selected_column_count"] = column_limit
    if value_format is not None:
        format_.attributes["value_format"] = value_format
    return format_

def get_dynamic_table_select_query(column_names, table_path):
    def wrap_system_column(column_name):
        if column_name.startswith("$"):
            column_name = "[" + column_name + "]"
        return column_name
    columns_selector = ", ".join(__builtin__.map(wrap_system_column, column_names))
    return "{} FROM [{}]".format(columns_selector, table_path)

def _assert_yql_row_match(actual_row, expected_row, type_registry):
    assert __builtin__.set(actual_row.keys()) == __builtin__.set(expected_row.keys())
    for key, entry in actual_row.items():
        assert isinstance(entry, list)
        assert len(entry) == 2
        actual_value, actual_type_index_str = entry
        expected_value, expected_type = expected_row[key]
        if expected_type in (["OptionalType", ["DataType", "Double"]], ["DataType", "Double"]):
            assert abs(float(expected_value) - float(actual_value)) < 1e-6
        else:
            assert expected_value == actual_value
        actual_type = type_registry[int(actual_type_index_str)]
        assert expected_type == actual_type


class TestWebJsonFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    USE_DYNAMIC_TABLES = True

    @authors("bidzilya")
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

    # NB! Expect to have |null| value in the output for every schema column not presented in the data.
    @authors("bidzilya")
    @unix_only
    def test_read_table_schema_column_null_values(self):
        schema = get_schema(strict=False)
        create("table", TABLE_PATH, attributes={"schema": schema})

        missed_schema_column = "int64_column"

        rows = copy.deepcopy(ROWS)
        for row in rows:
            row.pop(missed_schema_column)
        write_table(TABLE_PATH, rows)

        format_ = get_web_json_format(100500, 100500)
        output = json.loads(read_table(TABLE_PATH, output_format=format_))

        assert len(output["rows"]) > 0
        assert all(missed_schema_column in row for row in output["rows"])
        assert missed_schema_column in output["all_column_names"]

    @authors("bidzilya")
    @unix_only
    def test_select_rows_from_sorted_dynamic_table(self):
        sync_create_cells(1)
        schema = get_schema(key_column_names=["string32_column"], unique_keys=True, strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})
        sync_mount_table(TABLE_PATH)

        insert_rows(TABLE_PATH, ROWS)

        column_names = get_column_names(dynamic_ordered=False)

        format_ = get_web_json_format(7, len(column_names))
        query = get_dynamic_table_select_query(column_names, TABLE_PATH)
        output = json.loads(select_rows(query, output_format=format_))

        expected_output = copy.deepcopy(EXPECTED_OUTPUT_BASE)
        expected_output["all_column_names"] = get_expected_all_column_names(dynamic_ordered=False)
        expected_output["rows"].sort(key=lambda c: c["string32_column"]["$value"])
        assert output == expected_output

        sync_unmount_table(TABLE_PATH)

    @authors("ignat")
    @unix_only
    def test_select_rows_from_sorted_dynamic_table_with_duplicate_columns(self):
        sync_create_cells(1)
        schema = get_schema(key_column_names=["string32_column"], unique_keys=True, strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})
        sync_mount_table(TABLE_PATH)

        insert_rows(TABLE_PATH, ROWS)

        column_names = get_column_names(dynamic_ordered=False) * 2

        format_ = get_web_json_format(7, len(column_names))
        query = get_dynamic_table_select_query(column_names, TABLE_PATH)
        with pytest.raises(YtError):
            json.loads(select_rows(query, output_format=format_))

        sync_unmount_table(TABLE_PATH)

    @authors("bidzilya")
    @unix_only
    def test_select_rows_from_ordered_dynamic_table(self):
        sync_create_cells(1)
        schema = get_schema(strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})

        rows = copy.deepcopy(ROWS)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = i
        reshard_table(TABLE_PATH, len(rows))

        sync_mount_table(TABLE_PATH)

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

        sync_unmount_table(TABLE_PATH)

    @authors("levysotsky")
    @pytest.mark.parametrize("value_format", ["yql"])
    def test_yql_value_formats(self, value_format):
        schema = get_schema(strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema})
        write_table(TABLE_PATH, ROWS)

        column_names = get_column_names(dynamic_ordered=False)
        assert len(column_names) > 0

        field_weight_limit = 9
        format_ = get_web_json_format(field_weight_limit, len(column_names), value_format=value_format)
        output = json.loads(read_table(TABLE_PATH, output_format=format_))

        assert "yql_type_registry" in output
        type_registry = output["yql_type_registry"]
        expected_output_rows = get_output_yql_rows(value_format, field_weight_limit)

        assert len(expected_output_rows) == len(output["rows"])
        for actual_row, expected_row in zip(output["rows"], expected_output_rows):
            _assert_yql_row_match(actual_row, expected_row, type_registry)

        assert not output["incomplete_columns"]
        assert not output["incomplete_all_column_names"]
        assert output["all_column_names"] == get_expected_all_column_names(dynamic_ordered=False)
