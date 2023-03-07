from yt_commands import *
from yt_env_setup import YTEnvSetup, unix_only

import yt.yson as yson

import pytest

import __builtin__
import base64
import collections
import copy
import json

Column = collections.namedtuple("Column", ["name", "type", "values", "expected", "complex"])

COLUMN_LIST = [
    Column(
        "string32_column",
        optional_type("string"),
        [
            "abcdefghij",
            "abcd",
        ],
        [
            {
                "$incomplete": True,
                "$type": "string",
                "$value": "abcdefg"
            },
            {
                "$type": "string",
                "$value": "abcd"
            },
        ],
        False),
    Column(
        "yson32_column",
        optional_type("yson"),
        [
            [110, "xxx", {"foo": "bar"}],
            "just a string",
        ],
        [
            {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            {
                "$incomplete": True,
                "$type": "string",
                "$value": "just a ",
            },
        ],
        False),
    Column(
        "int64_column",
        optional_type("int64"),
        [
            -42,
            -42,
        ],
        [
            {
                "$type": "int64",
                "$value": "-42"
            },
            {
                "$type": "int64",
                "$value": "-42"
            },
        ],
        False),
    Column(
        "uint64_column",
        optional_type("uint64"),
        [
            yson.YsonUint64(25),
            yson.YsonUint64(25),
        ],
        [
            {
                "$type": "uint64",
                "$value": "25"
            },
            {
                "$type": "uint64",
                "$value": "25"
            },
        ],
        False),
    Column(
        "double_column",
        optional_type("double"),
        [
            3.14,
            3.14,
        ],
        [
            {
                "$type": "double",
                "$value": "3.14"
            },
            {
                "$type": "double",
                "$value": "3.14"
            },
        ],
        False),
    Column(
        "boolean_column",
        optional_type("bool"),
        [
            True,
            True,
        ],
        [
            {
                "$type": "boolean",
                "$value": "true"
            },
            {
                "$type": "boolean",
                "$value": "true"
            },
        ],
        False),
    Column(
        "non_utf_string_column",
        optional_type("string"),
        [
            b"\xFF\xFE\xFD\xFC",
            b"ab\xFAcd",
        ],
        [
            {
                "$type": "string",
                "$value": u"\xFF\xFE\xFD\xFC"
            },
            {
                "$type": "string",
                "$value": u"ab\xFAcd"
            },
        ],
        False),
    Column(
        "optional_list_int64_column",
        optional_type(list_type("int64")),
        [
            [11, 12, 13, 14],
            yson.YsonEntity(),
        ],
        [
            {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            yson.YsonEntity(),
        ],
        True),
    Column(
        "variant_tuple_column",
        variant_tuple_type(["string", list_type("int8")]),
        [
            [0, "bar"],
            [1, [7, 8]],
        ],
        [
            {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
        ],
        True),
    Column(
        "struct_column",
        struct_type([
            ("a", "bool"),
            ("b", "uint64"),
            ("c", "string"),
            ("d", optional_type("null")),
        ]),
        [
            {"a": False, "b": yson.YsonUint64(1), "c": "flame", "d": yson.YsonEntity()},
            {"a": True, "b": yson.YsonUint64(12), "c": "quite long string", "d": [yson.YsonEntity()]},
        ],
        [
            {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
            {
                "$incomplete": True,
                "$type": "any",
                "$value": ""
            },
        ],
        True),
    Column(
        "list_optional_int64_column",
        list_type(optional_type("int64")),
        [
            [21, 22, 23],
            [100, yson.YsonEntity(), 200],
        ],
        [
            {
                "$type": "any",
                "$value": "",
                "$incomplete": True,
            },
            {
                "$type": "any",
                "$value": "",
                "$incomplete": True,
            },
        ],
        True),
]

DYNAMIC_ORDERED_TABLE_SYSTEM_COLUMN_NAMES = [
    "$row_index",
    "$tablet_index",
]


FIELD_WEIGHT_LIMIT = 20
STRING_WEIGHT_LIMIT = 5

# Instead of type indices this function returns types.
# Later they will be matched with actual rows using yql_type_registry.
def get_output_yql_rows():
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
                ["abcdefghij"],
                ["OptionalType", ["DataType", "String"]]
            ],
            "yson32_column": [
                [{"val": "", "inc": True}],
                ["OptionalType", ["DataType", "Yson"]]
            ],
            "int64_column": [
                ["-42"],
                ["OptionalType", ["DataType", "Int64"]]
            ],
            "uint64_column": [
                ["25"],
                ["OptionalType", ["DataType", "Uint64"]]
            ],
            "double_column": [
                ["3.14"],
                ["OptionalType", ["DataType", "Double"]]
            ],
            "boolean_column": [
                [True],
                ["OptionalType", ["DataType", "Boolean"]]
            ],
            "optional_list_int64_column": [
                [{"val": ["11", "12", "13"], "inc": True}],
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
                [{"val": base64.b64encode(b"\xFF\xFE\xFD\xFC"), "b64": True}],
                ["OptionalType", ["DataType", "String"]]
            ],
            "list_optional_int64_column": [
                {"val": [["21"], ["22"]], "inc": True},
                ["ListType", ["OptionalType", ["DataType", "Int64"]]],
            ],
        },
        {
            "string32_column": [
                ["abcd"],
                ["OptionalType", ["DataType", "String"]]
            ],
            "yson32_column": [
                [{"val": {"$type": "string", "$value": "just a string"}}],
                ["OptionalType", ["DataType", "Yson"]]
            ],
            "int64_column": [
                ["-42"],
                ["OptionalType", ["DataType", "Int64"]]
            ],
            "uint64_column": [
                ["25"],
                ["OptionalType", ["DataType", "Uint64"]]
            ],
            "double_column": [
                ["3.14"],
                ["OptionalType", ["DataType", "Double"]]
            ],
            "boolean_column": [
                [True],
                ["OptionalType", ["DataType", "Boolean"]]
            ],
            "optional_list_int64_column": [
                yson.YsonEntity(),
                optional_list_int64_type
            ],
            "variant_tuple_column": [
                [
                    "1",
                    {"val": ["7", "8"]}
                ],
                variant_tuple_type
            ],
            "struct_column": [
                [True, "12", {"val": "quite", "inc": True}, [yson.YsonEntity()]],
                struct_type
            ],
            "non_utf_string_column": [
                [{"val": base64.b64encode(b"ab\xFAcd"), "b64": True}],
                ["OptionalType", ["DataType", "String"]]
            ],
            "list_optional_int64_column": [
                {"val": [["100"], yson.YsonEntity()], "inc": True},
                ["ListType", ["OptionalType", ["DataType", "Int64"]]],
            ],
        },
    ]

TABLE_PATH = "//tmp/table"

def get_column_names(dynamic=False, ordered=False):
    result = [c["name"] for c in get_schema(dynamic)]
    if dynamic and ordered:
        result += DYNAMIC_ORDERED_TABLE_SYSTEM_COLUMN_NAMES
    return result

def get_rows(dynamic=False):
    res = [{}, {}]
    for column in COLUMN_LIST:
        if dynamic and column.complex:
            continue
        for i in xrange(2):
            res[i][column.name] = column.values[i]
    return res

def get_expected_output(dynamic=False, ordered=False):
    rows = [{}, {}]
    for column in COLUMN_LIST:
        if dynamic and column.complex:
            continue
        for i in xrange(2):
            rows[i][column.name] = column.expected[i]

    if dynamic:
        # insert_rows actually writes value with type "any".
        rows[1]["yson32_column"] = {
            "$incomplete": True,
            "$type": "any",
            "$value": "",
        }

    return {
        "rows": rows,
        "incomplete_columns": "false",
        "all_column_names": get_expected_all_column_names(dynamic, ordered),
        "incomplete_all_column_names": "false"
    }


def get_schema(dynamic=False, key_column_names=(), **kwargs):
    schema = []
    for column in COLUMN_LIST:
        if dynamic and column.complex:
            continue

        column_schema = {
            "name": column.name,
            "type_v3": column.type,
        }

        if column.name in key_column_names:
            column_schema["sort_order"] = "ascending"
        schema.append(column_schema)
    return make_schema(schema, **kwargs)

def get_expected_all_column_names(dynamic=False, ordered=False):
    result = get_column_names(dynamic, ordered)
    result.sort()
    return result

def get_web_json_format(field_weight_limit, column_limit, value_format=None, string_weight_limit=None):
    format_ = yson.YsonString("web_json")
    format_.attributes["field_weight_limit"] = field_weight_limit
    format_.attributes["max_selected_column_count"] = column_limit
    if value_format is not None:
        format_.attributes["value_format"] = value_format
    if string_weight_limit is not None:
        format_.attributes["string_weight_limit"] = string_weight_limit
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
        if expected_type == ["OptionalType", ["DataType", "Double"]]:
            assert abs(float(expected_value[0]) - float(actual_value[0])) < 1e-6
        elif expected_type == ["DataType", "Double"]:
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
        write_table(TABLE_PATH, get_rows())

        column_names = get_column_names()
        assert len(column_names) > 0

        # Do not slice columns.
        format_ = get_web_json_format(7, len(column_names))
        output = json.loads(read_table(TABLE_PATH, output_format=format_))

        expected_output = get_expected_output()
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

        rows = get_rows()
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
        schema = get_schema(dynamic=True, key_column_names=["string32_column"], unique_keys=True, strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})
        sync_mount_table(TABLE_PATH)

        insert_rows(TABLE_PATH, get_rows(dynamic=True))

        column_names = get_column_names(dynamic=True)

        format_ = get_web_json_format(7, len(column_names))
        query = get_dynamic_table_select_query(column_names, TABLE_PATH)
        output = json.loads(select_rows(query, output_format=format_))

        expected_output = get_expected_output(dynamic=True)
        expected_output["rows"].sort(key=lambda c: c["string32_column"]["$value"])
        print(output)
        print(expected_output)
        assert output == expected_output

        sync_unmount_table(TABLE_PATH)

    @authors("ignat")
    @unix_only
    def test_select_rows_from_sorted_dynamic_table_with_duplicate_columns(self):
        sync_create_cells(1)
        schema = get_schema(dynamic=True, key_column_names=["string32_column"], unique_keys=True, strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})
        sync_mount_table(TABLE_PATH)

        insert_rows(TABLE_PATH, get_rows(dynamic=True))

        column_names = get_column_names() * 2

        format_ = get_web_json_format(7, len(column_names))
        query = get_dynamic_table_select_query(column_names, TABLE_PATH)
        with pytest.raises(YtError):
            json.loads(select_rows(query, output_format=format_))

        sync_unmount_table(TABLE_PATH)

    @authors("bidzilya")
    @unix_only
    def test_select_rows_from_ordered_dynamic_table(self):
        sync_create_cells(1)
        schema = get_schema(dynamic=True)
        create("table", TABLE_PATH, attributes={"schema": schema, "dynamic": True})

        rows = get_rows(True)
        for i in range(len(rows)):
            rows[i]["$tablet_index"] = i
        reshard_table(TABLE_PATH, len(rows))

        sync_mount_table(TABLE_PATH)

        insert_rows(TABLE_PATH, rows)

        column_names = get_column_names(dynamic=True, ordered=True)

        format_ = get_web_json_format(7, len(column_names))
        query = get_dynamic_table_select_query(column_names, TABLE_PATH)
        output = json.loads(select_rows(query, output_format=format_))

        expected_output = get_expected_output(True, ordered=True)
        for i in range(len(rows)):
            expected_output["rows"][i]["$$tablet_index"] = {
                "$type": "int64",
                "$value": str(i),
            }
            expected_output["rows"][i]["$$row_index"] = {
                "$type": "int64",
                "$value": "0"
            }
        output["rows"].sort(key=lambda column: (column["$$tablet_index"], column["$$row_index"]))
        assert output == expected_output

        sync_unmount_table(TABLE_PATH)

    @authors("levysotsky")
    @pytest.mark.parametrize("value_format", ["yql"])
    def test_yql_value_formats(self, value_format):
        schema = get_schema(strict=True)
        create("table", TABLE_PATH, attributes={"schema": schema})
        write_table(TABLE_PATH, get_rows())

        column_names = get_column_names()
        assert len(column_names) > 0

        format_ = get_web_json_format(
            FIELD_WEIGHT_LIMIT,
            len(column_names),
            value_format=value_format,
            string_weight_limit=STRING_WEIGHT_LIMIT,
        )
        output = json.loads(read_table(TABLE_PATH, output_format=format_))

        assert "yql_type_registry" in output
        type_registry = output["yql_type_registry"]
        expected_output_rows = get_output_yql_rows()

        assert len(expected_output_rows) == len(output["rows"])
        for actual_row, expected_row in zip(output["rows"], expected_output_rows):
            _assert_yql_row_match(actual_row, expected_row, type_registry)

        assert output["incomplete_columns"] == "false"
        assert output["incomplete_all_column_names"] == "false"
        assert output["all_column_names"] == get_expected_all_column_names()
