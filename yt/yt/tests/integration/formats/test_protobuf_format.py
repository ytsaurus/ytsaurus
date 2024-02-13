from . import protobuf_format

from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, insert_rows, read_table, write_table, map, map_reduce, sync_create_cells,
    sync_mount_table, sync_unmount_table,
    sync_reshard_table, make_random_string)

from yt_type_helpers import optional_type, list_type, dict_type, struct_type, variant_struct_type, make_schema, make_column

from yt.test_helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson
import pytest

from copy import deepcopy
import random

##################################################################


def assert_rowsets_equal(first, second):
    assert len(first) == len(second)
    for i, (row_first, row_second) in enumerate(zip(first, second)):
        assert row_first == row_second, "Mismatch at index {}".format(i)


def create_protobuf_format(configs, enumerations={}):
    return yson.to_yson_type("protobuf", attributes={
        "tables": configs,
        "enumerations": enumerations,
    })


def create_protobuf_descriptor_format(file_descriptor_set_text: str, type_names: list[str]):
    return yson.to_yson_type("protobuf", attributes={
        "file_descriptor_set_text": file_descriptor_set_text,
        "type_names": type_names,
    })


ENUMERATIONS = {
    "MyEnum": {
        "Red": 12,
        "Yellow": 2,
        "Green": -42,
    },
}

SCHEMALESS_TABLE_ROWS = [
    {
        "int64_column": -42,
        "uint64_column": yson.YsonUint64(25),
        "double_column": 3.14,
        "bool_column": True,
        "string_column": "foo",
        "any_column": [110, "xxx", {"foo": "bar"}],
        "enum_string_column": "Red",
        "enum_int_column": 12,
    },
    {
        "int64_column": -15,
        "uint64_column": yson.YsonUint64(25),
        "double_column": 2.7,
        "bool_column": False,
        "string_column": "qux",
        "any_column": 234,
        "enum_string_column": "Green",
        "enum_int_column": -42,
    },
]

PROTOBUF_SCHEMALESS_TABLE_ROWS = SCHEMALESS_TABLE_ROWS

SCHEMALESS_TABLE_PROTOBUF_CONFIG = {
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
        {
            "name": "enum_string_column",
            "field_number": 7,
            "enumeration_name": "MyEnum",
            "proto_type": "enum_string",
        },
        {
            "name": "enum_int_column",
            "field_number": 8,
            "enumeration_name": "MyEnum",
            "proto_type": "enum_int",
        },
    ],
}


##################################################################


class TestSchemalessProtobufFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    USE_DYNAMIC_TABLES = True

    @authors("levysotsky")
    def test_protobuf_read(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", SCHEMALESS_TABLE_ROWS)
        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)
        data = read_table("//tmp/t", output_format=format)
        parsed_rows = protobuf_format.parse_lenval_protobuf(data, format)
        assert_rowsets_equal(parsed_rows, PROTOBUF_SCHEMALESS_TABLE_ROWS)

    @authors("levysotsky")
    def test_protobuf_write(self):
        create("table", "//tmp/t")
        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)
        data = protobuf_format.write_lenval_protobuf(SCHEMALESS_TABLE_ROWS, format)
        assert_rowsets_equal(
            protobuf_format.parse_lenval_protobuf(data, format),
            PROTOBUF_SCHEMALESS_TABLE_ROWS,
        )
        write_table("//tmp/t", value=data, is_raw=True, input_format=format)
        read_rows = read_table("//tmp/t")
        assert_rowsets_equal(read_rows, SCHEMALESS_TABLE_ROWS)

    def _generate_random_rows(self, count):
        rows = []
        random.seed(42)
        enum_names = list(ENUMERATIONS["MyEnum"].keys())
        for _ in range(count):
            rows.append(
                {
                    "int64_column": random.randrange(1 << 63),
                    "uint64_column": yson.YsonUint64(random.randrange(1 << 64)),
                    "double_column": random.random(),
                    "bool_column": random.randrange(2) == 0,
                    "string_column": make_random_string(20),
                    "any_column": ["a", {"v": 4}],
                    "enum_string_column": random.choice(enum_names),
                    "enum_int_column": ENUMERATIONS["MyEnum"][random.choice(enum_names)],
                }
            )
        return rows

    @authors("levysotsky")
    @pytest.mark.timeout(150)
    def test_large_read(self):
        create("table", "//tmp/t")
        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)

        row_count = 50000
        rows = self._generate_random_rows(row_count)
        write_table("//tmp/t", rows, verbose=False)
        data = read_table("//tmp/t", output_format=format, verbose=False)
        parsed_rows = protobuf_format.parse_lenval_protobuf(data, format)
        assert_rowsets_equal(parsed_rows, rows)

    @authors("levysotsky")
    @pytest.mark.timeout(150)
    def test_large_write(self):
        create("table", "//tmp/t")
        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)

        row_count = 50000
        rows = self._generate_random_rows(row_count)
        data = protobuf_format.write_lenval_protobuf(rows, format)
        write_table("//tmp/t", value=data, is_raw=True, input_format=format, verbose=False)
        read_rows = read_table("//tmp/t", verbose=False)
        assert_rowsets_equal(read_rows, rows)

    @authors("levysotsky")
    def test_multi_output_map(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", SCHEMALESS_TABLE_ROWS)

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        input_format = create_protobuf_format([table_config], ENUMERATIONS)
        output_format = create_protobuf_format([table_config] * 2, ENUMERATIONS)

        protobuf_dump = read_table("//tmp/t_in", output_format=input_format)
        parsed_rows = protobuf_format.parse_lenval_protobuf(protobuf_dump, input_format)
        assert_rowsets_equal(parsed_rows, PROTOBUF_SCHEMALESS_TABLE_ROWS)

        map(
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            command="tee /dev/fd/4",
            spec={
                "mapper": {
                    "input_format": input_format,
                    "output_format": output_format,
                },
                "job_count": 1,
            },
        )

        assert_rowsets_equal(read_table("//tmp/t_out1"), SCHEMALESS_TABLE_ROWS)
        assert_rowsets_equal(read_table("//tmp/t_out2"), SCHEMALESS_TABLE_ROWS)

    @authors("levysotsky")
    def test_multi_output_map_wrong_config(self):
        """Check bad format (number of tables in format doesn't match number of output tables)"""
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", SCHEMALESS_TABLE_ROWS)

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        input_format = create_protobuf_format([table_config], ENUMERATIONS)
        # Note single table config.
        output_format = create_protobuf_format([table_config], ENUMERATIONS)

        try:
            map(
                in_="//tmp/t_in",
                out=["//tmp/t_out1", "//tmp/t_out2"],
                command="tee /dev/fd/4",
                spec={
                    "mapper": {
                        "input_format": input_format,
                        "output_format": output_format,
                    },
                    "max_failed_job_count": 1,
                },
            )
            assert False, "Mapper should have failed"
        except YtError as error:
            assert "Protobuf format does not have table with index 1" in str(error)

    @authors("levysotsky")
    def test_id_map(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", SCHEMALESS_TABLE_ROWS)

        create("table", "//tmp/t_out")

        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "mapper": {
                    "format": format,
                },
                "job_count": 1,
            },
        )

        protobuf_dump = read_table("//tmp/t_in", output_format=format)
        parsed_rows = protobuf_format.parse_lenval_protobuf(protobuf_dump, format)
        assert_rowsets_equal(parsed_rows, PROTOBUF_SCHEMALESS_TABLE_ROWS)

        assert_rowsets_equal(read_table("//tmp/t_out"), SCHEMALESS_TABLE_ROWS)

        format = create_protobuf_format(
            [
                {
                    "columns": [
                        {
                            "name": "other_columns",
                            "field_number": 1,
                            "proto_type": "other_columns",
                        },
                    ],
                },
            ],
            ENUMERATIONS,
        )

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="cat",
            spec={
                "mapper": {
                    "format": format,
                },
                "job_count": 1,
            },
        )

        assert_rowsets_equal(read_table("//tmp/t_out"), SCHEMALESS_TABLE_ROWS)

    @authors("levysotsky")
    def test_id_map_reduce(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", SCHEMALESS_TABLE_ROWS)

        create("table", "//tmp/t_out")

        table_config = SCHEMALESS_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)

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
                },
            },
        )

        protobuf_dump = read_table("//tmp/t_in", output_format=format)
        parsed_rows = protobuf_format.parse_lenval_protobuf(protobuf_dump, format)
        assert_rowsets_equal(parsed_rows, PROTOBUF_SCHEMALESS_TABLE_ROWS)
        assert_items_equal(read_table("//tmp/t_out"), SCHEMALESS_TABLE_ROWS)

    @authors("levysotsky")
    def test_tablet_index(self):
        table = "//tmp/t"
        sync_create_cells(1)
        schema = [
            {
                "name": "a",
                "type_v3": "int64",
            },
        ]
        create("table", table, attributes={"schema": schema, "dynamic": True})
        sync_reshard_table("//tmp/t", 10)
        sync_mount_table(table)

        rows = [
            ({"a": 1, "$tablet_index": 0}, 0),
            ({"a": 17, "$tablet_index": 0}, 1),
            ({"a": 3, "$tablet_index": 4}, 0),
            ({"a": 2, "$tablet_index": 6}, 0),
            ({"a": 27, "$tablet_index": 6}, 1),
        ]
        for row, row_index in rows:
            insert_rows(table, [row])

        sync_unmount_table(table)

        config = {
            "columns": [
                {
                    "name": "a",
                    "field_number": 1,
                    "proto_type": "int64",
                },
            ],
        }
        format = create_protobuf_format([config])
        control_attributes = {
            "enable_tablet_index": True,
            "enable_row_index": True,
        }
        data = read_table(table, output_format=format, control_attributes=control_attributes)
        parsed_rows = protobuf_format.parse_lenval_protobuf(data, format, enable_control_attributes=True)
        parsed_rows.sort(key=lambda row: row.attributes["tablet_index"])
        assert len(parsed_rows) == len(rows)
        for (row, row_index), parsed_row in zip(rows, parsed_rows):
            assert parsed_row["a"] == row["a"]
            assert parsed_row.attributes["tablet_index"] == row["$tablet_index"]
            assert parsed_row.attributes["row_index"] == row_index


SCHEMA = [
    {
        "name": "int16",
        "type_v3": "int16",
    },
    {
        "name": "list_of_strings",
        "type_v3": list_type("string"),
    },
    {
        "name": "optional_list_of_int64",
        "type_v3": optional_type(list_type("int64")),
    },
    {
        "name": "optional_boolean",
        "type_v3": optional_type("bool"),
    },
    {
        "name": "list_of_optional_any",
        "type_v3": list_type(optional_type("yson")),
    },
    {
        "name": "struct",
        "type_v3": struct_type(
            [
                ("key", "string"),
                (
                    "points",
                    list_type(
                        struct_type(
                            [
                                ("x", "int64"),
                                ("y", "int64"),
                            ]
                        )
                    ),
                ),
                ("enum_string", "string"),
                ("enum_int", "int32"),
                ("extra_field", optional_type("string")),
                (
                    "optional_list_of_structs",
                    optional_type(
                        list_type(
                            struct_type(
                                [
                                    ("a", "string"),
                                    ("b", "bool"),
                                ]
                            )
                        )
                    ),
                ),
            ]
        ),
    },
    {
        "name": "utf8",
        "type_v3": "utf8",
    },
    {
        "name": "packed_repeated_int8",
        "type_v3": list_type("int8"),
    },
    {
        "name": "variant",
        "type_v3": optional_type(
            variant_struct_type(
                [
                    ("f1", "int8"),
                    ("f2", "int8"),
                    (
                        "f3",
                        struct_type(
                            [
                                (
                                    "var",
                                    optional_type(
                                        variant_struct_type(
                                            [
                                                ("g1", "string"),
                                                ("g2", "string"),
                                            ]
                                        )
                                    ),
                                ),
                                ("list_of_ints", optional_type(list_type("int64"))),
                            ]
                        ),
                    ),
                ]
            )
        ),
    },
    {
        "name": "dict",
        "type_v3": dict_type(
            "int64",
            struct_type(
                [
                    ("f1", "int8"),
                    ("f2", "string"),
                ]
            ),
        ),
    },
]

SCHEMAFUL_TABLE_PROTOBUF_CONFIG = {
    "columns": [
        {
            "name": "int16",
            "field_number": 1,
            "proto_type": "int32",
        },
        {
            "name": "list_of_strings",
            "field_number": 2122,
            "proto_type": "string",
            "repeated": True,
        },
        {
            "name": "optional_boolean",
            "field_number": 3,
            "proto_type": "bool",
        },
        {
            "name": "list_of_optional_any",
            "field_number": 4,
            "proto_type": "any",
            "repeated": True,
        },
        # Note that "extra_field" is missing from protobuf description.
        {
            "name": "struct",
            "field_number": 5,
            "proto_type": "structured_message",
            "fields": [
                {
                    "name": "key",
                    "field_number": 117,
                    "proto_type": "string",
                },
                {
                    "name": "points",
                    "field_number": 1,
                    "proto_type": "structured_message",
                    "repeated": True,
                    "fields": [
                        {
                            "name": "x",
                            "field_number": 1,
                            "proto_type": "int64",
                        },
                        {
                            "name": "y",
                            "field_number": 2,
                            "proto_type": "int64",
                        },
                    ],
                },
                {
                    "name": "enum_int",
                    "field_number": 2,
                    "enumeration_name": "MyEnum",
                    "proto_type": "enum_int",
                },
                {
                    "name": "enum_string",
                    "field_number": 3,
                    "enumeration_name": "MyEnum",
                    "proto_type": "enum_string",
                },
                {
                    "name": "optional_list_of_structs",
                    "field_number": 4,
                    "proto_type": "structured_message",
                    "fields": [
                        {
                            "name": "a",
                            "field_number": 1,
                            "proto_type": "string",
                        },
                        {
                            "name": "b",
                            "field_number": 2,
                            "proto_type": "bool",
                        },
                    ],
                    "repeated": True,
                },
            ],
        },
        {
            "name": "utf8",
            "field_number": 6,
            "proto_type": "string",
        },
        {
            "name": "packed_repeated_int8",
            "field_number": 7,
            "proto_type": "int32",
            "packed": True,
            "repeated": True,
        },
        {
            "name": "optional_list_of_int64",
            "field_number": 8,
            "proto_type": "int64",
            "repeated": True,
        },
        {
            "name": "variant",
            "field_number": 0,
            "proto_type": "oneof",
            "fields": [
                {
                    "name": "f1",
                    "field_number": 1001,
                    "proto_type": "int32",
                },
                {
                    "name": "f2",
                    "field_number": 1002,
                    "proto_type": "sfixed64",
                },
                {
                    "name": "f3",
                    "field_number": 1111,
                    "proto_type": "structured_message",
                    "fields": [
                        {
                            "name": "var",
                            "field_number": 0,
                            "proto_type": "oneof",
                            "fields": [
                                {
                                    "name": "g1",
                                    "field_number": 1,
                                    "proto_type": "string",
                                },
                                {
                                    "name": "g2",
                                    "field_number": 2,
                                    "proto_type": "string",
                                },
                            ],
                        },
                        {
                            "name": "list_of_ints",
                            "field_number": 3,
                            "proto_type": "int64",
                            "repeated": True,
                        },
                    ],
                },
            ],
        },
        {
            "name": "dict",
            "field_number": 9,
            "proto_type": "structured_message",
            "repeated": True,
            "fields": [
                {
                    "name": "key",
                    "field_number": 1,
                    "proto_type": "int64",
                },
                {
                    "name": "value",
                    "field_number": 2,
                    "proto_type": "structured_message",
                    "fields": [
                        {
                            "name": "f1",
                            "field_number": 2,
                            "proto_type": "int32",
                        },
                        {
                            "name": "f2",
                            "field_number": 17,
                            "proto_type": "string",
                        },
                    ],
                },
            ],
        },
    ],
}

HELLO_WORLD = b"\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82, \xd0\xbc\xd0\xb8\xd1\x80!"
GOODBYE_WORLD = b"\xd0\x9f\xd0\xbe\xd0\xba\xd0\xb0, \xd0\xbc\xd0\xb8\xd1\x80!"

SCHEMAFUL_TABLE_ROWS = [
    {
        "int16": 32767,
        "list_of_strings": ["foo", "bar", "baz"],
        "optional_boolean": False,
        "list_of_optional_any": [yson.YsonEntity(), {"x": 3}, []],
        "struct": {
            "key": "qux",
            "points": [{"x": 1, "y": 4}, {"x": 5, "y": 4}],
            "enum_int": -42,
            "enum_string": "Green",
            "extra_field": "baz",
            "optional_list_of_structs": [
                {"a": "AAA", "b": False},
                {"a": "---", "b": True},
            ],
        },
        "utf8": HELLO_WORLD.decode("utf-8"),
        "packed_repeated_int8": [0, 12, 127],
        "optional_list_of_int64": yson.YsonEntity(),
        "dict": [[12, {"f1": -9, "f2": "-9"}], [13, {"f1": -127, "f2": "-127"}]],
    },
    {
        "int16": -32768,
        "list_of_strings": ["a", "bc"],
        "optional_boolean": None,
        "list_of_optional_any": [[yson.YsonEntity()], [1, "baz"]],
        "struct": {
            "key": "lol",
            "points": [],
            "enum_int": 12,
            "enum_string": "Red",
            "optional_list_of_structs": yson.YsonEntity(),
        },
        "utf8": GOODBYE_WORLD.decode("utf-8"),
        "packed_repeated_int8": [],
        "optional_list_of_int64": [-300, -200, -100],
        "variant": ["f3", {"var": ["g2", "spam"], "list_of_ints": [3, 4, 5]}],
        "dict": [],
    },
]

PROTOBUF_SCHEMAFUL_TABLE_ROWS = [
    {
        "int16": 32767,
        "list_of_strings": ["foo", "bar", "baz"],
        "optional_boolean": False,
        "list_of_optional_any": [yson.YsonEntity(), {"x": 3}, []],
        "struct": {
            "key": "qux",
            "points": [{"x": 1, "y": 4}, {"x": 5, "y": 4}],
            "enum_int": -42,
            "enum_string": "Green",
            "optional_list_of_structs": [
                {"a": "AAA", "b": False},
                {"a": "---", "b": True},
            ],
        },
        "utf8": HELLO_WORLD.decode("utf-8"),
        "packed_repeated_int8": [0, 12, 127],
        "dict": [
            {"key": 12, "value": {"f1": -9, "f2": "-9"}},
            {"key": 13, "value": {"f1": -127, "f2": "-127"}},
        ],
    },
    {
        "int16": -32768,
        "list_of_strings": ["a", "bc"],
        "list_of_optional_any": [[yson.YsonEntity()], [1, "baz"]],
        "struct": {
            "key": "lol",
            "enum_int": 12,
            "enum_string": "Red",
        },
        "utf8": GOODBYE_WORLD.decode("utf-8"),
        "optional_list_of_int64": [-300, -200, -100],
        "variant": ["f3", {"var": ["g2", "spam"], "list_of_ints": [3, 4, 5]}],
    },
]

SCHEMAFUL_TABLE_ROWS_WITH_ENTITY_EXTRA_FIELD = [
    {
        "int16": 32767,
        "list_of_strings": ["foo", "bar", "baz"],
        "optional_boolean": False,
        "list_of_optional_any": [yson.YsonEntity(), {"x": 3}, []],
        "struct": {
            "key": "qux",
            "points": [{"x": 1, "y": 4}, {"x": 5, "y": 4}],
            "enum_int": -42,
            "enum_string": "Green",
            "extra_field": yson.YsonEntity(),
            "optional_list_of_structs": [
                {"a": "AAA", "b": False},
                {"a": "---", "b": True},
            ],
        },
        "utf8": HELLO_WORLD.decode("utf8"),
        "packed_repeated_int8": [0, 12, 127],
        "optional_list_of_int64": yson.YsonEntity(),
        "variant": yson.YsonEntity(),
        "dict": [[12, {"f1": -9, "f2": "-9"}], [13, {"f1": -127, "f2": "-127"}]],
    },
    {
        "int16": -32768,
        "list_of_strings": ["a", "bc"],
        "optional_boolean": yson.YsonEntity(),
        "list_of_optional_any": [[yson.YsonEntity()], [1, "baz"]],
        "struct": {
            "key": "lol",
            "points": [],
            "enum_int": 12,
            "enum_string": "Red",
            "extra_field": yson.YsonEntity(),
            "optional_list_of_structs": yson.YsonEntity(),
        },
        "utf8": GOODBYE_WORLD.decode("utf8"),
        "packed_repeated_int8": [],
        "optional_list_of_int64": [-300, -200, -100],
        "variant": ["f3", {"var": ["g2", "spam"], "list_of_ints": [3, 4, 5]}],
        "dict": [],
    },
]


def make_random_bool():
    return random.randrange(2) == 0


def make_random_list(max_len, generator, optional=False):
    min_len = -1 if optional else 0
    length = random.randrange(min_len, max_len + 1)
    if length == -1:
        return yson.YsonEntity()
    return [generator() for _ in range(length)]


def make_random_variant_struct(fields):
    name, generator = random.choice(fields)
    return [name, generator()]


@authors("levysotsky")
class TestSchemafulProtobufFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("levysotsky")
    def test_protobuf_read(self):
        create("table", "//tmp/t", attributes={"schema": SCHEMA})
        write_table("//tmp/t", SCHEMAFUL_TABLE_ROWS)
        table_config = SCHEMAFUL_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)
        data = read_table("//tmp/t", output_format=format)
        parsed_rows = protobuf_format.parse_lenval_protobuf(data, format)
        assert_rowsets_equal(parsed_rows, PROTOBUF_SCHEMAFUL_TABLE_ROWS)

    @authors("levysotsky")
    def test_protobuf_write(self):
        create("table", "//tmp/t", attributes={"schema": SCHEMA})
        table_config = SCHEMAFUL_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)
        data = protobuf_format.write_lenval_protobuf(PROTOBUF_SCHEMAFUL_TABLE_ROWS, format)
        assert_rowsets_equal(
            protobuf_format.parse_lenval_protobuf(data, format),
            PROTOBUF_SCHEMAFUL_TABLE_ROWS,
        )
        write_table("//tmp/t", value=data, is_raw=True, input_format=format)
        assert_rowsets_equal(read_table("//tmp/t"), SCHEMAFUL_TABLE_ROWS_WITH_ENTITY_EXTRA_FIELD)

    def _generate_random_rows(self, count):
        rows = []
        random.seed(42)
        enum_names = list(ENUMERATIONS["MyEnum"].keys())
        for _ in range(count):
            rows.append(
                {
                    "int16": random.randrange(1 << 15),
                    "list_of_strings": make_random_list(5, lambda: make_random_string(random.randrange(10))),
                    "optional_boolean": make_random_bool(),
                    "list_of_optional_any": [
                        yson.YsonEntity(),
                        {"x": random.randrange(1 << 64)},
                        [],
                    ],
                    "struct": {
                        "key": make_random_string(10),
                        "points": make_random_list(
                            5,
                            lambda: {
                                "x": random.randrange(100),
                                "y": random.randrange(100),
                            },
                        ),
                        "enum_int": ENUMERATIONS["MyEnum"][random.choice(enum_names)],
                        "enum_string": random.choice(enum_names),
                        "extra_field": make_random_string(7),
                        "optional_list_of_structs": make_random_list(
                            5,
                            lambda: {
                                "a": make_random_string(3),
                                "b": make_random_bool(),
                            },
                            optional=True,
                        ),
                    },
                    "utf8": make_random_string(10),
                    "packed_repeated_int8": make_random_list(5, lambda: random.randrange(-128, 128)),
                    "optional_list_of_int64": make_random_list(5, lambda: random.randrange(1 << 63), optional=True),
                    "variant": make_random_variant_struct(
                        [
                            ("f1", lambda: random.randrange(-128, 128)),
                            ("f2", lambda: random.randrange(-128, 128)),
                            (
                                "f3",
                                lambda: {
                                    "var": make_random_variant_struct(
                                        [
                                            ("g1", lambda: make_random_string(7)),
                                            ("g2", lambda: make_random_string(7)),
                                        ]
                                    ),
                                    "list_of_ints": make_random_list(5, lambda: random.randrange(1 << 63)),
                                },
                            ),
                        ]
                    ),
                    "dict": make_random_list(
                        5,
                        lambda: [
                            random.randrange(1 << 63),
                            {
                                "f1": random.randrange(-128, 128),
                                "f2": make_random_string(3),
                            },
                        ],
                    ),
                }
            )
        return rows

    @authors("levysotsky")
    @pytest.mark.timeout(150)
    def test_large_write(self):
        create("table", "//tmp/t", attributes={"schema": SCHEMA})
        table_config = SCHEMAFUL_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)

        row_count = 30000
        rows = self._generate_random_rows(row_count)
        expected_rows = deepcopy(rows)

        for row in rows:
            row["dict"] = [{"key": key, "value": value} for key, value in row["dict"]]

        data = protobuf_format.write_lenval_protobuf(rows, format)
        write_table("//tmp/t", value=data, is_raw=True, input_format=format, verbose=False)
        read_rows = read_table("//tmp/t", verbose=False)

        def empty_to_entity(d, key):
            if isinstance(d[key], list) and len(d[key]) == 0:
                d[key] = yson.YsonEntity()

        for row in expected_rows:
            row["struct"]["extra_field"] = yson.YsonEntity()
            empty_to_entity(row, "optional_list_of_int64")
            empty_to_entity(row["struct"], "optional_list_of_structs")
            if row["variant"][0] == "f3":
                empty_to_entity(row["variant"][1], "list_of_ints")

        assert_rowsets_equal(read_rows, expected_rows)

    @authors("levysotsky")
    @pytest.mark.timeout(150)
    def test_large_read(self):
        create("table", "//tmp/t", attributes={"schema": SCHEMA})
        table_config = SCHEMAFUL_TABLE_PROTOBUF_CONFIG
        format = create_protobuf_format([table_config], ENUMERATIONS)

        row_count = 30000
        rows = self._generate_random_rows(row_count)
        write_table("//tmp/t", rows, verbose=False)
        data = read_table("//tmp/t", output_format=format, verbose=False)
        parsed_rows = protobuf_format.parse_lenval_protobuf(data, format)

        def remove_empty(d, key):
            value = d[key]
            if not isinstance(value, list) or len(value) == 0:
                del d[key]

        expected_rows = deepcopy(rows)
        for row in expected_rows:
            del row["struct"]["extra_field"]
            remove_empty(row, "list_of_strings")
            remove_empty(row["struct"], "points")
            remove_empty(row, "packed_repeated_int8")
            remove_empty(row, "optional_list_of_int64")
            remove_empty(row["struct"], "optional_list_of_structs")
            if row["variant"][0] == "f3":
                remove_empty(row["variant"][1], "list_of_ints")
            if "dict" in row:
                row["dict"] = [{"key": key, "value": value} for key, value in row["dict"]]
            remove_empty(row, "dict")
        assert_rowsets_equal(parsed_rows, expected_rows)

    def test_multi_output_map(self):
        create("table", "//tmp/t_in", attributes={"schema": SCHEMA})
        write_table("//tmp/t_in", SCHEMAFUL_TABLE_ROWS)

        table_config = SCHEMAFUL_TABLE_PROTOBUF_CONFIG
        input_format = create_protobuf_format([table_config], ENUMERATIONS)
        output_format = create_protobuf_format([table_config] * 2, ENUMERATIONS)

        protobuf_dump = read_table("//tmp/t_in", output_format=input_format)
        parsed_rows = protobuf_format.parse_lenval_protobuf(protobuf_dump, input_format)
        assert_rowsets_equal(parsed_rows, PROTOBUF_SCHEMAFUL_TABLE_ROWS)

        create("table", "//tmp/t_out1", attributes={"schema": SCHEMA})
        create("table", "//tmp/t_out2", attributes={"schema": SCHEMA})

        map(
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            command="tee /dev/fd/4",
            spec={
                "mapper": {
                    "input_format": input_format,
                    "output_format": output_format,
                },
                "job_count": 1,
            },
        )

        assert_rowsets_equal(read_table("//tmp/t_out1"), SCHEMAFUL_TABLE_ROWS_WITH_ENTITY_EXTRA_FIELD)
        assert_rowsets_equal(read_table("//tmp/t_out2"), SCHEMAFUL_TABLE_ROWS_WITH_ENTITY_EXTRA_FIELD)

    def test_intermediate_mapreduce(self):
        complex_schema = make_schema([
            make_column("key", "int64"),
            make_column("value", struct_type([("field", "string")])),
        ])
        create("table", "//tmp/t_in1", attributes={"schema": complex_schema})
        create("table", "//tmp/t_in2")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in1", [
            {"key": 1, "value": {"field": "foo"}},
        ])
        write_table("//tmp/t_in2", [
            {"key": 1},
        ])

        proto_format = create_protobuf_descriptor_format(
            file_descriptor_set_text="""
            file {
                name: "google/protobuf/descriptor.proto"
                package: "google.protobuf"
                options {
                    java_package: "com.google.protobuf"
                    java_outer_classname: "DescriptorProtos"
                    optimize_for: SPEED
                    go_package: "google.golang.org/protobuf/types/descriptorpb"
                    cc_enable_arenas: true objc_class_prefix: "GPB"
                    csharp_namespace: "Google.Protobuf.Reflection"
                }
            }
            file {
                name: "data.proto"
                options { }
                message_type {
                    name: "TInner"
                    options { }
                    field {
                        name: "field"
                        number: 1
                        label: LABEL_OPTIONAL
                        type: TYPE_STRING options { }
                    }
                }
                message_type {
                    name: "TRow1"
                    options { [NYT.default_field_flags]: SERIALIZATION_YT }
                    field {
                        name: "key"
                        number: 1
                        label: LABEL_OPTIONAL
                        type: TYPE_INT64
                        options { }
                    }
                    field {
                        name: "value"
                        number: 2
                        label: LABEL_OPTIONAL
                        type: TYPE_MESSAGE
                        type_name: ".TInner"
                        options { }
                    }
                }
                message_type {
                    name: "TRow2"
                    options { }
                    field {
                        name: "key"
                        number: 1
                        label: LABEL_OPTIONAL
                        type: TYPE_INT64 options { }
                    }
                }
            }
            """,
            type_names=["TRow1", "TRow2"]
        )
        map_reduce(
            in_=["//tmp/t_in1", "//tmp/t_in2"],
            out="//tmp/t_out",
            reduce_by=["key"],
            spec={
                "reducer": {
                    "enable_input_table_index": True,
                    "input_format": proto_format,
                    "output_format": "yson",
                    "command": "cat > /dev/null",
                },
            },
        )
