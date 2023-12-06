from yt_env_setup import YTEnvSetup

from yt_commands import authors, create, read_table, write_table

from yt_type_helpers import optional_type, list_type

import pytest

import yt.yson as yson

import pyarrow as pa
import pandas as pd


HELLO_WORLD = b"\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82, \xd0\xbc\xd0\xb8\xd1\x80!"
GOODBYE_WORLD = b"\xd0\x9f\xd0\xbe\xd0\xba\xd0\xb0, \xd0\xbc\xd0\xb8\xd1\x80!"


def parse_list_to_arrow():
    data = [
        {'string': 'one', 'list_strings': ['bar', 'foo']},
        {'string': 'two', 'list_strings': []}
    ]

    schema = pa.schema([
        ('string', pa.string()),
        ('list_strings', pa.list_(pa.string()))
    ])

    table = pa.Table.from_pandas(pd.DataFrame(data), schema=schema)

    sink = pa.BufferOutputStream()

    with pa.RecordBatchStreamWriter(sink, table.schema) as writer:
        writer.write(table)

    buffer = sink.getvalue()

    return bytes(buffer)


def parse_arrow_stream(data):
    return pa.ipc.open_stream(data).read_all()


@authors("nadya02")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestArrowFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def test_simple_reader(self, optimize_for):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "int", "type_v3": "int64"},
                    {"name": "opt_string", "type_v3": optional_type("string")},
                ],
                "optimize_for": optimize_for
            },
            force=True,
        )

        write_table(
            "//tmp/t_in",
            [
                {
                    "int": 53,
                    "opt_string": "foobar",
                },
                {
                    "int": 82,
                    "opt_string": None,
                },
            ],
        )

        format = yson.YsonString(b"arrow")

        parsed_table = parse_arrow_stream(read_table("//tmp/t_in", output_format=format))
        column_names = parsed_table.column_names

        assert column_names[0] == "int"
        assert parsed_table[column_names[0]].to_pylist() == [53, 82]

        assert column_names[1] == "opt_string"
        assert parsed_table[column_names[1]].to_pylist() == [b'foobar', None]

    def test_all_types(self, optimize_for):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "int", "type_v3": "int64"},
                    {"name": "opt_string", "type_v3": optional_type("string")},
                    {"name": "uint", "type_v3": "uint64"},
                    {"name": "double", "type_v3": "double"},
                    {"name": "utf8", "type_v3": "utf8"},
                    {"name": "bool", "type_v3": "bool"},
                ],
                "optimize_for": optimize_for
            },
            force=True,
        )

        write_table(
            "//tmp/t_in",
            [
                {
                    "int": 53,
                    "opt_string": "foobar",
                    "uint" : 120,
                    "double" : 3.14,
                    "utf8" : HELLO_WORLD.decode("utf-8"),
                    "bool" : True,
                },
                {
                    "int": -82,
                    "opt_string": None,
                    "uint" : 42,
                    "double" : 1.5,
                    "utf8" : GOODBYE_WORLD.decode("utf-8"),
                    "bool" : False,
                },
            ],
        )

        format = yson.YsonString(b"arrow")

        parsed_table = parse_arrow_stream(read_table("//tmp/t_in", output_format=format))
        column_names = parsed_table.column_names

        assert column_names[0] == "int"
        assert parsed_table[column_names[0]].to_pylist() == [53, -82]

        assert column_names[1] == "opt_string"
        assert parsed_table[column_names[1]].to_pylist() == [b'foobar', None]

        assert column_names[2] == "uint"
        assert parsed_table[column_names[2]].to_pylist() == [120, 42]

        assert column_names[3] == "double"
        assert parsed_table[column_names[3]].to_pylist() == [3.14, 1.5]

        assert column_names[4] == "utf8"
        assert parsed_table[column_names[4]].to_pylist() == [HELLO_WORLD.decode("utf-8"), GOODBYE_WORLD.decode("utf-8")]

        assert column_names[5] == "bool"
        assert parsed_table[column_names[5]].to_pylist() == [True, False]

    @authors("nadya02")
    def test_write_arrow(self, optimize_for):
        schema = [
            {"name": "int", "type_v3": "int64"},
            {"name": "opt_string", "type_v3": optional_type("string")}
        ]
        create("table", "//tmp/table1", attributes={"schema": schema, "optimize_for": optimize_for})
        write_table(
            "//tmp/table1",
            [
                {
                    "int": 53,
                    "opt_string": "foobar",
                },
                {
                    "int": 82,
                    "opt_string": None,
                },
            ],
        )

        format = yson.YsonString(b"arrow")

        arrow_dump = read_table("//tmp/table1", output_format=format)

        create("table", "//tmp/table2", attributes={"schema": schema})
        write_table("//tmp/table2", arrow_dump, is_raw=True, input_format=format)

        assert read_table("//tmp/table1") == read_table("//tmp/table2")

    @authors("nadya02")
    def test_write_arrow_complex(self, optimize_for):
        schema = [
            {"name": "int", "type_v3": "int64"},
            {"name": "opt_string", "type_v3": optional_type("string")},
            {"name": "uint", "type_v3": "uint64"},
            {"name": "double", "type_v3": "double"},
            {"name": "utf8", "type_v3": "utf8"},
            {"name": "bool", "type_v3": "bool"},
            {"name": "list_of_strings", "type_v3": list_type("string")},
        ]

        create("table", "//tmp/table1", attributes={"schema": schema, "optimize_for": optimize_for})
        write_table(
            "//tmp/table1",
            [
                {
                    "int": 53,
                    "opt_string": "foobar",
                    "uint" : 120,
                    "double" : 3.14,
                    "utf8" : HELLO_WORLD.decode("utf-8"),
                    "bool" : True,
                    "list_of_strings": ["foo", "bar", "baz"],
                },
                {
                    "int": -82,
                    "opt_string": None,
                    "uint" : 42,
                    "double" : 1.5,
                    "utf8" : GOODBYE_WORLD.decode("utf-8"),
                    "bool" : False,
                    "list_of_strings": ["yt", "bar"],
                },
            ],
        )

        format = yson.YsonString(b"arrow")

        arrow_dump = read_table("//tmp/table1", output_format=format)
        create("table", "//tmp/table2", attributes={"schema": schema})
        write_table("//tmp/table2", arrow_dump, is_raw=True, input_format=format)

        assert read_table("//tmp/table1") == read_table("//tmp/table2")

    @authors("nadya02")
    def test_write_list_arrow(self, optimize_for):
        schema = [
            {"name": "string", "type_v3": "string"},
            {"name": "list_strings", "type_v3": list_type("string")}
        ]
        create("table", "//tmp/table1", attributes={"schema": schema, "optimize_for": optimize_for})
        create("table", "//tmp/table2", attributes={"schema": schema, "optimize_for": optimize_for})

        arrow_dump = parse_list_to_arrow()
        format = yson.YsonString(b"arrow")
        write_table("//tmp/table1", arrow_dump, is_raw=True, input_format=format)

        write_table(
            "//tmp/table2",
            [
                {
                    "string": "one",
                    "list_strings": ["bar", "foo"],
                },
                {
                    "string": "two",
                    "list_strings": [],
                },
            ],
        )

        assert read_table("//tmp/table1") == read_table("//tmp/table2")


@authors("nadya73")
class TestArrowIntegerColumn_YTADMINREQ_34427(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def test_integer_column(self):
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "a", "type_v3": "int64"},
                ],
                "optimize_for": "scan"
            },
            force=True,
        )

        for i in range(0, 6, 3):
            write_table(
                "<append=%true>//tmp/t_in",
                [
                    {
                        "a": i,
                    },
                    {
                        "a": i + 1,
                    },
                    {
                        "a": i + 2,
                    },
                ]
            )

        format = yson.YsonString(b"arrow")

        parsed_table = parse_arrow_stream(read_table("//tmp/t_in", output_format=format, control_attributes={"enable_row_index": True, "enable_range_index": True}))
        column_names = parsed_table.column_names

        assert "a" in column_names
        assert parsed_table["a"].to_pylist() == [0, 1, 2, 3, 4, 5]
