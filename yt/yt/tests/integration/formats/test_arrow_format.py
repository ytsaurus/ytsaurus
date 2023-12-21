from yt_env_setup import YTEnvSetup

from yt_commands import authors, create, read_table, write_table, map

from yt_type_helpers import optional_type

import pytest

import yt.yson as yson

import pyarrow as pa


HELLO_WORLD = b"\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82, \xd0\xbc\xd0\xb8\xd1\x80!"
GOODBYE_WORLD = b"\xd0\x9f\xd0\xbe\xd0\xba\xd0\xb0, \xd0\xbc\xd0\xb8\xd1\x80!"


def parse_arrow_stream(data):
    return pa.ipc.open_stream(data).read_all()


@authors("nadya02")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestArrowFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

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

    @authors("apollo1321")
    def test_map(self, optimize_for):
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
                    "uint": 120,
                    "double": 3.14,
                    "utf8": HELLO_WORLD.decode("utf-8"),
                    "bool": True,
                },
                {
                    "int": -82,
                    "opt_string": None,
                    "uint": 42,
                    "double": 1.5,
                    "utf8": GOODBYE_WORLD.decode("utf-8"),
                    "bool": False,
                },
            ],
        )

        create("table", "//tmp/t_out")

        input_format = yson.YsonString(b"arrow")

        output_format = yson.YsonString(b"skiff")
        output_format.attributes["table_skiff_schemas"] = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "yson32",
                        "name": "$other_columns",
                    },
                ],
            }
        ]

        map(
            in_="//tmp/t_in{int,uint,double}",
            out="//tmp/t_out",
            command="cat > /dev/null",
            spec={"mapper": {"input_format": input_format, "output_format": output_format}},
        )

        assert read_table("//tmp/t_out") == []
