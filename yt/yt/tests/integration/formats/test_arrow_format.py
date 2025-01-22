from yt_env_setup import YTEnvSetup

from yt_commands import authors, create, read_table, write_table, map, merge, get

from yt_type_helpers import optional_type, list_type, decimal_type

import pytest

import yt.yson as yson

import pyarrow as pa
import pandas as pd

HELLO_WORLD = b"\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82, \xd0\xbc\xd0\xb8\xd1\x80!"
GOODBYE_WORLD = b"\xd0\x9f\xd0\xbe\xd0\xba\xd0\xb0, \xd0\xbc\xd0\xb8\xd1\x80!"
ARROW_FORMAT = yson.YsonString(b"arrow")


def serialize_arrow_table(table):
    sink = pa.BufferOutputStream()
    with pa.RecordBatchStreamWriter(sink, table.schema) as writer:
        writer.write(table)
    return bytes(sink.getvalue())


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

    return serialize_arrow_table(table)


def parse_arrow_stream(data):
    return pa.ipc.open_stream(data).read_all()


@authors("nadya02")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
@pytest.mark.enabled_multidaemon
class TestArrowFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_MULTIDAEMON = True

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

        parsed_table = parse_arrow_stream(read_table("//tmp/t_in", output_format=ARROW_FORMAT))
        column_names = parsed_table.column_names

        assert column_names[0] == "int"
        assert parsed_table[column_names[0]].to_pylist() == [53, 82]

        assert column_names[1] == "opt_string"
        assert parsed_table[column_names[1]].to_pylist() == [b'foobar', None]

    @authors("nadya02")
    def test_read_table_with_different_chunk_meta(self, optimize_for):
        schema1 = [
            {"name": "string", "type_v3": "string"},
            {"name": "int", "type_v3": "int64"},
        ]

        schema2 = [
            {"name": "int", "type_v3": "int64"},
            {"name": "string", "type_v3": "string"},
        ]

        create("table", "//tmp/table1", attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        }, force=True)

        create("table", "//tmp/table2", attributes={
            "schema": schema2,
            "optimize_for": optimize_for,
        }, force=True)

        write_table("//tmp/table1", [{
            "int": 53,
            "string": "table1",
        }])

        write_table("//tmp/table2", [{
            "int": -82,
            "string": "table2",
        }])

        create("table", "//tmp/merged_table", attributes={
            "optimize_for": "lookup",
        }, force=True)

        merge(in_=["//tmp/table1", "//tmp/table2"], out="//tmp/merged_table")

        expected_table = [
            {'string': 'table1', 'int': 53},
            {'string': 'table2', 'int': -82},
        ]

        assert read_table("//tmp/merged_table") == expected_table

        assert get("//tmp/table1/@chunk_count") == 1
        assert get("//tmp/table2/@chunk_count") == 1
        assert get("//tmp/merged_table/@chunk_count") == 2

        arrow_dump = read_table("//tmp/merged_table", output_format=yson.YsonString(b"arrow"))
        parsed_table = parse_arrow_stream(arrow_dump)

        column_names = parsed_table.column_names

        assert column_names[0] == "string"
        assert parsed_table[column_names[0]].to_pylist() == [b'table1', b'table2']

        assert column_names[1] == "int"
        assert parsed_table[column_names[1]].to_pylist() == [53, -82]

    @authors("nadya02")
    def test_read_table_with_different_columns_in_chunk_meta(self, optimize_for):
        schema1 = [
            {"name": "string", "type_v3": "string"},
            {"name": "int", "type_v3": optional_type("int64")},
        ]

        schema2 = [
            {"name": "string", "type_v3": "string"},
        ]

        create("table", "//tmp/table1", attributes={
            "schema": schema1,
            "optimize_for": optimize_for,
        }, force=True)

        create("table", "//tmp/table2", attributes={
            "schema": schema2,
            "optimize_for": optimize_for,
        }, force=True)

        write_table("//tmp/table1", [{
            "int": 53,
            "string": "table1",
        }])

        write_table("//tmp/table2", [{
            "string": "table2",
        }])

        create("table", "//tmp/merged_table", attributes={
            "schema": schema1,
            "optimize_for": "scan",
        }, force=True)

        merge(in_=["//tmp/table1", "//tmp/table2"], out="//tmp/merged_table")

        expected_table = [
            {'string': 'table1', 'int': 53},
            {'string': 'table2'},
        ]

        assert read_table("//tmp/merged_table") == expected_table

        assert get("//tmp/table1/@chunk_count") == 1
        assert get("//tmp/table2/@chunk_count") == 1
        assert get("//tmp/merged_table/@chunk_count") == 2

        arrow_dump = read_table("//tmp/merged_table", output_format=yson.YsonString(b"arrow"))
        parsed_table = parse_arrow_stream(arrow_dump)

        column_names = parsed_table.column_names

        assert column_names[0] == "string"
        assert parsed_table[column_names[0]].to_pylist() == [b'table1', b'table2']

        assert column_names[1] == "int"
        assert parsed_table[column_names[1]].to_pylist() == [53, None]

    def test_filter_column(self, optimize_for):
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

        parsed_table = parse_arrow_stream(read_table("//tmp/t_in{int,uint,double}", output_format=format))
        column_names = parsed_table.column_names

        assert column_names[0] == "int"
        assert parsed_table[column_names[0]].to_pylist() == [53, -82]

        assert column_names[1] == "uint"
        assert parsed_table[column_names[1]].to_pylist() == [120, 42]

        assert column_names[2] == "double"
        assert parsed_table[column_names[2]].to_pylist() == [3.14, 1.5]

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

        parsed_table = parse_arrow_stream(read_table("//tmp/t_in", output_format=ARROW_FORMAT))
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

        arrow_dump = read_table("//tmp/table1", output_format=ARROW_FORMAT)

        create("table", "//tmp/table2", attributes={"schema": schema})
        write_table("//tmp/table2", arrow_dump, is_raw=True, input_format=ARROW_FORMAT)

        assert read_table("//tmp/table1") == read_table("//tmp/table2")

    @authors("max42")
    def test_write_arrow_decimal(self, optimize_for):
        schema = [
            {"name": "d128", "type_v3": decimal_type(10, 3)},
            {"name": "d256", "type_v3": decimal_type(10, 3)},
            {"name": "ld128", "type_v3": list_type(decimal_type(10, 3))},
            {"name": "ld256", "type_v3": list_type(decimal_type(10, 3))}
        ]

        fields = [
            pa.field("d128", pa.decimal128(10, 3)),
            pa.field("d256", pa.decimal256(10, 3)),
            pa.field("ld128", pa.list_(pa.decimal128(10, 3))),
            pa.field("ld256", pa.list_(pa.decimal256(10, 3))),
        ]
        arrow_schema = pa.schema(fields)

        d_list = [3.141, 0.000, -2.718, 9999999.999]
        ld_list = [[d] for d in d_list]

        d_arr = pa.array(d_list)
        ld_arr = pa.array(ld_list)
        arrow_table = pa.Table.from_arrays([d_arr, d_arr, ld_arr, ld_arr], schema=arrow_schema)

        create("table", "//tmp/table", attributes={"schema": schema, "optimize_for": optimize_for})

        format = yson.YsonString(b"arrow")
        write_table("//tmp/table", serialize_arrow_table(arrow_table), is_raw=True, input_format=format)

        rows = list(yson.loads(read_table("//tmp/table", output_format=yson.loads(b"<decimal_mode=text>yson")),
                               yson_type="list_fragment"))

        assert rows == [
            {'d128': "3.141", 'd256': "3.141", 'ld128': ["3.141"], 'ld256': ["3.141"]},
            {'d128': "0.000", 'd256': "0.000", 'ld128': ["0.000"], 'ld256': ["0.000"]},
            {'d128': "-2.718", 'd256': "-2.718", 'ld128': ["-2.718"], 'ld256': ["-2.718"]},
            {'d128': "9999999.999", 'd256': "9999999.999", 'ld128': ["9999999.999"], 'ld256': ["9999999.999"]}
        ]

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

        arrow_dump = read_table("//tmp/table1", output_format=ARROW_FORMAT)
        create("table", "//tmp/table2", attributes={"schema": schema})
        write_table("//tmp/table2", arrow_dump, is_raw=True, input_format=ARROW_FORMAT)

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
        write_table("//tmp/table1", arrow_dump, is_raw=True, input_format=ARROW_FORMAT)

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

    @authors("cherepashka")
    def test_non_materializied_computed_columns(self, optimize_for):
        create(
            "table",
            "//tmp/table",
            attributes={
                "optimize_for": optimize_for,
                "schema": [
                    {"name": "num", "type": "int64"},
                    {
                        "name": "doubled_num",
                        "type": "int64",
                        "expression": "num * 2",
                        "materialized": False,
                    },
                ]
            },
        )

        rows = [{"num": 1}, {"num": 1}, {"num": 2}, {"num": 3}, {"num": 5}, {"num": 8}, {"num": 13}]
        write_table("//tmp/table", rows)

        read_result = parse_arrow_stream(read_table("//tmp/table", output_format=ARROW_FORMAT)).to_pydict()
        assert read_result["num"] == [row["num"] for row in rows]
        assert read_result["doubled_num"] == [2 * row["num"] for row in rows]

        read_result = parse_arrow_stream(read_table("//tmp/table{doubled_num}", output_format=ARROW_FORMAT)).to_pydict()
        assert read_result["doubled_num"] == [2 * row["num"] for row in rows]


@authors("nadya02")
@pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
@pytest.mark.enabled_multidaemon
class TestMapArrowFormat(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_MULTIDAEMON = True

    @staticmethod
    def get_row_and_columnar_batch_count(operation):
        input_statistics = operation.get_statistics()["data"]["input"]
        encoded_row_batch_count = 0
        encoded_columnar_batch_count = 0

        for item in input_statistics["encoded_row_batch_count"]:
            encoded_row_batch_count += item["summary"]["sum"]

        for item in input_statistics["encoded_columnar_batch_count"]:
            encoded_columnar_batch_count += item["summary"]["sum"]

        return (encoded_row_batch_count, encoded_columnar_batch_count)

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
                "optimize_for": optimize_for,
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

        operation = map(
            in_="//tmp/t_in{int,uint,double}",
            out="//tmp/t_out",
            command="cat > /dev/null",
            spec={"mapper": {"input_format": ARROW_FORMAT, "output_format": output_format}},
        )

        assert read_table("//tmp/t_out") == []

        row_batch_count, columnar_batch_count = self.get_row_and_columnar_batch_count(operation)

        if optimize_for == "scan":
            assert row_batch_count == 0 and columnar_batch_count > 0
        else:
            assert row_batch_count > 0 and columnar_batch_count == 0

    @authors("nadya02")
    def test_map_with_arrow(self, optimize_for):
        schema = [
            {"name": "int", "type_v3": "int64"},
            {"name": "opt_string", "type_v3": optional_type("string")},
            {"name": "uint", "type_v3": "uint64"},
            {"name": "double", "type_v3": "double"},
            {"name": "utf8", "type_v3": "utf8"},
            {"name": "bool", "type_v3": "bool"},
        ]

        output_schema = [
            {"name": "int", "type_v3": "int64"},
            {"name": "uint", "type_v3": "uint64"},
            {"name": "double", "type_v3": "double"},
        ]

        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": schema,
                "optimize_for": optimize_for,
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

        create(
            "table",
            "//tmp/t_out",
            attributes={
                "schema": output_schema,
                "optimize_for": optimize_for,
            },
            force=True,
        )

        operation = map(
            in_="//tmp/t_in{int,uint,double}",
            out="//tmp/t_out",
            command="cat",
            spec={"mapper": {"format": ARROW_FORMAT}},
        )

        assert read_table("//tmp/t_in{int,uint,double}") == read_table("//tmp/t_out")

        row_batch_count, columnar_batch_count = self.get_row_and_columnar_batch_count(operation)

        if optimize_for == "scan":
            assert row_batch_count == 0 and columnar_batch_count > 0
        else:
            assert row_batch_count > 0 and columnar_batch_count == 0

    @authors("nadya02")
    def test_multi_table(self, optimize_for):
        schema = [
            {"name": "string", "type_v3": "string"},
        ]

        schema2 = [
            {"name": "int", "type_v3": "int64"},
        ]

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": schema,
                "optimize_for": optimize_for,
            },
            force=True,
        )

        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": schema2,
                "optimize_for": optimize_for,
            },
            force=True,
        )

        write_table(
            "//tmp/t1",
            [
                {
                    "string": "foobar"
                },
                {
                    "string": "lol"
                },
            ],
        )

        write_table(
            "//tmp/t2",
            [
                {
                    "int": 53
                },
                {
                    "int": 42
                },
                {
                    "int": 179
                }
            ]
        )

        create(
            "table",
            "//tmp/t_out",
            force=True,
        )

        op = map(
            in_=["//tmp/t1", "//tmp/t2"],
            out=["//tmp/t_out"],
            command="cat 1>&2",
            spec={"mapper": {"input_format": ARROW_FORMAT}},
        )

        assert read_table("//tmp/t_out") == []

        job_ids = op.list_jobs()
        stderr_bytes = op.read_stderr(job_ids[0])
        buffer = pa.py_buffer(stderr_bytes)

        reader = pa.BufferReader(buffer)
        table1 = pa.ipc.open_stream(reader).read_all()

        reader.read(4)
        table2 = pa.ipc.open_stream(reader).read_all()
        if table1.column_names[0] == "int":
            table1, table2 = table2, table1

        column_names = table1.column_names
        assert column_names[0] == "string"
        assert table1[column_names[0]].to_pylist() == [b'foobar', b'lol']

        column_names = table2.column_names
        assert column_names[0] == "int"
        assert table2[column_names[0]].to_pylist() == [53, 42, 179]

        row_batch_count, columnar_batch_count = self.get_row_and_columnar_batch_count(op)

        if optimize_for == "scan":
            assert row_batch_count == 0 and columnar_batch_count > 0
        else:
            assert row_batch_count > 0 and columnar_batch_count == 0

    @authors("nadya02")
    def test_multi_table_with_same_column(self, optimize_for):
        schema = [
            {"name": "column", "type_v3": "string"},
            {"name": "int", "type_v3": "int64"},
        ]

        schema2 = [
            {"name": "column", "type_v3": "int64"},
        ]

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": schema,
                "optimize_for": optimize_for,
            },
            force=True,
        )

        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": schema2,
                "optimize_for": optimize_for,
            },
            force=True,
        )

        write_table(
            "//tmp/t1",
            [
                {
                    "column": "foobar",
                    "int": 53,
                },
                {
                    "column": "lol",
                    "int": 42,
                },
            ],
        )

        write_table(
            "//tmp/t2",
            [
                {
                    "column": 53
                },
                {
                    "column": 42
                },
                {
                    "column": 179
                }
            ]
        )

        create(
            "table",
            "//tmp/t_out",
            force=True,
        )

        op = map(
            in_=["//tmp/t1", "//tmp/t2"],
            out=["//tmp/t_out"],
            command="cat 1>&2",
            spec={"mapper": {"input_format": ARROW_FORMAT}},
        )

        assert read_table("//tmp/t_out") == []

        job_ids = op.list_jobs()
        stderr_bytes = op.read_stderr(job_ids[0])
        buffer = pa.py_buffer(stderr_bytes)

        reader = pa.BufferReader(buffer)
        table1 = pa.ipc.open_stream(reader).read_all()

        reader.read(4)
        table2 = pa.ipc.open_stream(reader).read_all()
        if len(table1.column_names) == 1:
            table1, table2 = table2, table1

        column_names = table1.column_names
        assert column_names[0] == "column"
        assert column_names[1] == "int"
        assert table1[column_names[0]].to_pylist() == [b'foobar', b'lol']
        assert table1[column_names[1]].to_pylist() == [53, 42]

        column_names = table2.column_names
        assert column_names[0] == "column"
        assert table2[column_names[0]].to_pylist() == [53, 42, 179]

        row_batch_count, columnar_batch_count = self.get_row_and_columnar_batch_count(op)

        if optimize_for == "scan":
            assert row_batch_count == 0 and columnar_batch_count > 0
        else:
            assert row_batch_count > 0 and columnar_batch_count == 0


@authors("nadya73")
@pytest.mark.enabled_multidaemon
class TestArrowIntegerColumn_YTADMINREQ_34427(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    ENABLE_MULTIDAEMON = True

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

        parsed_table = parse_arrow_stream(read_table("//tmp/t_in", output_format=ARROW_FORMAT, control_attributes={"enable_row_index": True, "enable_range_index": True}))
        column_names = parsed_table.column_names

        assert "a" in column_names
        assert parsed_table["a"].to_pylist() == [0, 1, 2, 3, 4, 5]
