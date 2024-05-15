from yt.testlib import authors

from yt.wrapper.testlib.helpers import TEST_DIR

import yt.wrapper as yt

import pytest

import pyarrow as pa

from yt.wrapper.schema import TableSchema

import yt.type_info as typing


@yt.yt_dataclass
class Row:
    key: str
    value: int


@pytest.mark.usefixtures("yt_env")
class TestArrow(object):
    def _create_format(self, **kwargs):
        return yt.format.ArrowFormat(**kwargs)

    def _parse_arrow_stream(self, data):
        return pa.ipc.open_stream(data).read_all()

    def _get_serialized_arrow(self, arrow_table):
        sink = pa.BufferOutputStream()
        writer = pa.ipc.RecordBatchStreamWriter(sink, arrow_table.schema)
        writer.write(arrow_table)
        writer.close()
        return sink.getvalue().to_pybytes()

    @authors("nadya02")
    def test_arrow_reader(self):
        table = TEST_DIR + "/table"

        yt.write_table_structured(table, Row, [
            Row(key="one", value=1),
            Row(key="two", value=2),
            Row(key="three", value=3),
        ])

        stream = yt.read_table(table, format=self._create_format(raw=True))
        parsed_table = self._parse_arrow_stream(stream.read())
        column_names = parsed_table.column_names

        assert column_names[0] == "key"
        assert parsed_table[column_names[0]].to_pylist() == ["one", "two", "three"]
        assert column_names[1] == "value"
        assert parsed_table[column_names[1]].to_pylist() == [1, 2, 3]

    @authors("nadya02")
    def test_arrow_multi_chunks_reader(self):
        table = TEST_DIR + "/empty_table"
        yt.create("table", table, attributes={"schema": [{"name": "index", "type": "int64"}]})

        for i in range(3):
            yt.write_table("<append=%true>" + table, [{"index": i}])

        stream = yt.read_table(table, format=self._create_format(raw=True))
        parsed_table = self._parse_arrow_stream(stream.read())

        column_names = parsed_table.column_names
        assert column_names[0] == "index"
        assert parsed_table[column_names[0]].to_pylist() == [0, 1, 2]

    @authors("nadya02")
    def test_empty_arrow_reader(self):
        table = TEST_DIR + "/empty_table"
        yt.create("table", table, attributes={"schema": [{"name": "key", "type": "string"}]})
        res_arrow = yt.read_table(table, format=self._create_format(raw=True))
        assert len(res_arrow.read()) == 0

    @authors("nadya02")
    def test_arrow_writer(self):
        schema = TableSchema() \
            .add_column("x", typing.Int64) \
            .add_column("z", typing.Struct[
                "bar": typing.String,
            ])

        struct_data = [
            {"bar": "ten"},
            {"bar": "twenty"},
        ]

        inner_struct_type = pa.struct([pa.field("bar", pa.string())])

        struct_array = pa.array(struct_data, type=inner_struct_type)

        data = [
            pa.array([1, 2]),
            struct_array
        ]

        fields = [
            pa.field("x", pa.int64()),
            pa.field("z", inner_struct_type)
        ]

        arrow_schema = pa.schema(fields)
        arrow_table = pa.Table.from_arrays(data, schema=arrow_schema)

        output_table = TEST_DIR + "/output_table"

        yt.create("table", output_table, attributes={"schema": schema})

        yt.write_table(output_table, self._get_serialized_arrow(arrow_table), format=self._create_format(), raw=True)

        assert list(yt.read_table(output_table)) == [
            {'x': 1, 'z': {'bar': 'ten'}},
            {'x': 2, 'z': {'bar': 'twenty'}}
        ]

    @authors("nadya02")
    def test_complex_arrow_writer(self):
        schema = TableSchema() \
            .add_column("x", typing.Int64) \
            .add_column("y", typing.Optional[typing.Double]) \
            .add_column("z", typing.Struct[
                "foo": typing.Struct["a": typing.Uint8, "b": typing.String],
                "bar": typing.String,
            ])

        struct_data = [
            {"foo": {"a": 1, "b": "one"}, "bar": "ten"},
            {"foo": {"a": 2, "b": "two"}, "bar": "twenty"},
            {"foo": {"a": 3, "b": "three"}, "bar": "thirty"},
        ]
        struct_type = pa.struct([
            pa.field("a", pa.uint8()),
            pa.field("b", pa.string())
        ])

        inner_struct_type = pa.struct([pa.field("foo", struct_type), pa.field("bar", pa.string())])

        struct_array = pa.array(struct_data, type=inner_struct_type)

        data = [
            pa.array([1, 2, 3]),
            pa.array([1.4, 3.14, 2.5]),
            struct_array
        ]

        fields = [
            pa.field("x", pa.int64()),
            pa.field("y", pa.float64()),
            pa.field("z", inner_struct_type)
        ]

        arrow_schema = pa.schema(fields)
        arrow_table = pa.Table.from_arrays(data, schema=arrow_schema)

        output_table = TEST_DIR + "/output_table"

        yt.create("table", output_table, attributes={"schema": schema})

        yt.write_table(output_table, self._get_serialized_arrow(arrow_table), format=self._create_format(), raw=True)

        assert list(yt.read_table(output_table)) == [
            {'x': 1, 'y': 1.4, 'z': struct_data[0]},
            {'x': 2, 'y': 3.14, 'z': struct_data[1]},
            {'x': 3, 'y': 2.5, 'z': struct_data[2]}
        ]
