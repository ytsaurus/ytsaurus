from __future__ import with_statement, print_function

from yt.testlib import authors
from yt.wrapper.testlib.helpers import TEST_DIR, set_config_option, inject_http_error

import yt.wrapper as yt

from yt.wrapper.schema import TableSchema

import yt.type_info as type_info

import pytest

import os

import pandas
import pyarrow
import pyarrow.parquet

import random
import string
import tempfile

LONG_STRING = "abcdefghijklmnopqrst"
A_STRING = "aaaaaaaaaaaaaaaaaaaaaaa"

LONG_STRING_BIN = b"abcdefghijklmnopqrst"
A_STRING_BIN = b"aaaaaaaaaaaaaaaaaaaaaaa"
CAT = "cat"
FOO = "foofoofoofoofoofoo"
BAR = "barbarbarbarbarbarbar"


@yt.yt_dataclass
class Row:
    key: str
    value: int


def decode_utf8_list(array):
    decode_array = []
    for i in range(len(array)):
        decode_array.append(array[i].decode())
    return decode_array


def generate_random_string(length):
    letters = string.ascii_lowercase
    rand_string = "".join(random.choice(letters) for i in range(length))
    return rand_string


def check_table(table, chunks):
    column_names = table.column_names
    for column_name in column_names:
        column_elements = table[column_name].to_pylist()
        column_elements_index = 0
        for chunk_index in range(len(chunks)):
            for index in range(len(chunks[chunk_index])):
                elem = column_elements[column_elements_index]
                column_elements_index += 1
                if (isinstance(elem, bytes)):
                    elem = elem.decode()
                assert chunks[chunk_index][index][column_name] == elem


@pytest.mark.usefixtures("yt_env_v4")
class TestParquet(object):
    @authors("nadya02")
    @pytest.mark.parametrize("enable_parallel", [True, False])
    def test_dump_parquet(self, enable_parallel):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            table = TEST_DIR + "/table"

            yt.write_table_structured(table, Row, [
                Row(key="one", value=1),
                Row(key="two", value=2),
                Row(key="three", value=3),
            ])

            with set_config_option("read_parallel/enable", enable_parallel):
                yt.dump_parquet(table, filename, enable_several_files=False)

            table = pyarrow.parquet.read_table(filename)
            column_names = table.column_names

            assert column_names[0] == "key"
            assert table[column_names[0]].to_pylist() == ["one", "two", "three"]
            assert column_names[1] == "value"
            assert table[column_names[1]].to_pylist() == [1, 2, 3]

    @authors("nadya02")
    @pytest.mark.parametrize("enable_parallel", [True, False])
    @pytest.mark.parametrize("unordered", [True, False])
    def test_multi_chunks_dump_parquet(self, enable_parallel, unordered):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            table = TEST_DIR + "/table"

            yt.create("table", table, attributes={"schema": [{"name": "index", "type": "int64"}]})

            for i in range(5):
                yt.write_table(yt.TablePath(table, append=True), [{"index": i}])

            with set_config_option("read_parallel/enable", enable_parallel):
                yt.dump_parquet(table, filename, enable_several_files=False, unordered=unordered)

            table = pyarrow.parquet.read_table(filename)
            column_names = table.column_names

            assert column_names[0] == "index"

            if unordered:
                assert sorted(table[column_names[0]].to_pylist()) == [0, 1, 2, 3, 4]
            else:
                assert table[column_names[0]].to_pylist() == [0, 1, 2, 3, 4]

    @authors("nadya02")
    @pytest.mark.parametrize("enable_parallel", [True, False])
    def test_dictionary_dump_parquet(self, enable_parallel):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            table = TEST_DIR + "/table"

            yt.create(
                "table",
                table,
                attributes={
                    "schema": [{"name": "string", "type": "string"}, {"name": "index", "type": "int64"}],
                    "optimize_for": "scan"})

            dictionary_array = [LONG_STRING, A_STRING, A_STRING, LONG_STRING, A_STRING, LONG_STRING]
            dense_array = ["one", "two", "three"]

            yt.write_table(yt.TablePath(table, append=True), [
                {"string": dictionary_array[0], "index": 1},
                {"string": dictionary_array[1], "index": 2},
                {"string": dictionary_array[2], "index": 3},
                {"string": dictionary_array[3], "index": 4},
                {"string": dictionary_array[4], "index": 5},
                {"string": dictionary_array[5], "index": 6},
            ])

            yt.write_table(yt.TablePath(table, append=True), [
                {"string": dense_array[0], "index": 1},
                {"string": dense_array[1], "index": 2},
                {"string": dense_array[2], "index": 3},
            ])

            with set_config_option("read_parallel/enable", enable_parallel):
                yt.dump_parquet(table, filename, enable_several_files=False)

            table = pyarrow.parquet.read_table(filename)

            column_names = table.column_names

            assert column_names[0] == "string"
            assert decode_utf8_list(table[column_names[0]].to_pylist()) == dictionary_array + ["one", "two", "three"]

            assert column_names[1] == "index"
            assert table[column_names[1]].to_pylist() == [1, 2, 3, 4, 5, 6, 1, 2, 3]

    @authors("nadya02")
    def test_empty_dump_parquet(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            table = TEST_DIR + "/empty_table"

            yt.create("table", table, attributes={"schema": [{"name": "key", "type": "string"}]})
            yt.dump_parquet(table, filename, enable_several_files=False)

            destination = yt.smart_upload_file(filename)
            assert yt.read_file(destination).read() == b""

    @authors("nadya02")
    @pytest.mark.parametrize("enable_parallel", [True, False])
    def test_complex_dump_parquet(self, enable_parallel):
        random.seed(10)

        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name

            table = TEST_DIR + "/framing_table"

            yt.create(
                "table",
                table,
                attributes={
                    "schema": [
                        {"name": "string", "type": "string"},
                        {"name": "int", "type": "int64"},
                        {"name": "enum_string", "type": "string"},
                        {"name": "opt_string", "type_v3": {"type_name": "optional", "item": "string"}},
                        {"name": "double", "type_v3": "double"},
                        {"name": "second_enum_string", "type": "string"},
                        {"name": "bool", "type_v3": "bool"},
                        {"name": "constant_string", "type": "string"}
                    ],
                    "optimize_for": "scan",
                })

            enum_array = [LONG_STRING, A_STRING, CAT, FOO, BAR]

            chunks = []
            for i in range(10):
                chunk = []
                for j in range(10):
                    chunk.append({
                        "string": generate_random_string(random.randint(1, 20)),
                        "int": random.randint(1, 1_000_000_000),
                        "enum_string": random.choice(enum_array),
                        "opt_string": random.choice([None, generate_random_string(random.randint(1, 20))]),
                        "double": random.uniform(0, 1e9),
                        "second_enum_string": random.choice([random.choice(enum_array), generate_random_string(8)]),
                        "bool": random.choice([True, False]),
                        "constant_string": LONG_STRING,
                    })
                chunks.append(chunk)

                yt.write_table(
                    yt.TablePath(table, append=True),
                    chunk)

            with set_config_option("read_parallel/enable", enable_parallel):
                yt.dump_parquet(table, filename, enable_several_files=False)

            table = pyarrow.parquet.read_table(filename)

            assert table.column_names == ["string", "int", "enum_string", "opt_string", "double", "second_enum_string", "bool", "constant_string"]
            check_table(table, chunks)

    @authors("nadya02")
    def test_several_files_parquet(self):
        with tempfile.TemporaryDirectory() as dirname:
            table = TEST_DIR + "/table"
            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String]) \
                .add_column("bool", type_info.Optional[type_info.Bool])

            yt.create("table", table, attributes={"schema": schema})
            rows = [
                {"key": 1, "value": "one", "bool" : True},
                {"key": 2, "value": "two", "bool" : True},
                {"key": 3, "value": "three", "bool" : True}
            ]
            yt.write_table(table, rows)

            with set_config_option("read_parallel/data_size_per_thread", 1):
                with set_config_option("read_parallel/enable", True):
                    yt.dump_parquet(table, dirname, enable_several_files=True)

            output_table = TEST_DIR + "/output_table"

            for root, _, files in os.walk(dirname):
                for file in sorted(files):
                    full_path = os.path.join(root, file)
                    yt.upload_parquet(yt.TablePath(output_table, append=True), full_path)

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == list(yt.read_table(table))

    @authors("nadya02")
    def test_multi_chunks_several_files_parquet(self):
        with tempfile.TemporaryDirectory() as dirname:
            input_table = TEST_DIR + "/table"

            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String])

            yt.create("table", input_table, attributes={"schema": schema})
            rows_in_chunk = 10

            for i in range(5):
                rows = []
                for j in range(rows_in_chunk):
                    rows.append({"key": i * rows_in_chunk + j, "value": generate_random_string(5)})
                yt.write_table(yt.TablePath(input_table, append=True), rows)

            with set_config_option("read_parallel/data_size_per_thread", 5):
                with set_config_option("read_parallel/enable", True):
                    yt.dump_parquet(input_table, dirname, enable_several_files=True)

            output_table = TEST_DIR + "/output_table"

            for root, _, files in os.walk(dirname):
                for file in sorted(files):
                    full_path = os.path.join(root, file)
                    yt.upload_parquet(yt.TablePath(output_table, append=True), full_path)

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == list(yt.read_table(input_table))

    @authors("nadya02")
    def test_upload_parquet(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            table = TEST_DIR + "/table"
            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String]) \
                .add_column("bool", type_info.Optional[type_info.Bool])

            yt.create("table", table, attributes={"schema": schema})
            row = {"key": 1, "value": "one", "bool" : True}
            yt.write_table(table, [row])
            yt.dump_parquet(table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_parquet(output_table, filename)

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == list(yt.read_table(table))

    @authors("nadya02")
    def test_multi_chunks_upload_parquet(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            input_table = TEST_DIR + "/table"

            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String])

            yt.create("table", input_table, attributes={"schema": schema})
            rows_in_chunk = 10

            for i in range(5):
                rows = []
                for j in range(rows_in_chunk):
                    rows.append({"key": i * rows_in_chunk + j, "value": generate_random_string(5)})
                yt.write_table(yt.TablePath(input_table, append=True), rows)

            yt.dump_parquet(input_table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_parquet(output_table, filename)

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == list(yt.read_table(input_table))

    @authors("nadya02")
    def test_optional_upload_parquet(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name

            data = [
                pyarrow.array([1, 2, 3]),
                pyarrow.array([1.4, None, 2.5]),
            ]

            fields = [
                pyarrow.field("x", pyarrow.int64()),
                pyarrow.field("y", pyarrow.float64()),
            ]

            arrow_schema = pyarrow.schema(fields)
            parquet_table = pyarrow.Table.from_arrays(data, schema=arrow_schema)

            pyarrow.parquet.write_table(parquet_table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_parquet(output_table, filename)

            schema = TableSchema() \
                .add_column("x", type_info.Optional[type_info.Int64]) \
                .add_column("y", type_info.Optional[type_info.Double])

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == [{"x": 1, "y": 1.4}, {"x": 2, "y": None}, {"x": 3, "y": 2.5}]

    @authors("nadya02")
    def test_double_list_with_struct_upload_parquet(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name

            data = {
                "x": [1, 2],
                "y": [[[{"foo": 1}, {"foo": 3}]], [[{"foo": 1}], []]]
            }

            df = pandas.DataFrame(data)

            table = pyarrow.Table.from_pandas(df)

            pyarrow.parquet.write_table(table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_parquet(output_table, filename)

            schema = TableSchema() \
                .add_column("x", type_info.Optional[type_info.Int64]) \
                .add_column("y", type_info.Optional[type_info.List[
                    type_info.Optional[type_info.List[type_info.Optional[type_info.Struct[
                        "foo": type_info.Optional[type_info.Int64]]]]]]])

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == [
                {"x": 1, "y": [[{"foo": 1}, {"foo": 3}]]},
                {"x": 2, "y": [[{"foo": 1}], []]}
            ]

    @authors("nadya02")
    def test_double_list_upload_parquet(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name

            data = {
                "x": [1, 2],
                "y": [[[1, 3]], [[5], []]]
            }

            df = pandas.DataFrame(data)

            table = pyarrow.Table.from_pandas(df)

            pyarrow.parquet.write_table(table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_parquet(output_table, filename)

            schema = TableSchema() \
                .add_column("x", type_info.Optional[type_info.Int64]) \
                .add_column("y", type_info.Optional[type_info.List[
                    type_info.Optional[type_info.List[type_info.Optional[type_info.Int64]]]]])

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == [
                {"x": 1, "y": [[1, 3]]},
                {"x": 2, "y": [[5], []]}
            ]

    @authors("nadya02")
    def test_double_struct_upload_parquet(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name

            struct_data = [
                {"foo": {"a": 1, "b": "one"}, "bar": "ten"},
                {"foo": {"a": 2, "b": "two"}, "bar": "twenty"},
                {"foo": {"a": 3, "b": "three"}, "bar": "thirty"},
            ]
            struct_type = pyarrow.struct([
                pyarrow.field("a", pyarrow.uint8()),
                pyarrow.field("b", pyarrow.string())
            ])

            inner_struct_type = pyarrow.struct([pyarrow.field("foo", struct_type), pyarrow.field("bar", pyarrow.string())])

            struct_array = pyarrow.array(struct_data, type=inner_struct_type)

            data = [
                pyarrow.array([1, 2, 3]),
                pyarrow.array([1.4, 3.14, 2.5]),
                struct_array
            ]

            fields = [
                pyarrow.field("x", pyarrow.int64()),
                pyarrow.field("y", pyarrow.float64()),
                pyarrow.field("z", inner_struct_type)
            ]

            arrow_schema = pyarrow.schema(fields)
            parquet_table = pyarrow.Table.from_arrays(data, schema=arrow_schema)

            pyarrow.parquet.write_table(parquet_table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_parquet(output_table, filename)

            schema = TableSchema() \
                .add_column("x", type_info.Optional[type_info.Int64]) \
                .add_column("y", type_info.Optional[type_info.Double]) \
                .add_column("z", type_info.Optional[type_info.Struct[
                    "foo": type_info.Optional[type_info.Struct[
                        "a": type_info.Optional[type_info.Uint8],
                        "b": type_info.Optional[type_info.String]]],
                    "bar": type_info.Optional[type_info.String]]])

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == [
                {"x": 1, "y": 1.4, "z": struct_data[0]},
                {"x": 2, "y": 3.14, "z": struct_data[1]},
                {"x": 3, "y": 2.5, "z": struct_data[2]}
            ]

    @authors("nadya02")
    def test_upload_parquet_retry(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            client = yt.YtClient(config=yt.config.config)

            client.config["write_retries"]["chunk_size"] = 500
            client.config["arrow_options"]["write_arrow_batch_size"] = 10

            filename = temp_file.name
            input_table = "//tmp/dump_parquet"

            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String])

            client.create("table", input_table, attributes={"schema": schema})
            rows_in_chunk = 20

            for i in range(5):
                rows = []
                for j in range(rows_in_chunk):
                    rows.append({"key": i * rows_in_chunk + j, "value": generate_random_string(5)})
                client.write_table(yt.TablePath(input_table, append=True), rows)

            client.dump_parquet(input_table, filename)

            output_table = "//tmp/upload_parquet"

            with inject_http_error(client, filter_url='/write_table', interrupt_from=0, interrupt_till=999, interrupt_every=2, raise_connection_reset=True) as cnt:
                client.upload_parquet(output_table, filename)

            assert list(yt.read_table(output_table)) == list(yt.read_table(input_table))
            assert client.get(output_table + "/@row_count") == 100
            assert client.get(output_table + "/@chunk_count") == 10
            assert cnt.filtered_total_calls == 20
            assert cnt.filtered_raises == 10
