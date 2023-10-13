from __future__ import with_statement, print_function

from yt.testlib import authors
from yt.wrapper.testlib.helpers import TEST_DIR

import yt.wrapper as yt

import pytest
import tempfile

import random
import string

import pyarrow.parquet as pq
import sys

LONG_STRING = "abcdefghijklmnopqrst"
A_STRING = "aaaaaaaaaaaaaaaaaaaaaaa"

LONG_STRING_BIN = b'abcdefghijklmnopqrst'
A_STRING_BIN = b'aaaaaaaaaaaaaaaaaaaaaaa'
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
    rand_string = ''.join(random.choice(letters) for i in range(length))
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


@pytest.mark.usefixtures("yt_env")
class TestParquete(object):

    @authors("nadya02")
    def test_dump_parquete(self):
        fd, filename = tempfile.mkstemp()
        table = TEST_DIR + "/table"

        yt.write_table_structured(table, Row, [
            Row(key="one", value=1),
            Row(key="two", value=2),
            Row(key="three", value=3),
        ])

        yt.dump_parquete(table, filename)

        table = pq.read_table(filename)
        column_names = table.column_names

        assert column_names[0] == "key"
        assert table[column_names[0]].to_pylist() == ["one", "two", "three"]
        assert column_names[1] == "value"
        assert table[column_names[1]].to_pylist() == [1, 2, 3]

    @authors("nadya02")
    def test_multi_chunks_parquete(self):
        _, filename = tempfile.mkstemp()
        table = TEST_DIR + "/table"

        yt.create("table", table, attributes={"schema": [{"name": "index", "type": "int64"}]})

        for i in range(5):
            yt.write_table(yt.TablePath(table, append=True), [{"index": i}])

        yt.dump_parquete(table, filename)

        table = pq.read_table(filename)
        column_names = table.column_names

        assert column_names[0] == "index"
        assert table[column_names[0]].to_pylist() == [0, 1, 2, 3, 4]

    @authors("nadya02")
    def test_dictionary_parquete(self):
        _, filename = tempfile.mkstemp()
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

        yt.dump_parquete(table, filename)

        table = pq.read_table(filename)
        print(table, file=sys.stderr)

        column_names = table.column_names

        assert column_names[0] == "string"
        assert decode_utf8_list(table[column_names[0]].to_pylist()) == dictionary_array + ["one", "two", "three"]

        assert column_names[1] == "index"
        assert table[column_names[1]].to_pylist() == [1, 2, 3, 4, 5, 6, 1, 2, 3]

    @authors("nadya02")
    def test_empty_parquete(self):
        fd, filename = tempfile.mkstemp()
        table = TEST_DIR + "/empty_table"

        yt.create("table", table, attributes={"schema": [{"name": "key", "type": "string"}]})
        yt.dump_parquete(table, filename)

        destination = yt.smart_upload_file(filename)
        assert yt.read_file(destination).read() == b""

    @authors("nadya02")
    def test_complex_parquete(self):
        random.seed(10)

        _, filename = tempfile.mkstemp()

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
                    "int": random.randint(1, 1e9),
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

        yt.dump_parquete(table, filename)

        table = pq.read_table(filename)

        assert table.column_names == ["string", "int", "enum_string", "opt_string", "double", "second_enum_string", "bool", "constant_string"]
        check_table(table, chunks)
