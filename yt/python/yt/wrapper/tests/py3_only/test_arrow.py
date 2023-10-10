from yt.testlib import authors

from yt.wrapper.testlib.helpers import TEST_DIR

import yt.wrapper as yt

import pytest

import pyarrow as pa


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
