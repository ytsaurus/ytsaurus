from yt.testlib import authors

from yt.wrapper.testlib.helpers import TEST_DIR

import yt.wrapper as yt

import pytest


@yt.yt_dataclass
class Row:
    part_index: int
    data: bytes


@yt.yt_dataclass
class CustomRow:
    my_part_index: int
    my_data: bytes


@pytest.mark.usefixtures("yt_env")
class TestBlobFormat(object):
    part_size = 4 * yt.common.MB

    def _create_format(self, **kwargs):
        return yt.format.BlobFormat(**kwargs)

    @authors("achains")
    def test_simple_blob(self):
        table = TEST_DIR + "/simple_blob"

        data_first_part = b"x" * self.part_size
        data_second_part = b"y" * self.part_size
        yt.write_table_structured(
            table,
            Row,
            [
                Row(part_index=0, data=data_first_part),
                Row(part_index=1, data=data_second_part),
            ],
        )

        blob_dump = yt.read_table(table, format=self._create_format(raw=True))
        assert blob_dump.read() == data_first_part + data_second_part

    @authors("achains")
    def test_multiple_files(self):
        table = TEST_DIR + "/multiple_files"

        data_first_part = b"x" * self.part_size
        data_second_part = b"y" * self.part_size

        yt.write_table_structured(
            table,
            Row,
            [
                Row(part_index=0, data=data_first_part),
                Row(part_index=1, data=data_second_part),
                Row(part_index=0, data=b"data"),
            ],
        )

        with pytest.raises(yt.YtError):
            yt.read_table(table, format=self._create_format(raw=True))

    @authors("achains")
    def test_custom_column_names(self):
        table = TEST_DIR + "/custom_column_names"

        data_first_part = b"x" * self.part_size
        data_second_part = b"y" * self.part_size

        yt.write_table_structured(
            table,
            CustomRow,
            [
                CustomRow(my_part_index=0, my_data=data_first_part),
                CustomRow(my_part_index=1, my_data=data_second_part),
            ],
        )

        blob_dump = yt.read_table(
            table,
            format=self._create_format(
                raw=True, attributes={"part_index_column_name": "my_part_index", "data_column_name": "my_data"}
            ),
        )
        assert blob_dump.read() == data_first_part + data_second_part
