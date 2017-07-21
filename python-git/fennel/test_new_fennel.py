from .new_fennel import StringIOWrapper, round_down_to, monitor, convert_rows_to_chunks, make_read_tasks

from yt.packages.six.moves import xrange

from datetime import datetime, timedelta

try:
    from cStringIO import StringIO
except ImportError:  # Python 3
    from io import BytesIO as StringIO

from gzip import GzipFile

import pytest
pytestmark = pytest.mark.skipif("sys.version_info[:2] != (2, 7)", reason="requires python2.7")

def test_string_io_wrapper():
    stream = StringIOWrapper()

    stream.write("a")
    assert stream.size == 1

    stream.write("abcd")
    assert stream.size == 5

def test_round_down_to():
    assert round_down_to(10, 5) == 10
    assert round_down_to(14, 5) == 10
    assert round_down_to(15, 5) == 15
    assert round_down_to(100, 7) == 98


def test_monitor():
    class MockYtClient(object):
        def __init__(self, shift):
            self.shift = shift
        def get(self, *args, **kwargs):
            return datetime.strftime(datetime.utcnow() - self.shift, "%Y-%m-%dT%H:%M:%S.%z")

    output = StringIO()
    monitor(MockYtClient(timedelta(minutes=5)), "", 10, output)
    assert output.getvalue().startswith("0; Lag equals to:")

    output = StringIO()
    monitor(MockYtClient(timedelta(minutes=15)), "", 10, output)
    assert output.getvalue().startswith("2; Lag equals to:")


def test_convert_rows_to_chunks():
    def encode(str):
        output = StringIO()
        with GzipFile(fileobj=output, mode="a", mtime=0.0) as zipped_stream:
            zipped_stream.write(str)
        return output.getvalue()

    row = {"key": "value"}
    encoded_row = encode("tskv\tkey=value\n")
    start_header = ("\x01" + "\x00" * 7) * 2 + "\x00" * 8

    chunks = list(convert_rows_to_chunks([row], chunk_size=1024, seqno=0))
    assert len(chunks) == 1
    data, row_count = chunks[0]
    assert row_count == 1
    assert data == start_header + encoded_row

    row_count_to_have_two_chunks = (1024 / len(encoded_row)) + 5
    chunks = list(convert_rows_to_chunks([row] * row_count_to_have_two_chunks, chunk_size=1024, seqno=0))
    assert len(chunks) == 2

    data, row_count1 = chunks[0]
    assert data == start_header + encoded_row * row_count1

    data, row_count2 = chunks[1]
    assert data[24:] == encoded_row * row_count2

    assert row_count1 + row_count2 == row_count_to_have_two_chunks

def test_make_read_tasks_simple_cases():
    class MockYtClient(object):
        def get(self, *args, **kwargs):
            return {"processed_row_count": 0, "table_start_row_index": 0, "row_count": 10}

    def make_range(start_index, end_index):
        return {"lower_limit": {"row_index": start_index}, "upper_limit": {"row_index": end_index}}

    tasks, row_count = make_read_tasks(
        MockYtClient(),
        "", # fake table path
        1, # session_count
        1, # range_row_count
        1) # max_range_count
    assert row_count == 1
    assert tasks == [(0, 0, [make_range(0, 1)])]

    tasks, row_count = make_read_tasks(
        MockYtClient(),
        "", # fake table path
        1, # session_count
        1, # range_row_count
        100) # max_range_count
    assert row_count == 10
    assert tasks == [(0, 0, [make_range(i, i + 1) for i in xrange(10)])]

    tasks, row_count = make_read_tasks(
        MockYtClient(),
        "", # fake table path
        1, # session_count
        3, # range_row_count
        3) # max_range_count
    assert row_count == 9
    assert tasks == [(0, 0, [make_range(i * 3, (i + 1) * 3) for i in xrange(3)])]

    tasks, row_count = make_read_tasks(
        MockYtClient(),
        "", # fake table path
        1, # session_count
        3, # range_row_count
        4) # max_range_count
    assert row_count == 10
    assert tasks == [(0, 0, [make_range(i * 3, (i + 1) * 3) for i in xrange(3)] + [make_range(9, 10)])]

    tasks, row_count = make_read_tasks(
        MockYtClient(),
        "", # fake table path
        3, # session_count
        2, # range_row_count
        3) # max_range_count
    assert row_count == 10
    assert tasks == [
        (0, 0, [make_range(0, 2), make_range(6, 8)]),
        (1, 0, [make_range(2, 4), make_range(8, 10)]),
        (2, 0, [make_range(4, 6)]),
    ]

def test_make_read_tasks_complicated_cases():
    class MockYtClient(object):
        def get(self, *args, **kwargs):
            return {"processed_row_count": 17, "table_start_row_index": 50, "row_count": 50}

    def make_range(start_index, end_index):
        return {"lower_limit": {"row_index": start_index}, "upper_limit": {"row_index": end_index}}

    tasks, row_count = make_read_tasks(
        MockYtClient(),
        "", # fake table path
        3, # session_count
        3, # range_row_count
        3) # max_range_count
    assert row_count == 26
    assert tasks == [
        (0, 24, [make_range(22, 25), make_range(31, 34), make_range(40, 43)]),
        (1, 22, [make_range(17, 19), make_range(25, 28), make_range(34, 37)]),
        (2, 21, [make_range(19, 22), make_range(28, 31), make_range(37, 40)]),
    ]
