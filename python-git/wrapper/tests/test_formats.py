from yt.wrapper.string_iter_io import StringIterIO
import yt.yson as yson
from yt.wrapper.format import extract_key, create_format

from yt.packages.six import byte2int, iterbytes, PY3
from yt.packages.six.moves import xrange

import yt.wrapper as yt

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

import pytest
import random
import time
from itertools import chain

def test_string_iter_io_read():
    strings = [b"ab", b"", b"c", b"de", b""]

    for add_eoln in [False, True]:
        io = StringIterIO(iter(strings), add_eoln)
        sep = b"\n" if add_eoln else b""
        for c in iterbytes(sep.join(strings)):
            assert c == byte2int(io.read(1))

def test_string_iter_io_readline():
    strings = [b"ab", b"", b"c\n", b"d\ne", b""]

    for add_eoln in [False, True]:
        io = StringIterIO(iter(strings), add_eoln)
        sep = b"\n" if add_eoln else b""
        lines = sep.join(strings).split(b"\n")
        for i in xrange(len(lines)):
            line = lines[i]
            if i + 1 != len(lines):
                line += b"\n"
            assert line == io.readline()

def check_format(format, strings, row):
    # In some formats (e.g. dsv) dict '{"a": "b", "c": "d"}' can be
    # dumped to "a=b\tc=d\n" or "c=d\ta=b\n" and both variants are
    # correct. So strings list contain all valid representations of row and
    # checks are performed appropriately.
    if not isinstance(strings, list):
        strings = [strings]

    assert all(format.loads_row(s) == row for s in strings)
    if format.is_raw_load_supported():
        assert all(format.load_row(BytesIO(s), raw=True) == s for s in strings)
    assert all(format.load_row(BytesIO(s)) == row for s in strings)
    assert list(chain.from_iterable(format.load_rows(BytesIO(s)) for s in strings)) == \
        [row] * len(strings)

    assert any(format.dumps_row(row) == s for s in strings)
    stream = BytesIO()
    format.dump_row(row, stream)
    assert any(stream.getvalue().rstrip(b"\n") == s.rstrip(b"\n") for s in strings)

def check_table_index(format, raw_row, raw_table_switcher, rows, process_output=None):
    input_stream = BytesIO(b"".join([raw_row, raw_table_switcher, raw_row]))
    input_stream_str = input_stream.getvalue()
    parsed_rows = list(format.load_rows(input_stream))
    assert parsed_rows == rows

    output_stream = BytesIO()
    format.dump_rows(rows, output_stream)
    output_stream_str = output_stream.getvalue()
    if process_output:
        output_stream_str = process_output(output_stream_str)

    assert output_stream_str == input_stream_str

def test_yson_format():
    format = yt.YsonFormat(format="text")
    row = {"a": 1, "b": 2}
    serialized_row = b'{"a"=1;"b"=2}'
    yson_rows = list(format.load_rows(BytesIO(serialized_row)))
    assert yson_rows == [row]
    stream = BytesIO()
    format.dump_row(row, stream)
    # COMPAT: '.replace(";}", "}")' is for compatibility with different yson representations in different branches.
    assert stream.getvalue().rstrip(b";\n").replace(b";}", b"}") in [serialized_row, b'{"b"=2;"a"=1}']

    format = yt.YsonFormat(format="binary")
    assert format.dumps_row({"a": 1}).rstrip(b";\n") == yson.dumps({"a": 1}, yson_format="binary")

def test_yson_table_switch():
    format = yt.YsonFormat(control_attributes_mode="row_fields", format="text")
    input = b'<"row_index"=0>#;{"a"=1};\n<"table_index"=1>#;\n{"a"=1};{"b"=2}\n'

    yson_rows = format.load_rows(BytesIO(input))
    parsed_rows = [dict(yson) for yson in yson_rows]
    true_input_rows = [{"a": 1, "@table_index": None, "@row_index": 0},
                       {"a": 1, "@table_index": 1, "@row_index": 1},
                       {"b": 2, "@table_index": 1, "@row_index": 2}]
    assert true_input_rows == parsed_rows
    output_rows = [{"a": 1}, {"a": 1, "@table_index": 1}, {"b": 2, "@table_index": 1}]
    stream = BytesIO()
    format.dump_rows(output_rows, stream)
    dumped_output = stream.getvalue()
    # COMPAT: '.replace(";}", "}")' and '.replace(";>", ">")' is for compatibility with different yson representations in different branches.
    assert dumped_output.replace(b";}", b"}").replace(b";>", b">") == b'{"a"=1};\n<"table_index"=1>#;\n{"a"=1};\n{"b"=2};\n'

def test_yson_iterator_mode():
    format = yt.YsonFormat(control_attributes_mode="iterator")
    input = b'<"row_index"=0>#;<"range_index"=0>#;\n{"a"=1};\n<"row_index"=2>#;<"range_index"=3>#;<"table_index"=1>#;\n{"a"=1};\n{"b"=2};\n'

    iterator = format.load_rows(BytesIO(input))
    assert iterator.table_index is None
    assert iterator.row_index is None
    assert iterator.range_index is None
    assert next(iterator) == {"a": 1}
    assert iterator.table_index is None
    assert iterator.row_index == 0
    assert iterator.range_index == 0
    assert next(iterator) == {"a": 1}
    assert iterator.table_index == 1
    assert iterator.row_index == 2
    assert iterator.range_index == 3
    assert next(iterator) == {"b": 2}
    assert iterator.table_index == 1
    assert iterator.row_index == 3
    assert iterator.range_index == 3
    with pytest.raises(StopIteration):
        next(iterator)


def test_dsv_format():
    format = yt.DsvFormat()
    check_format(format, [b"a=1\tb=2\n", b"b=2\ta=1\n"], {"a": "1", "b": "2"})
    check_format(format, [b"a=1\\t\tb=2\n", b"b=2\ta=1\\t\n"], {"a": "1\t", "b": "2"})

def test_yamr_format():
    format = yt.YamrFormat(lenval=False, has_subkey=False)
    check_format(format, b"a\tb\n", yt.Record("a", "b"))
    check_format(format, b"a\tb\tc\n", yt.Record("a", "b\tc"))

    format = yt.YamrFormat(lenval=False, has_subkey=True)
    check_format(format, b"a\tb\tc\n", yt.Record("a", "b", "c"))
    check_format(format, b"a\t\tc\td\n", yt.Record("a", "", "c\td"))

    format = yt.YamrFormat(lenval=True, has_subkey=False)
    check_format(format, b"\x01\x00\x00\x00a\x01\x00\x00\x00b", yt.Record("a", "b"))

def test_yamr_load_records_raw():
    records = [b"a\tb\tc\n", b"d\te\tf\n", b"g\th\ti\n"]
    stream = BytesIO(b"".join(records))
    format = yt.YamrFormat(has_subkey=True)
    assert list(format.load_rows(stream, raw=True)) == records

def test_yamr_table_switcher():
    format = yt.YamrFormat(lenval=False, has_subkey=False)
    check_table_index(format, b"a\tb\n", b"1\n",
                      [yt.Record("a", "b", tableIndex=0), yt.Record("a", "b", tableIndex=1)])

    format = yt.YamrFormat(lenval=True, has_subkey=False)
    import struct
    table_switcher = struct.pack("ii", -1, 1)
    check_table_index(format, b"\x01\x00\x00\x00a\x01\x00\x00\x00b", table_switcher,
                      [yt.Record("a", "b", tableIndex=0), yt.Record("a", "b", tableIndex=1)])

def test_yamr_record_index():
    format = yt.YamrFormat(lenval=False, has_subkey=False)
    data = b"0\n" b"0\n" b"a\tb\n" b"1\n" b"5\n" b"c\td\n" b"e\tf\n"
    records = [yt.Record("a", "b", tableIndex=0, recordIndex=0),
               yt.Record("c", "d", tableIndex=1, recordIndex=5),
               yt.Record("e", "f", tableIndex=1, recordIndex=6)]
    assert list(format.load_rows(BytesIO(data))) == records

    format = yt.YamrFormat(lenval=True, has_subkey=False)
    import struct
    table_switch = struct.pack("ii", -1, 1)
    row_switch = struct.pack("ii", -4, 2)
    row = b"\x01\x00\x00\x00a\x01\x00\x00\x00b"
    data = table_switch + row_switch + row + row
    records = [yt.Record("a", "b", tableIndex=1, recordIndex=2),
               yt.Record("a", "b", tableIndex=1, recordIndex=3)]
    assert list(format.load_rows(BytesIO(data))) == records

def test_json_format():
    format = yt.JsonFormat(process_table_index=False, enable_ujson=False)
    check_format(format, b'{"a": 1}', {"a": 1})

    stream = BytesIO(b'{"a": 1}\n{"b": 2}')
    assert list(format.load_rows(stream)) == [{"a": 1}, {"b": 2}]

def test_json_format_table_index():
    format = yt.JsonFormat(process_table_index=True, enable_ujson=False)

    stream = BytesIO()
    format.dump_rows([{"a": 1, "@table_index": 1}], stream)
    assert stream.getvalue() in [b'{"$value": null, "$attributes": {"table_index": 1}}\n{"a": 1}\n',
                                 b'{"$attributes": {"table_index": 1}, "$value": null}\n{"a": 1}\n']

    assert [{"@table_index": 1, "a": 1}] == \
        list(format.load_rows(BytesIO(b'{"$value": null, "$attributes": {"table_index": 1}}\n'
                                      b'{"a": 1}')))

def test_json_format_row_index():
    format = yt.JsonFormat(process_table_index=None, control_attributes_mode="row_fields", enable_ujson=False)
    assert [{"@table_index": 1, "@row_index": 5, "a": 1},
            {"@table_index": 1, "@row_index": 6, "a": 2}] == \
        list(format.load_rows(BytesIO(b'{"$value": null, "$attributes": {"table_index": 1, "row_index": 5}}\n'
                                      b'{"a": 1}\n'
                                      b'{"a": 2}')))

def test_json_format_row_iterator():
    format = yt.JsonFormat(process_table_index=None, control_attributes_mode="iterator", enable_ujson=False)
    iterator = format.load_rows(BytesIO(b'{"$value": null, "$attributes": {"table_index": 1, "row_index": 5}}\n'
                                        b'{"a": 1}\n'
                                        b'{"a": 2}'))

    assert next(iterator) == {"a": 1}
    assert iterator.table_index == 1
    assert iterator.row_index == 5
    assert iterator.range_index is None

    assert next(iterator) == {"a": 2}
    assert iterator.table_index == 1
    assert iterator.row_index == 6
    assert iterator.range_index is None

def test_schemaful_dsv_format():
    format = yt.SchemafulDsvFormat(columns=["a", "b"])
    check_format(format, b"1\t2\n", {"a": "1", "b": "2"})
    check_format(format, b"\\n\t2\n", {"a": "\n", "b": "2"})

def test_yamred_dsv_format():
    format = yt.YamredDsvFormat(key_column_names=["a", "b"], subkey_column_names=["c"],
                                has_subkey=False, lenval=False)
    check_format(format, b"a\tb\n", yt.Record("a", "b"))
    check_format(format, b"a\tb\tc\n", yt.Record("a", "b\tc"))

def test_extract_key():
    assert extract_key(yt.Record("k", "s", "v"), ["k"]) == "k"
    assert extract_key(yt.Record("k", "v"), ["k"]) == "k"
    assert extract_key({"k": "v", "x": "y"}, ["k"]) == {"k": "v"}

def test_create_format():
    format = create_format("<has_subkey=%true>yamr")
    assert format.attributes["has_subkey"]
    assert format.name() == "yamr"
    assert format == yt.format.YamrFormat(has_subkey=True)
    assert format != yt.format.DsvFormat()

    for format_name in ["dsv", "yamred_dsv", "schemaful_dsv", "yson", "json"]:
        if format_name == "schemaful_dsv":
            format = create_format(format_name, columns=["x"])
        else:
            format = create_format(format_name)
        assert format.name() == format_name

    with pytest.raises(yt.YtError):
        create_format("best_format")

def test_raw_dump_records():
    def check_format(format, value):
        stream = BytesIO()
        if isinstance(value, list):
            format.dump_rows(value, stream, raw=True)
            assert stream.getvalue() == b"".join(value)
        else:
            format.dump_row(value, stream, raw=True)
            assert stream.getvalue() == value

    check_format(yt.YamrFormat(), b"1\t2\n")
    check_format(yt.YamredDsvFormat(), b"x=1\n")
    check_format(yt.SchemafulDsvFormat(columns=["x"]), b"x=1\n")
    check_format(yt.YsonFormat(), b'{"x"=1; "y"=2}')
    check_format(yt.JsonFormat(), b'{"x": 1}')

    check_format(yt.YamrFormat(), [b"1\t2\n", b"3\t4\n"])
    check_format(yt.YamredDsvFormat(), [b"x=1\n", b"x=2\n"])
    check_format(yt.SchemafulDsvFormat(columns=["x"]), [b"x=1\n", b"x=2\n"])
    check_format(yt.YsonFormat(), [b'{"x"=1}', b'{"x"=2}'])
    check_format(yt.JsonFormat(), [b'{"x":1}', b'{"x":2}'])

def test_yson_dump_rows_speed():
    format = yt.YsonFormat()
    rows = []

    for _ in xrange(1000):
        if random.randint(0, 10) == 1:
            entity = yson.YsonEntity()
            entity.attributes["@table_index"] = random.randint(0, 1)
            rows.append(entity)
            continue

        rows.append({"a": "abacaba", "b": 1234, "c": 123.123, "d": [1, 2, {}]})

    NUM_ITERATIONS = 1000

    start_time = time.time()
    for _ in xrange(NUM_ITERATIONS):
        stream = BytesIO()
        format.dump_rows(rows, stream)

    one_stream_dump_time = time.time() - start_time
    start_time = time.time()
    for _ in xrange(NUM_ITERATIONS):
        stream = BytesIO()
        stream2 = BytesIO()
        format.dump_rows(rows, [stream, stream2])

    two_streams_dump_time = time.time() - start_time

    if two_streams_dump_time / one_stream_dump_time > 2:
        assert False, "Dump rows to one stream took {0} seconds, " \
                      "to two streams took {1} seconds".format(one_stream_dump_time, two_streams_dump_time)

if PY3:
    def test_none_encoding():
        def check(format, bytes_row, text_row, expected_bytes, fail_exc=yt.YtError):
            stream = BytesIO()

            format.dump_rows([bytes_row], stream)
            assert stream.getvalue() == expected_bytes

            with pytest.raises(fail_exc):
                format.dump_rows([text_row], stream)

        check(yt.YamrFormat(encoding=None), yt.Record(b"1", b"2"), yt.Record("1", "2"), b"1\t2\n")
        check(yt.DsvFormat(encoding=None), {b"x": b"y"}, {"x": "y"}, b"x=y\n")
        check(yt.YamredDsvFormat(encoding=None), yt.Record(b"1", b"2"), yt.Record("1", "2"), b"1\t2\n")
        check(yt.YsonFormat(format="text", encoding=None), {b"x": b"y"}, {"x": "y"}, b'{"x"="y";};\n',
              fail_exc=yson.YsonError)
        check(yt.SchemafulDsvFormat(encoding=None, columns=["x"]), {b"x": b"y"}, {"x": "y"}, b"y\n",
              fail_exc=KeyError)
