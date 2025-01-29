# -- coding: utf-8 --

from yt.testlib import authors

import yt.yson as yson
import yt_yson_bindings as yson_bindings

from yt.wrapper.string_iter_io import StringIterIO
from yt.wrapper.format import extract_key, create_format

import yt.wrapper as yt

import pytest
import random
import time
from functools import partial
from flaky import flaky
from io import BytesIO
from itertools import chain


@authors("ignat")
def test_string_iter_io_read():
    strings = [b"ab", b"", b"c", b"de", b""]

    for add_eoln in [False, True]:
        io = StringIterIO(iter(strings), add_eoln)
        sep = b"\n" if add_eoln else b""
        for c in bytes(sep.join(strings)):
            assert c == io.read(1)[0]


@authors("ignat")
def test_string_iter_io_readline():
    strings = [b"ab", b"", b"c\n", b"d\ne", b""]

    for add_eoln in [False, True]:
        io = StringIterIO(iter(strings), add_eoln)
        sep = b"\n" if add_eoln else b""
        lines = sep.join(strings).split(b"\n")
        for i in range(len(lines)):
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


@authors("ignat")
def test_yson_format():
    format = yt.YsonFormat(format="text")
    row = {"a": 1, "b": 2}
    serialized_row = b'{"a"=1;"b"=2;}'
    yson_rows = list(format.load_rows(BytesIO(serialized_row)))
    assert yson_rows == [row]
    stream = BytesIO()
    format.dump_row(row, stream)
    assert stream.getvalue().rstrip(b";\n") in [serialized_row, b'{"b"=2;"a"=1;}']

    format = yt.YsonFormat(format="binary")
    assert format.dumps_row({"a": 1}).rstrip(b";\n") == yson.dumps({"a": 1}, yson_format="binary")


@authors("ignat")
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
    assert dumped_output == b'{"a"=1;};\n<"table_index"=1;>#;\n{"a"=1;};\n{"b"=2;};\n'


@authors("ignat")
def test_yson_iterator_mode():
    format = yt.YsonFormat(control_attributes_mode="iterator")
    input = b'<"row_index"=0>#;<"range_index"=0>#;<"tablet_index"=0>#;\n' \
            b'{"a"=1};\n' \
            b'<"row_index"=1>#;<"tablet_index"=1>#;\n' \
            b'{"b"=1};\n' \
            b'<"row_index"=2>#;<"range_index"=3>#;<"table_index"=1>#;<"tablet_index"=4>#;\n' \
            b'{"a"=2};\n' \
            b'{"b"=2};\n'

    iterator = format.load_rows(BytesIO(input))
    assert iterator.table_index is None
    assert iterator.row_index is None
    assert iterator.range_index is None
    assert iterator.tablet_index is None

    assert next(iterator) == {"a": 1}
    assert iterator.table_index is None
    assert iterator.row_index == 0
    assert iterator.range_index == 0
    assert iterator.tablet_index == 0

    assert next(iterator) == {"b": 1}
    assert iterator.table_index is None
    assert iterator.row_index == 1
    assert iterator.range_index == 0
    assert iterator.tablet_index == 1

    assert next(iterator) == {"a": 2}
    assert iterator.table_index == 1
    assert iterator.row_index == 2
    assert iterator.range_index == 3
    assert iterator.tablet_index == 4

    assert next(iterator) == {"b": 2}
    assert iterator.table_index == 1
    assert iterator.row_index == 3
    assert iterator.range_index == 3
    assert iterator.tablet_index == 4

    with pytest.raises(StopIteration):
        next(iterator)


@authors("ignat")
def test_yson_dump_sort_keys():
    keys = ["key_" + chr(ord("a") + i) for i in range(26)]
    shuffled_keys = ["key_" + chr(ord("a") + i) for i in range(26)]
    random.shuffle(shuffled_keys)
    data = {key: "foo" for key in shuffled_keys}

    def _extract_keys_in_order(dumped):
        dumped = str(dumped)
        res = []
        while "key_" in dumped:
            idx = dumped.index("key_")
            res.append(dumped[idx:idx+5])
            dumped = dumped[idx+5:]
        return res

    sorted_keys = yson.dumps(data, yson_format="text", sort_keys=True)
    non_sorted_keys = yson.dumps(data, yson_format="text", sort_keys=False)

    assert _extract_keys_in_order(sorted_keys) == keys
    # Non necessarily fails but very likely should.
    assert _extract_keys_in_order(non_sorted_keys) != keys


@authors("ignat")
def test_dsv_format():
    format = yt.DsvFormat()
    check_format(format, [b"a=1\tb=2\n", b"b=2\ta=1\n"], {"a": "1", "b": "2"})
    check_format(format, [b"a=1\\t\tb=2\n", b"b=2\ta=1\\t\n"], {"a": "1\t", "b": "2"})


@authors("ignat")
def test_yamr_format():
    format = yt.YamrFormat(lenval=False, has_subkey=False)
    check_format(format, b"a\tb\n", yt.Record("a", "b"))
    check_format(format, b"a\tb\tc\n", yt.Record("a", "b\tc"))

    format = yt.YamrFormat(lenval=False, has_subkey=True)
    check_format(format, b"a\tb\tc\n", yt.Record("a", "b", "c"))
    check_format(format, b"a\t\tc\td\n", yt.Record("a", "", "c\td"))

    format = yt.YamrFormat(lenval=True, has_subkey=False)
    check_format(format, b"\x01\x00\x00\x00a\x01\x00\x00\x00b", yt.Record("a", "b"))


@authors("ignat")
def test_yamr_load_records_raw():
    records = [b"a\tb\tc\n", b"d\te\tf\n", b"g\th\ti\n"]
    stream = BytesIO(b"".join(records))
    format = yt.YamrFormat(has_subkey=True)
    assert list(format.load_rows(stream, raw=True)) == records


@authors("ignat")
def test_yamr_table_switcher():
    format = yt.YamrFormat(lenval=False, has_subkey=False)
    check_table_index(format, b"a\tb\n", b"1\n",
                      [yt.Record("a", "b", tableIndex=0), yt.Record("a", "b", tableIndex=1)])

    format = yt.YamrFormat(lenval=True, has_subkey=False)
    import struct
    table_switcher = struct.pack("ii", -1, 1)
    check_table_index(format, b"\x01\x00\x00\x00a\x01\x00\x00\x00b", table_switcher,
                      [yt.Record("a", "b", tableIndex=0), yt.Record("a", "b", tableIndex=1)])


@authors("ignat")
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


@authors("ignat")
def test_json_format():
    format = yt.JsonFormat(enable_ujson=False)
    check_format(format, b'{"a": 1}', {"a": 1})

    stream = BytesIO(b'{"a": 1}\n{"b": 2}')
    assert list(format.load_rows(stream)) == [{"a": 1}, {"b": 2}]


@authors("ignat")
def test_json_format_table_index():
    format = yt.JsonFormat(control_attributes_mode="row_fields", enable_ujson=False)

    stream = BytesIO()
    format.dump_rows([{"a": 1, "@table_index": 1}], stream)
    assert stream.getvalue() in [b'{"$value": null, "$attributes": {"table_index": 1}}\n{"a": 1}\n',
                                 b'{"$attributes": {"table_index": 1}, "$value": null}\n{"a": 1}\n']

    assert [{"@table_index": 1, "a": 1}] == \
        list(format.load_rows(BytesIO(b'{"$value": null, "$attributes": {"table_index": 1}}\n'
                                      b'{"a": 1}')))


@authors("ignat")
def test_json_format_row_index():
    format = yt.JsonFormat(control_attributes_mode="row_fields", enable_ujson=False)
    assert [{"@table_index": 1, "@row_index": 5, "a": 1},
            {"@table_index": 1, "@row_index": 6, "a": 2}] == \
        list(format.load_rows(BytesIO(b'{"$value": null, "$attributes": {"table_index": 1, "row_index": 5}}\n'
                                      b'{"a": 1}\n'
                                      b'{"a": 2}')))


@authors("ignat")
def test_json_format_row_iterator():
    format = yt.JsonFormat(control_attributes_mode="iterator", enable_ujson=False)
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


@authors("ignat")
def test_schemaful_dsv_format():
    format = yt.SchemafulDsvFormat(columns=["a", "b"])
    check_format(format, b"1\t2\n", {"a": "1", "b": "2"})
    check_format(format, b"\\n\t2\n", {"a": "\n", "b": "2"})


@authors("ignat")
def test_yamred_dsv_format():
    format = yt.YamredDsvFormat(key_column_names=["a", "b"], subkey_column_names=["c"],
                                has_subkey=False, lenval=False)
    check_format(format, b"a\tb\n", yt.Record("a", "b"))
    check_format(format, b"a\tb\tc\n", yt.Record("a", "b\tc"))


@authors("ignat")
def test_extract_key():
    assert extract_key(yt.Record("k", "s", "v"), ["k"]) == "k"
    assert extract_key(yt.Record("k", "v"), ["k"]) == "k"
    assert extract_key({"k": "v", "x": "y"}, ["k"]) == {"k": "v"}


@authors("ignat")
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

    table_skiff_schemas = [
        {
            "wire_type": "tuple",
            "children": [
                {
                    "wire_type": "int64",
                    "name": "x"
                },
                {
                    "wire_type": "variant8",
                    "children": [
                        {
                            "wire_type": "nothing"
                        },
                        {
                            "wire_type": "int64"
                        }
                    ],
                    "name": "y"
                }
            ]
        }
    ]
    skiff_format = create_format(yson.to_yson_type("skiff", attributes={"table_skiff_schemas": table_skiff_schemas}))
    assert skiff_format.name() == "skiff"


@authors("ignat")
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


@flaky(max_runs=5)
@authors("ignat")
def test_yson_dump_rows_speed():
    format = yt.YsonFormat()
    rows = []

    for _ in range(1000):
        if random.randint(0, 10) == 1:
            entity = yson.YsonEntity()
            entity.attributes["@table_index"] = random.randint(0, 1)
            rows.append(entity)
            continue

        rows.append({"a": "abacaba", "b": 1234, "c": 123.123, "d": [1, 2, {}]})

    NUM_ITERATIONS = 1000

    start_time = time.time()
    for _ in range(NUM_ITERATIONS):
        stream = BytesIO()
        format.dump_rows(rows, stream)

    one_stream_dump_time = time.time() - start_time
    start_time = time.time()
    for _ in range(NUM_ITERATIONS):
        stream = BytesIO()
        stream2 = BytesIO()
        format.dump_rows(rows, [stream, stream2])

    two_streams_dump_time = time.time() - start_time

    if two_streams_dump_time / one_stream_dump_time > 2:
        assert False, "Dump rows to one stream took {0} seconds, " \
                      "to two streams took {1} seconds".format(one_stream_dump_time, two_streams_dump_time)


@authors("ignat")
def test_none_encoding():
    def check(format, bytes_row, text_row, expected_bytes, fail_exc=yt.YtError):
        stream = BytesIO()

        format.dump_rows([bytes_row], stream)
        assert stream.getvalue() == expected_bytes

        if fail_exc is not None:
            with pytest.raises(fail_exc):
                format.dump_rows([text_row], stream)
        else:
            stream = BytesIO()
            format.dump_rows([text_row], stream)
            assert stream.getvalue() == expected_bytes

    check(yt.YamrFormat(encoding=None), yt.Record(b"1", b"2"), yt.Record("1", "2"), b"1\t2\n")
    check(yt.DsvFormat(encoding=None), {b"x": b"y"}, {"x": "y"}, b"x=y\n")
    check(yt.YamredDsvFormat(encoding=None), yt.Record(b"1", b"2"), yt.Record("1", "2"), b"1\t2\n")
    check(yt.YsonFormat(format="text", encoding=None), {b"x": b"y"}, {"x": "y"}, b'{"x"="y";};\n',
          fail_exc=yson.YsonError)
    check(yt.SchemafulDsvFormat(encoding=None, columns=["x"]), {b"x": b"y"}, {"x": "y"}, b"y\n",
          fail_exc=KeyError)
    check(yt.JsonFormat(encoding=None), {b"x": b"y"}, {"x": "y"}, b'{"x": "y"}\n',
          fail_exc=None)


@authors("levysotsky")
def test_default_encoding():
    rows = [{u"x": u"Ы"}]

    yf = yt.YsonFormat(format="text")
    expected = b'{"x"="\\xD0\\xAB";};\n'
    assert expected == yf.dumps_rows(rows)

    jf = yt.JsonFormat(encode_utf8=False)
    expected = b'{"x": "\\u042b"}\n'
    assert expected == jf.dumps_rows(rows)

    jf = yt.JsonFormat(encode_utf8=True)
    expected = b'{"x": "\\u00d0\\u00ab"}\n'
    assert expected == jf.dumps_rows(rows)


@authors("denvr")
def test_yson_escape():
    def _safe_call(f, s):
        try:
            return f(s)
        except Exception:
            return None

    safe_bindings = partial(_safe_call, yson_bindings.loads)
    safe_python = partial(_safe_call, yson.parser.loads)

    quoted_parts = [
        # unicode
        (b"\"\\u0041\"", "A"),
        # hex
        (b"\"\\x410\"", "A0"),
        # oct
        (b"\"\\07510\"", "=10"),
        # spec
        (b"\"\\n\"", "\n"),
        (b"\"\\r\"", "\r"),
        (b"\"\\t\"", "\t"),
        (b"\"\\\\\"", "\\"),  # "\\"
        (b"\"\\\"\"", "\""),  # "\""
        (b"\"\\'\"", "'"),  # "'"
        # regular
        (b"\"\\z\"", "z"),
        (b"\"\\;\"", ";"),
    ]
    assert all(map(lambda i: i[1] == safe_bindings(i[0]) == safe_python(i[0]) , quoted_parts))

    quoted_simbols = list(map(str.encode, ["\"\\" + chr(i) + "\"" for i in range(0, 255) if i not in b'Uux']))
    bindings_string = list(map(partial(_safe_call, yson_bindings.loads), quoted_simbols))
    python_string = list(map(partial(_safe_call, yson.parser.loads), quoted_simbols))
    assert bindings_string == python_string
