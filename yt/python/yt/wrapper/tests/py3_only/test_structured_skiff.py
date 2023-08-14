from yt.testlib import authors

from yt.wrapper.schema import (
    yt_dataclass,
    Int64,
    Uint64,
    OtherColumns,
    SkiffError,
)

from yt.wrapper.schema.internal_schema import _row_py_schema_to_skiff_schema
from yt.wrapper.schema import _SchemaRuntimeCtx

from yt.skiff import SkiffSchema, load_structured, dump_structured

import yt.wrapper as yt

import io
import math
import pytest
import struct
import typing


def skiff_optional(skiff_value):
    return b"\x01" + skiff_value


SKIFF_INT64_15 = b"\x0F\x00\x00\x00\x00\x00\x00\x00"
SKIFF_INT64_13330 = b"\x12\x34\x00\x00\x00\x00\x00\x00"
SKIFF_INT64_MAX = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F"
SKIFF_INT64_MIN = b"\x00\x00\x00\x00\x00\x00\x00\x80"

SKIFF_UINT64_15 = b"\x0F\x00\x00\x00\x00\x00\x00\x00"
SKIFF_UINT64_MAX = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"

SKIFF_STRING32_EMPTY = b"\x00\x00\x00\x00"
SKIFF_STRING32_ABC = b"\x03\x00\x00\x00" + b"ABC"
SKIFF_STRING32_BINARY = b"\x03\x00\x00\x00" + b"\xFA\xFB\xFC"

SKIFF_YSON32_EMPTY_MAP = b"\x02\x00\x00\x00" + b"{}"
SKIFF_YSON32_MAP = b"\x06\x00\x00\x00" + b"{a=bc}"

SKIFF_BOOLEAN_FALSE = b"\x00"
SKIFF_BOOLEAN_TRUE = b"\x01"

SKIFF_DOUBLE_ZERO = b"\x00\x00\x00\x00\x00\x00\x00\x00"
SKIFF_DOUBLE_ONE = b"\x00\x00\x00\x00\x00\x00\xf0\x3f"
SKIFF_DOUBLE_MINUS_PI = b"\x18\x2d\x44\x54\xfb\x21\x09\xc0"

SKIFF_OPTIONAL_NOTHING = b"\x00"
SKIFF_OPTIONAL_INT64_15 = skiff_optional(SKIFF_INT64_15)


@yt_dataclass
class Struct1:
    f1: int
    f2: Uint64
    f3: str
    f4: bytes
    f5: bool
    f6: float


@yt_dataclass
class Struct2:
    f1: bool
    f2: Struct1
    f3: float


@yt_dataclass
class Struct3:
    f1: Struct2


STRUCT1_VALUE = Struct1(
    f1=15,
    f2=2 ** 64 - 1,
    f3="ABC",
    f4=b"\xFA\xFB\xFC",
    f5=True,
    f6=1.0,
)


SKIFF_STRUCT1 = b"".join(
    (
        SKIFF_INT64_15,
        SKIFF_UINT64_MAX,
        SKIFF_STRING32_ABC,
        SKIFF_STRING32_BINARY,
        SKIFF_BOOLEAN_TRUE,
        SKIFF_DOUBLE_ONE,
    )
)
SKIFF_STRUCT2 = b"".join(
    (
        SKIFF_BOOLEAN_FALSE,
        SKIFF_STRUCT1,
        SKIFF_DOUBLE_ZERO,
    )
)
SKIFF_STRUCT3 = SKIFF_STRUCT2


SKIFF_LIST_INT64 = b"".join(
    (
        b"\x00",
        SKIFF_INT64_15,
        b"\x00",
        SKIFF_INT64_15,
        b"\x00",
        SKIFF_INT64_MAX,
        b"\xFF",
    )
)

SKIFF_TUPLE_INT_BOOL_13330_TRUE = \
    SKIFF_INT64_13330 + b"\x01"

SKIFF_DICT_STRING_INT64 = b"".join(
    (
        b"\x00",
        SKIFF_STRING32_EMPTY,
        SKIFF_INT64_15,
        b"\x00",
        SKIFF_STRING32_ABC,
        SKIFF_INT64_15,
        b"\x00",
        SKIFF_STRING32_BINARY,
        SKIFF_INT64_MAX,
        b"\xFF",
    )
)


def _default_value_eq(actual, expected):
    assert actual == expected


def create_single_value_row_class(type_):
    class Row:
        pass

    Row.__annotations__ = {"x": type_}
    return yt_dataclass(Row)


def dump_single_value_row(type_, value):
    Row = create_single_value_row_class(type_)

    py_schema = _SchemaRuntimeCtx().set_for_reading_only(False).create_row_py_schema(Row)
    skiff_schema_for_writing = SkiffSchema([_row_py_schema_to_skiff_schema(py_schema)])

    stream = io.BytesIO()
    row = Row(x=value)
    dump_structured(objects=[row], streams=[stream], py_schemas=[py_schema], skiff_schemas=[skiff_schema_for_writing])

    return stream.getvalue()


def check_dump_load(type_, skiff_bytes, value, *, expected_type=None, assert_value_eq=_default_value_eq):
    Row = create_single_value_row_class(type_)

    py_schema = _SchemaRuntimeCtx().set_for_reading_only(True).create_row_py_schema(Row)

    skiff_schema_for_reading = SkiffSchema([_row_py_schema_to_skiff_schema(py_schema)])

    skiff_bytes_extended = b"\x00\x00" + skiff_bytes
    stream = io.BytesIO(skiff_bytes_extended)
    actual_list = list(load_structured(stream, py_schemas=[py_schema], skiff_schemas=[skiff_schema_for_reading]))

    assert len(actual_list) == 1
    assert type(actual_list[0]) == Row
    actual = actual_list[0].x
    if expected_type is None:
        assert type(actual) == type_
    else:
        assert type(actual) == expected_type
    assert_value_eq(actual=actual, expected=value)

    actual_skiff_bytes_with_table_index = dump_single_value_row(type_, value)

    assert actual_skiff_bytes_with_table_index[:2] == b"\x00\x00"
    actual_skiff_bytes = actual_skiff_bytes_with_table_index[2:]
    assert actual_skiff_bytes == skiff_bytes


class TestDumpLoadStructuredSkiff(object):
    @authors("levysotsky")
    def test_primitive(self):
        check_dump_load(int, SKIFF_INT64_15, 15)
        check_dump_load(Int64, SKIFF_INT64_15, 15, expected_type=int)
        check_dump_load(int, SKIFF_INT64_MAX, 2 ** 63 - 1)
        check_dump_load(int, SKIFF_INT64_MIN, -(2 ** 63))

        with pytest.raises(yt.YtError) as ex:
            dump_single_value_row(int, 2**63)
        assert "Got too large integer value" in str(ex.value)

        check_dump_load(Uint64, SKIFF_UINT64_15, 15, expected_type=int)
        check_dump_load(Uint64, SKIFF_UINT64_MAX, 2 ** 64 - 1, expected_type=int)

        check_dump_load(str, SKIFF_STRING32_EMPTY, "")
        check_dump_load(str, SKIFF_STRING32_ABC, "ABC")

        with pytest.raises(SkiffError):
            check_dump_load(str, SKIFF_STRING32_BINARY, "Does not matter")

        check_dump_load(bytes, SKIFF_STRING32_EMPTY, b"")
        check_dump_load(bytes, SKIFF_STRING32_ABC, b"ABC")
        check_dump_load(bytes, SKIFF_STRING32_BINARY, b"\xFA\xFB\xFC")

        check_dump_load(bool, SKIFF_BOOLEAN_FALSE, False)
        check_dump_load(bool, SKIFF_BOOLEAN_TRUE, True)

        check_dump_load(float, SKIFF_DOUBLE_ZERO, 0.0)
        check_dump_load(float, SKIFF_DOUBLE_ONE, 1.0)
        check_dump_load(float, SKIFF_DOUBLE_MINUS_PI, -math.pi)

        check_dump_load(bytes, SKIFF_YSON32_EMPTY_MAP, b"{}")
        check_dump_load(bytes, SKIFF_YSON32_MAP, b"{a=bc}")

    @authors("levysotsky")
    def test_struct(self):
        struct2 = Struct2(
            f1=False,
            f2=STRUCT1_VALUE,
            f3=0.0,
        )

        struct3 = Struct3(f1=struct2)

        check_dump_load(
            Struct1,
            SKIFF_STRUCT1,
            STRUCT1_VALUE,
        )

        check_dump_load(
            Struct2,
            SKIFF_STRUCT2,
            struct2,
        )

        check_dump_load(
            Struct3,
            SKIFF_STRUCT3,
            struct3,
        )

    @authors("levysotsky")
    def test_list(self):
        check_dump_load(typing.List[int], b"\xFF", [], expected_type=list)

        check_dump_load(typing.List[int], SKIFF_LIST_INT64, [15, 15, 2 ** 63 - 1], expected_type=list)

        skiff_list_uint64 = b"".join(
            (
                b"\x00",
                SKIFF_UINT64_15,
                b"\x00",
                SKIFF_UINT64_MAX,
                b"\xFF",
            )
        )
        check_dump_load(typing.List[Uint64], skiff_list_uint64, [15, 2 ** 64 - 1], expected_type=list)

        skiff_list_struct1 = b"".join(
            (
                b"\x00",
                SKIFF_STRUCT1,
                b"\x00",
                SKIFF_STRUCT1,
                b"\xFF",
            )
        )
        check_dump_load(typing.List[Struct1], skiff_list_struct1, [STRUCT1_VALUE] * 2, expected_type=list)

    @authors("denvr")
    def test_tuple(self):
        with pytest.raises(AssertionError) as ex:
            check_dump_load(typing.Tuple, SKIFF_INT64_13330 + b"\x01", (13330, True, ), expected_type=tuple)
        assert "members must be specified" in str(ex.value)

        check_dump_load(
            typing.List[typing.Tuple[int, int]],
            b"\x00" + SKIFF_INT64_13330 + SKIFF_INT64_13330 + b"\x00" + SKIFF_INT64_15 + SKIFF_INT64_15 + b"\xff",
            [(13330, 13330,), (15, 15,)],
            expected_type=list
        )

        check_dump_load(typing.Tuple[str], SKIFF_STRING32_ABC, ("ABC", ), expected_type=tuple)

        check_dump_load(typing.Optional[typing.Tuple[int]], SKIFF_OPTIONAL_INT64_15, (15, ), expected_type=tuple)

        check_dump_load(typing.Optional[typing.Tuple[int]], SKIFF_OPTIONAL_NOTHING, None, expected_type=type(None))

        check_dump_load(typing.Tuple[int, typing.List[int]], SKIFF_INT64_13330 + b"\x00" + SKIFF_UINT64_15 + b"\x00" + SKIFF_UINT64_15 + b"\xff", (13330, [15, 15],), expected_type=tuple)

        check_dump_load(typing.Tuple[int, bool], SKIFF_TUPLE_INT_BOOL_13330_TRUE, (13330, True, ), expected_type=tuple)

        check_dump_load(typing.Tuple[int, typing.Optional[int], int], SKIFF_INT64_13330 + SKIFF_OPTIONAL_INT64_15 + SKIFF_INT64_13330, (13330, 15, 13330, ), expected_type=tuple)

        check_dump_load(typing.Tuple[int, typing.Optional[int], int], SKIFF_INT64_13330 + SKIFF_OPTIONAL_NOTHING + SKIFF_INT64_13330, (13330, None, 13330, ), expected_type=tuple)

        check_dump_load(typing.Tuple[typing.Optional[int]], SKIFF_OPTIONAL_NOTHING, (None, ), expected_type=tuple)

        check_dump_load(typing.Tuple[typing.Optional[int]], SKIFF_OPTIONAL_INT64_15, (15, ), expected_type=tuple)

        with pytest.raises(SkiffError) as ex:
            check_dump_load(typing.Tuple[int, bool], SKIFF_INT64_13330 + b"", tuple(), expected_type=tuple)
        assert "Premature end of stream while parsing Skiff" in str(ex.value)

        with pytest.raises(SkiffError) as ex:
            check_dump_load(typing.Tuple[int, bool], SKIFF_TUPLE_INT_BOOL_13330_TRUE + b"\x00\x00", tuple(), expected_type=tuple)
        assert "Premature end of stream while parsing Skiff" in str(ex.value)

    @authors("levysotsky")
    def test_optional(self):
        check_dump_load(typing.Optional[int], SKIFF_OPTIONAL_INT64_15, 15, expected_type=int)
        check_dump_load(typing.Optional[int], SKIFF_OPTIONAL_NOTHING, None, expected_type=type(None))

        skiff_optional_list_int64 = skiff_optional(SKIFF_LIST_INT64)
        check_dump_load(
            typing.Optional[typing.List[int]],
            skiff_optional_list_int64,
            [15, 15, 2 ** 63 - 1],
            expected_type=list,
        )
        check_dump_load(
            typing.Optional[typing.List[int]],
            SKIFF_OPTIONAL_NOTHING,
            None,
            expected_type=type(None),
        )

        skiff_list_optional_int64 = b"".join(
            (
                b"\x00",
                b"\x00",
                b"\x00" b"\x01",
                SKIFF_INT64_MIN,
                b"\x00",
                b"\x00",
                b"\xFF",
            )
        )
        list_optional_int64 = [None, -(2 ** 63), None]
        check_dump_load(
            typing.List[typing.Optional[int]],
            skiff_list_optional_int64,
            list_optional_int64,
            expected_type=list,
        )

        @yt_dataclass
        class Struct:
            f1: typing.Optional[Struct1]
            f2: typing.List[typing.Optional[int]]

        struct_with_value = Struct(
            f1=STRUCT1_VALUE,
            f2=list_optional_int64,
        )
        skiff_struct_with_value = b"".join((skiff_optional(SKIFF_STRUCT1), skiff_list_optional_int64))
        check_dump_load(Struct, skiff_struct_with_value, struct_with_value)

        struct_with_none = Struct(
            f1=None,
            f2=list_optional_int64,
        )
        assert struct_with_none.f1 is None
        skiff_struct_with_none = SKIFF_OPTIONAL_NOTHING + skiff_list_optional_int64
        check_dump_load(Struct, skiff_struct_with_none, struct_with_none)

    @authors("levysotsky")
    def test_other_columns(self):
        d = {
            "a": "foo",
            "b": -123,
            "c": [
                {
                    "x": "",
                }
            ],
        }
        d_bytes = yt.yson.dumps(d, yson_format="binary")
        coded_length = struct.pack("<I", len(d_bytes))

        def assert_other_columns_eq(actual, expected):
            assert len(actual) == len(expected) == 3
            for k in ["a", "b", "c"]:
                assert actual[k] == expected[k]

        check_dump_load(
            OtherColumns,
            coded_length + d_bytes,
            OtherColumns(d),
            assert_value_eq=assert_other_columns_eq,
        )

    @authors("ignat")
    def test_dict(self):
        check_dump_load(typing.Dict[bytes, int], b"\xFF", {}, expected_type=dict)
        check_dump_load(typing.Dict[bytes, int], SKIFF_DICT_STRING_INT64, {b"": 15, b"ABC": 15, b"\xFA\xFB\xFC": 2 ** 63 - 1}, expected_type=dict)
