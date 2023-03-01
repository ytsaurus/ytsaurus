from yt_env_setup import YTEnvSetup

from yt_commands import authors, write_table, read_table, create, YtResponseError

from yt_type_helpers import (
    make_schema,
    struct_type,
    optional_type,
    variant_struct_type,
    dict_type,
    list_type,
    decimal_type
)

import yt.yson as yson

import pytest

import uuid

##################################################################


def _prepare_table(type, optimize_for):
    path = "//tmp/{}".format(str(uuid.uuid4()))
    schema = make_schema(
        [
            {
                "name": "column",
                "type_v3": type,
            }
        ]
    )
    create(
        "table",
        path,
        attributes={"schema": schema, "optimize_for": optimize_for},
        force=True,
    )
    return path


def _test_yson_row(type, optimize_for, canonical_value, format, format_value):
    path = _prepare_table(type, optimize_for)

    rows = [{"column": format_value}]
    write_table(path, rows, input_format=format)
    assert read_table(path) == [{"column": canonical_value}]

    read = read_table(path, output_format=format)
    read_rows = list(yson.loads(read, yson_type="list_fragment"))
    assert read_rows == [{"column": format_value}]


def _test_invalid_write(type, optimize_for, value, format):
    path = _prepare_table(type, optimize_for)
    rows = [{"column": value}]
    with pytest.raises(YtResponseError):
        write_table(path, rows, input_format=format)

##################################################################


@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestYsonPositionalFormat(YTEnvSetup):
    POSITIONAL_YSON = yson.loads(b"<complex_type_mode=positional>yson")
    POSITIONAL_DICT_YSON = yson.loads(b"<string_keyed_dict_mode=positional>yson")

    STRUCT = struct_type(
        [
            ("f1", "int64"),
            ("f2", optional_type("utf8")),
        ]
    )
    VARIANT = variant_struct_type(
        [
            ("f1", "int64"),
            ("f2", optional_type("utf8")),
        ]
    )
    DICT = dict_type(
        "string", "int64"
    )

    @authors("ermolovd")
    def test_struct(self, optimize_for):
        _test_yson_row(
            self.STRUCT,
            optimize_for,
            {"f1": 42, "f2": "forty_two"},
            self.POSITIONAL_YSON,
            [42, "forty_two"],
        )
        _test_yson_row(self.STRUCT, optimize_for, {"f1": 53, "f2": None}, self.POSITIONAL_YSON, [53, None])
        _test_yson_row(self.STRUCT, optimize_for, {"f1": 83, "f2": None}, self.POSITIONAL_YSON, [83, None])

        _test_yson_row(self.VARIANT, optimize_for, ["f1", 42], self.POSITIONAL_YSON, [0, 42])
        _test_yson_row(self.VARIANT, optimize_for, ["f2", None], self.POSITIONAL_YSON, [1, None])
        _test_yson_row(self.VARIANT, optimize_for, ["f2", "foo"], self.POSITIONAL_YSON, [1, "foo"])

    @authors("egor-gutrov")
    def test_in_optional(self, optimize_for):
        optional_struct = optional_type(self.STRUCT)
        _test_yson_row(optional_struct, optimize_for, {"f1": 53, "f2": None}, self.POSITIONAL_YSON, [53, None])
        _test_yson_row(optional_struct, optimize_for, None, self.POSITIONAL_YSON, None)

        optional_variant = optional_type(self.VARIANT)
        _test_yson_row(optional_variant, optimize_for, ["f2", None], self.POSITIONAL_YSON, [1, None])
        _test_yson_row(optional_variant, optimize_for, None, self.POSITIONAL_YSON, None)

    @authors("egor-gutrov")
    def test_invalid(self, optimize_for):
        for val in [{"f1": 53, "f2": None}, "lol", None, 42, ["f1", 42]]:
            _test_invalid_write(self.STRUCT, optimize_for, val, self.POSITIONAL_YSON)
            _test_invalid_write(self.VARIANT, optimize_for, val, self.POSITIONAL_YSON)

    @authors("nadya73")
    def test_dict(self, optimize_for):
        _test_yson_row(
            self.DICT, optimize_for, [["f1", 1], ["f2", 2]],
            self.POSITIONAL_DICT_YSON, [["f1", 1], ["f2", 2]])


@authors("nadya73")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestYsonNamedFormat(YTEnvSetup):
    NAMED_YSON = yson.loads(b"<complex_type_mode=named>yson")
    NAMED_DICT_YSON = yson.loads(b"<string_keyed_dict_mode=named>yson")

    STRUCT = struct_type(
        [
            ("f1", "int64"),
            ("f2", optional_type("utf8")),
        ]
    )

    DICT = dict_type(
        "string", "int64"
    )

    INT_DICT = dict_type(
        "int64", "string"
    )

    @authors("nadya73")
    def test_struct(self, optimize_for):
        _test_yson_row(
            self.STRUCT,
            optimize_for,
            {"f1": 42, "f2": "forty_two"},
            self.NAMED_YSON,
            {"f1": 42, "f2": "forty_two"},
        )
        _test_yson_row(
            self.STRUCT, optimize_for, {"f1": 53, "f2": None},
            self.NAMED_YSON, {"f1": 53, "f2": None})
        _test_yson_row(
            self.STRUCT, optimize_for, {"f1": 83, "f2": None},
            self.NAMED_YSON, {"f1": 83, "f2": None})

    @authors("nadya73")
    def test_dict(self, optimize_for):
        _test_yson_row(
            self.DICT, optimize_for, [["f1", 1], ["f2", 2]],
            self.NAMED_DICT_YSON, {"f1": 1, "f2": 2})

    @authors("nadya73")
    def test_not_string_keyed_dict(self, optimize_for):
        _test_yson_row(
            self.INT_DICT, optimize_for, [[1, "v1"], [2, "v2"]],
            self.NAMED_DICT_YSON, [[1, "v1"], [2, "v2"]])


@authors("egor-gutrov")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestYsonDecimalFormat(YTEnvSetup):
    TEXT_DECIMAL_FORMAT = yson.loads(b"<decimal_mode=text>yson")
    BINARY_PI = b"\x80\x00\x7A\xB7"
    TEXT_PI = "3.1415"
    DECIMAL = decimal_type(5, 4)

    def test_simple(self, optimize_for):
        _test_yson_row(self.DECIMAL, optimize_for, self.BINARY_PI, self.TEXT_DECIMAL_FORMAT, self.TEXT_PI)

    def test_in_optional(self, optimize_for):
        decimal_optional = optional_type(self.DECIMAL)
        _test_yson_row(decimal_optional, optimize_for, None, self.TEXT_DECIMAL_FORMAT, None)
        _test_yson_row(decimal_optional, optimize_for, self.BINARY_PI, self.TEXT_DECIMAL_FORMAT, self.TEXT_PI)

    def test_in_list(self, optimize_for):
        decimal_list = list_type(self.DECIMAL)
        _test_yson_row(decimal_list, optimize_for, [self.BINARY_PI] * 3, self.TEXT_DECIMAL_FORMAT, [self.TEXT_PI] * 3)

    def test_in_struct(self, optimize_for):
        decimal_struct = struct_type([("f1", self.DECIMAL)])
        _test_yson_row(decimal_struct, optimize_for, {"f1": self.BINARY_PI}, self.TEXT_DECIMAL_FORMAT, {"f1": self.TEXT_PI})

        text_decimal_positional_format = yson.loads(b"<decimal_mode=text; complex_type_mode=positional>yson")
        _test_yson_row(decimal_struct, optimize_for, {"f1": self.BINARY_PI}, text_decimal_positional_format, [self.TEXT_PI])

    def test_in_variant_struct(self, optimize_for):
        decimal_variant_struct = variant_struct_type([("f1", self.DECIMAL)])
        text_decimal_positional_format = yson.loads(b"<decimal_mode=text; complex_type_mode=positional>yson")
        _test_yson_row(decimal_variant_struct, optimize_for, ["f1", self.BINARY_PI], self.TEXT_DECIMAL_FORMAT, ["f1", self.TEXT_PI])
        _test_yson_row(decimal_variant_struct, optimize_for, ["f1", self.BINARY_PI], text_decimal_positional_format, [0, self.TEXT_PI])

    def test_invalid(self, optimize_for):
        for val in [{"a": 0}, "31.415", "3.14159", None, 42, "a.bcde", self.BINARY_PI]:
            _test_invalid_write(self.DECIMAL, optimize_for, val, self.TEXT_DECIMAL_FORMAT)


@authors("egor-gutrov")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestYsonTimeFormat(YTEnvSetup):
    TEXT_TIME_FORMAT = yson.loads(b"<time_mode=text>yson")
    DATE = {"type_name": "date"}
    DATETIME = {"type_name": "datetime"}
    TIMESTAMP = {"type_name": "timestamp"}
    INTERVAL = {"type_name": "interval"}

    DAYS = 18810
    DATE_TEXT = "2021-07-02"
    SECONDS = 1625242720
    DATETIME_TEXT = "2021-07-02T16:18:40Z"
    MICROSECONDS = 1625242720 * 10**6 + 123456
    TIMESTAMP_TEXT = "2021-07-02T16:18:40.123456Z"

    def test_zero(self, optimize_for):
        _test_yson_row(self.DATE, optimize_for, 0, self.TEXT_TIME_FORMAT, "1970-01-01")
        _test_yson_row(self.DATETIME, optimize_for, 0, self.TEXT_TIME_FORMAT, "1970-01-01T00:00:00Z")
        _test_yson_row(self.TIMESTAMP, optimize_for, 0, self.TEXT_TIME_FORMAT, "1970-01-01T00:00:00.000000Z")

    def test_simple(self, optimize_for):
        _test_yson_row(self.DATE, optimize_for, self.DAYS, self.TEXT_TIME_FORMAT, self.DATE_TEXT)
        _test_yson_row(self.DATETIME, optimize_for, self.SECONDS, self.TEXT_TIME_FORMAT, self.DATETIME_TEXT)
        _test_yson_row(self.TIMESTAMP, optimize_for, self.MICROSECONDS, self.TEXT_TIME_FORMAT, self.TIMESTAMP_TEXT)

    def test_in_optional(self, optimize_for):
        date_optional = optional_type(self.DATE)
        _test_yson_row(date_optional, optimize_for, None, self.TEXT_TIME_FORMAT, None)
        _test_yson_row(date_optional, optimize_for, self.DAYS, self.TEXT_TIME_FORMAT, self.DATE_TEXT)

        datetime_optional = optional_type(self.DATETIME)
        _test_yson_row(datetime_optional, optimize_for, None, self.TEXT_TIME_FORMAT, None)
        _test_yson_row(datetime_optional, optimize_for, self.SECONDS, self.TEXT_TIME_FORMAT, self.DATETIME_TEXT)

        timestamp_optional = optional_type(self.TIMESTAMP)
        _test_yson_row(timestamp_optional, optimize_for, None, self.TEXT_TIME_FORMAT, None)
        _test_yson_row(timestamp_optional, optimize_for, self.MICROSECONDS, self.TEXT_TIME_FORMAT, self.TIMESTAMP_TEXT)

    def test_in_list(self, optimize_for):
        _test_yson_row(list_type(self.DATE), optimize_for, [self.DAYS] * 3, self.TEXT_TIME_FORMAT, [self.DATE_TEXT] * 3)
        _test_yson_row(list_type(self.DATETIME), optimize_for, [self.SECONDS] * 3, self.TEXT_TIME_FORMAT, [self.DATETIME_TEXT] * 3)
        _test_yson_row(list_type(self.TIMESTAMP), optimize_for, [self.MICROSECONDS] * 3, self.TEXT_TIME_FORMAT, [self.TIMESTAMP_TEXT] * 3)

    def test_in_struct(self, optimize_for):
        struct = struct_type([
            ("f1", self.DATE),
            ("f2", self.DATETIME),
            ("f3", self.TIMESTAMP),
        ])
        _test_yson_row(
            struct,
            optimize_for,
            {"f1": self.DAYS, "f2": self.SECONDS, "f3": self.MICROSECONDS},
            self.TEXT_TIME_FORMAT,
            {"f1": self.DATE_TEXT, "f2": self.DATETIME_TEXT, "f3": self.TIMESTAMP_TEXT}
        )
        text_time_positional_format = yson.loads(b"<time_mode=text; complex_type_mode=positional>yson")
        _test_yson_row(
            struct,
            optimize_for,
            {"f1": self.DAYS, "f2": self.SECONDS, "f3": self.MICROSECONDS},
            text_time_positional_format,
            [self.DATE_TEXT, self.DATETIME_TEXT, self.TIMESTAMP_TEXT],
        )

    def test_in_variant_struct(self, optimize_for):
        variant_struct = variant_struct_type([
            ("f1", self.DATE),
            ("f2", self.DATETIME),
            ("f3", self.TIMESTAMP),
        ])
        _test_yson_row(variant_struct, optimize_for, ["f1", self.DAYS], self.TEXT_TIME_FORMAT, ["f1", self.DATE_TEXT])
        _test_yson_row(variant_struct, optimize_for, ["f2", self.SECONDS], self.TEXT_TIME_FORMAT, ["f2", self.DATETIME_TEXT])
        _test_yson_row(variant_struct, optimize_for, ["f3", self.MICROSECONDS], self.TEXT_TIME_FORMAT, ["f3", self.TIMESTAMP_TEXT])

    def test_invalid(self, optimize_for):
        for val in [
            "1970",
            "1970-01",
            "1970-01-0",
            "1970-13-01",
            "1970-00-01",
            "1970.01.01",
            "1970-01-01T",
            "70-01-01",
            "01-01-1970",
            "1970-1-1",
            None,
            42
        ]:
            _test_invalid_write(self.DATE, optimize_for, val, self.TEXT_TIME_FORMAT)
            _test_invalid_write(self.DATETIME, optimize_for, val, self.TEXT_TIME_FORMAT)
            _test_invalid_write(self.TIMESTAMP, optimize_for, val, self.TEXT_TIME_FORMAT)

        for type_ in [self.DATETIME, self.TIMESTAMP]:
            _test_invalid_write(type_, optimize_for, self.DATE_TEXT, self.TEXT_TIME_FORMAT)
        for type_ in [self.DATE, self.DATETIME]:
            _test_invalid_write(type_, optimize_for, self.TIMESTAMP_TEXT, self.TEXT_TIME_FORMAT)
        _test_invalid_write(self.DATE, optimize_for, self.DATETIME_TEXT, self.TEXT_TIME_FORMAT)


@authors("egor-gutrov")
@pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
class TestYsonUuidFormat(YTEnvSetup):
    TEXT_YT_FORMAT = yson.loads(b"<uuid_mode=text_yt>yson")
    TEXT_YQL_FORMAT = yson.loads(b"<uuid_mode=text_yql>yson")
    UUID = {"type_name": "uuid"}

    UUID_BINARY = b"\x01\x10\x20\x30\x40\x50\x60\x70\x80\x90\xa0\xb0\xc0\xd0\xe0\xf0"
    UUID_TEXT_YT = "1102030-40506070-8090a0b0-c0d0e0f0"
    UUID_TEXT_YQL = "30201001-5040-7060-8090-a0b0c0d0e0f0"

    def test_simple(self, optimize_for):
        _test_yson_row(self.UUID, optimize_for, self.UUID_BINARY, self.TEXT_YT_FORMAT, self.UUID_TEXT_YT)
        _test_yson_row(self.UUID, optimize_for, self.UUID_BINARY, self.TEXT_YQL_FORMAT, self.UUID_TEXT_YQL)

    def test_in_optional(self, optimize_for):
        uuid_optional = optional_type(self.UUID)
        _test_yson_row(uuid_optional, optimize_for, None, self.TEXT_YT_FORMAT, None)
        _test_yson_row(uuid_optional, optimize_for, None, self.TEXT_YQL_FORMAT, None)

        _test_yson_row(uuid_optional, optimize_for, self.UUID_BINARY, self.TEXT_YT_FORMAT, self.UUID_TEXT_YT)
        _test_yson_row(uuid_optional, optimize_for, self.UUID_BINARY, self.TEXT_YQL_FORMAT, self.UUID_TEXT_YQL)

    def test_in_list(self, optimize_for):
        _test_yson_row(list_type(self.UUID), optimize_for, [self.UUID_BINARY] * 3, self.TEXT_YT_FORMAT, [self.UUID_TEXT_YT] * 3)
        _test_yson_row(list_type(self.UUID), optimize_for, [self.UUID_BINARY] * 3, self.TEXT_YQL_FORMAT, [self.UUID_TEXT_YQL] * 3)

    def test_in_struct(self, optimize_for):
        struct = struct_type([
            ("f1", self.UUID),
        ])
        _test_yson_row(struct, optimize_for, {"f1": self.UUID_BINARY}, self.TEXT_YT_FORMAT, {"f1": self.UUID_TEXT_YT})
        _test_yson_row(struct, optimize_for, {"f1": self.UUID_BINARY}, self.TEXT_YQL_FORMAT, {"f1": self.UUID_TEXT_YQL})

    def test_in_variant_struct(self, optimize_for):
        variant_struct = variant_struct_type([
            ("f1", self.UUID),
        ])
        _test_yson_row(variant_struct, optimize_for, ["f1", self.UUID_BINARY], self.TEXT_YT_FORMAT, ["f1", self.UUID_TEXT_YT])
        _test_yson_row(variant_struct, optimize_for, ["f1", self.UUID_BINARY], self.TEXT_YQL_FORMAT, ["f1", self.UUID_TEXT_YQL])

    def test_invalid(self, optimize_for):
        for val in [
            self.UUID_TEXT_YT,
            "3022110g-5544-7766-8899-a0bbccddeeff",
            "302211f-5544-7766-8899-a0bbccddeeff",
            "302211f-5544-7766-8899-a0bbccddeefff",
            "302211-005544-7766-8899-a0bbccddeeff",
            "30221100.5544.7766.8899.a0bbccddeeff",
            "30221100-55447766-8899-a0bbccddeeff",
            None,
            42,
        ]:
            _test_invalid_write(self.UUID, optimize_for, val, self.TEXT_YQL_FORMAT)

        for val in [
            self.UUID_TEXT_YQL,
            "11223044556677-8899a0bb-ccddeeff",
            "112230.44556677.8899a0bb.ccddeeff",
            "11223g-44556677-8899a0bb-ccddeeff",
            "-1-1-1",
            "11223-44556677-8899a0bb-ccddeeff-a",
            None,
            42,
        ]:
            _test_invalid_write(self.UUID, optimize_for, val, self.TEXT_YT_FORMAT)
