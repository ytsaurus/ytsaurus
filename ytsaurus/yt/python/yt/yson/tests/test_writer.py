# -*- coding: utf-8 -*-

from __future__ import absolute_import

import pytest

import yt.yson.writer
from yt.yson import YsonUint64, YsonInt64, YsonEntity, YsonMap, YsonDouble, YsonError
from yt.yson.yson_types import YsonStringProxy, make_byte_key

try:
    from yt.packages.six import b, PY3
except ImportError:
    from six import b, PY3

try:
    import yt_yson_bindings
except ImportError:
    yt_yson_bindings = None

import random


class YsonWriterTestBase(object):
    @staticmethod
    def dumps(*args, **kws):
        raise NotImplementedError()

    def dumps_binary(self, obj, *args, **kwargs):
        assert "yson_format" not in kwargs
        kwargs["yson_format"] = "binary"
        return self.dumps(obj, *args, **kwargs)

    def test_slash(self):
        assert self.dumps({"key": "1\\"}, yson_format="text") == b'{"key"="1\\\\";}'

    def test_boolean(self):
        assert self.dumps(False) == b"%false"
        assert self.dumps(True) == b"%true"
        assert b"\x04" == self.dumps_binary(False)
        assert b"\x05" == self.dumps_binary(True)

    def test_integers(self):
        value = 0
        assert b("0") == self.dumps(value)

        value = YsonInt64(1)
        value.attributes = {"foo": "bar"}
        assert b('<"foo"="bar";>1') == self.dumps(value)

        assert b"\x02\x00" == self.dumps_binary(0)

        value = YsonInt64(1)
        value.attributes = {"foo": "bar"}
        assert b"<\x01\x06foo=\x01\x06bar;>\x02\x02" == self.dumps_binary(value)

    def test_long_integers(self):
        value = 2 ** 63
        assert b(str(value) + "u") == self.dumps(value)
        assert b"\x06\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01" == self.dumps_binary(value)

        value = 2 ** 63 - 1
        assert b(str(value)) == self.dumps(value)
        assert b(str(value) + "u") == self.dumps(YsonUint64(value))
        assert b"\x02\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01" == self.dumps_binary(value)
        assert b"\x06\xff\xff\xff\xff\xff\xff\xff\xff\x7f" == self.dumps_binary(YsonUint64(value))

        value = -2 ** 63
        assert str(value).encode("ascii") == self.dumps(value)
        assert b"\x02\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01" == self.dumps_binary(value)

        with pytest.raises(Exception):
            self.dumps(2 ** 64)
        with pytest.raises(Exception):
            self.dumps(-2 ** 63 - 1)
        with pytest.raises(Exception):
            self.dumps(YsonUint64(-2 ** 63))
        with pytest.raises(Exception):
            self.dumps(YsonInt64(2 ** 63 + 1))

    def test_doubles(self):
        value = -1.23456e-47
        assert abs(float(self.dumps(value)) - value) < abs(value) * 1e-9
        assert b"%nan" == self.dumps(float("nan"))
        assert b"%-inf" == self.dumps(float("-inf"))
        assert b"%inf" == self.dumps(float("inf"))

        assert b"\x03\xca\x63\x30\x8f\x09\x0b\x32\xb6" == self.dumps_binary(value)
        assert b"\x03\x00\x00\x00\x00\x00\x00\xf8\x7f" == self.dumps_binary(float("nan"))

        value = YsonDouble(0.0)
        value.attributes = {"foo": "bar"}
        assert self.dumps(value) in (b('<"foo"="bar";>0.0'), b('<"foo"="bar";>0.'))

    def test_custom_integers(self):
        class MyInt(int):
            pass
        assert b"10" == self.dumps(MyInt(10))
        assert b"\x02\x14" == self.dumps_binary(MyInt(10))

        if not PY3:
            class MyLong(long):  # noqa
                pass
            assert b"10" == self.dumps(MyLong(10))
            assert b"\x02\x14" == self.dumps_binary(MyLong(10))

    def test_context_in_errors(self):
        try:
            self.dumps([0, 1, 2, 2 ** 64])
        except YsonError as error:
            row_key_path = error.attributes.get("row_key_path")
            row_key_path = row_key_path.decode() if isinstance(row_key_path, bytes) else row_key_path
            assert row_key_path == "/3"
            assert "row_index" not in error.attributes
        except RuntimeError:
            # Old version of YSON bindings.
            pass

        try:
            self.dumps([0, 1, 2, 2 ** 64], yson_type="list_fragment")
        except YsonError as error:
            assert "row_key_path" not in error.attributes
            assert error.attributes.get("row_index") == 3
        except RuntimeError:
            # Old version of YSON bindings.
            pass

        try:
            self.dumps([0, 1, 2, {"a": [5, 6, {"b": {1: 2}}]}])
        except YsonError as error:
            row_key_path = error.attributes.get("row_key_path")
            row_key_path = row_key_path.decode() if isinstance(row_key_path, bytes) else row_key_path
            assert row_key_path == "/3/a/2/b"
            assert "row_index" not in error.attributes
        except RuntimeError:
            # Old version of YSON bindings.
            pass

        try:
            obj = YsonEntity()
            obj.attributes["a"] = [2 ** 65]
            self.dumps(obj)
        except YsonError as error:
            row_key_path = error.attributes.get("row_key_path")
            row_key_path = row_key_path.decode() if isinstance(row_key_path, bytes) else row_key_path
            assert row_key_path == "/@a/0"
            assert "row_index" not in error.attributes
        except RuntimeError:
            # Old version of YSON bindings.
            pass

    def test_list_fragment_text(self):
        assert self.dumps(
            ["a", "b", "c", 42],
            yson_format="text",
            yson_type="list_fragment"
        ) == b'"a";\n"b";\n"c";\n42;\n'

    def test_map_fragment_text(self):
        assert self.dumps(
            {"a": "b", "c": "d"},
            yson_format="text",
            yson_type="map_fragment"
        ) in [b'"a"="b";\n"c"="d";\n', b'"c"="d";\n"a"="b";\n']

    def test_list_fragment_pretty(self):
        assert self.dumps(
            ["a", "b", "c", 42],
            yson_format="pretty",
            yson_type="list_fragment"
        ) == b'"a";\n"b";\n"c";\n42;\n'

    def test_map_fragment_pretty(self):
        assert self.dumps(
            {"a": "b", "c": "d"},
            yson_format="pretty",
            yson_type="map_fragment"
        ) in [b'"a" = "b";\n"c" = "d";\n', b'"c" = "d";\n"a" = "b";\n']

    def test_invalid_attributes(self):
        obj = YsonEntity()

        obj.attributes = None
        assert self.dumps(obj) == b"#"

        obj.attributes = []
        with pytest.raises(Exception):
            self.dumps(obj)

    def test_invalid_params_in_dumps(self):
        with pytest.raises(Exception):
            self.dumps({"a": "b"}, xxx=True)
        with pytest.raises(Exception):
            self.dumps({"a": "b"}, yson_format="aaa")
        with pytest.raises(Exception):
            self.dumps({"a": "b"}, yson_type="bbb")

    def test_entity(self):
        assert b"#" == self.dumps(None)
        assert b"#" == self.dumps(YsonEntity())
        assert b"#" == self.dumps_binary(None)
        assert b"#" == self.dumps_binary(YsonEntity())

    @pytest.mark.skipif("not PY3")
    def test_dump_encoding(self):
        assert self.dumps({"a": 1}, yson_format="pretty") == b'{\n    "a" = 1;\n}'
        assert self.dumps_binary("ABC") == b"\x01\x06ABC"
        assert self.dumps({b"a": 1, "b": 2}, yson_format="pretty") in (b'{\n    "a" = 1;\n    "b" = 2;\n}', b'{\n    "b" = 2;\n    "a" = 1;\n}')
        assert self.dumps({b"a": 1}, yson_format="pretty", encoding=None) == b'{\n    "a" = 1;\n}'
        with pytest.raises(Exception):
            assert self.dumps({"a": 1}, encoding=None)

    def test_formatting(self):
        assert b'{\n    "a" = "b";\n}' == self.dumps({"a": "b"}, yson_format="pretty")
        assert b'{"a"="b";}' == self.dumps({"a": "b"})
        canonical_result = b"""\
{
    "x" = {
        "a" = [
            1;
            2;
            3;
        ];
    };
}"""
        assert canonical_result == self.dumps({"x": {"a": [1, 2, 3]}}, yson_format="pretty")
        assert b'{"x"={"a"=[1;2;3;];};}' == self.dumps({"x": {"a": [1, 2, 3]}})
        assert b'"x"=1;\n' == self.dumps({"x": 1}, yson_type="map_fragment")
        assert b'"x" = 1;\n' == self.dumps({"x": 1}, yson_type="map_fragment", yson_format="pretty")
        assert b"1;\n2;\n3;\n" == self.dumps([1, 2, 3], yson_type="list_fragment")
        assert b"1;\n2;\n3;\n" == self.dumps([1, 2, 3], yson_type="list_fragment", yson_format="pretty")

    def test_frozen_dict(self):
        from yt.wrapper.mappings import FrozenDict
        d = FrozenDict({"a": "b"})
        assert b'{"a"="b";}' == self.dumps(d)

    def test_to_yson_type(self):
        class A:
            def __init__(self):
                self.value = 234
                self.attributes = {"foo": "bar"}

            def to_yson_type(self):
                d = YsonUint64(self.value)
                d.attributes = self.attributes
                return d

        assert b'<"foo"="bar";>234u' == self.dumps(A())

    @pytest.mark.skipif("not PY3")
    def test_string_proxy(self):
        key = YsonStringProxy()
        key._bytes = b"\xFA"
        value = YsonStringProxy()
        value._bytes = b"\xFB"
        d = {key: "a"}
        assert b'{"\\xfa"="a";}' == self.dumps(d).lower()
        assert b"{\x01\x02\xfa=\x01\x02a;}" == self.dumps_binary(d)
        d = {"b": value}
        assert b'{"b"="\\xfb";}' == self.dumps(d).lower()
        d = {make_byte_key(b"\xFF"): "x"}
        assert b'{"\\xff"="x";}' == self.dumps(d).lower()

    def test_ignore_inner_attributes(self):
        m = YsonMap()
        m["value"] = YsonEntity()
        m["value"].attributes = {"attr": 10}
        assert self.dumps(m) in \
            [b'{"value"=<"attr"=10;>#;}', b'{"value"=<"attr"=10>#}']
        assert self.dumps(m, ignore_inner_attributes=True) in \
            [b'{"value"=#;}', b'{"value"=#}']

    def test_escaping(self):
        assert b'"\\0\\1\\2\\3\\4\\5\\6\\7\\x08\\x0B\\x0C"' == self.dumps(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c")
        assert b'"\\t\\r\\n\\\\\\""' == self.dumps(b"\t\r\n\\\"")

        # escaping accounts for the next character
        assert b'"\\1z\\1A\\0017\\3777\\377A\\377a\\xFFz"' == self.dumps(b"\x01z\x01A\x017\xff7\xffA\xffa\xffz")


class TestWriterDefault(YsonWriterTestBase):
    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.dumps(*args, **kws)


class TestWriterPython(YsonWriterTestBase):
    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.writer.dumps(*args, **kws)


@pytest.mark.skipif("not yt_yson_bindings")
class TestWriterBindings(YsonWriterTestBase):
    @staticmethod
    def dumps(*args, **kws):
        return yt_yson_bindings.dumps(*args, **kws)


@pytest.mark.skipif("not yt_yson_bindings")
def test_equal_formatting():
    def _assert_dumps_equal(obj, **kwargs):
        assert yt.yson.writer.dumps(obj, **kwargs) == yt_yson_bindings.dumps(obj, **kwargs)

    _assert_dumps_equal({"a": "b"})
    _assert_dumps_equal({"a": {"b": [1, 2, 3]}})
    _assert_dumps_equal({"a": "b"}, yson_format="pretty")
    _assert_dumps_equal({"a": {"b": [1, 2, 3]}}, yson_format="pretty")
    _assert_dumps_equal({"a": "b", "c": {"d": "e"}}, yson_type="map_fragment")
    _assert_dumps_equal({"a": "b", "c": {"d": "e"}}, yson_type="map_fragment", yson_format="pretty")
    _assert_dumps_equal([1, 2, 3, "four", "five"], yson_type="list_fragment")
    _assert_dumps_equal([1, 2, 3, "four_five"], yson_type="list_fragment", yson_format="pretty")


@pytest.mark.skipif("not yt_yson_bindings")
def test_equal_escaping():
    alphabet = b"abcdef0123456789\x00\x01\x02\x08\x09\n\t\r\b\a\xff\x1f\x2f\x3f\x7f\x7e"
    size = 10000
    random.seed(42)
    string = bytes(random.choice(alphabet) for _ in range(size))
    assert yt.yson.writer.dumps(string, yson_format="text") == \
           yt_yson_bindings.dumps(string, yson_format="text")
    assert yt.yson.writer.dumps(string, yson_format="binary") == \
           yt_yson_bindings.dumps(string, yson_format="binary")
