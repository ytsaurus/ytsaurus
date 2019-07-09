# -*- coding: utf-8 -*-

from __future__ import absolute_import

import pytest

import yt.yson.writer
from yt.yson import YsonUint64, YsonInt64, YsonEntity, YsonMap, YsonDouble, YsonError
import yt.subprocess_wrapper as subprocess
from yt.packages.six import b, PY3
from yt.packages.six.moves import map as imap

try:
    import yt_yson_bindings
except ImportError:
    yt_yson_bindings = None

import os

def get_debian_version(root):
    try:
        output = subprocess.check_output(["dpkg-parsechangelog"], cwd=root)
    except (OSError, subprocess.CalledProcessError):
        # Infinite version
        return (100, )
    return tuple(imap(int, output.split("Version:")[1].split()[0].split(".")))

try:
    import yatest.common
    PARENT_REPO_DIR = yatest.common.source_path("yt/python")
except ImportError:
    PARENT_REPO_DIR = os.path.abspath(os.path.join(__file__, "../../../../../"))

VERSION = get_debian_version(PARENT_REPO_DIR)

class YsonWriterTestBase(object):
    @staticmethod
    def dumps(*args, **kws):
        raise NotImplementedError()

    def test_slash(self):
        assert self.dumps({"key": "1\\"}, yson_format="text") == b'{"key"="1\\\\";}'

    def test_boolean(self):
        assert self.dumps(False, boolean_as_string=True) == b'"false"'
        assert self.dumps(True, boolean_as_string=True) == b'"true"'
        assert self.dumps(False, boolean_as_string=False) == b"%false"
        assert self.dumps(True, boolean_as_string=False) == b"%true"

    def test_integers(self):
        value = 0
        assert b("0") == self.dumps(value)

        value = YsonInt64(1)
        value.attributes = {"foo": "bar"}
        assert b('<"foo"="bar";>1') == self.dumps(value)

    def test_long_integers(self):
        value = 2 ** 63
        assert b(str(value) + "u") == self.dumps(value)

        value = 2 ** 63 - 1
        assert b(str(value)) == self.dumps(value)
        assert b(str(value) + "u") == self.dumps(YsonUint64(value))

        value = -2 ** 63
        assert str(value).encode("ascii") == self.dumps(value)

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

        value = YsonDouble(0.0)
        value.attributes = {"foo": "bar"}
        assert self.dumps(value) in (b('<"foo"="bar";>0.0'), b('<"foo"="bar";>0.'))

    @pytest.mark.skipif("VERSION < (19, 2)")
    def test_custom_integers(self):
        class MyInt(int):
            pass
        assert self.dumps(MyInt(10)) == b"10"

        if not PY3:
            class MyLong(long):
                pass
            assert self.dumps(MyLong(10)) == b"10"

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
            self.dumps([0, 1, 2, {"a": [5, 6, {"b": {1:2}}]}])
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

    @pytest.mark.skipif("not PY3")
    def test_dump_encoding(self):
        assert self.dumps({"a": 1}, yson_format="pretty") == b'{\n    "a" = 1;\n}'
        assert self.dumps({b"a": 1, "b": 2}, yson_format="pretty") == b'{\n    "a" = 1;\n    "b" = 2;\n}'
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

class TestWriterDefault(YsonWriterTestBase):
    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.dumps(*args, **kws)


class TestWriterPython(YsonWriterTestBase):
    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.writer.dumps(*args, **kws)


if yt_yson_bindings:
    class TestWriterBindings(YsonWriterTestBase):
        @staticmethod
        def dumps(*args, **kws):
            return yt_yson_bindings.dumps(*args, **kws)

        def test_ignore_inner_attributes(self):
            m = YsonMap()
            m["value"] = YsonEntity()
            m["value"].attributes = {"attr": 10}
            assert self.dumps(m) in \
                [b'{"value"=<"attr"=10;>#;}', b'{"value"=<"attr"=10>#}']
            assert self.dumps(m, ignore_inner_attributes=True) in \
                [b'{"value"=#;}', b'{"value"=#}']

        def test_zero_byte(self):
            assert b'"\\0"' == self.dumps("\x00")
            assert b"\x01\x02\x00" == self.dumps("\x00", yson_format="binary")

if yt_yson_bindings:
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
