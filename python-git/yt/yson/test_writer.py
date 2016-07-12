# -*- coding: utf-8 -*-

from __future__ import absolute_import

import pytest

import yt.yson.writer
from yt.yson import YsonUint64, YsonInt64, YsonEntity

try:
    import yt_yson_bindings
except ImportError:
    yt_yson_bindings = None


class YsonWriterTestBase(object):
    @staticmethod
    def dumps(*args, **kws):
        raise NotImplementedError

    def test_slash(self):
        assert self.dumps({"key": "1\\"}, yson_format="text") == '{"key"="1\\\\";}'

    def test_boolean(self):
        assert self.dumps(False, boolean_as_string=True) == '"false"'
        assert self.dumps(True, boolean_as_string=True) == '"true"'
        assert self.dumps(False, boolean_as_string=False) == "%false"
        assert self.dumps(True, boolean_as_string=False) == "%true"

    def test_long_integers(self):
        value = 2 ** 63
        assert '%su' % str(value) == self.dumps(value)

        value = 2 ** 63 - 1
        assert '%s' % str(value) == self.dumps(value)
        assert '%su' % str(value) == self.dumps(YsonUint64(value))

        value = -2 ** 63
        assert '%s' % str(value) == self.dumps(value)

        with pytest.raises(Exception):
            self.dumps(2 ** 64)
        with pytest.raises(Exception):
            self.dumps(-2 ** 63 - 1)
        with pytest.raises(Exception):
            self.dumps(YsonUint64(-2 ** 63))
        with pytest.raises(Exception):
            self.dumps(YsonInt64(2 ** 63 + 1))

    def test_list_fragment_text(self):
        assert self.dumps(
            ["a", "b", "c", 42],
            yson_format="text",
            yson_type="list_fragment"
        ) == '"a";\n"b";\n"c";\n42;\n'

    def test_map_fragment_text(self):
        assert self.dumps(
            {"a": "b", "c": "d"},
            yson_format="text",
            yson_type="map_fragment"
        ) == '"a"="b";\n"c"="d";\n'

    def test_list_fragment_pretty(self):
        assert self.dumps(
            ["a", "b", "c", 42],
            yson_format="pretty",
            yson_type="list_fragment"
        ) == '"a";\n"b";\n"c";\n42;\n'

    def test_map_fragment_pretty(self):
        assert self.dumps(
            {"a": "b", "c": "d"},
            yson_format="pretty",
            yson_type="map_fragment"
        ) == '"a" = "b";\n"c" = "d";\n'

    def test_invalid_attributes(self):
        obj = YsonEntity()

        obj.attributes = None
        assert self.dumps(obj) == "#"

        obj.attributes = []
        with pytest.raises(Exception):
            self.dumps(obj)


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
