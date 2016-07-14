# -*- coding: utf-8 -*-

from __future__ import absolute_import

from cStringIO import StringIO

import yt.yson
from yt.yson.yson_types import YsonEntity, YsonMap, YsonList, YsonInt64, YsonString

from yt.yson import to_yson_type

import pytest

try:
    import yt_yson_bindings
except ImportError:
    yt_yson_bindings = None


class YsonParserTestBase(object):
    @staticmethod
    def load(*args, **kws):
        raise NotImplementedError()

    @staticmethod
    def loads(*args, **kws):
        raise NotImplementedError()

    @staticmethod
    def assert_equal(parsed, expected, attributes):
        if expected is None:
            assert isinstance(parsed, YsonEntity)
            assert parsed.attributes == attributes
        else:
            assert parsed == to_yson_type(expected, attributes)

    def assert_parse(self, string, expected, attributes={}):
        self.assert_equal(self.loads(string), expected, attributes)
        stream = StringIO(string)
        self.assert_equal(self.load(stream), expected, attributes)

    def test_quoted_string(self):
        self.assert_parse('"abc\\"\\n"', 'abc"\n')

    def test_unquoted_string(self):
        self.assert_parse('abc10', 'abc10')

    def test_binary_string(self):
        self.assert_parse('\x01\x06abc', 'abc')

    def test_int(self):
        self.assert_parse('64', 64)

    def test_uint(self):
        self.assert_parse('64u', 64)

    def test_binary_int(self):
        self.assert_parse('\x02\x81\x40', -(2 ** 12) - 1)

    def test_double(self):
        self.assert_parse('1.5', 1.5)

    def test_exp_double(self):
        self.assert_parse('1.73e23', 1.73e23)

    def test_binary_double(self):
        self.assert_parse('\x03\x00\x00\x00\x00\x00\x00\xF8\x3F', 1.5)

    def test_boolean(self):
        self.assert_parse('%false', False)
        self.assert_parse('%true', True)
        self.assert_parse('\x04', False)
        self.assert_parse('\x05', True)

    def test_empty_list(self):
        self.assert_parse('[ ]', [])

    def test_one_element_list(self):
        self.assert_parse('[a]', ['a'])

    def test_list(self):
        self.assert_parse('[1; 2]', [1, 2])

    def test_empty_map(self):
        self.assert_parse('{ }', {})

    def test_one_element_map(self):
        self.assert_parse('{a=1}', {'a': 1})

    def test_map(self):
        self.assert_parse(
            '<attr1 = e; attr2 = f> {a = b; c = d}',
            {'a': 'b', 'c': 'd'},
            {'attr1': 'e', 'attr2': 'f'}
        )

    def test_entity(self):
        self.assert_parse('#', None)
        self.assert_parse('#', YsonEntity())

    def test_nested(self):
        self.assert_parse(
            '''
            {
                path = "/home/sandello";
                mode = 755;
                read = [
                        "*.sh";
                        "*.py"
                       ]
            }
            ''',
            {
                'path': '/home/sandello',
                'mode': 755,
                'read': ['*.sh', '*.py']
            }
        )

    def test_incorrect_params_in_loads(self):
        with pytest.raises(Exception):
            self.loads('123;#;{a={b=[";"]}};<attr=10>0.1;', raw=True, yson_type="aaa")
        with pytest.raises(Exception):
            self.loads('123;#;{a={b=[";"]}};<attr=10>0.1;', raw=True, yson_format="bbb")
        with pytest.raises(Exception):
            self.loads('123;#;{a={b=[";"]}};<attr=10>0.1;', xxx=True)

    def test_list_fragment(self):
        assert list(self.loads("{x=1};{y=2}", yson_type="list_fragment")) == [{"x": 1}, {"y": 2}]
        assert list(self.loads("{x=[1.0;abc]};#", yson_type="list_fragment")) == \
            [{"x": [1.0, "abc"]}, to_yson_type(None)]

    def test_map_fragment(self):
        assert self.loads("x=z;y=1", yson_type="map_fragment") == {"x": "z", "y": 1}
        assert self.loads('k={x=#; o=[%true; "1"; 2]};t=3', yson_type="map_fragment") == \
            {"k": {"x": to_yson_type(None), "o": [True, "1", 2]}, "t": 3}


class TestParserDefault(YsonParserTestBase):
    @staticmethod
    def load(*args, **kws):
        return yt.yson.load(*args, **kws)

    @staticmethod
    def loads(*args, **kws):
        return yt.yson.loads(*args, **kws)


class TestParserPython(YsonParserTestBase):
    @staticmethod
    def load(*args, **kws):
        return yt.yson.parser.load(*args, **kws)

    @staticmethod
    def loads(*args, **kws):
        return yt.yson.parser.loads(*args, **kws)


if yt_yson_bindings:
    class TestParserBindings(YsonParserTestBase):
        @staticmethod
        def load(*args, **kws):
            return yt_yson_bindings.load(*args, **kws)

        @staticmethod
        def loads(*args, **kws):
            return yt_yson_bindings.loads(*args, **kws)

        def test_loading_raw_rows(self):
            rows = list(self.loads("{a=b};{c=d};", raw=True, yson_type="list_fragment"))
            assert ["{a=b};", "{c=d};"] == rows

            rows = list(self.loads('123;#;{a={b=[";"]}};<attr=10>0.1;', raw=True, yson_type="list_fragment"))
            assert ["123;", "#;", '{a={b=[";"]}};', "<attr=10>0.1;"] == rows

            rows = list(self.loads("123;#", raw=True, yson_type="list_fragment"))
            assert ["123;", "#;"] == rows

            with pytest.raises(Exception):
                self.loads("{a=b")
            with pytest.raises(Exception):
                self.loads("{a=b}{c=d}")

        def test_always_create_attributes(self):
            obj = self.loads("{a=[b;1]}")
            assert isinstance(obj, YsonMap)
            assert isinstance(obj["a"], YsonList)
            assert isinstance(obj["a"][0], YsonString)
            assert isinstance(obj["a"][1], YsonInt64)

            obj = self.loads("{a=[b;1]}", always_create_attributes=False)
            assert not isinstance(obj, YsonMap)
            assert not isinstance(obj["a"], YsonList)
            assert not isinstance(obj["a"][0], YsonString)
            assert not isinstance(obj["a"][1], YsonInt64)
            assert isinstance(obj, dict)
            assert isinstance(obj["a"], list)
            assert isinstance(obj["a"][0], str)
            assert isinstance(obj["a"][1], long)

            obj = self.loads("{a=[b;<attr=#>1]}", always_create_attributes=False)
            assert not isinstance(obj, YsonMap)
            assert not isinstance(obj["a"], YsonList)
            assert not isinstance(obj["a"][0], YsonString)
            assert isinstance(obj["a"][1], YsonInt64)
            assert obj["a"][1].attributes["attr"] is None
