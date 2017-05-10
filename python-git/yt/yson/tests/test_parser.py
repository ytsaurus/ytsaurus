# -*- coding: utf-8 -*-

from __future__ import absolute_import

from io import BytesIO, StringIO

import yt.yson
from yt.yson.yson_types import YsonEntity, YsonMap, YsonList, YsonInt64, YsonString, YsonUnicode
from yt.yson import to_yson_type, YsonError
from yt.packages.six import PY3

import copy
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
        stream = BytesIO(string)
        self.assert_equal(self.load(stream), expected, attributes)

    def test_quoted_string(self):
        self.assert_parse(b'"abc\\"\\n"', 'abc"\n')

    def test_unquoted_string(self):
        self.assert_parse(b'abc10', 'abc10')

    def test_binary_string(self):
        self.assert_parse(b'\x01\x06abc', 'abc')

    def test_int(self):
        self.assert_parse(b'64', 64)

    def test_uint(self):
        self.assert_parse(b'64u', 64)

    def test_binary_int(self):
        self.assert_parse(b'\x02\x81\x40', -(2 ** 12) - 1)

    def test_double(self):
        self.assert_parse(b'1.5', 1.5)

    def test_exp_double(self):
        self.assert_parse(b'1.73e23', 1.73e23)

    def test_binary_double(self):
        self.assert_parse(b'\x03\x00\x00\x00\x00\x00\x00\xF8\x3F', 1.5)

    def test_boolean(self):
        self.assert_parse(b'%false', False)
        self.assert_parse(b'%true', True)
        self.assert_parse(b'\x04', False)
        self.assert_parse(b'\x05', True)

    def test_empty_list(self):
        self.assert_parse(b'[ ]', [])

    def test_one_element_list(self):
        self.assert_parse(b'[a]', ['a'])

    def test_list(self):
        self.assert_parse(b'[1; 2]', [1, 2])

    def test_empty_map(self):
        self.assert_parse(b'{ }', {})

    def test_one_element_map(self):
        self.assert_parse(b'{a=1}', {'a': 1})

    def test_map(self):
        self.assert_parse(
            b'<attr1 = e; attr2 = f> {a = b; c = d}',
            {'a': 'b', 'c': 'd'},
            {'attr1': 'e', 'attr2': 'f'}
        )

    def test_entity(self):
        self.assert_parse(b'#', None)
        self.assert_parse(b'#', YsonEntity())

    def test_nested(self):
        self.assert_parse(
            b'''
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
            self.loads(b'123;#;{a={b=[";"]}};<attr=10>0.1;', raw=True, yson_type="aaa")
        with pytest.raises(Exception):
            self.loads(b'123;#;{a={b=[";"]}};<attr=10>0.1;', raw=True, yson_format="bbb")
        with pytest.raises(Exception):
            self.loads(b'123;#;{a={b=[";"]}};<attr=10>0.1;', xxx=True)

    def test_list_fragment(self):
        assert list(self.loads(b"{x=1};{y=2}", yson_type="list_fragment")) == [{"x": 1}, {"y": 2}]
        assert list(self.loads(b"{x=[1.0;abc]};#", yson_type="list_fragment")) == \
            [{"x": [1.0, "abc"]}, to_yson_type(None)]

    def test_map_fragment(self):
        assert self.loads(b"x=z;y=1", yson_type="map_fragment") == {"x": "z", "y": 1}
        assert self.loads(b'k={x=#; o=[%true; "1"; 2]};t=3', yson_type="map_fragment") == \
            {"k": {"x": to_yson_type(None), "o": [True, "1", 2]}, "t": 3}

    def test_decoding_from_bytes(self):
        s = b"{key=value; x=1; y=2; z=%true}"
        self.assert_equal(self.loads(s), {"key": "value", "x": 1, "y": 2, "z": True}, None)
        self.assert_equal(self.loads(s, encoding=None),
            {b"key": b"value", b"x": 1, b"y": 2, b"z": True}, None)

        s = b"<a=b>{x=1;y=2}"
        assert self.loads(s) == to_yson_type({"x": 1, "y": 2}, attributes={"a": "b"})
        assert self.loads(s, encoding=None) == to_yson_type({b"x": 1, b"y": 2}, attributes={b"a": b"b"})

        if not PY3:
            with pytest.raises(Exception):
                self.loads(b"{a=1}", encoding="utf-8")

    def test_default_encoding(self):
        if PY3:
            with pytest.raises(Exception):
                self.loads('"\xFF"')
        else:
            assert self.loads('"\xFF"') == "\xFF"

    def test_parse_from_non_binary_stream(self):
        if PY3:
            with pytest.raises(TypeError):
                self.loads(u"1")
            with pytest.raises(TypeError):
                self.load(StringIO(u"abcdef"))
        else:  # COMPAT
            assert self.loads(u"1") == 1
            assert self.load(StringIO(u"abcdef")) == u"abcdef"

    def test_always_create_attributes(self):
        obj = self.loads(b"{a=[b;1]}")
        assert isinstance(obj, YsonMap)
        assert isinstance(obj["a"], YsonList)
        if PY3:
            assert isinstance(obj["a"][0], YsonUnicode)
        else:
            assert isinstance(obj["a"][0], YsonString)
        assert isinstance(obj["a"][1], YsonInt64)

        obj = self.loads(b"{a=[b;1]}", always_create_attributes=False)
        assert not isinstance(obj, YsonMap)
        assert not isinstance(obj["a"], YsonList)
        assert not isinstance(obj["a"][0], (YsonUnicode, YsonString))
        assert not isinstance(obj["a"][1], YsonInt64)
        assert isinstance(obj, dict)
        assert isinstance(obj["a"], list)
        assert isinstance(obj["a"][0], str)
        if not PY3:
            assert isinstance(obj["a"][1], long)
        else:
            assert isinstance(obj["a"][1], int)

        obj = self.loads(b"{a=[b;<attr=#>1]}", always_create_attributes=False)
        assert not isinstance(obj, YsonMap)
        assert not isinstance(obj["a"], YsonList)
        assert not isinstance(obj["a"][0], (YsonUnicode, YsonString))
        assert isinstance(obj["a"][1], YsonInt64)
        assert obj["a"][1].attributes["attr"] is None


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
            rows = list(self.loads(b"{a=b};{c=d};", raw=True, yson_type="list_fragment"))
            assert [b"{a=b};", b"{c=d};"] == rows

            rows = list(self.loads(b'123;#;{a={b=[";"]}};<attr=10>0.1;', raw=True, yson_type="list_fragment"))
            assert [b"123;", b"#;", b'{a={b=[";"]}};', b"<attr=10>0.1;"] == rows

            rows = list(self.loads(b"123;#", raw=True, yson_type="list_fragment"))
            assert [b"123;", b"#;"] == rows

            with pytest.raises(Exception):
                self.loads(b"{a=b")
            with pytest.raises(Exception):
                self.loads(b"{a=b}{c=d}")

        def test_context(self):
            def check_context(error, context, context_pos):
                error_attrs = error.inner_errors[0]["attributes"]
                if "context_pos" in error_attrs:
                    assert context == error_attrs["context"]
                    assert context_pos == error_attrs["context_pos"]
                else:
                    assert context[context_pos:] == error_attrs["context"]

            STREAM_BLOCK_SIZE = 1024 * 1024
            try:
                yt_yson_bindings.loads(b"abacaba{")
            except YsonError as error:
                check_context(error, b"abacaba{", 0)

            try:
                yt_yson_bindings.loads(b"{a=b;c=d;e=f;[}")
            except YsonError as error:
                check_context(error, b"b;c=d;e=f;[}", 10)

            try:
                yt_yson_bindings.loads(b"[0;1;2;3;4;5;{1=2}]")
            except YsonError as error:
                check_context(error, b";2;3;4;5;{1=2}]", 10)

            try:
                yt_yson_bindings.loads(b"[1;5;{1=2}]")
            except YsonError as error:
                check_context(error, b"[1;5;{1=2}]", 6)

            try:
                yt_yson_bindings.loads(b"[" + b"ab" * (STREAM_BLOCK_SIZE // 2) + b";{1=2}]")
            except YsonError as error:
                check_context(error, b"abababab;{1=2}]", 10)

            try:
                yt_yson_bindings.loads(b"[" + b"a" * STREAM_BLOCK_SIZE + b";{1=2}]")
            except YsonError as error:
                check_context(error, b"aaaaaaaa;{1=2}]", 10)

    class TestLazyDict(object):
        def test_class(self):
            result = yt_yson_bindings.loads(b"{a=b;c=d}", lazy=True)
            assert result["a"] == b"b"
            assert result["c"] == b"d"
            assert not result.attributes

            assert not isinstance(result, (YsonMap, dict))

            result["a"] = 1
            assert result["a"] == 1

            result["123"] = b"abacaba"
            assert result["123"] == b"abacaba"

            assert len(result) == 3

            assert result.has_key("a")
            assert not result.has_key("some_key")

            del result["a"]
            assert len(result) == 2

            result.clear()
            assert len(result) == 0

            result.setdefault("a", 1)
            assert result["a"] == 1
            result.setdefault("a", 5)
            assert result["a"] == 1

            assert result.get("a", 5) == 1
            assert result.get("some_key", 5) == 5
            assert "a" in result
            assert "some_key" not in result
            assert len(result) == 1

        def test_map_fragment(self):
            result = yt_yson_bindings.loads(b"a=b;c=1;e=[abacaba;1.5];", lazy=True, yson_type="map_fragment")
            assert result["a"] == b"b"
            assert result["c"] == 1
            assert result["e"] == [b"abacaba", 1.5]
            assert len(result) == 3
            assert not result.attributes

            result = yt_yson_bindings.loads(b"a=b", lazy=True, yson_type="map_fragment")
            assert result["a"] == b"b"
            assert len(result) == 1
            assert not result.attributes

        def test_attributes(self):
            result = yt_yson_bindings.loads(b"<a=b;c=d>{e=k}", lazy=True)
            assert result.attributes["a"] == b"b"
            assert result.attributes["c"] == b"d"
            assert result["e"] == b"k"

            result = yt_yson_bindings.loads(b"<a=b;>{c=d;}", lazy=True)
            assert result.attributes["a"] == b"b"
            assert result["c"] == b"d"

            result = yt_yson_bindings.loads(b"<>{}", lazy=True)
            assert not result.attributes
            assert not result

        def test_list_fragment(self):
            result = list(yt_yson_bindings.loads(b"{a=0};{a=1};{a=2}", lazy=True, yson_type="list_fragment"))
            assert len(result) == 3

            for i in xrange(3):
                assert result[i]["a"] == i
                assert len(result[i]) == 1
                assert not result[i].attributes

            result = list(yt_yson_bindings.loads(b"{a=[];b=1};<testattr=abacaba>{a=2;b=3}",
                                                 lazy=True, yson_type="list_fragment"))
            assert len(result) == 2

            assert result[0]["a"] == []
            assert result[0]["b"] == 1
            assert len(result[0]) == 2
            assert not result[0].attributes

            assert result[1]["a"] == 2
            assert result[1]["b"] == 3
            assert len(result[1]) == 2
            assert len(result[1].attributes) == 1
            assert result[1].attributes["testattr"] == b"abacaba"

        def test_parse_objects(self):
            result = yt_yson_bindings.loads(b"[1;2;abacaba]", lazy=True)
            assert result == [1, 2, b"abacaba"]

            result = yt_yson_bindings.loads(b"<a=b>abacaba", lazy=True)
            assert str(result) == b"abacaba"
            assert result.attributes["a"] == b"b"

        def test_dumps(self):
            obj = yt_yson_bindings.loads(b"<a=b>{c=d;e=[1;2;3]}", lazy=True)
            assert yt_yson_bindings.dumps(obj) == b'<"a"="b";>{"c"="d";"e"=[1;2;3;];}'

            obj = yt_yson_bindings.loads(b"<a=b>{c=d;e=[1;2;3]}", lazy=True)
            obj["e"]
            obj.attributes["a"]
            assert yt_yson_bindings.dumps(obj) == b'<"a"="b";>{"c"="d";"e"=[1;2;3;];}'

        def test_copy(self):
            obj = yt_yson_bindings.loads(b"<a=b;g=[[1];[2]]>{c=d;e=[1;2;3]}", lazy=True)
            obj["e"]
            obj.attributes["g"]

            obj2 = copy.copy(obj)
            assert obj2["c"] == b"d"
            assert obj2["e"] == [1, 2, 3]
            assert obj2.attributes["a"] == b"b"
            assert obj2.attributes["g"] == [[1], [2]]

            obj2.attributes["g"][0].append(2)
            assert obj.attributes["g"] == [[1, 2], [2]]

            obj2["e"].append(4)
            assert obj["e"] == [1, 2, 3, 4]

            obj3 = copy.deepcopy(obj)
            assert obj3["e"] == [1, 2, 3, 4]

            obj3["e"].append(5)
            obj3.attributes["g"][1].append(3)
            assert obj["e"] == [1, 2, 3, 4]
            assert obj.attributes["g"] == [[1, 2], [2]]
