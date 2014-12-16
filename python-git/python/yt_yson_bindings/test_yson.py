#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from yt.yson.yson_types import *
from yt.yson import to_yson_type
from yt.yson.test import YsonParserTestBase

import yson_lib
from yson_lib import load, loads, dump, dumps

import unittest


class TestParser(unittest.TestCase, YsonParserTestBase):
    YsonParserTestBase.parser = yson_lib

class TestYsonStream(YsonParserTestBase, unittest.TestCase):
    def load_fragment(self, str):
        return list(loads(str, yson_type="list_fragment"))

    def dump_fragment(self, iter):
        return dumps(iter, yson_type="list_fragment", yson_format="text")

    def test_load(self):
        self.assertEqual(
            self.load_fragment("{x=1};{y=2}"),
            [{"x": 1}, {"y": 2}])
        self.assertEqual(
            self.load_fragment("{x=[1.0;abc]};#"),
            [{"x": [1.0, "abc"]}, to_yson_type(None)])

    def test_dump(self):
        self.assertEqual(
            self.dump_fragment([{"x": 1}, {"y": 2}]),
            '{"x"=1};\n{"y"=2};\n')

        self.assertEqual(
            self.dump_fragment((x for x in [{"x": None}, to_yson_type({"y": 2}, {"a": 10})])),
            '{"x"=#};\n<"a"=10>{"y"=2};\n')

    def test_unicode(self):
        unicode_str = u"ав"
        self.assertEqual(unicode_str, loads(dumps(unicode_str)).decode("utf-8"))

    def test_zero_byte(self):
        self.assertEqual('"\\0"', dumps("\x00"))
        self.assertEqual('\x01\x02\x00', dumps("\x00", yson_format="binary"))

    def test_none(self):
        self.assertEqual("#", dumps(None))
        self.assertEqual("#", dumps(YsonEntity()))

        self.assertTrue(loads("#", always_create_attributes=False) is None)
        self.assertEqual(YsonEntity(), loads("#"))

    def test_always_create_attributes(self):
        obj = loads("{a=[b;1]}")
        self.assertTrue(isinstance(obj, YsonMap))
        self.assertTrue(isinstance(obj["a"], YsonList))
        self.assertTrue(isinstance(obj["a"][0], YsonString))
        self.assertTrue(isinstance(obj["a"][1], YsonInt64))

        obj = loads("{a=[b;1]}", always_create_attributes=False)
        self.assertFalse(isinstance(obj, YsonMap))
        self.assertFalse(isinstance(obj["a"], YsonList))
        self.assertFalse(isinstance(obj["a"][0], YsonString))
        self.assertFalse(isinstance(obj["a"][1], YsonInt64))
        self.assertTrue(isinstance(obj, dict))
        self.assertTrue(isinstance(obj["a"], list))
        self.assertTrue(isinstance(obj["a"][0], str))
        self.assertTrue(isinstance(obj["a"][1], int))

        obj = loads("{a=[b;<attr=#>1]}", always_create_attributes=False)
        self.assertFalse(isinstance(obj, YsonMap))
        self.assertFalse(isinstance(obj["a"], YsonList))
        self.assertFalse(isinstance(obj["a"][0], YsonString))
        self.assertTrue(isinstance(obj["a"][1], YsonInt64))
        self.assertTrue(obj["a"][1].attributes["attr"] is None)

    def test_ignore_inner_attributes(self):
        map = YsonMap()
        map["value"] = YsonEntity()
        map["value"].attributes = {"attr": 10}
        self.assertEqual('{"value"=<"attr"=10>#}', dumps(map))
        self.assertEqual('{"value"=#}', dumps(map, ignore_inner_attributes=True))

    def test_long_integers(self):
        long = 2 ** 63
        self.assertEqual('%su' % str(long), dumps(long))

        long = 2 ** 63 - 1
        self.assertEqual('%s' % str(long), dumps(long))

        long = -2 ** 63
        self.assertEqual('%s' % str(long), dumps(long))

        self.assertRaises(Exception, lambda: dumps(2 ** 64))
        self.assertRaises(Exception, lambda: dumps(-2 ** 63 - 1))

    def test_loading_raw_rows(self):
        rows = list(loads("{a=b};{c=d};", raw=True, yson_type="list_fragment"))
        assert ["{a=b};", "{c=d};"] == rows

        rows = list(loads('123;#;{a={b=[";"]}};<attr=10>0.1;', raw=True, yson_type="list_fragment"))
        assert ["123;", "#;", '{a={b=[";"]}};', "<attr=10>0.1;"] == rows

        rows = list(loads("123;#", raw=True, yson_type="list_fragment"))
        assert ["123;", "#;"] == rows

        self.assertRaises(Exception, lambda: loads("{a=b"))
        self.assertRaises(Exception, lambda: loads("{a=b}{c=d}"))


if __name__ == "__main__":
    unittest.main()
