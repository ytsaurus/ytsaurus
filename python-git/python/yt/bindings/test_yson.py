#!/usr/bin/env python

from yt.yson import to_yson_type
from yt.yson.test import TestYsonParser

import yson_lib
from yson_lib import load, loads, dump, dumps

import unittest


class TestParser(unittest.TestCase, TestYsonParser):
    TestYsonParser.parser = yson_lib


class TestYsonStream(unittest.TestCase, TestYsonParser):
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


if __name__ == "__main__":
    unittest.main()
