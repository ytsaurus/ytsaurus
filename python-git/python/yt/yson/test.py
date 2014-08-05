#!/usr/bin/python
#!-*-coding:utf-8-*-

import yt

import parser
import writer

import convert
import yson_to_bash

from StringIO import StringIO
import unittest

class PrintBashTest(unittest.TestCase):
    def yson_to_bash_test(self, input, correct_output, path=""):
        class Options(object):
            def __init__(self):
                self.sentinel = ""
                self.list_begin = ""
                self.list_separator = "\n"
                self.list_end = ""
                self.none_literal = "<None>"
                self.map_begin = ""
                self.map_separator = "\n"
                self.map_key_value_separator = "\t"
                self.map_end = ""
        yson_to_bash.options = Options()
        yson_to_bash.stdout = StringIO()
        yson_to_bash.print_bash(yson_to_bash.go_by_path(parser.loads(input), path), 3)
        self.assertEqual(yson_to_bash.stdout.getvalue(), correct_output)

    def test_print_bash(self):
        return # TODO(roizner): Tests are broken -- yson_to_bash.options are incorrect
        self.yson_to_bash_test("123", "123")
        self.yson_to_bash_test("[a; b; c]", "a\nb\nc")
        self.yson_to_bash_test("[{a=1; b=2}; {c=3; d=4}]", "a\t1\nb\t2\nc\t3\nd\t4")
        self.yson_to_bash_test("[{a=1; b=2}; {c=3; d=4}]", "c\t3\nd\t4", "1")
        self.yson_to_bash_test("[{a=1; b=2}; {c=3; d=4}]", "3", "1/c")

class YsonParserTestBase(object):
    def assert_equal(self, parsed, expected, attributes):
        if expected is None:
            assert isinstance(parsed, yt.yson.yson_types.YsonEntity)
            self.assertEqual(parsed.attributes, attributes)
        else:
            self.assertEqual(parsed, convert.to_yson_type(expected, attributes))

    def assert_parse(self, string, expected, attributes = {}):
        self.assert_equal(YsonParserTestBase.parser.loads(string), expected, attributes)
        stream = StringIO(string)
        self.assert_equal(YsonParserTestBase.parser.load(stream), expected, attributes)

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
        self.assert_parse('<attr1 = e; attr2 = f> {a = b; c = d}', {'a': 'b', 'c': 'd'}, {'attr1': 'e', 'attr2': 'f'})

    def test_entity(self):
        self.assert_parse('#', None)

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
            {'path' : '/home/sandello', 'mode' : 755, 'read' : ['*.sh', '*.py']})

    def test_convert(self):
        x = convert.json_to_yson({
            "$value": {
                "x": {
                    "$value": 10,
                    "$attributes": {}
                },
                "y": {
                    "$value": 11,
                    "$attributes": {}
                },
                "z": u"Брюссельская капуста"
            },
            "$attributes": {
                "$value": "abc",
                "$attributes": {}
            }
        })

        z = str(bytearray(u"Брюссельская капуста", "utf-8"))
        self.assertEqual(dict(x), {"x": 10, "y": 11, "z": z})
        self.assertEqual(x.attributes, "abc")

        self.assertEqual(convert.json_to_yson("abc"), "abc")

class TestParser(unittest.TestCase, YsonParserTestBase):
    YsonParserTestBase.parser = parser

class TestWriter(unittest.TestCase):
    def test_slash(self):
        self.assertEqual(writer.dumps({"key": "1\\"}, yson_format="text"), '{"key"="1\\\\";}')

    def test_boolean(self):
        self.assertEqual(writer.dumps(False), "%false")
        self.assertEqual(writer.dumps(True), "%true")

if __name__ == "__main__":
    unittest.main()
