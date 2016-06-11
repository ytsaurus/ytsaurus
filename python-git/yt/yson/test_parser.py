# -*- coding: utf-8 -*-

from __future__ import absolute_import

import unittest
from cStringIO import StringIO

import yt.yson
import yt.yson.yson_types

from yt.yson import to_yson_type


class YsonParserTestBase(object):
    @staticmethod
    def load(*args, **kws):
        raise NotImplementedError

    @staticmethod
    def loads(*args, **kws):
        raise NotImplementedError

    def assert_equal(self, parsed, expected, attributes):
        if expected is None:
            assert isinstance(parsed, yt.yson.yson_types.YsonEntity)
            self.assertEqual(parsed.attributes, attributes)
        else:
            self.assertEqual(parsed, to_yson_type(expected, attributes))

    def assert_parse(self, string, expected, attributes = {}):
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


class TestParser(unittest.TestCase, YsonParserTestBase):
    @staticmethod
    def load(*args, **kws):
        return yt.yson.parser.load(*args, **kws)

    @staticmethod
    def loads(*args, **kws):
        return yt.yson.parser.loads(*args, **kws)
