# -*- coding: utf-8 -*-

from __future__ import absolute_import

import unittest
from cStringIO import StringIO

from yt.yson import loads, yson_to_bash


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
        yson_to_bash.print_bash(yson_to_bash.go_by_path(loads(input), path), 3)
        self.assertEqual(yson_to_bash.stdout.getvalue(), correct_output)

    def test_print_bash(self):
        return # TODO(roizner): Tests are broken -- yson_to_bash.options are incorrect
        self.yson_to_bash_test("123", "123")
        self.yson_to_bash_test("[a; b; c]", "a\nb\nc")
        self.yson_to_bash_test("[{a=1; b=2}; {c=3; d=4}]", "a\t1\nb\t2\nc\t3\nd\t4")
        self.yson_to_bash_test("[{a=1; b=2}; {c=3; d=4}]", "c\t3\nd\t4", "1")
        self.yson_to_bash_test("[{a=1; b=2}; {c=3; d=4}]", "3", "1/c")
