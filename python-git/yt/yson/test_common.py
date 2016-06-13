# -*- coding: utf-8 -*-

from __future__ import absolute_import

import unittest

import yt.yson.writer
import yt.yson.parser
from yt.yson.yson_types import YsonUint64

try:
    import yt_yson_bindings
except ImportError:
    yt_yson_bindings = None


class CommonTestBase(object):
    @staticmethod
    def loads(*args, **kws):
        raise NotImplementedError

    @staticmethod
    def dumps(*args, **kws):
        raise NotImplementedError

    def test_long_integers(self):
        num = 1
        self.assertEqual("1", self.dumps(num))
        loaded = self.loads("1")
        self.assertEqual(1, loaded)
        self.assertTrue(isinstance(loaded, long))

        num = 2 ** 50
        loaded = self.loads(self.dumps(num))
        self.assertEqual(2 ** 50, loaded)
        self.assertTrue(isinstance(loaded, long))

        yson_num = "1u"
        loaded = self.loads(yson_num)
        self.assertEqual(1, loaded)
        self.assertTrue(isinstance(loaded, YsonUint64))
        self.assertEqual("1u", self.dumps(loaded))

    def test_equalities(self):
        num = 1
        num_long = 1L
        lst = [1, 2, 3]
        s = "abc"
        f = 1.0
        d = {"x": 2}
        self.assertEqual(self.loads(self.dumps(num)), num_long)

        self.assertFalse(None == self.loads(self.dumps(num)))
        self.assertFalse(None == self.loads(self.dumps(lst)))
        self.assertFalse(None == self.loads(self.dumps(s)))
        self.assertFalse(None == self.loads(self.dumps(f)))

        self.assertFalse(lst == self.loads(self.dumps(f)))
        self.assertFalse(num == self.loads(self.dumps(s)))
        self.assertFalse(self.loads(self.dumps(d)) == s)


class TestCommonDefault(unittest.TestCase, CommonTestBase):
    @staticmethod
    def loads(*args, **kws):
        return yt.yson.loads(*args, **kws)

    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.dumps(*args, **kws)


class TestCommonPython(unittest.TestCase, CommonTestBase):
    @staticmethod
    def loads(*args, **kws):
        return yt.yson.parser.loads(*args, **kws)

    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.writer.dumps(*args, **kws)


if yt_yson_bindings:
    class TestCommonBindings(unittest.TestCase, CommonTestBase):
        @staticmethod
        def loads(*args, **kws):
            return yt_yson_bindings.loads(*args, **kws)

        @staticmethod
        def dumps(*args, **kws):
            return yt_yson_bindings.dumps(*args, **kws)
