# -*- coding: utf-8 -*-

from __future__ import absolute_import

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
        assert self.dumps(num) == "1"
        loaded = self.loads("1")
        assert loaded == 1
        assert isinstance(loaded, long)

        num = 2 ** 50
        loaded = self.loads(self.dumps(num))
        assert loaded == 2 ** 50
        assert isinstance(loaded, long)

        yson_num = "1u"
        loaded = self.loads(yson_num)
        assert loaded == 1
        assert isinstance(loaded, YsonUint64)
        assert self.dumps(loaded) == "1u"

    def test_equalities(self):
        num = 1
        num_long = 1L
        lst = [1, 2, 3]
        s = "abc"
        f = 1.0
        d = {"x": 2}
        assert self.loads(self.dumps(num)) == num_long

        assert self.loads(self.dumps(num)) is not None
        assert self.loads(self.dumps(lst)) is not None
        assert self.loads(self.dumps(s)) is not None
        assert self.loads(self.dumps(f)) is not None

        assert lst != self.loads(self.dumps(f))
        assert num != self.loads(self.dumps(s))
        assert self.loads(self.dumps(d)) != s


class TestCommonDefault(CommonTestBase):
    @staticmethod
    def loads(*args, **kws):
        return yt.yson.loads(*args, **kws)

    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.dumps(*args, **kws)


class TestCommonPython(CommonTestBase):
    @staticmethod
    def loads(*args, **kws):
        return yt.yson.parser.loads(*args, **kws)

    @staticmethod
    def dumps(*args, **kws):
        return yt.yson.writer.dumps(*args, **kws)


if yt_yson_bindings:
    class TestCommonBindings(CommonTestBase):
        @staticmethod
        def loads(*args, **kws):
            return yt_yson_bindings.loads(*args, **kws)

        @staticmethod
        def dumps(*args, **kws):
            return yt_yson_bindings.dumps(*args, **kws)
