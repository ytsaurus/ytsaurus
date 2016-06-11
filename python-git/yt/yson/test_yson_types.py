# -*- coding: utf-8 -*-

from __future__ import absolute_import

import unittest

from yt.yson.yson_types import YsonEntity, YsonBoolean


class TestYsonTypes(unittest.TestCase):
    def test_entity(self):
        self.assertEqual(YsonEntity(), YsonEntity())

    def test_boolead(self):
        a = YsonBoolean(False)
        b = YsonBoolean(False)
        self.assertTrue(a == b)
        self.assertFalse(a != b)

        a.attributes["attr"] = 10
        b.attributes["attr"] = 20
        self.assertFalse(a == b)
        self.assertTrue(a != b)
