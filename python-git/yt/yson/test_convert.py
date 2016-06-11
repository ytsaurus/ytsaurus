# -*- coding: utf-8 -*-

from __future__ import absolute_import

import unittest

from yt.yson.convert import to_yson_type, json_to_yson, yson_to_json


class TestConvert(unittest.TestCase):
    def test_convert_json_to_yson(self):
        x = json_to_yson({
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

        self.assertEqual(json_to_yson("abc"), "abc")

    def test_convert_yson_to_json(self):
        x = yson_to_json({
            "a": to_yson_type(10, attributes={"attr": 1}),
            "b": to_yson_type(5.0, attributes={"attr": 2}),
            "c": to_yson_type("string", attributes={"attr": 3}),
            "d": to_yson_type(
                {"key": [1, 2]},
                attributes={
                    "attr": 4,
                    "$xxx": "yyy",
                    "other_attr": to_yson_type(10, attributes={}),
                    u"ключ": None
                }
            ),
            "e": to_yson_type(None, attributes={"x": "y"}),
            "f": to_yson_type(u"abacaba", attributes={"attr": 4})
        })

        self.assertEqual(x["a"], {"$value": 10, "$attributes": {"attr": 1}})
        self.assertEqual(x["b"], {"$value": 5.0, "$attributes": {"attr": 2}})
        self.assertEqual(x["c"], {"$value": "string", "$attributes": {"attr": 3}})
        self.assertEqual(x["d"], {"$value": {"key": [1, 2]}, "$attributes": {"attr": 4, "$$xxx": "yyy", "other_attr": 10, u"ключ": None}})
        self.assertEqual(x["e"], {"$value": None, "$attributes": {"x": "y"}})
        self.assertEqual(x["f"], {"$value": "abacaba", "$attributes": {"attr": 4}})
        self.assertEqual(set(x.keys()), set(["a", "b", "c", "d", "e", "f"]))
