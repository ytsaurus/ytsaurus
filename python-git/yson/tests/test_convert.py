# -*- coding: utf-8 -*-

from __future__ import absolute_import

from yt.yson.convert import to_yson_type, json_to_yson, yson_to_json
from yt.packages.six import PY3

import json

def test_convert_json_to_yson():
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

    if not PY3:
        z = str(bytearray(u"Брюссельская капуста", "utf-8"))
    else:
        z = u"Брюссельская капуста"
    assert dict(x) == {"x": 10, "y": 11, "z": z}
    assert x.attributes == "abc"

    assert json_to_yson("abc") == "abc"
    assert json_to_yson({"$type": "string", "$value": "abc"}) == "abc"

def test_convert_yson_to_json():
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
        "f": to_yson_type(u"abacaba", attributes={"attr": 4}),
        "g": {
            b"binary_key": b"binary value",
            to_yson_type(b"yson_key"): to_yson_type(b"yson value"),
        },
        "h": {
            "true_value": to_yson_type(True),
            "false_value": to_yson_type(False),
        },
    })

    # Verify that result is json-serializable
    assert json.loads(json.dumps(x)) == x

    assert x["a"] == {"$value": 10, "$attributes": {"attr": 1}}
    assert x["b"] == {"$value": 5.0, "$attributes": {"attr": 2}}
    assert x["c"] == {"$value": "string", "$attributes": {"attr": 3}}
    assert x["d"] == {"$value": {"key": [1, 2]}, "$attributes": {"attr": 4, "$$xxx": "yyy", "other_attr": 10, u"ключ": None}}
    assert x["e"] == {"$value": None, "$attributes": {"x": "y"}}
    assert x["f"] == {"$value": "abacaba", "$attributes": {"attr": 4}}
    assert x["g"] == {"binary_key": "binary value", "yson_key": "yson value"}
    assert x["h"] == {"true_value": True, "false_value": False}
    assert set(x.keys()) == set(["a", "b", "c", "d", "e", "f", "g", "h"])
