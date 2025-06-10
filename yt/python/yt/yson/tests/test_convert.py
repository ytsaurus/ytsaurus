# -*- coding: utf-8 -*-

from __future__ import absolute_import

from yt.test_helpers import assert_items_equal
from yt.yson.convert import YsonUint64, to_yson_type, json_to_yson, yson_to_json

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


def test_convert_yson_to_json_annotate_with_types():
    check_list = [1, 1.0, "1", b"1", to_yson_type(b"1"), YsonUint64(1), True]
    check_list_annotated = [
        {"$value": 1, "$type": "int64"},
        {"$value": 1.0, "$type": "double"},
        {"$value": "1", "$type": "string"},
        {"$value": "1", "$type": "string"},
        {"$value": "1", "$type": "string"},
        {"$value": 1, "$type": "uint64"},
        {"$value": True, "$type": "bool"},
    ]

    x = yson_to_json(
        {
            "a": to_yson_type(10, attributes={"attr": 1.0}),
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
            "e": to_yson_type(None),
            "f": to_yson_type(u"abacaba"),
            "g": {
                b"binary_key": b"binary value",
                to_yson_type(b"yson_key"): to_yson_type(b"yson value"),
            },
            "h": {
                "true_value": to_yson_type(True),
                "false_value": to_yson_type(False),
            },
            "z": ["a", "b", to_yson_type(
                {"c": {"d" : check_list}},
                attributes={"a": check_list},
            )],
        },
        annotate_with_types=True,
    )

    # Verify that result is json-serializable
    assert json.loads(json.dumps(x)) == x

    assert x["a"] == {"$value": 10, "$attributes": {"attr": {"$value": 1.0, "$type": "double"}}, "$type": "int64"}
    assert x["b"] == {"$value": 5.0, "$attributes": {"attr": {"$value": 2, "$type": "int64"}}, "$type": "double"}
    assert x["c"] == {"$value": "string", "$attributes": {"attr": {"$value": 3, "$type": "int64"}}, "$type": "string"}
    assert x["d"] == {
        "$value": {"key": [{"$value": 1, "$type": "int64"}, {"$value": 2, "$type": "int64"}]},
        "$attributes": {
            "attr": {"$value": 4, "$type": "int64"},
            "$$xxx": {"$value": "yyy", "$type": "string"},
            "other_attr": {"$value": 10, "$type": "int64"},
            u"ключ": None
        }
    }
    assert x["e"] is None
    assert x["f"] == {"$value": "abacaba", "$type": "string"}
    assert x["g"] == {"binary_key": {"$value": "binary value", "$type": "string"}, "yson_key": {"$value": "yson value", "$type": "string"}}
    assert x["h"] == {"true_value": {"$value": True, "$type": "bool"}, "false_value": {"$value": False, "$type": "bool"}}

    assert_items_equal(x["z"], [
        {"$value": "a", "$type": "string"},
        {"$value": "b", "$type": "string"},
        {"$value": {"c": {"d": check_list_annotated}}, "$attributes": {"a": check_list_annotated}},
    ])

    assert set(x.keys()) == set(["a", "b", "c", "d", "e", "f", "g", "h", "z"])


def test_convert_yson_to_json_print_attributes_passing():
    x = yson_to_json(
        {
            "a": to_yson_type(10, attributes={"a": 1}),
            "d": to_yson_type(
                {
                    "b": [
                        to_yson_type(
                            11,
                            attributes={"a": 4},
                        ),
                        to_yson_type(
                            10,
                            attributes={"a": 5},
                        ),
                    ],
                },
                attributes={"a": 6},
            ),
        },
        print_attributes=False,
    )

    # Verify that result is json-serializable
    assert json.loads(json.dumps(x)) == x

    assert x == {
        "a": 10,
        "d": {"b": [11, 10]},
    }

    assert set(x.keys()) == set(["a", "d"])
