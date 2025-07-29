# -*- coding: utf-8 -*-

from __future__ import absolute_import

from yt.test_helpers import assert_items_equal
from yt.yson.convert import YsonUint64, to_yson_type, json_to_yson, yson_to_json
from yt.yson.yson_types import YsonStringProxy, YsonMap, YsonInt64, YsonString, YsonUnicode

import json
import pytest


def test_convert_json_to_yson():
    test_data_python = {
        "$value": {
            "x": {
                "$value": 10,
                "$attributes": {},
            },
            "y": {
                "$value": 11,
                "$attributes": {},
            },
            "z": u"Брюссельская капуста",
            b"g": b"Some invalid utf bytes \xee\xdc",
        },
        "$attributes": {
            "$value": "abc",
            "$attributes": {},
        }
    }

    test_data_yson = json_to_yson(test_data_python)

    assert type(test_data_yson) is YsonMap
    assert test_data_yson["x"] == 10 and type(test_data_yson["x"]) is YsonInt64
    assert test_data_yson["y"] == 11 and type(test_data_yson["y"]) is YsonInt64
    assert test_data_yson["z"] == "Брюссельская капуста" and type(test_data_yson["z"]) is YsonUnicode
    assert test_data_yson["g"] == b"Some invalid utf bytes \xee\xdc" and type(test_data_yson["g"]) is YsonString
    assert test_data_yson.attributes == "abc"

    assert json_to_yson("abc") == "abc"

    abc_string = json_to_yson({"$type": "string", "$value": "abc"})
    assert abc_string == "abc" and type(abc_string) is YsonUnicode


def test_convert_yson_to_json():
    yson_string_proxy = YsonStringProxy()
    yson_string_proxy._bytes = b'some_invalid_bytes'

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
                u"ключ": None,
            }
        ),
        "e": to_yson_type(None, attributes={"x": "y"}),
        "f": to_yson_type(u"abacaba", attributes={"attr": 4}),
        "g": {
            b"binary_key": b"binary value",
            b"binary_proxy_key": yson_string_proxy,
            to_yson_type(b"yson_key"): to_yson_type(b"yson value"),
        },
        "h": {
            "true_value": to_yson_type(True),
            "false_value": to_yson_type(False),
        },
    })

    assert x["a"] == {"$value": 10, "$attributes": {"attr": 1}}
    assert x["b"] == {"$value": 5.0, "$attributes": {"attr": 2}}
    assert x["c"] == {"$value": "string", "$attributes": {"attr": 3}}
    assert x["d"] == {"$value": {"key": [1, 2]}, "$attributes": {"attr": 4, "$$xxx": "yyy", "other_attr": 10, u"ключ": None}}
    assert x["e"] == {"$value": None, "$attributes": {"x": "y"}}
    assert x["f"] == {"$value": "abacaba", "$attributes": {"attr": 4}}
    assert x["g"] == {"binary_key": "binary value", "binary_proxy_key": "some_invalid_bytes", "yson_key": "yson value"}
    assert x["h"] == {"true_value": True, "false_value": False}
    assert set(x.keys()) == set(["a", "b", "c", "d", "e", "f", "g", "h"])

    assert json.loads(json.dumps(x)) == x

    # with bytes
    with pytest.raises(UnicodeDecodeError):
        yson_to_json(
            use_byte_strings=False,
            yson_tree={
                "b_invalid": b"str invalid bytes \xee\xdc",
            },
        )
    value_yson_proxy = YsonStringProxy()
    value_yson_proxy._bytes = b"str proxy"
    test_data_python = yson_to_json(
        use_byte_strings=True,
        yson_tree={
            "s": YsonUnicode("str unicode"),
            "b": YsonString(b"str bytes"),
            "b_invalid": YsonString(b"str invalid bytes \xee\xdc"),
            "bp": value_yson_proxy,
        },
    )
    assert test_data_python["s"] == "str unicode" and type(test_data_python["s"]) is str
    assert test_data_python["b"] == b"str bytes" and type(test_data_python["b"]) is YsonString
    assert test_data_python["b_invalid"] == b"str invalid bytes \xee\xdc" and type(test_data_python["b_invalid"]) is YsonString
    assert test_data_python["bp"] == b"str proxy" and type(test_data_python["bp"]) is bytes


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
                    u"ключ": None,
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
