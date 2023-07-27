import pytest
import re
import sys
import devtools.ya.yalibrary.streaming_json_dumper as dumper


def check(obj, expected):
    val = dumper.dumps(obj)
    if isinstance(expected, list):
        assert val in expected
    else:
        assert expected == val


def test_null():
    check(None, b"null")


def test_bool():
    check(False, b"false")
    check(True, b"true")


def test_ascii():
    given = "Simple ASCII string"
    expected = ('"' + given + '"').encode()
    check(given, expected)


def test_escaping():
    check("\x00", br'"\u0000"')
    check("\x03", br'"\u0003"')
    check("\b", br'"\b"')
    check("\f", br'"\f"')
    check("\t", br'"\t"')
    check("\r", br'"\r"')
    check("\n", br'"\n"')
    check("\"", br'"\""')
    check("\\", br'"\\"')


@pytest.mark.skipif(sys.version_info.major > 2, reason="Python2 only tests")
def test_utf8_str():
    inputs = [
        u"\u0080",
        u"\u0480",
        u"\ud7ff",
        u"\ue000",
        u"\U000100ff",
        u"\U001000ff",
    ]
    for inp in inputs:
        given = inp.encode("utf-8")
        expected = b'"' + given + b'"'
        check(given, expected)


def test_unicode():
    inputs = [
        u"\u0080",
        u"\u0480",
        u"\ud7ff",
        u"\ue000",
        u"\U000100ff",
        u"\U001000ff",
    ]
    for inp in inputs:
        expected = b'"' + inp.encode("utf-8") + b'"'
        check(inp, expected)


@pytest.mark.skipif(sys.version_info < (3, 3), reason="Python3 only tests")
def test_bytes():
    # It's not possible to serialize bytes type
    with pytest.raises(ValueError):
        check(b"", None)


def test_int():
    # int32
    check(2147483647, b"2147483647")
    check(-2147483648, b"-2147483648")
    # int64
    check(9223372036854775807, b"9223372036854775807")
    check(-9223372036854775808, b"-9223372036854775808")


def test_big_int():
    with pytest.raises(ValueError):
        check(340282366920938463463374607431768211456, b"340282366920938463463374607431768211456")


def test_float():
    check(1.0, b"1.0")
    check(-1.0, b"-1.0")
    check(0.1, b"0.1")
    check(-0.1, b"-0.1")
    check(0.000001, [b"0.000001", b"1e-06"])
    check(-0.000001, [b"-0.000001", b"-1e-06"])
    check(0.00000000001, [b"0.00000000001", b"1e-11"])
    check(-0.00000000001, [b"-0.00000000001", b"-1e-11"])
    # Just a number from the real case
    check(6445223.822633137, b"6445223.822633137")


def test_inf_nan():
    with pytest.raises(ValueError):
        check(float("nan"), "dummy")
    with pytest.raises(ValueError):
        check(float("inf"), "dummy")
    with pytest.raises(ValueError):
        check(float("-inf"), "dummy")


def test_dict():
    check({}, b'{}')
    check({"k": "v"}, b'{"k":"v"}')
    check({"k": "v", "k2": "v2"}, [b'{"k":"v","k2":"v2"}', b'{"k2":"v2","k":"v"}'])


def test_list():
    check([], b"[]")
    check([1, 2, 3], b"[1,2,3]")


def test_nested():
    check({"k": [1, 2, 3]}, b'{"k":[1,2,3]}')
    check([{"k1": "v1"}, {"k2": "v2"}, {"k3": "v3"}], b'[{"k1":"v1"},{"k2":"v2"},{"k3":"v3"}]')


def test_tuple():
    check((), b"[]")
    check((1, 2, 3), b"[1,2,3]")


def test_named_tuple():
    from collections import namedtuple
    t1 = namedtuple("T1", "")()
    t2 = namedtuple("T2", "item1,item2,item3")(1, 2, 3)
    check(t1, b"[]")
    check(t2, b"[1,2,3]")


def test_callable():
    def callable(f):
        f.write(b'"value"')

    data = {
        "k": callable,
    }
    check(data, b'{"k":"value"}')


def test_dict_loop_detection():
    d1 = {}
    d2 = {"k1": d1}
    d1["k2"] = d2
    with pytest.raises(ValueError, match=re.escape('Circular reference found: -->["k2"]["k1"]-->')):
        check(d1, "dummy")


def test_list_loop_detection():
    d1 = []
    d2 = [d1]
    d1.append(d2)
    with pytest.raises(ValueError, match=re.escape('Circular reference found: -->[0][0]-->')):
        check(d1, "dummy")


def test_tuple_loop_detection():
    lst = []
    tpl = ([], lst)
    lst.append(tpl)
    with pytest.raises(ValueError, match=re.escape('Circular reference found: -->[1][0]-->')):
        check(tpl, "dummy")


def test_mixed_loop_detection():
    data = {
        "tree": {
            "items": [
                # LOOP_START
                {
                    "children": [
                        {},  # 0
                        {},  # 1
                        {},  # 2
                        {},  # 3
                        {},  # 4
                        # 5 will refer to the LOOP_START
                    ]
                },
                {},
            ]
        }
    }
    # Create a loop:
    data["tree"]["items"][0]["children"].append(data["tree"]["items"][0])

    with pytest.raises(ValueError, match=re.escape('Circular reference found: ["tree"]["items"][0]-->["children"][5]-->')):
        check(data, "dummy")
