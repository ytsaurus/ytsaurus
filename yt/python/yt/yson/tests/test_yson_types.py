# -*- coding: utf-8 -*-

from __future__ import absolute_import

from yt.yson.yson_types import YsonEntity, YsonBoolean, YsonUnicode, YsonString

from yt.packages.six import PY3 # noqa

import pytest

import re

if PY3:
    from yt.yson.yson_types import YsonStringProxy, NotUnicodeError


def test_entity():
    assert YsonEntity() == YsonEntity()


def test_boolean():
    a = YsonBoolean(False)
    b = YsonBoolean(False)
    assert a == b
    assert not (a != b)

    a.attributes["attr"] = 10
    b.attributes["attr"] = 20
    assert not (a == b)
    assert a != b


def test_string():
    if PY3:
        a = YsonUnicode("Кириллица")
        assert a.is_unicode()
        assert a.get_bytes() == "Кириллица".encode("utf-8")
        b = YsonUnicode("Мефодица", encoding="cp1251")
        assert b.is_unicode()
        assert b.get_bytes() == "Мефодица".encode("cp1251")
    c = YsonString(b"Some bytes \xFF")
    assert not c.is_unicode()
    assert c.get_bytes() == b"Some bytes \xFF"


@pytest.mark.skipif("not PY3")
def test_string_proxy():
    a = YsonStringProxy(b"aba")
    assert not a.is_unicode()
    assert a.get_bytes() == b"aba"
    assert isinstance(a, YsonStringProxy)

    b = YsonStringProxy(b"\xFB")
    assert not b.is_unicode()
    assert b.get_bytes() == b"\xFB"

    assert a == b"aba"
    assert b == b"\xFB"
    assert hash(a) == hash(b"aba")
    assert hash(b) == hash(b"\xFB")

    d = {a: "a", "a": a, "b": b, b: "b"}
    assert d[a] == "a"
    assert d[b"aba"] == "a"
    assert d[b] == "b"
    assert d[b"\xFB"] == "b"
    assert "aba" not in d
    assert d["a"] == a
    assert d["b"] == b

    with pytest.raises(NotUnicodeError):
        len(a)
    with pytest.raises(NotUnicodeError):
        str(a)
    with pytest.raises(NotUnicodeError):
        a[0]
    with pytest.raises(NotUnicodeError):
        a.strip()
    with pytest.raises(NotUnicodeError):
        a.replace("a", "A")
    with pytest.raises(NotUnicodeError):
        a + "abc"
    with pytest.raises(NotUnicodeError):
        a + b"abc"
    with pytest.raises(NotUnicodeError):
        "abc" + a
    with pytest.raises(NotUnicodeError):
        b"abc" + a
    with pytest.raises(NotUnicodeError):
        a + a
    with pytest.raises(NotUnicodeError):
        a * 3
    with pytest.raises(NotUnicodeError):
        3 * a

    with pytest.raises(TypeError):
        re.findall("a", a)
