# -*- coding: utf-8 -*-

from __future__ import absolute_import

from yt.yson.yson_types import YsonEntity, YsonBoolean


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
