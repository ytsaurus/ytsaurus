# -*- coding: utf-8 -*-

from yt.type_info import typing

from yt.testlib import authors

from yt.wrapper.schema import ColumnSchema, TableSchema
from yt import yson

import copy


@authors("levysotsky")
def test_column_schema():
    a = ColumnSchema("a", typing.Int8, sort_order="ascending")
    b = ColumnSchema("b", typing.List[typing.Optional[typing.Yson]])
    a2 = ColumnSchema("a", typing.Int8)
    assert a.name == "a"
    assert a.type == typing.Int8
    assert a.sort_order == "ascending"
    assert b.name == "b"
    assert b.type == typing.List[typing.Optional[typing.Yson]]
    assert b.sort_order is None
    assert a2.name == "a"
    assert a2.type == typing.Int8
    assert a2.sort_order is None

    assert a.to_yson_type() == {"name": "a", "type_v3": "int8", "sort_order": "ascending"}
    assert b.to_yson_type() == {"name": "b", "type_v3": {"type_name": "list", "item": {"type_name": "optional", "item": "yson"}}}
    assert a2.to_yson_type() == {"name": "a", "type_v3": "int8"}

    assert "type_v3" in str(a)

    assert a == ColumnSchema.from_yson_type(a.to_yson_type())
    assert b == ColumnSchema.from_yson_type(b.to_yson_type())
    assert a2 == ColumnSchema.from_yson_type(a2.to_yson_type())

    a_same = ColumnSchema("a", typing.Int8, sort_order="ascending")
    assert a == a
    assert a == a_same
    assert not (a != a)
    assert not (a != a_same)
    assert a != a2
    assert not (a == a2)
    assert a != b
    assert a != 1
    assert a != None # noqa
    assert not (a == None) # noqa


@authors("levysotsky")
def test_table_schema():
    t1_columns = [
        ColumnSchema("a", typing.String, sort_order="ascending"),
        ColumnSchema("b", typing.Struct["x": typing.Null]),
    ]
    t1 = TableSchema(t1_columns, unique_keys=False, strict=False)
    assert t1.columns == t1_columns
    assert t1.unique_keys == False # noqa
    assert t1.strict == False # noqa

    t1_strict = TableSchema(t1_columns, unique_keys=False)
    assert t1_strict.columns == t1_columns
    assert t1_strict.unique_keys == False # noqa
    assert t1_strict.strict == True # noqa

    t1_yson = yson.to_yson_type(
        [
            {"name": "a", "type_v3": "string", "sort_order": "ascending"},
            {"name": "b", "type_v3": {"type_name": "struct", "members": [{"name": "x", "type": "null"}]}},
        ],
        attributes={"unique_keys": False, "strict": False},
    )
    assert t1.to_yson_type() == t1_yson
    t1_strict_yson = copy.deepcopy(t1_yson)
    t1_strict_yson.attributes["strict"] = True
    assert t1_strict.to_yson_type() == t1_strict_yson

    assert "members" in str(t1)
    assert "strict" in str(t1)

    assert TableSchema.from_yson_type(t1_yson) == t1
    assert TableSchema.from_yson_type(t1_strict_yson) == t1_strict

    t1_same = TableSchema(t1_columns, unique_keys=False, strict=False)
    assert t1 == t1
    assert t1 == t1_same
    assert not (t1 != t1_same)
    assert t1 != t1_strict
    assert not (t1 == t1_strict)
    assert t1_strict == t1_strict
    assert t1 != 1
    assert t1 != None # noqa
    assert not (t1 == None) # noqa
