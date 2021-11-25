# -*- coding: utf-8 -*-

from yt.testlib import authors

from yt.wrapper.schema import TableSchema, yt_dataclass
from yt.wrapper.schema.types import Uint8, Int32

from typing import Optional, List


@yt_dataclass
class MySubclass:
    foo: int
    bar: bytes
    baz: bool


@yt_dataclass
class MyOtherSubclass:
    time: int


@yt_dataclass
class MyClass:
    my_string: str
    my_int: int
    my_uint8: Uint8
    my_int32: Int32
    my_optional: Optional[int]
    my_list: List[str]
    my_subclass: MySubclass
    my_other_subclass: Optional[MyOtherSubclass]


@authors("ignat")
def test_yt_dataclass():
    table_schema = TableSchema.from_row_type(MyClass)
    yson_repr = table_schema.to_yson_type()
    assert list(yson_repr) == [
        {"name": "my_string", "type_v3": "utf8"},
        {"name": "my_int", "type_v3": "int64"},
        {"name": "my_uint8", "type_v3": "uint8"},
        {"name": "my_int32", "type_v3": "int32"},
        {"name": "my_optional", "type_v3": {"type_name": "optional", "item": "int64"}},
        {"name": "my_list", "type_v3": {"type_name": "list", "item": "utf8"}},
        {"name": "my_subclass", "type_v3": {"type_name": "struct", "members": [
            {"name": "foo", "type": "int64"},
            {"name": "bar", "type": "string"},
            {"name": "baz", "type": "bool"}
        ]}},
        {"name": "my_other_subclass", "type_v3": {"type_name": "optional", "item": {"type_name": "struct", "members": [
            {"name": "time", "type": "int64"},
        ]}}},
    ]

    assert TableSchema.from_yson_type(yson_repr) == table_schema
