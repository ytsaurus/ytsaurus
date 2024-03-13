# -*- coding: utf-8 -*-

from yt.testlib import authors

from yt.wrapper.schema import _create_row_py_schema, SortColumn, ColumnSchema, TableSchema, yt_dataclass
from yt.wrapper.schema.types import Uint8, Int32, YsonBytes
from yt.wrapper.format import StructuredSkiffFormat
from yt.common import YtError

from typing import Optional, List, Tuple

import yt.type_info as ti

from copy import deepcopy

import pytest


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
    my_yson: YsonBytes
    my_optional: Optional[int]
    my_list: List[str]
    my_subclass: MySubclass
    my_other_subclass: Optional[MyOtherSubclass]


@authors("ignat")
def test_yt_dataclass():
    table_schema = TableSchema.from_row_type(MyClass)

    # Format creation runs validation of py_schema.
    py_schema = _create_row_py_schema(MyClass, table_schema)
    StructuredSkiffFormat([py_schema], for_reading=False)
    StructuredSkiffFormat([py_schema], for_reading=True)

    yson_repr = table_schema.to_yson_type()
    assert list(yson_repr) == [
        {"name": "my_string", "type_v3": "utf8"},
        {"name": "my_int", "type_v3": "int64"},
        {"name": "my_uint8", "type_v3": "uint8"},
        {"name": "my_int32", "type_v3": "int32"},
        {"name": "my_yson", "type_v3": "yson"},
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


@authors("levysotsky")
def test_table_schema():
    table_schema = TableSchema() \
        .add_column("a", ti.String, sort_order="descending") \
        .add_column("b", ti.List[ti.Int8], sort_order="ascending", group="group1") \
        .add_column("c", ti.Utf8) \
        .add_column("d", ti.Struct["x": ti.Optional[ti.Int64], "y": ti.Uuid], group="group1")

    yson_repr = table_schema.to_yson_type()
    assert list(yson_repr) == [
        {"name": "a", "type_v3": "string", "sort_order": "descending"},
        {
            "name": "b",
            "type_v3": {"type_name": "list", "item": "int8"},
            "sort_order": "ascending",
            "group": "group1",
        },
        {"name": "c", "type_v3": "utf8"},
        {
            "name": "d",
            "type_v3": {"type_name": "struct", "members": [
                {"name": "x", "type": {"type_name": "optional", "item": "int64"}},
                {"name": "y", "type": "uuid"},
            ]},
            "group": "group1",
        },
    ]

    assert table_schema.columns == [
        ColumnSchema("a", ti.String, sort_order="descending"),
        ColumnSchema("b", ti.List[ti.Int8], sort_order="ascending", group="group1"),
        ColumnSchema("c", ti.Utf8),
        ColumnSchema("d", ti.Struct["x": ti.Optional[ti.Int64], "y": ti.Uuid], group="group1"),
    ]

    assert table_schema.strict
    assert not table_schema.unique_keys

    assert table_schema == table_schema
    table_schema_copy = deepcopy(table_schema)
    assert table_schema_copy == table_schema

    table_schema_copy.columns[1].sort_order = None
    assert table_schema_copy != table_schema

    assert not TableSchema().is_empty_nonstrict()
    assert TableSchema(strict=False).is_empty_nonstrict()


@authors("levysotsky")
def test_table_schema_sorting():
    table_schema = TableSchema(unique_keys=True) \
        .add_column("a", ti.String, sort_order="ascending") \
        .add_column("b", ti.List[ti.Int8], sort_order="descending") \
        .add_column("c", ti.Utf8) \
        .add_column("d", ti.Struct["x": ti.Optional[ti.Int64], "y": ti.Uuid])
    assert table_schema.unique_keys

    old_table_schema = deepcopy(table_schema)
    sorted_table_schema = table_schema.build_schema_sorted_by(["b", "c"])
    assert sorted_table_schema.columns == [
        ColumnSchema("b", ti.List[ti.Int8], sort_order="ascending"),
        ColumnSchema("c", ti.Utf8, sort_order="ascending"),
        ColumnSchema("a", ti.String),
        ColumnSchema("d", ti.Struct["x": ti.Optional[ti.Int64], "y": ti.Uuid]),
    ]
    # The original schema is unchanged.
    assert old_table_schema == table_schema

    # unique_keys is reset.
    assert not sorted_table_schema.unique_keys

    sorted_table_schema = table_schema.build_schema_sorted_by([
        SortColumn("b"),
        SortColumn("c", sort_order=SortColumn.DESCENDING),
        SortColumn("a", sort_order=SortColumn.ASCENDING),
    ])

    assert sorted_table_schema.columns == [
        ColumnSchema("b", ti.List[ti.Int8], sort_order="ascending"),
        ColumnSchema("c", ti.Utf8, sort_order="descending"),
        ColumnSchema("a", ti.String, sort_order="ascending"),
        ColumnSchema("d", ti.Struct["x": ti.Optional[ti.Int64], "y": ti.Uuid]),
    ]

    # unique_keys is not reset.
    assert sorted_table_schema.unique_keys

    sorted_table_schema = table_schema.build_schema_sorted_by(SortColumn("b"))

    assert sorted_table_schema.columns == [
        ColumnSchema("b", ti.List[ti.Int8], sort_order="ascending"),
        ColumnSchema("a", ti.String),
        ColumnSchema("c", ti.Utf8),
        ColumnSchema("d", ti.Struct["x": ti.Optional[ti.Int64], "y": ti.Uuid]),
    ]


@authors("denvr")
def test_elements_diverge():
    @yt_dataclass
    class MyClassV1:
        my_tuple: Tuple[Int32, Uint8]

    @yt_dataclass
    class MyTableClassV2:
        my_tuple: Tuple[Int32, Uint8, Uint8]

    table_schema_v1 = TableSchema.from_row_type(MyClassV1)
    py_schema = _create_row_py_schema(MyClassV1, table_schema_v1)
    assert py_schema

    with pytest.raises(YtError) as ex:
        table_schema_v2 = TableSchema.from_row_type(MyTableClassV2)
        py_schema = _create_row_py_schema(MyClassV1, table_schema_v2)
    assert "elements type are diverged" in str(ex.value)
