"""Tests for schema() helper."""

import pytest
import yt.type_info as ti

from yt.yt.flow.library.python.companion.test_harness.schema import schema
from yt.yt.flow.library.python.companion.wire_protocol import ColumnValueType


def test_single_string_column():
    s = schema(name="string")
    assert len(s) == 1
    assert s.columns[0].name == "name"
    assert s.columns[0].type == ti.String


def test_all_types():
    s = schema(
        a="string", b="int64", c="uint64",
        d="double", e="bool", f="boolean", g="any",
    )
    expected = [
        ("a", ti.String),
        ("b", ti.Int64),
        ("c", ti.Uint64),
        ("d", ti.Double),
        ("e", ti.Bool),
        ("f", ti.Bool),
        ("g", ti.Yson),
    ]
    assert len(s) == len(expected)
    for col, (name, ti_type) in zip(s.columns, expected):
        assert col.name == name
        assert col.type == ti_type


def test_wire_types_via_get_column_type():
    s = schema(a="string", b="int64", c="uint64", d="double", e="bool", f="any")
    assert s.get_column_type(0) == ColumnValueType.STRING
    assert s.get_column_type(1) == ColumnValueType.INT64
    assert s.get_column_type(2) == ColumnValueType.UINT64
    assert s.get_column_type(3) == ColumnValueType.DOUBLE
    assert s.get_column_type(4) == ColumnValueType.BOOLEAN
    assert s.get_column_type(5) == ColumnValueType.ANY


def test_column_order_preserved():
    s = schema(z="string", a="int64", m="double")
    assert [c.name for c in s.columns] == ["z", "a", "m"]


def test_find_column_by_name():
    s = schema(x="string", y="int64")
    assert s.find_column("x") == 0
    assert s.find_column("y") == 1
    assert s.find_column("missing") is None


def test_case_insensitive_type():
    s = schema(a="String", b="INT64", c="Bool")
    assert s.columns[0].type == ti.String
    assert s.columns[1].type == ti.Int64
    assert s.columns[2].type == ti.Bool


def test_unknown_type_raises():
    with pytest.raises(ValueError, match="Unknown type.*'blob'"):
        schema(data="blob")


def test_empty_schema():
    s = schema()
    assert len(s) == 0
