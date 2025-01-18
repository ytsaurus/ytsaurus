import pytest

from yt.yt_sync.core.model.column import YtColumn
from yt.yt_sync.core.spec import Column
from yt.yt_sync.core.spec import TypeV1
from yt.yt_sync.core.spec.details import SchemaSpec


def _make(name: str, type_name: str | None, **kwargs) -> YtColumn:
    type_v3 = kwargs.pop("type_v3", None)
    required = kwargs.pop("required", None)
    if type_v3:
        type_v3 = SchemaSpec.parse([{"name": name, "type_v3": type_v3}])[0].type_v3
    return YtColumn.make(Column(name=name, type=type_name, required=required, type_v3=type_v3, **kwargs))


class TestYtColumn:
    def test_column_eq_bad_type(self):
        with pytest.raises(AssertionError):
            _ = YtColumn.make(Column("Key", "uint64")) == 1

    def test_column_eq(self):
        assert _make("Key", "uint64") == _make("Key", "uint64")
        assert _make("Key", "uint64", sort_order="ascending") == _make("Key", "uint64", sort_order="ascending")
        assert _make("Key", "uint64", expression="farm_hash(Key) % 10") == _make(
            "Key", "uint64", expression="farm_hash(Key) % 10"
        )
        assert _make("Key", "uint64", expression="farm_hash(Key )% 10") == _make(
            "Key", "uint64", expression="farm_hash( Key) %10"
        )
        assert _make("Key", "uint64", required=True) == _make("Key", "uint64", required=True)
        assert _make("Key", "uint64", lock="l1") == _make("Key", "uint64", lock="l1")
        assert _make("Key", "uint64", lock="") == _make("Key", "uint64", lock=None)
        assert _make("Key", "uint64", group="g1") == _make("Key", "uint64", group="g1")
        assert _make("Key", "uint64", group="") == _make("Key", "uint64", group=None)
        assert _make("Key", "uint64", aggregate="sum") == _make("Key", "uint64", aggregate="sum")
        assert _make("Key", "uint64", aggregate="") == _make("Key", "uint64", aggregate=None)
        assert _make("Key", "uint64", max_inline_hunk_size=1) == _make("Key", "uint64", max_inline_hunk_size=1)
        assert _make("Key", "any") == _make("Key", None, type_v3={"type_name": "optional", "item": "yson"})

    def test_field_not_eq(self):
        assert _make("Key1", "uint64") != _make("Key2", "uint64")
        assert _make("Key", "uint64") != _make("Key", "int64")
        assert _make("Key", "uint64", sort_order="ascending") != _make("Key", "uint64", sort_order="descending")
        assert _make("Key", "uint64", expression="farm_hash(Key) % 10") != _make(
            "Key", "uint64", expression="farm_hash(Key) % 20"
        )
        assert _make("Key", "uint64", required=True) != _make("Key", "uint64", required=False)
        assert _make("Key", "uint64", lock="l1") != _make("Key", "uint64", lock="l2")
        assert _make("Key", "uint64", group="g1") != _make("Key", "uint64", group="g2")
        assert _make("Key", "uint64", aggregate="sum") != _make("Key", "uint64", aggregate="min")
        assert _make("Key", "uint64", max_inline_hunk_size=1) != _make("Key", "uint64", max_inline_hunk_size=2)
        assert _make("Key", "any") != _make(
            "Key", None, type_v3={"type_name": "optional", "item": {"type_name": "list", "item": "int64"}}
        )

    def test_normalized_expression_disabled(self):
        YtColumn.DISABLE_NORMALIZED_EXPRESSION = True
        assert _make("Key", "uint64", expression="farm_hash(Key )% 10") != _make(
            "Key", "uint64", expression="farm_hash( Key) %10"
        )
        assert _make("Key", "uint64", expression="farm_hash(Key) % 10") == _make(
            "Key", "uint64", expression="farm_hash(Key) % 10"
        )
        YtColumn.DISABLE_NORMALIZED_EXPRESSION = False

    def test_yt_attributes(self):
        assert {
            "name": "Key",
            "type": "uint64",
            "type_v3": {"type_name": "optional", "item": "uint64"},
        } == _make("Key", "uint64").yt_attributes

        assert {
            "name": "Key",
            "type": "uint64",
            "type_v3": "uint64",
            "sort_order": "ascending",
            "expression": "farm_hash(Key) % 10",
            "required": True,
            "lock": "api",
            "group": "default",
            "aggregate": "sum",
            "max_inline_hunk_size": 10,
        } == _make(
            "Key",
            "uint64",
            sort_order="ascending",
            expression="farm_hash(Key) % 10",
            required=True,
            lock="api",
            group="default",
            aggregate="sum",
            max_inline_hunk_size=10,
        ).yt_attributes

        assert {"name": "Key", "type": "any", "type_v3": {"type_name": "optional", "item": "yson"}} == _make(
            "Key", "any"
        ).yt_attributes

        assert {
            "name": "Key",
            "type": "any",
            "required": True,
            "type_v3": {"type_name": "list", "item": "int64"},
        } == _make("Key", "any", required=True, type_v3={"type_name": "list", "item": "int64"}).yt_attributes

        assert {
            "name": "Key",
            "type": "any",
            "required": True,
            "type_v3": {"type_name": "list", "item": "int64"},
        } == _make("Key", None, type_v3={"type_name": "list", "item": "int64"}).yt_attributes

        assert {
            "name": "Key",
            "type": "any",
            "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "uint64"}},
        } == _make(
            "Key", None, type_v3={"type_name": "optional", "item": {"type_name": "list", "item": "uint64"}}
        ).yt_attributes

    def test_make_minimal(self):
        column = _make("k", "uint64")
        assert column.name == "k"
        assert column.column_type == "uint64"
        assert column.column_type_v3 == {"type_name": "optional", "item": "uint64"}
        assert not column.sort_order
        assert not column.expression
        assert not column.required
        assert not column.lock
        assert not column.group
        assert not column.aggregate
        assert not column.max_inline_hunk_size
        assert not column.is_key_column
        assert not column.has_hunks

    def test_make_minimal_type_v3_only(self):
        column = _make("k", None, type_v3="uint64")
        assert column.name == "k"
        assert column.column_type == "uint64"
        assert column.column_type_v3 == "uint64"
        assert not column.sort_order
        assert not column.expression
        assert column.required
        assert not column.lock
        assert not column.group
        assert not column.aggregate
        assert not column.max_inline_hunk_size
        assert not column.is_key_column
        assert not column.has_hunks

    def test_make_minimal_type_and_type_v3(self):
        column = _make("k", "any", type_v3={"type_name": "optional", "item": "yson"})
        assert column.name == "k"
        assert column.column_type == "any"
        assert column.column_type_v3 == {"type_name": "optional", "item": "yson"}
        assert not column.sort_order
        assert not column.expression
        assert not column.required
        assert not column.lock
        assert not column.group
        assert not column.aggregate
        assert not column.max_inline_hunk_size
        assert not column.is_key_column
        assert not column.has_hunks

    def test_make_full_key(self):
        column = YtColumn.make(
            Column(
                name="k",
                type=TypeV1.UINT64,
                sort_order=Column.SortOrder.ASCENDING,
                expression="farm_hash(k) % 10",
                required=True,
                group="default",
            )
        )
        assert column.name == "k"
        assert column.column_type == "uint64"
        assert column.column_type_v3 == "uint64"
        assert column.sort_order == "ascending"
        assert column.expression == "farm_hash(k) % 10"
        assert column.required
        assert not column.lock
        assert column.group == "default"
        assert not column.aggregate
        assert not column.max_inline_hunk_size
        assert column.is_key_column
        assert not column.has_hunks

    def test_make_full_data_with_aggregate(self):
        column = YtColumn.make(
            Column(
                name="k",
                type=TypeV1.UINT64,
                required=True,
                lock="api",
                group="default",
                aggregate=Column.AggregateFunc.SUM,
            )
        )
        assert "k" == column.name
        assert "uint64" == column.column_type
        assert not column.sort_order
        assert not column.expression
        assert column.required
        assert "api" == column.lock
        assert "default" == column.group
        assert "sum" == column.aggregate
        assert not column.max_inline_hunk_size
        assert not column.has_hunks

    def test_make_full_data_with_hunks(self):
        column = YtColumn.make(
            Column(
                name="k",
                type=TypeV1.STRING,
                required=True,
                lock="api",
                group="default",
                max_inline_hunk_size=10,
            )
        )
        assert "k" == column.name
        assert "string" == column.column_type
        assert not column.sort_order
        assert not column.expression
        assert column.required
        assert "api" == column.lock
        assert "default" == column.group
        assert not column.aggregate
        assert 10 == column.max_inline_hunk_size
        assert column.has_hunks

    def test_is_data_modification_required_sort_order(self):
        assert not _make("k", "uint64").is_data_modification_required(_make("k", "uint64"))
        assert not _make("k", "uint64", sort_order="ascending").is_data_modification_required(
            _make("k", "uint64", sort_order="ascending")
        )
        assert _make("k", "uint64", sort_order="ascending").is_data_modification_required(
            _make("k", "uint64", sort_order="descending")
        )
        assert _make("k", "uint64", sort_order="ascending").is_data_modification_required(_make("k", "uint64"))
        assert _make("k", "uint64").is_data_modification_required(_make("k", "uint64", sort_order="ascending"))

    def test_is_data_modification_required_expression(self):
        assert not _make("k", "uint64").is_data_modification_required(_make("k", "uint64"))
        assert not _make(
            "k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10"
        ).is_data_modification_required(_make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10"))
        assert not _make(
            "k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10"
        ).is_data_modification_required(_make("k", "uint64", sort_order="ascending"))
        assert not _make("k", "uint64", sort_order="ascending").is_data_modification_required(
            _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10")
        )
        assert _make(
            "k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10"
        ).is_data_modification_required(_make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 20"))

    def test_is_downtime_required(self):
        assert _make("k", "uint64", sort_order="ascending").is_downtime_required(
            _make("k", "uint64", sort_order="descending")
        )

        assert not _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10").is_downtime_required(
            _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 20")
        )

        assert not _make("k", "uint64").is_downtime_required(_make("k", "uint64"))

    def test_is_compatible(self):
        # basic
        assert _make("k", "uint64").is_compatible(_make("k", "uint64"))
        assert not _make("k1", "uint64").is_compatible(_make("k2", "uint64"))
        assert not _make("k", "uint64").is_compatible(_make("k", "int64"))
        # sort order
        assert _make("k", "uint64", sort_order="ascending").is_compatible(_make("k", "uint64", sort_order="ascending"))
        assert _make("k", "uint64", sort_order="ascending").is_compatible(_make("k", "uint64", sort_order="descending"))
        # expression
        assert _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10").is_compatible(
            _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10")
        )
        assert _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10").is_compatible(
            _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 20")
        )
        assert not _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 10").is_compatible(
            _make("k", "uint64", sort_order="ascending")
        )
        assert not _make("k", "uint64", sort_order="ascending").is_compatible(
            _make("k", "uint64", sort_order="ascending", expression="farm_hash(k) % 20")
        )
        # required
        assert _make("k", "uint64", required=True).is_compatible(_make("k", "uint64", required=True))
        assert _make("k", "uint64", required=True).is_compatible(_make("k", "uint64", required=False))
        assert not _make("k", "uint64", required=False).is_compatible(_make("k", "uint64", required=True))
        # lock
        assert _make("k", "uint64", lock=None).is_compatible(_make("k", "uint64", lock="l1"))
        assert _make("k", "uint64", lock="l1").is_compatible(_make("k", "uint64", lock=None))
        assert _make("k", "uint64", lock="l1").is_compatible(_make("k", "uint64", lock="l2"))
        # group
        assert _make("k", "uint64", group=None).is_compatible(_make("k", "uint64", group="g1"))
        assert _make("k", "uint64", group="g1").is_compatible(_make("k", "uint64", group=None))
        assert _make("k", "uint64", group="g1").is_compatible(_make("k", "uint64", group="g2"))
        # aggregate
        assert _make("k", "uint64", aggregate=None).is_compatible(_make("k", "uint64", aggregate="sum"))
        assert _make("k", "uint64", aggregate="sum").is_compatible(_make("k", "uint64", aggregate="sum"))
        assert not _make("k", "uint64", aggregate="sum").is_compatible(_make("k", "uint64", aggregate=None))
        assert not _make("k", "uint64", aggregate="sum").is_compatible(_make("k", "uint64", aggregate="min"))
        # max_inline_hunk
        assert _make("k", "string", max_inline_hunk_size=None).is_compatible(
            _make("k", "string", max_inline_hunk_size=1)
        )
        assert _make("k", "string", max_inline_hunk_size=1).is_compatible(_make("k", "string", max_inline_hunk_size=2))
        assert not _make("k", "string", max_inline_hunk_size=1).is_compatible(
            _make("k", "string", max_inline_hunk_size=None)
        )
