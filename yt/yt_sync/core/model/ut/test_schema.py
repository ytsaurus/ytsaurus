import pytest

from yt.yson.yson_types import YsonList
from yt.yt_sync.core.model.column import YtColumn
from yt.yt_sync.core.model.schema import YtSchema
from yt.yt_sync.core.model.types import Types


class TestYtSchema:
    def test_make_empty(self):
        with pytest.raises(AssertionError):
            YtSchema.parse([])

    def test_make_ordered_simple(self):
        schema = YtSchema.parse([{"name": "v", "type": "uint64"}])
        assert schema.strict
        assert not schema.unique_keys
        assert schema.is_ordered
        assert 1 == len(schema.columns)
        column = schema.columns[0]
        assert "v" == column.name
        assert "uint64" == column.column_type

    def test_make_sorted_simple(self):
        schema = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "uint64"}]
        )
        assert schema.strict
        assert schema.unique_keys
        assert not schema.is_ordered
        assert 2 == len(schema.columns)
        assert "k" == schema.columns[0].name
        assert "v" == schema.columns[1].name
        assert not schema.has_hunks

    def test_make_full(self):
        schema = [
            {
                "name": "hash",
                "type": "uint64",
                "sort_order": "ascending",
                "expression": "farm_hash(k) % 10",
                "group": "g1",
            },
            {"name": "k", "type": "uint64", "sort_order": "ascending", "group": "g1"},
            {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": True},
            {"name": "v2", "type": "string", "lock": "l1", "group": "g2", "max_inline_hunk_size": 16},
            {"name": "v3", "type": "double", "lock": "l2", "group": "g3", "aggregate": "sum"},
        ]
        yt_schema = YtSchema.parse(schema)
        assert yt_schema.strict
        assert yt_schema.unique_keys
        assert not yt_schema.is_ordered
        assert 5 == len(yt_schema.columns)
        assert (
            YtColumn(
                name="hash",
                column_type="uint64",
                column_type_v3={"type_name": "optional", "item": "uint64"},
                sort_order="ascending",
                expression="farm_hash(k) % 10",
                group="g1",
            )
            == yt_schema.columns[0]
        )
        assert (
            YtColumn(
                name="k",
                column_type="uint64",
                column_type_v3={"type_name": "optional", "item": "uint64"},
                sort_order="ascending",
                group="g1",
            )
            == yt_schema.columns[1]
        )
        assert (
            YtColumn(name="v1", column_type="uint64", column_type_v3="uint64", lock="l1", group="g2", required=True)
            == yt_schema.columns[2]
        )
        assert (
            YtColumn(
                name="v2",
                column_type="string",
                column_type_v3={"type_name": "optional", "item": "string"},
                lock="l1",
                group="g2",
                max_inline_hunk_size=16,
            )
            == yt_schema.columns[3]
        )
        assert (
            YtColumn(
                name="v3",
                column_type="double",
                column_type_v3={"type_name": "optional", "item": "double"},
                lock="l2",
                group="g3",
                aggregate="sum",
            )
            == yt_schema.columns[4]
        )
        assert yt_schema.has_hunks

    def test_make_from_yson_simple(self):
        schema = YtSchema.parse(YsonList([{"name": "k", "type": "uint64"}]))
        assert schema.strict is True
        assert schema.unique_keys is False
        assert 1 == len(schema.columns)

    def test_make_from_yson_not_strict(self):
        yson_schema = YsonList([{"name": "k", "type": "uint64"}])
        yson_schema.attributes["strict"] = False
        schema = YtSchema.parse(yson_schema)
        assert schema.strict is False
        assert schema.unique_keys is False
        assert 1 == len(schema.columns)

    def test_make_from_yson_implicit_unique_keys(self):
        yson_schema = YsonList(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        schema = YtSchema.parse(yson_schema)
        assert schema.strict is True
        assert schema.unique_keys is True
        assert 2 == len(schema.columns)

    def test_yt_schema(self):
        schema = [
            {
                "name": "hash",
                "type": "uint64",
                "type_v3": {"type_name": "optional", "item": "uint64"},
                "sort_order": "ascending",
                "expression": "farm_hash(k) % 10",
                "group": "g1",
            },
            {
                "name": "k",
                "type": "uint64",
                "type_v3": {"type_name": "optional", "item": "uint64"},
                "sort_order": "ascending",
                "group": "g1",
            },
            {"name": "v1", "type": "uint64", "type_v3": "uint64", "lock": "l1", "group": "g2", "required": True},
            {
                "name": "v2",
                "type": "string",
                "type_v3": {"type_name": "optional", "item": "string"},
                "lock": "l1",
                "group": "g2",
                "max_inline_hunk_size": 16,
            },
            {
                "name": "v3",
                "type": "double",
                "type_v3": {"type_name": "optional", "item": "double"},
                "lock": "l2",
                "group": "g3",
                "aggregate": "sum",
            },
        ]
        yt_schema = YtSchema.parse(schema).yt_schema
        assert isinstance(yt_schema, YsonList)
        assert schema == list(yt_schema)
        assert yt_schema.attributes["unique_keys"] is True
        assert yt_schema.attributes["strict"] is True

    def test_eq_bad_argument(self):
        with pytest.raises(AssertionError):
            _ = YtSchema.parse([]) == 1

    def test_eq_same(self):
        schema = [
            {
                "name": "hash",
                "type": "uint64",
                "sort_order": "ascending",
                "expression": "farm_hash(k) % 10",
                "group": "g1",
            },
            {"name": "k", "type": "uint64", "sort_order": "ascending", "group": "g1"},
            {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": True},
            {"name": "v2", "type": "string", "lock": "l1", "group": "g2", "max_inline_hunk_size": 16},
            {"name": "v3", "type": "double", "lock": "l2", "group": "g3", "aggregate": "sum"},
        ]
        s1 = YtSchema.parse(schema)
        s2 = YtSchema.parse(schema)
        assert s1 == s2
        assert not s1.is_column_set_changed(s2)

    def test_eq_key_column_diff(self):
        s1 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        s2 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "descending"}, {"name": "v", "type": "boolean"}]
        )
        assert s1 != s2

    def test_eq_key_column_order_diff(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "v", "type": "boolean"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v", "type": "boolean"},
            ]
        )
        assert s1 != s2

    def test_eq_non_key_column_diff(self):
        s1 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        s2 = YtSchema.parse([{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "utf8"}])
        assert s1 != s2
        assert not s1.is_column_set_changed(s2)

    def test_eq_non_key_column_order_diff(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "boolean"},
                {"name": "v2", "type": "boolean"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v2", "type": "boolean"},
                {"name": "v1", "type": "boolean"},
            ]
        )
        assert s1 == s2
        assert not s1.is_column_set_changed(s2)

    def test_eq_column_count_diff(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "boolean"},
                {"name": "v2", "type": "boolean"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "boolean"},
            ]
        )
        assert s1 != s2
        assert s1.is_column_set_changed(s2)

    def test_is_uniform_distribution_supported(self):
        assert not YtSchema.parse(
            [{"name": "k", "type": "string", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        ).is_uniform_distribution_supported

        assert YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        ).is_uniform_distribution_supported

        assert YtSchema.parse(
            [{"name": "k", "type": "int64", "sort_order": "descending"}, {"name": "v", "type": "boolean"}]
        ).is_uniform_distribution_supported

        assert not YtSchema.parse(
            [{"name": "k", "type": "uint64"}, {"name": "v", "type": "boolean"}]
        ).is_uniform_distribution_supported

        assert not YtSchema.parse(
            [{"name": "k", "type": "int64"}, {"name": "v", "type": "boolean"}]
        ).is_uniform_distribution_supported

    def test_is_compatible_no_common_key(self):
        s1 = YtSchema.parse(
            [{"name": "k1", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        s2 = YtSchema.parse(
            [{"name": "k2", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        assert not s1.is_compatible(s2)

    def test_is_compatible_no_common_data(self):
        s1 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v1", "type": "boolean"}]
        )
        s2 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v2", "type": "boolean"}]
        )
        assert not s1.is_compatible(s2)

    def test_is_compatible_diff_key_type(self):
        s1 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        s2 = YtSchema.parse(
            [{"name": "k", "type": "int64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        assert not s1.is_compatible(s2)

    def test_is_compatible_diff_data_type(self):
        s1 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        s2 = YtSchema.parse([{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "utf8"}])
        assert not s1.is_compatible(s2)

    def test_is_compatible_diff_optional_to_required(self):
        s1 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v", "type": "boolean", "required": True},
            ]
        )
        assert not s1.is_compatible(s2)

    def test_is_compatible_diff_required_to_optional(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v", "type": "boolean", "required": True},
            ]
        )
        s2 = YtSchema.parse(
            [{"name": "k", "type": "uint64", "sort_order": "ascending"}, {"name": "v", "type": "boolean"}]
        )
        assert s1.is_compatible(s2)

    def test_is_compatible_diff_column_attrs(self):
        s1 = YtSchema.parse(
            [
                {
                    "name": "hash",
                    "type": "uint64",
                    "sort_order": "ascending",
                    "expression": "farm_hash(k) % 10",
                    "group": "g1",
                },
                {"name": "k", "type": "uint64", "sort_order": "ascending", "group": "g1"},
                {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": True},
                {"name": "v2", "type": "string", "lock": "l1", "group": "g2", "max_inline_hunk_size": 16},
                {"name": "v3", "type": "double", "lock": "l2", "group": "g3"},
                {"name": "v4", "type": "any", "lock": "l2", "group": "g3"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {
                    "name": "hash",
                    "type": "uint64",
                    "sort_order": "ascending",
                    "expression": "farm_hash(k) % 20",
                    "group": "g1",
                },
                {"name": "k", "type": "uint64", "sort_order": "descending", "group": "g2"},
                {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": False},
                {"name": "v2", "type": "string", "lock": "l2", "group": "g3", "max_inline_hunk_size": 32},
                {"name": "v3", "type": "double", "lock": "l3", "group": "g4", "aggregate": "sum"},
                {"name": "v4", "type": "any", "lock": "l4", "group": "g5", "max_inline_hunk_size": 32},
            ]
        )
        assert s1.is_compatible(s2)
        assert s1.is_change_with_downtime(s2)

    @pytest.mark.parametrize("allow_add_remove", [True, False])
    def test_is_compatible_diff_column_count(self, allow_add_remove: bool):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending", "group": "g1"},
                {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": True},
                {"name": "v2", "type": "string", "lock": "l1", "group": "g2", "max_inline_hunk_size": 16},
                {"name": "v3", "type": "double", "lock": "l2", "group": "g3"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {
                    "name": "hash",
                    "type": "uint64",
                    "sort_order": "ascending",
                    "expression": "farm_hash(k) % 10",
                    "group": "g1",
                },
                {"name": "k", "type": "uint64", "sort_order": "ascending", "group": "g1"},
                {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": True},
                {"name": "v3", "type": "double", "lock": "l2", "group": "g3"},
                {"name": "v4", "type": "any", "lock": "l2", "group": "g3"},
            ]
        )
        assert allow_add_remove == s1.is_compatible(s2, allow_add_remove)
        assert s1.is_column_set_changed(s2)

    def test_is_compatible_add_required_key_column(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "k2", "type": "uint64", "sort_order": "ascending", "required": True},
                {"name": "v", "type": "uint64"},
            ]
        )
        assert not s1.is_compatible(s2)
        assert s1.is_column_set_changed(s2)

    def test_is_compatible_add_required_data_column(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v2", "type": "uint64", "required": True},
            ]
        )
        assert not s1.is_compatible(s2)
        assert s1.is_column_set_changed(s2)

    def test_is_compatible_type_v3_change(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "any", "type_v3": {"type_name": "optional", "item": "yson"}},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {
                    "name": "v1",
                    "type": "any",
                    "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                },
            ]
        )
        assert not s1.is_compatible(s2)
        assert not s2.is_compatible(s1)

    def test_get_deleted_columns_same(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert not next(s1.get_deleted_columns(s2), None)

    def test_get_deleted_columns(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v2", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert "v2" == next(s1.get_deleted_columns(s2)).name

    def test_get_added_columns_same(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert not next(s1.get_added_columns(s2), None)

    def test_get_added_columns(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v2", "type": "uint64"},
            ]
        )
        assert "v2" == next(s1.get_added_columns(s2)).name

    def test_get_changed_columns_same(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert not next(s1.get_changed_columns(s2), None)
        assert not s1.is_change_with_downtime(s2)

    def test_get_changed_columns_add_delete(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v2", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v3", "type": "uint64"},
            ]
        )
        assert not next(s1.get_changed_columns(s2), None)
        assert s1.is_change_with_downtime(s2)
        assert s1.is_compatible(s2, True)
        assert not s1.is_compatible(s2, False)

    def test_get_changed_columns(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64", "lock": "l1"},
            ]
        )
        s1_column, s2_column = next(s1.get_changed_columns(s2))
        assert "v1" == s1_column.name == s2_column.name
        assert not s1_column.lock
        assert "l1" == s2_column.lock

    def test_is_data_modification_required_same(self):
        s1 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert not s1.is_data_modification_required(s2)
        assert not s1.is_change_with_downtime(s2)

    def test_is_data_modification_required_column_attrs(self):
        s1 = YtSchema.parse(
            [
                {
                    "name": "hash",
                    "type": "uint64",
                    "sort_order": "ascending",
                    "expression": "farm_hash(k) % 10",
                    "group": "g1",
                },
                {"name": "k", "type": "uint64", "sort_order": "ascending", "group": "g1"},
                {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": True},
                {"name": "v2", "type": "string", "lock": "l1", "group": "g2", "max_inline_hunk_size": 16},
                {"name": "v3", "type": "double", "lock": "l2", "group": "g3"},
                {"name": "v4", "type": "any", "lock": "l2", "group": "g3"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {
                    "name": "hash",
                    "type": "uint64",
                    "sort_order": "ascending",
                    "expression": "farm_hash(k) % 10",
                    "group": "g2",
                },
                {"name": "k", "type": "uint64", "sort_order": "ascending", "group": "g2"},
                {"name": "v1", "type": "uint64", "lock": "l1", "group": "g2", "required": False},
                {"name": "v2", "type": "string", "lock": "l2", "group": "g3", "max_inline_hunk_size": 32},
                {"name": "v3", "type": "double", "lock": "l3", "group": "g4", "aggregate": "sum"},
                {"name": "v4", "type": "any", "lock": "l4", "group": "g5", "max_inline_hunk_size": 32},
            ]
        )
        assert not s1.is_data_modification_required(s2)
        assert not s1.is_change_with_downtime(s2)

    def test_is_data_modification_required_remove_column(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v2", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_data_modification_required(s2)

        # Until https://st.yandex-team.ru/YTSYNC-25
        # assert not s1.is_change_with_downtime(s2)
        assert s1.is_change_with_downtime(s2)

    def test_is_data_modification_required_add_key_column(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert not s1.is_key_changed_with_data_modification(s2)
        assert not s1.is_key_changed_with_downtime(s2)
        assert not s1.is_data_modification_required(s2)
        assert not s1.is_change_with_downtime(s2)

    def test_is_data_modification_required_add_data_column(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v2", "type": "uint64"},
            ]
        )
        assert not s1.is_data_modification_required(s2)
        assert not s1.is_change_with_downtime(s2)

    def test_is_data_modification_required_change_sort_order(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "descending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert s1.is_key_changed_with_data_modification(s2)
        assert s1.is_key_changed_with_downtime(s2)
        assert s1.is_data_modification_required(s2)
        assert s1.is_change_with_downtime(s2)

    def test_is_data_modification_required_change_expression(self):
        s1 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 20"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert s1.is_key_changed_with_data_modification(s2)
        assert not s1.is_key_changed_with_downtime(s2)
        assert s1.is_data_modification_required(s2)

        # Until https://st.yandex-team.ru/YTSYNC-25
        # assert not s1.is_change_with_downtime(s2)
        assert s1.is_change_with_downtime(s2)

    def test_is_key_changed_same_schema(self, default_schema: Types.Schema):
        s1 = YtSchema.parse(default_schema)
        s2 = YtSchema.parse(default_schema)
        assert not s1.is_key_changed(s2)
        assert not s1.is_key_changed_with_data_modification(s2)
        assert not s1.is_key_changed_with_downtime(s2)
        assert not s1.is_change_with_downtime(s2)

    def test_is_key_changed_non_key_column_diff(self):
        s1 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
                {"name": "v2", "type": "uint64"},
            ]
        )
        assert not s1.is_key_changed(s2)
        assert not s1.is_key_changed_with_data_modification(s2)
        assert not s1.is_key_changed_with_downtime(s2)
        assert not s1.is_change_with_downtime(s2)

    def test_is_key_changed_expression(self):
        s1 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 20"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert s1.is_key_changed_with_data_modification(s2)
        assert not s1.is_key_changed_with_downtime(s2)

        # Until https://st.yandex-team.ru/YTSYNC-25
        # assert not s1.is_change_with_downtime(s2)
        assert s1.is_change_with_downtime(s2)

    def test_is_key_changed_sort_order(self):
        s1 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "descending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert s1.is_key_changed_with_data_modification(s2)
        assert s1.is_key_changed_with_downtime(s2)
        assert s1.is_change_with_downtime(s2)

    def test_is_key_column_changed_broader_key(self):
        s1 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert not s1.is_key_changed_with_data_modification(s2)
        assert not s1.is_key_changed_with_downtime(s2)
        assert not s1.is_change_with_downtime(s2)

    def test_is_key_changed_for_key_column_order_change(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "k3", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "k3", "type": "uint64", "sort_order": "ascending"},
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert s1.is_key_changed_with_data_modification(s2)
        assert s1.is_key_changed_with_downtime(s2)
        assert s1.is_change_with_downtime(s2)

    def test_is_key_changed_remove_key_column(self):
        s1 = YtSchema.parse(
            [
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "k3", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = YtSchema.parse(
            [
                {"name": "k2", "type": "uint64", "sort_order": "ascending"},
                {"name": "k3", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        assert s1.is_key_changed(s2)
        assert s1.is_key_changed_with_data_modification(s2)
        assert s1.is_key_changed_with_downtime(s2)
        assert s1.is_change_with_downtime(s2)

    def test_to_unsorted(self):
        s1 = YtSchema.parse(
            [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(k1) % 10"},
                {"name": "k1", "type": "uint64", "sort_order": "ascending"},
                {"name": "v1", "type": "uint64"},
            ]
        )
        s2 = s1.to_unsorted()
        assert s2.strict is True
        assert s2.unique_keys is False
        for c in s2.columns:
            assert c.sort_order is None
