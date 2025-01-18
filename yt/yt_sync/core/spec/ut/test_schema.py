import random
import string
from typing import Any
from typing import ClassVar

import pytest

from yt.yt_sync.core.spec.details import SchemaSpec
from yt.yt_sync.core.spec.details import TypeV1Spec
from yt.yt_sync.core.spec.details import TypeV3Spec
from yt.yt_sync.core.spec.table import Column
from yt.yt_sync.core.spec.table import TypeV1
from yt.yt_sync.core.spec.table import TypeV3


class TestTypeV1Specification:
    def test_ensure_required(self):
        spec = TypeV1Spec(Column(name="c", type=TypeV1.ANY, required=True))
        spec.ensure(True)

    def test_dump_v3(self):
        for type_name in TypeV1:
            type_v3_name = str(type_name)
            if type_name == TypeV1.BOOLEAN:
                type_v3_name = str(TypeV3.Type.BOOL)
            elif type_name == TypeV1.ANY:
                type_v3_name = str(TypeV3.Type.YSON)

            TypeV1Spec(Column(name="c", type=type_name, required=True)).dump_v3() == type_v3_name
            TypeV1Spec(Column(name="c", type=type_name)).dump_v3() == {"type_name": "optional", "item": type_v3_name}


class TestTypeV3Specification:
    _SIMPLE_TYPES: ClassVar[frozenset[TypeV3.Type]] = TypeV3Spec.SIMPLE_TYPES - frozenset({TypeV3.Type.YSON})

    @pytest.mark.parametrize("type_v3", ["", 1, 1.0, True, False, list()])
    def test_bad_type_v3(self, type_v3: Any):
        with pytest.raises(ValueError):
            SchemaSpec.parse([{"name": "data", "type": "int64", "type_v3": type_v3}])

    def test_type_v1_v3_mismatch(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [{"name": "data", "type": "int64", "type_v3": {"type_name": "optional", "item": "string"}}]
            )

    def test_required_v1_v3_mismatch(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type": "int64",
                        "type_v3": {"type_name": "optional", "item": "int64"},
                        "required": True,
                    }
                ]
            )

    def test_required_v3_v1_mismatch(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type": "int64",
                        "type_v3": "int64",
                        "required": False,
                    }
                ]
            )

    def test_simple(self):
        for simple_type in self._SIMPLE_TYPES:
            schema = SchemaSpec.parse([{"name": "data", "type_v3": str(simple_type)}])
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert column.type_v3 == simple_type

    def test_yson(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse([{"name": "data", "type_v3": str(TypeV3.Type.YSON)}])

    def test_decimal(self):
        schema = SchemaSpec.parse([{"name": "data", "type_v3": {"type_name": "decimal", "precision": 10, "scale": 2}}])
        assert len(schema) == 1
        column = schema[0]
        assert column.type is None
        assert isinstance(column.type_v3, TypeV3.TypeDecimal)
        assert column.type_v3.type_name == TypeV3.Type.DECIMAL
        assert column.type_v3.precision == 10
        assert column.type_v3.scale == 2
        spec = TypeV3Spec(column)
        assert spec.type_v1 == TypeV1.STRING

    @pytest.mark.parametrize("precision,scale", [(0, 0), (36, 1), (30, 11), (8, 9)])
    def test_decimal_bad_precision_scale(self, precision: int, scale: int):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [{"name": "data", "type_v3": {"type_name": "decimal", "precision": precision, "scale": scale}}]
            )

    def test_optional(self):
        for simple_type in TypeV3Spec.SIMPLE_TYPES:
            schema = SchemaSpec.parse(
                [{"name": "data", "type_v3": {"type_name": "optional", "item": str(simple_type)}}]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeItem)
            assert column.type_v3.type_name == TypeV3.Type.OPTIONAL
            assert column.type_v3.item == simple_type

    def test_list_simple(self):
        for simple_type in self._SIMPLE_TYPES:
            schema = SchemaSpec.parse([{"name": "data", "type_v3": {"type_name": "list", "item": str(simple_type)}}])
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeItem)
            assert column.type_v3.type_name == TypeV3.Type.LIST
            assert column.type_v3.item == simple_type

    def test_list_yson(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse([{"name": "data", "type_v3": {"type_name": "list", "item": str(TypeV3.Type.YSON)}}])

    def test_list_optional(self):
        for simple_type in TypeV3Spec.SIMPLE_TYPES:
            schema = SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {"type_name": "list", "item": {"type_name": "optional", "item": str(simple_type)}},
                    }
                ]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeItem)
            assert column.type_v3.type_name == TypeV3.Type.LIST
            assert isinstance(column.type_v3.item, TypeV3.TypeItem)
            assert column.type_v3.item.type_name == TypeV3.Type.OPTIONAL
            assert column.type_v3.item.item == simple_type

    def test_required_v1_any_with_type_v3(self):
        schema = SchemaSpec.parse(
            [{"name": "data", "required": True, "type": "any", "type_v3": {"type_name": "list", "item": "int64"}}]
        )
        assert len(schema) == 1

    def test_list_no_item(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse([{"name": "data", "type_v3": {"type_name": "list"}}])

    def test_list_bad_item(self):
        with pytest.raises((AssertionError, ValueError)):
            SchemaSpec.parse([{"name": "data", "type_v3": {"type_name": "list", "item": 1}}])

    def test_struct(self):
        for simple_type in self._SIMPLE_TYPES:
            schema = SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "struct",
                            "members": [
                                {"name": "m1", "type": str(simple_type)},
                                {"name": "m2", "type": {"type_name": "optional", "item": str(simple_type)}},
                            ],
                        },
                    }
                ]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeMembersOrElements)
            assert column.type_v3.type_name == TypeV3.Type.STRUCT
            assert len(column.type_v3.members) == 2
            m1 = column.type_v3.members[0]
            assert m1.name == "m1"
            assert m1.type == simple_type
            m2 = column.type_v3.members[1]
            assert m2.name == "m2"
            assert isinstance(m2.type, TypeV3.TypeItem)
            assert m2.type.type_name == TypeV3.Type.OPTIONAL
            assert m2.type.item == simple_type

    def test_struct_no_member_name(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {"type_name": "struct", "members": [{"type": "string"}]},
                    }
                ]
            )

    def test_struct_no_member_type(self):
        with pytest.raises(ValueError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {"type_name": "struct", "members": [{"name": "m1"}]},
                    }
                ]
            )

    def test_struct_elements(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {"type_name": "struct", "elements": [{"type": "string"}]},
                    }
                ]
            )

    def test_tuple(self):
        for simple_type in self._SIMPLE_TYPES:
            schema = SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "tuple",
                            "elements": [
                                {"type": str(simple_type)},
                                {"type": {"type_name": "optional", "item": str(simple_type)}},
                            ],
                        },
                    }
                ]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeMembersOrElements)
            assert column.type_v3.type_name == TypeV3.Type.TUPLE
            assert len(column.type_v3.elements) == 2
            e1 = column.type_v3.elements[0]
            assert e1.type == simple_type
            e2 = column.type_v3.elements[1]
            assert isinstance(e2.type, TypeV3.TypeItem)
            assert e2.type.type_name == TypeV3.Type.OPTIONAL
            assert e2.type.item == simple_type

    def test_variant_members(self):
        for simple_type in self._SIMPLE_TYPES:
            schema = SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "variant",
                            "members": [
                                {"name": "m1", "type": str(simple_type)},
                                {"name": "m2", "type": {"type_name": "optional", "item": str(simple_type)}},
                            ],
                        },
                    }
                ]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeMembersOrElements)
            assert column.type_v3.type_name == TypeV3.Type.VARIANT
            assert len(column.type_v3.members) == 2
            m1 = column.type_v3.members[0]
            assert m1.name == "m1"
            assert m1.type == simple_type
            m2 = column.type_v3.members[1]
            assert m2.name == "m2"
            assert isinstance(m2.type, TypeV3.TypeItem)
            assert m2.type.type_name == TypeV3.Type.OPTIONAL
            assert m2.type.item == simple_type

    def test_variant_elements(self):
        for simple_type in self._SIMPLE_TYPES:
            schema = SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "variant",
                            "elements": [
                                {"type": str(simple_type)},
                                {"type": {"type_name": "optional", "item": str(simple_type)}},
                            ],
                        },
                    }
                ]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeMembersOrElements)
            assert column.type_v3.type_name == TypeV3.Type.VARIANT
            assert len(column.type_v3.elements) == 2
            e1 = column.type_v3.elements[0]
            assert e1.type == simple_type
            e2 = column.type_v3.elements[1]
            assert isinstance(e2.type, TypeV3.TypeItem)
            assert e2.type.type_name == TypeV3.Type.OPTIONAL
            assert e2.type.item == simple_type

    def test_dict(self):
        for simple_type in TypeV3Spec.HASHABLE:
            schema = SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "dict",
                            "key": str(simple_type),
                            "value": {"type_name": "optional", "item": str(simple_type)},
                        },
                    }
                ]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeDict)
            assert column.type_v3.type_name == TypeV3.Type.DICT
            assert column.type_v3.key == simple_type
            assert isinstance(column.type_v3.value, TypeV3.TypeItem)
            assert column.type_v3.value.type_name == TypeV3.Type.OPTIONAL
            assert column.type_v3.value.item == simple_type

    def test_dict_unhashable_key(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "dict",
                            "key": "json",
                            "value": "int64",
                        },
                    }
                ]
            )

    def test_tagged(self):
        for simple_type in TypeV3Spec.SIMPLE_TYPES:
            schema = SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "tagged",
                            "tag": str(simple_type),
                            "item": {"type_name": "optional", "item": str(simple_type)},
                        },
                    }
                ]
            )
            assert len(schema) == 1
            column = schema[0]
            assert column.type is None
            assert isinstance(column.type_v3, TypeV3.TypeTagged)
            assert column.type_v3.type_name == TypeV3.Type.TAGGED
            assert column.type_v3.tag == str(simple_type)
            assert column.type_v3.item.type_name == TypeV3.Type.OPTIONAL
            assert column.type_v3.item.item == simple_type

    def test_tagged_empty_tag(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "tagged",
                            "tag": "",
                            "item": {"type_name": "optional", "item": "string"},
                        },
                    }
                ]
            )

    def test_tagged_no_tag(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {
                            "type_name": "tagged",
                            "item": {"type_name": "optional", "item": "string"},
                        },
                    }
                ]
            )

    def test_aggregate_nested_no_type_v3(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse([{"name": "data", "type": "any", "aggregate": "nested_key(n)"}])

    def test_aggregate_nested_type_v3_not_any(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {"type_name": "optional", "item": "int64"},
                        "aggregate": "nested_key(n)",
                    }
                ]
            )

    def test_nested_aggregate_type_v3_bad_nested_aggregate(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "data",
                        "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                        "aggregate": "nested_value(n, min)",
                    }
                ]
            )

    def test_nested_aggregate_type_v3(self):
        schema = SchemaSpec.parse(
            [
                {
                    "name": "data1",
                    "type": "any",
                    "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                    "aggregate": "nested_key(n)",
                },
                {
                    "name": "data2",
                    "type": "any",
                    "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                    "aggregate": "nested_value(n)",
                },
                {
                    "name": "data3",
                    "type": "any",
                    "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                    "aggregate": "nested_value(n,sum)",
                },
                {
                    "name": "data4",
                    "type": "any",
                    "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                    "aggregate": "nested_value(n, max)",
                },
            ]
        )
        assert schema

    def test_dump(self):
        def _assert(type_v3_spec: str | dict[str, Any]):
            schema_spec = SchemaSpec.parse([{"name": "data", "type_v3": type_v3_spec}])
            parsed_type_v3_spec = TypeV3Spec(schema_spec[0])
            dumped_type_v3_spec = parsed_type_v3_spec.dump()
            assert dumped_type_v3_spec == type_v3_spec
            if isinstance(dumped_type_v3_spec, dict) and (members := dumped_type_v3_spec.get("members")):
                for member in members:
                    member_type = member["type"]
                    assert isinstance(member_type, (str, dict)) and not isinstance(member_type, TypeV3.Type)

        assert TypeV3Spec(SchemaSpec.parse([{"name": "data", "type": "string"}])[0]).dump() is None

        _assert("string")
        _assert("int64")
        _assert({"type_name": "optional", "item": "string"})
        _assert({"type_name": "optional", "item": "yson"})
        _assert(
            {
                "type_name": "optional",
                "item": {
                    "type_name": "list",
                    "item": {
                        "type_name": "struct",
                        "members": [
                            {"name": "m1", "type": "string"},
                            {"name": "m2", "type": {"type_name": "optional", "item": "yson"}},
                        ],
                    },
                },
            }
        )
        _assert(
            {
                "type_name": "variant",
                "members": [
                    {"name": "m1", "type": "string"},
                    {"name": "m2", "type": "void"},
                ],
            }
        )


class TestSchemaSpecification:
    @staticmethod
    def long_literal() -> str:
        return "".join([random.choice(string.ascii_letters) for _ in range(SchemaSpec.MAX_LITERAL_LEN + 1)])

    def test_parse_empty(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse([])

    @pytest.mark.parametrize("value", [1, 1.0, "1", True, {}])
    def test_parse_non_list(self, value: Any):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(value)

    def test_parse_only_key(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse([{"name": "key", "type": "uint64", "sort_order": "ascending"}])

    def test_parse_non_unique(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "key", "type": "string"},
                ]
            )

    def test_parse_empty_name(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "", "type": "string"},
                ]
            )

    def test_parse_empty_type(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value"},
                ]
            )

    def test_parse_too_long_name(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": self.long_literal(), "type": "string"},
                ]
            )

    def test_unknown_type(self):
        with pytest.raises(ValueError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "_unknown_"},
                ]
            )

    def test_unknown_type_v3(self):
        with pytest.raises(ValueError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type_v3": "_unknown_"},
                ]
            )

    def test_unknown_sort_order(self):
        with pytest.raises(ValueError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "_unknown_"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_required_any(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "any", "required": True},
                ]
            )

    def test_required_yson(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type_v3": "yson"},
                ]
            )

    def test_type_v1_vs_v3_required_mismatch(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "value", "type": "int64", "type_v3": "int64"},
                ]
            )

        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "value",
                        "type": "int64",
                        "required": True,
                        "type_v3": {"type_name": "optional", "item": "int64"},
                    },
                ]
            )

    @pytest.mark.parametrize("key_type", ["json"])
    def test_key_non_comparable(self, key_type: str):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": key_type, "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            )

    @pytest.mark.parametrize("key_type", ["json", "yson"])
    def test_key_non_comparable_type_v3(self, key_type: str):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type_v3": key_type, "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_key_not_schema_prefix(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "value", "type": "string"},
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                ]
            )

    def test_empty_expression(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": ""},
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_non_key_expression(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "hash", "type": "uint64", "expression": "farm_hash(key)"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_required_expression(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "hash",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key)",
                        "required": True,
                    },
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_required_expression_type_v3(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "hash",
                        "type_v3": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key)",
                    },
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_expression_bad_type(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "hash",
                        "type": "int64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key)",
                    },
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_expression_bad_type_v3(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "hash",
                        "type_v3": {"type_name": "optional", "item": "int64"},
                        "sort_order": "ascending",
                        "expression": "farm_hash(key)",
                    },
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_expression_explicit_int64_cast(self):
        schema = SchemaSpec.parse(
            [
                {
                    "name": "hash",
                    "type": "int64",
                    "sort_order": "ascending",
                    "expression": "int64(farm_hash(key))",
                },
                {"name": "key", "type": "uint64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]
        )
        assert schema[0].type == TypeV1.INT64

    def test_expression_explicit_int64_cast_type_v3(self):
        schema = SchemaSpec.parse(
            [
                {
                    "name": "hash",
                    "type_v3": {"type_name": "optional", "item": "int64"},
                    "sort_order": "ascending",
                    "expression": "int64(farm_hash(key))",
                },
                {"name": "key", "type": "uint64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]
        )
        type_v3_spec = TypeV3Spec(schema[0])
        type_v3_spec.ensure()
        assert type_v3_spec.type_v3 == TypeV3.Type.INT64
        assert type_v3_spec.type_v1 == TypeV1.INT64

    def test_too_many_computed(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {
                        "name": "hash",
                        "type": "int64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(value) % 10",
                    },
                    {
                        "name": "key",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(value) % 20",
                    },
                    {"name": "value", "type": "string"},
                ]
            )

    def test_lock_empty(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string", "lock": ""},
                ]
            )

    def test_lock_too_long(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string", "lock": self.long_literal()},
                ]
            )

    def test_lock_is_key(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending", "lock": "lock"},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_group_empty(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending", "group": ""},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_group_too_long(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending", "group": self.long_literal()},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_unknown_aggregate(self):
        with pytest.raises(ValueError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string", "aggregate": "_unknown_"},
                ]
            )

    def test_aggregate_key(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending", "aggregate": "first"},
                    {"name": "value", "type": "string"},
                ]
            )

    @pytest.mark.parametrize(
        "func,column_type",
        [("xdelta", "uint64"), ("sum", "string"), ("min", "string"), ("max", "string"), ("first", "string")],
    )
    def test_aggregate_bad_type(self, func: str, column_type: str):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": column_type, "aggregate": func},
                ]
            )

    @pytest.mark.parametrize(
        "func,column_type",
        [("xdelta", "uint64"), ("sum", "string"), ("min", "string"), ("max", "string"), ("first", "string")],
    )
    def test_aggregate_bad_type_v3(self, func: str, column_type: str):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type_v3": {"type_name": "optional", "item": column_type}, "aggregate": func},
                ]
            )

    def test_negative_max_inline_hunk_size(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string", "max_inline_hunk_size": -1},
                ]
            )

    def test_max_inline_hunk_size_for_key(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending", "max_inline_hunk_size": 1},
                    {"name": "value", "type": "string"},
                ]
            )

    def test_max_inline_hunk_size_for_aggregate(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "string", "max_inline_hunk_size": 1, "aggregate": "xdelta"},
                ]
            )

    def test_max_inline_hunk_size_for_non_bytes(self):
        with pytest.raises(AssertionError):
            SchemaSpec.parse(
                [
                    {"name": "key", "type": "uint64", "sort_order": "ascending"},
                    {"name": "value", "type": "uint64", "max_inline_hunk_size": 1},
                ]
            )

    def _assert_schema(self, schema: list[Column], has_type_v1: bool, has_type_v3: bool):
        assert len(schema) == 7
        m = {c.name: c for c in schema}

        def _assert_type(column: Column, type_v1: TypeV1, type_v3: TypeV3.Type, required: bool):
            if has_type_v1:
                assert column.type == type_v1
                assert bool(column.required) == required
            else:
                assert column.type is None
            if has_type_v3:
                assert column.type_v3
                spec = TypeV3Spec(column)
                assert spec.type_v1 == type_v1
                assert spec.type_v3 == type_v3
                assert spec.is_required == required
            else:
                assert column.type_v3 is None

        assert m["hash"].name == "hash"
        _assert_type(m["hash"], TypeV1.UINT64, TypeV3.Type.UINT64, False)
        assert m["hash"].sort_order == Column.SortOrder.ASCENDING
        assert m["hash"].expression == "farm_hash(key)"
        assert m["hash"].group == "k"
        assert m["hash"].lock is None
        assert m["hash"].aggregate is None
        assert m["hash"].max_inline_hunk_size is None

        assert m["key"].name == "key"
        _assert_type(m["key"], TypeV1.UINT32, TypeV3.Type.UINT32, False)
        assert m["key"].sort_order == Column.SortOrder.ASCENDING
        assert m["key"].expression is None
        assert m["key"].group == "k"
        assert m["key"].lock is None
        assert m["key"].aggregate is None
        assert m["key"].max_inline_hunk_size is None

        assert m["v1"].name == "v1"
        _assert_type(m["v1"], TypeV1.STRING, TypeV3.Type.STRING, True)
        assert m["v1"].sort_order is None
        assert m["v1"].expression is None
        assert m["v1"].group is None
        assert m["v1"].lock is None
        assert m["v1"].aggregate is None
        assert m["v1"].max_inline_hunk_size is None

        assert m["v2"].name == "v2"
        _assert_type(m["v2"], TypeV1.STRING, TypeV3.Type.STRING, False)
        assert m["v2"].sort_order is None
        assert m["v2"].expression is None
        assert m["v2"].group == "v"
        assert m["v2"].lock == "l1"
        assert m["v2"].aggregate is None
        assert m["v2"].max_inline_hunk_size is None

        assert m["v3"].name == "v3"
        _assert_type(m["v3"], TypeV1.STRING, TypeV3.Type.STRING, False)
        assert m["v3"].sort_order is None
        assert m["v3"].expression is None
        assert m["v3"].group == "v"
        assert m["v3"].lock == "l2"
        assert m["v3"].aggregate == Column.AggregateFunc.XDELTA
        assert m["v3"].max_inline_hunk_size is None

        assert m["v4"].name == "v4"
        _assert_type(m["v4"], TypeV1.INT64, TypeV3.Type.INT64, False)
        assert m["v4"].sort_order is None
        assert m["v4"].expression is None
        assert m["v4"].group == "v"
        assert m["v4"].lock == "l3"
        assert m["v4"].aggregate == Column.AggregateFunc.SUM
        assert m["v4"].max_inline_hunk_size is None

        assert m["v5"].name == "v5"
        _assert_type(m["v5"], TypeV1.ANY, TypeV3.Type.LIST, False)
        assert m["v5"].sort_order is None
        assert m["v5"].expression is None
        assert m["v5"].group == "v"
        assert m["v5"].lock == "l4"
        assert m["v5"].aggregate is None
        assert m["v5"].max_inline_hunk_size == 1

    def test_parse_schema(self):
        schema = SchemaSpec.parse(
            [
                {
                    "name": "hash",
                    "type": "uint64",
                    "sort_order": "ascending",
                    "expression": "farm_hash(key)",
                    "group": "k",
                },
                {"name": "key", "type": "uint32", "sort_order": "ascending", "group": "k"},
                {"name": "v1", "type": "string", "required": True},
                {"name": "v2", "type": "string", "group": "v", "lock": "l1", "required": False},
                {"name": "v3", "type": "string", "group": "v", "lock": "l2", "aggregate": "xdelta"},
                {"name": "v4", "type": "int64", "group": "v", "lock": "l3", "aggregate": "sum"},
                {"name": "v5", "type": "any", "group": "v", "lock": "l4", "max_inline_hunk_size": 1},
            ]
        )

        self._assert_schema(schema, has_type_v1=True, has_type_v3=False)

    def test_parse_schema_type_v3_only(self):
        schema = SchemaSpec.parse(
            [
                {
                    "name": "hash",
                    "type_v3": {"type_name": "optional", "item": "uint64"},
                    "sort_order": "ascending",
                    "expression": "farm_hash(key)",
                    "group": "k",
                },
                {
                    "name": "key",
                    "type_v3": {"type_name": "optional", "item": "uint32"},
                    "sort_order": "ascending",
                    "group": "k",
                },
                {"name": "v1", "type_v3": "string"},
                {"name": "v2", "type_v3": {"type_name": "optional", "item": "string"}, "group": "v", "lock": "l1"},
                {
                    "name": "v3",
                    "type_v3": {"type_name": "optional", "item": "string"},
                    "group": "v",
                    "lock": "l2",
                    "aggregate": "xdelta",
                },
                {
                    "name": "v4",
                    "type_v3": {"type_name": "optional", "item": "int64"},
                    "group": "v",
                    "lock": "l3",
                    "aggregate": "sum",
                },
                {
                    "name": "v5",
                    "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                    "group": "v",
                    "lock": "l4",
                    "max_inline_hunk_size": 1,
                },
            ]
        )

        self._assert_schema(schema, has_type_v1=False, has_type_v3=True)

    def test_parse_schema_type_v1_type_v3(self):
        schema = SchemaSpec.parse(
            [
                {
                    "name": "hash",
                    "type": "uint64",
                    "type_v3": {"type_name": "optional", "item": "uint64"},
                    "sort_order": "ascending",
                    "expression": "farm_hash(key)",
                    "group": "k",
                },
                {
                    "name": "key",
                    "type": "uint32",
                    "type_v3": {"type_name": "optional", "item": "uint32"},
                    "sort_order": "ascending",
                    "group": "k",
                },
                {"name": "v1", "type": "string", "type_v3": "string", "required": True},
                {
                    "name": "v2",
                    "type": "string",
                    "type_v3": {"type_name": "optional", "item": "string"},
                    "group": "v",
                    "lock": "l1",
                },
                {
                    "name": "v3",
                    "type": "string",
                    "type_v3": {"type_name": "optional", "item": "string"},
                    "group": "v",
                    "lock": "l2",
                    "aggregate": "xdelta",
                },
                {
                    "name": "v4",
                    "type": "int64",
                    "type_v3": {"type_name": "optional", "item": "int64"},
                    "group": "v",
                    "lock": "l3",
                    "aggregate": "sum",
                },
                {
                    "name": "v5",
                    "type": "any",
                    "type_v3": {"type_name": "optional", "item": {"type_name": "list", "item": "int64"}},
                    "group": "v",
                    "lock": "l4",
                    "max_inline_hunk_size": 1,
                },
            ]
        )

        self._assert_schema(schema, has_type_v1=True, has_type_v3=True)
