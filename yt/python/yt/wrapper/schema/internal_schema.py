from .types import (is_yt_dataclass, Annotation,
                    _is_py_type_compatible_with_ti_type, _check_ti_types_compatible,
                    _is_py_type_optional)
from . import types
from .helpers import check_schema_module_available

from ..errors import YtError

import copy

try:
    import yandex.type_info.typing as ti

    import dataclasses
except ImportError:
    pass


class _PySchemaSerializer:
    _TI_TYPE_FIELDS = ("_ti_type", "_annotation_ti_type")

    def __getstate__(self):
        d = copy.deepcopy(self.__dict__)
        for name in self._TI_TYPE_FIELDS:
            if name in d and d[name] is not None:
                d[name] = ti.serialize_yson(d[name])
        return d

    def __setstate__(self, d):
        d = copy.deepcopy(d)
        for name in self._TI_TYPE_FIELDS:
            if name in d and d[name] is not None:
                d[name] = ti.deserialize_yson(d[name])
        self.__dict__ = d


class PrimitiveSchema(_PySchemaSerializer):
    def __init__(self, py_type, ti_type, to_yt_type=None, from_yt_type=None, is_ti_type_optional=False, annotation_ti_type=None):
        self._py_type = py_type
        self._ti_type = ti_type
        self._wire_type = _ti_type_to_wire_type(ti_type)
        self._to_yt_type = to_yt_type
        self._from_yt_type = from_yt_type
        self._is_ti_type_optional = is_ti_type_optional
        self._annotation_ti_type = annotation_ti_type

    def _check_ti_types_compatible(self, for_reading, field_path):
        if self._annotation_ti_type is None:
            return
        if for_reading:
            src_type = self._ti_type
            dst_type = self._annotation_ti_type
        else:
            src_type = self._annotation_ti_type
            dst_type = self._ti_type
        _check_ti_types_compatible(src_type=src_type, dst_type=dst_type, field_path=field_path)


class OptionalSchema:
    def __init__(self, item, is_ti_type_optional=True):
        self._item = item
        self._is_ti_type_optional = is_ti_type_optional


class StructField:
    def __init__(self, name, schema, yt_name=None):
        self._name = name
        self._py_schema = schema
        self._yt_name = yt_name if yt_name is not None else name


class StructSchema(_PySchemaSerializer):
    def __init__(self, fields, py_type, is_ti_type_optional=False, other_columns_field=None):
        self._other_columns_field = other_columns_field
        self._fields = fields
        self._py_type = py_type
        self._is_ti_type_optional = is_ti_type_optional


class ListSchema:
    def __init__(self, item, is_ti_type_optional=False):
        self._item = item
        self._is_ti_type_optional = is_ti_type_optional


class RowSchema:
    # NB. This list is used to keep consistency of the order of system columns.
    _SYSTEM_COLUMNS = ("key_switch", "row_index", "range_index", "other_columns")

    def __init__(self, struct_schema, control_attributes=None):
        if control_attributes is None:
            control_attributes = {}
        self._struct_schema = struct_schema
        self._control_attributes = control_attributes
        self._is_ti_type_optional = False

    def get_columns_for_reading(self):
        if self._struct_schema._other_columns_field is not None:
            return None
        return [
            field._yt_name
            for field in self._struct_schema._fields
            if isinstance(field, StructField)
        ]


class FieldMissingFromRowClass(_PySchemaSerializer):
    def __init__(self, name, ti_type):
        self._name = name
        self._ti_type = ti_type


class FieldMissingFromSchema:
    def __init__(self, name, py_type):
        self._name = name
        self._py_type = py_type


def _get_origin(py_type):
    return getattr(py_type, "__origin__", None)


def _get_args(py_type):
    return getattr(py_type, "__args__", [])


def _get_primitive_type_origin_and_annotation(py_type):
    origin = None
    for type_ in (int, str, bytes, bool, float):
        if py_type is type_ or _get_origin(py_type) is type_:
            origin = type_
            break
    annotation = None
    for metadata in getattr(py_type, "__metadata__", []):
        if isinstance(metadata, Annotation):
            annotation = metadata
            break
    return origin, annotation


def _get_list_item_type(py_type):
    if _get_origin(py_type) is not list:
        return None
    assert len(_get_args(py_type)) == 1
    return _get_args(py_type)[0]


def _create_primitive_schema(py_type, ti_type=None, is_ti_type_optional=False):
    if not hasattr(_create_primitive_schema, "_default_ti_type"):
        _create_primitive_schema._default_ti_type = {
            int: ti.Int64,
            str: ti.Utf8,
            bytes: ti.String,
            float: ti.Double,
            bool: ti.Bool,
        }
    py_type_origin, annotation = _get_primitive_type_origin_and_annotation(py_type)
    assert py_type_origin is not None

    annotation_ti_type = None
    to_yt_type = None
    from_yt_type = None
    if annotation is not None:
        annotation_ti_type = annotation._ti_type
        to_yt_type = annotation._to_yt_type
        from_yt_type = annotation._from_yt_type

    if ti_type is None:
        if annotation_ti_type is not None:
            ti_type = annotation_ti_type
            assert _is_py_type_compatible_with_ti_type(py_type_origin, ti_type)
        else:
            ti_type = _create_primitive_schema._default_ti_type[py_type_origin]
    else:
        if not _is_py_type_compatible_with_ti_type(py_type_origin, ti_type):
            raise YtError('Python type {} is not compatible with type "{}" from table schema'
                          .format(py_type, ti_type))

    return PrimitiveSchema(
        py_type_origin,
        ti_type,
        to_yt_type=to_yt_type,
        from_yt_type=from_yt_type,
        is_ti_type_optional=is_ti_type_optional,
        annotation_ti_type=annotation_ti_type,
    )


def _is_nullable_ti_type(ti_type):
    return (
        ti_type == ti.Null or
        ti_type == ti.Void or
        ti_type.name == "Optional" or
        (ti_type.name == "Tagged" and _is_nullable_ti_type(ti_type.item))
    )


def _validate_py_schema(py_schema, for_reading):
    check_schema_module_available()
    assert isinstance(py_schema, RowSchema)
    struct_schema = py_schema._struct_schema
    field_path = struct_schema._py_type.__qualname__
    _validate_py_schema_impl(struct_schema, for_reading=for_reading, field_path=field_path)


def _validate_py_schema_impl(py_schema, for_reading, field_path):
    def validate_not_optional():
        if for_reading and py_schema._is_ti_type_optional:
            assert field_path is not None
            raise YtError("Schema and yt_dataclass mismatch: field \"{}\" is non-nullable in yt_dataclass and "
                          "optional in table schema"
                          .format(field_path))
    
    if isinstance(py_schema, StructSchema):
        validate_not_optional()
        for field in py_schema._fields:
            subfield_path = "{}.{}".format(field_path, field._name)
            if isinstance(field, FieldMissingFromRowClass):
                if (
                    not for_reading and
                    not _is_nullable_ti_type(field._ti_type) and
                    py_schema._other_columns_field is None
                ):
                    raise YtError(
                        "Schema and yt_dataclass mismatch: yt_dataclass is missing non-nullable field \"{}\""
                        .format(subfield_path),
                        attributes={
                            "type_in_schema": ti.serialize_yson(field._ti_type),
                        },
                    )
            elif isinstance(field, FieldMissingFromSchema):
                if for_reading and not _is_py_type_optional(field._py_type):
                    raise YtError(
                        "Schema and yt_dataclass mismatch: struct schema is missing non-nullable field \"{}\""
                        .format(subfield_path),
                        attributes={"type": field._py_type},
                    )
                if not for_reading:
                    raise YtError(
                        "Schema and yt_dataclass mismatch: struct schema is missing field \"{}\""
                        .format(subfield_path),
                        attributes={"type": field._py_type},
                    )
            elif isinstance(field, StructField):
                _validate_py_schema_impl(field._py_schema, for_reading=for_reading, field_path=subfield_path)
            else:
                assert False
    elif isinstance(py_schema, OptionalSchema):
        if not for_reading and not py_schema._is_ti_type_optional:
            raise YtError("Schema and yt_dataclass mismatch: field \"{}\" is optional in yt_dataclass and "
                          "required in table schema"
                          .format(field_path))
        _validate_py_schema_impl(py_schema._item, for_reading=for_reading, field_path=field_path + ".<optional-element>")
    elif isinstance(py_schema, PrimitiveSchema):
        validate_not_optional()
        py_schema._check_ti_types_compatible(for_reading=for_reading, field_path=field_path)
    elif isinstance(py_schema, ListSchema):
        validate_not_optional()
        _validate_py_schema(py_schema._item, for_reading=for_reading, field_path=field_path + ".<list-element>")
    else:
        assert False


def _create_struct_schema(py_type, yt_fields=None, is_ti_type_optional=False, allow_other_columns=False):
    assert is_yt_dataclass(py_type)
    if yt_fields is None:
        py_schema_fields = []
        other_columns_field = None
        for field in dataclasses.fields(py_type):
            if field.type == types.OtherColumns:
                other_columns_field = FieldMissingFromSchema(field.name, field.type)
            else:
                py_schema_fields.append(StructField(field.name, _create_py_schema(field.type)))
        return StructSchema(py_schema_fields, py_type, other_columns_field=other_columns_field)
    yt_fields = list(yt_fields)
    py_schema_fields = []
    name_to_field = {}
    for field in dataclasses.fields(py_type):
        # TODO: Switch to yt_name.
        name_to_field[field.name] = field
    py_schema_fields = []
    for yt_name, ti_type in yt_fields:
        field = name_to_field.get(yt_name)
        if field is None:
            py_schema_fields.append(FieldMissingFromRowClass(yt_name, ti_type))
        else:
            struct_field = StructField(field.name, _create_py_schema(field.type, ti_type))
            py_schema_fields.append(struct_field)
            del name_to_field[yt_name]
    other_columns_field = None
    for field in name_to_field.values():
        struct_field = FieldMissingFromSchema(field.name, field.type)
        if field.type == types.OtherColumns:
            other_columns_field = struct_field
        else:
            py_schema_fields.append(struct_field)
    if other_columns_field is not None and not allow_other_columns:
        raise YtError(
            "Field of type OtherColumns is allowed only on top level",
            attributes={"field_name": field._name},
        )
    return StructSchema(
        py_schema_fields,
        py_type,
        is_ti_type_optional=is_ti_type_optional,
        other_columns_field=other_columns_field,
    )


def _create_py_schema(py_type, ti_type=None):
    is_ti_type_optional = False
    primitive_origin, annotation = _get_primitive_type_origin_and_annotation(py_type)
    if ti_type is not None and ti_type.name == "Optional":
        is_ti_type_optional = True
        ti_type = ti_type.item
    if is_yt_dataclass(py_type):
        yt_fields = None
        if ti_type is not None:
            if ti_type.name != "Struct":
                raise YtError(
                    "Schema and row class mismatch: "
                    "expected \"Struct\" type, found \"{}\""
                    .format(ti_type)
                )
            yt_fields = ti_type.items
        return _create_struct_schema(py_type, yt_fields, is_ti_type_optional=is_ti_type_optional)
    elif _is_py_type_optional(py_type):
        if ti_type is None:
            is_ti_type_optional = True
        return OptionalSchema(
            _create_py_schema(py_type.__args__[0], ti_type),
            is_ti_type_optional=is_ti_type_optional,
        )
    elif _get_list_item_type(py_type) is not None:
        if ti_type is None:
            item_ti_type = None
        else:
            if ti_type.name != "List":
                raise YtError(
                    "Schema and row class mismatch: "
                    "expected \"List\" type, found \"{}\""
                    .format(ti_type)
                )
            item_ti_type = ti_type.item
        item_py_schema = _create_py_schema(_get_list_item_type(py_type), ti_type=item_ti_type)
        return ListSchema(item_py_schema, is_ti_type_optional=is_ti_type_optional)
    elif primitive_origin is not None:
        return _create_primitive_schema(py_type, ti_type, is_ti_type_optional=is_ti_type_optional)
    else:
        raise YtError("Cannot create py_schema from type {}".format(py_type))


def _create_row_py_schema(py_type, control_attributes=None):
    assert is_yt_dataclass(py_type)
    return RowSchema(
        _create_struct_schema(py_type, allow_other_columns=True),
        control_attributes=control_attributes,
    )


def _ti_type_to_wire_type(ti_type):
    if not hasattr(_ti_type_to_wire_type, "_dict"):
        _ti_type_to_wire_type._dict = {
            # TODO(levysotsky): use small ints when they are supported in skiff.
            ti.Int8: "int64",
            ti.Int16: "int64",
            ti.Int32: "int64",
            ti.Int64: "int64",

            ti.Uint8: "uint64",
            ti.Uint16: "uint64",
            ti.Uint32: "uint64",
            ti.Uint64: "uint64",

            ti.String: "string32",
            ti.Utf8: "string32",

            ti.Float: "double",
            ti.Double: "double",

            ti.Bool: "boolean",
        }
    assert ti.is_valid_type(ti_type)
    return _ti_type_to_wire_type._dict[ti_type]


def _create_optional_skiff_schema(item, name=None):
    schema = {
        "wire_type": "variant8",
        "children": [
            {"wire_type": "nothing"},
            item,
        ],
    }
    if name is not None:
        schema["name"] = name
    return schema


def _row_py_schema_to_skiff_schema(py_schema, for_reading):
    assert isinstance(py_schema, RowSchema)
    skiff_schema = _py_schema_to_skiff_schema(py_schema._struct_schema)
    system_column_iter = iter(RowSchema._SYSTEM_COLUMNS)
    assert next(system_column_iter) == "key_switch"
    if for_reading and py_schema._control_attributes.get("enable_key_switch", False):
        skiff_schema["children"].append({"name": "$key_switch", "wire_type": "boolean"})
    assert next(system_column_iter) == "row_index"
    if for_reading and py_schema._control_attributes.get("enable_row_index", False):
        row_index_schema = _create_optional_skiff_schema({"wire_type": "int64"}, name="$row_index")
        skiff_schema["children"].append(row_index_schema)
    assert next(system_column_iter) == "range_index"
    if for_reading and py_schema._control_attributes.get("enable_range_index", False):
        range_index_schema = _create_optional_skiff_schema({"wire_type": "int64"}, name="$range_index")
        skiff_schema["children"].append(range_index_schema)
    assert next(system_column_iter) == "other_columns"
    if py_schema._struct_schema._other_columns_field is not None:
        skiff_schema["children"].append({"name": "$other_columns", "wire_type": "yson32"})
    return skiff_schema


def _py_schema_to_skiff_schema(py_schema, name=None):
    check_schema_module_available()
    if isinstance(py_schema, StructSchema):
        children = []
        for field in py_schema._fields:
            if not isinstance(field, StructField):
                continue
            child_skiff_schema = _py_schema_to_skiff_schema(field._py_schema, name=field._yt_name)
            if child_skiff_schema is not None:
                children.append(child_skiff_schema)
        skiff_schema = {
            "children": children,
            "wire_type": "tuple",
        }
    elif isinstance(py_schema, OptionalSchema):
        skiff_schema = _py_schema_to_skiff_schema(py_schema._item)
    elif isinstance(py_schema, PrimitiveSchema):
        skiff_schema = {"wire_type": py_schema._wire_type}
    elif isinstance(py_schema, ListSchema):
        skiff_schema = {
            "children": [
                _py_schema_to_skiff_schema(py_schema._item),
            ],
            "wire_type": "repeated_variant8",
        }
    else:
        assert False, "Unexpected py_schema type: {}".format(type(py_schema))
    if py_schema._is_ti_type_optional:
        skiff_schema = _create_optional_skiff_schema(skiff_schema)
    if name is not None:
        skiff_schema["name"] = name
    return skiff_schema


def _py_schema_to_ti_type(py_schema):
    if isinstance(py_schema, RowSchema):
        ti_type = _py_schema_to_ti_type(py_schema._struct_schema)
    elif isinstance(py_schema, StructSchema):
        ti_items = []
        for field in py_schema._fields:
            assert isinstance(field, StructField)
            ti_items.append(slice(field._yt_name, _py_schema_to_ti_type(field._py_schema)))
        ti_type = ti.Struct.__getitem__(tuple(ti_items))
    elif isinstance(py_schema, OptionalSchema):
        ti_type = ti.Optional[_py_schema_to_ti_type(py_schema._item)]
    elif isinstance(py_schema, PrimitiveSchema):
        ti_type = py_schema._ti_type
    else:
        assert False
    if py_schema._is_ti_type_optional and not isinstance(py_schema, OptionalSchema):
        ti_type = ti.Optional[ti_type]
    return ti_type
