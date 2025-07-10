import yt.type_info as ti  # noqa
from .types import yt_dataclass, is_yt_dataclass, create_annotated_type, OutputRow, create_yt_enum  # noqa
from .table_schema import ColumnSchema, TableSchema, SortColumn  # noqa

from .internal_schema import _row_py_schema_to_skiff_schema, _validate_py_schema  # noqa
from . import internal_schema

from .helpers import check_schema_module_available, is_schema_module_available

from ..errors import YtError

import re
import typing

if is_schema_module_available():
    from .types import (  # noqa
        Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
        Float, Double,
        Date, Datetime, Timestamp, Interval,
        YsonBytes, OtherColumns, FormattedPyDatetime,
        _PY_TYPE_BY_TI_TYPE,
        TzDate, TzDatetime, TzTimestamp,
        TzDate32, TzDatetime64, TzTimestamp64)

    from .types import RowIteratorProtocol as RowIterator, ContextProtocol as Context  # noqa
    from .variant import Variant  # noqa


class SkiffError(YtError):
    pass


class _SchemaRuntimeCtx:
    def __init__(self):
        self._for_reading = None
        self._validate_optional_on_runtime = None

    def set_validation_mode_from_config(self, config):
        # type: (Config) -> _SchemaRuntimeCtx
        self._validate_optional_on_runtime = bool(config.get("runtime_type_validation"))
        return self

    def set_for_reading_only(self, for_reading=True):
        # type: (bool | True) -> _SchemaRuntimeCtx
        self._for_reading = for_reading
        return self

    def create_row_py_schema(self, py_type, schema=None, control_attributes=None, column_renaming=None):
        return _create_row_py_schema(
            py_type,
            schema=schema,
            control_attributes=control_attributes,
            column_renaming=column_renaming,
            schema_runtime_context=self,
        )


def _create_row_py_schema(
    py_type,                     # type: object
    schema=None,                 # type: TableSchema | None
    control_attributes=None,     # type: dict[str, str] | None
    column_renaming=None,        # type: dict[str, str] | None
    schema_runtime_context=None  # type: _SchemaRuntimeCtx | None
):
    check_schema_module_available()
    if not schema_runtime_context:
        schema_runtime_context = _SchemaRuntimeCtx()
    if not is_yt_dataclass(py_type):
        raise TypeError("Expected type marked with @yt.wrapper.schema.yt_dataclass, got {}".format(py_type))
    yt_fields = None
    if column_renaming is None:
        column_renaming = {}
    if schema is not None:
        assert isinstance(schema, TableSchema)
        if not schema.is_empty_nonstrict():
            yt_fields = []
            for column in schema.columns:
                name = column_renaming.get(column.name, column.name)
                yt_fields.append((name, column.type))
    struct_schema = internal_schema._create_struct_schema(py_type, yt_fields, allow_other_columns=True, schema_runtime_context=schema_runtime_context)
    return internal_schema.RowSchema(struct_schema, control_attributes=control_attributes, schema_runtime_context=schema_runtime_context)


def make_dataclass_from_table_schema(table_type: TableSchema, type_name: typing.Optional[str] = None) -> typing.Type:
    """Create yt_dataclass class from TableSchema object on the fly.
    :param TableSchema table_type: YT table schema.
    :param str type_name: name of new class (usefull for debug), optional.
    :return: yt_dataclass class to create records
    """
    if isinstance(table_type, TableSchema):
        if not type_name:
            type_name = f"YtGeneratedDataclass_{id(table_type)}"
        obj = {
            "__annotations__": {},
        }
        for column in table_type.columns:
            name = column.name
            py_type = _make_py_type_from_ti_type(column, type_name)
            if not re.match(r"^[a-z_][a-z0-9_]*$", name, re.IGNORECASE):
                raise ValueError(f"Wrong table column name: \"{name}\"")
            obj[name] = None
            obj["__annotations__"][name] = py_type
        return yt_dataclass(type(type_name, (object, ), obj))
    else:
        raise RuntimeError(f"Unknown table type \"{type(table_type)}\"")


def _make_py_type_from_ti_type(table_type: typing.Union[ColumnSchema, ti.type_base.Primitive, ti.Type], type_name: typing.Optional[str] = None) -> typing.Type:
    if isinstance(table_type, ColumnSchema):
        return _make_py_type_from_ti_type(table_type.type, type_name)
    elif isinstance(table_type, ti.type_base.Primitive):
        if table_type in _PY_TYPE_BY_TI_TYPE:
            return _PY_TYPE_BY_TI_TYPE[table_type]
        else:
            raise ValueError(f"Unknown type \"{table_type}\"")
    elif isinstance(table_type, ti.Type) and table_type.name == "Optional":
        return typing.Optional[_make_py_type_from_ti_type(table_type.item, type_name)]
    elif isinstance(table_type, ti.Type) and table_type.name == "List":
        return typing.List[_make_py_type_from_ti_type(table_type.item, type_name)]
    elif isinstance(table_type, ti.Type) and table_type.name == "Tuple":
        py_type = typing.Tuple
        py_type.__args__ = tuple([_make_py_type_from_ti_type(item, type_name) for item in table_type.items])
        return py_type
    elif isinstance(table_type, ti.Type) and table_type.name == "Dict":
        return typing.Dict[_make_py_type_from_ti_type(table_type.key, type_name), _make_py_type_from_ti_type(table_type.value, type_name)]
    elif isinstance(table_type, ti.Type) and table_type.name == "Struct":
        obj = {
            "__annotations__": {},
        }
        for name, yt_type in table_type.items:
            obj[name] = None
            obj["__annotations__"][name] = _make_py_type_from_ti_type(yt_type, type_name)
        return yt_dataclass(type(f"{type_name}_{table_type.name.upper()}", (object, ), obj))
    else:
        raise RuntimeError(f"Unknown field type {table_type}")
