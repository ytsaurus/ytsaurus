import yt.type_info as ti  # noqa
from .types import yt_dataclass, is_yt_dataclass, create_annotated_type, OutputRow, create_yt_enum  # noqa
from .table_schema import ColumnSchema, TableSchema, SortColumn  # noqa

from .internal_schema import _row_py_schema_to_skiff_schema, _validate_py_schema  # noqa
from . import internal_schema

from .helpers import check_schema_module_available, is_schema_module_available

from ..errors import YtError

if is_schema_module_available():
    from .types import (  # noqa
        Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
        Float, Double,
        Date, Datetime, Timestamp, Interval,
        YsonBytes, OtherColumns, FormattedPyDatetime)

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
