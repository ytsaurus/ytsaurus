from .types import yt_dataclass, is_yt_dataclass, create_annotated_type, OutputRow
from .table_schema import ColumnSchema, TableSchema

from .internal_schema import _row_py_schema_to_skiff_schema, _validate_py_schema
from . import internal_schema
from .helpers import check_schema_module_available, is_schema_module_available

from ..errors import YtError


class SkiffError(YtError):
    pass


if is_schema_module_available():
    from .types import (Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
                        OtherColumns)


def _create_row_py_schema(py_type, schema=None, control_attributes=None, column_renaming=None):
    check_schema_module_available()
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
    struct_schema = internal_schema._create_struct_schema(py_type, yt_fields, allow_other_columns=True)
    return internal_schema.RowSchema(struct_schema, control_attributes=control_attributes)
