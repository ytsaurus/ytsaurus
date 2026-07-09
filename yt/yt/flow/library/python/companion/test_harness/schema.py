"""Helpers for defining TableSchema inline in tests."""

from ..row import ColumnSchema, TableSchema, YT_TYPE_NAME_TO_TI


def schema(**columns):
    """Create a TableSchema from keyword arguments.

    Example::

        schema(word="string", count="int64")
    """
    cols = []
    for name, type_name in columns.items():
        ti_type = YT_TYPE_NAME_TO_TI.get(type_name.lower())
        if ti_type is None:
            raise ValueError(
                f"Unknown type {type_name!r} for column {name!r}. "
                f"Supported: {', '.join(sorted(YT_TYPE_NAME_TO_TI))}"
            )
        cols.append(ColumnSchema(name=name, type=ti_type))
    return TableSchema(cols)
