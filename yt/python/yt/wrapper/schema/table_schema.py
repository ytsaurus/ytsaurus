from .helpers import check_schema_module_available, is_schema_module_available
from .types import is_yt_dataclass
from .internal_schema import _py_schema_to_ti_type, _create_py_schema

from ..errors import YtError

import copy
import collections
import typing

import yt.type_info as ti
import yt.yson


class SortColumn:
    ASCENDING = "ascending"
    DESCENDING = "descending"

    def __init__(self, name, sort_order=ASCENDING):
        self.name = name
        self.sort_order = sort_order

    def to_yson_type(self):
        return {
            "name": self.name,
            "sort_order": self.sort_order,
        }


class ColumnSchema:
    """Class representing column schema.

    :param name:
        name of the column
    :param type:
        type of the column, could be either a type_info type or a class object annotated with @yt.wrapper.yt_dataclass
    :param sort_order:
        column sort order, if argument is missing column is not sorted.
    :param group:
        column group name, columns with same group name are stored in the same block,
        used for tuning read write performance
    :param aggregate:
        aggregating function column name, for nonkey columns of dynamic tables
    :param expression:
        expression for computed column

    See https://ytsaurus.tech/docs/en/user-guide/storage/static-schema
    """

    def __init__(
        self,
        name: str,
        type,
        sort_order=None,
        group: typing.Optional[str] = None,
        aggregate: typing.Optional[str] = None,
        expression: typing.Optional[str] = None,
    ):

        self.name = name
        if ti.is_valid_type(type):
            self.type = type
        elif is_schema_module_available() and is_yt_dataclass(type):
            self.type = _py_schema_to_ti_type(_create_py_schema(type))
        else:
            raise TypeError("Expected type_info type or class marked with @yt.wrapper.schema.yt_dataclass, "
                            "got <{}>{!r}".format(type(type), type))

        self.sort_order = sort_order
        self.group = group
        self.expression = expression
        self.aggregate = aggregate

    def to_yson_type(self):
        result = {
            "name": self.name,
            "type_v3": yt.yson.loads(ti.serialize_yson(self.type)),
        }

        for k, v in [
            ("sort_order", self.sort_order),
            ("group", self.group),
            ("aggregate", self.aggregate),
            ("expression", self.expression),
        ]:
            if v is not None:
                result[k] = v
        return result

    @classmethod
    def from_yson_type(cls, obj):
        if "type_v3" in obj:
            type = ti.deserialize_yson(yt.yson.dumps(obj["type_v3"]))
        else:
            required = obj.get("required", False)
            type = ti.deserialize_yson_v1(yt.yson.dumps(obj["type"]), required)

        return ColumnSchema(
            obj["name"],
            type,
            sort_order=obj.get("sort_order"),
            group=obj.get("group"),
            aggregate=obj.get("aggregate"),
            expression=obj.get("expression"),
        )

    def __eq__(self, other):
        if not isinstance(other, ColumnSchema):
            return False

        def to_tuple(schema):
            return (
                schema.name,
                schema.type,
                schema.sort_order,
                schema.group,
                schema.expression,
                schema.aggregate,
            )

        return to_tuple(self) == to_tuple(other)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "ColumnSchema({})".format(self.to_yson_type())

    def __getstate__(self):
        return self.to_yson_type()

    def __setstate__(self, d):
        self.__dict__ = ColumnSchema.from_yson_type(d).__dict__


class TableSchema:
    """Class representing table schema.

    It can be built using the constructor or fluently using add_column method:

        TableSchema() \
            .add_column("key", ti.String, sort_order="ascending") \
            .add_column("value", ti.List[ti.Int32])

    See https://ytsaurus.tech/docs/en/user-guide/storage/static-schema
    """

    def __init__(self, columns=None, strict=None, unique_keys=None):
        if columns is None:
            self.columns = []
        else:
            self.columns = columns[:]

        if strict is None:
            strict = True
        self.strict = strict

        if unique_keys is None:
            unique_keys = False
        self.unique_keys = unique_keys

    @classmethod
    def from_row_type(cls, row_type, strict=None, unique_keys=False):
        """Infer schema from yt_dataclass.

        :param strict:
            Whether the inferred schema is strict.
            If strict is None (default), the strictness is inferred
            from presence of OtherColumns field.
        :param unique_keys: Whether the inferred has unique_keys.
        """

        check_schema_module_available()
        if not is_yt_dataclass(row_type):
            raise TypeError("Expected class marked with @yt.wrapper.schema.yt_dataclass, got {}"
                            .format(row_type.__qualname__))
        py_schema = _create_py_schema(row_type)
        has_other_columns = (py_schema._other_columns_field is not None)
        if strict is None:
            strict = not has_other_columns
        if strict and has_other_columns:
            raise YtError('Cannot infer strict schema from yt_dataclass "{}" with field marked with "OtherColumns"'
                          .format(row_type.__qualname__))
        columns = [
            ColumnSchema(column._yt_name, _py_schema_to_ti_type(column._py_schema))
            for column in py_schema._fields
        ]
        return cls(columns, strict=strict, unique_keys=unique_keys)

    def add_column(self, *args, **kwargs):
        """Add column.

        Call as either .add_column(ColumnSchema(...)) or .add_column(name, type, ...).
        """
        if len(args) == 1:
            if not isinstance(args[0], ColumnSchema):
                raise TypeError("If add_column() is called with single argument, it must be of type ColumnSchema")
            self.columns.append(args[0])
        else:
            self.columns.append(ColumnSchema(*args, **kwargs))
        return self

    def build_schema_sorted_by(self, sort_columns):
        if isinstance(sort_columns, str) or isinstance(sort_columns, SortColumn):
            sort_columns = [sort_columns]

        sort_columns = self._to_sort_columns(sort_columns)

        column_name_to_column = collections.OrderedDict(
            (column.name, copy.deepcopy(column))
            for column in self.columns
        )
        new_columns = []
        for sort_column in sort_columns:
            column = column_name_to_column.get(sort_column.name)
            if column is None:
                raise ValueError("Column \"{}\" is not found".format(sort_column.name))
            column.sort_order = sort_column.sort_order
            new_columns.append(column)
            del column_name_to_column[sort_column.name]
        for column in column_name_to_column.values():
            column.sort_order = None
            new_columns.append(column)

        old_key_columns = set(
            column.name
            for column in self.columns
            if column.sort_order is not None
        )
        new_key_columns = set(sort_column.name for sort_column in sort_columns)
        new_unique_keys = self.unique_keys and old_key_columns.issubset(new_key_columns)

        return TableSchema(
            columns=new_columns,
            strict=self.strict,
            unique_keys=new_unique_keys,
        )

    def to_yson_type(self):
        columns = yt.yson.to_yson_type([c.to_yson_type() for c in self.columns])
        columns.attributes["strict"] = self.strict
        columns.attributes["unique_keys"] = self.unique_keys
        return columns

    @classmethod
    def from_yson_type(cls, obj):
        columns = [ColumnSchema.from_yson_type(c) for c in obj]
        attrs = obj.attributes
        kwargs = {}
        if "strict" in attrs:
            kwargs["strict"] = attrs["strict"]
        if "unique_keys" in attrs:
            kwargs["unique_keys"] = attrs["unique_keys"]
        return cls(columns, **kwargs)

    def is_empty_nonstrict(self):
        return not self.strict and len(self.columns) == 0

    def __eq__(self, other):
        if not isinstance(other, TableSchema):
            return False
        return (self.columns, self.strict, self.unique_keys) == \
            (other.columns, other.strict, other.unique_keys)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "TableSchema({})".format(self.to_yson_type())

    @staticmethod
    def _to_sort_columns(sort_columns):
        actual_sort_columns = []
        for sort_column in sort_columns:
            if isinstance(sort_column, SortColumn):
                actual_sort_columns.append(sort_column)
            elif isinstance(sort_column, str):
                actual_sort_columns.append(SortColumn(sort_column))
            else:
                raise TypeError(
                    "Expected sort_columns to be iterable "
                    "over strings or SortColumn instances"
                )
        return actual_sort_columns
