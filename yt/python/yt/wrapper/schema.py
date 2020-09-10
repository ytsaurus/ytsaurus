import yt.yson
from yt.wrapper import YtError

try:
    import yandex.type_info # noqa
except ImportError:
    raise YtError("yandex-type-info package is required to work with schemas. "
                  "It is shipped as additional package and "
                  'can be installed as pip package "yandex-type-info"')

from yandex.type_info import typing


class ColumnSchema(object):
    """ Class representing table column schema.

    See https://yt.yandex-team.ru/docs/description/storage/static_schema.html#schema_overview
    """

    def __init__(self, name, type, sort_order=None, group=None):
        self.name = name
        self.type = type
        self.sort_order = sort_order
        self.group = group

    def to_yson_type(self):
        result = {
            "name": self.name,
            "type_v3": yt.yson.loads(typing.serialize_yson(self.type)),
        }
        if self.sort_order is not None:
            result["sort_order"] = self.sort_order
        if self.group is not None:
            result["group"] = self.group
        return result

    @classmethod
    def from_yson_type(cls, obj):
        type = typing.deserialize_yson(yt.yson.dumps(obj["type_v3"]))
        return ColumnSchema(obj["name"], type, sort_order=obj.get("sort_order"), group=obj.get("group"))

    def __eq__(self, other):
        if not isinstance(other, ColumnSchema):
            return False
        return (self.name, self.type, self.sort_order, self.group) == \
            (other.name, other.type, other.sort_order, other.group)

    def __ne__(self, other):
        return not (self == other)

    def __str__(self):
        return str(self.to_yson_type())


class TableSchema(object):
    """ Class representing table schema.

    It can be built using the constructor or fluently using add_column method:

        TableSchema() \
            .add_column("key", typing.String, sort_order="ascending") \
            .add_column("value", typing.List[typing.Int32])

    See https://yt.yandex-team.ru/docs/description/storage/static_schema.html#schema_overview
    """

    def __init__(self, columns=[], strict=True, unique_keys=False):
        self.columns = columns[:]
        self.strict = strict
        self.unique_keys = unique_keys

    def add_column(self, *args, **kwargs):
        """ Add column.

        Call as either .add_column(ColumnSchema(...)) or .add_column(name, type, ...).
        """
        if len(args) == 1:
            assert isinstance(args[0], ColumnSchema)
            self.columns.append(args[0])
        else:
            self.columns.append(ColumnSchema(*args, **kwargs))
        return self

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
        return TableSchema(columns, **kwargs)

    def __eq__(self, other):
        if not isinstance(other, TableSchema):
            return False
        return (self.columns, self.strict, self.unique_keys) == \
            (other.columns, other.strict, other.unique_keys)

    def __ne__(self, other):
        return not (self == other)

    def __str__(self):
        return str(self.to_yson_type())
