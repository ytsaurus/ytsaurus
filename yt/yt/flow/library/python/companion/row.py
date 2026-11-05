"""Row data classes: Message, ExtendedMessage, Timer, NewTimer, Payload, TableSchema."""

from dataclasses import dataclass
from typing import Any, Optional

from yt.type_info.typing import PRIMITIVES_V1, PRIMITIVES_V3

from yt.wrapper.schema.table_schema import ColumnSchema  # noqa: F401 — re-exported
from yt.wrapper.schema.table_schema import TableSchema as _YTTableSchema

from .wire_protocol import ColumnValueType, UnversionedRow, UnversionedValue


# Maps YT type name strings to type_info types.
# V3 names first (e.g. "bool", "yson"), then V1 aliases on top (e.g. "boolean", "any").
YT_TYPE_NAME_TO_TI = {**PRIMITIVES_V3, **PRIMITIVES_V1}

# Maps type_info primitive type names to wire ColumnValueType.
_TI_NAME_TO_CVT = {
    'Int8': ColumnValueType.INT64,
    'Int16': ColumnValueType.INT64,
    'Int32': ColumnValueType.INT64,
    'Int64': ColumnValueType.INT64,
    'Uint8': ColumnValueType.UINT64,
    'Uint16': ColumnValueType.UINT64,
    'Uint32': ColumnValueType.UINT64,
    'Uint64': ColumnValueType.UINT64,
    'Float': ColumnValueType.DOUBLE,
    'Double': ColumnValueType.DOUBLE,
    'Bool': ColumnValueType.BOOLEAN,
    'String': ColumnValueType.STRING,
    'Utf8': ColumnValueType.STRING,
    'Json': ColumnValueType.STRING,
    'Uuid': ColumnValueType.STRING,
    'Yson': ColumnValueType.ANY,
    'Null': ColumnValueType.NULL,
}


def ti_to_cvt(ti_type) -> ColumnValueType:
    """Map a type_info type to its wire ColumnValueType.

    Optional types are unwrapped. Unknown/complex types map to ANY.
    """
    name = getattr(ti_type, 'name', None)
    if name == 'Optional':
        return ti_to_cvt(ti_type.item)
    return _TI_NAME_TO_CVT.get(name, ColumnValueType.ANY)


class TableSchema(_YTTableSchema):
    """YT TableSchema extended with positional column lookup for UnversionedRow access."""

    def __init__(self, columns=None, strict=None, unique_keys=None):
        super().__init__(columns=columns, strict=strict, unique_keys=unique_keys)
        self._rebuild_index()

    def _rebuild_index(self):
        self._name_to_id = {col.name: i for i, col in enumerate(self.columns)}

    def add_column(self, *args, **kwargs):
        result = super().add_column(*args, **kwargs)
        self._rebuild_index()
        return result

    def find_column(self, name: str) -> Optional[int]:
        return self._name_to_id.get(name)

    def get_column_name(self, column_id: int) -> str:
        return self.columns[column_id].name

    def get_column_type(self, column_id: int) -> ColumnValueType:
        return ti_to_cvt(self.columns[column_id].type)

    def __len__(self):
        return len(self.columns)


EMPTY_SCHEMA = TableSchema()
EMPTY_ROW = UnversionedRow(values=[])


class Payload:
    """Wraps UnversionedRow + TableSchema for named access to values."""

    EMPTY: "Payload"

    def __init__(self, row: Optional[UnversionedRow] = None, schema: Optional[TableSchema] = None):
        self.row = row or EMPTY_ROW
        self.schema = schema or EMPTY_SCHEMA

    def get(self, column_name: str, value_type: type = None) -> Any:
        """Get value by column name."""
        column_id = self.schema.find_column(column_name)
        if column_id is None:
            return None
        return self.get_by_id(column_id, value_type)

    def get_by_id(self, column_id: int, value_type: type = None) -> Any:
        """Get value by column id."""
        if column_id >= len(self.row.values):
            return None
        uv = self.row.values[column_id]
        if uv.type == ColumnValueType.NULL:
            return None
        value = uv.value
        if value_type == str and isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def __getitem__(self, column_name: str) -> Any:
        """Dict-like access: payload["word"]. Auto-decodes bytes to str for string columns."""
        column_id = self.schema.find_column(column_name)
        if column_id is None:
            raise KeyError(column_name)
        if column_id >= len(self.row.values):
            raise KeyError(column_name)
        uv = self.row.values[column_id]
        if uv.type == ColumnValueType.NULL:
            raise KeyError(column_name)
        value = uv.value
        if uv.type.is_string_like_type() and isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def __contains__(self, column_name: str) -> bool:
        """Check if column exists and has a non-null value."""
        column_id = self.schema.find_column(column_name)
        if column_id is None:
            return False
        if column_id >= len(self.row.values):
            return False
        return self.row.values[column_id].type != ColumnValueType.NULL

    def keys(self) -> list:
        """Return column names that have non-null values."""
        result = []
        for i, val in enumerate(self.row.values):
            if val.type != ColumnValueType.NULL and i < len(self.schema):
                result.append(self.schema.get_column_name(i))
        return result

    def to_dict(self) -> dict:
        """Convert to a plain dict. Auto-decodes bytes to str for string columns."""
        result = {}
        for i, val in enumerate(self.row.values):
            if val.type != ColumnValueType.NULL and i < len(self.schema):
                name = self.schema.get_column_name(i)
                value = val.value
                if val.type.is_string_like_type() and isinstance(value, bytes):
                    value = value.decode("utf-8")
                result[name] = value
        return result

    def to_builder(self) -> "PayloadBuilder":
        """Create a PayloadBuilder pre-filled with this payload's values."""
        builder = PayloadBuilder(self.schema)
        for i, val in enumerate(self.row.values):
            if i < len(builder.values):
                builder.values[i] = UnversionedValue(
                    column_id=val.column_id, type=val.type, value=val.value,
                )
        return builder

    def __repr__(self):
        return f"Payload(row={self.row}, schema={self.schema})"


Payload.EMPTY = Payload(EMPTY_ROW, EMPTY_SCHEMA)


class PayloadBuilder:
    """Mutable builder for Payload."""

    def __init__(self, schema: TableSchema):
        self.schema = schema
        self.values: list[UnversionedValue] = [
            UnversionedValue(column_id=i, type=ColumnValueType.NULL)
            for i in range(len(schema))
        ]

    def set(self, column_name: str, value: Any) -> "PayloadBuilder":
        column_id = self.schema.find_column(column_name)
        if column_id is None:
            raise ValueError(f"Column not found: {column_name}")
        col_type = self.schema.get_column_type(column_id)
        if value is None:
            self.values[column_id] = UnversionedValue(column_id=column_id, type=ColumnValueType.NULL)
        else:
            if isinstance(value, str):
                value = value.encode("utf-8")
            self.values[column_id] = UnversionedValue(column_id=column_id, type=col_type, value=value)
        return self

    def finish(self) -> Payload:
        row = UnversionedRow(values=list(self.values))
        payload = Payload(row, self.schema)
        self.reset()
        return payload

    def reset(self):
        self.values = [
            UnversionedValue(column_id=i, type=ColumnValueType.NULL)
            for i in range(len(self.schema))
        ]


@dataclass
class Message:
    """A message in the flow pipeline."""
    message_id: str
    event_timestamp: int = 0
    system_timestamp: int = 0
    stream_id: str = ""
    stream_spec_id: int = 0
    payload: Any = None  # Typically a Payload.


@dataclass
class ExtendedMessage(Message):
    """Message with an additional key (from group_by_schema)."""
    key: Optional[Payload] = None


@dataclass
class Timer:
    """A timer event."""
    message_id: str
    event_timestamp: int = 0
    system_timestamp: int = 0
    stream_id: str = ""
    stream_spec_id: int = -1
    trigger_timestamp: int = 0
    key: Optional[Payload] = None


@dataclass
class NewTimer:
    """A timer created at the companion side."""
    trigger_timestamp: int = 0
    event_timestamp: int = 0
    stream_id: Optional[str] = None


@dataclass
class Visit:
    """A visit event emitted by a key-visitor stream."""
    message_id: str
    event_timestamp: int = 0
    system_timestamp: int = 0
    stream_id: str = ""
    key: Optional[Payload] = None


class MessageBuilder:
    """Reusable builder for constructing Message objects."""

    def __init__(self, stream_id: str, schema: TableSchema):
        self.stream_id = stream_id
        self.schema = schema
        self._payload_builder = PayloadBuilder(schema)
        self._system_timestamp = 0
        self._event_timestamp = 0

    def set(self, column_name: str, value: Any) -> "MessageBuilder":
        self._payload_builder.set(column_name, value)
        return self

    def set_system_timestamp(self, ts: int) -> "MessageBuilder":
        self._system_timestamp = ts
        return self

    def set_event_timestamp(self, ts: int) -> "MessageBuilder":
        self._event_timestamp = ts
        return self

    def finish(self) -> Message:
        payload = self._payload_builder.finish()
        msg = Message(
            message_id="1",
            event_timestamp=self._event_timestamp,
            system_timestamp=self._system_timestamp,
            stream_id=self.stream_id,
            payload=payload,
        )
        self._system_timestamp = 0
        self._event_timestamp = 0
        return msg
