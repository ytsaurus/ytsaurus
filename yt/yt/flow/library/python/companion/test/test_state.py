"""Tests for StatesHolder modified-state tracking.

Only states changed via set() (accessor writes) are reported by modified_items()/has_modified()
and serialized into the response proto; states populated from the request via load() are not, so
unmodified states are not sent back to the worker.
"""

from yt.yt.flow.library.python.companion.proto_mapper import (
    internal_states_to_proto,
    joined_external_states_from_proto,
)
from yt.yt.flow.library.python.companion.row import (
    ColumnSchema,
    TableSchema,
)
from yt.yt.flow.library.python.companion.state import (
    ExternalState,
    State,
    StatesHolder,
)
from yt.yt.flow.library.python.companion.wire_protocol import (
    ColumnValueType,
    UnversionedRow,
    UnversionedValue,
    WireProtocolWriter,
)
import yt.type_info as ti
import yt.yson as yson

_KEY_SCHEMA = TableSchema([ColumnSchema("id", ti.String)])


def _key(value: str) -> UnversionedRow:
    return UnversionedRow(values=[UnversionedValue(column_id=0, type=ColumnValueType.STRING, value=value.encode())])


# Minimal stand-ins for the injected proto classes (TState / TStateItem).
class _FakeStateItem:
    def __init__(self):
        self.key = None
        self.reset = False
        self.state = None


class _FakeState:
    def __init__(self):
        self.name = None
        self.schema = None
        self.stateItems = []


def test_load_does_not_mark_modified():
    holder = StatesHolder("s", _KEY_SCHEMA, None)
    holder.load(_key("a"), State(state=b"1"))
    holder.load(_key("b"), State(state=b"2"))

    assert len(list(holder.items())) == 2
    assert list(holder.modified_items()) == []
    assert not holder.has_modified()


def test_set_marks_modified():
    holder = StatesHolder("s", _KEY_SCHEMA, None)
    holder.set(_key("a"), State(state=b"1"))

    assert holder.has_modified()
    assert len(list(holder.modified_items())) == 1


def test_only_modified_after_load_is_reported():
    holder = StatesHolder("s", _KEY_SCHEMA, None)
    holder.load(_key("a"), State(state=b"1"))
    holder.load(_key("b"), State(state=b"2"))
    # Accessor changes only key "a".
    holder.set(_key("a"), State(state=b"11"))

    assert len(list(holder.items())) == 2
    modified = list(holder.modified_items())
    assert len(modified) == 1
    ((_, value),) = modified
    assert value.state == b"11"


def test_to_proto_emits_only_modified():
    holder = StatesHolder("s", _KEY_SCHEMA, None)
    holder.load(_key("a"), State(reset=False, state=b"1"))
    holder.set(_key("b"), State(reset=False, state=b"2"))

    proto = internal_states_to_proto(holder, _FakeState, _FakeStateItem)

    assert len(proto.stateItems) == 1
    assert proto.stateItems[0].state == b"2"


def test_loaded_holder_has_no_modifications():
    holder = StatesHolder("/joined", _KEY_SCHEMA, None)
    holder.load(_key("a"), ExternalState(reset=False))

    assert not holder.has_modified()
    assert list(holder.modified_items()) == []


def test_joined_external_states_from_proto_loads_without_modification():
    state_schema_yson = yson.dumps([{"name": "count", "type": "int64"}])
    value_row = UnversionedRow(values=[UnversionedValue(column_id=0, type=ColumnValueType.INT64, value=7)])
    writer = WireProtocolWriter()

    item = _FakeStateItem()
    item.key = writer.write_unversioned_row(_key("hello"))
    item.reset = False
    item.state = writer.write_unversioned_row(value_row)

    proto_state = _FakeState()
    proto_state.name = "/word-state-external"
    proto_state.schema = state_schema_yson
    proto_state.stateItems = [item]

    holders = joined_external_states_from_proto([proto_state], "job", "req", _KEY_SCHEMA)

    holder = holders["/word-state-external"]
    assert holder.has_modified() is False
    assert list(holder.modified_items()) == []
    assert len(list(holder.items())) == 1
