"""Tests for ProcessResponse."""

import yt.yson as yson

from yt.yt.flow.library.python.companion.computation import TransformResult
from yt.yt.flow.library.python.companion.context import ResponseContext
from yt.yt.flow.library.python.companion.row import (
    Message,
    NewTimer,
    Payload,
    PayloadBuilder,
)
from yt.yt.flow.library.python.companion.state import (
    ExternalState,
    State,
    StatesHolder,
)
from yt.yt.flow.library.python.companion.test_harness.response import (
    ProcessResponse,
)
from yt.yt.flow.library.python.companion.test_harness.schema import schema


def _make_key(s, **kw):
    builder = PayloadBuilder(s)
    for name, value in kw.items():
        builder.set(name, value)
    return builder.finish()


# -- ProcessResponse.messages / timers --


def test_messages_flattened():
    tr1 = TransformResult(messages=[
        Message(message_id="1", stream_id="a"),
        Message(message_id="2", stream_id="b"),
    ])
    tr2 = TransformResult(messages=[
        Message(message_id="3", stream_id="c"),
    ])
    ctx = ResponseContext(transform_results=[tr1, tr2])
    r = ProcessResponse(ctx)
    assert len(r.messages) == 3
    assert [m.message_id for m in r.messages] == ["1", "2", "3"]


def test_timers_flattened():
    tr1 = TransformResult(timers=[NewTimer(trigger_timestamp=10)])
    tr2 = TransformResult(timers=[NewTimer(trigger_timestamp=20), NewTimer(trigger_timestamp=30)])
    ctx = ResponseContext(transform_results=[tr1, tr2])
    r = ProcessResponse(ctx)
    assert len(r.timers) == 3
    assert [t.trigger_timestamp for t in r.timers] == [10, 20, 30]


def test_empty_response():
    ctx = ResponseContext()
    r = ProcessResponse(ctx)
    assert r.messages == []
    assert r.timers == []


# -- ProcessResponse internal state --


def test_internal_state_yson():
    key_schema = schema(k="string")
    key = _make_key(key_schema, k="a")
    holder = StatesHolder("s", key_schema, None)
    holder.set(key.row, State(reset=False, state=yson.dumps({"x": 42})))

    ctx = ResponseContext(internal_states={"s": holder})
    r = ProcessResponse(ctx)
    assert r.internal_state("s", key)["x"] == 42


def test_internal_state_raw():
    key_schema = schema(k="string")
    key = _make_key(key_schema, k="a")
    holder = StatesHolder("s", key_schema, None)
    state = State(reset=False, state=b"raw-data")
    holder.set(key.row, state)

    ctx = ResponseContext(internal_states={"s": holder})
    r = ProcessResponse(ctx)
    raw = r.internal_state_raw("s", key)
    assert raw is state


def test_internal_state_size():
    key_schema = schema(k="string")
    k1 = _make_key(key_schema, k="a")
    k2 = _make_key(key_schema, k="b")
    holder = StatesHolder("s", key_schema, None)
    holder.set(k1.row, State(reset=False, state=b"1"))
    holder.set(k2.row, State(reset=False, state=b"2"))

    ctx = ResponseContext(internal_states={"s": holder})
    r = ProcessResponse(ctx)
    assert r.internal_state_size("s") == 2


def test_internal_state_is_reset():
    key_schema = schema(k="string")
    key = _make_key(key_schema, k="a")
    holder = StatesHolder("s", key_schema, None)
    holder.set(key.row, State(reset=True, state=None))

    ctx = ResponseContext(internal_states={"s": holder})
    r = ProcessResponse(ctx)
    assert r.internal_state_is_reset("s", key)


def test_internal_state_missing_name():
    key_schema = schema(k="string")
    key = _make_key(key_schema, k="a")

    ctx = ResponseContext()
    r = ProcessResponse(ctx)
    assert r.internal_state("nonexistent", key) is None
    assert r.internal_state_raw("nonexistent", key) is None
    assert r.internal_state_size("nonexistent") == 0


def test_internal_state_missing_key():
    key_schema = schema(k="string")
    key_present = _make_key(key_schema, k="a")
    key_missing = _make_key(key_schema, k="b")
    holder = StatesHolder("s", key_schema, None)
    holder.set(key_present.row, State(reset=False, state=yson.dumps({"x": 1})))

    ctx = ResponseContext(internal_states={"s": holder})
    r = ProcessResponse(ctx)
    assert r.internal_state("s", key_missing) is None
    assert r.internal_state_raw("s", key_missing) is None


# -- ProcessResponse external state --


def test_external_state():
    key_schema = schema(k="string")
    state_schema = schema(val="int64")
    key = _make_key(key_schema, k="a")
    payload = PayloadBuilder(state_schema).set("val", 7).finish()
    holder = StatesHolder("ext", key_schema, state_schema)
    holder.set(key.row, ExternalState(reset=False, state=payload))

    ctx = ResponseContext(external_states={"ext": holder})
    r = ProcessResponse(ctx)
    assert r.external_state("ext", key)["val"] == 7


def test_external_state_raw():
    key_schema = schema(k="string")
    state_schema = schema(val="int64")
    key = _make_key(key_schema, k="a")
    payload = PayloadBuilder(state_schema).set("val", 3).finish()
    ext = ExternalState(reset=False, state=payload)
    holder = StatesHolder("ext", key_schema, state_schema)
    holder.set(key.row, ext)

    ctx = ResponseContext(external_states={"ext": holder})
    r = ProcessResponse(ctx)
    assert r.external_state_raw("ext", key) is ext


def test_external_state_size():
    key_schema = schema(k="string")
    state_schema = schema(val="int64")
    k1 = _make_key(key_schema, k="a")
    k2 = _make_key(key_schema, k="b")
    k3 = _make_key(key_schema, k="c")
    holder = StatesHolder("ext", key_schema, state_schema)
    for k in [k1, k2, k3]:
        holder.set(k.row, ExternalState(reset=False, state=Payload.EMPTY))

    ctx = ResponseContext(external_states={"ext": holder})
    r = ProcessResponse(ctx)
    assert r.external_state_size("ext") == 3


def test_external_state_is_reset():
    key_schema = schema(k="string")
    state_schema = schema(val="int64")
    key = _make_key(key_schema, k="a")
    holder = StatesHolder("ext", key_schema, state_schema)
    holder.set(key.row, ExternalState(reset=True, state=None))

    ctx = ResponseContext(external_states={"ext": holder})
    r = ProcessResponse(ctx)
    assert r.external_state_is_reset("ext", key)


def test_external_state_missing():
    key_schema = schema(k="string")
    key = _make_key(key_schema, k="a")

    ctx = ResponseContext()
    r = ProcessResponse(ctx)
    assert r.external_state("nonexistent", key) is None
    assert r.external_state_raw("nonexistent", key) is None
    assert r.external_state_size("nonexistent") == 0
