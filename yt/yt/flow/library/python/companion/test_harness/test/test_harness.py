"""Tests for ComputationHarness."""

import pytest

from yt.yt.flow.library.python.companion.computation import (
    RowFunction,
)
from yt.yt.flow.library.python.companion.row import (
    ExtendedMessage,
    Message,
    Payload,
    Timer,
)
from yt.yt.flow.library.python.companion.state import State
from yt.yt.flow.library.python.companion.test_harness import (
    ComputationHarness,
    schema,
)
from yt.yt.flow.library.python.companion.test_harness.harness import (
    _patch_binary_values,
)

# -- Stub functions --


class PassthroughFunction(RowFunction):
    def on_message(self, message, output, ctx):
        output.add_message(
            Message(
                message_id="out",
                stream_id=message.stream_id,
                payload=message.payload,
            )
        )


class TimerFunction(RowFunction):
    def on_message(self, message, output, ctx):
        output.add_timer(trigger_timestamp=42, event_timestamp=10, stream_id="t")

    def on_timer(self, timer, output, ctx):
        output.add_message(Message(message_id="from-timer", stream_id="out"))


class StatefulFunction(RowFunction):
    """Reads/writes YSON internal state."""

    def on_message(self, message, output, ctx):
        state = ctx.state("my-state", message)
        data = state.get_or_default({"count": 0})
        data["count"] += 1
        state.set(data)


class ExternalStatefulFunction(RowFunction):
    """Reads/writes external state."""

    def on_message(self, message, output, ctx):
        state = ctx.external_state("/ext", message)
        builder = state.to_builder()
        builder.set("val", (state.get("val") or 0) + 1)
        state.set(builder.finish())


class ParameterFunction(RowFunction):
    """Emits parameter value as a message."""

    def on_message(self, message, output, ctx):
        v = ctx.parameters.get("key", "missing")
        builder = ctx.message_builder("out")
        builder.set("data", v)
        output.add_message(builder.finish())


class WatermarkFunction(RowFunction):
    """Emits min_watermark as a message field."""

    def on_message(self, message, output, ctx):
        builder = ctx.message_builder("out")
        builder.set("wm", ctx.min_watermark)
        output.add_message(builder.finish())


# -- build_key tests --


def test_build_key():
    h = ComputationHarness(
        PassthroughFunction(),
        streams={"s": schema(x="string")},
        key_schema=schema(a="string", b="int64"),
    )
    key = h.build_key(a="foo", b=42)
    assert isinstance(key, Payload)
    assert key.get("a") == b"foo"
    assert key.get("b") == 42


# -- build_message tests --


def test_build_message_fields():
    h = ComputationHarness(
        PassthroughFunction(),
        streams={"input": schema(x="string", y="int64")},
    )
    msg = h.build_message("input", x="hello", y=7, event_timestamp=100)
    assert isinstance(msg, ExtendedMessage)
    assert msg.stream_id == "input"
    assert msg.event_timestamp == 100
    assert msg.payload["x"] == "hello"
    assert msg.payload["y"] == 7


def test_build_message_with_key():
    h = ComputationHarness(
        PassthroughFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
    )
    key = h.build_key(k="abc")
    msg = h.build_message("input", key=key, x="val")
    assert msg.key is key


def test_build_message_unknown_stream_raises():
    h = ComputationHarness(
        PassthroughFunction(),
        streams={"input": schema(x="string")},
    )
    with pytest.raises(ValueError, match="Unknown stream"):
        h.build_message("nonexistent", x="val")


def test_build_timer():
    h = ComputationHarness(
        PassthroughFunction(),
        streams={"s": schema(x="string")},
        key_schema=schema(k="string"),
    )
    key = h.build_key(k="abc")
    timer = h.build_timer(key=key, trigger_timestamp=999, stream_id="t", event_timestamp=10)
    assert isinstance(timer, Timer)
    assert timer.trigger_timestamp == 999
    assert timer.event_timestamp == 10
    assert timer.stream_id == "t"
    assert timer.key is key


# -- _patch_binary_values tests --


def test_patch_binary_values_non_utf8():
    from yt.yt.flow.library.python.companion.row import PayloadBuilder

    s = schema(data="string")
    builder = PayloadBuilder(s)
    raw = b"\x80\x81\x82"
    builder.set("data", raw)
    payload = builder.finish()
    _patch_binary_values(payload, {"data": raw})
    assert isinstance(payload.row.values[0].value, bytearray)


def test_patch_binary_values_valid_utf8_untouched():
    from yt.yt.flow.library.python.companion.row import PayloadBuilder

    s = schema(data="string")
    builder = PayloadBuilder(s)
    raw = "hello".encode()
    builder.set("data", raw)
    payload = builder.finish()
    _patch_binary_values(payload, {"data": raw})
    assert isinstance(payload.row.values[0].value, bytes)


# -- processing() tests --


def test_processing_passthrough():
    h = ComputationHarness(
        PassthroughFunction(),
        streams={"input": schema(x="string")},
    )
    msg = h.build_message("input", x="hello")

    with h.processing([msg]) as r:
        assert len(r.messages) == 1
        assert r.messages[0].stream_id == "input"


def test_processing_timers():
    h = ComputationHarness(
        TimerFunction(),
        streams={"input": schema(x="string"), "out": schema(x="string")},
    )
    msg = h.build_message("input", x="hello")

    with h.processing([msg]) as r:
        assert len(r.timers) == 1
        assert r.timers[0].trigger_timestamp == 42


def test_processing_on_timer():
    h = ComputationHarness(
        TimerFunction(),
        streams={"input": schema(x="string"), "out": schema(x="string")},
        key_schema=schema(k="string"),
    )
    key = h.build_key(k="a")
    timer = h.build_timer(key=key, trigger_timestamp=42, stream_id="t")

    with h.processing(timers=[timer]) as r:
        assert len(r.messages) == 1
        assert r.messages[0].message_id == "from-timer"


def test_processing_watermarks():
    h = ComputationHarness(
        WatermarkFunction(),
        streams={"input": schema(x="string"), "out": schema(wm="int64")},
    )
    msg = h.build_message("input", x="hello")

    with h.processing([msg], watermarks={"input": 100, "out": 200}) as r:
        assert len(r.messages) == 1
        assert r.messages[0].payload["wm"] == 100  # min of 100, 200


def test_processing_parameters():
    h = ComputationHarness(
        ParameterFunction(),
        streams={"input": schema(x="string"), "out": schema(data="string")},
        parameters={"key": "myvalue"},
    )
    msg = h.build_message("input", x="hello")

    with h.processing([msg]) as r:
        assert r.messages[0].payload["data"] == "myvalue"


def test_processing_internal_state_yson():
    h = ComputationHarness(
        StatefulFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
        internal_states={"my-state"},
    )
    key = h.build_key(k="a")
    msg = h.build_message("input", key=key, x="hello")

    with h.processing([msg]) as r:
        assert r.internal_state("my-state", key)["count"] == 1


def test_processing_internal_state_prepopulated_dict():
    h = ComputationHarness(
        StatefulFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
        internal_states={"my-state"},
    )
    key = h.build_key(k="a")
    msg = h.build_message("input", key=key, x="hello")

    with h.processing([msg], internal_states={"my-state": {key: {"count": 10}}}) as r:
        assert r.internal_state("my-state", key)["count"] == 11


def test_processing_internal_state_prepopulated_bytes():
    import yt.yson as yson

    h = ComputationHarness(
        StatefulFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
        internal_states={"my-state"},
    )
    key = h.build_key(k="a")
    msg = h.build_message("input", key=key, x="hello")
    raw = yson.dumps({"count": 5})

    with h.processing([msg], internal_states={"my-state": {key: raw}}) as r:
        assert r.internal_state("my-state", key)["count"] == 6


def test_processing_internal_state_prepopulated_state_object():
    import yt.yson as yson

    h = ComputationHarness(
        StatefulFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
        internal_states={"my-state"},
    )
    key = h.build_key(k="a")
    msg = h.build_message("input", key=key, x="hello")
    state_obj = State(reset=False, state=yson.dumps({"count": 99}))

    with h.processing([msg], internal_states={"my-state": {key: state_obj}}) as r:
        assert r.internal_state("my-state", key)["count"] == 100


def test_processing_external_state():
    h = ComputationHarness(
        ExternalStatefulFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
        external_states={"/ext": schema(val="int64")},
    )
    key = h.build_key(k="a")
    msg = h.build_message("input", key=key, x="hello")

    with h.processing([msg]) as r:
        assert r.external_state("/ext", key)["val"] == 1


def test_processing_external_state_prepopulated():
    from yt.yt.flow.library.python.companion.row import PayloadBuilder

    h = ComputationHarness(
        ExternalStatefulFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
        external_states={"/ext": schema(val="int64")},
    )
    key = h.build_key(k="a")
    msg = h.build_message("input", key=key, x="hello")
    pre = PayloadBuilder(schema(val="int64")).set("val", 10).finish()

    with h.processing([msg], external_states={"/ext": {key: pre}}) as r:
        assert r.external_state("/ext", key)["val"] == 11


def test_multi_step_stateful():
    h = ComputationHarness(
        StatefulFunction(),
        streams={"input": schema(x="string")},
        key_schema=schema(k="string"),
        internal_states={"my-state"},
    )
    key = h.build_key(k="a")
    msg = h.build_message("input", key=key, x="hello")

    # Step 1
    with h.processing([msg]) as r1:
        assert r1.internal_state("my-state", key)["count"] == 1

    # Step 2: carry state forward
    with h.processing([msg], internal_states={"my-state": {key: r1.internal_state_raw("my-state", key)}}) as r2:
        assert r2.internal_state("my-state", key)["count"] == 2


# -- State coercion tests --


def test_coerce_internal_state_unsupported():
    with pytest.raises(TypeError, match="Cannot coerce"):
        ComputationHarness._coerce_internal_state(12345)


def test_coerce_external_state_unsupported():
    with pytest.raises(TypeError, match="Cannot coerce"):
        ComputationHarness._coerce_external_state("bad")


def test_coerce_internal_state_protobuf_like():
    class FakeProto:
        def SerializeToString(self):
            return b"proto-bytes"

    state = ComputationHarness._coerce_internal_state(FakeProto())
    assert isinstance(state, State)
    assert state.state == b"proto-bytes"
    assert state.reset is False
