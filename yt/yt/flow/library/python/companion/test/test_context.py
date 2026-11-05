"""Tests for context classes: state accessors, RuntimeContext, Payload.to_builder()."""

import pytest

from yt.yt.flow.library.python.companion.context import (
    ExternalStateAccessor,
    PipelineContext,
    ProtoStateAccessor,
    RawStateAccessor,
    ReadOnlyExternalStateAccessor,
    ReadOnlyExternalStateError,
    YsonStateAccessor,
    DefaultRuntimeContext,
)
from yt.yt.flow.library.python.companion.row import (
    ColumnSchema,
    ExtendedMessage,
    MessageBuilder,
    Payload,
    PayloadBuilder,
    TableSchema,
    Timer,
)
from yt.yt.flow.library.python.companion.state import (
    State,
    StatesHolder,
)
from yt.yt.flow.library.python.companion.stream import (
    StreamIdsMapping,
    StreamSpecs,
    RawStream,
)
import yt.type_info as ti

from yt.yt.flow.library.python.companion.wire_protocol import (
    ColumnValueType,
    UnversionedRow,
    UnversionedValue,
)
from yt.yt.flow.library.python.companion.test.proto.message_pb2 import (
    TJoinState,
    TJoinedAction,
)

# ---------- helpers ----------

_STATE_SCHEMA = TableSchema(
    [
        ColumnSchema("count", ti.Int64),
        ColumnSchema("name", ti.String),
    ]
)

_KEY_SCHEMA = TableSchema([ColumnSchema("id", ti.String)])

_STREAM_SCHEMA = TableSchema(
    [
        ColumnSchema("word", ti.String),
        ColumnSchema("value", ti.Int64),
    ]
)


def _make_key_payload():
    row = UnversionedRow(
        values=[
            UnversionedValue(column_id=0, type=ColumnValueType.STRING, value=b"test-key"),
        ]
    )
    return Payload(row, _KEY_SCHEMA)


def _make_internal_holder():
    return StatesHolder("int-state", _KEY_SCHEMA, None)


def _make_external_holder():
    return StatesHolder("ext", _KEY_SCHEMA, _STATE_SCHEMA)


def _make_state_payload(**kwargs):
    builder = PayloadBuilder(_STATE_SCHEMA)
    for k, v in kwargs.items():
        builder.set(k, v)
    return builder.finish()


def _make_ctx(
    internal_state_names=None,
    internal_states=None,
    external_states=None,
    joined_external_states=None,
    joiner_state_names=None,
    watermarks=None,
    min_watermark=0,
    parameters=None,
    dynamic_parameters=None,
    streams=None,
):
    if streams is None:
        streams = [RawStream("input", _STREAM_SCHEMA), RawStream("output", _STREAM_SCHEMA)]
    mapping = StreamIdsMapping({s.stream_id: i for i, s in enumerate(streams)})
    specs = StreamSpecs(mapping, streams)
    return DefaultRuntimeContext(
        internal_state_names=internal_state_names or set(),
        stream_specs=specs,
        internal_states=internal_states or {},
        external_states=external_states or {},
        watermarks=watermarks or {},
        min_watermark=min_watermark,
        computation_parameters=parameters or {},
        computation_dynamic_parameters=dynamic_parameters or {},
        key_schema=_KEY_SCHEMA,
        joined_external_states=joined_external_states or {},
        joiner_state_names=joiner_state_names or set(),
    )


# ---------- Payload.to_builder ----------


class TestPayloadToBuilder:
    def test_empty_payload_to_builder(self):
        payload = PayloadBuilder(_STATE_SCHEMA).finish()
        builder = payload.to_builder()
        result = builder.finish()
        assert result.get("count") is None
        assert result.get("name") is None

    def test_roundtrip_preserves_values(self):
        payload = _make_state_payload(count=42, name="hello")
        builder = payload.to_builder()
        result = builder.finish()
        assert result.get("count") == 42
        assert result.get("name", str) == "hello"

    def test_builder_can_override_values(self):
        payload = _make_state_payload(count=1, name="old")
        builder = payload.to_builder()
        builder.set("count", 2)
        result = builder.finish()
        assert result.get("count") == 2
        assert result.get("name", str) == "old"

    def test_builder_can_set_null(self):
        payload = _make_state_payload(count=10, name="x")
        builder = payload.to_builder()
        builder.set("count", None)
        result = builder.finish()
        assert result.get("count") is None
        assert result.get("name", str) == "x"


# ---------- RawStateAccessor ----------


class TestRawStateAccessor:
    def _make_accessor(self):
        key = _make_key_payload()
        holder = _make_internal_holder()
        return RawStateAccessor(key, holder)

    def test_get_returns_none_when_no_state(self):
        assert self._make_accessor().get() is None

    def test_set_and_get_roundtrip(self):
        accessor = self._make_accessor()
        accessor.set(b"hello")
        assert accessor.get() == b"hello"

    def test_clear(self):
        accessor = self._make_accessor()
        accessor.set(b"data")
        accessor.clear()
        assert accessor.get() is None

    def test_get_or_default_when_missing(self):
        accessor = self._make_accessor()
        assert accessor.get_or_default(b"fallback") == b"fallback"

    def test_get_or_default_when_present(self):
        accessor = self._make_accessor()
        accessor.set(b"real")
        assert accessor.get_or_default(b"fallback") == b"real"

    def test_none_key(self):
        holder = _make_internal_holder()
        accessor = RawStateAccessor(None, holder)
        assert accessor.get() is None


# ---------- YsonStateAccessor ----------


class TestYsonStateAccessor:
    def _make_accessor(self):
        key = _make_key_payload()
        holder = _make_internal_holder()
        return YsonStateAccessor(key, holder)

    def test_get_returns_none_when_no_state(self):
        assert self._make_accessor().get() is None

    def test_set_and_get_roundtrip(self):
        accessor = self._make_accessor()
        accessor.set({"word": "hello", "count": 5})
        result = accessor.get()
        assert result["word"] == "hello"
        assert result["count"] == 5

    def test_set_raw_bytes(self):
        import yt.yson as yson

        accessor = self._make_accessor()
        accessor.set(yson.dumps({"x": 1}))
        result = accessor.get()
        assert result["x"] == 1

    def test_clear(self):
        accessor = self._make_accessor()
        accessor.set({"a": 1})
        accessor.clear()
        assert accessor.get() is None

    def test_get_or_default_when_missing(self):
        accessor = self._make_accessor()
        assert accessor.get_or_default({"default": True}) == {"default": True}

    def test_get_or_default_when_present(self):
        accessor = self._make_accessor()
        accessor.set({"real": True})
        result = accessor.get_or_default({"default": True})
        assert "real" in result

    def test_none_key(self):
        holder = _make_internal_holder()
        accessor = YsonStateAccessor(None, holder)
        assert accessor.get() is None

    def test_get_returns_none_when_state_is_none(self):
        accessor = self._make_accessor()
        key = accessor._get_row_key()
        accessor._states_holder.set(key, State(reset=False, state=None))
        assert accessor.get() is None


# ---------- ProtoStateAccessor ----------


class TestProtoStateAccessor:
    def _make_accessor(self, proto_class=TJoinState):
        key = _make_key_payload()
        holder = StatesHolder("test-state", _KEY_SCHEMA, None)
        return ProtoStateAccessor(key, holder, proto_class)

    def test_get_returns_none_when_no_state(self):
        assert self._make_accessor().get() is None

    def test_set_and_get_roundtrip(self):
        accessor = self._make_accessor()
        state = TJoinState()
        state.show_time = 100
        state.click_time = 200
        state.hit_payload = "test_payload"
        accessor.set(state)
        result = accessor.get()
        assert result.show_time == 100
        assert result.click_time == 200
        assert result.hit_payload == "test_payload"

    def test_get_or_default_returns_default_when_no_state(self):
        accessor = self._make_accessor()
        default = TJoinState()
        default.show_time = 42
        assert accessor.get_or_default(default).show_time == 42

    def test_get_or_default_returns_empty_proto_when_no_default(self):
        accessor = self._make_accessor()
        result = accessor.get_or_default()
        assert isinstance(result, TJoinState)

    def test_get_or_default_returns_stored_value(self):
        accessor = self._make_accessor()
        state = TJoinState()
        state.show_time = 555
        accessor.set(state)
        assert accessor.get_or_default().show_time == 555

    def test_clear(self):
        accessor = self._make_accessor()
        state = TJoinState()
        state.show_time = 100
        accessor.set(state)
        assert accessor.get() is not None
        accessor.clear()
        assert accessor.get() is None

    def test_none_key(self):
        holder = StatesHolder("test-state", None, None)
        accessor = ProtoStateAccessor(None, holder, TJoinState)
        assert accessor.get() is None

    def test_different_proto_class(self):
        accessor = self._make_accessor(TJoinedAction)
        action = TJoinedAction()
        action.hit_id = "hit-123"
        action.hit_time = 1000
        action.is_click = True
        action.show_time = 900
        action.click_time = 950
        action.hit_payload = "payload"
        accessor.set(action)
        result = accessor.get()
        assert result.hit_id == "hit-123"
        assert result.hit_time == 1000
        assert result.is_click is True
        assert result.show_time == 900
        assert result.click_time == 950
        assert result.hit_payload == "payload"

    def test_get_returns_none_when_state_bytes_none(self):
        accessor = self._make_accessor()
        key = accessor._get_row_key()
        accessor._states_holder.set(key, State(reset=False, state=None))
        assert accessor.get() is None


# ---------- ExternalStateAccessor ----------


class TestExternalStateAccessor:
    def test_is_payload_subclass(self):
        holder = _make_external_holder()
        accessor = ExternalStateAccessor(holder, _make_key_payload())
        assert isinstance(accessor, Payload)

    def test_empty_state_returns_default(self):
        holder = _make_external_holder()
        accessor = ExternalStateAccessor(holder, _make_key_payload())
        assert accessor.get("count") is None
        assert accessor.get("name") is None

    def test_set_and_read_back(self):
        holder = _make_external_holder()
        key = _make_key_payload()
        ExternalStateAccessor(holder, key).set(_make_state_payload(count=7, name="test"))
        accessor2 = ExternalStateAccessor(holder, key)
        assert accessor2.get("count") == 7
        assert accessor2.get("name", str) == "test"

    def test_clear(self):
        holder = _make_external_holder()
        key = _make_key_payload()
        ExternalStateAccessor(holder, key).set(_make_state_payload(count=5))
        ExternalStateAccessor(holder, key).clear()
        accessor = ExternalStateAccessor(holder, key)
        assert accessor.get("count") is None

    def test_to_builder_modify_and_set(self):
        holder = _make_external_holder()
        key = _make_key_payload()
        ExternalStateAccessor(holder, key).set(_make_state_payload(count=3, name="abc"))
        accessor = ExternalStateAccessor(holder, key)
        builder = accessor.to_builder()
        builder.set("count", 4)
        accessor.set(builder.finish())
        result = ExternalStateAccessor(holder, key)
        assert result.get("count") == 4
        assert result.get("name", str) == "abc"

    def test_none_key(self):
        holder = _make_external_holder()
        accessor = ExternalStateAccessor(holder, None)
        assert accessor.get("count") is None

    def test_dict_access(self):
        holder = _make_external_holder()
        key = _make_key_payload()
        ExternalStateAccessor(holder, key).set(_make_state_payload(count=1))
        accessor = ExternalStateAccessor(holder, key)
        assert "count" in accessor
        assert accessor["count"] == 1


# ---------- DefaultRuntimeContext ----------


class TestDefaultRuntimeContext:
    def test_parameters(self):
        ctx = _make_ctx(parameters={"key": "value"})
        assert ctx.parameters == {"key": "value"}

    def test_dynamic_parameters(self):
        ctx = _make_ctx(dynamic_parameters={"key": "value"})
        assert ctx.dynamic_parameters == {"key": "value"}

    def test_min_watermark(self):
        ctx = _make_ctx(min_watermark=42)
        assert ctx.min_watermark == 42

    def test_watermark_for_known_stream(self):
        ctx = _make_ctx(watermarks={"input": 100})
        assert ctx.watermark("input") == 100

    def test_watermark_for_unknown_stream(self):
        ctx = _make_ctx()
        assert ctx.watermark("nonexistent") is None

    def test_stream_specs(self):
        ctx = _make_ctx()
        assert ctx.stream_specs is not None
        assert ctx.stream_specs.get_stream("input") is not None

    def test_message_builder(self):
        ctx = _make_ctx()
        builder = ctx.message_builder("input")
        assert isinstance(builder, MessageBuilder)

    def test_message_builder_unknown_stream_raises(self):
        ctx = _make_ctx()
        with pytest.raises(ValueError, match="Unknown streamId"):
            ctx.message_builder("nonexistent")


class TestDefaultRuntimeContextState:
    def test_state_creates_yson_accessor(self):
        ctx = _make_ctx(internal_state_names={"word-state"})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        accessor = ctx.state("word-state", message)
        assert isinstance(accessor, YsonStateAccessor)

    def test_state_roundtrip(self):
        ctx = _make_ctx(internal_state_names={"word-state"})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        accessor = ctx.state("word-state", message)
        accessor.set({"word": "hi", "count": 1})
        accessor2 = ctx.state("word-state", message)
        result = accessor2.get()
        assert result["word"] == "hi"
        assert result["count"] == 1

    def test_state_invalid_name_raises(self):
        ctx = _make_ctx(internal_state_names=set())
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="State must be configured"):
            ctx.state("bad-state", message)

    def test_state_without_key_attribute(self):
        ctx = _make_ctx(internal_state_names={"s"})

        class NoKey:
            pass

        accessor = ctx.state("s", NoKey())
        assert isinstance(accessor, YsonStateAccessor)


class TestDefaultRuntimeContextRawState:
    def test_raw_state_creates_accessor(self):
        ctx = _make_ctx(internal_state_names={"raw"})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        accessor = ctx.raw_state("raw", message)
        assert isinstance(accessor, RawStateAccessor)

    def test_raw_state_roundtrip(self):
        ctx = _make_ctx(internal_state_names={"raw"})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        accessor = ctx.raw_state("raw", message)
        accessor.set(b"bytes-data")
        accessor2 = ctx.raw_state("raw", message)
        assert accessor2.get() == b"bytes-data"


class TestDefaultRuntimeContextExternalState:
    def test_returns_external_state_accessor(self):
        ext_holder = _make_external_holder()
        ctx = _make_ctx(external_states={"/join-state": ext_holder})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        state = ctx.external_state("/join-state", message)
        assert isinstance(state, ExternalStateAccessor)
        assert isinstance(state, Payload)

    def test_roundtrip_via_context(self):
        ext_holder = _make_external_holder()
        ctx = _make_ctx(external_states={"/join-state": ext_holder})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        state = ctx.external_state("/join-state", message)
        state.set(_make_state_payload(count=99))
        state2 = ctx.external_state("/join-state", message)
        assert state2.get("count") == 99

    def test_unknown_state_raises(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="not found"):
            ctx.external_state("/nonexistent", message)

    def test_rejects_name_without_leading_slash(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="does not start with '/'"):
            ctx.external_state("state", message)

    def test_rejects_empty_name(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="empty"):
            ctx.external_state("", message)

    def test_rejects_root_name(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="root"):
            ctx.external_state("/", message)

    def test_rejects_trailing_slash(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="ends with '/'"):
            ctx.external_state("/state/", message)

    def test_rejects_double_slash(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="two adjacent '/'"):
            ctx.external_state("/foo//bar", message)


class TestReadOnlyExternalStateAccessor:
    def _make_accessor(self, **fields):
        holder = _make_external_holder()
        key = _make_key_payload()
        if fields:
            ExternalStateAccessor(holder, key).set(_make_state_payload(**fields))
        return ReadOnlyExternalStateAccessor(holder, key)

    def test_is_payload_subclass(self):
        assert isinstance(self._make_accessor(), Payload)

    def test_get_reads_joined_value(self):
        accessor = self._make_accessor(count=7, name="hello")
        assert accessor.get("count") == 7
        assert accessor.get("name", str) == "hello"
        assert accessor["count"] == 7

    def test_empty_state_returns_default(self):
        accessor = self._make_accessor()
        assert accessor.get("count") is None

    def test_set_raises(self):
        accessor = self._make_accessor()
        with pytest.raises(ReadOnlyExternalStateError, match="read-only"):
            accessor.set(_make_state_payload(count=1))

    def test_clear_raises(self):
        accessor = self._make_accessor(count=5)
        with pytest.raises(ReadOnlyExternalStateError, match="read-only"):
            accessor.clear()


class TestDefaultRuntimeContextJoinedExternalState:
    def _ctx(self):
        holder = _make_external_holder()
        ExternalStateAccessor(holder, _make_key_payload()).set(_make_state_payload(count=7))
        return _make_ctx(
            joined_external_states={"/word-state-external": holder},
            joiner_state_names={"/word-state-external"},
        )

    def test_returns_read_only_accessor(self):
        ctx = self._ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        state = ctx.joined_external_state("/word-state-external", message)
        assert isinstance(state, ReadOnlyExternalStateAccessor)
        assert isinstance(state, Payload)

    def test_get_succeeds(self):
        ctx = self._ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        assert ctx.joined_external_state("/word-state-external", message).get("count") == 7

    def test_set_raises(self):
        ctx = self._ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ReadOnlyExternalStateError):
            ctx.joined_external_state("/word-state-external", message).set(_make_state_payload(count=1))

    def test_clear_raises(self):
        ctx = self._ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ReadOnlyExternalStateError):
            ctx.joined_external_state("/word-state-external", message).clear()

    def test_unknown_joiner_name_raises(self):
        ctx = _make_ctx(joiner_state_names={"/word-state-external"})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="not found"):
            ctx.joined_external_state("/word-state-external", message)

    def test_name_not_in_joiner_set_raises(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="external_state_joiners"):
            ctx.joined_external_state("/word-state-external", message)

    def test_rejects_name_without_leading_slash(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="does not start with '/'"):
            ctx.joined_external_state("state", message)

    def test_rejects_empty_name(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="empty"):
            ctx.joined_external_state("", message)

    def test_rejects_root_name(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="root"):
            ctx.joined_external_state("/", message)

    def test_rejects_trailing_slash(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="ends with '/'"):
            ctx.joined_external_state("/state/", message)

    def test_rejects_double_slash(self):
        ctx = _make_ctx()
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError, match="two adjacent '/'"):
            ctx.joined_external_state("/foo//bar", message)


class TestDefaultRuntimeContextProtoState:
    def test_proto_state_creates_accessor(self):
        ctx = _make_ctx(internal_state_names={"join-state"})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        accessor = ctx.proto_state("join-state", message, TJoinState)
        assert isinstance(accessor, ProtoStateAccessor)

    def test_proto_state_roundtrip(self):
        ctx = _make_ctx(internal_state_names={"join-state"})
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        accessor = ctx.proto_state("join-state", message, TJoinState)
        state = TJoinState()
        state.show_time = 42
        accessor.set(state)
        accessor2 = ctx.proto_state("join-state", message, TJoinState)
        assert accessor2.get().show_time == 42

    def test_proto_state_with_timer(self):
        ctx = _make_ctx(internal_state_names={"join-state"})
        timer = Timer(message_id="t1", key=_make_key_payload())
        accessor = ctx.proto_state("join-state", timer, TJoinState)
        state = TJoinState()
        state.hit_payload = "test"
        accessor.set(state)
        assert accessor.get().hit_payload == "test"

    def test_proto_state_invalid_name_raises(self):
        ctx = _make_ctx(internal_state_names=set())
        message = ExtendedMessage(message_id="m1", key=_make_key_payload())
        with pytest.raises(ValueError):
            ctx.proto_state("nonexistent-state", message, TJoinState)


class _StubComputation:
    """Minimal stand-in for a Computation: only ``computation_id`` is needed
    by ``PipelineContext.register_computation``."""

    def __init__(self, computation_id):
        self.computation_id = computation_id


class TestPipelineContextFreeze:
    """Tests that PipelineContext can be frozen to prevent post-start mutation."""

    def test_freeze_blocks_register_computation(self):
        ctx = PipelineContext()
        ctx.register_computation(_StubComputation("c1"))

        ctx._freeze()

        with pytest.raises(RuntimeError, match="frozen"):
            ctx.register_computation(_StubComputation("c2"))

        # Read accessors still work after freezing.
        assert ctx.get_computation("c1") is not None
        assert ctx.get_computation("c2") is None

    def test_freeze_blocks_register_stream(self):
        ctx = PipelineContext()
        ctx.register_stream(RawStream("s1"))

        ctx._freeze()

        with pytest.raises(RuntimeError, match="frozen"):
            ctx.register_stream(RawStream("s2"))

        # Read accessors still work after freezing.
        assert ctx.get_stream_context().get_stream("s1") is not None
        assert ctx.get_stream_context().get_stream("s2") is None

    def test_registrations_work_before_freeze(self):
        ctx = PipelineContext()
        ctx.register_computation(_StubComputation("c1"))
        ctx.register_stream(RawStream("s1"))
        ctx.register_computation(_StubComputation("c2"))
        ctx.register_stream(RawStream("s2"))

        assert ctx.get_computation("c1") is not None
        assert ctx.get_computation("c2") is not None
        assert ctx.get_stream_context().get_stream("s1") is not None
        assert ctx.get_stream_context().get_stream("s2") is not None
