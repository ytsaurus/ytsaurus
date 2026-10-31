"""External protobuf API and companion wire-contract tests."""

import pytest

from yt.yt.flow.library.python.companion._proto_compat import ensure_proto_imports
from yt.yt.flow.library.python.companion.computation import Computation
from yt.yt.flow.library.python.companion.context import (
    ExternalStateAccessor,
    ProtoExternalStateAccessor,
    ReadOnlyExternalStateError,
    ResponseContext,
)
from yt.yt.flow.library.python.companion.proto_mapper import (
    external_states_from_proto,
    external_states_to_proto,
    joined_external_states_from_proto,
    map_process_batch_response,
)
from yt.yt.flow.library.python.companion.state import ProtoExternalState, StateFormat, StatesHolder
from yt.yt.flow.library.python.companion.test.proto.message_pb2 import TJoinedAction, TJoinState
from yt.yt.flow.library.python.companion.test_harness import schema
from yt.yt.flow.library.python.companion.row import PayloadBuilder
from yt.yt.flow.library.python.companion.stream import StreamSpecs, StreamIdsMapping
from yt.yt.flow.library.python.companion.wire_protocol import WireProtocolReader, WireProtocolWriter

ensure_proto_imports()
from yt.flow.library.cpp.companion.proto import companion_service_pb2 as cs_pb2  # noqa: E402
from yt.flow.library.cpp.common.proto import message_pb2 as msg_pb2  # noqa: E402

KEY_SCHEMA = schema(id="string")
KEY = PayloadBuilder(KEY_SCHEMA).set("id", "one").finish()


def holder(proto_class=TJoinState):
    return StatesHolder(
        "/profile", KEY_SCHEMA, state_format=StateFormat.PROTO, proto_type=proto_class.DESCRIPTOR.full_name
    )


def wire_state(payload=None, *, reset=False):
    state = cs_pb2.TState(name="/profile", format=1, proto_type=TJoinState.DESCRIPTOR.full_name)
    if payload is not None or reset:
        item = state.stateItems.add(key=WireProtocolWriter().write_unversioned_row(KEY.row), reset=reset)
        if payload is not None:
            item.state = payload
    return state


def encode(state_holder):
    return external_states_to_proto(state_holder, cs_pb2.TState, cs_pb2.TStateItem)


@pytest.mark.parametrize("decode", [external_states_from_proto, joined_external_states_from_proto])
def test_all_missing_batch_keeps_descriptor(decode):
    states = decode([wire_state()], "job", "request", KEY_SCHEMA)
    state_holder = states["/profile"]
    assert state_holder.state_format == StateFormat.PROTO
    assert state_holder.proto_type == TJoinState.DESCRIPTOR.full_name
    assert list(state_holder.items()) == []
    assert not state_holder.collect_modified()
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinState)
    assert accessor.get() is None
    accessor.set(TJoinState())
    result = encode(state_holder)
    assert result.format == 1
    assert result.proto_type == TJoinState.DESCRIPTOR.full_name
    assert len(result.stateItems) == 1
    assert result.stateItems[0].HasField("state")
    assert result.stateItems[0].state == b""
    assert not result.stateItems[0].reset


@pytest.mark.parametrize(
    "reset,payload", [(False, b""), (False, TJoinState(show_time=7).SerializeToString()), (True, None)]
)
def test_wire_round_trip_preserves_presence(reset, payload):
    state_holder = external_states_from_proto([wire_state(payload, reset=reset)], "job", "req", KEY_SCHEMA)["/profile"]
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinState)
    value = accessor.get()
    assert (value is None) == reset
    assert not state_holder.collect_modified()
    if reset:
        accessor.clear()
    else:
        accessor.set(value)
    result = encode(state_holder)
    item = result.stateItems[0]
    assert item.reset == reset
    assert item.HasField("state") == (not reset)
    assert item.state == (payload or b"")
    assert WireProtocolReader(item.key).read_unversioned_row() == KEY.row


def test_missing_read_and_default_do_not_create_state():
    state_holder = holder()
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinState)
    assert accessor.get() is None
    default = accessor.get_or_default()
    default.show_time = 10
    assert accessor.get() is None
    assert accessor.get_or_default() == TJoinState()
    assert not state_holder.collect_modified()
    assert list(state_holder.items()) == []


def test_set_and_get_are_snapshots_and_replacement_clears_absent_fields():
    state_holder = holder()
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinState)
    value = TJoinState(show_time=7, click_time=9)
    accessor.set(value)
    value.show_time = 100
    read = accessor.get()
    read.show_time = 200
    assert accessor.get().show_time == 7
    second = ProtoExternalStateAccessor(state_holder, KEY, TJoinState)
    second.set(TJoinState(show_time=12))
    assert accessor.get().show_time == 12
    assert not accessor.get().HasField("click_time")
    assert len(list(state_holder.modified_items())) == 1


@pytest.mark.parametrize("operations,expected_reset", [("set clear", True), ("clear set", False)])
def test_last_explicit_operation_wins(operations, expected_reset):
    state_holder = holder()
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinState)
    for operation in operations.split():
        if operation == "set":
            accessor.set(TJoinState())
        else:
            accessor.clear()
    item = encode(state_holder).stateItems[0]
    assert item.reset == expected_reset
    assert item.HasField("state") != expected_reset
    assert (accessor.get() is None) == expected_reset


def test_read_only_accessor_isolated_and_never_dirty():
    state_holder = holder()
    state_holder.load(KEY.row, ProtoExternalState(state=TJoinState(show_time=7).SerializeToString()))
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinState, read_only=True)
    accessor.get().show_time = 99
    assert accessor.get().show_time == 7
    with pytest.raises(ReadOnlyExternalStateError):
        accessor.set(TJoinState())
    with pytest.raises(ReadOnlyExternalStateError):
        accessor.clear()
    assert not state_holder.collect_modified()


def test_type_and_format_validation():
    state_holder = holder()
    with pytest.raises(ValueError, match="Proto type mismatch.*profile"):
        ProtoExternalStateAccessor(state_holder, KEY, TJoinedAction)
    with pytest.raises(ValueError, match="row format"):
        ExternalStateAccessor(state_holder, KEY)
    with pytest.raises(ValueError, match="proto format"):
        ProtoExternalStateAccessor(StatesHolder("/row", KEY_SCHEMA, schema(count="uint64")), KEY, TJoinState)
    with pytest.raises(TypeError, match="message class"):
        ProtoExternalStateAccessor(state_holder, KEY, TJoinState())
    with pytest.raises(ValueError, match="requires a key"):
        ProtoExternalStateAccessor(state_holder, None, TJoinState)
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinState)
    accessor.set(TJoinState(show_time=7))
    with pytest.raises(TypeError, match="expects"):
        accessor.set(TJoinedAction())
    assert accessor.get().show_time == 7


def test_invalid_write_does_not_change_pending_state():
    state_holder = holder(TJoinedAction)
    accessor = ProtoExternalStateAccessor(state_holder, KEY, TJoinedAction)
    accessor.clear()
    with pytest.raises(ValueError, match="Failed to serialize.*profile.*required"):
        accessor.set(TJoinedAction())
    assert accessor.get() is None
    assert encode(state_holder).stateItems[0].reset


@pytest.mark.parametrize(
    "proto_class,payload,error", [(TJoinState, b"\x80", "Failed to parse"), (TJoinedAction, b"", "Uninitialized")]
)
def test_malformed_message_reports_state_name(proto_class, payload, error):
    state_holder = holder(proto_class)
    state_holder.load(KEY.row, ProtoExternalState(state=payload))
    with pytest.raises(ValueError, match=error + ".*profile"):
        ProtoExternalStateAccessor(state_holder, KEY, proto_class).get()
    assert not state_holder.collect_modified()


@pytest.mark.parametrize("decode", [external_states_from_proto, joined_external_states_from_proto])
def test_reject_unknown_format_missing_type_and_duplicate_descriptor(decode):
    state = wire_state()
    state.format = 3
    with pytest.raises(ValueError, match="Unknown external state format.*profile.*job.*req"):
        decode([state], "job", "req", KEY_SCHEMA)
    state.format = 1
    state.ClearField("proto_type")
    with pytest.raises(ValueError, match="proto_type"):
        decode([state], "job", "req", KEY_SCHEMA)
    state.proto_type = TJoinState.DESCRIPTOR.full_name
    with pytest.raises(ValueError, match="Duplicate"):
        decode([state, state], "job", "req", KEY_SCHEMA)


def test_row_format_remains_default_and_resets_need_no_payload():
    import yt.yson as yson

    state = cs_pb2.TState(name="/row", schema=yson.dumps([{"name": "count", "type": "uint64"}]))
    row = PayloadBuilder(schema(count="uint64")).set("count", 4).finish()
    item = state.stateItems.add(key=WireProtocolWriter().write_unversioned_row(KEY.row), reset=False)
    item.state = WireProtocolWriter().write_unversioned_row(row.row)
    states = external_states_from_proto([state], "job", "req", KEY_SCHEMA)
    accessor = ExternalStateAccessor(states["/row"], KEY)
    assert accessor["count"] == 4
    accessor.set(row)
    result = encode(states["/row"])
    assert not result.HasField("format")
    assert not result.HasField("proto_type")
    assert result.stateItems[0].state == item.state
    item.reset = True
    item.ClearField("state")
    states = external_states_from_proto([state], "job", "req", KEY_SCHEMA)
    assert states["/row"].get(KEY.row).reset


def test_response_emits_only_modified_proto_states():
    class ProtoModule:
        TResponseData = cs_pb2.TResponseData
        TState = cs_pb2.TState
        TStateItem = cs_pb2.TStateItem
        TNewTimer = cs_pb2.TNewTimer
        TMessage = msg_pb2.TMessage

    state_holder = holder()
    state_holder.load(KEY.row, ProtoExternalState(state=b""))
    response = ResponseContext(external_states={"/profile": state_holder})
    streams = StreamSpecs(StreamIdsMapping(), [])
    assert not map_process_batch_response(streams, response, ProtoModule).external_states
    ProtoExternalStateAccessor(state_holder, KEY, TJoinState).set(TJoinState())
    result = map_process_batch_response(streams, response, ProtoModule)
    assert len(result.external_states) == 1
    assert result.external_states[0].format == 1
    assert result.external_states[0].stateItems[0].HasField("state")


def test_computation_advertises_proto_format():
    comp = Computation("test", lambda message, output, ctx: None)
    assert comp.to_dict()["supported_state_formats"] == ["simple_row", "proto"]


@pytest.mark.parametrize("present", [False, True])
def test_request_processor_handles_proto_manager_and_joiner(present):
    import yt.yson as yson
    from yt.yt.flow.library.python.companion.context import PipelineContext
    from yt.yt.flow.library.python.companion.job import JobContext
    from yt.yt.flow.library.python.companion.service import CompanionRequestProcessor

    class ProtoModule:
        TResponseData = cs_pb2.TResponseData
        TState = cs_pb2.TState
        TStateItem = cs_pb2.TStateItem
        TNewTimer = cs_pb2.TNewTimer
        TMessage = msg_pb2.TMessage

    def process(message, output, ctx):
        joined = ctx.joined_proto_external_state("/joined", message, TJoinState)
        assert (joined.get() is not None) == present
        state = ctx.proto_external_state("/profile", message, TJoinState)
        assert (state.get() is not None) == present
        value = joined.get_or_default()
        value.show_time += 1
        state.set(value)

    pipeline = PipelineContext()
    pipeline.register_computation(Computation("test", process))
    processor = CompanionRequestProcessor(pipeline, JobContext())
    request = cs_pb2.TReqProcessBatch(computation_id="test")
    request.request_id.first, request.request_id.second = 1, 2
    request.job_id.first, request.job_id.second = 3, 4
    key_schema = [{"name": "id", "type": "string"}]
    request.job_info.spec = yson.dumps(
        {
            "group_by_schema": key_schema,
            "external_state_managers": {"/profile": {}},
            "external_state_joiners": {"/joined": {}},
        }
    )
    request.job_info.dynamic_spec = b"{}"
    request.job_info.streams.add(stream_id="input", stream_spec_id=0, schema=yson.dumps(key_schema))
    incoming = request.messages.add(key=WireProtocolWriter().write_unversioned_row(KEY.row))
    incoming.message.CopyFrom(
        msg_pb2.TMessage(
            message_id="input-1",
            system_timestamp=1,
            stream_spec_id=0,
            payload=WireProtocolWriter().write_unversioned_row(KEY.row),
        )
    )
    state = wire_state(TJoinState(show_time=7).SerializeToString() if present else None)
    request.external_states.add().CopyFrom(state)
    state.name = "/joined"
    request.joined_external_states.add().CopyFrom(state)
    try:
        info = processor.get_companion_info()["payload"]
        assert "proto" in info["computations"]["test"]["supported_state_formats"]
        # Serialize both directions to catch required fields and empty-byte presence.
        result = processor.process_batch(cs_pb2.TReqProcessBatch.FromString(request.SerializeToString()), ProtoModule)
        assert result["status"] == "RS_OK"
        response = cs_pb2.TResponseData.FromString(result["data"].SerializeToString())
        assert len(response.external_states) == 1
        written = response.external_states[0]
        assert written.name == "/profile"
        assert written.format == 1
        assert written.proto_type == TJoinState.DESCRIPTOR.full_name
        assert len(written.stateItems) == 1
        assert TJoinState.FromString(written.stateItems[0].state).show_time == (8 if present else 1)
    finally:
        processor.shutdown()
