"""Proto mapping utilities for protobuf requests/responses to Python domain objects."""

import logging
from typing import Any, Dict

from .context import RequestContext, ResponseContext
from .job import Job
from .row import (
    ColumnSchema,
    ExtendedMessage,
    Message,
    NewTimer,
    Payload,
    TableSchema,
    Timer,
    Visit,
    YT_TYPE_NAME_TO_TI,
    EMPTY_SCHEMA,
)
from .state import ExternalState, State, StatesHolder
from .stream import (
    FlowStreamsContext,
    RawStream,
    StreamIdsMapping,
    StreamSpecs,
)
from .wire_protocol import WireProtocolReader, WireProtocolWriter

log = logging.getLogger(__name__)

FAKE_MESSAGE_ID = "1"
UNSET_TIMESTAMP = 0


def _guid_to_str(guid_proto) -> str:
    """Convert a TGuid protobuf to string."""
    return f"{guid_proto.first:x}-{guid_proto.second:x}"


def _table_schema_from_yson(yson_data) -> TableSchema:
    """Parse a TableSchema from YSON bytes or dict."""
    if yson_data is None:
        return EMPTY_SCHEMA
    try:
        import yt.yson as yson

        if isinstance(yson_data, bytes):
            columns_list = yson.loads(yson_data)
        else:
            columns_list = yson_data
    except Exception:
        return EMPTY_SCHEMA

    if not isinstance(columns_list, list):
        return EMPTY_SCHEMA

    columns = []
    for col in columns_list:
        name = col.get("name", "")
        type_name = col.get("type", "string")
        ti_type = _type_name_to_ti_type(type_name)
        columns.append(ColumnSchema(name=name, type=ti_type))
    return TableSchema(columns)


def _type_name_to_ti_type(type_name: str):
    """Map YT type name string to type_info type."""
    import yt.type_info as ti

    return YT_TYPE_NAME_TO_TI.get(type_name, ti.String)


def _yson_from_proto(data: bytes) -> Any:
    """Decode YSON bytes."""
    try:
        import yt.yson as yson

        return yson.loads(data)
    except Exception:
        return {}


def _yson_to_proto(data: Any) -> bytes:
    """Encode to YSON bytes."""
    try:
        import yt.yson as yson

        return yson.dumps(data)
    except Exception:
        return b""


# ---------- Request Mapping ----------


def streams_from_proto(proto_streams, streams_context: FlowStreamsContext) -> StreamSpecs:
    """Map proto TStream list to StreamSpecs."""
    mapping = StreamIdsMapping()
    flow_streams = []
    for proto_stream in proto_streams:
        stream_id = proto_stream.stream_id
        spec_id = proto_stream.stream_spec_id
        mapping.add_mapping(stream_id, spec_id)

        stream = streams_context.get_stream(stream_id)
        if stream is None:
            schema = _table_schema_from_yson(proto_stream.schema)
            stream = RawStream(stream_id, schema)
        flow_streams.append(stream)

    return StreamSpecs(mapping, flow_streams)


def extended_message_from_proto(proto_message, key_schema, stream_specs: StreamSpecs) -> ExtendedMessage:
    """Map proto TExtendedMessage to ExtendedMessage."""
    inner = proto_message.message
    stream_id = stream_specs.get_stream_id(inner.stream_spec_id)
    stream = stream_specs.get_stream(stream_id)

    key = Payload(
        WireProtocolReader(proto_message.key).read_unversioned_row(),
        key_schema,
    )

    payload = stream.from_proto(inner.payload) if stream else Payload.EMPTY

    return ExtendedMessage(
        message_id=inner.message_id,
        event_timestamp=inner.event_timestamp if inner.HasField("event_timestamp") else 0,
        system_timestamp=inner.system_timestamp,
        stream_id=stream_id or "",
        stream_spec_id=inner.stream_spec_id,
        payload=payload,
        key=key,
    )


def message_from_proto(proto_message, stream_specs: StreamSpecs) -> Message:
    """Map proto TMessage to Message."""
    stream_id = stream_specs.get_stream_id(proto_message.stream_spec_id)
    stream = stream_specs.get_stream(stream_id)
    payload = stream.from_proto(proto_message.payload) if stream else Payload.EMPTY

    return Message(
        message_id=proto_message.message_id,
        event_timestamp=proto_message.event_timestamp if proto_message.HasField("event_timestamp") else 0,
        system_timestamp=proto_message.system_timestamp,
        stream_id=stream_id or "",
        stream_spec_id=proto_message.stream_spec_id,
        payload=payload,
    )


def message_to_proto(message: Message, stream_specs: StreamSpecs, TMessage):
    """Map Message to proto TMessage."""
    msg = TMessage()
    spec_id = stream_specs.get_stream_spec_id(message.stream_id)
    stream = stream_specs.get_stream(message.stream_id)

    msg.stream_spec_id = spec_id if spec_id is not None else 0
    if stream:
        msg.payload = stream.to_proto(message.payload)
    msg.message_id = FAKE_MESSAGE_ID
    msg.system_timestamp = message.system_timestamp if message.system_timestamp > 0 else UNSET_TIMESTAMP
    msg.event_timestamp = message.event_timestamp if message.event_timestamp > 0 else UNSET_TIMESTAMP
    return msg


def timer_from_proto(proto_timer, key_schema) -> Timer:
    """Map proto TTimer to Timer."""
    return Timer(
        message_id=proto_timer.message_id,
        event_timestamp=proto_timer.event_timestamp if proto_timer.HasField("event_timestamp") else 0,
        system_timestamp=proto_timer.system_timestamp,
        stream_id=(
            proto_timer.stream_id.decode("utf-8") if isinstance(proto_timer.stream_id, bytes) else proto_timer.stream_id
        ),
        stream_spec_id=-1,
        trigger_timestamp=proto_timer.trigger_timestamp,
        key=Payload(
            WireProtocolReader(proto_timer.key).read_unversioned_row(),
            key_schema,
        ),
    )


def visit_from_proto(proto_visit, key_schema) -> Visit:
    """Map proto TVisit to Visit."""
    return Visit(
        message_id=proto_visit.message_id,
        event_timestamp=proto_visit.event_timestamp if proto_visit.HasField("event_timestamp") else 0,
        system_timestamp=proto_visit.system_timestamp,
        stream_id=(
            proto_visit.stream_id.decode("utf-8") if isinstance(proto_visit.stream_id, bytes) else proto_visit.stream_id
        ),
        key=Payload(
            WireProtocolReader(proto_visit.key).read_unversioned_row(),
            key_schema,
        ),
    )


def timer_to_proto(timer: NewTimer, TNewTimer):
    """Map NewTimer to proto TNewTimer."""
    t = TNewTimer()
    t.trigger_timestamp = timer.trigger_timestamp
    if timer.event_timestamp > 0:
        t.event_timestamp = timer.event_timestamp
    if timer.stream_id is not None:
        t.stream_id = timer.stream_id.encode("utf-8") if isinstance(timer.stream_id, str) else timer.stream_id
    return t


def internal_states_from_proto(proto_states, key_schema) -> Dict[str, StatesHolder]:
    """Map proto TState list to dict of internal StatesHolders."""
    states = {}
    for proto_state in proto_states:
        for state_item in proto_state.stateItems:
            name = proto_state.name
            if name not in states:
                states[name] = StatesHolder(name, key_schema, None)
            row_key = WireProtocolReader(state_item.key).read_unversioned_row()
            state_data = state_item.state if state_item.HasField("state") else b""
            states[name].load(row_key, State(reset=state_item.reset, state=bytes(state_data)))
    return states


def external_states_from_proto(proto_states, job_id, request_id, key_schema) -> Dict[str, StatesHolder]:
    """Map proto TState list to dict of external StatesHolders."""
    states = {}
    for proto_state in proto_states:
        if not proto_state.schema:
            raise ValueError(
                f"External state must have a schema (StateName: {proto_state.name}, "
                f"JobId: {job_id}, RequestId: {request_id})"
            )
        state_schema = _table_schema_from_yson(proto_state.schema)
        for state_item in proto_state.stateItems:
            name = proto_state.name
            if name not in states:
                states[name] = StatesHolder(name, key_schema, state_schema)
            row_key = WireProtocolReader(state_item.key).read_unversioned_row()
            state_value = WireProtocolReader(state_item.state).read_unversioned_row()
            states[name].load(
                row_key,
                ExternalState(
                    reset=state_item.reset,
                    state=Payload(state_value, state_schema),
                ),
            )
    return states


def joined_external_states_from_proto(proto_states, job_id, request_id, key_schema) -> Dict[str, StatesHolder]:
    """Map proto TState list to dict of read-only joined external StatesHolders.

    Holders are populated via ``load()`` so the joined rows are never marked modified and are
    never echoed back: joined external state is read-only at the protocol layer.
    """
    states = {}
    for proto_state in proto_states:
        if not proto_state.schema:
            raise ValueError(
                f"Joined external state must have a schema (StateName: {proto_state.name}, "
                f"JobId: {job_id}, RequestId: {request_id})"
            )
        state_schema = _table_schema_from_yson(proto_state.schema)
        for state_item in proto_state.stateItems:
            name = proto_state.name
            if name not in states:
                states[name] = StatesHolder(name, key_schema, state_schema)
            row_key = WireProtocolReader(state_item.key).read_unversioned_row()
            state_value = WireProtocolReader(state_item.state).read_unversioned_row()
            states[name].load(
                row_key,
                ExternalState(
                    reset=state_item.reset,
                    state=Payload(state_value, state_schema),
                ),
            )
    return states


def internal_states_to_proto(states_holder: StatesHolder, TState, TStateItem):
    """Map internal StatesHolder to proto TState."""
    state = TState()
    state.name = states_holder.name
    writer = WireProtocolWriter()
    for row_key, state_val in states_holder.modified_items():
        item = TStateItem()
        item.key = writer.write_unversioned_row(row_key)
        item.reset = state_val.reset
        if not state_val.reset and state_val.state is not None:
            item.state = state_val.state
        state.stateItems.append(item)
    return state


def external_states_to_proto(states_holder: StatesHolder, TState, TStateItem):
    """Map external StatesHolder to proto TState."""
    state = TState()
    state.name = states_holder.name
    writer = WireProtocolWriter()
    for row_key, state_val in states_holder.modified_items():
        item = TStateItem()
        item.key = writer.write_unversioned_row(row_key)
        item.reset = state_val.reset
        if not state_val.reset and state_val.state is not None:
            item.state = writer.write_unversioned_row(state_val.state.row)
        state.stateItems.append(item)
    return state


# ---------- High-level Mapping Functions ----------


def map_process_batch_request(request, job: Job, streams_context: FlowStreamsContext) -> RequestContext:
    """Map TReqProcessBatch to RequestContext."""
    job_id = _guid_to_str(request.request_id)
    request_id = _guid_to_str(request.request_id)
    key_schema = job.group_by_schema
    stream_specs = job.stream_specs

    # Streams overrides for source computation.
    stream_specs_override = None
    if len(request.streams) > 0:
        stream_specs_override = streams_from_proto(request.streams, streams_context)
        stream_specs = stream_specs_override

    messages = [extended_message_from_proto(pm, key_schema, stream_specs) for pm in request.messages]

    timers = [timer_from_proto(pt, key_schema) for pt in request.timers]

    visits = [visit_from_proto(pv, key_schema) for pv in request.visits]

    internal_states = internal_states_from_proto(request.internal_states, key_schema)
    external_states = external_states_from_proto(request.external_states, job_id, request_id, key_schema)
    joined_external_states = joined_external_states_from_proto(
        request.joined_external_states, job_id, request_id, key_schema
    )

    watermarks = {}
    min_watermark = 2**63 - 1
    for pw in request.watermarks:
        if pw.watermark < min_watermark:
            min_watermark = pw.watermark
        watermarks[pw.stream_id] = pw.watermark

    return RequestContext(
        job_id=job_id,
        request_id=request_id,
        computation_id=request.computation_id,
        messages=messages,
        timers=timers,
        visits=visits,
        stream_specs=stream_specs,
        internal_states=internal_states,
        external_states=external_states,
        joined_external_states=joined_external_states,
        watermarks=watermarks,
        min_watermark=min_watermark,
        job=job,
        stream_specs_override=stream_specs_override,
    )


def map_process_batch_response(stream_specs: StreamSpecs, response: ResponseContext, proto_module):
    """Map ResponseContext to TResponseData proto."""
    TResponseData = proto_module.TResponseData
    TMessage = proto_module.TMessage
    TNewTimerProto = proto_module.TNewTimer
    TState = proto_module.TState
    TStateItem = proto_module.TStateItem

    data = TResponseData()

    for transform_result in response.transform_results:
        group = TResponseData.TGroup()

        for parent_id in transform_result.parent_ids:
            group.parent_ids.append(parent_id.encode("utf-8"))

        if not group.parent_ids:
            raise ValueError("Parent ids are empty")

        for msg in transform_result.messages:
            group.messages.append(message_to_proto(msg, stream_specs, TMessage))

        for distribute in transform_result.distribute:
            group.distribute.append(distribute)

        for timer in transform_result.timers:
            group.timers.append(timer_to_proto(timer, TNewTimerProto))

        data.output.append(group)

    # Only states changed via accessors are sent back; skip holders with no modifications.
    for states_holder in response.internal_states.values():
        if states_holder.has_modified():
            data.internal_states.append(internal_states_to_proto(states_holder, TState, TStateItem))

    for states_holder in response.external_states.values():
        if states_holder.has_modified():
            data.external_states.append(external_states_to_proto(states_holder, TState, TStateItem))

    return data


def map_put_job_request(request, streams_context: FlowStreamsContext) -> Job:
    """Map TReqPutJob to Job."""
    job_id = _guid_to_str(request.job_id)
    computation_id = request.computation_id
    return job_from_proto_job_info(job_id, computation_id, request.job_info, streams_context)


def job_from_proto_job_info(
    job_id: str,
    computation_id: str,
    job_info,
    streams_context: FlowStreamsContext,
) -> Job:
    """Create a Job from proto TJobInfo."""
    stream_specs = streams_from_proto(job_info.streams, streams_context)
    static_spec = _yson_from_proto(job_info.spec)
    dynamic_spec = _yson_from_proto(job_info.dynamic_spec)

    group_by_schema = EMPTY_SCHEMA
    if isinstance(static_spec, dict):
        group_by_key = static_spec.get("group_by_schema")
        if group_by_key is not None:
            group_by_schema = _table_schema_from_yson(group_by_key)

    return Job(
        job_id=job_id,
        computation_id=computation_id,
        stream_specs=stream_specs,
        static_spec=static_spec,
        dynamic_spec=dynamic_spec,
        group_by_schema=group_by_schema,
    )
