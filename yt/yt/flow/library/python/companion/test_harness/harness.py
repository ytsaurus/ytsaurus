"""ComputationHarness — the core test harness for Flow pipeline unit tests."""

import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Set

from ..computation import Computation, SourceComputation, _classify_callable
from ..context import RequestContext
from ..job import Job
from ..row import (
    ExtendedMessage,
    Payload,
    PayloadBuilder,
    TableSchema,
    Timer,
    Visit,
    EMPTY_SCHEMA,
)
from ..state import ExternalState, State, StatesHolder
from ..stream import RawStream, StreamIdsMapping, StreamSpecs
from .response import ProcessResponse


def _patch_binary_values(payload: Payload, fields: dict):
    """Ensure non-UTF-8 bytes survive Payload.__getitem__ access.

    ``Payload.__getitem__`` auto-decodes ``bytes`` to ``str`` for STRING
    columns.  This crashes for raw protobuf data that isn't valid UTF-8.
    We store such values as ``bytearray`` instead of ``bytes`` so the
    ``isinstance(value, bytes)`` check in ``__getitem__`` is False and
    the raw binary data is returned as-is.
    """
    for name, value in fields.items():
        if not isinstance(value, bytes):
            continue
        col_id = payload.schema.find_column(name)
        if col_id is None:
            continue
        uv = payload.row.values[col_id]
        if not uv.type.is_string_like_type():
            continue
        try:
            value.decode("utf-8")
        except UnicodeDecodeError:
            uv.value = bytearray(value)


class ComputationHarness:
    """Test harness for Flow pipeline business logic.

    Mirrors Java's ``TestComputationHarness`` but uses Pythonic conventions:
    keyword arguments, context managers, pytest fixtures.

    Example::

        harness = ComputationHarness(
            WordCountMapper(),
            streams={"words": schema(word="string")},
            key_schema=schema(word="string"),
            internal_states={"word-state"},
        )
        msg = harness.build_message("words", key=key, word="hello")
        with harness.processing([msg]) as r:
            assert r.internal_state("word-state", key)[b"count"] == 1
    """

    def __init__(
        self,
        function=None,
        *,
        streams: Dict[str, TableSchema],
        key_schema: Optional[TableSchema] = None,
        internal_states: Optional[Set[str]] = None,
        external_states: Optional[Dict[str, TableSchema]] = None,
        joined_external_states: Optional[Dict[str, TableSchema]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        source: bool = False,
        computation_id: str = "test",
    ):
        self._computation_id = computation_id
        self._key_schema = key_schema or EMPTY_SCHEMA
        self._internal_state_names = internal_states or set()
        self._external_state_schemas = external_states or {}
        self._joined_external_state_schemas = joined_external_states or {}
        self._parameters = parameters or {}
        self._source = source

        # Build stream infrastructure.
        self._stream_schemas = streams
        mapping = StreamIdsMapping()
        stream_list = []
        for i, (stream_id, table_schema) in enumerate(streams.items()):
            mapping.add_mapping(stream_id, i)
            stream_list.append(RawStream(stream_id, table_schema))
        self._stream_specs = StreamSpecs(mapping, stream_list)

        # Build the computation.
        proc_fn, fn_type = _classify_callable(function)
        if source:
            self._computation = SourceComputation(computation_id, proc_fn, fn_type)
        else:
            self._computation = Computation(computation_id, proc_fn, fn_type)

    # -- Message / timer / key builders --

    def build_key(self, **fields) -> Payload:
        """Build a key Payload from keyword arguments using key_schema."""
        builder = PayloadBuilder(self._key_schema)
        for name, value in fields.items():
            builder.set(name, value)
        return builder.finish()

    def build_message(self, stream_id: str, *, key=None, event_timestamp=0, **fields) -> ExtendedMessage:
        """Build an ExtendedMessage for the given stream."""
        stream_schema = self._stream_schemas.get(stream_id)
        if stream_schema is None:
            raise ValueError(f"Unknown stream: {stream_id}")
        builder = PayloadBuilder(stream_schema)
        for name, value in fields.items():
            builder.set(name, value)
        payload = builder.finish()
        _patch_binary_values(payload, fields)
        spec_id = self._stream_specs.get_stream_spec_id(stream_id)
        return ExtendedMessage(
            message_id=str(uuid.uuid4()),
            stream_id=stream_id,
            stream_spec_id=spec_id if spec_id is not None else 0,
            event_timestamp=event_timestamp,
            payload=payload,
            key=key,
        )

    def build_timer(self, *, key, trigger_timestamp, stream_id="timer", event_timestamp=0) -> Timer:
        """Build a Timer."""
        return Timer(
            message_id=str(uuid.uuid4()),
            stream_id=stream_id,
            trigger_timestamp=trigger_timestamp,
            event_timestamp=event_timestamp,
            key=key,
        )

    def build_visit(self, *, key, stream_id="visit", event_timestamp=0) -> Visit:
        """Build a Visit (key-visitor event)."""
        return Visit(
            message_id=str(uuid.uuid4()),
            stream_id=stream_id,
            event_timestamp=event_timestamp,
            key=key,
        )

    # -- Processing --

    @contextmanager
    def processing(
        self,
        messages=None,
        timers=None,
        *,
        visits=None,
        watermarks=None,
        internal_states=None,
        external_states=None,
        joined_external_states=None,
    ):
        """Execute a processing step and yield the response for assertions.

        Args:
            messages: Input messages (list of ExtendedMessage).
            timers: Input timers (list of Timer).
            visits: Input key visits (list of Visit).
            watermarks: Per-stream watermarks (dict of stream_id -> int).
            internal_states: Pre-populated internal state.
                ``{state_name: {key_payload: value}}`` where value is
                State, bytes, dict (YSON-serialized), or protobuf message.
            external_states: Pre-populated external state.
                ``{state_name: {key_payload: value}}`` where value is
                ExternalState or Payload.
            joined_external_states: Pre-populated read-only joined external state.
                ``{state_name: {key_payload: value}}`` where value is
                ExternalState or Payload.

        Yields:
            ProcessResponse for assertions.
        """
        response_ctx, joined_holders = self._do_process(
            messages=messages or [],
            timers=timers or [],
            visits=visits or [],
            watermarks=watermarks or {},
            internal_states=internal_states or {},
            external_states=external_states or {},
            joined_external_states=joined_external_states or {},
        )
        yield ProcessResponse(response_ctx, joined_holders)

    # -- Internal --

    def _do_process(
        self,
        messages: List[ExtendedMessage],
        timers: List[Timer],
        visits: List[Any],
        watermarks: Dict[str, int],
        internal_states: Dict[str, Dict[Payload, Any]],
        external_states: Dict[str, Dict[Payload, Any]],
        joined_external_states: Dict[str, Dict[Payload, Any]],
    ):
        # Build static_spec for Job.
        static_params = dict(self._parameters)
        if self._internal_state_names:
            static_params["internal_states"] = list(self._internal_state_names)

        static_spec = {"parameters": static_params}
        if self._external_state_schemas:
            static_spec["external_state_managers"] = {name: {} for name in self._external_state_schemas}
        if self._joined_external_state_schemas:
            static_spec["external_state_joiners"] = {name: {} for name in self._joined_external_state_schemas}

        job = Job(
            job_id="test-job",
            computation_id=self._computation_id,
            stream_specs=self._stream_specs,
            static_spec=static_spec,
            group_by_schema=self._key_schema,
        )

        # Build internal StatesHolders and pre-populate.
        int_holders: Dict[str, StatesHolder] = {}
        for state_name in self._internal_state_names:
            holder = StatesHolder(state_name, self._key_schema, None)
            pre = internal_states.get(state_name, {})
            for key_payload, value in pre.items():
                state_obj = self._coerce_internal_state(value)
                holder.load(key_payload.row, state_obj)
            int_holders[state_name] = holder

        # Build external StatesHolders and pre-populate.
        ext_holders: Dict[str, StatesHolder] = {}
        for state_name, state_schema in self._external_state_schemas.items():
            holder = StatesHolder(state_name, self._key_schema, state_schema)
            pre = external_states.get(state_name, {})
            for key_payload, value in pre.items():
                state_obj = self._coerce_external_state(value)
                holder.load(key_payload.row, state_obj)
            ext_holders[state_name] = holder

        # Build read-only joined external StatesHolders and pre-populate via load().
        joined_holders: Dict[str, StatesHolder] = {}
        for state_name, state_schema in self._joined_external_state_schemas.items():
            holder = StatesHolder(state_name, self._key_schema, state_schema)
            pre = joined_external_states.get(state_name, {})
            for key_payload, value in pre.items():
                state_obj = self._coerce_external_state(value)
                holder.load(key_payload.row, state_obj)
            joined_holders[state_name] = holder

        # Compute min watermark.
        min_wm = min(watermarks.values()) if watermarks else 0

        request_ctx = RequestContext(
            job_id="test-job",
            request_id="test-request",
            computation_id=self._computation_id,
            messages=messages,
            timers=timers,
            visits=visits,
            stream_specs=self._stream_specs,
            internal_states=int_holders,
            external_states=ext_holders,
            joined_external_states=joined_holders,
            watermarks=watermarks,
            min_watermark=min_wm,
            job=job,
        )

        return self._computation.do_process(request_ctx), joined_holders

    @staticmethod
    def _coerce_internal_state(value) -> State:
        if isinstance(value, State):
            return value
        if isinstance(value, bytes):
            return State(reset=False, state=value)
        if isinstance(value, dict):
            import yt.yson as yson

            return State(reset=False, state=yson.dumps(value))
        # Assume protobuf message.
        if hasattr(value, "SerializeToString"):
            return State(reset=False, state=value.SerializeToString())
        raise TypeError(f"Cannot coerce {type(value)} to internal State")

    @staticmethod
    def _coerce_external_state(value) -> ExternalState:
        if isinstance(value, ExternalState):
            return value
        if isinstance(value, Payload):
            return ExternalState(reset=False, state=value)
        raise TypeError(f"Cannot coerce {type(value)} to ExternalState")
