"""
Context classes: PipelineContext, RuntimeContext, DefaultRuntimeContext,
StateAccessor, ResponseContext, RequestContext.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from .computation import TransformResult
from .row import (
    ExtendedMessage,
    MessageBuilder,
    Payload,
    TableSchema,
    Timer,
    Visit,
    EMPTY_SCHEMA,
)
from .state import (
    ExternalState,
    EXTERNAL_STATE_RESET,
    State,
    STATE_RESET,
    StatesHolder,
)
from .stream import FlowStream, FlowStreamsContext, StreamSpecs

log = logging.getLogger(__name__)


def _validate_external_state_name(name: str) -> None:
    """Validate an external state name.

    External state names must mirror the keys used in ``external_state_managers``
    of the pipeline spec: absolute paths starting with ``/``, non-empty, not
    ending with ``/``, not equal to ``/`` and without two adjacent ``/``.
    """
    if not isinstance(name, str):
        raise TypeError(f"External state name must be a str, got {type(name).__name__}")
    if not name:
        raise ValueError("External state name is empty")
    if name == "/":
        raise ValueError(f"External state name is root: {name!r}")
    if not name.startswith("/"):
        raise ValueError(f"External state name does not start with '/': {name!r}")
    if name.endswith("/"):
        raise ValueError(f"External state name ends with '/': {name!r}")
    pos = name.find("//")
    if pos != -1:
        raise ValueError(f"External state name contains two adjacent '/' at position {pos}: {name!r}")


# ---------- State Accessors ----------


class StateAccessor:
    """Generic state accessor."""

    def __init__(self, key: Payload, states_holder: StatesHolder):
        self._key = key
        self._states_holder = states_holder

    def _get_row_key(self):
        return self._key.row if self._key else None


class RawStateAccessor(StateAccessor):
    """State accessor for raw bytes."""

    def get(self) -> Optional[bytes]:
        key = self._get_row_key()
        if key is None:
            return None
        state = self._states_holder.get(key)
        if state is None or state.reset:
            return None
        return state.state

    def set(self, value: bytes):
        key = self._get_row_key()
        self._states_holder.set(key, State(reset=False, state=value))

    def clear(self):
        key = self._get_row_key()
        self._states_holder.set(key, STATE_RESET)

    def get_or_default(self, default: bytes) -> bytes:
        result = self.get()
        return result if result is not None else default


class YsonStateAccessor(StateAccessor):
    """State accessor for YSON-encoded dict values."""

    def get(self) -> Optional[Any]:
        import yt.yson as yson

        key = self._get_row_key()
        if key is None:
            return None
        state = self._states_holder.get(key)
        if state is None or state.reset:
            return None
        if state.state is None:
            return None
        return yson.loads(state.state)

    def set(self, value: Any):
        import yt.yson as yson

        key = self._get_row_key()
        data = yson.dumps(value) if not isinstance(value, bytes) else value
        self._states_holder.set(key, State(reset=False, state=data))

    def clear(self):
        key = self._get_row_key()
        self._states_holder.set(key, STATE_RESET)

    def get_or_default(self, default: Any) -> Any:
        result = self.get()
        return result if result is not None else default


class ProtoStateAccessor(StateAccessor):
    """State accessor for protobuf message values."""

    def __init__(self, key, states_holder, proto_class):
        super().__init__(key, states_holder)
        self._proto_class = proto_class

    def get(self):
        key = self._get_row_key()
        if key is None:
            return None
        state = self._states_holder.get(key)
        if state is None or state.reset:
            return None
        if state.state is None:
            return None
        msg = self._proto_class()
        msg.ParseFromString(state.state)
        return msg

    def set(self, value):
        key = self._get_row_key()
        data = value.SerializeToString()
        self._states_holder.set(key, State(reset=False, state=data))

    def clear(self):
        key = self._get_row_key()
        self._states_holder.set(key, STATE_RESET)

    def get_or_default(self, default=None):
        result = self.get()
        if result is not None:
            return result
        if default is not None:
            return default
        return self._proto_class()


class ExternalStateAccessor(Payload):
    """Accessor for external state that behaves as a Payload.

    Returned by ``ctx.external_state(name, message)``.  You can read
    column values directly (``state.get("count")``, ``state["count"]``,
    ``state.to_builder()``) and persist changes with ``state.set(payload)``
    or ``state.clear()``.
    """

    def __init__(self, states_holder: StatesHolder, key: Payload):
        self._states_holder = states_holder
        self._row_key = key.row if key else None
        # Resolve current payload.
        payload = self._resolve()
        super().__init__(payload.row, payload.schema)

    def _resolve(self) -> Payload:
        if self._row_key is not None:
            state = self._states_holder.get(self._row_key)
            if state is not None and not state.reset and state.state is not None:
                return state.state
        from .row import PayloadBuilder

        return PayloadBuilder(self._states_holder.state_schema).finish()

    def set(self, value: Payload):
        self._states_holder.set(self._row_key, ExternalState(reset=False, state=value))

    def clear(self):
        self._states_holder.set(self._row_key, EXTERNAL_STATE_RESET)


class ReadOnlyExternalStateError(RuntimeError):
    """Raised when code tries to mutate a joined external state."""


class ReadOnlyExternalStateAccessor(ExternalStateAccessor):
    """Read-only accessor for external state joined from another computation.

    Returned by ``ctx.joined_external_state(name, message)``.  Reads behave
    exactly like :class:`ExternalStateAccessor` (``get``, ``__getitem__``,
    ``get_or_default``, returning an empty Payload with the joined schema when
    no row was joined), but any write is rejected: joiners never write back.
    """

    def set(self, value: Payload):
        raise ReadOnlyExternalStateError("joined external state is read-only; joiners never write back")

    def clear(self):
        raise ReadOnlyExternalStateError("joined external state is read-only; joiners never write back")


# ---------- RuntimeContext ----------


class DefaultRuntimeContext:
    """Default implementation of RuntimeContext."""

    def __init__(
        self,
        internal_state_names: Set[str],
        stream_specs: StreamSpecs,
        internal_states: Dict[str, StatesHolder],
        external_states: Dict[str, StatesHolder],
        watermarks: Dict[str, int],
        min_watermark: int,
        computation_parameters: Dict[str, Any],
        computation_dynamic_parameters: Dict[str, Any],
        key_schema: Optional[TableSchema] = None,
        joined_external_states: Optional[Dict[str, StatesHolder]] = None,
        joiner_state_names: Optional[Set[str]] = None,
    ):
        self._internal_state_names = internal_state_names
        self._stream_specs = stream_specs
        self._internal_states = internal_states
        self._external_states = external_states
        self._watermarks = watermarks
        self._min_watermark = min_watermark
        self._computation_parameters = computation_parameters
        self._computation_dynamic_parameters = computation_dynamic_parameters
        self._key_schema = key_schema or EMPTY_SCHEMA
        self._joined_external_states = joined_external_states or {}
        self._joiner_state_names = joiner_state_names or set()

    # --- Pythonic shorthand API ---

    def state(self, name: str, message_or_timer) -> YsonStateAccessor:
        """Shorthand for YSON state accessor: ctx.state("name", message)."""
        key = message_or_timer.key if hasattr(message_or_timer, "key") else None
        return YsonStateAccessor(key, self._get_or_create_state_holder(name))

    def raw_state(self, name: str, message_or_timer) -> RawStateAccessor:
        """Shorthand for raw bytes state accessor."""
        key = message_or_timer.key if hasattr(message_or_timer, "key") else None
        return RawStateAccessor(key, self._get_or_create_state_holder(name))

    def proto_state(self, name: str, message_or_timer, proto_class) -> ProtoStateAccessor:
        """Shorthand for protobuf state accessor."""
        key = message_or_timer.key if hasattr(message_or_timer, "key") else None
        return ProtoStateAccessor(key, self._get_or_create_state_holder(name), proto_class)

    def external_state(self, name: str, message_or_timer) -> ExternalStateAccessor:
        """Shorthand for external state accessor."""
        _validate_external_state_name(name)
        key = message_or_timer.key if hasattr(message_or_timer, "key") else None
        states_holder = self._external_states.get(name)
        if states_holder is None:
            raise ValueError(f"External state {name} not found")
        return ExternalStateAccessor(states_holder, key)

    def joined_external_state(self, name: str, message_or_timer) -> ReadOnlyExternalStateAccessor:
        """Shorthand for read-only joined external state accessor."""
        _validate_external_state_name(name)
        self._validate_joiner_state_name(name)
        key = message_or_timer.key if hasattr(message_or_timer, "key") else None
        states_holder = self._joined_external_states.get(name)
        if states_holder is None:
            raise ValueError(f"Joined external state {name} not found")
        return ReadOnlyExternalStateAccessor(states_holder, key)

    @property
    def parameters(self) -> Dict[str, Any]:
        """Computation parameters."""
        return self._computation_parameters

    @property
    def dynamic_parameters(self) -> Dict[str, Any]:
        """Computation dynamic parameters."""
        return self._computation_dynamic_parameters

    @property
    def min_watermark(self) -> int:
        """Minimum input event watermark across all streams."""
        return self._min_watermark

    def watermark(self, stream_id: str) -> Optional[int]:
        """Event watermark for a specific stream."""
        return self._watermarks.get(stream_id)

    def message_builder(self, stream_id: str) -> MessageBuilder:
        """Create a MessageBuilder for the given stream."""
        stream = self._stream_specs.get_stream(stream_id)
        if stream is None:
            raise ValueError(f"Unknown streamId: {stream_id}")
        return MessageBuilder(stream_id, stream.schema)

    @property
    def stream_specs(self) -> StreamSpecs:
        """Access stream specs."""
        return self._stream_specs

    def _get_or_create_state_holder(self, state_name: str) -> StatesHolder:
        self._validate_internal_state_name(state_name)
        states_holder = self._internal_states.get(state_name)
        if states_holder is None:
            log.debug("Creating new state for name: %s", state_name)
            states_holder = StatesHolder(state_name, self._key_schema, None)
            self._internal_states[state_name] = states_holder
        return states_holder

    def _validate_internal_state_name(self, state_name: str):
        if state_name not in self._internal_state_names:
            raise ValueError(
                f"State must be configured at computation static spec parameters (StateName: {state_name})"
            )

    def _validate_joiner_state_name(self, state_name: str):
        if state_name not in self._joiner_state_names:
            raise ValueError(
                f"Joined external state must be configured at computation static spec "
                f"external_state_joiners (StateName: {state_name})"
            )


# ---------- RequestContext ----------


@dataclass
class RequestContext:
    """Full context for processing a request."""

    job_id: str = ""
    request_id: str = ""
    computation_id: str = ""
    messages: List[ExtendedMessage] = field(default_factory=list)
    timers: List[Timer] = field(default_factory=list)
    visits: List[Visit] = field(default_factory=list)
    stream_specs: Optional[StreamSpecs] = None
    internal_states: Dict[str, StatesHolder] = field(default_factory=dict)
    external_states: Dict[str, StatesHolder] = field(default_factory=dict)
    joined_external_states: Dict[str, StatesHolder] = field(default_factory=dict)
    watermarks: Dict[str, int] = field(default_factory=dict)
    min_watermark: int = 0
    job: Any = None
    stream_specs_override: Optional[StreamSpecs] = None


# ---------- ResponseContext ----------


@dataclass
class ResponseContext:
    """Response context with transform results and states."""

    job_id: str = ""
    request_id: str = ""
    transform_results: List[TransformResult] = field(default_factory=list)
    internal_states: Dict[str, StatesHolder] = field(default_factory=dict)
    external_states: Dict[str, StatesHolder] = field(default_factory=dict)


# ---------- PipelineContext ----------


class PipelineContext:
    """Central registry for all Computation and FlowStream instances within a pipeline."""

    _FROZEN_MSG = "PipelineContext is frozen; mutations after server start are not allowed"

    def __init__(self):
        self._computations: Dict[str, Any] = {}
        self._streams_context = FlowStreamsContext()
        self._frozen = False

    def register_computation(self, computation):
        if self._frozen:
            raise RuntimeError(self._FROZEN_MSG)
        if computation.computation_id in self._computations:
            raise ValueError(f"Computation {computation.computation_id} already exists")
        self._computations[computation.computation_id] = computation

    def register_stream(self, stream: FlowStream):
        if self._frozen:
            raise RuntimeError(self._FROZEN_MSG)
        if self._streams_context.get_stream(stream.stream_id) is not None:
            raise ValueError(f"Stream {stream.stream_id} already exists")
        self._streams_context.add_stream(stream.stream_id, stream)

    def _freeze(self) -> None:
        """Mark this context as read-only.

        Intended to be called by ``GrpcServerExecution.start()`` to prevent
        accidental mutation of the pipeline configuration after the server
        has begun serving requests.
        """
        self._frozen = True

    def get_computation(self, computation_id: str):
        return self._computations.get(computation_id)

    def get_stream_context(self) -> FlowStreamsContext:
        return self._streams_context

    def to_dict(self) -> dict:
        computations = {}
        for cid, comp in self._computations.items():
            computations[cid] = comp.to_dict()
        # PID of the worker interpreter serving this CompanionInfo call. With the
        # pre-fork supervisor each worker is a distinct process; surfacing the PID lets
        # callers observe which worker handled the request and verify fan-out.
        return {"computations": computations, "pid": os.getpid()}
