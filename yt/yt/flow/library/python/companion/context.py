"""
Context classes: PipelineContext, RuntimeContext, DefaultRuntimeContext,
StateAccessor, ResponseContext, RequestContext.
"""

import copy
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


class ReadOnlyStateError(RuntimeError):
    """Raised when code tries to write through a read-only state accessor."""


class StateAccessor:
    """Generic internal state accessor.

    The value returned by ``get()`` is live: for a given key it is decoded once per request, every
    accessor for that key returns the same object, and the changes made to that object in place
    are written back at the end of the request without a ``set()`` call.  A value that re-encodes
    to the bytes it arrived with is not sent back, the default of ``get_or_default()`` included.
    ``read_only()`` returns the untracked view.
    """

    def __init__(self, key: Payload, states_holder: StatesHolder, read_only: bool = False):
        self._key = key
        self._states_holder = states_holder
        self._read_only = read_only

    def set(self, value: Any) -> None:
        """Store |value| under the accessor's key.

        The stored object stays live: the changes made to it in place afterwards are written back
        as well.  A value that encodes to no bytes is no value and removes the state.
        """
        self._store(self._entry(value))

    def clear(self) -> None:
        """Remove the state stored under the accessor's key."""
        # A fresh entry per key: a State memoizes the value handed out, and one shared across
        # keys would let them share that memo.
        self._store(State(reset=True))

    def get_or_default(self, default: Any) -> Any:
        """Value stored under the accessor's key, or |default|.

        The default is attached to the key, not written: it can be changed in place right away,
        and it becomes the state value only once it is changed.  A ``None`` default carries no
        value and is not attached, and neither is the default of an accessor that cannot write.
        """
        state = self._get_state()
        if state is not None:
            return self._read(state)
        default = self._default_value(default)
        if default is not None and self._is_writable():
            self._attach(default)
        return default

    def read_only(self) -> "StateAccessor":
        """Read-only view of this accessor.

        It returns the same object, but reading through it does not track the state for changes,
        ``get_or_default()`` does not create the state, and ``set()`` and ``clear()`` raise
        :class:`ReadOnlyStateError`.

        The value is shared, not a copy: a change made through this view still reaches the state
        when a writable accessor read the same key earlier in the request.
        """
        if self._read_only:
            return self
        view = copy.copy(self)
        view._read_only = True
        return view

    def _get_row_key(self):
        return self._key.row if self._key else None

    def _get_state(self) -> Optional[State]:
        """State entry stored under the accessor's key, or None when there is no value."""
        key = self._get_row_key()
        if key is None:
            return None
        state = self._states_holder.get(key)
        if state is None or state.reset or state.state is None:
            return None
        return state

    def _is_writable(self) -> bool:
        """Whether a value can be stored: a read-only view and a keyless accessor never store."""
        return not self._read_only and self._get_row_key() is not None

    def _read(self, state: State) -> Any:
        """Value of |state|, tracked for changes unless this accessor is read-only."""
        if self._read_only:
            return state.get_value(self._codec(), self._decode)
        return state.get_mutable_value(self._codec(), self._decode, self._encode)

    def _read_state(self) -> Any:
        """Value stored under the accessor's key, or None when the state is absent."""
        state = self._get_state()
        return self._read(state) if state is not None else None

    def _store(self, state: State) -> None:
        if self._read_only:
            raise ReadOnlyStateError(f"Internal state is read-only (StateName: {self._states_holder.name})")
        self._states_holder.set(self._get_row_key(), state)

    def _attach(self, value: Any) -> None:
        """Put |value| under the accessor's key as an unmodified state.

        The bytes are encoded right away and serve as the baseline the end-of-batch sweep
        compares against: an untouched value produces no write, one changed in place does.
        """
        self._states_holder.load(self._get_row_key(), self._entry(value))

    def _entry(self, value: Any) -> State:
        """State entry holding |value| together with the encoder that turns it back into bytes."""
        return State(state=self._encode(value), value=value, codec=self._codec(), encode=self._encode)

    def _default_value(self, default: Any) -> Any:
        """Value to attach when the state is absent, resolved only then."""
        return default

    def _codec(self) -> Any:
        """Identity of this accessor's encoding, telling whose value a state memoized."""
        return type(self)

    def _decode(self, data: bytes) -> Any:
        raise NotImplementedError

    def _encode(self, value: Any) -> bytes:
        raise NotImplementedError


class RawStateAccessor(StateAccessor):
    """State accessor for raw bytes.

    ``bytes`` is immutable, so a raw state cannot be changed in place; it is written by ``set()``.
    """

    def get(self) -> Optional[bytes]:
        """Bytes stored under the accessor's key."""
        return self._read_state()

    def get_or_default(self, default: bytes) -> bytes:
        """Bytes stored under the accessor's key, or |default|.

        The default is not even attached: ``bytes`` is immutable, so there is nothing to change
        in place afterwards and a raw state is written by ``set()`` only.
        """
        value = self._read_state()
        return value if value is not None else default

    def _read(self, state: State) -> Optional[bytes]:
        # Immutable bytes have nothing to track: a read never marks the state modified.
        return state.get_value(self._codec(), self._decode)

    @staticmethod
    def _decode(data: bytes) -> bytes:
        return data

    @staticmethod
    def _encode(value: bytes) -> bytes:
        return value


class YsonStateAccessor(StateAccessor):
    """State accessor for YSON-encoded dict values.

    A decoded value is mutable, so the changes made to it in place are written back; see
    :class:`StateAccessor`.  A ``bytes`` value handed to ``set()`` is taken as already
    YSON-encoded.
    """

    def get(self) -> Optional[Any]:
        """YSON-decoded value stored under the accessor's key."""
        return self._read_state()

    def _entry(self, value: Any) -> State:
        """Entry for |value|; a ``bytes`` value is taken as already YSON-encoded, and carries no
        encoder: there is nothing to change in place in it.
        """
        if isinstance(value, bytes):
            return State(state=value)
        return super()._entry(value)

    @staticmethod
    def _decode(data: bytes) -> Any:
        import yt.yson as yson

        return yson.loads(data)

    @staticmethod
    def _encode(value: Any) -> bytes:
        import yt.yson as yson

        return yson.dumps(value)


class ProtoStateAccessor(StateAccessor):
    """State accessor for protobuf message values.

    A message is mutable, so the changes made to it in place are written back; see
    :class:`StateAccessor`.
    """

    def __init__(self, key: Payload, states_holder: StatesHolder, proto_class, read_only: bool = False):
        super().__init__(key, states_holder, read_only)
        self._proto_class = proto_class

    def get(self):
        """Protobuf message stored under the accessor's key."""
        return self._read_state()

    def get_or_default(self, default=None):
        """Value stored under the accessor's key, |default|, or an empty message of the state's
        proto class; the default is attached as in :meth:`StateAccessor.get_or_default`.
        """
        return super().get_or_default(default)

    def _default_value(self, default):
        return default if default is not None else self._proto_class()

    def _codec(self):
        # Two accessors share a memoized message only when they decode the same proto class.
        return type(self), self._proto_class

    def _decode(self, data: bytes):
        message = self._proto_class()
        message.ParseFromString(data)
        return message

    @staticmethod
    def _encode(value) -> bytes:
        return value.SerializeToString()


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
        resources: Optional[Dict[str, Any]] = None,
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
        self._resources = resources or {}

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

    def get_resource(self, alias: str):
        """Companion-hosted resource by the alias from the computation's
        ``required_resource_ids`` entry (the resource id when no alias is set).

        Read it here on every call: the instance is served per batch, and one
        cached across batches keeps being used after the worker retired it and
        its unload hook ran.
        """
        resource = self._resources.get(alias)
        if resource is None:
            raise ValueError(
                f"Companion resource is not available in this process; companion-hosted "
                f"resources must be listed in the computation's required_resource_ids "
                f"(Alias: {alias})"
            )
        return resource

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
    resources: Dict[str, Any] = field(default_factory=dict)


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
        self._resource_factories: Dict[str, Any] = {}
        self._frozen = False

    def register_computation(self, computation):
        if self._frozen:
            raise RuntimeError(self._FROZEN_MSG)
        if computation.computation_id in self._computations:
            raise ValueError(f"Computation {computation.computation_id} already exists")
        self._computations[computation.computation_id] = computation

    def register_resource_class(self, resource_class_name: str, factory):
        """Register a companion resource class by the name the pipeline spec
        uses under the ``companion_resource_class`` parameter; ``factory`` is
        called without arguments to build a fresh instance per init.
        """
        if self._frozen:
            raise RuntimeError(self._FROZEN_MSG)
        if resource_class_name in self._resource_factories:
            raise ValueError(f"Resource class {resource_class_name} already exists")
        self._resource_factories[resource_class_name] = factory

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

    def get_resource_factories(self) -> Dict[str, Any]:
        return self._resource_factories

    def to_dict(self) -> dict:
        computations = {}
        for cid, comp in self._computations.items():
            computations[cid] = comp.to_dict()
        # PID of the worker interpreter serving this CompanionInfo call. With the
        # pre-fork supervisor each worker is a distinct process; surfacing the PID lets
        # callers observe which worker handled the request and verify fan-out.
        return {"computations": computations, "pid": os.getpid()}
