"""
Computation classes: Computation, SourceComputation, OutputCollector,
RootCollector, TransformResult, function protocols.
"""

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

from .row import ExtendedMessage, Message, NewTimer, Timer, Visit

# ---------- Internal constants ----------

_ROW = "Row"
_BATCH = "Batch"
_SOURCE = "Source"
_TRANSFORM = "Transform"


# ---------- Function protocols ----------


class RowFunction(ABC):
    """Per-message processing function."""

    @abstractmethod
    def on_message(self, message: ExtendedMessage, output: "OutputCollector", ctx: Any): ...

    def on_timer(self, timer: Timer, output: "OutputCollector", ctx: Any):
        """Default no-op timer handler."""
        pass

    def on_visit(self, visit: Visit, output: "OutputCollector", ctx: Any):
        """Default no-op key-visitor handler."""
        pass


class BatchFunction(ABC):
    """Batch processing function."""

    @abstractmethod
    def on_messages(self, messages: List[ExtendedMessage], output: "OutputCollector", ctx: Any): ...

    def on_timers(self, timers: List[Timer], output: "OutputCollector", ctx: Any):
        """Default no-op timer handler."""
        pass

    def on_visits(self, visits: List[Visit], output: "OutputCollector", ctx: Any):
        """Default no-op key-visitor handler."""
        pass


# ---------- TransformResult ----------


@dataclass
class TransformResult:
    """Result of a transformation: messages, timers, and parent IDs."""

    parent_ids: List[str] = field(default_factory=list)
    messages: List[Message] = field(default_factory=list)
    # Per-message distribute flag, kept in lockstep with messages.
    distribute: List[bool] = field(default_factory=list)
    timers: List[NewTimer] = field(default_factory=list)


# ---------- OutputCollector ----------


class OutputCollector(ABC):
    """Interface for collecting output messages."""

    @abstractmethod
    def add_message(self, message: Message, distribute: bool = True): ...

    def add_timer(self, trigger_timestamp: int, event_timestamp: int = 0, stream_id: Optional[str] = None): ...

    def set_parent_ids(self, parent_ids: List[str]) -> "OutputCollector": ...


class DefaultOutputCollector(OutputCollector):
    """Default implementation collecting into a TransformResult."""

    def __init__(self, root_collector: "RootCollector", result: TransformResult):
        self._root_collector = root_collector
        self._result = result

    def add_message(self, message: Message, distribute: bool = True):
        self._result.messages.append(message)
        self._result.distribute.append(distribute)

    def add_timer(self, trigger_timestamp: int, event_timestamp: int = 0, stream_id: Optional[str] = None):
        self._result.timers.append(
            NewTimer(
                trigger_timestamp=trigger_timestamp,
                event_timestamp=event_timestamp,
                stream_id=stream_id,
            )
        )

    def set_parent_ids(self, parent_ids: List[str]) -> OutputCollector:
        return self._root_collector.set_parent_ids(parent_ids)


class RootCollector:
    """Root collector managing child OutputCollectors and accumulating TransformResults."""

    def __init__(self):
        self._results: List[TransformResult] = []

    def set_parent_ids(self, parent_ids: Any) -> OutputCollector:
        """Create a new OutputCollector with given parent IDs.

        Args:
            parent_ids: Either a single string ID or a list of string IDs.
        """
        if isinstance(parent_ids, str):
            parent_ids = [parent_ids]
        result = TransformResult(parent_ids=list(parent_ids))
        self._results.append(result)
        return DefaultOutputCollector(self, result)

    def collect_results(self) -> List[TransformResult]:
        return self._results


# ---------- Callable classification ----------


def _classify_callable(fn):
    """Classify a callable and return (process_function, function_type).

    Accepts:
    - RowFunction/BatchFunction instances -> use as-is
    - Plain callable with first param 'message' -> wrap as RowFunction
    - Plain callable with first param 'messages' -> wrap as BatchFunction
    - Class with on_message -> instantiate and use as RowFunction
    """
    if isinstance(fn, (RowFunction, BatchFunction)):
        if isinstance(fn, RowFunction):
            return fn, _ROW
        return fn, _BATCH

    if isinstance(fn, type):
        # It's a class - check for on_message/on_messages
        if hasattr(fn, "on_message"):
            instance = fn()
            return instance, _ROW
        elif hasattr(fn, "on_messages"):
            instance = fn()
            return instance, _BATCH
        raise ValueError(f"Class {fn.__name__} must have on_message or on_messages method")

    if callable(fn):
        # Plain function - inspect first parameter name
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
        if not params:
            raise ValueError("Function must accept at least one parameter (message or messages)")

        first_param = params[0]
        if first_param == "messages":
            from ._api import _FunctionBatchAdapter

            return _FunctionBatchAdapter(fn), _BATCH
        else:
            from ._api import _FunctionRowAdapter

            return _FunctionRowAdapter(fn), _ROW

    raise ValueError(f"Unsupported callable type: {type(fn)}")


# ---------- Computation ----------


class Computation:
    """Basic building block of Flow Pipeline - a vertex in the execution graph."""

    def __init__(
        self,
        computation_id: str,
        process_function: Any = None,
        function_type: Optional[str] = None,
    ):
        if process_function is None:
            raise ValueError(
                "Computation requires a process function; " "creating a computation without one is not supported"
            )

        self.computation_id = computation_id

        if function_type is None:
            # Auto-classify: supports RowFunction, BatchFunction, plain callables
            if isinstance(process_function, RowFunction):
                self.process_function = process_function
                self.function_type = _ROW
            elif isinstance(process_function, BatchFunction):
                self.process_function = process_function
                self.function_type = _BATCH
            elif callable(process_function):
                self.process_function, self.function_type = _classify_callable(process_function)
            else:
                raise ValueError(f"Unsupported function type: {type(process_function)}")
        else:
            self.process_function = process_function
            self.function_type = function_type

    @property
    def computation_type(self) -> str:
        return _TRANSFORM

    @property
    def is_source(self) -> bool:
        return False

    def do_process(self, request_ctx: Any) -> Any:
        """Process an incoming request and return a ResponseContext."""
        from .context import DefaultRuntimeContext, ResponseContext

        job = request_ctx.job
        root_collector = RootCollector()

        # Initialize runtime context.
        runtime_ctx = DefaultRuntimeContext(
            internal_state_names=job.internal_state_names,
            stream_specs=request_ctx.stream_specs,
            internal_states=request_ctx.internal_states,
            external_states=request_ctx.external_states,
            watermarks=request_ctx.watermarks,
            min_watermark=request_ctx.min_watermark,
            computation_parameters=job.static_parameters,
            computation_dynamic_parameters=job.dynamic_parameters,
            key_schema=job.group_by_schema,
            joined_external_states=getattr(request_ctx, "joined_external_states", {}),
            joiner_state_names=job.joiner_state_names,
        )

        # Process messages.
        self._do_process_messages(
            request_ctx.messages,
            request_ctx.timers,
            getattr(request_ctx, "visits", []),
            root_collector,
            runtime_ctx,
        )

        # Filter empty results.
        filtered = [r for r in root_collector.collect_results() if r.messages or r.timers]

        return ResponseContext(
            job_id=request_ctx.job_id,
            request_id=request_ctx.request_id,
            transform_results=filtered,
            internal_states=request_ctx.internal_states,
            external_states=request_ctx.external_states,
        )

    def _do_process_messages(
        self,
        messages: List[ExtendedMessage],
        timers: List[Timer],
        visits: List[Visit],
        root_collector: RootCollector,
        ctx: Any,
    ):
        if self.function_type == _ROW:
            for message in messages:
                self.process_function.on_message(
                    message,
                    root_collector.set_parent_ids(message.message_id),
                    ctx,
                )
            for timer in timers:
                self.process_function.on_timer(
                    timer,
                    root_collector.set_parent_ids(timer.message_id),
                    ctx,
                )
            for visit in visits:
                self.process_function.on_visit(
                    visit,
                    root_collector.set_parent_ids(visit.message_id),
                    ctx,
                )
        elif self.function_type == _BATCH:
            if messages:
                parent_ids = [m.message_id for m in messages]
                self.process_function.on_messages(
                    messages,
                    root_collector.set_parent_ids(parent_ids),
                    ctx,
                )
            if timers:
                timer_parent_ids = [t.message_id for t in timers]
                self.process_function.on_timers(
                    timers,
                    root_collector.set_parent_ids(timer_parent_ids),
                    ctx,
                )
            if visits:
                visit_parent_ids = [v.message_id for v in visits]
                self.process_function.on_visits(
                    visits,
                    root_collector.set_parent_ids(visit_parent_ids),
                    ctx,
                )

    def to_dict(self) -> dict:
        return {
            "computation_id": self.computation_id,
            "computation_type": self.computation_type,
        }


class SourceComputation(Computation):
    """Source computation.

    A source computation consumes input messages and produces output messages.
    Like any :class:`Computation`, it requires a process function at
    construction time (enforced by the base class).
    """

    @property
    def computation_type(self) -> str:
        return _SOURCE

    @property
    def is_source(self) -> bool:
        return True
