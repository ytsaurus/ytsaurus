"""Pythonic facade for the Flow companion API."""

import os
import sys
from typing import Any, Callable, List, Optional

from .computation import (
    BatchFunction,
    Computation,
    RowFunction,
    SourceComputation,
    _classify_callable,
)
from .context import PipelineContext
from .row import ExtendedMessage
from .server import GrpcServerExecution

# ---------- Adapter classes ----------


class _FunctionRowAdapter(RowFunction):
    """Wraps a plain fn(message, output, ctx) as a RowFunction."""

    def __init__(self, fn: Callable):
        self._fn = fn

    def on_message(self, message: ExtendedMessage, output, ctx):
        self._fn(message, output, ctx)


class _FunctionBatchAdapter(BatchFunction):
    """Wraps a plain fn(messages, output, ctx) as a BatchFunction."""

    def __init__(self, fn: Callable):
        self._fn = fn

    def on_messages(self, messages: List[ExtendedMessage], output, ctx):
        self._fn(messages, output, ctx)


# ---------- Pipeline ----------


class Pipeline:
    """Unified entry point for building and running a Flow companion pipeline.

    Usage::

        pipeline = Pipeline()

        @pipeline.computation("mapper")
        def mapper(message, output, ctx):
            word = message.payload["word"]
            state = ctx.state("word-state", message)
            data = state.get_or_default({"word": word, "count": 0})
            data["count"] += 1
            state.set(data)

        pipeline.run()
    """

    def __init__(self):
        self._context = PipelineContext()
        self._execution: Optional[GrpcServerExecution] = None

    @property
    def context(self) -> PipelineContext:
        """Access underlying PipelineContext for advanced use."""
        return self._context

    def computation(self, computation_id: str, *, source: bool = False):
        """Decorator that registers a function or class as a computation.

        Args:
            computation_id: Unique identifier for the computation.
            source: If True, creates a SourceComputation.

        Returns:
            Decorator that registers and returns the original callable.
        """

        def decorator(fn):
            self.add(computation_id, fn, source=source)
            return fn

        return decorator

    def add(self, computation_id: str, fn: Any, *, source: bool = False):
        """Imperative registration of a computation.

        Args:
            computation_id: Unique identifier for the computation.
            fn: A callable, RowFunction, BatchFunction, or class with on_message/on_messages.
            source: If True, creates a SourceComputation.
        """
        process_fn, function_type = _classify_callable(fn)

        if source:
            comp = SourceComputation(
                computation_id=computation_id,
                process_function=process_fn,
                function_type=function_type,
            )
        else:
            comp = Computation(
                computation_id=computation_id,
                process_function=process_fn,
                function_type=function_type,
            )
        self._context.register_computation(comp)

    def run(self, *, port: Optional[int] = None, job_ttl: int = 600):
        """Entry point with two modes, selected by ``YT_FLOW_COMPANION_CONFIG``:

        * unset → host launch: enrich the pipeline spec to ship this binary as the companion and
          hand off to flow_server (``--config <pipeline.yson> --flow-bin <flow_server>``), which
          bootstraps the pipeline.
        * set → flow_server spawned us as the companion: run the gRPC companion server.
        """
        if "YT_FLOW_COMPANION_CONFIG" not in os.environ:
            from yt.yt.flow.library.python.runner import launch, parse_launch_args

            launch(*parse_launch_args(sys.argv))
            return
        self._execution = GrpcServerExecution(self._context, port=port, job_ttl=job_ttl)
        self._execution.start()

    def start(self, *, port: Optional[int] = None, job_ttl: int = 600):
        """Start the gRPC server in non-blocking mode."""
        self._execution = GrpcServerExecution(self._context, port=port, job_ttl=job_ttl)
        self._execution.start_async()

    def stop(self):
        """Stop the gRPC server."""
        if self._execution is not None:
            self._execution.stop()
            self._execution = None

    @property
    def port(self) -> Optional[int]:
        """Return the port the server is listening on, or None if not started."""
        if self._execution is not None:
            return self._execution.port
        return None
