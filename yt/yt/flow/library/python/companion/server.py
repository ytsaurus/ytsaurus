"""gRPC server for the CompanionService."""

import logging
import os
import signal
import threading
from concurrent import futures
from typing import Optional

import grpc

import yt.yson as yson

from .context import PipelineContext
from .job import JobContext
from .proto_mapper import _guid_parts_from_str
from .service import CompanionRequestProcessor

log = logging.getLogger(__name__)

DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30
# The shutdown budget is split: batches drain first, then the unload hooks run. Draining for
# the whole budget would leave the hooks to be killed rather than run.
DEFAULT_DRAIN_TIMEOUT_SECONDS = 20
DEFAULT_UNLOAD_TIMEOUT_SECONDS = DEFAULT_SHUTDOWN_TIMEOUT_SECONDS - DEFAULT_DRAIN_TIMEOUT_SECONDS
DEFAULT_COMPANION_PROCESS_COUNT = 0  # 0 = auto-size from cgroup CPU quota.


def _get_proto_module():
    """Import the generated proto module."""
    from ._proto_compat import ensure_proto_imports

    ensure_proto_imports()
    from yt.flow.library.cpp.companion.proto import (
        companion_service_pb2,
    )

    return companion_service_pb2


def _get_grpc_module():
    """Import the generated gRPC module."""
    from ._proto_compat import ensure_proto_imports

    ensure_proto_imports()
    from yt.flow.library.cpp.companion.proto import (
        companion_service_pb2_grpc,
    )

    return companion_service_pb2_grpc


class StoppingGeneration:
    """Handle on a stopped serving generation whose resources are not released yet."""

    def __init__(self, name):
        self._name = name
        self._done = threading.Event()

    def finish(self):
        self._done.set()

    def is_alive(self):
        return not self._done.is_set()

    def __str__(self):
        return self._name


# Process-wide, not per-execution: `Pipeline.stop()` drops its execution and `Pipeline.start()`
# builds a fresh one, so a per-instance guard would not see the generation still holding the
# previous store and its resources. The lock covers the whole transition — a start checks
# and starts under it, a stop captures and registers under it — so neither can slip into the
# other's window.
_stopping_generations = []
lifecycle_lock = threading.RLock()


def register_stopping_generation(token):
    """Records a generation that has not released its resources. Call under lifecycle_lock."""
    _stopping_generations.append(token)


def stopping_generations():
    """Generations still holding their resources, oldest first. Call under lifecycle_lock."""
    _stopping_generations[:] = [token for token in _stopping_generations if token.is_alive()]
    return list(_stopping_generations)


def shutdown_resources_within(servicer, timeout_seconds):
    """Release the servicer's resources, giving up the wait after ``timeout_seconds``.

    An unload hook is user code and may block; the process still has to stop within its
    budget, so the hook is left to a daemon thread that dies with the process rather than
    holding the stop path open until something kills it.

    Returns that thread when it is still running, so a caller that keeps the process alive
    can refuse to start a new generation beside the one still being released.
    """
    failure = []

    def run():
        try:
            servicer.shutdown()
        except BaseException as e:  # noqa: B036 - reported below; the process is stopping.
            failure.append(e)

    worker = threading.Thread(target=run, name="companion-resource-shutdown", daemon=True)
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        log.warning(
            "Companion resource shutdown did not finish in %ss; leaving it to the exiting process",
            timeout_seconds,
        )
        return worker
    if failure:
        log.warning("Companion resource shutdown failed", exc_info=failure[0])
    return None


def _try_add_health_servicer(server):
    """Best-effort registration of the standard gRPC health protocol (always SERVING)."""
    try:
        from grpc_health.v1 import health, health_pb2, health_pb2_grpc
    except ImportError:
        log.debug("Health check service not available (grpc_health not installed)")
        return
    health_servicer = health.HealthServicer()
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)


def _get_message_proto_module():
    """Import the generated message proto module."""
    from ._proto_compat import ensure_proto_imports

    ensure_proto_imports()
    from yt.flow.library.cpp.common.proto import message_pb2

    return message_pb2


class CompanionServiceServicer:
    """gRPC service implementation for communication with worker."""

    def __init__(self, pipeline_context: PipelineContext, job_context: JobContext):
        self._processor = CompanionRequestProcessor(pipeline_context, job_context)
        self._proto = _get_proto_module()
        self._msg_proto = _get_message_proto_module()

    def ProcessBatch(self, request, context):
        # An abandoned request must not register a job nobody will remove.
        if not context.is_active():
            context.set_code(grpc.StatusCode.CANCELLED)
            context.set_details("Request abandoned by the caller")
            return self._proto.TRspProcessBatch()
        try:
            result = self._processor.process_batch(request, self._build_proto_module())
            response = self._proto.TRspProcessBatch()
            response.request_id.CopyFrom(request.request_id)
            response.job_id.CopyFrom(request.job_id)
            response.status = self._status_to_enum(result["status"])

            if result["data"] is not None:
                response.data.CopyFrom(result["data"])

            response.metrics.cpu_time_ns = result["stats"].cpu_time_ns
            response.metrics.allocated_bytes = result["stats"].allocated_bytes

            return response
        except Exception as e:
            log.error("Error processing batch: %s", e, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error processing batch: {e}")
            return self._proto.TRspProcessBatch()

    def CompanionInfo(self, request, context):
        try:
            result = self._processor.get_companion_info()
            response = self._proto.TRspCompanionInfo()
            response.status = self._status_to_enum(result["status"])

            response.payload = yson.dumps(result["payload"])

            return response
        except Exception as e:
            log.error("Error processing CompanionInfo: %s", e, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error processing CompanionInfo: {e}")
            return self._proto.TRspCompanionInfo()

    def PutJob(self, request, context):
        # An abandoned request must not register a job nobody will remove.
        if not context.is_active():
            context.set_code(grpc.StatusCode.CANCELLED)
            context.set_details("Request abandoned by the caller")
            return self._proto.TRspPutJob()
        try:
            result = self._processor.put_job(request)
            response = self._proto.TRspPutJob()
            response.request_id.CopyFrom(request.request_id)
            response.job_id.CopyFrom(request.job_id)
            response.status = self._status_to_enum(result["status"])
            response.metrics.cpu_time_ns = result["stats"].cpu_time_ns
            response.metrics.allocated_bytes = result["stats"].allocated_bytes

            return response
        except Exception as e:
            log.error("Error processing PutJob: %s", e, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error processing PutJob: {e}")
            return self._proto.TRspPutJob()

    def RemoveJob(self, request, context):
        try:
            result = self._processor.remove_job(request)
            response = self._proto.TRspRemoveJob()
            response.request_id.CopyFrom(request.request_id)
            response.job_id.CopyFrom(request.job_id)
            response.status = self._status_to_enum(result["status"])

            return response
        except Exception as e:
            log.error("Error processing RemoveJob: %s", e, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error processing RemoveJob: {e}")
            return self._proto.TRspRemoveJob()

    def ListJobs(self, request, context):
        try:
            result = self._processor.list_jobs(request)
            response = self._proto.TRspListJobs()
            response.request_id.CopyFrom(request.request_id)
            for job_id in result["job_ids"]:
                first, second = _guid_parts_from_str(job_id)
                guid = response.job_ids.add()
                guid.first = first
                guid.second = second
            response.process_id = os.getpid()
            response.status = self._status_to_enum(result["status"])

            return response
        except Exception as e:
            log.error("Error processing ListJobs: %s", e, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error processing ListJobs: {e}")
            return self._proto.TRspListJobs()

    def ResourceExecute(self, request, context):
        try:
            # The store maps user-code failures to in-band statuses; exceptions
            # escape only on companion bugs, surfacing as an RPC error the
            # worker retries.
            outcome = self._processor.resource_execute(request)
            response = self._proto.TRspResourceExecute()
            response.request_id.CopyFrom(request.request_id)
            response.status = self._resource_status_to_enum(outcome.status)
            if outcome.error_message:
                response.error.code = 1
                response.error.message = outcome.error_message
            return response
        except Exception as e:
            log.error("Error processing ResourceExecute: %s", e, exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error processing ResourceExecute: {e}")
            # request_id and status are required fields: a default message
            # fails to serialize, and the worker would see an opaque
            # serialization error instead of the cause set just above.
            response = self._proto.TRspResourceExecute()
            response.request_id.CopyFrom(request.request_id)
            response.status = self._proto.RES_ERROR
            response.error.code = 1
            response.error.message = f"Error processing ResourceExecute: {e}"
            return response

    def shutdown(self):
        """Releases the resources this serving generation hosts."""
        self._processor.shutdown()

    def GetJfr(self, request, context):
        """JFR is only supported by the Java companion. Return UNIMPLEMENTED for Python."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("GetJfr is not supported by the Python companion")
        return self._proto.TRspGetJfr()

    def _status_to_enum(self, status_str: str) -> int:
        mapping = {
            "RS_OK": self._proto.RS_OK,
            "RS_ERROR": self._proto.RS_ERROR,
            "RS_JOB_NOT_FOUND": self._proto.RS_JOB_NOT_FOUND,
            "RS_RESOURCE_NOT_INITIALIZED": self._proto.RS_RESOURCE_NOT_INITIALIZED,
        }
        return mapping.get(status_str, self._proto.RS_ERROR)

    def _resource_status_to_enum(self, status_str: str) -> int:
        mapping = {
            "RES_OK": self._proto.RES_OK,
            "RES_ERROR": self._proto.RES_ERROR,
            "RES_RESOURCE_NOT_FOUND": self._proto.RES_RESOURCE_NOT_FOUND,
            "RES_RESOURCE_NOT_INITIALIZED": self._proto.RES_RESOURCE_NOT_INITIALIZED,
            "RES_UNSUPPORTED": self._proto.RES_UNSUPPORTED,
            "RES_STALE_RESOURCE_INCARNATION": self._proto.RES_STALE_RESOURCE_INCARNATION,
        }
        return mapping.get(status_str, self._proto.RES_ERROR)

    def _build_proto_module(self):
        """Build a module-like object with proto classes needed for response mapping."""

        class ProtoModule:
            pass

        pm = ProtoModule()
        pm.TResponseData = self._proto.TResponseData
        pm.TNewTimer = self._proto.TNewTimer
        pm.TState = self._proto.TState
        pm.TStateItem = self._proto.TStateItem
        pm.TMessage = self._msg_proto.TMessage
        return pm


def _load_companion_config_from_env() -> dict:
    """
    Parse the YT_FLOW_COMPANION_CONFIG environment variable (YSON text format).

    Companion startup contract:
      - YT_FLOW_MODE must equal "Worker".
      - YT_FLOW_COMPANION_CONFIG must be set (YSON text).

    If neither variable is set, this is treated as an embedded/test usage and an
    empty dict is returned. If either is set, the full contract is enforced.
    """
    mode = os.environ.get("YT_FLOW_MODE")
    raw = os.environ.get("YT_FLOW_COMPANION_CONFIG")

    # Embedded/test usage: no companion env vars at all — be lenient.
    if mode is None and not raw:
        return {}

    # Strict contract.
    if mode is None:
        raise ValueError("YT_FLOW_MODE environment variable is not set")
    if mode != "Worker":
        raise ValueError(f"Companion process started in non-worker mode: {mode}")
    if not raw:
        raise ValueError("YT_FLOW_COMPANION_CONFIG environment variable is not set")

    parsed = yson.loads(raw.encode("utf-8") if isinstance(raw, str) else raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"YT_FLOW_COMPANION_CONFIG must decode to a YSON map, got {type(parsed).__name__}")
    return parsed


class GrpcServerExecution:
    """Companion execution entry point. Starts gRPC server and serves requests from worker.

    The provided ``PipelineContext`` is a build-phase mutable container: all
    registrations must happen before the pipeline starts serving requests.
    Once the gRPC server is started (``GrpcServerExecution.start()``), the
    context is frozen — further ``register_computation()`` /
    ``register_stream()`` calls raise ``RuntimeError``. Read accessors
    (``get_computation``, ``get_stream_context``, ``to_dict``) remain
    available after freezing.
    """

    def __init__(
        self,
        pipeline_context: PipelineContext,
        port: Optional[int] = None,
        companion_process_count: Optional[int] = None,
    ):
        self._pipeline_context = pipeline_context
        self._port = port
        self._server: Optional[grpc.Server] = None
        self._servicer: Optional[CompanionServiceServicer] = None
        self._running = False
        self._lock = threading.Lock()

        # Load companion config from YT_FLOW_COMPANION_CONFIG (mirrors C++ side).
        companion_config = _load_companion_config_from_env()

        if self._port is None:
            if "port" in companion_config:
                self._port = int(companion_config["port"])
            else:
                self._port = 0

        if companion_process_count is None:
            companion_process_count = int(
                companion_config.get("companion_process_count", DEFAULT_COMPANION_PROCESS_COUNT)
            )
        self._companion_process_count = companion_process_count

        self._job_context = JobContext()

    def start(self):
        """Start gRPC server in blocking mode with signal handling.

        With ``companion_process_count`` resolving to >=2, fan out to N pre-forked
        single-interpreter companion processes behind one SO_REUSEPORT address (real
        multi-core parallelism). With 1, fall back to today's single-process server.
        """
        from .parent import serve_parent
        from .sizing import read_cpu_quota, resolve_companion_process_count

        n = resolve_companion_process_count(self._companion_process_count, read_cpu_quota())
        if n >= 2:
            # Fan-out mode is signal-stop-only: serve_parent blocks until SIGTERM/SIGINT,
            # so the start_async()/stop()/running contract does not apply on this branch.
            # Freeze BEFORE fork so every child sees the user's registered computations
            # and so subsequent registration attempts (rare) fail loudly.
            self._pipeline_context._freeze()
            self._port = serve_parent(
                pipeline_context=self._pipeline_context,
                port=self._port,
                companion_process_count=n,
            )
            return

        self.start_async()

        stop_event = threading.Event()

        def _signal_handler(signum, frame):
            log.info("Signal %s received, stopping gRPC server", signum)
            stop_event.set()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

        stop_event.wait()
        self.stop()

    def start_async(self):
        """Start gRPC server asynchronously (non-blocking)."""
        # Held across the check and the start: a stop registering in between would
        # otherwise let this generation come up beside one that is still draining.
        with lifecycle_lock, self._lock:
            if self._running:
                return
            stopping = stopping_generations()
            if stopping:
                # A stopped generation still holds its store and resources, either draining
                # or with unload hooks past their budget. Starting beside it would stack
                # generations, one per restart, with no way to reclaim the abandoned ones.
                raise RuntimeError(
                    "Cannot start the companion server: resources of a stopped generation "
                    f"are still being released ({', '.join(str(token) for token in stopping)})"
                )
            self._running = True
            self._start_server_internal()

    def _start_server_internal(self):
        log.info("Starting gRPC server on port %s", self._port)

        # Freeze the pipeline configuration: further register_computation() /
        # register_stream() calls on the PipelineContext will raise RuntimeError.
        self._pipeline_context._freeze()
        log.debug("Pipeline context frozen at server start")

        # Do not answer for jobs of a previous serving generation.
        self._job_context.clear()

        grpc_module = _get_grpc_module()

        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[("grpc.max_receive_message_length", -1)],
        )

        servicer = CompanionServiceServicer(self._pipeline_context, self._job_context)
        self._servicer = servicer
        grpc_module.add_CompanionServiceServicer_to_server(servicer, self._server)
        _try_add_health_servicer(self._server)

        actual_port = self._server.add_insecure_port(f"[::]:{self._port}")
        self._port = actual_port

        self._server.start()
        log.info("gRPC server started successfully on port %s", self._port)

    def stop(self):
        """Stop the gRPC server gracefully."""
        stopping = StoppingGeneration(f"port {self._port}")
        with lifecycle_lock, self._lock:
            if not self._running:
                return
            self._running = False
            # Taken together with the flag: a restart racing this stop installs its own
            # pair, and draining that one would leave the new server running against a
            # closed store while this generation is never released.
            server, self._server = self._server, None
            servicer, self._servicer = self._servicer, None
            # Published here, not after the drain: until this generation has let go of its
            # resources, no start may run beside it, and the drain below takes time.
            register_stopping_generation(stopping)

        log.info("Stopping gRPC server...")
        try:
            if server is not None:
                # stop() is non-blocking and returns an Event signalled once the
                # grace period drains in-flight RPCs; wait on it so the batches
                # still holding leases finish before the resources are released.
                server.stop(DEFAULT_DRAIN_TIMEOUT_SECONDS).wait()
            if servicer is not None:
                # Release the resources of the generation that just stopped, so
                # a restart does not leave their pools and threads running
                # beside their replacements.
                abandoned = shutdown_resources_within(servicer, DEFAULT_UNLOAD_TIMEOUT_SECONDS)
                if abandoned is not None:
                    # Hooks outran the budget: the generation keeps holding its store, so
                    # it stays registered until that thread finally lets go.
                    with lifecycle_lock:
                        register_stopping_generation(abandoned)
        finally:
            stopping.finish()
            log.info("gRPC server stopped")

    @property
    def port(self) -> int:
        return self._port

    @property
    def running(self) -> bool:
        return self._running
