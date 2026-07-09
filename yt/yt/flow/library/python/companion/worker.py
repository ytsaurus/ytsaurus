"""Single-interpreter worker gRPC server.

One companion process tree is N of these, all bound to the same address with
``SO_REUSEPORT``. Each one is a regular single-threaded GIL-bound interpreter; the
kernel load-balances incoming RPCs across them, giving real N-core parallelism.

The servicer is the existing ``CompanionServiceServicer`` — workers are stateless
across batches (durable state lives in the worker-side state API) and the existing
``RS_JOB_NOT_FOUND`` retry on the C++ client side rehydrates an unknown ``job_id``
inline via ``TReqProcessBatch.job_info`` on the next attempt.
"""

from concurrent import futures

import grpc

from .context import PipelineContext
from .job import JobContext
from .server import CompanionServiceServicer, _get_grpc_module, _try_add_health_servicer

# Per-worker RPC thread pool, deliberately smaller than the single-process server's 10:
# the total across the fan-out is N times this, and each worker serves only its share of
# per-computation connections, not the whole pipeline's.
DEFAULT_IO_THREADS = 4


def build_worker_server(
    address: str,
    pipeline_context: PipelineContext,
    job_context: JobContext,
    *,
    io_threads: int = DEFAULT_IO_THREADS,
    extra_options=None,
    server_factory=None,
) -> grpc.Server:
    """Build one worker's gRPC server, ready to ``start()``.

    Multiple servers built with this function on the same address can coexist because
    ``grpc.so_reuseport=1`` lets each child bind the port and the kernel spreads load.
    ``server_factory`` is the test seam for the ``grpc.server`` call.
    """
    options = [
        ("grpc.so_reuseport", 1),
        ("grpc.max_receive_message_length", -1),
    ]
    if extra_options:
        options.extend(extra_options)

    factory = server_factory if server_factory is not None else grpc.server
    server = factory(
        futures.ThreadPoolExecutor(max_workers=io_threads),
        options=options,
    )

    grpc_module = _get_grpc_module()
    servicer = CompanionServiceServicer(pipeline_context, job_context)
    grpc_module.add_CompanionServiceServicer_to_server(servicer, server)
    _try_add_health_servicer(server)

    # grpc reports a failed bind by returning 0, not raising; a silently unbound worker
    # would look alive to the supervisor while serving nothing.
    if server.add_insecure_port(address) == 0:
        raise RuntimeError(f"Failed to bind companion worker to {address}")
    return server
