"""Parent supervisor that pre-forks N companion interpreters behind one address.

All N children bind the same gRPC data address with ``grpc.so_reuseport=1``; the
kernel load-balances incoming RPCs across them, giving real per-process GIL
parallelism. The parent has no RPC role: it just forks, ``waitpid``-s on dead
children, and re-spawns them.
"""

import errno
import logging
import os
import signal
import socket
import threading
import time

from .context import PipelineContext
from .job import JobContext
from .server import DEFAULT_SHUTDOWN_TIMEOUT_SECONDS
from .worker import DEFAULT_IO_THREADS, build_worker_server

log = logging.getLogger(__name__)

# Default poll cadence of the supervisor loop.
DEFAULT_SUPERVISE_INTERVAL_SECONDS = 5.0

# Per-child restart backoff: a crash-looping companion process is re-forked with growing
# delay, and after too many crashes in a short window the supervisor gives up so the C++
# process manager (with its own TBackoffStrategy) restarts the whole companion.
# Consistency invariant: the window must exceed the worst-case sum of the THRESHOLD - 1
# backoff gaps (0.5 + 1 + 2 + 4 + 8 + 16 + 3 * 30 = 121.5s) plus poll-cadence slop,
# otherwise the give-up ceiling is unreachable and a dying child is re-forked forever.
RESTART_BACKOFF_BASE_SECONDS = 0.5
RESTART_BACKOFF_MAX_SECONDS = 30.0
CRASH_LOOP_THRESHOLD = 10
CRASH_LOOP_WINDOW_SECONDS = 300.0

# Honour SIGTERM cleanly when the supervisor is asked to stop.
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


class CrashLoopError(RuntimeError):
    """Raised when a companion process crash-loops beyond the supervisor's give-up ceiling."""


class CompanionProcessSupervisor:
    """N stateless companion interpreters sharing a single SO_REUSEPORT address.

    ``spawn_child(idx)`` is injected so this is testable without forking; each
    child handle must expose ``is_alive() -> bool`` and ``stop()`` for graceful
    teardown. Dead children are re-forked with per-child exponential backoff, and a
    crash loop (too many restarts of one slot in a short window) raises
    ``CrashLoopError`` so the caller can exit non-zero.
    """

    def __init__(
        self,
        n,
        spawn_child,
        *,
        backoff_base=RESTART_BACKOFF_BASE_SECONDS,
        backoff_max=RESTART_BACKOFF_MAX_SECONDS,
        crash_loop_threshold=CRASH_LOOP_THRESHOLD,
        crash_loop_window=CRASH_LOOP_WINDOW_SECONDS,
        clock=time.monotonic,
    ):
        if n < 1:
            raise ValueError(f"CompanionProcessSupervisor requires n >= 1, got {n}")
        self._n = n
        self._spawn_child = spawn_child
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._crash_loop_threshold = crash_loop_threshold
        self._crash_loop_window = crash_loop_window
        self._clock = clock
        self.children = []
        # Per-slot restart bookkeeping: timestamps of recent restarts and the next
        # monotonic time the slot is allowed to be re-forked.
        self._restart_times = [[] for _ in range(n)]
        self._next_restart_at = [0.0] * n

    def start(self):
        self.children = [self._spawn_child(i) for i in range(self._n)]

    def reap_and_restart(self):
        now = self._clock()
        for i, child in enumerate(self.children):
            if child.is_alive() or now < self._next_restart_at[i]:
                continue
            self._note_restart(i, now)
            log.info("Restarting dead companion process (Index: %d)", i)
            self.children[i] = self._spawn_child(i)

    def _note_restart(self, idx, now):
        times = self._restart_times[idx]
        times.append(now)
        del times[: max(0, len(times) - self._crash_loop_threshold)]
        recent = [t for t in times if now - t < self._crash_loop_window]
        if len(recent) >= self._crash_loop_threshold:
            raise CrashLoopError(
                f"Companion process (Index: {idx}) crashed {len(recent)} times in "
                f"{self._crash_loop_window:.0f}s; giving up"
            )
        # Exponential backoff in the number of recent crashes for this slot.
        delay = min(self._backoff_max, self._backoff_base * (2 ** (len(recent) - 1)))
        self._next_restart_at[idx] = now + delay

    def stop(self):
        for child in self.children:
            stop = getattr(child, "stop", None)
            if stop is not None:
                stop()


class _ForkedChild:
    """Handle for a real os.fork()-ed companion process living in the parent."""

    def __init__(self, pid):
        self.pid = pid
        self._exited = False

    def is_alive(self):
        if self._exited:
            return False
        try:
            wpid, _ = os.waitpid(self.pid, os.WNOHANG)
        except ChildProcessError:
            self._exited = True
            return False
        if wpid == self.pid:
            self._exited = True
            return False
        return True

    def stop(self):
        if self._exited:
            return
        try:
            os.kill(self.pid, signal.SIGTERM)
        except OSError as ex:
            if ex.errno != errno.ESRCH:
                log.warning("Failed to SIGTERM companion process (Pid: %d): %s", self.pid, ex)


def _bind_data_port(requested_port):
    """Resolve the requested port to a concrete one shared across the N children.

    A user-supplied ``0`` (ephemeral) must be turned into a fixed port before fork,
    otherwise the N children would pick different ports.
    """
    if requested_port:
        return requested_port
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        s.bind(("::", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _run_companion_child(address, pipeline_context, job_context):
    """Body of a forked companion process: build the gRPC server and block on it.

    Runs in the child after ``os.fork()``; never returns to the caller.
    """
    server = build_worker_server(
        address=address,
        pipeline_context=pipeline_context,
        job_context=job_context,
        io_threads=DEFAULT_IO_THREADS,
    )

    def _terminate(signum, frame):
        log.info("Companion process received signal %s, stopping", signum)
        # ``server.stop`` is non-blocking and returns an Event signalled once the grace
        # period drains in-flight RPCs — wait on it before exiting. Same grace as the
        # single-process path so shutdown semantics do not depend on the fan-out mode.
        drained = server.stop(grace=DEFAULT_SHUTDOWN_TIMEOUT_SECONDS)
        drained.wait()
        os._exit(0)

    for sig in _STOP_SIGNALS:
        signal.signal(sig, _terminate)
    # Deliver any stop signal that arrived while blocked across the fork (see spawn()).
    signal.pthread_sigmask(signal.SIG_UNBLOCK, _STOP_SIGNALS)

    server.start()
    server.wait_for_termination()
    os._exit(0)


def serve_parent(
    pipeline_context: PipelineContext,
    *,
    port: int,
    companion_process_count: int,
    job_ttl_seconds: int,
):
    """Pre-fork ``companion_process_count`` companion interpreters and supervise them.

    ``companion_process_count`` is the already-resolved process count (the caller owns
    the single ``resolve_companion_process_count`` site). Blocks until SIGTERM/SIGINT,
    or raises ``CrashLoopError`` if a child crash-loops past the give-up ceiling so the
    C++ process manager restarts the whole companion. The pipeline_context must already
    be frozen by the caller (we fork it as-is so every child has the user pipeline
    registered). Returns the data port the children are bound to (useful for testing).
    """
    n = companion_process_count
    actual_port = _bind_data_port(port)
    address = f"[::]:{actual_port}"

    log.info("Starting companion process fan-out (Port: %d, N: %d)", actual_port, n)

    def spawn(idx):
        # Block stop signals across the fork: the child inherits the mask and unblocks
        # only after its own SIGTERM handler is installed (_run_companion_child), so a
        # stop() racing the fork cannot be swallowed while the gRPC server is still
        # being built — that would leave an orphan child serving the port forever.
        signal.pthread_sigmask(signal.SIG_BLOCK, _STOP_SIGNALS)
        pid = os.fork()
        if pid == 0:
            # Child: build per-child JobContext (every interpreter has its own cache).
            child_job_context = JobContext(job_ttl_seconds)
            try:
                _run_companion_child(address, pipeline_context, child_job_context)
            except BaseException:
                log.exception("Companion process crashed (Index: %d)", idx)
                os._exit(1)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, _STOP_SIGNALS)
        return _ForkedChild(pid)

    supervisor = CompanionProcessSupervisor(n=n, spawn_child=spawn)

    # SIGTERM/SIGINT set this event; the supervise loop waits on it so shutdown is
    # prompt instead of lagging a full poll interval (mirrors the single-process path
    # in server.py).
    stop_event = threading.Event()

    def _on_stop(signum, frame):
        log.info("Companion parent received signal %s, stopping children", signum)
        stop_event.set()

    for sig in _STOP_SIGNALS:
        signal.signal(sig, _on_stop)

    supervisor.start()
    try:
        while not stop_event.wait(DEFAULT_SUPERVISE_INTERVAL_SECONDS):
            supervisor.reap_and_restart()
    finally:
        supervisor.stop()

    return actual_port
