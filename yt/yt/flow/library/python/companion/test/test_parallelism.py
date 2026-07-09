"""End-to-end CPU-parallelism tests for the pre-fork companion supervisor.

Each forked companion process records its PID per served RPC, so the tests can assert
which interpreters served the load. A single pooled channel is one connection that
SO_REUSEPORT pins to one process; fan-out across interpreters requires multiple
independent connections.
"""

import multiprocessing
import os
import signal
import time
from concurrent import futures

import pytest

import yt.yson as yson

from library.python.port_manager import PortManager

# Spawn (not fork) for the supervisor subprocess: by the time the fixture starts
# the supervisor, the test process has already imported gRPC and spun up its
# native threadpool — forking a multi-threaded gRPC process leaves the child
# with broken native state, and the worker forks inside it then segfault inside
# `_grpc_server._serve` (and the supervisor logs a flurry of
# "Restarting dead companion process"). Spawn re-bootstraps the subprocess with
# a clean interpreter; `_supervisor_target` builds the pipeline_context fresh
# in the subprocess so there is nothing to inherit.
if "spawn" in multiprocessing.get_all_start_methods():
    _MP_CONTEXT = multiprocessing.get_context("spawn")
else:
    _MP_CONTEXT = multiprocessing.get_context()

# Each worker drops a marker file named by its PID into this directory on every served
# RPC. The directory is handed to the subprocess via this env var (inherited across the
# worker forks), letting the test count distinct serving interpreters.
_WORKER_PID_DIR_ENV = "YT_FLOW_TEST_WORKER_PID_DIR"

# Per-message sleep in the CPU-bound transform, for tests that need an RPC to stay
# in flight long enough to race it against a worker shutdown.
_WORKER_SLEEP_ENV = "YT_FLOW_TEST_WORKER_SLEEP_SECONDS"


def _get_modules():
    from yt.yt.flow.library.python.companion._proto_compat import (
        ensure_proto_imports,
    )

    ensure_proto_imports()
    from yt.flow.library.cpp.companion.proto import (
        companion_service_pb2 as cs_pb2,
        companion_service_pb2_grpc as cs_grpc,
    )
    from yt.flow.library.cpp.common.proto import (
        message_pb2 as msg_pb2,
    )
    from yt_proto.yt.core.misc.proto import guid_pb2
    import grpc

    return cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc


def _make_cpu_pipeline_context():
    """Build a pipeline with a CPU-bound transform that holds the GIL and records the
    serving worker's PID."""
    from yt.yt.flow.library.python.companion.computation import (
        Computation,
        RowFunction,
    )
    from yt.yt.flow.library.python.companion.context import (
        PipelineContext,
    )
    from yt.yt.flow.library.python.companion.row import Message

    class CpuBoundFunction(RowFunction):
        # Number of iterations of a tight pure-python loop per message. Chosen so a
        # single batch takes ~tens of milliseconds — small enough that the test runs
        # quickly, large enough that wall-clock dominates over gRPC overhead.
        _ITERATIONS = 300_000

        def on_message(self, message, output, ctx):
            pid_dir = os.environ.get(_WORKER_PID_DIR_ENV)
            if pid_dir:
                # touch <pid_dir>/<pid> so the test can count distinct serving workers.
                open(os.path.join(pid_dir, str(os.getpid())), "a").close()
            sleep_seconds = float(os.environ.get(_WORKER_SLEEP_ENV, "0"))
            if sleep_seconds:
                time.sleep(sleep_seconds)
            acc = 0
            for i in range(self._ITERATIONS):
                acc = (acc + i * i) & 0xFFFFFFFF
            output.add_message(
                Message(
                    message_id=message.message_id,
                    stream_id=message.stream_id,
                    payload=message.payload,
                )
            )

    ctx = PipelineContext()
    ctx.register_computation(Computation(computation_id="cpu", process_function=CpuBoundFunction()))
    return ctx


def _supervisor_target(port, n, pid_dir, sleep_seconds):
    """Subprocess entrypoint: build the pipeline and serve. Readiness is detected
    by the parent via a real `CompanionInfo` RPC probe — no separate signal needed."""
    from yt.yt.flow.library.python.companion.parent import (
        serve_parent,
    )

    if pid_dir:
        os.environ[_WORKER_PID_DIR_ENV] = pid_dir
    if sleep_seconds:
        os.environ[_WORKER_SLEEP_ENV] = str(sleep_seconds)

    pipeline_context = _make_cpu_pipeline_context()
    pipeline_context._freeze()

    serve_parent(
        pipeline_context=pipeline_context,
        port=port,
        companion_process_count=n,
        job_ttl_seconds=600,
    )


class _CompanionFixture:
    """Loopback companion: ``serve_parent`` in a subprocess + ephemeral port."""

    def __init__(self, n, pid_dir=None, sleep_seconds=0.0):
        self.n = n
        self.pid_dir = pid_dir
        self.sleep_seconds = sleep_seconds
        # PortManager keeps the port reserved for the fixture's lifetime, so the forked
        # workers can claim it without a race against other tests on the same machine.
        self._port_manager = PortManager()
        self.port = self._port_manager.get_port()
        self.proc = None

    def start(self):
        self.proc = _MP_CONTEXT.Process(
            target=_supervisor_target,
            args=(self.port, self.n, self.pid_dir, self.sleep_seconds),
        )
        self.proc.start()

        # Probe readiness via a real `CompanionInfo` RPC. A bare TCP probe (port
        # listening) is not enough: gRPC server startup adds a window where the
        # kernel accepts the connection but the worker has not finished installing
        # the servicer, and the test client then hits a transient `UNAVAILABLE`.
        # The actual RPCs use ``wait_for_ready=True`` to absorb the residual race
        # where SO_REUSEPORT lands a fresh channel on a still-starting worker.
        cs_pb2, cs_grpc, _, _, grpc = _get_modules()
        deadline = time.time() + 60.0
        last_error = None
        while time.time() < deadline:
            try:
                with grpc.insecure_channel(f"ipv6:[::1]:{self.port}") as probe_channel:
                    stub = cs_grpc.CompanionServiceStub(probe_channel)
                    stub.CompanionInfo(cs_pb2.TReqCompanionInfo(), timeout=2.0)
                return self
            except grpc.RpcError as ex:
                last_error = ex
                time.sleep(0.1)
        raise RuntimeError(f"Companion port {self.port} never served for N={self.n}: {last_error}")

    def child_pids(self):
        """The supervisor's live worker PIDs, or None on non-Linux/proc-less hosts."""
        children_path = f"/proc/{self.proc.pid}/task/{self.proc.pid}/children"
        if not os.path.exists(children_path):
            return None
        with open(children_path, "r") as f:
            return [int(p) for p in f.read().split() if p.strip()]

    def stop(self):
        if self.proc is None:
            return
        if self.proc.is_alive():
            try:
                os.kill(self.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            self.proc.join(timeout=10)
        if self.proc.is_alive():
            self.proc.terminate()
            self.proc.join(timeout=5)
        self.proc = None
        self._port_manager.release()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()


def _make_process_batch_request(cs_pb2, msg_pb2, guid_pb2, *, job_id_seed, batch_index):
    request = cs_pb2.TReqProcessBatch()
    rid = guid_pb2.TGuid()
    rid.first = 0xCAFE0000 | batch_index
    rid.second = 0xC0DE0000 | batch_index
    request.request_id.CopyFrom(rid)

    jid = guid_pb2.TGuid()
    jid.first = job_id_seed
    jid.second = job_id_seed ^ 0xABCDEF
    request.job_id.CopyFrom(jid)

    request.computation_id = "cpu"

    stream = cs_pb2.TStream()
    stream.stream_id = "input"
    stream.stream_spec_id = 0
    stream.schema = b"[]"

    job_info = cs_pb2.TJobInfo()
    job_info.spec = b"{}"
    job_info.dynamic_spec = b"{}"
    job_info.streams.append(stream)
    request.job_info.CopyFrom(job_info)

    # Wire-protocol encoding of an empty UnversionedRow: version=0, value_count=0.
    empty_row = b"\x00\x00"

    # One CPU-bound message per batch keeps the test deterministic.
    msg = request.messages.add()
    msg.message.message_id = f"m-{batch_index}"
    msg.message.system_timestamp = 1
    msg.message.stream_spec_id = 0
    msg.message.payload = empty_row
    msg.key = empty_row

    return request


def _drive_pooled_batches(port, *, n_batches, parallelism, cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc):
    """Issue ``n_batches`` ProcessBatch RPCs concurrently over ONE pooled channel.

    A single channel is one connection, so SO_REUSEPORT (which hashes the connection
    4-tuple) pins all of its RPCs to a single worker interpreter — this helper is used
    only to check correctness/equivalence, NOT fan-out. Worker fan-out comes from
    multiple independent channels; see ``_drive_multi_channel_batches``. Returns the
    list of statuses.
    """
    # Explicit IPv6 loopback: the worker binds `[::]:port`. Sandbox hosts where
    # `localhost` resolves to 127.0.0.1 first (or where `IPV6_V6ONLY` is set on the
    # bound socket) used to hang the first RPC until the gRPC deadline expired.
    channel = grpc.insecure_channel(f"ipv6:[::1]:{port}")
    try:
        stub = cs_grpc.CompanionServiceStub(channel)

        def one_call(batch_index):
            request = _make_process_batch_request(
                cs_pb2,
                msg_pb2,
                guid_pb2,
                job_id_seed=0xDEAD0001,
                batch_index=batch_index,
            )
            response = stub.ProcessBatch(request, timeout=120.0, wait_for_ready=True)
            return response.status

        statuses = []
        with futures.ThreadPoolExecutor(max_workers=parallelism) as pool:
            for status in pool.map(one_call, range(n_batches)):
                statuses.append(status)
        return statuses
    finally:
        channel.close()


def _drive_multi_channel_batches(port, *, n_channels, batches_per_channel, cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc):
    """Issue batches over ``n_channels`` INDEPENDENT pooled channels concurrently.

    Each channel models a separate computation's own TCompanionClient (its own
    connection). SO_REUSEPORT distributes independent connections across the worker
    interpreters, so this is how production realizes companion CPU parallelism — one
    client per computation — without the per-call subchannel trick the old gate used.
    Returns (statuses, info_pids): the flat list of batch statuses and the set of
    worker PIDs reported by a per-channel ``CompanionInfo`` call (the ``pid`` payload
    field exists exactly so callers can observe which worker serves a connection).
    """
    # Each channel uses a LOCAL subchannel pool so it is a genuinely distinct connection.
    # gRPC's default GLOBAL subchannel pool would otherwise share a single connection
    # across all channels to the same target, pinning every RPC to one worker. The
    # production C++ companion client achieves exactly this topology: companion_proxy.cpp
    # sets GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL on every per-computation channel, so this
    # test models the real client's fan-out and proves the SO_REUSEPORT distribution works.
    channels = [
        grpc.insecure_channel(f"ipv6:[::1]:{port}", options=[("grpc.use_local_subchannel_pool", 1)])
        for _ in range(n_channels)
    ]
    try:

        def one_call(args):
            channel_index, batch_index = args
            stub = cs_grpc.CompanionServiceStub(channels[channel_index])
            request = _make_process_batch_request(
                cs_pb2,
                msg_pb2,
                guid_pb2,
                job_id_seed=0xDEAD0001,
                batch_index=batch_index,
            )
            return stub.ProcessBatch(request, timeout=120.0, wait_for_ready=True).status

        work = [
            (channel_index, channel_index * batches_per_channel + batch)
            for channel_index in range(n_channels)
            for batch in range(batches_per_channel)
        ]
        statuses = []
        with futures.ThreadPoolExecutor(max_workers=n_channels) as pool:
            for status in pool.map(one_call, work):
                statuses.append(status)

        # Each channel stays pinned to its worker, so a per-channel CompanionInfo
        # reports the PID that served that channel's batches.
        info_pids = set()
        for channel in channels:
            stub = cs_grpc.CompanionServiceStub(channel)
            response = stub.CompanionInfo(cs_pb2.TReqCompanionInfo(), timeout=30.0, wait_for_ready=True)
            info_pids.add(yson.loads(response.payload)["pid"])
        return statuses, info_pids
    finally:
        for channel in channels:
            channel.close()


def _distinct_serving_pids(pid_dir):
    return {int(name) for name in os.listdir(pid_dir) if name.isdigit()}


def test_n1_and_n4_produce_same_statuses(tmp_path):
    cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()

    n_batches = 8

    with _CompanionFixture(n=1, pid_dir=str(tmp_path / "n1")) as c1:
        os.makedirs(c1.pid_dir, exist_ok=True)
        statuses1 = _drive_pooled_batches(
            c1.port,
            n_batches=n_batches,
            parallelism=4,
            cs_pb2=cs_pb2,
            cs_grpc=cs_grpc,
            msg_pb2=msg_pb2,
            guid_pb2=guid_pb2,
            grpc=grpc,
        )

    with _CompanionFixture(n=4, pid_dir=str(tmp_path / "n4")) as c4:
        os.makedirs(c4.pid_dir, exist_ok=True)
        statuses4 = _drive_pooled_batches(
            c4.port,
            n_batches=n_batches,
            parallelism=4,
            cs_pb2=cs_pb2,
            cs_grpc=cs_grpc,
            msg_pb2=msg_pb2,
            guid_pb2=guid_pb2,
            grpc=grpc,
        )

    # Each request carries inline job_info, so every worker can serve any batch
    # immediately — all responses must be OK.
    assert all(s == cs_pb2.RS_OK for s in statuses1), statuses1
    assert all(s == cs_pb2.RS_OK for s in statuses4), statuses4


def test_multiple_clients_fan_across_workers(tmp_path):
    """YTFLOW-608 acceptance gate: production realizes companion CPU parallelism by running
    one TCompanionClient per computation — each its own pooled channel/connection. With
    several independent channels, SO_REUSEPORT distributes them across worker interpreters,
    so the fan-out works WITHOUT the per-call subchannel trick the old gate relied on.
    (A single pooled channel deliberately pins to one worker; see _drive_pooled_batches.)
    """
    cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()

    if (os.cpu_count() or 1) < 4:
        pytest.skip("Need at least 4 cores to demonstrate worker fan-out")

    pid_dir = str(tmp_path / "fanout")
    os.makedirs(pid_dir, exist_ok=True)

    n_channels = 16
    with _CompanionFixture(n=4, pid_dir=pid_dir) as comp:
        statuses, info_pids = _drive_multi_channel_batches(
            comp.port,
            n_channels=n_channels,
            batches_per_channel=4,
            cs_pb2=cs_pb2,
            cs_grpc=cs_grpc,
            msg_pb2=msg_pb2,
            guid_pb2=guid_pb2,
            grpc=grpc,
        )

    assert all(s == cs_pb2.RS_OK for s in statuses), statuses

    pids = _distinct_serving_pids(pid_dir)
    print(
        f"YTFLOW-608 acceptance: {n_channels} independent channels served by "
        f"{len(pids)} distinct worker PIDs: {sorted(pids)}"
    )
    # With 16 uniform connections over 4 SO_REUSEPORT sockets, P(<= 2 distinct
    # sockets hit) ~ 6 * 2^-16 ~ 1e-4, so >= 3 is flake-safe while still failing
    # on a real fan-out collapse (e.g. half the children not binding).
    assert len(pids) >= 3, (
        f"Expected {n_channels} independent channels to fan across >= 3 of 4 workers, "
        f"but only {len(pids)} PID(s) served them: {sorted(pids)}"
    )
    # The wire-level `pid` payload field must report the same serving workers the
    # marker files observed — per-channel CompanionInfo is how a live caller can
    # verify fan-out without filesystem access.
    assert info_pids == pids, (info_pids, pids)


def test_worker_crash_is_restarted_by_supervisor(tmp_path):
    """Kill one of the N=2 workers; the supervisor must restart it (a fresh PID
    replaces the dead one) and the companion keeps serving.

    The crash-recovery assertion needs Linux's /proc children listing; skip cleanly
    where it is unavailable rather than reading a non-portable path blind.
    """
    cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()

    pid_dir = str(tmp_path / "crash")
    os.makedirs(pid_dir, exist_ok=True)

    with _CompanionFixture(n=2, pid_dir=pid_dir) as comp:
        # Warm the companion (a single pooled channel pins to one of the two workers).
        statuses = _drive_pooled_batches(
            comp.port,
            n_batches=4,
            parallelism=2,
            cs_pb2=cs_pb2,
            cs_grpc=cs_grpc,
            msg_pb2=msg_pb2,
            guid_pb2=guid_pb2,
            grpc=grpc,
        )
        assert all(s == cs_pb2.RS_OK for s in statuses), statuses

        before = comp.child_pids()
        if before is None:
            pytest.skip("/proc/<pid>/task/<pid>/children unavailable on this host")
        assert before, "Expected the supervisor to have forked children"

        victim = before[0]
        os.kill(victim, signal.SIGKILL)

        # Wait for the supervisor to reap and respawn (poll interval is ~5s; give it
        # margin). The dead PID must disappear and the slot count must recover.
        deadline = time.time() + 20.0
        restarted = False
        while time.time() < deadline:
            now_pids = comp.child_pids() or []
            if victim not in now_pids and len(now_pids) == len(before):
                restarted = True
                break
            time.sleep(0.5)
        assert restarted, (
            f"Supervisor did not restart the killed worker: before={before}, " f"after={comp.child_pids()}"
        )

        # Companion still serves after the restart. Every request carries inline
        # job_info, so even a freshly-respawned worker serves it RS_OK first try.
        statuses2 = _drive_pooled_batches(
            comp.port,
            n_batches=4,
            parallelism=2,
            cs_pb2=cs_pb2,
            cs_grpc=cs_grpc,
            msg_pb2=msg_pb2,
            guid_pb2=guid_pb2,
            grpc=grpc,
        )
        assert all(s == cs_pb2.RS_OK for s in statuses2), statuses2


def test_sigterm_mid_flight_drains_in_flight_rpc(tmp_path):
    """A worker child SIGTERMed mid-RPC must drain the in-flight batch (RS_OK), not
    cancel it: the child's _terminate handler stops the server with a grace period and
    waits for the drain before exiting. Deleting the drained.wait() or exiting
    immediately in the handler fails this test with UNAVAILABLE/CANCELLED."""
    cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()

    pid_dir = str(tmp_path / "drain")
    os.makedirs(pid_dir, exist_ok=True)

    with _CompanionFixture(n=1, pid_dir=pid_dir, sleep_seconds=2.0) as comp:
        children = comp.child_pids()
        if children is None:
            pytest.skip("/proc/<pid>/task/<pid>/children unavailable on this host")
        assert len(children) == 1, children

        channel = grpc.insecure_channel(f"ipv6:[::1]:{comp.port}")
        try:
            stub = cs_grpc.CompanionServiceStub(channel)
            request = _make_process_batch_request(
                cs_pb2,
                msg_pb2,
                guid_pb2,
                job_id_seed=0xDEAD0002,
                batch_index=0,
            )
            future = stub.ProcessBatch.future(request, timeout=120.0, wait_for_ready=True)

            # The worker touches its PID marker before the 2s in-batch sleep, so a
            # marker means the RPC is in flight right now.
            deadline = time.time() + 30.0
            while time.time() < deadline and not _distinct_serving_pids(pid_dir):
                time.sleep(0.05)
            assert _distinct_serving_pids(pid_dir), "ProcessBatch never reached the worker"

            os.kill(children[0], signal.SIGTERM)

            response = future.result(timeout=60.0)
            assert response.status == cs_pb2.RS_OK
        finally:
            channel.close()


def test_start_dispatches_single_process_when_n_is_one(monkeypatch):
    """GrpcServerExecution.start() must take the single-process path when the resolved
    companion process count is 1 (no fork, no serve_parent)."""
    from yt.yt.flow.library.python.companion import server as server_mod
    from yt.yt.flow.library.python.companion import sizing as sizing_mod
    from yt.yt.flow.library.python.companion import parent as parent_mod
    from yt.yt.flow.library.python.companion.context import (
        PipelineContext,
    )

    # start() imports resolve_companion_process_count from .sizing and serve_parent from
    # .parent at call time, so the patches land on those source modules.
    monkeypatch.setattr(sizing_mod, "resolve_companion_process_count", lambda configured, quota: 1)

    serve_parent_called = {"value": False}

    def fake_serve_parent(**kwargs):
        serve_parent_called["value"] = True
        return kwargs["port"]

    monkeypatch.setattr(parent_mod, "serve_parent", fake_serve_parent)

    import threading

    start_async_called = {"value": False}

    exe = server_mod.GrpcServerExecution(
        PipelineContext(),
        port=0,
        companion_process_count=1,
    )

    def fake_start_async():
        start_async_called["value"] = True
        # Signal stop shortly after, so start() (which installs its SIGTERM handler and
        # then blocks on stop_event.wait()) returns instead of blocking forever. The
        # delay ensures the handler is registered before the signal arrives.
        threading.Timer(0.3, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()

    monkeypatch.setattr(exe, "start_async", fake_start_async)
    monkeypatch.setattr(exe, "stop", lambda: None)

    exe.start()

    assert start_async_called["value"], "expected single-process path (start_async)"
    assert not serve_parent_called["value"], "n=1 must not fork via serve_parent"


def test_env_config_delivers_companion_process_count(monkeypatch):
    """Pin the cross-language wire contract: the C++ manager serializes TCompanionConfig
    into the YT_FLOW_COMPANION_CONFIG env var (YSON text), and Python must read the count
    from the `companion_process_count` key of it. Guards the env var name, the YSON map
    shape, and the key spelling on the Python side."""
    from yt.yt.flow.library.python.companion import server as server_mod
    from yt.yt.flow.library.python.companion import sizing as sizing_mod
    from yt.yt.flow.library.python.companion import parent as parent_mod
    from yt.yt.flow.library.python.companion.context import (
        PipelineContext,
    )

    monkeypatch.setenv("YT_FLOW_MODE", "Worker")
    monkeypatch.setenv("YT_FLOW_COMPANION_CONFIG", "{companion_process_count=4}")

    captured = {}

    def fake_resolve(configured, quota):
        captured["configured"] = configured
        return 2

    monkeypatch.setattr(sizing_mod, "resolve_companion_process_count", fake_resolve)
    monkeypatch.setattr(parent_mod, "serve_parent", lambda **kwargs: kwargs["port"])

    exe = server_mod.GrpcServerExecution(PipelineContext())
    exe.start()

    assert captured["configured"] == 4


def test_start_dispatches_fork_when_n_is_two(monkeypatch):
    """GrpcServerExecution.start() must fan out via serve_parent when resolved n >= 2."""
    from yt.yt.flow.library.python.companion import server as server_mod  # noqa: F401
    from yt.yt.flow.library.python.companion import sizing as sizing_mod
    from yt.yt.flow.library.python.companion import parent as parent_mod
    from yt.yt.flow.library.python.companion.context import (
        PipelineContext,
    )

    monkeypatch.setattr(sizing_mod, "resolve_companion_process_count", lambda configured, quota: 2)

    captured = {}

    def fake_serve_parent(**kwargs):
        captured.update(kwargs)
        return 4242

    monkeypatch.setattr(parent_mod, "serve_parent", fake_serve_parent)

    exe = server_mod.GrpcServerExecution(
        PipelineContext(),
        port=0,
        companion_process_count=2,
    )
    exe.start()

    assert captured.get("companion_process_count") == 2, captured
    assert exe.port == 4242
