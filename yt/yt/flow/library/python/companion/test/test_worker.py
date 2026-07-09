"""Tests for the worker gRPC server bootstrap."""

import pytest

from yt.yt.flow.library.python.companion.context import PipelineContext
from yt.yt.flow.library.python.companion.job import JobContext
from yt.yt.flow.library.python.companion.worker import (
    build_worker_server,
)


class FakeServer:
    """Records the args grpc.server() was called with."""

    def __init__(self, executor, options, bind_result=1):
        self.executor = executor
        self.options = list(options)
        self.address = None
        self.bind_result = bind_result

    def add_generic_rpc_handlers(self, handlers):
        # add_CompanionServiceServicer_to_server registers the servicer through this.
        pass

    def add_insecure_port(self, address):
        self.address = address
        return self.bind_result


def test_worker_server_sets_so_reuseport():
    captured = {}

    def factory(executor, options):
        srv = FakeServer(executor, options)
        captured["server"] = srv
        return srv

    pipeline_context = PipelineContext()
    job_context = JobContext(job_ttl_seconds=60)

    server = build_worker_server(
        address="[::]:5005",
        pipeline_context=pipeline_context,
        job_context=job_context,
        io_threads=2,
        server_factory=factory,
    )

    fake = captured["server"]
    options_dict = dict(fake.options)
    assert options_dict.get("grpc.so_reuseport") == 1
    assert fake.address == "[::]:5005"
    assert server is fake


def test_worker_server_honours_extra_options():
    captured = {}

    def factory(executor, options):
        srv = FakeServer(executor, options)
        captured["server"] = srv
        return srv

    build_worker_server(
        address="[::]:0",
        pipeline_context=PipelineContext(),
        job_context=JobContext(job_ttl_seconds=60),
        extra_options=[("custom.option", "x")],
        server_factory=factory,
    )

    options_dict = dict(captured["server"].options)
    assert options_dict.get("grpc.so_reuseport") == 1
    assert options_dict.get("custom.option") == "x"


def test_worker_server_raises_on_bind_failure():
    """grpc reports a failed bind by returning 0 from add_insecure_port, not raising;
    build_worker_server must turn that into an error so the child exits and the
    supervisor's restart machinery takes over instead of keeping a deaf worker."""

    def factory(executor, options):
        return FakeServer(executor, options, bind_result=0)

    with pytest.raises(RuntimeError, match="bind"):
        build_worker_server(
            address="[::]:5005",
            pipeline_context=PipelineContext(),
            job_context=JobContext(job_ttl_seconds=60),
            server_factory=factory,
        )
