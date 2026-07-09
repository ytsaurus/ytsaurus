"""Tests for gRPC server lifecycle and round-trips."""

import pytest

from yt.yt.flow.library.python.companion.computation import (
    Computation,
    RowFunction,
)
from yt.yt.flow.library.python.companion.context import PipelineContext
from yt.yt.flow.library.python.companion.row import (
    Message,
)


class PassthroughFunction(RowFunction):
    def on_message(self, message, output, ctx):
        output.add_message(
            Message(
                message_id=message.message_id,
                stream_id=message.stream_id,
                payload=message.payload,
            )
        )


def _get_modules():
    try:
        from yt.yt.flow.library.python.companion._proto_compat import ensure_proto_imports
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
    except ImportError:
        pytest.skip("Proto/gRPC modules not available")


def _make_guid(guid_pb2):
    guid = guid_pb2.TGuid()
    guid.first = 1
    guid.second = 2
    return guid


class TestServerLifecycle:
    def test_start_stop(self):
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()
        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        ctx.register_computation(comp)

        server = GrpcServerExecution(ctx, port=0)
        server.start_async()

        assert server.running
        assert server.port > 0

        server.stop()
        assert not server.running

    def test_double_start(self):
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()
        server = GrpcServerExecution(ctx, port=0)
        server.start_async()

        port1 = server.port
        server.start_async()  # Should be no-op.
        assert server.port == port1

        server.stop()

    def test_double_stop(self):
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()
        server = GrpcServerExecution(ctx, port=0)
        server.start_async()
        server.stop()
        server.stop()  # Should be no-op.

    def test_context_is_frozen_after_start(self):
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()
        ctx.register_computation(Computation(
            computation_id="before_start", process_function=PassthroughFunction()))

        server = GrpcServerExecution(ctx, port=0)
        server.start_async()
        try:
            with pytest.raises(RuntimeError, match="frozen"):
                ctx.register_computation(Computation(
                    computation_id="after_start", process_function=PassthroughFunction()))
        finally:
            server.stop()


class TestGrpcRoundTrip:
    def test_companion_info(self):
        cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()
        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        ctx.register_computation(comp)

        server = GrpcServerExecution(ctx, port=0)
        server.start_async()

        try:
            channel = grpc.insecure_channel(f"localhost:{server.port}")
            try:
                stub = cs_grpc.CompanionServiceStub(channel)

                request = cs_pb2.TReqCompanionInfo()
                response = stub.CompanionInfo(request)

                assert response.status == cs_pb2.RS_OK
                assert len(response.payload) > 0
            finally:
                channel.close()
        finally:
            server.stop()

    def test_put_job(self):
        cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()
        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        ctx.register_computation(comp)

        server = GrpcServerExecution(ctx, port=0)
        server.start_async()

        try:
            channel = grpc.insecure_channel(f"localhost:{server.port}")
            try:
                stub = cs_grpc.CompanionServiceStub(channel)

                request = cs_pb2.TReqPutJob()
                request.request_id.CopyFrom(_make_guid(guid_pb2))
                request.job_id.CopyFrom(_make_guid(guid_pb2))
                request.computation_id = "mapper"

                stream = cs_pb2.TStream()
                stream.stream_id = "input"
                stream.stream_spec_id = 0
                stream.schema = b"[]"

                request.job_info.spec = b"{}"
                request.job_info.dynamic_spec = b"{}"
                request.job_info.streams.append(stream)

                response = stub.PutJob(request)
                assert response.status == cs_pb2.RS_OK
            finally:
                channel.close()
        finally:
            server.stop()

    def test_process_batch_empty(self):
        cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()
        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        ctx.register_computation(comp)

        server = GrpcServerExecution(ctx, port=0)
        server.start_async()

        try:
            channel = grpc.insecure_channel(f"localhost:{server.port}")
            try:
                stub = cs_grpc.CompanionServiceStub(channel)

                request = cs_pb2.TReqProcessBatch()
                request.request_id.CopyFrom(_make_guid(guid_pb2))
                request.job_id.CopyFrom(_make_guid(guid_pb2))
                request.computation_id = "mapper"

                stream = cs_pb2.TStream()
                stream.stream_id = "input"
                stream.stream_spec_id = 0
                stream.schema = b"[]"

                request.job_info.spec = b"{}"
                request.job_info.dynamic_spec = b"{}"
                request.job_info.streams.append(stream)

                response = stub.ProcessBatch(request)
                assert response.status == cs_pb2.RS_OK
            finally:
                channel.close()
        finally:
            server.stop()

    def test_error_computation_not_found(self):
        cs_pb2, cs_grpc, msg_pb2, guid_pb2, grpc = _get_modules()
        from yt.yt.flow.library.python.companion.server import GrpcServerExecution

        ctx = PipelineContext()

        server = GrpcServerExecution(ctx, port=0)
        server.start_async()

        try:
            channel = grpc.insecure_channel(f"localhost:{server.port}")
            try:
                stub = cs_grpc.CompanionServiceStub(channel)

                request = cs_pb2.TReqProcessBatch()
                request.request_id.CopyFrom(_make_guid(guid_pb2))
                request.job_id.CopyFrom(_make_guid(guid_pb2))
                request.computation_id = "nonexistent"

                stream = cs_pb2.TStream()
                stream.stream_id = "input"
                stream.stream_spec_id = 0
                stream.schema = b"[]"

                request.job_info.spec = b"{}"
                request.job_info.dynamic_spec = b"{}"
                request.job_info.streams.append(stream)

                with pytest.raises(grpc.RpcError) as exc_info:
                    stub.ProcessBatch(request)
                assert exc_info.value.code() == grpc.StatusCode.INTERNAL
            finally:
                channel.close()
        finally:
            server.stop()
