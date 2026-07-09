"""Tests for CompanionRequestProcessor."""

import os

import pytest

from yt.yt.flow.library.python.companion.computation import (
    Computation,
    RowFunction,
)
from yt.yt.flow.library.python.companion.context import PipelineContext
from yt.yt.flow.library.python.companion.job import JobContext
from yt.yt.flow.library.python.companion.row import (
    Message,
)
from yt.yt.flow.library.python.companion.service import (
    CompanionRequestProcessor,
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


class FilterFunction(RowFunction):
    """Only distribute messages with message_id starting with 'keep'."""

    def on_message(self, message, output, ctx):
        output.add_message(
            Message(
                message_id=message.message_id,
                stream_id=message.stream_id,
                payload=message.payload,
            ),
            distribute=message.message_id.startswith("keep"),
        )


def _make_pipeline_context(computations=None):
    ctx = PipelineContext()
    for comp in computations or []:
        ctx.register_computation(comp)
    return ctx


def _make_guid_proto():
    """Create a minimal TGuid-like object for testing."""
    from yt_proto.yt.core.misc.proto import guid_pb2

    guid = guid_pb2.TGuid()
    guid.first = 0x12345678
    guid.second = 0xABCDEF00
    return guid


def _make_job_info(proto_module, streams=None, spec=b"{}", dynamic_spec=b"{}"):
    """Create a TJobInfo proto."""
    job_info = proto_module.TJobInfo()
    job_info.spec = spec
    job_info.dynamic_spec = dynamic_spec
    if streams:
        for s in streams:
            job_info.streams.append(s)
    return job_info


def _make_stream_proto(proto_module, stream_id, spec_id, schema=b"[]"):
    stream = proto_module.TStream()
    stream.stream_id = stream_id
    stream.stream_spec_id = spec_id
    stream.schema = schema
    return stream


def _get_proto_modules():
    try:
        from yt.yt.flow.library.python.companion._proto_compat import ensure_proto_imports

        ensure_proto_imports()
        from yt.flow.library.cpp.companion.proto import (
            companion_service_pb2 as cs_pb2,
        )
        from yt.flow.library.cpp.common.proto import (
            message_pb2 as msg_pb2,
        )

        return cs_pb2, msg_pb2
    except ImportError:
        pytest.skip("Proto modules not available")


class TestProcessBatch:
    def test_basic_with_job_info(self):
        cs_pb2, msg_pb2 = _get_proto_modules()

        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        pipeline_ctx = _make_pipeline_context([comp])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(_make_guid_proto())
        request.job_id.CopyFrom(_make_guid_proto())
        request.computation_id = "mapper"

        stream = _make_stream_proto(cs_pb2, "input", 0)
        job_info = _make_job_info(cs_pb2, streams=[stream])
        request.job_info.CopyFrom(job_info)

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        result = processor.process_batch(request, ProtoModule)
        assert result["status"] == "RS_OK"

    def test_computation_not_found(self):
        cs_pb2, msg_pb2 = _get_proto_modules()

        pipeline_ctx = _make_pipeline_context([])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(_make_guid_proto())
        request.job_id.CopyFrom(_make_guid_proto())
        request.computation_id = "nonexistent"

        stream = _make_stream_proto(cs_pb2, "input", 0)
        job_info = _make_job_info(cs_pb2, streams=[stream])
        request.job_info.CopyFrom(job_info)

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        with pytest.raises(ValueError, match="Computation not found"):
            processor.process_batch(request, ProtoModule)

    def test_job_not_found(self):
        cs_pb2, msg_pb2 = _get_proto_modules()

        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        pipeline_ctx = _make_pipeline_context([comp])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(_make_guid_proto())
        request.job_id.CopyFrom(_make_guid_proto())
        request.computation_id = "mapper"
        # No job_info and no cached job.

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        result = processor.process_batch(request, ProtoModule)
        assert result["status"] == "RS_JOB_NOT_FOUND"

    def test_empty_batch(self):
        cs_pb2, msg_pb2 = _get_proto_modules()

        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        pipeline_ctx = _make_pipeline_context([comp])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(_make_guid_proto())
        request.job_id.CopyFrom(_make_guid_proto())
        request.computation_id = "mapper"

        stream = _make_stream_proto(cs_pb2, "input", 0)
        job_info = _make_job_info(cs_pb2, streams=[stream])
        request.job_info.CopyFrom(job_info)

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        result = processor.process_batch(request, ProtoModule)
        assert result["status"] == "RS_OK"
        assert result["data"] is not None

    def test_resource_stats_populated(self):
        cs_pb2, msg_pb2 = _get_proto_modules()

        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        pipeline_ctx = _make_pipeline_context([comp])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(_make_guid_proto())
        request.job_id.CopyFrom(_make_guid_proto())
        request.computation_id = "mapper"

        stream = _make_stream_proto(cs_pb2, "input", 0)
        job_info = _make_job_info(cs_pb2, streams=[stream])
        request.job_info.CopyFrom(job_info)

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        result = processor.process_batch(request, ProtoModule)
        assert result["stats"].cpu_time_ns >= 0


class TestPutJob:
    def test_basic_put_job(self):
        cs_pb2, _ = _get_proto_modules()

        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        pipeline_ctx = _make_pipeline_context([comp])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        request = cs_pb2.TReqPutJob()
        request.request_id.CopyFrom(_make_guid_proto())
        request.job_id.CopyFrom(_make_guid_proto())
        request.computation_id = "mapper"

        stream = _make_stream_proto(cs_pb2, "input", 0)
        job_info = _make_job_info(cs_pb2, streams=[stream])
        request.job_info.CopyFrom(job_info)

        result = processor.put_job(request)
        assert result["status"] == "RS_OK"

        # Verify job is cached.
        job_id = "12345678-abcdef00"
        cached_job = job_ctx.get_job(job_id)
        assert cached_job is not None


class TestCompanionInfo:
    def test_basic_info(self):
        pipeline_ctx = _make_pipeline_context([])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        result = processor.get_companion_info()
        assert result["status"] == "RS_OK"
        assert "computations" in result["payload"]
        # The serving worker's PID is surfaced so callers can observe which forked
        # worker handled the call.
        assert result["payload"]["pid"] == os.getpid()

    def test_info_with_computations(self):
        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        pipeline_ctx = _make_pipeline_context([comp])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        result = processor.get_companion_info()
        assert "mapper" in result["payload"]["computations"]


class TestIdempotency:
    def test_consecutive_identical_calls(self):
        cs_pb2, msg_pb2 = _get_proto_modules()

        comp = Computation(computation_id="mapper", process_function=PassthroughFunction())
        pipeline_ctx = _make_pipeline_context([comp])
        job_ctx = JobContext()
        processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)

        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(_make_guid_proto())
        request.job_id.CopyFrom(_make_guid_proto())
        request.computation_id = "mapper"

        stream = _make_stream_proto(cs_pb2, "input", 0)
        job_info = _make_job_info(cs_pb2, streams=[stream])
        request.job_info.CopyFrom(job_info)

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        result1 = processor.process_batch(request, ProtoModule)
        result2 = processor.process_batch(request, ProtoModule)
        assert result1["status"] == result2["status"]
