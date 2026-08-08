"""Tests for CompanionRequestProcessor."""

import os

import pytest

from yt.yt.flow.library.python.companion.computation import (
    Computation,
    RowFunction,
)
from yt.yt.flow.library.python.companion.context import PipelineContext, ResponseContext
from yt.yt.flow.library.python.companion.job import JobContext
from yt.yt.flow.library.python.companion.proto_mapper import _guid_to_str
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


def _make_guid_proto(first=0x12345678, second=0xABCDEF00):
    """Create a minimal TGuid-like object for testing."""
    from yt_proto.yt.core.misc.proto import guid_pb2

    guid = guid_pb2.TGuid()
    guid.first = first
    guid.second = second
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
    def test_maps_distinct_request_and_job_ids(self):
        cs_pb2, msg_pb2 = _get_proto_modules()

        class RecordingComputation:
            computation_id = "mapper"
            request_context = None

            def do_process(self, request_context):
                self.request_context = request_context
                return ResponseContext(
                    job_id=request_context.job_id,
                    request_id=request_context.request_id,
                )

        comp = RecordingComputation()
        pipeline_ctx = _make_pipeline_context([comp])
        processor = CompanionRequestProcessor(pipeline_ctx, JobContext())

        request = cs_pb2.TReqProcessBatch()
        request_id = _make_guid_proto(0x11111111, 0x22222222)
        job_id = _make_guid_proto(0x33333333, 0x44444444)
        request.request_id.CopyFrom(request_id)
        request.job_id.CopyFrom(job_id)
        request.computation_id = "mapper"

        stream = _make_stream_proto(cs_pb2, "input", 0)
        request.job_info.CopyFrom(_make_job_info(cs_pb2, streams=[stream]))

        class ProtoModule:
            TResponseData = cs_pb2.TResponseData
            TNewTimer = cs_pb2.TNewTimer
            TState = cs_pb2.TState
            TStateItem = cs_pb2.TStateItem
            TMessage = msg_pb2.TMessage

        result = processor.process_batch(request, ProtoModule)

        assert result["status"] == "RS_OK"
        assert comp.request_context.request_id == _guid_to_str(request_id)
        assert comp.request_context.job_id == _guid_to_str(job_id)

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
