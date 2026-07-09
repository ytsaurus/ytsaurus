"""Lazy-load correctness for stateless companion workers.

The pre-fork supervisor relies on the existing two-step rehydration: a worker that
has not yet seen a ``job_id`` replies ``RS_JOB_NOT_FOUND``; the C++ client retries
with ``request.job_info`` populated, and the worker registers the job in its own
``JobContext`` and serves the second attempt. This test proves the second half of
that loop directly against the existing ``CompanionRequestProcessor`` — no new
cache module is introduced.
"""

import pytest

from yt.yt.flow.library.python.companion.computation import (
    Computation,
    RowFunction,
)
from yt.yt.flow.library.python.companion.context import PipelineContext
from yt.yt.flow.library.python.companion.job import JobContext
from yt.yt.flow.library.python.companion.row import Message
from yt.yt.flow.library.python.companion.service import (
    CompanionRequestProcessor,
)


class _Passthrough(RowFunction):
    def on_message(self, message, output, ctx):
        output.add_message(
            Message(
                message_id=message.message_id,
                stream_id=message.stream_id,
                payload=message.payload,
            )
        )


def _make_pipeline_context():
    ctx = PipelineContext()
    ctx.register_computation(Computation(computation_id="mapper", process_function=_Passthrough()))
    return ctx


def _get_proto_modules():
    try:
        from yt.yt.flow.library.python.companion._proto_compat import (
            ensure_proto_imports,
        )

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


def _make_guid_proto(first, second):
    from yt_proto.yt.core.misc.proto import guid_pb2

    guid = guid_pb2.TGuid()
    guid.first = first
    guid.second = second
    return guid


def _proto_module_facade(cs_pb2, msg_pb2):
    class ProtoModule:
        TResponseData = cs_pb2.TResponseData
        TNewTimer = cs_pb2.TNewTimer
        TState = cs_pb2.TState
        TStateItem = cs_pb2.TStateItem
        TMessage = msg_pb2.TMessage

    return ProtoModule


def test_unknown_job_then_inline_job_info_recovers_and_caches():
    cs_pb2, msg_pb2 = _get_proto_modules()

    pipeline_ctx = _make_pipeline_context()
    job_ctx = JobContext()
    processor = CompanionRequestProcessor(pipeline_ctx, job_ctx)
    proto_module = _proto_module_facade(cs_pb2, msg_pb2)

    job_id_guid = _make_guid_proto(0xDEAD0001, 0xBEEF0001)

    # First attempt: worker has never seen this job and there's no inline job_info.
    first = cs_pb2.TReqProcessBatch()
    first.request_id.CopyFrom(_make_guid_proto(0x1, 0x1))
    first.job_id.CopyFrom(job_id_guid)
    first.computation_id = "mapper"

    first_result = processor.process_batch(first, proto_module)
    assert first_result["status"] == "RS_JOB_NOT_FOUND"

    # Second attempt: same job_id but the client now includes job_info (mirrors what
    # the C++ companion client does on RS_JOB_NOT_FOUND via SendJobInfo=true).
    second = cs_pb2.TReqProcessBatch()
    second.request_id.CopyFrom(_make_guid_proto(0x2, 0x2))
    second.job_id.CopyFrom(job_id_guid)
    second.computation_id = "mapper"

    stream = cs_pb2.TStream()
    stream.stream_id = "input"
    stream.stream_spec_id = 0
    stream.schema = b"[]"

    job_info = cs_pb2.TJobInfo()
    job_info.spec = b"{}"
    job_info.dynamic_spec = b"{}"
    job_info.streams.append(stream)
    second.job_info.CopyFrom(job_info)

    second_result = processor.process_batch(second, proto_module)
    assert second_result["status"] == "RS_OK"

    # Third attempt: same job_id, no inline job_info — must hit the cache the second
    # attempt populated. (RS_JOB_NOT_FOUND here would mean the cache was bypassed.)
    third = cs_pb2.TReqProcessBatch()
    third.request_id.CopyFrom(_make_guid_proto(0x3, 0x3))
    third.job_id.CopyFrom(job_id_guid)
    third.computation_id = "mapper"

    third_result = processor.process_batch(third, proto_module)
    assert third_result["status"] == "RS_OK"
