"""Explicit job lifecycle: RemoveJob and ListJobs.

The worker owns the companion job set: PutJob creates and updates entries,
RemoveJob deletes them, and ListJobs reports what this process holds so the
worker's reconcile pass can reclaim copies left behind by channel migrations.
"""

import pytest

from yt.yt.flow.library.python.companion.computation import (
    Computation,
    RowFunction,
)
from yt.yt.flow.library.python.companion.context import PipelineContext
from yt.yt.flow.library.python.companion.job import JobContext
from yt.yt.flow.library.python.companion.proto_mapper import (
    _guid_parts_from_str,
    _guid_to_str,
)
from yt.yt.flow.library.python.companion.service import (
    CompanionRequestProcessor,
)


class _Passthrough(RowFunction):
    def on_message(self, message, output, ctx):
        pass


def _make_pipeline_context():
    ctx = PipelineContext()
    ctx.register_computation(Computation(computation_id="mapper", process_function=_Passthrough()))
    return ctx


def _make_job():
    # JobContext never inspects the stored value; a marker object is enough.
    return object()


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


def _proto_module_facade(cs_pb2, msg_pb2):
    class ProtoModule:
        TResponseData = cs_pb2.TResponseData
        TNewTimer = cs_pb2.TNewTimer
        TState = cs_pb2.TState
        TStateItem = cs_pb2.TStateItem
        TMessage = msg_pb2.TMessage

    return ProtoModule


def _make_guid_proto(first, second):
    from yt_proto.yt.core.misc.proto import guid_pb2

    guid = guid_pb2.TGuid()
    guid.first = first
    guid.second = second
    return guid


class TestJobContext:
    def test_remove_job_is_idempotent(self):
        job_ctx = JobContext()
        job_ctx.remove_job("unknown")

        job_ctx.put_job("j1", _make_job())
        assert job_ctx.get_job("j1") is not None

        job_ctx.remove_job("j1")
        assert job_ctx.get_job("j1") is None
        job_ctx.remove_job("j1")

    def test_keeps_every_registered_job(self):
        job_ctx = JobContext()
        for index in range(2000):
            job_ctx.put_job(f"j{index}", _make_job())

        # No cap: a registered job is never evicted behind the worker's back.
        for index in range(2000):
            assert job_ctx.get_job(f"j{index}") is not None
        assert len(job_ctx.list_job_ids()) == 2000

    def test_registers_a_removed_job_again(self):
        job_ctx = JobContext()
        job_ctx.put_job("j1", _make_job())
        job_ctx.remove_job("j1")

        # A registration processed after a removal recreates the entry; if
        # its job is gone from the worker, the reconcile pass reclaims it.
        job_ctx.put_job("j1", _make_job())
        assert job_ctx.get_job("j1") is not None

    def test_clear_forgets_jobs(self):
        job_ctx = JobContext()
        job_ctx.put_job("j1", _make_job())

        job_ctx.clear()

        # A fresh serving generation starts empty.
        assert job_ctx.get_job("j1") is None
        job_ctx.put_job("j1", _make_job())
        assert job_ctx.get_job("j1") is not None


class TestProcessorLifecycle:
    def _make_processor(self):
        pipeline_ctx = _make_pipeline_context()
        job_ctx = JobContext()
        return CompanionRequestProcessor(pipeline_ctx, job_ctx), job_ctx

    def _make_batch_request(self, cs_pb2, request_id, job_id_guid, with_job_info):
        request = cs_pb2.TReqProcessBatch()
        request.request_id.CopyFrom(_make_guid_proto(request_id, request_id))
        request.job_id.CopyFrom(job_id_guid)
        request.computation_id = "mapper"
        if with_job_info:
            stream = cs_pb2.TStream()
            stream.stream_id = "input"
            stream.stream_spec_id = 0
            stream.schema = b"[]"

            job_info = cs_pb2.TJobInfo()
            job_info.spec = b"{}"
            job_info.dynamic_spec = b"{}"
            job_info.streams.append(stream)
            request.job_info.CopyFrom(job_info)
        return request

    def test_remove_job(self):
        cs_pb2, msg_pb2 = _get_proto_modules()
        proto_module = _proto_module_facade(cs_pb2, msg_pb2)
        processor, _ = self._make_processor()
        job_id_guid = _make_guid_proto(0xAB, 0xCD)

        # Register the job through the inline job_info path.
        seed = self._make_batch_request(cs_pb2, 1, job_id_guid, with_job_info=True)
        assert processor.process_batch(seed, proto_module)["status"] == "RS_OK"

        remove = cs_pb2.TReqRemoveJob()
        remove.request_id.CopyFrom(_make_guid_proto(2, 2))
        remove.job_id.CopyFrom(job_id_guid)
        assert processor.remove_job(remove)["status"] == "RS_OK"

        # The removed job is unknown to this process from now on.
        batch = self._make_batch_request(cs_pb2, 3, job_id_guid, with_job_info=False)
        assert processor.process_batch(batch, proto_module)["status"] == "RS_JOB_NOT_FOUND"

        # Removal is idempotent.
        assert processor.remove_job(remove)["status"] == "RS_OK"

        # Job info heals the removed job: the entry is recreated, and the
        # worker's reconcile pass reclaims it if its job is gone.
        late = self._make_batch_request(cs_pb2, 4, job_id_guid, with_job_info=True)
        assert processor.process_batch(late, proto_module)["status"] == "RS_OK"

    def test_list_jobs(self):
        cs_pb2, msg_pb2 = _get_proto_modules()
        proto_module = _proto_module_facade(cs_pb2, msg_pb2)
        processor, job_ctx = self._make_processor()
        job_id_guid = _make_guid_proto(0xAB, 0xCD)

        request = cs_pb2.TReqListJobs()
        request.request_id.CopyFrom(_make_guid_proto(1, 1))

        result = processor.list_jobs(request)
        assert result["status"] == "RS_OK"
        assert result["job_ids"] == []

        seed = self._make_batch_request(cs_pb2, 2, job_id_guid, with_job_info=True)
        assert processor.process_batch(seed, proto_module)["status"] == "RS_OK"

        # Jobs are keyed by the canonical text form of their id, the same one
        # the worker and controller logs print.
        job_id = _guid_to_str(job_id_guid)
        assert processor.list_jobs(request)["job_ids"] == [job_id]
        assert job_ctx.list_job_ids() == [job_id]
        # The ListJobs response converts the key back into proto halves.
        assert _guid_parts_from_str(job_id) == (job_id_guid.first, job_id_guid.second)

        remove = cs_pb2.TReqRemoveJob()
        remove.request_id.CopyFrom(_make_guid_proto(3, 3))
        remove.job_id.CopyFrom(job_id_guid)
        assert processor.remove_job(remove)["status"] == "RS_OK"

        assert processor.list_jobs(request)["job_ids"] == []
