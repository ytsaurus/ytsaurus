"""Request processor for the CompanionService."""

import logging
import time
from dataclasses import dataclass
from .context import PipelineContext
from .job import JobContext
from .proto_mapper import (
    _guid_to_str,
    job_from_proto_job_info,
    map_process_batch_request,
    map_process_batch_response,
    map_put_job_request,
)

log = logging.getLogger(__name__)


@dataclass
class ResourceStats:
    """Resource statistics for a single call."""
    allocated_bytes: int = 0
    cpu_time_ns: int = 0


class ResourceMonitor:
    """Measures CPU time for callback execution."""

    def call_measured(self, callback) -> ResourceStats:
        start_cpu = time.process_time_ns()
        callback()
        end_cpu = time.process_time_ns()
        return ResourceStats(
            allocated_bytes=0,
            cpu_time_ns=end_cpu - start_cpu,
        )


class CompanionRequestProcessor:
    """Core request processor for Companion service operations."""

    def __init__(self, pipeline_context: PipelineContext, job_context: JobContext):
        self._pipeline_context = pipeline_context
        self._job_context = job_context
        self._resource_monitor = ResourceMonitor()

    def process_batch(self, request, proto_module) -> dict:
        """Process a batch request. Returns dict with status, data, stats."""
        request_id = _guid_to_str(request.request_id)
        job_id = _guid_to_str(request.job_id)
        computation_id = request.computation_id

        log.debug(
            "Processing batch: (RequestId: %s, JobId: %s, ComputationId: %s, HasJobInfo: %s)",
            request_id, job_id, computation_id, request.HasField("job_info"),
        )

        result = {"status": "RS_OK", "data": None}

        def _process():
            job_result = self._retrieve_or_create_job(
                job_id, computation_id,
                request.job_info if request.HasField("job_info") else None,
                "processBatch", request_id,
            )
            if not job_result["success"]:
                result["status"] = job_result["status"]
                return

            computation = self._retrieve_computation(computation_id)
            stream_context = self._pipeline_context.get_stream_context()
            request_ctx = map_process_batch_request(request, job_result["job"], stream_context)
            response_ctx = computation.do_process(request_ctx)

            result["data"] = map_process_batch_response(
                request_ctx.stream_specs, response_ctx, proto_module
            )

        stats = self._resource_monitor.call_measured(_process)
        result["stats"] = stats
        return result

    def put_job(self, request) -> dict:
        """Process a PutJob request. Returns dict with status, stats."""
        request_id = _guid_to_str(request.request_id)
        job_id = _guid_to_str(request.job_id)

        log.debug("Processing PutJob: (RequestId: %s, JobId: %s)", request_id, job_id)

        def _process():
            job = map_put_job_request(request, self._pipeline_context.get_stream_context())
            self._job_context.put_job(job_id, job)

        stats = self._resource_monitor.call_measured(_process)
        return {"status": "RS_OK", "stats": stats}

    def get_companion_info(self) -> dict:
        """Get companion information. Returns dict with status, payload."""
        log.debug("Processing CompanionInfo request")
        context_dict = self._pipeline_context.to_dict()
        return {"status": "RS_OK", "payload": context_dict}

    def _retrieve_or_create_job(
        self, job_id: str, computation_id: str, job_info, operation_name: str, request_id: str
    ) -> dict:
        if job_info is not None:
            job = job_from_proto_job_info(
                job_id, computation_id, job_info,
                self._pipeline_context.get_stream_context(),
            )
            self._job_context.put_job(job_id, job)
            return {"success": True, "job": job, "status": "RS_OK"}

        job = self._job_context.get_job(job_id)
        if job is None:
            log.debug(
                "Job not found for %s: (RequestId: %s, JobId: %s, ComputationId: %s)",
                operation_name, request_id, job_id, computation_id,
            )
            return {"success": False, "job": None, "status": "RS_JOB_NOT_FOUND"}

        return {"success": True, "job": job, "status": "RS_OK"}

    def _retrieve_computation(self, computation_id: str):
        computation = self._pipeline_context.get_computation(computation_id)
        if computation is None:
            raise ValueError(f"Computation not found: (ComputationId: {computation_id})")
        return computation
