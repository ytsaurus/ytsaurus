"""Request processor for the CompanionService."""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from .context import PipelineContext
from .job import Job, JobContext
from .proto_mapper import (
    _guid_to_str,
    job_from_proto_job_info,
    map_process_batch_request,
    map_process_batch_response,
    map_put_job_request,
)
from .resource import CommandOutcome, ResourceStore

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
        self._resource_store = ResourceStore(pipeline_context.get_resource_factories())

    @property
    def resource_store(self) -> ResourceStore:
        return self._resource_store

    def shutdown(self) -> None:
        """Releases the resources this serving generation hosts."""
        self._resource_store.shutdown()

    def process_batch(self, request, proto_module) -> dict:
        """Process a batch request. Returns dict with status, data, stats."""
        request_id = _guid_to_str(request.request_id)
        job_id = _guid_to_str(request.job_id)
        computation_id = request.computation_id

        log.debug(
            "Processing batch: (RequestId: %s, JobId: %s, ComputationId: %s, HasJobInfo: %s)",
            request_id,
            job_id,
            computation_id,
            request.HasField("job_info"),
        )

        result = {"status": "RS_OK", "data": None}

        def _process():
            job = self._retrieve_or_create_job(
                job_id,
                computation_id,
                request.job_info if request.HasField("job_info") else None,
                "processBatch",
                request_id,
            )
            if job is None:
                result["status"] = "RS_JOB_NOT_FOUND"
                return

            # Resolve the job's exact resource references to initialized
            # instances. A lifecycle command may advance a resource at any
            # time, so a mismatch is reported in-band and the worker heals
            # with a re-init.
            lease = self._resource_store.acquire(job.companion_resources)
            if lease is None:
                log.debug(
                    "Companion resources are not initialized, rejecting batch: (JobId: %s)",
                    job_id,
                )
                result["status"] = "RS_RESOURCE_NOT_INITIALIZED"
                return

            # The lease keeps every acquired instance usable for the whole
            # batch even when a lifecycle command retires it meanwhile.
            with lease:
                computation = self._retrieve_computation(computation_id)
                stream_context = self._pipeline_context.get_stream_context()
                request_ctx = map_process_batch_request(request, job, stream_context)
                request_ctx.resources = lease.resources
                response_ctx = computation.do_process(request_ctx)

                result["data"] = map_process_batch_response(request_ctx.stream_specs, response_ctx, proto_module)

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

    def remove_job(self, request) -> dict:
        """Process a RemoveJob request. Removal is idempotent. Returns dict with status."""
        request_id = _guid_to_str(request.request_id)
        job_id = _guid_to_str(request.job_id)

        log.debug("Processing RemoveJob: (RequestId: %s, JobId: %s)", request_id, job_id)

        self._job_context.remove_job(job_id)
        return {"status": "RS_OK"}

    def list_jobs(self, request) -> dict:
        """Process a ListJobs request. Returns dict with status, job_ids."""
        del request
        return {"status": "RS_OK", "job_ids": self._job_context.list_job_ids()}

    def resource_execute(self, request) -> CommandOutcome:
        """Process a ResourceExecute request; user-code failures come back in-band."""
        request_id = _guid_to_str(request.request_id)
        log.debug(
            "Processing ResourceExecute: (RequestId: %s, ResourceId: %s, Command: %s)",
            request_id,
            request.resource_id,
            request.command,
        )
        argument = request.argument if request.HasField("argument") else None
        return self._resource_store.execute(request.resource_id, request.command, argument)

    def get_companion_info(self) -> dict:
        """Get companion information. Returns dict with status, payload."""
        log.debug("Processing CompanionInfo request")
        context_dict = self._pipeline_context.to_dict()
        return {"status": "RS_OK", "payload": context_dict}

    def _retrieve_or_create_job(
        self, job_id: str, computation_id: str, job_info, operation_name: str, request_id: str
    ) -> Optional[Job]:
        if job_info is not None:
            job = job_from_proto_job_info(
                job_id,
                computation_id,
                job_info,
                self._pipeline_context.get_stream_context(),
            )
            self._job_context.put_job(job_id, job)
            return job

        job = self._job_context.get_job(job_id)
        if job is None:
            log.debug(
                "Job not found for %s: (RequestId: %s, JobId: %s, ComputationId: %s)",
                operation_name,
                request_id,
                job_id,
                computation_id,
            )
        return job

    def _retrieve_computation(self, computation_id: str):
        computation = self._pipeline_context.get_computation(computation_id)
        if computation is None:
            raise ValueError(f"Computation not found: (ComputationId: {computation_id})")
        return computation
