"""Job classes: Job, JobContext."""

import logging
import threading
from typing import Any, Dict, List, Optional, Set

from .resource import CompanionResourceInstanceReference, _extract_parameters
from .row import TableSchema, EMPTY_SCHEMA
from .stream import StreamSpecs

log = logging.getLogger(__name__)

# The minimum watermark of a computation without input streams, as in the C++ runtime.
INFINITE_WATERMARK = 2**64 - 1


class Job:
    """Represents a computation job with static and dynamic configuration."""

    def __init__(
        self,
        job_id: str,
        computation_id: str,
        stream_specs: StreamSpecs,
        static_spec: Any = None,
        dynamic_spec: Any = None,
        group_by_schema: Optional[TableSchema] = None,
        companion_resources: Optional[List[CompanionResourceInstanceReference]] = None,
    ):
        self.job_id = job_id
        self.computation_id = computation_id
        self.stream_specs = stream_specs
        self.static_spec = static_spec or {}
        self.dynamic_spec = dynamic_spec or {}
        self.group_by_schema = group_by_schema or EMPTY_SCHEMA
        # Exact direct and transitive companion resources required by the job.
        self.companion_resources = companion_resources or []

        self.static_parameters = _extract_parameters(self.static_spec)
        self.dynamic_parameters = _extract_parameters(self.dynamic_spec)
        self.internal_state_names = self._extract_internal_states(self.static_parameters)
        self.external_state_names = self._extract_external_states(self.static_spec)
        self.joiner_state_names = self._extract_joiner_states(self.static_spec)
        self.input_stream_ids = self._extract_input_stream_ids(self.static_spec)

    def min_input_watermark(self, watermarks: Dict[str, int]) -> int:
        """Minimum event watermark across the input streams of the spec.

        As in the C++ runtime, an input stream without a watermark counts as 0, and a computation
        without input streams gets INFINITE_WATERMARK. A spec that does not list input_stream_ids
        falls back to the minimum of all the given watermarks, or 0 if there are none.
        """
        if self.input_stream_ids is None:
            return min(watermarks.values(), default=0)
        return min((watermarks.get(stream_id, 0) for stream_id in self.input_stream_ids), default=INFINITE_WATERMARK)

    @staticmethod
    def _extract_input_stream_ids(spec: Any) -> Optional[List[str]]:
        if not isinstance(spec, dict):
            return None
        stream_ids = spec.get("input_stream_ids")
        if stream_ids is None:
            return None
        return [str(stream_id) for stream_id in stream_ids]

    @staticmethod
    def _extract_internal_states(parameters: Dict[str, Any]) -> Set[str]:
        state_names = parameters.get("internal_states")
        if state_names is not None:
            return set(state_names)
        return set()

    @staticmethod
    def _extract_external_states(spec: Any) -> Set[str]:
        if not isinstance(spec, dict):
            return set()
        state_managers = spec.get("external_state_managers")
        if state_managers is not None and isinstance(state_managers, dict):
            return set(state_managers.keys())
        return set()

    @staticmethod
    def _extract_joiner_states(spec: Any) -> Set[str]:
        if not isinstance(spec, dict):
            return set()
        state_joiners = spec.get("external_state_joiners")
        if state_joiners is not None and isinstance(state_joiners, dict):
            return set(state_joiners.keys())
        return set()


class JobContext:
    """Registry of jobs owned by the worker: entries are created and updated by
    PutJob and removed by RemoveJob, so an entry lives exactly as long as its
    job. Copies left behind by a channel that moved to another process are
    reclaimed by the worker's reconcile pass."""

    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()

    def get_job(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def put_job(self, job_id: str, job: Job):
        """Register or replace a job."""
        with self._lock:
            self._jobs[job_id] = job

    def remove_job(self, job_id: str):
        """Remove a job; unknown ids are ignored (removal is idempotent)."""
        with self._lock:
            self._jobs.pop(job_id, None)

    def list_job_ids(self):
        """Ids of every registered job."""
        with self._lock:
            return list(self._jobs.keys())

    def clear(self):
        """Forget every job, so a server that starts serving again does not
        answer for jobs of its previous generation."""
        with self._lock:
            self._jobs.clear()
