"""Job classes: Job, JobContext."""

import logging
import threading
from typing import Any, Dict, Optional, Set

from .row import TableSchema, EMPTY_SCHEMA
from .stream import StreamSpecs

log = logging.getLogger(__name__)


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
    ):
        self.job_id = job_id
        self.computation_id = computation_id
        self.stream_specs = stream_specs
        self.static_spec = static_spec or {}
        self.dynamic_spec = dynamic_spec or {}
        self.group_by_schema = group_by_schema or EMPTY_SCHEMA

        self.static_parameters = self._extract_parameters(self.static_spec)
        self.dynamic_parameters = self._extract_parameters(self.dynamic_spec)
        self.internal_state_names = self._extract_internal_states(self.static_parameters)
        self.external_state_names = self._extract_external_states(self.static_spec)
        self.joiner_state_names = self._extract_joiner_states(self.static_spec)

    @staticmethod
    def _extract_parameters(spec: Any) -> Dict[str, Any]:
        if isinstance(spec, dict):
            return spec.get("parameters", {})
        return {}

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
