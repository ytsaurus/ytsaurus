"""Job classes: Job, JobContext."""

import threading
import time
from typing import Any, Dict, Optional, Set

from .row import TableSchema, EMPTY_SCHEMA
from .stream import StreamSpecs


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
    """Cache for Job instances with TTL expiration."""

    def __init__(self, job_ttl_seconds: int = 600):
        self._job_ttl = job_ttl_seconds
        self._cache: Dict[str, tuple[Job, float]] = {}
        self._lock = threading.Lock()

    def get_job(self, job_id: str) -> Optional[Job]:
        with self._lock:
            entry = self._cache.get(job_id)
            if entry is None:
                return None
            job, last_access = entry
            if time.monotonic() - last_access > self._job_ttl:
                del self._cache[job_id]
                return None
            # Update access time.
            self._cache[job_id] = (job, time.monotonic())
            return job

    def put_job(self, job_id: str, job: Job):
        with self._lock:
            self._cache[job_id] = (job, time.monotonic())

    def remove_job(self, job_id: str):
        with self._lock:
            self._cache.pop(job_id, None)
