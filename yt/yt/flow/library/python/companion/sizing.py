"""Companion-process count sizing for the Python companion.

The pure ``resolve_companion_process_count`` decides how many CPython interpreters to
pre-fork: an explicit configured value wins; ``0`` means "auto" — round the cgroup CPU
quota up and clamp to ``[1, cap]``. ``read_cpu_quota`` is the thin environment glue
that reads that quota from the host's cgroup filesystem.
"""

import logging
import math

log = logging.getLogger(__name__)

# A forked companion process is a full interpreter + gRPC server process (its own
# Solomon / ProfEncode thread pools), not a thread, so the fan-out ceiling stays
# bounded: 16 companion interpreters already saturate a sizeable CPU allocation, and
# going higher risks exhausting the job's thread/memory budget. Overridable via the
# ``cap`` argument.
DEFAULT_CAP = 16


def resolve_companion_process_count(configured, cpu_quota, cap=DEFAULT_CAP):
    """Decide the companion process count.

    - ``configured > 0`` wins (capped at ``cap``, with a WARNING when clamped).
    - ``configured == 0`` -> auto: only a *finite, explicit* quota drives
      ``ceil(cpu_quota)`` clamped to ``[1, cap]``. An unlimited / unreadable / unknown
      quota (``inf`` or ``None``, the common container case where no CPU limit is set)
      resolves to the conservative default of 1 — "no limit" means "do not fan out",
      not "fork ``cap`` heavyweight interpreters".
    """
    if configured > 0:
        if configured > cap:
            log.warning(
                "Configured companion_process_count %d exceeds the cap %d; clamping to %d",
                configured,
                cap,
                cap,
            )
        return min(configured, cap)
    if cpu_quota is None or math.isinf(cpu_quota) or cpu_quota <= 0:
        return 1
    return max(1, min(math.ceil(cpu_quota), cap))


def _parse_proc_self_cgroup():
    """Parse ``/proc/self/cgroup`` into (v2_path, v1_cpu_path); either may be ``None``."""
    v2_path = None
    v1_cpu_path = None
    try:
        with open("/proc/self/cgroup", "r") as f:
            for line in f:
                parts = line.strip().split(":", 2)
                if len(parts) != 3:
                    continue
                hierarchy_id, controllers, path = parts
                if hierarchy_id == "0" and not controllers:
                    v2_path = path
                elif "cpu" in controllers.split(","):
                    v1_cpu_path = path
    except OSError:
        pass
    return v2_path, v1_cpu_path


def _walk_up_dirs(root, rel_path):
    """Yield ``root/rel_path`` and every parent up to (and including) ``root``."""
    parts = [p for p in (rel_path or "").split("/") if p]
    while parts:
        yield "/".join([root] + parts)
        parts.pop()
    yield root


def _read_v2_quota(directory):
    """Quota in cores from ``<directory>/cpu.max``; ``inf`` for "max", ``None`` if unreadable."""
    try:
        with open(f"{directory}/cpu.max", "r") as f:
            quota_str, period_str = f.read().strip().split()
        if quota_str == "max":
            return float("inf")
        return int(quota_str) / int(period_str)
    except (OSError, ValueError):
        return None


def _read_v1_quota(directory):
    """Quota in cores from v1 ``cfs_quota_us/cfs_period_us``; non-positive quota is unlimited."""
    try:
        with open(f"{directory}/cpu.cfs_quota_us", "r") as f:
            quota = int(f.read().strip())
        with open(f"{directory}/cpu.cfs_period_us", "r") as f:
            period = int(f.read().strip())
        if quota <= 0:
            return float("inf")
        return quota / period
    except (OSError, ValueError):
        return None


def read_cpu_quota():
    """Read this process's effective CPU quota in core-equivalents.

    The quota is the tightest limit on the path from this process's own cgroup
    (resolved via ``/proc/self/cgroup``) up to the cgroup mount root. Reading only the
    root would miss the real limit on hosts without cgroup namespaces, where the
    container's quota lives in a nested cgroup while the root reports "unlimited".

    Returns ``float('inf')`` for "unlimited" and ``None`` if neither cgroup v2 nor v1 is
    readable. Both resolve to the conservative single-process default in
    ``resolve_companion_process_count`` — the host CPU count is deliberately *not* used
    as a fallback, since a CI/dev container with no CPU limit reports the whole host's
    cores and would otherwise fan out far beyond the job's real allocation.
    """
    v2_self, v1_self = _parse_proc_self_cgroup()

    for label, root, rel_path, read_quota in (
        ("v2", "/sys/fs/cgroup", v2_self, _read_v2_quota),
        ("v1", "/sys/fs/cgroup/cpu", v1_self, _read_v1_quota),
    ):
        readings = [
            (quota, directory)
            for directory in _walk_up_dirs(root, rel_path)
            if (quota := read_quota(directory)) is not None
        ]
        if readings:
            quota, directory = min(readings, key=lambda reading: reading[0])
            log.info("Resolved CPU quota from cgroup %s (Quota: %s, Source: %s)", label, quota, directory)
            return quota

    log.info("CPU quota is unknown (neither cgroup v2 nor v1 is readable)")
    return None
