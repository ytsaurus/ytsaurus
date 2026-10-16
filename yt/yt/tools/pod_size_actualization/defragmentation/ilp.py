"""
ILP solver orchestration for defragmentation.

Runs the ILP placement solver (pulp + highspy) in a separate system-Python
subprocess to avoid conflicts with the hermetic binary's protobuf version.

Scripts are extracted from embedded Arcadia resources (RESOURCE_FILES in ya.make)
to a temp directory on first call, then reused for the process lifetime.
"""

import os

from ..lib.subprocess_runner import extract_resources_to_tmpdir, run_solver_subprocess

_SCRIPTS_DIR: str | None = None

_RESOURCES = {
    'shared.py': 'defrag_solver/scripts/shared.py',
    'solver.py': 'defrag_solver/scripts/solver.py',
}


def _ensure_scripts_extracted() -> str:
    global _SCRIPTS_DIR
    if _SCRIPTS_DIR is not None and os.path.isfile(os.path.join(_SCRIPTS_DIR, 'solver.py')):
        return _SCRIPTS_DIR
    _SCRIPTS_DIR = extract_resources_to_tmpdir('defrag_solver_scripts_', _RESOURCES)
    return _SCRIPTS_DIR


def call_ilp_solver(payload: dict, verbose: bool) -> tuple:
    """Serialize payload, run solver.py in system Python, return (success, k_values, placement).

    payload keys (see solver.py docstring):
        cluster_dict, config_dict, pod_counts, weights,
        greedy_placement, time_limit_sec, verbose
    """
    scripts_dir = _ensure_scripts_extracted()
    result = run_solver_subprocess(
        solver_path=os.path.join(scripts_dir, 'solver.py'),
        payload=payload,
        verbose=verbose,
        failure_return=(False, {}, {}, None),
        log_tag='ilp_solver',
    )
    return result
