"""
Generic subprocess solver runner for Nirvana operations.

Shared by defragmentation (pulp/HiGHS) and optimization (ortools/CP-SAT).
Both solvers follow the same protocol:
    python3 solver.py <payload.pkl> <output.pkl>
"""

import os
import pickle
import subprocess
import sys
import tempfile
import threading
from typing import Any, Dict


def extract_resources_to_tmpdir(prefix: str, resources: Dict[str, str]) -> str:
    """Extract Arcadia RESOURCE_FILES to a temporary directory.

    Args:
        prefix: prefix for the temp directory name.
        resources: mapping of {filename: resource_key} to extract.
                   Resource keys must match the paths in ya.make RESOURCE_FILES.

    Returns:
        Absolute path to the created temp directory.

    Raises:
        RuntimeError: if any resource key is missing from the binary.
    """
    from library.python.resource import resfs_read

    tmp = tempfile.mkdtemp(prefix=prefix)
    for fname, key in resources.items():
        data = resfs_read(key.encode())
        if data is None:
            raise RuntimeError(f"Resource {key!r} not found in binary")
        with open(os.path.join(tmp, fname), 'wb') as fh:
            fh.write(data)
    return tmp


def run_solver_subprocess(
    solver_path: str,
    payload: dict,
    verbose: bool,
    failure_return: Any = None,
    log_tag: str = 'solver',
) -> Any:
    """Pickle payload, run solver.py in system Python, return unpickled result.

    Args:
        solver_path: absolute path to solver.py.
        payload: dict to pass as input (pickled to a temp file).
        verbose: if True, stream subprocess stdout/stderr in real time via
                 Python's sys.stdout/sys.stderr so output appears in vh3 logs.
        failure_return: value returned when the output file is missing.
        log_tag: prefix used in error log messages.

    Returns:
        Unpickled result from the output pickle file, or failure_return if the
        solver produced no output.
    """
    with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as pf:
        payload_path = pf.name
        pickle.dump(payload, pf)

    output_path = payload_path + '.out.pkl'

    try:
        if verbose:
            proc = subprocess.Popen(
                ['python3', solver_path, payload_path, output_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            def _forward(src, dst):
                for line in src:
                    print(line, end='', file=dst, flush=True)

            t_out = threading.Thread(target=_forward, args=(proc.stdout, sys.stdout))
            t_err = threading.Thread(target=_forward, args=(proc.stderr, sys.stderr))
            t_out.start()
            t_err.start()
            proc.wait()
            t_out.join()
            t_err.join()
        else:
            proc = subprocess.run(
                ['python3', solver_path, payload_path, output_path],
                capture_output=True,
                text=True,
            )

        if proc.returncode != 0:
            stage = payload.get('stage', '')
            stage_str = f', stage={stage}' if stage else ''
            if not verbose:
                print(
                    f"[{log_tag}] subprocess failed (rc={proc.returncode}{stage_str}):\n{proc.stderr}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"[{log_tag}] subprocess failed (rc={proc.returncode}{stage_str})",
                    file=sys.stderr,
                )

        if not os.path.isfile(output_path):
            stage = payload.get('stage', '')
            stage_str = f' (stage={stage})' if stage else ''
            print(f"WARNING: [{log_tag}] no output file{stage_str}", file=sys.stderr)
            return failure_return

        with open(output_path, 'rb') as fh:
            return pickle.load(fh)
    finally:
        for p in (payload_path, output_path):
            try:
                os.unlink(p)
            except OSError:
                pass
