#!/usr/bin/env python3
"""
Subprocess entry point for CP-SAT stage solving.

Usage:
    python3 solver.py <payload_pkl> <output_pkl>

Reads a payload dict from <payload_pkl>, runs the requested stage solver,
and writes the result tuple to <output_pkl>.

bundle_groups and hosts are serialized as plain dicts (via dataclasses.asdict)
and reconstructed here — no custom class pickling required.

Stage0/stage1 return: (success: bool, host_cost_int, res_df, sizes_df, assign_df, patterns_df, phys_hosts_df)
Stage2 return: (status: int, res_df, sizes_df, assign_df, patterns_df, phys_hosts_df)
"""

import os
import pickle
import sys

# ---------------------------------------------------------------------------
# Bootstrap: locate scripts directory and set up sys.path
# ---------------------------------------------------------------------------

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _SCRIPTS_DIR)

# Add system site-packages as fallback so ortools is importable from the porto
# layer even when system Python's default path doesn't include it.
# Append (not insert) to avoid shadowing packages already provided by the venv.
import glob as _glob

for _sp in _glob.glob('/usr/local/lib/python3*/dist-packages') + _glob.glob('/usr/lib/python3/dist-packages'):
    if _sp not in sys.path:
        sys.path.append(_sp)

# ---------------------------------------------------------------------------
# Imports (after path setup)
# ---------------------------------------------------------------------------

import shared as cfg  # noqa: E402 (local module from _SCRIPTS_DIR)
import model as _model  # noqa: E402
from shared import BundleGroup, BundleInstances, ContainerType, Host  # noqa: E402

from ortools.sat.python import cp_model  # noqa: E402

# ---------------------------------------------------------------------------
# Payload deserialization
#
# bundle_groups and hosts arrive as plain dicts (serialized via
# dataclasses.asdict() in optimize.py)
# ---------------------------------------------------------------------------


def _bg_from_dict(d: dict) -> BundleGroup:
    d = dict(d)
    d['bundles_by_cluster'] = {
        cluster: [
            BundleInstances(
                container_type=ContainerType(**bi['container_type']),
                **{k: v for k, v in bi.items() if k != 'container_type'},
            )
            for bi in blist
        ]
        for cluster, blist in d['bundles_by_cluster'].items()
    }
    return BundleGroup(**d)


def _load_payload(path: str) -> dict:
    with open(path, 'rb') as f:
        payload = pickle.load(f)
    payload['bundle_groups'] = [_bg_from_dict(d) for d in payload['bundle_groups']]
    payload['hosts'] = [Host(**d) for d in payload['hosts']]
    return payload


def _dump_output(result: tuple, path: str) -> None:
    with open(path, 'wb') as f:
        pickle.dump(result, f)


# ---------------------------------------------------------------------------
# Stage solver functions
# ---------------------------------------------------------------------------


def _run_cpsat(model_obj, host_cost_expr, objective_expr, objective_mode, time_limit_sec, workers, seed, verbose):
    """Set up CpSolver, optionally add host_cost_cap, run, return (status, solver)."""
    if objective_mode == 'minimize_host':
        model_obj.Minimize(host_cost_expr)
    elif objective_mode == 'minimize_container':
        model_obj.Minimize(objective_expr)
    elif objective_mode == 'maximize_container':
        model_obj.Maximize(objective_expr)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(time_limit_sec)
    solver.parameters.num_search_workers = int(workers)
    solver.parameters.log_search_progress = bool(verbose)
    solver.parameters.randomize_search = True
    solver.parameters.random_seed = int(seed)
    status = solver.Solve(model_obj)
    return status, solver


def _solve_stage01(payload: dict, sizes_fixed: bool, assignments_fixed: bool) -> tuple:
    """Common logic for stage0 and stage1 (minimize host cost)."""
    build = _model._build_model(
        bundle_groups=payload['bundle_groups'],
        hosts=payload['hosts'],
        K_node=payload['K_node'],
        K_proxy=payload['K_proxy'],
        resource_coefficients=payload['resource_coefficients'],
        node_catalog_cpu=payload['node_catalog_cpu'],
        node_catalog_mem=payload['node_catalog_mem'],
        node_catalog_net=payload['node_catalog_net'],
        proxy_catalog_cpu=payload['proxy_catalog_cpu'],
        proxy_catalog_mem=payload['proxy_catalog_mem'],
        proxy_catalog_net=payload['proxy_catalog_net'],
        warm_start_data=payload.get('warm_start_data'),
        prev_sizes_df=payload.get('prev_sizes_df'),
        max_changed_sizes=payload.get('max_changed_sizes'),
        fixed_container_ids=payload.get('fixed_container_ids'),
        bundle_size_fixed=payload.get('bundle_size_fixed', False),
        n_min_tables=payload['n_min_tables'],
        infeasible_catalog=payload['infeasible_catalog'],
        warm_start_size_based_x_hints=payload.get('warm_start_size_based_x_hints', False),
        min_numa_frac=payload.get('min_numa_frac', cfg.MIN_NUMA_FRAC),
        sizes_fixed=sizes_fixed,
        assignments_fixed=assignments_fixed,
        verbose=payload.get('verbose', False),
    )
    model_obj = build['model']
    host_cost_expr = build['host_cost_int_expr']
    model_obj.Minimize(host_cost_expr)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(payload.get('time_limit_sec', 60))
    solver.parameters.num_search_workers = int(payload.get('workers', 4))
    solver.parameters.log_search_progress = bool(payload.get('verbose', False))
    solver.parameters.randomize_search = True
    solver.parameters.random_seed = int(payload.get('seed', 0))

    status = solver.Solve(model_obj)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return (False, None, None, None, None, None, None)

    host_cost_int = int(solver.Value(host_cost_expr))
    res_df, sizes_df, assign_df, patterns_df, phys_hosts_df = _model._extract_solution(solver, build, status)
    return (True, host_cost_int, res_df, sizes_df, assign_df, patterns_df, phys_hosts_df)


def _solve_stage3(payload: dict) -> tuple:
    """Stage 3: minimize container cost with no host topology constraints."""
    build = _model._build_model_stage3(
        bundle_groups=payload['bundle_groups'],
        K_node=payload['K_node'],
        K_proxy=payload['K_proxy'],
        resource_coefficients=payload['resource_coefficients'],
        node_catalog_cpu=payload['node_catalog_cpu'],
        node_catalog_mem=payload['node_catalog_mem'],
        node_catalog_net=payload['node_catalog_net'],
        proxy_catalog_cpu=payload['proxy_catalog_cpu'],
        proxy_catalog_mem=payload['proxy_catalog_mem'],
        proxy_catalog_net=payload['proxy_catalog_net'],
        warm_start_data=payload.get('warm_start_data'),
        prev_sizes_df=payload.get('prev_sizes_df'),
        max_changed_sizes=payload.get('max_changed_sizes'),
        fixed_container_ids=payload.get('fixed_container_ids'),
        n_min_tables=payload['n_min_tables'],
        infeasible_catalog=payload['infeasible_catalog'],
        warm_start_size_based_x_hints=payload.get('warm_start_size_based_x_hints', False),
        verbose=payload.get('verbose', False),
    )
    model_obj = build['model']
    cc_expr = build['container_cost_int_expr']
    if isinstance(cc_expr, int):
        model_obj.Minimize(0)
    else:
        model_obj.Minimize(cc_expr)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(payload.get('time_limit_sec', 120))
    solver.parameters.num_search_workers = int(payload.get('workers', 4))
    solver.parameters.log_search_progress = bool(payload.get('verbose', False))
    solver.parameters.randomize_search = True
    solver.parameters.random_seed = int(payload.get('seed', 0))

    status = solver.Solve(model_obj)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return (False, None, None, None, None, None, None)

    cc_int = solver.Value(cc_expr) if not isinstance(cc_expr, int) else cc_expr
    res_df, sizes_df, assign_df, patterns_df, phys_hosts_df = _model._extract_solution(
        solver,
        build,
        status,
    )
    # Position [1] holds container_cost_int so top_k_selector sorts by container cost.
    return (True, int(cc_int), res_df, sizes_df, assign_df, patterns_df, phys_hosts_df)


def _solve_stage2(payload: dict) -> tuple:
    """Stage 2: minimize (or maximize) container cost with host_cost_cap."""
    build = _model._build_model(
        bundle_groups=payload['bundle_groups'],
        hosts=payload['hosts'],
        K_node=payload['K_node'],
        K_proxy=payload['K_proxy'],
        resource_coefficients=payload['resource_coefficients'],
        node_catalog_cpu=payload['node_catalog_cpu'],
        node_catalog_mem=payload['node_catalog_mem'],
        node_catalog_net=payload['node_catalog_net'],
        proxy_catalog_cpu=payload['proxy_catalog_cpu'],
        proxy_catalog_mem=payload['proxy_catalog_mem'],
        proxy_catalog_net=payload['proxy_catalog_net'],
        warm_start_data=payload.get('warm_start_data_stage2'),
        prev_sizes_df=payload.get('prev_sizes_df'),
        max_changed_sizes=payload.get('max_changed_sizes'),
        fixed_container_ids=payload.get('fixed_container_ids'),
        n_min_tables=payload['n_min_tables'],
        infeasible_catalog=payload['infeasible_catalog'],
        warm_start_size_based_x_hints=payload.get('warm_start_size_based_x_hints', False),
        min_numa_frac=payload.get('min_numa_frac', cfg.MIN_NUMA_FRAC),
        verbose=payload.get('verbose', False),
    )
    model_obj = build['model']
    host_cost_expr = build['host_cost_int_expr']
    container_cost_expr = build['container_cost_int_expr']

    model_obj.Add(host_cost_expr <= int(payload['host_cost_cap_int']))
    if payload.get('maximize_container_cost', False):
        model_obj.Maximize(container_cost_expr)
    else:
        model_obj.Minimize(container_cost_expr)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(payload.get('time_limit_sec', 60))
    solver.parameters.num_search_workers = int(payload.get('workers', 4))
    solver.parameters.log_search_progress = bool(payload.get('verbose', False))
    solver.parameters.randomize_search = True
    solver.parameters.random_seed = int(payload.get('seed', 0))

    status = solver.Solve(model_obj)
    res_df, sizes_df, assign_df, patterns_df, phys_hosts_df = _model._extract_solution(solver, build, status)
    # Return status as int so caller doesn't need cp_model imported
    return (int(status), res_df, sizes_df, assign_df, patterns_df, phys_hosts_df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Ensure print() output is immediately flushed when stdout is not a TTY
    # (e.g. when the parent process writes to a log file).
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <payload_pkl> <output_pkl>", file=sys.stderr)
        sys.exit(1)

    payload_path = sys.argv[1]
    output_path = sys.argv[2]

    payload = _load_payload(payload_path)
    stage = payload['stage']

    if stage == 'stage0':
        result = _solve_stage01(payload, sizes_fixed=True, assignments_fixed=True)
    elif stage == 'stage1':
        result = _solve_stage01(payload, sizes_fixed=False, assignments_fixed=False)
    elif stage == 'stage2':
        result = _solve_stage2(payload)
    elif stage == 'stage3':
        result = _solve_stage3(payload)
    else:
        raise ValueError(f"Unknown stage: {stage!r}")

    _dump_output(result, output_path)


if __name__ == '__main__':
    main()
