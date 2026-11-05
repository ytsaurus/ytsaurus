"""
Parallel optimization orchestration: stage solvers, beam search, refinement.

Stage solvers run in separate system-Python subprocesses to avoid the
protobuf version conflict between the hermetic binary (protobuf 22.5 static)
and ortools' bundled libprotobuf.so (protobuf 5.28+).
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor

from .scripts import shared as cfg
from .results import build_warm_start_from_solution
from ..lib.subprocess_runner import extract_resources_to_tmpdir, run_solver_subprocess

# CP-SAT status integer codes (from ortools.sat.pb2.CpSolverStatus).
# Listed here so we never import ortools in the hermetic binary.
_CPSAT_FEASIBLE = 2
_CPSAT_OPTIMAL = 4


# ---------------------------------------------------------------------------
# Solver script extraction
# ---------------------------------------------------------------------------

_SCRIPTS_DIR: str | None = None

_RESOURCES = {
    'shared.py': 'pod_solver/scripts/shared.py',
    'model.py': 'pod_solver/scripts/model.py',
    'solver.py': 'pod_solver/scripts/solver.py',
}


def _ensure_scripts_extracted() -> str:
    global _SCRIPTS_DIR
    if _SCRIPTS_DIR is not None and os.path.isfile(os.path.join(_SCRIPTS_DIR, 'solver.py')):
        return _SCRIPTS_DIR
    _SCRIPTS_DIR = extract_resources_to_tmpdir('pod_solver_scripts_', _RESOURCES)
    return _SCRIPTS_DIR


# ---------------------------------------------------------------------------
# Subprocess solver call
# ---------------------------------------------------------------------------


def _call_solver(payload: dict, verbose: bool) -> tuple:
    """Serialize payload, run solver.py in system Python, return result tuple."""
    scripts_dir = _ensure_scripts_extracted()
    result = run_solver_subprocess(
        solver_path=os.path.join(scripts_dir, 'solver.py'),
        payload=payload,
        verbose=verbose,
        failure_return=(False, None, None, None, None, None, None),
        log_tag='solver',
    )
    return result


# ---------------------------------------------------------------------------
# Parallel worker runner
# ---------------------------------------------------------------------------


def _run_single_worker(worker_cfg: dict) -> tuple:
    stage = worker_cfg['_stage']
    verbose = worker_cfg.get('verbose', False)
    payload = {k: v for k, v in worker_cfg.items() if not k.startswith('_')}
    payload['stage'] = stage
    try:
        return _call_solver(payload, verbose)
    except Exception as exc:
        import traceback

        print(
            f"ERROR in worker (stage={stage}): {exc}\n{traceback.format_exc()}",
            file=sys.stderr,
        )
        return (False, None, None, None, None, None, None)


def _run_parallel_workers(configs: list, selector, verbose: bool, parallel_runs=None):
    if not configs:
        return selector(configs, [])
    if verbose:
        # Sequential in verbose mode so output is readable
        results = [_run_single_worker(cfg_item) for cfg_item in configs]
    else:
        pool_size = len(configs) if parallel_runs is None else min(int(parallel_runs), len(configs))
        # Thread pool: each thread just spawns a subprocess, so GIL is not an issue
        with ThreadPoolExecutor(max_workers=pool_size) as pool:
            futures = [pool.submit(_run_single_worker, cfg_item) for cfg_item in configs]
            results = [f.result() for f in futures]
    return selector(configs, results)


# ---------------------------------------------------------------------------
# Shared payload builder
# ---------------------------------------------------------------------------


def _shared_payload(
    bundle_groups,
    hosts,
    K_node,
    K_proxy,
    resource_coefficients,
    node_catalog_cpu,
    node_catalog_mem,
    node_catalog_net,
    proxy_catalog_cpu,
    proxy_catalog_mem,
    proxy_catalog_net,
    workers,
    verbose,
    min_numa_frac,
    n_min_tables,
    infeasible_catalog,
) -> dict:
    import dataclasses

    return dict(
        bundle_groups=[dataclasses.asdict(bg) for bg in bundle_groups],
        hosts=[dataclasses.asdict(h) for h in hosts],
        K_node=K_node,
        K_proxy=K_proxy,
        resource_coefficients=resource_coefficients,
        node_catalog_cpu=node_catalog_cpu,
        node_catalog_mem=node_catalog_mem,
        node_catalog_net=node_catalog_net,
        proxy_catalog_cpu=proxy_catalog_cpu,
        proxy_catalog_mem=proxy_catalog_mem,
        proxy_catalog_net=proxy_catalog_net,
        workers=workers,
        verbose=verbose,
        min_numa_frac=min_numa_frac,
        n_min_tables=n_min_tables,
        infeasible_catalog=infeasible_catalog,
    )


# ---------------------------------------------------------------------------
# Main optimization loop
# ---------------------------------------------------------------------------


def optimize_cpsat(
    bundle_groups,
    hosts,
    K_node,
    K_proxy,
    resource_coefficients,
    node_catalog_cpu,
    node_catalog_mem,
    node_catalog_net,
    proxy_catalog_cpu,
    proxy_catalog_mem,
    proxy_catalog_net,
    data_node_configs=None,
    slot_cfgs=None,
    top_k=1,
    parallel_runs=32,
    tasks_per_stage=32,
    workers_per_run=4,
    seed_base=10000,
    run_stage0=False,
    stage0_time_limit_sec=60,
    run_stage0b=False,
    stage0b_time_limit_sec=600,
    run_stage1=True,
    stage1_time_limit_sec=120,
    run_stage2=True,
    stage2_time_limit_sec=120,
    stage2_host_cost_slack_pct=cfg.STAGE2_HOST_COST_SLACK_PCT,
    stage2_host_cost_slack_min_int=cfg.STAGE2_HOST_COST_SLACK_MIN_INT,
    cost_scale=cfg.COST_SCALE,
    progress_hook=None,
    verbose=False,
    warm_start_size_based_x_hints=False,
    min_numa_frac=cfg.MIN_NUMA_FRAC,
    globally_fixed_container_ids=None,
    n_min_tables=None,
    infeasible_catalog=None,
    stage2_maximize_container_cost=False,
    run_stage3=False,
    stage3_time_limit_sec=120,
):
    if slot_cfgs is None:
        slot_cfgs = [{"warm_start_data": None, "prev_sizes_df": None, "max_changed_sizes": None}]
    beam_count = len(slot_cfgs)

    assert n_min_tables is not None and infeasible_catalog is not None

    _shared = _shared_payload(
        bundle_groups=bundle_groups,
        hosts=hosts,
        K_node=K_node,
        K_proxy=K_proxy,
        resource_coefficients=resource_coefficients,
        node_catalog_cpu=node_catalog_cpu,
        node_catalog_mem=node_catalog_mem,
        node_catalog_net=node_catalog_net,
        proxy_catalog_cpu=proxy_catalog_cpu,
        proxy_catalog_mem=proxy_catalog_mem,
        proxy_catalog_net=proxy_catalog_net,
        workers=int(workers_per_run),
        verbose=verbose,
        min_numa_frac=min_numa_frac,
        n_min_tables=n_min_tables,
        infeasible_catalog=infeasible_catalog,
    )

    def top_k_selector(stage_tag, configs, results):
        valid = [
            (cfg_item["_slot"], cfg_item.get("_mode", "free"), r[1], r[2], r[3], r[4], r[5], r[6])
            for cfg_item, r in zip(configs, results)
            if r[0]
        ]
        if not valid and configs:
            print(f"WARNING: [{stage_tag}] all {len(configs)} task(s) returned no solution")
        valid.sort(key=lambda x: x[2])
        beam = []
        for rank, (slot, mode, hc_int, res_df, sizes_df, assign_df, patterns_df, phys_hosts_df) in enumerate(
            valid[:top_k]
        ):
            hc_int = int(hc_int)
            tag = f"{stage_tag}_rank{rank}_slot{slot}_{mode}" if top_k > 1 else f"{stage_tag}_{mode}"
            res_out = res_df.copy()
            res_out["stage1_best_host_cost"] = hc_int / cost_scale
            res_out["stage2_ran"] = False
            if progress_hook is not None:
                progress_hook(tag, res_out, sizes_df, assign_df, patterns_df, phys_hosts_df)
            beam.append(
                dict(
                    res_df=res_out,
                    sizes_df=sizes_df,
                    assign_df=assign_df,
                    patterns_df=patterns_df,
                    phys_hosts_df=phys_hosts_df,
                    host_cost=hc_int / cost_scale,
                    host_cost_int=hc_int,
                    slot_idx=slot,
                )
            )
        return beam

    # --- Stage 0 ---
    slots_with_ws = [i for i, sc in enumerate(slot_cfgs) if sc.get("warm_start_data") is not None]
    if run_stage0 and slots_with_ws:
        print("Running stage0...")
        stage0_slot_cfgs = [slot_cfgs[i] for i in slots_with_ws]
        stage0_beam_count = len(stage0_slot_cfgs)
        stage0_configs = [
            dict(
                _stage='stage0',
                _slot=si % stage0_beam_count,
                **_shared,
                warm_start_size_based_x_hints=False,
                warm_start_data=stage0_slot_cfgs[si % stage0_beam_count].get("warm_start_data"),
                prev_sizes_df=stage0_slot_cfgs[si % stage0_beam_count].get("prev_sizes_df"),
                max_changed_sizes=stage0_slot_cfgs[si % stage0_beam_count].get("max_changed_sizes"),
                seed=int(seed_base) + si,
                time_limit_sec=float(stage0_time_limit_sec),
            )
            for si in range(int(tasks_per_stage))
        ]
        beam_s0 = _run_parallel_workers(
            stage0_configs,
            lambda cfgs, res: top_k_selector("stage0", cfgs, res),
            verbose,
            parallel_runs=parallel_runs,
        )
        if beam_s0:
            slot_cfgs = [
                {
                    "warm_start_data": build_warm_start_from_solution(
                        cand["sizes_df"],
                        cand["assign_df"],
                        cand["patterns_df"],
                        K_node,
                        K_proxy,
                    ),
                    "prev_sizes_df": None,
                    "max_changed_sizes": None,
                }
                for cand in beam_s0
            ]
            beam_count = len(slot_cfgs)

    # --- Stage 0b ---
    beam_s0b = []
    if run_stage0b and slot_cfgs:
        print("Running stage0b...")
        stage0b_slot_cfgs = slot_cfgs
        stage0b_beam_count = len(stage0b_slot_cfgs)
        stage0b_configs = [
            dict(
                _stage='stage1',
                _slot=si % stage0b_beam_count,
                _mode="stage0b",
                **_shared,
                warm_start_size_based_x_hints=False,
                warm_start_data=stage0b_slot_cfgs[si % stage0b_beam_count].get("warm_start_data"),
                prev_sizes_df=stage0b_slot_cfgs[si % stage0b_beam_count].get("prev_sizes_df"),
                max_changed_sizes=stage0b_slot_cfgs[si % stage0b_beam_count].get("max_changed_sizes"),
                bundle_size_fixed=True,
                fixed_container_ids=globally_fixed_container_ids,
                seed=int(seed_base) + si,
                time_limit_sec=float(stage0b_time_limit_sec),
            )
            for si in range(int(tasks_per_stage))
        ]
        beam_s0b = _run_parallel_workers(
            stage0b_configs,
            lambda cfgs, res: top_k_selector("stage0b", cfgs, res),
            verbose,
            parallel_runs=parallel_runs,
        )
        if beam_s0b:
            slot_cfgs = [
                {
                    "warm_start_data": build_warm_start_from_solution(
                        cand["sizes_df"],
                        cand["assign_df"],
                        cand["patterns_df"],
                        K_node,
                        K_proxy,
                    ),
                    "prev_sizes_df": None,
                    "max_changed_sizes": None,
                }
                for cand in beam_s0b
            ]
            beam_count = len(slot_cfgs)

    # --- Stage 3 (host-free container cost minimization) ---
    if run_stage3:
        print("Running stage3...")
        stage3_configs = [
            dict(
                _stage='stage3',
                _slot=si % beam_count,
                _mode="free",
                **_shared,
                warm_start_data=slot_cfgs[si % beam_count].get("warm_start_data"),
                prev_sizes_df=slot_cfgs[si % beam_count].get("prev_sizes_df"),
                max_changed_sizes=slot_cfgs[si % beam_count].get("max_changed_sizes"),
                fixed_container_ids=globally_fixed_container_ids,
                seed=int(seed_base) + si,
                time_limit_sec=float(stage3_time_limit_sec),
            )
            for si in range(int(tasks_per_stage))
        ]
        beam = _run_parallel_workers(
            stage3_configs,
            lambda cfgs, res: top_k_selector("stage3", cfgs, res),
            verbose,
            parallel_runs=parallel_runs,
        )
        if not beam:
            print("WARNING: [stage3] returned no solutions")
        return beam

    # --- Stage 1 ---
    if run_stage1:
        stage1_configs = [
            dict(
                _stage='stage1',
                _slot=si % beam_count,
                _mode="free",
                **_shared,
                warm_start_size_based_x_hints=warm_start_size_based_x_hints,
                warm_start_data=slot_cfgs[si % beam_count].get("warm_start_data"),
                prev_sizes_df=slot_cfgs[si % beam_count].get("prev_sizes_df"),
                max_changed_sizes=slot_cfgs[si % beam_count].get("max_changed_sizes"),
                fixed_container_ids=globally_fixed_container_ids,
                bundle_size_fixed=False,
                seed=int(seed_base) + si,
                time_limit_sec=float(stage1_time_limit_sec),
            )
            for si in range(int(tasks_per_stage))
        ]
        beam = _run_parallel_workers(
            stage1_configs,
            lambda cfgs, res: top_k_selector("stage1", cfgs, res),
            verbose,
            parallel_runs=parallel_runs,
        )
        if not beam:
            print("WARNING: [stage1] returned no solutions")
            return beam
        if not run_stage2:
            beam.sort(key=lambda x: x["host_cost"])
            return beam
    else:
        beam = beam_s0b if (run_stage0b and beam_s0b) else []
        if not beam:
            print("WARNING: [stage1 skipped] no beam from stage0b")
            return beam
        if not run_stage2:
            beam.sort(key=lambda x: x["host_cost"])
            return beam

    # --- Stage 2 ---
    stage2_configs = []
    for si in range(int(tasks_per_stage)):
        cand_idx = si % len(beam)
        cand = beam[cand_idx]
        hc_int = cand["host_cost_int"]
        delta_int = max(int(stage2_host_cost_slack_min_int), int(round(hc_int * float(stage2_host_cost_slack_pct))))
        ws_s2 = build_warm_start_from_solution(
            cand["sizes_df"],
            cand["assign_df"],
            cand["patterns_df"],
            K_node,
            K_proxy,
        )
        stage2_configs.append(
            dict(
                _stage='stage2',
                _cand_idx=cand_idx,
                **_shared,
                warm_start_size_based_x_hints=False,
                warm_start_data_stage2=ws_s2,
                prev_sizes_df=None,
                max_changed_sizes=None,
                fixed_container_ids=globally_fixed_container_ids,
                host_cost_cap_int=hc_int + delta_int,
                seed=int(seed_base) + si,
                time_limit_sec=float(stage2_time_limit_sec),
                maximize_container_cost=stage2_maximize_container_cost,
            )
        )

    def best_per_beam_selector(configs, results):
        best_by_cand = {}
        for cfg_item, r in zip(configs, results):
            cand_idx = cfg_item["_cand_idx"]
            status, r2, s2, a2, p2, ph2 = r
            if status not in (_CPSAT_OPTIMAL, _CPSAT_FEASIBLE) or r2.empty:
                continue
            cc = float(r2["container_cost"].iloc[0])
            if cand_idx not in best_by_cand:
                best_by_cand[cand_idx] = (cc, (status, r2, s2, a2, p2, ph2))
            else:
                is_better = (
                    cc > best_by_cand[cand_idx][0] if stage2_maximize_container_cost else cc < best_by_cand[cand_idx][0]
                )
                if is_better:
                    best_by_cand[cand_idx] = (cc, (status, r2, s2, a2, p2, ph2))

        if not best_by_cand and configs:
            print(f"WARNING: [stage2] all {len(configs)} tasks returned no solution — falling back to stage1")

        final_beam = []
        for cand_idx, cand in enumerate(beam):
            slot = cand.get("slot_idx", "?")
            tag = f"stage2_rank{cand_idx}_slot{slot}" if len(beam) > 1 else "stage2"
            hc_int = cand["host_cost_int"]
            delta_int = max(int(stage2_host_cost_slack_min_int), int(round(hc_int * float(stage2_host_cost_slack_pct))))
            cap_int = hc_int + delta_int
            if cand_idx in best_by_cand:
                _, (status, r2, s2, a2, p2, ph2) = best_by_cand[cand_idx]
                r2 = r2.copy()
                r2["stage1_best_host_cost"] = hc_int / cost_scale
                r2["stage2_host_cost_cap"] = cap_int / cost_scale
                r2["stage2_ran"] = True
                if progress_hook is not None:
                    progress_hook(tag, r2, s2, a2, p2, ph2)
                final_beam.append(
                    dict(
                        res_df=r2,
                        sizes_df=s2,
                        assign_df=a2,
                        patterns_df=p2,
                        phys_hosts_df=ph2,
                        host_cost=float(r2["host_cost"].iloc[0]),
                        host_cost_int=int(round(float(r2["host_cost"].iloc[0]) * cost_scale)),
                    )
                )
            else:
                res_fb = cand["res_df"].copy()
                res_fb["stage2_host_cost_cap"] = cap_int / cost_scale
                if progress_hook is not None:
                    progress_hook(
                        tag, res_fb, cand["sizes_df"], cand["assign_df"], cand["patterns_df"], cand["phys_hosts_df"]
                    )
                final_beam.append(
                    dict(
                        res_df=res_fb,
                        sizes_df=cand["sizes_df"],
                        assign_df=cand["assign_df"],
                        patterns_df=cand["patterns_df"],
                        phys_hosts_df=cand["phys_hosts_df"],
                        host_cost=cand["host_cost"],
                        host_cost_int=cand["host_cost_int"],
                    )
                )
        final_beam.sort(key=lambda x: x["host_cost"])
        return final_beam

    return _run_parallel_workers(stage2_configs, best_per_beam_selector, verbose, parallel_runs=parallel_runs)


# ---------------------------------------------------------------------------
# Iterative beam refinement
# ---------------------------------------------------------------------------


def run_beam_refinement(
    initial_beam,
    bundle_groups,
    hosts,
    K_node,
    K_proxy,
    resource_coefficients,
    node_catalog_cpu,
    node_catalog_mem,
    node_catalog_net,
    proxy_catalog_cpu,
    proxy_catalog_mem,
    proxy_catalog_net,
    data_node_configs=None,
    beam_count=1,
    refine_iters=30,
    max_changed_sizes=3,
    parallel_runs=32,
    tasks_per_stage=32,
    workers_per_run=4,
    seed_base=20000,
    run_stage0=False,
    stage0_time_limit_sec=60,
    run_stage0b=False,
    stage0b_time_limit_sec=600,
    run_stage1=True,
    stage1_time_limit_sec=120,
    run_stage2=True,
    stage2_time_limit_sec=120,
    stage2_host_cost_slack_pct=cfg.STAGE2_HOST_COST_SLACK_PCT,
    stage2_host_cost_slack_min_int=cfg.STAGE2_HOST_COST_SLACK_MIN_INT,
    cost_scale=cfg.COST_SCALE,
    progress_hook=None,
    on_improvement=None,
    verbose=False,
    warm_start_size_based_x_hints=False,
    min_numa_frac=cfg.MIN_NUMA_FRAC,
    globally_fixed_container_ids=None,
    n_min_tables=None,
    infeasible_catalog=None,
    run_stage3=False,
    stage3_time_limit_sec=120,
):
    beam = list(initial_beam)
    print(
        f">>> Iterative refinement (beam={beam_count}): iters={refine_iters}, "
        f"R={max_changed_sizes}, s1={stage1_time_limit_sec}s s2={stage2_time_limit_sec}s"
    )

    for it in range(int(refine_iters)):
        slot_cfgs = [
            {
                "warm_start_data": build_warm_start_from_solution(
                    beam[bi % len(beam)]["sizes_df"],
                    beam[bi % len(beam)]["assign_df"],
                    beam[bi % len(beam)]["patterns_df"],
                    K_node,
                    K_proxy,
                ),
                "prev_sizes_df": beam[bi % len(beam)]["sizes_df"],
                "max_changed_sizes": max_changed_sizes,
            }
            for bi in range(int(beam_count))
        ]

        new_candidates = optimize_cpsat(
            bundle_groups=bundle_groups,
            hosts=hosts,
            K_node=K_node,
            K_proxy=K_proxy,
            resource_coefficients=resource_coefficients,
            node_catalog_cpu=node_catalog_cpu,
            node_catalog_mem=node_catalog_mem,
            node_catalog_net=node_catalog_net,
            proxy_catalog_cpu=proxy_catalog_cpu,
            proxy_catalog_mem=proxy_catalog_mem,
            proxy_catalog_net=proxy_catalog_net,
            data_node_configs=data_node_configs,
            slot_cfgs=slot_cfgs,
            top_k=beam_count,
            parallel_runs=parallel_runs,
            tasks_per_stage=tasks_per_stage,
            workers_per_run=workers_per_run,
            seed_base=seed_base + it * 1000,
            run_stage0=run_stage0,
            stage0_time_limit_sec=stage0_time_limit_sec,
            run_stage0b=run_stage0b,
            stage0b_time_limit_sec=stage0b_time_limit_sec,
            run_stage1=run_stage1,
            stage1_time_limit_sec=stage1_time_limit_sec,
            run_stage2=run_stage2,
            stage2_time_limit_sec=stage2_time_limit_sec,
            stage2_host_cost_slack_pct=stage2_host_cost_slack_pct,
            stage2_host_cost_slack_min_int=stage2_host_cost_slack_min_int,
            cost_scale=cost_scale,
            progress_hook=progress_hook,
            verbose=verbose,
            warm_start_size_based_x_hints=warm_start_size_based_x_hints,
            min_numa_frac=min_numa_frac,
            globally_fixed_container_ids=globally_fixed_container_ids,
            n_min_tables=n_min_tables,
            infeasible_catalog=infeasible_catalog,
            run_stage3=run_stage3,
            stage3_time_limit_sec=stage3_time_limit_sec,
        )

        if not new_candidates:
            print(f"  Iter {it}: all stage1 runs failed")
            continue

        all_cands = beam + new_candidates
        all_cands.sort(key=lambda x: x["host_cost"])
        new_beam = all_cands[: int(beam_count)]

        if new_beam[0]["host_cost"] + 1e-12 < beam[0]["host_cost"]:
            print(f"  Iter {it}: improved {beam[0]['host_cost']:.6f} -> {new_beam[0]['host_cost']:.6f}")
            if on_improvement is not None:
                on_improvement(new_beam[0])
        else:
            print(f"  Iter {it}: no improvement (best={beam[0]['host_cost']:.6f})")
        beam = new_beam

    return beam
