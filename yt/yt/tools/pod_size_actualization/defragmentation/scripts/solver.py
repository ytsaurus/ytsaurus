#!/usr/bin/env python3
"""
Subprocess entry point for defragmentation ILP solver.

Usage:
    python3 solver.py <payload_pkl> <output_pkl>

Payload (dict):
    cluster_dict:      serialized Cluster (from Cluster.to_dict())
    config_dict:       serialized ClusterConfig (from ClusterConfig.to_dict())
    pod_counts:        {config_name: int}  — required pods (before extras)
    weights:           {config_name: float}
    greedy_placement:  {config_name: {hostname: [numa_id_or_None, ...]}}
    time_limit_sec:    int
    verbose:           bool

Output (tuple):
    (success: bool, k_values: dict, placement: dict)
    placement: {hostname: {config_name: [numa_node_id_or_None, ...]}}
"""

import os
import pickle
import sys

# ---------------------------------------------------------------------------
# Bootstrap: set up sys.path for scripts dir + system dist-packages
# ---------------------------------------------------------------------------

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _SCRIPTS_DIR)

import glob as _glob

for _sp in _glob.glob('/usr/local/lib/python3*/dist-packages') + _glob.glob('/usr/lib/python3/dist-packages'):
    if _sp not in sys.path:
        sys.path.append(_sp)

# ---------------------------------------------------------------------------
# Imports (after path setup)
# ---------------------------------------------------------------------------

import shared  # noqa: E402 — local module from _SCRIPTS_DIR
from shared import (  # noqa: E402
    Cluster,
    ClusterConfig,
    AvailableResourcesRequest,
    Host,
    filter_host,
    get_disk_bandwidth_from_storage_class,
)

import pulp  # noqa: E402
import highspy  # noqa: E402

# ---------------------------------------------------------------------------
# HiGHS solver with MIP warm start
# ---------------------------------------------------------------------------


class _HiGHSMIPStart(pulp.HiGHS):
    """HiGHS solver with MIP warm start via highspy.setSolution()."""

    def __init__(self, mip_start=None, **kwargs):
        super().__init__(**kwargs)
        self._mip_start = mip_start or {}
        self.ilp_gap_pct = None

    def callSolver(self, lp):
        if self._mip_start:
            n = lp.solverModel.getNumCol()
            col_values = [0.0] * n
            for var in lp.variables():
                val = self._mip_start.get(var.name)
                if val is not None and hasattr(var, 'index'):
                    col_values[var.index] = float(val)
            sol = highspy.HighsSolution()
            sol.col_value = col_values
            lp.solverModel.setSolution(sol)
        lp.solverModel.run()
        try:
            _, primal_obj = lp.solverModel.getInfoValue('objective_function_value')
            _, dual_bnd = lp.solverModel.getInfoValue('mip_dual_bound')
            primal_obj = float(primal_obj)
            dual_bnd = float(dual_bnd)
            denom = max(1.0, abs(primal_obj))
            self.ilp_gap_pct = abs(primal_obj - dual_bnd) / denom * 100
        except Exception:
            pass


# ---------------------------------------------------------------------------
# ILP solver
# ---------------------------------------------------------------------------


def solve_placement_ilp(
    cluster: Cluster,
    pod_counts: dict,
    weights: dict,
    time_limit_sec: int = 3600,
    verbose: bool = True,
    greedy_placement: dict = None,
):
    """
    Solve the placement ILP.

    Returns:
        (success, k_values, placement)
        k_values:  {config_name: extra pods above pod_counts}
        placement: {hostname: {config_name: [numa_node_id_or_None, ...]}}
    """
    from io import StringIO

    cfg_map = cluster.config.pod_configurations
    antiaffinity = cluster.config.antiaffinity
    role_filter = cluster.config.role_specific_host_filter

    configs = [c for c in pod_counts if c in cfg_map]

    if verbose:
        print(f"Building ILP for {len(configs)} configs...")

    # Find eligible hosts per config (suppress filter output)
    eligible_hosts: dict = {}
    for config_name in configs:
        cfg = cfg_map[config_name]
        dummy_req = AvailableResourcesRequest(
            vcpu=cfg['vcpu'],
            memory=cfg['memory'],
            net=cfg['network'],
            disk_capacity=cfg['disk_capacity'],
            yt_role=cfg['yt_role'],
            antiaffinity=antiaffinity.get(cfg['yt_role']),
        )
        active_hosts = cluster.get_active_hosts()
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        try:
            eligible = [h for h in active_hosts if filter_host(h, dummy_req, role_filter)]
        finally:
            sys.stdout = old_stdout
        eligible_hosts[config_name] = eligible
        if verbose:
            print(f"  {config_name}: {len(eligible)} eligible hosts")

    all_hosts: dict = {}
    for hosts in eligible_hosts.values():
        for h in hosts:
            all_hosts[h.hostname] = h

    _CPU_SCALE = 1000.0
    _MEM_SCALE = 1024**3
    _NET_SCALE = 1024**2
    _DISK_SCALE = 1024**3
    _NUMA_RAM_SCALE = 1024

    def _s_cpu(v):
        return v / _CPU_SCALE

    def _s_mem(v):
        return v / _MEM_SCALE

    def _s_net(v):
        return v / _NET_SCALE

    def _s_disk(v):
        return v / _DISK_SCALE

    prob = pulp.LpProblem("defragmentation", pulp.LpMaximize)

    def vn(s: str) -> str:
        return s.replace('.', '_').replace('-', '_').replace(':', '_')

    x: dict = {}
    for config_name in configs:
        cfg = cfg_map[config_name]
        x[config_name] = {}
        for h in eligible_hosts[config_name]:
            hn = h.hostname
            eff = h.get_effective_free_resources()
            eff_cpu, eff_mem, eff_net, eff_disk_cap, eff_disk_bw = eff
            aa_existing = len(h.get_pods_by_role(cfg['yt_role']))
            aa_limit = antiaffinity.get(cfg['yt_role'], 9999)
            ub = min(
                int(eff_cpu / cfg['vcpu']) if cfg['vcpu'] > 0 else 9999,
                int(eff_mem / cfg['memory']) if cfg['memory'] > 0 else 9999,
                int(eff_net / cfg['network']) if cfg['network'] > 0 else 9999,
                int(eff_disk_cap / cfg['disk_capacity']) if cfg['disk_capacity'] > 0 else 9999,
                max(0, aa_limit - aa_existing),
            )
            x[config_name][hn] = pulp.LpVariable(
                f"x_{vn(config_name)}_{vn(hn)}", lowBound=0, upBound=max(0, ub), cat='Integer'
            )

    sum_w = sum(w for w in weights.values() if w > 0)
    k: dict = {
        c: pulp.LpVariable(
            f"k_{vn(c)}",
            lowBound=0,
            upBound=None if weights.get(c, 0) > 0 else 0,
            cat='Integer',
        )
        for c in configs
    }

    y: dict = {}
    for config_name in configs:
        y[config_name] = {}
        for host in eligible_hosts[config_name]:
            hn = host.hostname
            numa_count = max(len(host.numa_cpu_details), len(host.numa_memory_details))
            if numa_count > 0:
                y[config_name][hn] = [
                    pulp.LpVariable(f"y_{vn(config_name)}_{vn(hn)}_{n}", lowBound=0, cat='Integer')
                    for n in range(numa_count)
                ]

    t = pulp.LpVariable("t", lowBound=0, cat='Continuous')
    prob += t, "objective"
    for c in configs:
        w_c = weights.get(c, 0.0)
        if w_c > 0 and sum_w > 0:
            prob += (k[c] >= t * (w_c / sum_w), f"prop_{vn(c)}")

    for config_name in configs:
        prob += (
            pulp.lpSum(x[config_name].values()) == pod_counts.get(config_name, 0) + k[config_name],
            f"total_{vn(config_name)}",
        )

    for hn, host in all_hosts.items():
        eff = host.get_effective_free_resources()
        eff_cpu, eff_mem, eff_net, eff_disk_cap, eff_disk_bw = eff
        disk_bw_per_pod = get_disk_bandwidth_from_storage_class(host.disk_storage_class)

        host_vars = [(cn, x[cn][hn]) for cn in configs if hn in x.get(cn, {})]
        if not host_vars:
            continue

        prob += (pulp.lpSum(v * _s_cpu(cfg_map[cn]['vcpu']) for cn, v in host_vars) <= _s_cpu(eff_cpu), f"cpu_{vn(hn)}")
        prob += (
            pulp.lpSum(v * _s_mem(cfg_map[cn]['memory']) for cn, v in host_vars) <= _s_mem(eff_mem),
            f"mem_{vn(hn)}",
        )
        prob += (
            pulp.lpSum(v * _s_net(cfg_map[cn]['network']) for cn, v in host_vars) <= _s_net(eff_net),
            f"net_{vn(hn)}",
        )
        prob += (
            pulp.lpSum(v * _s_disk(cfg_map[cn]['disk_capacity']) for cn, v in host_vars) <= _s_disk(eff_disk_cap),
            f"dsk_{vn(hn)}",
        )
        prob += (
            pulp.lpSum(v for _, v in host_vars) * _s_disk(disk_bw_per_pod) <= _s_disk(eff_disk_bw),
            f"dbw_{vn(hn)}",
        )

        for role, limit in antiaffinity.items():
            existing = len(host.get_pods_by_role(role))
            role_vars = [v for cn, v in host_vars if cfg_map[cn]['yt_role'] == role]
            if role_vars:
                prob += (pulp.lpSum(role_vars) + existing <= limit, f"aa_{role}_{vn(hn)}")

        try:
            numa_resources = host.get_available_numa_resources()
        except Exception:
            numa_resources = []

        for config_name in configs:
            if hn not in y.get(config_name, {}):
                continue
            y_vars = y[config_name][hn]
            prob += (pulp.lpSum(y_vars) == x[config_name][hn], f"nl_{vn(config_name)}_{vn(hn)}")

        for n, numa_slot in enumerate(numa_resources):
            numa_cpu_exprs, numa_mem_exprs = [], []
            for config_name in configs:
                if hn in y.get(config_name, {}) and n < len(y[config_name][hn]):
                    yv = y[config_name][hn][n]
                    numa_cpu_exprs.append(yv * _s_cpu(cfg_map[config_name]['vcpu']))
                    numa_mem_exprs.append(yv * _s_mem(cfg_map[config_name]['memory']))
            if numa_cpu_exprs:
                prob += (pulp.lpSum(numa_cpu_exprs) <= _s_cpu(numa_slot.vcpu), f"nc_{vn(hn)}_{n}")
            if numa_mem_exprs:
                prob += (pulp.lpSum(numa_mem_exprs) <= numa_slot.ram / _NUMA_RAM_SCALE, f"nm_{vn(hn)}_{n}")

    if verbose:
        print(f"\nILP: {len(prob.variables())} variables, {len(prob.constraints)} constraints")

    # LP relaxation
    if verbose:
        print("Solving LP relaxation first...")
    _orig_cats = {v.name: v.cat for v in prob.variables()}
    try:
        for v in prob.variables():
            v.cat = pulp.constants.LpContinuous
            v.varValue = None
        prob.solve(pulp.HiGHS(msg=False, timeLimit=60))
        lp_status = pulp.LpStatus[prob.status]
        lp_obj = pulp.value(prob.objective) or 0.0
        if verbose:
            print(f"LP relaxation: status={lp_status}, objective={lp_obj:.1f}")
        if prob.status == -1:
            print("ERROR: LP relaxation is infeasible — check removed_counts vs cluster capacity")
            return False, {}, {}, None
    except Exception as e:
        if verbose:
            print(f"LP relaxation skipped: {e}")
    finally:
        for v in prob.variables():
            v.cat = _orig_cats[v.name]
            v.varValue = None

    # Greedy warm start
    mip_start = None
    if greedy_placement is not None:
        mip_start = {}
        total_greedy_extra = 0
        for cn in configs:
            numa_ids_by_host = greedy_placement.get(cn, {})
            total_placed = sum(len(v) for v in numa_ids_by_host.values())
            extra = max(0, total_placed - pod_counts.get(cn, 0))
            total_greedy_extra += extra
            mip_start[k[cn].name] = float(extra)
            for hn, var in x[cn].items():
                numa_ids = numa_ids_by_host.get(hn, [])
                mip_start[var.name] = float(len(numa_ids))
                if hn in y.get(cn, {}):
                    N = len(y[cn][hn])
                    counts = [0] * N
                    for nid in numa_ids:
                        if nid is not None and 0 <= nid < N:
                            counts[nid] += 1
                    for n, y_var in enumerate(y[cn][hn]):
                        mip_start[y_var.name] = float(counts[n])
        t_greedy = (
            min((extra / (weights.get(cn, 0) / sum_w)) for cn in configs if weights.get(cn, 0) > 0 and sum_w > 0)
            if any(weights.get(cn, 0) > 0 for cn in configs)
            else 0.0
        )
        mip_start[t.name] = float(t_greedy)
        if verbose:
            print(f"Warm start loaded: {total_greedy_extra} extra pods (t={t_greedy:.1f})")

    if verbose:
        print(f"Solving MIP (time limit: {time_limit_sec}s)...")

    solved = False
    _last_solver = None
    for make_solver, name in [
        (lambda: _HiGHSMIPStart(mip_start=mip_start, msg=int(verbose), timeLimit=time_limit_sec), "HiGHS"),
        (lambda: pulp.PULP_CBC_CMD(msg=int(verbose), timeLimit=time_limit_sec, warmStart=bool(mip_start)), "CBC"),
    ]:
        try:
            _last_solver = make_solver()
            prob.solve(_last_solver)
            solved = True
            if verbose:
                print(f"Solved with {name}")
            break
        except Exception as e:
            if verbose:
                print(f"{name} unavailable: {e}")

    if not solved:
        print("ERROR: no solver available, install highspy or CBC")
        return False, {}, {}, None

    status = pulp.LpStatus[prob.status]
    obj_val = pulp.value(prob.objective) or 0.0
    if verbose:
        print(f"Status: {status}, objective t (proportional scale): {obj_val:.1f}")

    if prob.status not in (1, -2):
        print(f"No feasible solution (status: {status})")
        return False, {}, {}, None

    k_values = {c: max(0, int(round(pulp.value(k[c]) or 0))) for c in configs}

    placement: dict = {}
    for config_name in configs:
        for hn, var in x[config_name].items():
            val = max(0, int(round(pulp.value(var) or 0)))
            if val <= 0:
                continue
            if hn not in placement:
                placement[hn] = {}
            if config_name not in placement[hn]:
                placement[hn][config_name] = []

            if hn in y.get(config_name, {}):
                for n, y_var in enumerate(y[config_name][hn]):
                    y_val = max(0, int(round(pulp.value(y_var) or 0)))
                    placement[hn][config_name].extend([n] * y_val)
            else:
                placement[hn][config_name].extend([None] * val)

    ilp_gap_pct = getattr(_last_solver, 'ilp_gap_pct', None)

    return True, k_values, placement, ilp_gap_pct


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass

    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <payload_pkl> <output_pkl>", file=sys.stderr)
        sys.exit(1)

    payload_path = sys.argv[1]
    output_path = sys.argv[2]

    with open(payload_path, 'rb') as f:
        payload = pickle.load(f)

    config = ClusterConfig.from_dict(payload['config_dict'])
    cluster = Cluster.from_dict(payload['cluster_dict'], config)

    result = solve_placement_ilp(
        cluster=cluster,
        pod_counts=payload['pod_counts'],
        weights=payload['weights'],
        time_limit_sec=int(payload.get('time_limit_sec', 3600)),
        verbose=bool(payload.get('verbose', False)),
        greedy_placement=payload.get('greedy_placement'),
    )

    with open(output_path, 'wb') as f:
        pickle.dump(result, f)


if __name__ == '__main__':
    main()
