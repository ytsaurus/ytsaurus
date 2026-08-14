"""
CP-SAT model building, warm-start hints, and solution extraction.

Subprocess-only module — imports from shared.py (co-located in the same temp dir).
Does NOT include precompute_n_min_tables (that lives in lib/precompute.py,
called before the subprocess).
"""

import math
import random

import pandas as pd
from ortools.sat.python import cp_model

import shared as cfg
from shared import disc_round, compute_n_min_from_req

# ---------------------------------------------------------------------------
# Container ID remapping (warm start)
# ---------------------------------------------------------------------------


def _remap_container_id(container_id, instance_type, prev_k_node, K_node, prev_k_proxy, K_proxy):
    if instance_type == 'node':
        if container_id < min(K_node, prev_k_node):
            return container_id
    elif instance_type == 'proxy':
        proxy_local = container_id - prev_k_node
        if 0 <= proxy_local < min(K_proxy, prev_k_proxy):
            return K_node + proxy_local
    return None


def _compute_bundle_to_container_from_sizes(
    bundle_groups,
    sizes_df,
    K_node,
    K_proxy,
    prev_k_node,
    prev_k_proxy,
    resource_coefficients,
):
    a = resource_coefficients["a"]
    b = resource_coefficients["b"]
    c = resource_coefficients["c"]

    container_sizes = {}
    for _, row in sizes_df.iterrows():
        k = _remap_container_id(
            int(row['ContainerTypeID']),
            row['InstanceType'],
            prev_k_node,
            K_node,
            prev_k_proxy,
            K_proxy,
        )
        if k is not None:
            container_sizes[k] = (
                disc_round(row['CPU'], cfg.CPU_STEP),
                disc_round(row['Memory'], cfg.MEM_STEP),
                disc_round(row['Network'], cfg.NET_STEP),
            )

    K = K_node + K_proxy

    def _find_best_container(req_c, req_m, req_n, instance_type, label):
        k_range = range(K_node) if instance_type == 'node' else range(K_node, K)
        best_k, best_cost = None, None
        for k in k_range:
            if k not in container_sizes:
                continue
            ci, mi, ni = container_sizes[k]
            if ci >= req_c and mi >= req_m and ni >= req_n:
                cost = a * ci + b * mi + c * ni
                if best_cost is None or cost < best_cost:
                    best_cost = cost
                    best_k = k
        if best_k is None:
            available = {k: container_sizes[k] for k in k_range if k in container_sizes}
            raise ValueError(
                f"No fitting container for '{label}' (req: cpu={req_c}, mem={req_m}, net={req_n}); "
                f"available: {available}"
            )
        return best_k

    group_to_k = {}
    for group_idx, bg in enumerate(bundle_groups):
        group_to_k[group_idx] = _find_best_container(
            bg.cpu_req_int,
            bg.mem_req_int,
            bg.net_req_int,
            bg.instance_type,
            bg.label,
        )
    return group_to_k


def _recompute_assign_df_from_sizes(warm_start_data, bundle_groups, K_node, K_proxy, resource_coefficients):
    sizes_df = warm_start_data["sizes_df"]
    prev_k_node = warm_start_data["prev_k_node"]
    prev_k_proxy = warm_start_data["prev_k_proxy"]
    group_to_k = _compute_bundle_to_container_from_sizes(
        bundle_groups,
        sizes_df,
        K_node,
        K_proxy,
        prev_k_node,
        prev_k_proxy,
        resource_coefficients,
    )
    rows = []
    for group_idx, bg in enumerate(bundle_groups):
        k = group_to_k[group_idx]
        for bi in bg.all_bundles:
            rows.append({"Bundle": bi.bundle, "AssignedContainerTypeID": k})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Per-host pattern count
# ---------------------------------------------------------------------------


def _patterns_for_host(n_phys: int) -> int:
    """More physical hosts → more packing patterns allowed."""
    # if n_phys < 30:
    #     return 1
    # if n_phys < 80:
    #     return 2
    # if n_phys < 300:
    #     return 3

    # return 4

    if n_phys < 100:
        return 1
    if n_phys < 400:
        return 2

    return 3


# ---------------------------------------------------------------------------
# Catalog index lookup
# ---------------------------------------------------------------------------


def find_nearest_catalog_index(
    cpu_int: int,
    mem_int: int,
    net_int: int,
    cat_cpu: list,
    cat_mem: list,
    cat_net: list,
) -> int | None:
    if not cat_cpu:
        return None
    max_cpu = max(cat_cpu) or 1
    max_mem = max(cat_mem) or 1
    max_net = max(cat_net) or 1
    best_idx = None
    best_dist = float('inf')
    for i, (cc, cm, cn) in enumerate(zip(cat_cpu, cat_mem, cat_net)):
        if cc < cpu_int or cm < mem_int or cn < net_int:
            continue
        dist = (cc - cpu_int) / max_cpu + (cm - mem_int) / max_mem + (cn - net_int) / max_net
        if dist < best_dist:
            best_dist = dist
            best_idx = i
    return best_idx


def _enrich_sizes_df_with_catalog(
    sizes_df,
    prev_k_node,
    prev_k_proxy,
    K_node,
    K_proxy,
    node_catalog_cpu,
    node_catalog_mem,
    node_catalog_net,
    proxy_catalog_cpu,
    proxy_catalog_mem,
    proxy_catalog_net,
    verbose=False,
):
    if "CatalogIndex" in sizes_df.columns:
        return sizes_df
    rows = []
    for _, row in sizes_df.iterrows():
        row = dict(row)
        k = _remap_container_id(
            int(row['ContainerTypeID']),
            row['InstanceType'],
            prev_k_node,
            K_node,
            prev_k_proxy,
            K_proxy,
        )
        req_cpu = disc_round(float(row.get('CPU', 0)), cfg.CPU_STEP)
        req_mem = disc_round(float(row.get('Memory', 0)), cfg.MEM_STEP)
        req_net = disc_round(float(row.get('Network', 0)), cfg.NET_STEP)
        if k is not None and k < K_node:
            idx = (
                find_nearest_catalog_index(
                    req_cpu,
                    req_mem,
                    req_net,
                    node_catalog_cpu,
                    node_catalog_mem,
                    node_catalog_net,
                )
                or 0
            )
            row['CPU'] = node_catalog_cpu[idx] * cfg.CPU_STEP
            row['Memory'] = node_catalog_mem[idx] * cfg.MEM_STEP
            row['Network'] = node_catalog_net[idx] * cfg.NET_STEP
        elif k is not None:
            idx = (
                find_nearest_catalog_index(
                    req_cpu,
                    req_mem,
                    req_net,
                    proxy_catalog_cpu,
                    proxy_catalog_mem,
                    proxy_catalog_net,
                )
                or 0
            )
            row['CPU'] = proxy_catalog_cpu[idx] * cfg.CPU_STEP
            row['Memory'] = proxy_catalog_mem[idx] * cfg.MEM_STEP
            row['Network'] = proxy_catalog_net[idx] * cfg.NET_STEP
        else:
            idx = 0
        row['CatalogIndex'] = idx
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Warm-start hints
# ---------------------------------------------------------------------------


def apply_warm_start_hints(
    model,
    sel_node,
    sel_proxy,
    cpu_size,
    mem_size,
    net_size,
    x,
    n_vars,
    f_vars,
    n_numa_nodes,
    bundle_groups,
    K_node,
    K_proxy,
    warm_start_data,
    node_catalog_cpu,
    node_catalog_mem,
    node_catalog_net,
    proxy_catalog_cpu,
    proxy_catalog_mem,
    proxy_catalog_net,
    sizes_fixed=False,
    assignments_fixed=False,
    assignments_recomputed=False,
    prod_vars=None,
    n_min_tables=None,
    host_n_patterns=None,
):
    if warm_start_data is None:
        return

    prev_k_node = warm_start_data["prev_k_node"]
    prev_k_proxy = warm_start_data["prev_k_proxy"]
    K = K_node + K_proxy

    k_to_cat_idx: dict = {}
    sizes_df = warm_start_data.get("sizes_df")
    if sizes_df is not None:
        for _, row in sizes_df.iterrows():
            k = _remap_container_id(
                int(row['ContainerTypeID']), row['InstanceType'], prev_k_node, K_node, prev_k_proxy, K_proxy
            )
            if k is None:
                continue
            cat_idx = int(row['CatalogIndex'])
            k_to_cat_idx[k] = cat_idx

            if not sizes_fixed:
                if k < K_node and not isinstance(sel_node[k], int):
                    N_node = len(node_catalog_cpu)
                    if 0 <= cat_idx < N_node:
                        model.AddHint(sel_node[k], cat_idx)
                        model.AddHint(cpu_size[k], node_catalog_cpu[cat_idx])
                        model.AddHint(mem_size[k], node_catalog_mem[cat_idx])
                        model.AddHint(net_size[k], node_catalog_net[cat_idx])
                elif k >= K_node and not isinstance(sel_proxy[k - K_node], int):
                    N_proxy = len(proxy_catalog_cpu)
                    if 0 <= cat_idx < N_proxy:
                        j = k - K_node
                        model.AddHint(sel_proxy[j], cat_idx)
                        model.AddHint(cpu_size[k], proxy_catalog_cpu[cat_idx])
                        model.AddHint(mem_size[k], proxy_catalog_mem[cat_idx])
                        model.AddHint(net_size[k], proxy_catalog_net[cat_idx])

    rng = random.Random(cfg.HINT_DROPOUT_SEED)

    if not assignments_fixed and "assign_df" in warm_start_data:
        assign_df = warm_start_data["assign_df"]
        bundle_to_k = {}
        for _, row in assign_df.iterrows():
            bundle = row['Bundle']
            instance_type = 'node' if int(row['AssignedContainerTypeID']) < prev_k_node else 'proxy'
            k = _remap_container_id(
                int(row['AssignedContainerTypeID']), instance_type, prev_k_node, K_node, prev_k_proxy, K_proxy
            )
            if k is not None:
                bundle_to_k[bundle] = k

        group_to_k = {}
        for group_idx, bg in enumerate(bundle_groups):
            assigned_ks = [bundle_to_k[bi.bundle] for bi in bg.all_bundles if bi.bundle in bundle_to_k]
            if not assigned_ks:
                continue
            unique_ks = set(assigned_ks)
            if len(unique_ks) > 1:
                print(f"Warning: group {group_idx} inconsistent assignments {unique_ks}, skipping hint")
                continue
            group_to_k[group_idx] = assigned_ks[0]

        assignment_dropout = cfg.HINT_ASSIGNMENTS_DROPOUT_RATE
        for group_idx, bg in enumerate(bundle_groups):
            if group_idx not in group_to_k:
                continue
            if assignment_dropout > 0.0 and rng.random() < assignment_dropout:
                continue
            k_assigned = group_to_k[group_idx]
            for k in range(K):
                if not isinstance(x[group_idx, k], int):
                    model.AddHint(x[group_idx, k], 1 if k == k_assigned else 0)

            for c in bg.bundles_by_cluster:
                k_range = range(K_node) if bg.instance_type == 'node' else range(K_node, K)
                nm_table = n_min_tables.get((group_idx, c)) if n_min_tables is not None else None
                for k in k_range:
                    n_v = n_vars.get((group_idx, c, k))
                    if n_v is not None and not isinstance(n_v, int):
                        cat_idx_k = k_to_cat_idx.get(k)
                        if nm_table is not None and cat_idx_k is not None and 0 <= cat_idx_k < len(nm_table):
                            model.AddHint(n_v, nm_table[cat_idx_k])

                    p_v = prod_vars.get((group_idx, c, k)) if prod_vars is not None else None
                    if p_v is not None and not isinstance(p_v, int):
                        cat_idx_assigned = k_to_cat_idx.get(k_assigned)
                        if (
                            k == k_assigned
                            and nm_table is not None
                            and cat_idx_assigned is not None
                            and 0 <= cat_idx_assigned < len(nm_table)
                        ):
                            model.AddHint(p_v, nm_table[cat_idx_assigned])
                        else:
                            model.AddHint(p_v, 0)

    if cfg.APPLY_PATTERNS_HINTS and host_n_patterns and "patterns_df" in warm_start_data:
        patterns_df = warm_start_data["patterns_df"]
        pattern_dropout = cfg.HINT_PATTERNS_DROPOUT_RATE
        f_hint_values = {}
        for (c, m, p), grp in patterns_df.groupby(['Cluster', 'HostModel', 'Pattern']):
            p = int(p)
            p_limit = host_n_patterns[(c, m)]
            if p < p_limit and (c, m, p) in n_numa_nodes:
                if pattern_dropout > 0.0 and rng.random() < pattern_dropout:
                    continue
                if not assignments_recomputed:
                    model.AddHint(n_numa_nodes[c, m, p], int(grp['NumaNodesUsed'].iloc[0]))
                for k in range(K):
                    if (c, m, p, k) in f_vars:
                        f_hint_values[(c, m, p, k)] = 0
                for _, prow in grp.iterrows():
                    instance_type = prow['InstanceType']
                    k = _remap_container_id(
                        int(prow['ContainerTypeID']), instance_type, prev_k_node, K_node, prev_k_proxy, K_proxy
                    )
                    if k is not None and (c, m, p, k) in f_vars:
                        f_hint_values[(c, m, p, k)] = int(prow['CountOnNode'])

        for (c, m, p, k), val in f_hint_values.items():
            model.AddHint(f_vars[c, m, p, k], val)


# ---------------------------------------------------------------------------
# Cheapest-feasible assignment constraints
# ---------------------------------------------------------------------------


def _add_cheapest_feasible_x_constraints(
    model,
    x,
    sel_node,
    sel_proxy,
    bundle_groups,
    instance_types,
    K_node,
    K,
    infeasible_catalog,
    use_fixed_sizes,
    n_min_tables,
    clusters,
    t_table_node,
    t_table_proxy,
    t_int=None,
):
    """
    Constrain each bundle group to be assigned to its cheapest feasible container.
    Cost for group i at slot k = sum_c n_min[i,c,k] * t[k].
    'Feasible' means the container's catalog index is not in infeasible_catalog[i].
    Enforces: if group i is assigned to k1, then for every feasible k2 != k1,
    total_cost(k1) <= total_cost(k2).
    """
    for i, bg in enumerate(bundle_groups):
        itype = instance_types[i]
        k_range = list(range(K_node) if itype == 'node' else range(K_node, K))
        if len(k_range) <= 1:
            continue
        infeasible_i = infeasible_catalog.get(i, set())
        t_table = t_table_node if itype == 'node' else t_table_proxy

        if use_fixed_sizes:
            # sel_node[k]/sel_proxy[k-K_node] are integers (catalog indices); t_int[k] is int.
            # total_cost[k] = t_int[k] * sum_c n_min_tables[i,c][sel_k]  — plain int.
            total_cost = {}
            for k in k_range:
                sel_k = sel_node[k] if k < K_node else sel_proxy[k - K_node]
                n_sum = sum(
                    n_min_tables[i, c][sel_k]
                    for c in clusters
                    if bg.counts_by_cluster.get(c, 0) > 0 and (i, c) in n_min_tables
                )
                total_cost[k] = t_int[k] * n_sum

            feasible_costs = {
                k: total_cost[k]
                for k in k_range
                if (sel_node[k] if k < K_node else sel_proxy[k - K_node]) not in infeasible_i
            }
            if not feasible_costs:
                continue
            min_cost = min(feasible_costs.values())
            for k in k_range:
                if k not in feasible_costs or feasible_costs[k] > min_cost:
                    x_ik = x[i, k]
                    if not isinstance(x_ik, int):
                        model.Add(x_ik == 0)
        else:
            # Build group_cost_table[j] = t_table[j] * sum_c n_min_tables[i,c][j] — plain int list.
            # Then total_cost[k] = AddElement(sel_k, group_cost_table) — one IntVar per (i,k).
            N = len(t_table)
            group_cost_table = [
                t_table[j]
                * sum(
                    n_min_tables[i, c][j]
                    for c in clusters
                    if bg.counts_by_cluster.get(c, 0) > 0 and (i, c) in n_min_tables
                )
                for j in range(N)
            ]
            cost_lb = min(group_cost_table) if group_cost_table else 0
            cost_ub = max(group_cost_table) if group_cost_table else 0

            total_cost = {}
            for k in k_range:
                sel_k = sel_node[k] if k < K_node else sel_proxy[k - K_node]
                if isinstance(sel_k, int):
                    total_cost[k] = group_cost_table[sel_k]
                else:
                    cost_v = model.NewIntVar(cost_lb, max(cost_lb, cost_ub), f"grp_cost_{i}_{k}")
                    model.AddElement(sel_k, group_cost_table, cost_v)
                    total_cost[k] = cost_v

            # Build infeasibility booleans: infeas_bool[k] == 1 iff sel_k in infeasible_i.
            infeas_bool = {}
            for k in k_range:
                if not infeasible_i:
                    infeas_bool[k] = 0
                    continue
                sel_k = sel_node[k] if k < K_node else sel_proxy[k - K_node]
                if isinstance(sel_k, int):
                    infeas_bool[k] = 1 if sel_k in infeasible_i else 0
                    continue
                ib = model.NewBoolVar(f"infeas_{i}_{k}")
                eq_vars = []
                for j in sorted(infeasible_i):
                    eq_j = model.NewBoolVar(f"eq_infeas_{i}_{k}_{j}")
                    model.Add(sel_k == j).OnlyEnforceIf(eq_j)
                    model.Add(sel_k != j).OnlyEnforceIf(eq_j.Not())
                    eq_vars.append(eq_j)
                    model.AddImplication(eq_j, ib)
                model.AddBoolOr(eq_vars + [ib.Not()])
                infeas_bool[k] = ib

            # For each pair (k1, k2): assigned to k1 ∧ k2 feasible → total_cost(k1) ≤ total_cost(k2).
            for k1 in k_range:
                x_ik1 = x[i, k1]
                if isinstance(x_ik1, int) and x_ik1 == 0:
                    continue
                c_k1 = total_cost[k1]
                for k2 in k_range:
                    if k2 == k1:
                        continue
                    ib2 = infeas_bool[k2]
                    c_k2 = total_cost[k2]
                    if isinstance(ib2, int):
                        if ib2 == 1:
                            continue  # k2 always infeasible, no constraint needed
                        if isinstance(x_ik1, int):  # x_ik1 == 1 here
                            model.Add(c_k1 <= c_k2)
                        else:
                            model.Add(c_k1 <= c_k2).OnlyEnforceIf(x_ik1)
                    else:
                        if isinstance(x_ik1, int):  # x_ik1 == 1 here
                            model.Add(c_k1 <= c_k2).OnlyEnforceIf(ib2.Not())
                        else:
                            model.Add(c_k1 <= c_k2).OnlyEnforceIf([x_ik1, ib2.Not()])


# ---------------------------------------------------------------------------
# Model building
# ---------------------------------------------------------------------------


def _build_model(
    bundle_groups: list,
    hosts: list,
    K_node: int,
    K_proxy: int,
    resource_coefficients: dict,
    node_catalog_cpu: list,
    node_catalog_mem: list,
    node_catalog_net: list,
    proxy_catalog_cpu: list,
    proxy_catalog_mem: list,
    proxy_catalog_net: list,
    data_node_configs=None,
    warm_start_data=None,
    prev_sizes_df=None,
    max_changed_sizes=None,
    fixed_container_ids=None,
    bundle_size_fixed=False,
    n_min_tables=None,
    infeasible_catalog=None,
    _cost_scale=cfg.COST_SCALE,
    warm_start_size_based_x_hints=False,
    min_numa_frac=cfg.MIN_NUMA_FRAC,
    sizes_fixed=False,
    assignments_fixed=False,
    min_usage_frac_node=cfg.MIN_USAGE_FRAC_NODE,
    min_usage_frac_proxy=cfg.MIN_USAGE_FRAC_PROXY,
    verbose=False,
):
    K = K_node + K_proxy
    I = len(bundle_groups)
    instance_types = [bg.instance_type for bg in bundle_groups]
    cpu_req_int_arr = [bg.cpu_req_int for bg in bundle_groups]
    mem_req_int_arr = [bg.mem_req_int for bg in bundle_groups]
    net_req_int_arr = [bg.net_req_int for bg in bundle_groups]

    clusters = sorted({c for bg in bundle_groups for c in bg.bundles_by_cluster})

    N_node = len(node_catalog_cpu)
    N_proxy = len(proxy_catalog_cpu)

    min_node_cpu = min(node_catalog_cpu) if node_catalog_cpu else 1
    min_node_mem = min(node_catalog_mem) if node_catalog_mem else 1
    min_node_net = min(node_catalog_net) if node_catalog_net else 1
    min_proxy_cpu = min(proxy_catalog_cpu) if proxy_catalog_cpu else 1
    min_proxy_mem = min(proxy_catalog_mem) if proxy_catalog_mem else 1
    min_proxy_net = min(proxy_catalog_net) if proxy_catalog_net else 1
    max_node_cpu = max(node_catalog_cpu) if node_catalog_cpu else 1
    max_node_mem = max(node_catalog_mem) if node_catalog_mem else 1
    max_node_net = max(node_catalog_net) if node_catalog_net else 1
    max_proxy_cpu = max(proxy_catalog_cpu) if proxy_catalog_cpu else 1
    max_proxy_mem = max(proxy_catalog_mem) if proxy_catalog_mem else 1
    max_proxy_net = max(proxy_catalog_net) if proxy_catalog_net else 1

    H_per_cluster = {c: [h for h in hosts if h.cluster == c] for c in clusters}

    a_int = int(round(resource_coefficients["a"] * _cost_scale))
    b_int = int(round(resource_coefficients["b"] * _cost_scale))
    c_int = int(round(resource_coefficients["c"] * _cost_scale))

    model = cp_model.CpModel()

    _effective_ws = warm_start_data
    if warm_start_data is not None and "sizes_df" in warm_start_data:
        _enriched_sizes_df = _enrich_sizes_df_with_catalog(
            warm_start_data["sizes_df"],
            warm_start_data["prev_k_node"],
            warm_start_data["prev_k_proxy"],
            K_node,
            K_proxy,
            node_catalog_cpu,
            node_catalog_mem,
            node_catalog_net,
            proxy_catalog_cpu,
            proxy_catalog_mem,
            proxy_catalog_net,
            verbose=verbose,
        )
        if _enriched_sizes_df is not warm_start_data["sizes_df"]:
            _effective_ws = dict(warm_start_data, sizes_df=_enriched_sizes_df)

    _fixed_ids = list(fixed_container_ids) if fixed_container_ids is not None else None

    assert not (
        warm_start_size_based_x_hints and _effective_ws is None
    ), "warm_start_size_based_x_hints=True requires warm_start_data"
    _assignments_recomputed = False
    if warm_start_size_based_x_hints:
        _effective_ws = dict(
            _effective_ws,
            assign_df=_recompute_assign_df_from_sizes(
                _effective_ws, bundle_groups, K_node, K_proxy, resource_coefficients
            ),
        )
        _assignments_recomputed = True

    changed = None
    sel_node = []
    sel_proxy = []
    cpu_size = []
    mem_size = []
    net_size = []

    if sizes_fixed:
        assert (
            _effective_ws is not None and "sizes_df" in _effective_ws
        ), "sizes_fixed=True requires warm_start_data with sizes_df"
        ws_sizes = _effective_ws["sizes_df"]
        prev_map_sz = ws_sizes.set_index("ContainerTypeID").to_dict(orient="index")
        for t in range(K_node):
            row = prev_map_sz.get(t, {})
            idx = max(0, min(N_node - 1, int(row.get("CatalogIndex", 0))))
            sel_node.append(idx)
            cpu_size.append(node_catalog_cpu[idx])
            mem_size.append(node_catalog_mem[idx])
            net_size.append(node_catalog_net[idx])
        for j in range(K_proxy):
            row = prev_map_sz.get(K_node + j, {})
            idx = max(0, min(N_proxy - 1, int(row.get("CatalogIndex", 0))))
            sel_proxy.append(idx)
            cpu_size.append(proxy_catalog_cpu[idx])
            mem_size.append(proxy_catalog_mem[idx])
            net_size.append(proxy_catalog_net[idx])
        _use_fixed_sizes = True
    else:
        sel_node = [model.NewIntVar(0, N_node - 1, f"sel_node_{t}") for t in range(K_node)]
        sel_proxy = [model.NewIntVar(0, N_proxy - 1, f"sel_proxy_{j}") for j in range(K_proxy)]

        if K_node > 1:
            model.AddAllDifferent(sel_node)
        if K_proxy > 1:
            model.AddAllDifferent(sel_proxy)

        cpu_size_node, mem_size_node, net_size_node = [], [], []
        for t in range(K_node):
            cv = model.NewIntVar(min_node_cpu, max_node_cpu, f"cpu_node_{t}")
            mv = model.NewIntVar(min_node_mem, max_node_mem, f"mem_node_{t}")
            nv = model.NewIntVar(min_node_net, max_node_net, f"net_node_{t}")
            model.AddElement(sel_node[t], node_catalog_cpu, cv)
            model.AddElement(sel_node[t], node_catalog_mem, mv)
            model.AddElement(sel_node[t], node_catalog_net, nv)
            cpu_size_node.append(cv)
            mem_size_node.append(mv)
            net_size_node.append(nv)

        cpu_size_proxy, mem_size_proxy, net_size_proxy = [], [], []
        for j in range(K_proxy):
            cv = model.NewIntVar(min_proxy_cpu, max_proxy_cpu, f"cpu_proxy_{j}")
            mv = model.NewIntVar(min_proxy_mem, max_proxy_mem, f"mem_proxy_{j}")
            nv = model.NewIntVar(min_proxy_net, max_proxy_net, f"net_proxy_{j}")
            model.AddElement(sel_proxy[j], proxy_catalog_cpu, cv)
            model.AddElement(sel_proxy[j], proxy_catalog_mem, mv)
            model.AddElement(sel_proxy[j], proxy_catalog_net, nv)
            cpu_size_proxy.append(cv)
            mem_size_proxy.append(mv)
            net_size_proxy.append(nv)

        cpu_size = cpu_size_node + cpu_size_proxy
        mem_size = mem_size_node + mem_size_proxy
        net_size = net_size_node + net_size_proxy
        _use_fixed_sizes = False

        if _fixed_ids is not None:
            _fix_src_df = None
            if prev_sizes_df is not None and "CatalogIndex" in prev_sizes_df.columns:
                _fix_src_df = prev_sizes_df
            elif _effective_ws is not None and "sizes_df" in _effective_ws:
                _ws_sz = _effective_ws["sizes_df"]
                if "CatalogIndex" in _ws_sz.columns:
                    _fix_src_df = _ws_sz
            if _fix_src_df is not None:
                _fix_map = _fix_src_df.set_index("ContainerTypeID").to_dict(orient="index")
                for k in _fixed_ids:
                    if k < K_node and k in _fix_map:
                        model.Add(sel_node[k] == int(_fix_map[k]["CatalogIndex"]))
                    elif k < K and k in _fix_map:
                        model.Add(sel_proxy[k - K_node] == int(_fix_map[k]["CatalogIndex"]))

        if prev_sizes_df is not None and "CatalogIndex" in prev_sizes_df.columns and max_changed_sizes is not None:
            prev_map = prev_sizes_df.set_index("ContainerTypeID").to_dict(orient="index")
            prev_sel_node = [int(prev_map[t]["CatalogIndex"]) for t in range(K_node) if t in prev_map]
            prev_sel_proxy = [
                int(prev_map[K_node + j]["CatalogIndex"]) for j in range(K_proxy) if (K_node + j) in prev_map
            ]
            if len(prev_sel_node) == K_node and len(prev_sel_proxy) == K_proxy:
                fixed_set = set(_fixed_ids or [])
                free_node = [t for t in range(K_node) if t not in fixed_set]
                free_proxy = [j for j in range(K_proxy) if (K_node + j) not in fixed_set]
                changed_node = [model.NewBoolVar(f"ch_node_{t}") for t in free_node]
                changed_proxy = [model.NewBoolVar(f"ch_proxy_{j}") for j in free_proxy]
                for i_t, t in enumerate(free_node):
                    model.Add(sel_node[t] == prev_sel_node[t]).OnlyEnforceIf(changed_node[i_t].Not())
                for i_j, j in enumerate(free_proxy):
                    model.Add(sel_proxy[j] == prev_sel_proxy[j]).OnlyEnforceIf(changed_proxy[i_j].Not())
                model.Add(sum(changed_node) + sum(changed_proxy) <= int(max_changed_sizes))
                model.Add(sum(changed_node) + sum(changed_proxy) >= 1)
                changed = changed_node + changed_proxy

    _use_fixed_x = assignments_fixed
    if _use_fixed_x:
        assert (
            _effective_ws is not None and "assign_df" in _effective_ws
        ), "assignments_fixed=True requires warm_start_data with assign_df"
        _assign_df = _effective_ws["assign_df"]
        _bundle_to_k = {row["Bundle"]: int(row["AssignedContainerTypeID"]) for _, row in _assign_df.iterrows()}
        x = {}
        for i, bg in enumerate(bundle_groups):
            ks = {_bundle_to_k[bi.bundle] for bi in bg.all_bundles if bi.bundle in _bundle_to_k}
            assert len(ks) == 1, (
                f"group {i} ({bg.label}) has " f"{'missing' if len(ks) == 0 else 'inconsistent'} assignment: {ks}"
            )
            assigned_k = next(iter(ks))
            for k in range(K):
                x[i, k] = 1 if k == assigned_k else 0
    else:
        x = {(i, k): model.NewBoolVar(f"x_{i}_{k}") for i in range(I) for k in range(K)}
        for i in range(I):
            if instance_types[i] == 'node':
                model.Add(sum(x[i, k] for k in range(K_node)) == 1)
                model.Add(sum(x[i, k] for k in range(K_node, K)) == 0)
            else:
                model.Add(sum(x[i, k] for k in range(K_node)) == 0)
                model.Add(sum(x[i, k] for k in range(K_node, K)) == 1)

        I_node = sum(1 for it in instance_types if it == 'node')
        I_proxy = I - I_node
        min_node = math.ceil(min_usage_frac_node * I_node) if min_usage_frac_node > 0 and I_node > 0 else 0
        min_proxy = math.ceil(min_usage_frac_proxy * I_proxy) if min_usage_frac_proxy > 0 and I_proxy > 0 else 0
        if min_node > 0:
            for k in range(K_node):
                model.Add(sum(x[i, k] for i in range(I) if instance_types[i] == 'node') >= min_node)
        if min_proxy > 0:
            for k in range(K_node, K):
                model.Add(sum(x[i, k] for i in range(I) if instance_types[i] == 'proxy') >= min_proxy)

    assert n_min_tables is not None and infeasible_catalog is not None

    for i, bg in enumerate(bundle_groups):
        itype = instance_types[i]
        k_range = range(K_node) if itype == 'node' else range(K_node, K)
        infeasible_i = infeasible_catalog.get(i, set())
        if not infeasible_i:
            continue
        for k in k_range:
            sel_k = sel_node[k] if k < K_node else sel_proxy[k - K_node]
            x_ik = x[i, k]
            if isinstance(sel_k, int):
                if sel_k in infeasible_i:
                    if isinstance(x_ik, int):
                        assert x_ik == 0, f"Infeasible fixed: group {i}, k={k}, idx={sel_k}"
                    else:
                        model.Add(x_ik == 0)
            elif not isinstance(x_ik, int):
                model.AddForbiddenAssignments([sel_k, x_ik], [(idx, 1) for idx in infeasible_i])

    n_vars: dict = {}
    prod_vars: dict = {}
    U_ic: dict = {}

    _bundle_to_ws_count: dict = {}
    if bundle_size_fixed:
        if _effective_ws is not None and "assign_df" in _effective_ws:
            _adf = _effective_ws["assign_df"]
            if "NewCount" in _adf.columns:
                for _, _row in _adf.iterrows():
                    _bundle_to_ws_count[str(_row["Bundle"])] = int(_row["NewCount"])

    for i, bg in enumerate(bundle_groups):
        itype = instance_types[i]
        k_range = range(K_node) if itype == 'node' else range(K_node, K)

        for c in clusters:
            base = bg.counts_by_cluster.get(c, 0)

            if base == 0:
                for k in k_range:
                    n_vars[i, c, k] = 0
                    prod_vars[i, c, k] = 0
                continue

            U_ic[i, c] = cfg.MAX_EXTRA_RATIO * base

            if bundle_size_fixed:
                ws_total = sum(_bundle_to_ws_count.get(bi.bundle, bi.count) for bi in bg.bundles_by_cluster.get(c, []))
                for k in k_range:
                    x_ik = x[i, k]
                    n_vars[i, c, k] = ws_total
                    if isinstance(x_ik, int):
                        prod_vars[i, c, k] = ws_total * x_ik
                    else:
                        prod_v = model.NewIntVar(0, ws_total, f"prod_{i}_{c}_{k}")
                        model.Add(prod_v == ws_total).OnlyEnforceIf(x_ik)
                        model.Add(prod_v == 0).OnlyEnforceIf(x_ik.Not())
                        prod_vars[i, c, k] = prod_v
            else:
                nm_table = n_min_tables[i, c]
                nm_max = cfg.MAX_EXTRA_RATIO * base

                for k in k_range:
                    sel_k = sel_node[k] if k < K_node else sel_proxy[k - K_node]
                    x_ik = x[i, k]

                    if isinstance(sel_k, int):
                        n_val = nm_table[sel_k]
                        n_vars[i, c, k] = n_val
                        if isinstance(x_ik, int):
                            prod_vars[i, c, k] = n_val * x_ik
                        else:
                            prod_v = model.NewIntVar(0, n_val, f"prod_{i}_{c}_{k}")
                            model.Add(prod_v == n_val).OnlyEnforceIf(x_ik)
                            model.Add(prod_v == 0).OnlyEnforceIf(x_ik.Not())
                            prod_vars[i, c, k] = prod_v
                    else:
                        nm_v = model.NewIntVar(1, nm_max, f"nm_{i}_{c}_{k}")
                        model.AddElement(sel_k, nm_table, nm_v)
                        n_vars[i, c, k] = nm_v

                        if isinstance(x_ik, int):
                            prod_vars[i, c, k] = nm_v if x_ik == 1 else 0
                        else:
                            prod_v = model.NewIntVar(0, nm_max, f"prod_{i}_{c}_{k}")
                            model.Add(prod_v <= nm_v)
                            model.Add(prod_v >= nm_v - nm_max + nm_max * x_ik)
                            model.Add(prod_v <= nm_max * x_ik)
                            prod_vars[i, c, k] = prod_v

    D = {}
    for c in clusters:
        for k in range(K):
            k_is_node = k < K_node
            terms_var = [
                prod_vars[i, c, k]
                for i, bg in enumerate(bundle_groups)
                if (bg.instance_type == 'node') == k_is_node and not isinstance(prod_vars.get((i, c, k), 0), int)
            ]
            fixed_sum = sum(
                prod_vars[i, c, k]
                for i, bg in enumerate(bundle_groups)
                if (bg.instance_type == 'node') == k_is_node and isinstance(prod_vars.get((i, c, k), 0), int)
            )
            D_ub = sum(
                U_ic.get((i, c), bg.counts_by_cluster.get(c, 0))
                for i, bg in enumerate(bundle_groups)
                if (bg.instance_type == 'node') == k_is_node
            )
            if terms_var:
                D_v = model.NewIntVar(0, max(1, D_ub), f"D_{c}_{k}")
                model.Add(D_v == sum(terms_var) + fixed_sum)
                D[c, k] = D_v
            else:
                D[c, k] = fixed_sum

    host_n_patterns: dict = {}
    for c in clusters:
        for h in H_per_cluster.get(c, []):
            n_phys = h.available_physical_hosts
            host_n_patterns[c, h.host_key] = _patterns_for_host(n_phys)

    n_numa_nodes, f, g = {}, {}, {}
    for c in clusters:
        for h in H_per_cluster.get(c, []):
            host_key = h.host_key
            Cc, Cn, Cm = h.cpu_capacity, h.net_capacity, h.mem_capacity
            avail = h.total_available_numa_nodes
            f_ub = cfg.ANTIAFFINITY_PER_NUMA
            P = host_n_patterns[c, host_key]
            for p in range(P):
                n_numa_nodes[c, host_key, p] = model.NewIntVar(0, int(avail), f"n_{c}_{host_key}_{p}")
                for k in range(K):
                    f[c, host_key, p, k] = model.NewIntVar(0, f_ub, f"f_{c}_{host_key}_{p}_{k}")
                    g_ub = int(avail) * f_ub
                    g[c, host_key, p, k] = model.NewIntVar(0, g_ub, f"g_{c}_{host_key}_{p}_{k}")
                    model.AddMultiplicationEquality(
                        g[c, host_key, p, k], [n_numa_nodes[c, host_key, p], f[c, host_key, p, k]]
                    )

            for p in range(P):
                if _use_fixed_sizes:
                    model.Add(sum(f[c, host_key, p, k] * int(cpu_size[k]) for k in range(K)) <= int(Cc))
                    model.Add(sum(f[c, host_key, p, k] * int(net_size[k]) for k in range(K)) <= int(Cn))
                    model.Add(sum(f[c, host_key, p, k] * int(mem_size[k]) for k in range(K)) <= int(Cm))
                else:
                    wcpu = [model.NewIntVar(0, int(Cc), f"wcpu_{c}_{host_key}_{p}_{k}") for k in range(K)]
                    wnet = [model.NewIntVar(0, int(Cn), f"wnet_{c}_{host_key}_{p}_{k}") for k in range(K)]
                    wmem = [model.NewIntVar(0, int(Cm), f"wmem_{c}_{host_key}_{p}_{k}") for k in range(K)]
                    for k in range(K):
                        model.AddMultiplicationEquality(wcpu[k], [f[c, host_key, p, k], cpu_size[k]])
                        model.AddMultiplicationEquality(wnet[k], [f[c, host_key, p, k], net_size[k]])
                        model.AddMultiplicationEquality(wmem[k], [f[c, host_key, p, k], mem_size[k]])
                    model.Add(sum(wcpu) <= int(Cc))
                    model.Add(sum(wnet) <= int(Cn))
                    model.Add(sum(wmem) <= int(Cm))

            model.Add(sum(n_numa_nodes[c, host_key, p] for p in range(P)) <= int(avail))

    for c in clusters:
        for k in range(K):
            cover_terms = [
                g[c, h.host_key, p, k] for h in H_per_cluster.get(c, []) for p in range(host_n_patterns[c, h.host_key])
            ]
            D_ck = D[c, k]
            if cover_terms:
                model.Add(sum(cover_terms) >= D_ck)
            elif not isinstance(D_ck, int) or D_ck > 0:
                if not isinstance(D_ck, int):
                    model.Add(D_ck == 0)

    if min_numa_frac > 0:
        for c in clusters:
            for h in H_per_cluster.get(c, []):
                avail = int(h.total_available_numa_nodes)
                if avail == 0:
                    continue
                min_used = math.ceil(min_numa_frac * avail)
                model.Add(
                    sum(n_numa_nodes[c, h.host_key, p] for p in range(host_n_patterns[c, h.host_key])) >= min_used
                )

    host_cost_terms = [
        int(round(h.numa_node_value(resource_coefficients) * _cost_scale)) * n_numa_nodes[c, h.host_key, p]
        for c in clusters
        for h in H_per_cluster.get(c, [])
        for p in range(host_n_patterns[c, h.host_key])
        if h.numa_node_value(resource_coefficients) > 0
    ]
    host_cost_int_expr = sum(host_cost_terms) if host_cost_terms else 0

    t_table_node = [
        a_int * node_catalog_cpu[j] + b_int * node_catalog_mem[j] + c_int * node_catalog_net[j] for j in range(N_node)
    ]
    t_table_proxy = [
        a_int * proxy_catalog_cpu[j] + b_int * proxy_catalog_mem[j] + c_int * proxy_catalog_net[j]
        for j in range(N_proxy)
    ]

    if _use_fixed_sizes:
        t_int = [a_int * int(cpu_size[k]) + b_int * int(mem_size[k]) + c_int * int(net_size[k]) for k in range(K)]
        container_cost_int_expr = sum(D[c, k] * t_int[k] for c in clusters for k in range(K))
    else:
        t = []
        for t_idx in range(K_node):
            t_lb = min(t_table_node)
            t_ub_k = max(t_table_node)
            t_k = model.NewIntVar(t_lb, t_ub_k, f"t_{t_idx}")
            model.AddElement(sel_node[t_idx], t_table_node, t_k)
            t.append(t_k)
        for j in range(K_proxy):
            t_lb = min(t_table_proxy)
            t_ub_k = max(t_table_proxy)
            t_k = model.NewIntVar(t_lb, t_ub_k, f"t_{K_node + j}")
            model.AddElement(sel_proxy[j], t_table_proxy, t_k)
            t.append(t_k)
        t_ub = max(max(t_table_node), max(t_table_proxy))
        cost_ck = {}
        for c in clusters:
            for k in range(K):
                D_ck = D[c, k]
                if isinstance(D_ck, int):
                    if D_ck > 0:
                        cost_v = model.NewIntVar(0, D_ck * t_ub, f"cost_{c}_{k}")
                        model.Add(cost_v == D_ck * t[k])
                        cost_ck[c, k] = cost_v
                else:
                    k_is_node = k < K_node
                    D_ub = sum(
                        U_ic.get((i, c), bg.counts_by_cluster.get(c, 0))
                        for i, bg in enumerate(bundle_groups)
                        if (bg.instance_type == "node") == k_is_node
                    )
                    cost_v = model.NewIntVar(0, max(1, D_ub * t_ub), f"cost_{c}_{k}")
                    model.AddMultiplicationEquality(cost_v, [D_ck, t[k]])
                    cost_ck[c, k] = cost_v
        container_cost_int_expr = sum(cost_ck.values()) if cost_ck else 0

    # if not _use_fixed_x:
    #     _add_cheapest_feasible_x_constraints(
    #         model, x, sel_node, sel_proxy,
    #         bundle_groups, instance_types, K_node, K,
    #         infeasible_catalog, _use_fixed_sizes,
    #         n_min_tables, clusters,
    #         t_table_node, t_table_proxy,
    #         t_int=t_int if _use_fixed_sizes else None,
    #     )

    apply_warm_start_hints(
        model,
        sel_node,
        sel_proxy,
        cpu_size,
        mem_size,
        net_size,
        x,
        n_vars,
        f,
        n_numa_nodes,
        bundle_groups,
        K_node,
        K_proxy,
        _effective_ws,
        node_catalog_cpu,
        node_catalog_mem,
        node_catalog_net,
        proxy_catalog_cpu,
        proxy_catalog_mem,
        proxy_catalog_net,
        sizes_fixed=_use_fixed_sizes,
        assignments_fixed=_use_fixed_x,
        assignments_recomputed=_assignments_recomputed,
        prod_vars=prod_vars,
        n_min_tables=n_min_tables,
        host_n_patterns=host_n_patterns,
    )

    return {
        "model": model,
        "K": K,
        "I": I,
        "clusters": clusters,
        "H_per_cluster": H_per_cluster,
        "host_cost_int_expr": host_cost_int_expr,
        "container_cost_int_expr": container_cost_int_expr,
        "x": x,
        "sel_node": sel_node,
        "sel_proxy": sel_proxy,
        "cpu_size": cpu_size,
        "mem_size": mem_size,
        "net_size": net_size,
        "D": D,
        "n_vars": n_vars,
        "n_numa_nodes": n_numa_nodes,
        "f": f,
        "instance_types": instance_types,
        "bundle_groups": bundle_groups,
        "K_node": K_node,
        "K_proxy": K_proxy,
        "_cost_scale": _cost_scale,
        "changed": changed,
        "resource_coefficients": resource_coefficients,
        "node_catalog_cpu": node_catalog_cpu,
        "node_catalog_mem": node_catalog_mem,
        "node_catalog_net": node_catalog_net,
        "proxy_catalog_cpu": proxy_catalog_cpu,
        "proxy_catalog_mem": proxy_catalog_mem,
        "proxy_catalog_net": proxy_catalog_net,
        "host_n_patterns": host_n_patterns,
    }


# ---------------------------------------------------------------------------
# Stage-3 model: host-free container cost minimization
# ---------------------------------------------------------------------------


def _build_model_stage3(
    bundle_groups: list,
    K_node: int,
    K_proxy: int,
    resource_coefficients: dict,
    node_catalog_cpu: list,
    node_catalog_mem: list,
    node_catalog_net: list,
    proxy_catalog_cpu: list,
    proxy_catalog_mem: list,
    proxy_catalog_net: list,
    warm_start_data=None,
    prev_sizes_df=None,
    max_changed_sizes=None,
    fixed_container_ids=None,
    n_min_tables=None,
    infeasible_catalog=None,
    _cost_scale=cfg.COST_SCALE,
    warm_start_size_based_x_hints=False,
    min_usage_frac_node=cfg.MIN_USAGE_FRAC_NODE,
    min_usage_frac_proxy=cfg.MIN_USAGE_FRAC_PROXY,
    verbose=False,
):
    """CP-SAT model that minimizes container cost with no host topology constraints.

    Identical to _build_model but drops n_numa_nodes/f/g variables, host capacity
    constraints, the coverage constraint, and min_numa_frac.  Returns the same dict
    shape so _extract_solution works unchanged (patterns_df / phys_hosts_df will be
    empty because H_per_cluster contains empty lists).
    """
    K = K_node + K_proxy
    I = len(bundle_groups)
    instance_types = [bg.instance_type for bg in bundle_groups]
    clusters = sorted({c for bg in bundle_groups for c in bg.bundles_by_cluster})

    N_node = len(node_catalog_cpu)
    N_proxy = len(proxy_catalog_cpu)

    min_node_cpu = min(node_catalog_cpu) if node_catalog_cpu else 1
    min_node_mem = min(node_catalog_mem) if node_catalog_mem else 1
    min_node_net = min(node_catalog_net) if node_catalog_net else 1
    min_proxy_cpu = min(proxy_catalog_cpu) if proxy_catalog_cpu else 1
    min_proxy_mem = min(proxy_catalog_mem) if proxy_catalog_mem else 1
    min_proxy_net = min(proxy_catalog_net) if proxy_catalog_net else 1
    max_node_cpu = max(node_catalog_cpu) if node_catalog_cpu else 1
    max_node_mem = max(node_catalog_mem) if node_catalog_mem else 1
    max_node_net = max(node_catalog_net) if node_catalog_net else 1
    max_proxy_cpu = max(proxy_catalog_cpu) if proxy_catalog_cpu else 1
    max_proxy_mem = max(proxy_catalog_mem) if proxy_catalog_mem else 1
    max_proxy_net = max(proxy_catalog_net) if proxy_catalog_net else 1

    a_int = int(round(resource_coefficients["a"] * _cost_scale))
    b_int = int(round(resource_coefficients["b"] * _cost_scale))
    c_int = int(round(resource_coefficients["c"] * _cost_scale))

    model = cp_model.CpModel()

    _effective_ws = warm_start_data
    if warm_start_data is not None and "sizes_df" in warm_start_data:
        _enriched_sizes_df = _enrich_sizes_df_with_catalog(
            warm_start_data["sizes_df"],
            warm_start_data["prev_k_node"],
            warm_start_data["prev_k_proxy"],
            K_node,
            K_proxy,
            node_catalog_cpu,
            node_catalog_mem,
            node_catalog_net,
            proxy_catalog_cpu,
            proxy_catalog_mem,
            proxy_catalog_net,
            verbose=verbose,
        )
        if _enriched_sizes_df is not warm_start_data["sizes_df"]:
            _effective_ws = dict(warm_start_data, sizes_df=_enriched_sizes_df)

    _fixed_ids = list(fixed_container_ids) if fixed_container_ids is not None else None

    assert not (
        warm_start_size_based_x_hints and _effective_ws is None
    ), "warm_start_size_based_x_hints=True requires warm_start_data"
    _assignments_recomputed = False
    if warm_start_size_based_x_hints:
        _effective_ws = dict(
            _effective_ws,
            assign_df=_recompute_assign_df_from_sizes(
                _effective_ws, bundle_groups, K_node, K_proxy, resource_coefficients
            ),
        )
        _assignments_recomputed = True

    changed = None

    sel_node = [model.NewIntVar(0, N_node - 1, f"sel_node_{t}") for t in range(K_node)]
    sel_proxy = [model.NewIntVar(0, N_proxy - 1, f"sel_proxy_{j}") for j in range(K_proxy)]

    if K_node > 1:
        model.AddAllDifferent(sel_node)
    if K_proxy > 1:
        model.AddAllDifferent(sel_proxy)

    cpu_size_node, mem_size_node, net_size_node = [], [], []
    for t in range(K_node):
        cv = model.NewIntVar(min_node_cpu, max_node_cpu, f"cpu_node_{t}")
        mv = model.NewIntVar(min_node_mem, max_node_mem, f"mem_node_{t}")
        nv = model.NewIntVar(min_node_net, max_node_net, f"net_node_{t}")
        model.AddElement(sel_node[t], node_catalog_cpu, cv)
        model.AddElement(sel_node[t], node_catalog_mem, mv)
        model.AddElement(sel_node[t], node_catalog_net, nv)
        cpu_size_node.append(cv)
        mem_size_node.append(mv)
        net_size_node.append(nv)

    cpu_size_proxy, mem_size_proxy, net_size_proxy = [], [], []
    for j in range(K_proxy):
        cv = model.NewIntVar(min_proxy_cpu, max_proxy_cpu, f"cpu_proxy_{j}")
        mv = model.NewIntVar(min_proxy_mem, max_proxy_mem, f"mem_proxy_{j}")
        nv = model.NewIntVar(min_proxy_net, max_proxy_net, f"net_proxy_{j}")
        model.AddElement(sel_proxy[j], proxy_catalog_cpu, cv)
        model.AddElement(sel_proxy[j], proxy_catalog_mem, mv)
        model.AddElement(sel_proxy[j], proxy_catalog_net, nv)
        cpu_size_proxy.append(cv)
        mem_size_proxy.append(mv)
        net_size_proxy.append(nv)

    cpu_size = cpu_size_node + cpu_size_proxy
    mem_size = mem_size_node + mem_size_proxy
    net_size = net_size_node + net_size_proxy

    if _fixed_ids is not None:
        _fix_src_df = None
        if prev_sizes_df is not None and "CatalogIndex" in prev_sizes_df.columns:
            _fix_src_df = prev_sizes_df
        elif _effective_ws is not None and "sizes_df" in _effective_ws:
            _ws_sz = _effective_ws["sizes_df"]
            if "CatalogIndex" in _ws_sz.columns:
                _fix_src_df = _ws_sz
        if _fix_src_df is not None:
            _fix_map = _fix_src_df.set_index("ContainerTypeID").to_dict(orient="index")
            for k in _fixed_ids:
                if k < K_node and k in _fix_map:
                    model.Add(sel_node[k] == int(_fix_map[k]["CatalogIndex"]))
                elif k < K and k in _fix_map:
                    model.Add(sel_proxy[k - K_node] == int(_fix_map[k]["CatalogIndex"]))

    if prev_sizes_df is not None and "CatalogIndex" in prev_sizes_df.columns and max_changed_sizes is not None:
        prev_map = prev_sizes_df.set_index("ContainerTypeID").to_dict(orient="index")
        prev_sel_node = [int(prev_map[t]["CatalogIndex"]) for t in range(K_node) if t in prev_map]
        prev_sel_proxy = [int(prev_map[K_node + j]["CatalogIndex"]) for j in range(K_proxy) if (K_node + j) in prev_map]
        if len(prev_sel_node) == K_node and len(prev_sel_proxy) == K_proxy:
            fixed_set = set(_fixed_ids or [])
            free_node = [t for t in range(K_node) if t not in fixed_set]
            free_proxy = [j for j in range(K_proxy) if (K_node + j) not in fixed_set]
            changed_node = [model.NewBoolVar(f"ch_node_{t}") for t in free_node]
            changed_proxy = [model.NewBoolVar(f"ch_proxy_{j}") for j in free_proxy]
            for i_t, t in enumerate(free_node):
                model.Add(sel_node[t] == prev_sel_node[t]).OnlyEnforceIf(changed_node[i_t].Not())
            for i_j, j in enumerate(free_proxy):
                model.Add(sel_proxy[j] == prev_sel_proxy[j]).OnlyEnforceIf(changed_proxy[i_j].Not())
            model.Add(sum(changed_node) + sum(changed_proxy) <= int(max_changed_sizes))
            model.Add(sum(changed_node) + sum(changed_proxy) >= 1)
            changed = changed_node + changed_proxy

    x = {(i, k): model.NewBoolVar(f"x_{i}_{k}") for i in range(I) for k in range(K)}
    for i in range(I):
        if instance_types[i] == 'node':
            model.Add(sum(x[i, k] for k in range(K_node)) == 1)
            model.Add(sum(x[i, k] for k in range(K_node, K)) == 0)
        else:
            model.Add(sum(x[i, k] for k in range(K_node)) == 0)
            model.Add(sum(x[i, k] for k in range(K_node, K)) == 1)

    I_node = sum(1 for it in instance_types if it == 'node')
    I_proxy = I - I_node
    min_node = math.ceil(min_usage_frac_node * I_node) if min_usage_frac_node > 0 and I_node > 0 else 0
    min_proxy = math.ceil(min_usage_frac_proxy * I_proxy) if min_usage_frac_proxy > 0 and I_proxy > 0 else 0
    if min_node > 0:
        for k in range(K_node):
            model.Add(sum(x[i, k] for i in range(I) if instance_types[i] == 'node') >= min_node)
    if min_proxy > 0:
        for k in range(K_node, K):
            model.Add(sum(x[i, k] for i in range(I) if instance_types[i] == 'proxy') >= min_proxy)

    assert n_min_tables is not None and infeasible_catalog is not None

    for i, bg in enumerate(bundle_groups):
        itype = instance_types[i]
        k_range = range(K_node) if itype == 'node' else range(K_node, K)
        infeasible_i = infeasible_catalog.get(i, set())
        if not infeasible_i:
            continue
        for k in k_range:
            sel_k = sel_node[k] if k < K_node else sel_proxy[k - K_node]
            x_ik = x[i, k]
            if isinstance(sel_k, int):
                if sel_k in infeasible_i:
                    if isinstance(x_ik, int):
                        assert x_ik == 0, f"Infeasible fixed: group {i}, k={k}, idx={sel_k}"
                    else:
                        model.Add(x_ik == 0)
            elif not isinstance(x_ik, int):
                model.AddForbiddenAssignments([sel_k, x_ik], [(idx, 1) for idx in infeasible_i])

    n_vars: dict = {}
    prod_vars: dict = {}
    U_ic: dict = {}

    for i, bg in enumerate(bundle_groups):
        itype = instance_types[i]
        k_range = range(K_node) if itype == 'node' else range(K_node, K)

        for c in clusters:
            base = bg.counts_by_cluster.get(c, 0)

            if base == 0:
                for k in k_range:
                    n_vars[i, c, k] = 0
                    prod_vars[i, c, k] = 0
                continue

            U_ic[i, c] = cfg.MAX_EXTRA_RATIO * base
            nm_table = n_min_tables[i, c]
            nm_max = cfg.MAX_EXTRA_RATIO * base

            for k in k_range:
                sel_k = sel_node[k] if k < K_node else sel_proxy[k - K_node]
                x_ik = x[i, k]

                if isinstance(sel_k, int):
                    n_val = nm_table[sel_k]
                    n_vars[i, c, k] = n_val
                    if isinstance(x_ik, int):
                        prod_vars[i, c, k] = n_val * x_ik
                    else:
                        prod_v = model.NewIntVar(0, n_val, f"prod_{i}_{c}_{k}")
                        model.Add(prod_v == n_val).OnlyEnforceIf(x_ik)
                        model.Add(prod_v == 0).OnlyEnforceIf(x_ik.Not())
                        prod_vars[i, c, k] = prod_v
                else:
                    nm_v = model.NewIntVar(1, nm_max, f"nm_{i}_{c}_{k}")
                    model.AddElement(sel_k, nm_table, nm_v)
                    n_vars[i, c, k] = nm_v

                    if isinstance(x_ik, int):
                        prod_vars[i, c, k] = nm_v if x_ik == 1 else 0
                    else:
                        prod_v = model.NewIntVar(0, nm_max, f"prod_{i}_{c}_{k}")
                        model.Add(prod_v <= nm_v)
                        model.Add(prod_v >= nm_v - nm_max + nm_max * x_ik)
                        model.Add(prod_v <= nm_max * x_ik)
                        prod_vars[i, c, k] = prod_v

    D = {}
    for c in clusters:
        for k in range(K):
            k_is_node = k < K_node
            terms_var = [
                prod_vars[i, c, k]
                for i, bg in enumerate(bundle_groups)
                if (bg.instance_type == 'node') == k_is_node and not isinstance(prod_vars.get((i, c, k), 0), int)
            ]
            fixed_sum = sum(
                prod_vars[i, c, k]
                for i, bg in enumerate(bundle_groups)
                if (bg.instance_type == 'node') == k_is_node and isinstance(prod_vars.get((i, c, k), 0), int)
            )
            D_ub = sum(
                U_ic.get((i, c), bg.counts_by_cluster.get(c, 0))
                for i, bg in enumerate(bundle_groups)
                if (bg.instance_type == 'node') == k_is_node
            )
            if terms_var:
                D_v = model.NewIntVar(0, max(1, D_ub), f"D_{c}_{k}")
                model.Add(D_v == sum(terms_var) + fixed_sum)
                D[c, k] = D_v
            else:
                D[c, k] = fixed_sum

    t_table_node = [
        a_int * node_catalog_cpu[j] + b_int * node_catalog_mem[j] + c_int * node_catalog_net[j] for j in range(N_node)
    ]
    t_table_proxy = [
        a_int * proxy_catalog_cpu[j] + b_int * proxy_catalog_mem[j] + c_int * proxy_catalog_net[j]
        for j in range(N_proxy)
    ]

    t = []
    for t_idx in range(K_node):
        t_lb = min(t_table_node)
        t_ub_k = max(t_table_node)
        t_k = model.NewIntVar(t_lb, t_ub_k, f"t_{t_idx}")
        model.AddElement(sel_node[t_idx], t_table_node, t_k)
        t.append(t_k)
    for j in range(K_proxy):
        t_lb = min(t_table_proxy)
        t_ub_k = max(t_table_proxy)
        t_k = model.NewIntVar(t_lb, t_ub_k, f"t_{K_node + j}")
        model.AddElement(sel_proxy[j], t_table_proxy, t_k)
        t.append(t_k)
    t_ub = max(max(t_table_node), max(t_table_proxy))

    cost_ck = {}
    for c in clusters:
        for k in range(K):
            D_ck = D[c, k]
            if isinstance(D_ck, int):
                if D_ck > 0:
                    cost_v = model.NewIntVar(0, D_ck * t_ub, f"cost_{c}_{k}")
                    model.Add(cost_v == D_ck * t[k])
                    cost_ck[c, k] = cost_v
            else:
                k_is_node = k < K_node
                D_ub = sum(
                    U_ic.get((i, c), bg.counts_by_cluster.get(c, 0))
                    for i, bg in enumerate(bundle_groups)
                    if (bg.instance_type == "node") == k_is_node
                )
                cost_v = model.NewIntVar(0, max(1, D_ub * t_ub), f"cost_{c}_{k}")
                model.AddMultiplicationEquality(cost_v, [D_ck, t[k]])
                cost_ck[c, k] = cost_v
    container_cost_int_expr = sum(cost_ck.values()) if cost_ck else 0

    apply_warm_start_hints(
        model,
        sel_node,
        sel_proxy,
        cpu_size,
        mem_size,
        net_size,
        x,
        n_vars,
        {},
        {},
        bundle_groups,
        K_node,
        K_proxy,
        _effective_ws,
        node_catalog_cpu,
        node_catalog_mem,
        node_catalog_net,
        proxy_catalog_cpu,
        proxy_catalog_mem,
        proxy_catalog_net,
        sizes_fixed=False,
        assignments_fixed=False,
        assignments_recomputed=_assignments_recomputed,
        prod_vars=prod_vars,
        n_min_tables=n_min_tables,
        host_n_patterns={},
    )

    return {
        "model": model,
        "K": K,
        "I": I,
        "clusters": clusters,
        "H_per_cluster": {c: [] for c in clusters},
        "host_cost_int_expr": 0,
        "container_cost_int_expr": container_cost_int_expr,
        "x": x,
        "sel_node": sel_node,
        "sel_proxy": sel_proxy,
        "cpu_size": cpu_size,
        "mem_size": mem_size,
        "net_size": net_size,
        "D": D,
        "n_vars": n_vars,
        "n_numa_nodes": {},
        "f": {},
        "instance_types": instance_types,
        "bundle_groups": bundle_groups,
        "K_node": K_node,
        "K_proxy": K_proxy,
        "_cost_scale": _cost_scale,
        "changed": changed,
        "resource_coefficients": resource_coefficients,
        "node_catalog_cpu": node_catalog_cpu,
        "node_catalog_mem": node_catalog_mem,
        "node_catalog_net": node_catalog_net,
        "proxy_catalog_cpu": proxy_catalog_cpu,
        "proxy_catalog_mem": proxy_catalog_mem,
        "proxy_catalog_net": proxy_catalog_net,
        "host_n_patterns": {},
    }


# ---------------------------------------------------------------------------
# Solution extraction
# ---------------------------------------------------------------------------


def _extract_solution(solver, build, status):
    def _val(v):
        return v if isinstance(v, int) else solver.Value(v)

    K = build["K"]
    I = build["I"]
    bundle_groups_list = build["bundle_groups"]
    sel_node = build["sel_node"]
    sel_proxy = build["sel_proxy"]
    cpu_size = build["cpu_size"]
    mem_size = build["mem_size"]
    net_size = build["net_size"]
    x = build["x"]
    n_vars = build["n_vars"]
    clusters = build["clusters"]
    H_per_cluster = build["H_per_cluster"]
    n_numa_nodes = build["n_numa_nodes"]
    f = build["f"]
    K_node = build["K_node"]
    K_proxy = build["K_proxy"]
    host_cost_int_expr = build["host_cost_int_expr"]
    container_cost_int_expr = build["container_cost_int_expr"]
    _cost_scale = build["_cost_scale"]
    host_n_patterns = build["host_n_patterns"]

    empty = pd.DataFrame()
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return empty, empty, empty, empty, empty

    host_cost = _val(host_cost_int_expr) / _cost_scale
    container_cost = _val(container_cost_int_expr) / _cost_scale

    res_df = pd.DataFrame(
        [
            {
                "status": solver.StatusName(status),
                "host_cost": float(host_cost),
                "container_cost": float(container_cost),
            }
        ]
    )

    sizes_rows = []
    for k in range(K):
        cat_idx = _val(sel_node[k]) if k < K_node else _val(sel_proxy[k - K_node])
        sizes_rows.append(
            {
                "ContainerTypeID": k,
                "InstanceType": "node" if k < K_node else "proxy",
                "CPU": _val(cpu_size[k]) * cfg.CPU_STEP,
                "Network": _val(net_size[k]) * cfg.NET_STEP,
                "Memory": _val(mem_size[k]) * cfg.MEM_STEP,
                "CatalogIndex": cat_idx,
            }
        )
    sizes_df = pd.DataFrame(sizes_rows)

    # Состав строки — тот же, что у simple.optimize_assignment: результаты обоих
    # солверов читают одни и те же потребители. Имени конфигурации здесь нет:
    # размеры приходят из сгенерированного каталога, а не из бандл-контроллера.
    # TODO: зоны доступности не учитываются, счётчики позонные — как и вся модель.
    # Для cross-dc кластера каждый ДЦ считать отдельным кластером.
    part_ids = {}
    for bg in bundle_groups_list:
        for part in bg.parts:
            part_id = len(part_ids)
            for bundle_key in part:
                part_ids[bundle_key] = part_id

    assign_rows = []
    for i, bg in enumerate(bundle_groups_list):
        assigned_k = next((k for k in range(K) if _val(x[i, k])), None)
        if assigned_k is None:
            continue
        cpu_s = _val(cpu_size[assigned_k])
        mem_s = _val(mem_size[assigned_k])
        net_s = _val(net_size[assigned_k])
        for c, blist in bg.bundles_by_cluster.items():
            for bi in blist:
                cpu_r, mem_r, net_r = bg.bundle_key_max_req[bi.bundle]
                new_cnt, _ = compute_n_min_from_req(
                    bg.instance_type,
                    bi.count,
                    cpu_r,
                    mem_r,
                    net_r,
                    cpu_s,
                    mem_s,
                    net_s,
                )
                assign_rows.append(
                    {
                        "GroupID": i,
                        "SubGroupID": part_ids[bi.bundle],
                        "Cluster": bi.cluster,
                        "BundleName": bi.name,
                        "Bundle": bi.bundle,
                        "InstanceType": bg.instance_type,
                        "AssignedContainerTypeID": assigned_k,
                        "NewCount": new_cnt,
                        "BaseCount": bi.count,
                        "UsageCPU": bi.cpu,
                        "UsageMemory": bi.memory,
                        "UsageNetwork": bi.network,
                        "BaseContainerType": bi.container_type.name,
                        "BaseCPU": bi.container_type.cpu_limit,
                        "BaseMemory": bi.container_type.mem_limit,
                        "BaseNetwork": bi.container_type.net_limit,
                        "NewContainerType": None,
                        "NewCPU": cpu_s * cfg.CPU_STEP,
                        "NewMemory": mem_s * cfg.MEM_STEP,
                        "NewNetwork": net_s * cfg.NET_STEP,
                    }
                )
    assign_df = pd.DataFrame(assign_rows)

    patterns_rows, phys_hosts_rows = [], []
    for c in clusters:
        for h in H_per_cluster.get(c, []):
            host_key = h.host_key
            nodes_per_host = h.numa_nodes_per_host
            total_numa_nodes_used = 0
            P = host_n_patterns[(c, host_key)]
            for p in range(P):
                used = solver.Value(n_numa_nodes[c, host_key, p])
                if used > 0:
                    total_numa_nodes_used += used
                    for k in range(K):
                        f_count = solver.Value(f[c, host_key, p, k])
                        if f_count > 0:
                            patterns_rows.append(
                                {
                                    "Cluster": c,
                                    "HostModel": host_key,
                                    "Pattern": p,
                                    "NumaNodesUsed": used,
                                    "ContainerTypeID": k,
                                    "CountOnNode": f_count,
                                    "CPU": _val(cpu_size[k]) * cfg.CPU_STEP,
                                    "Memory": _val(mem_size[k]) * cfg.MEM_STEP,
                                    "Network": _val(net_size[k]) * cfg.NET_STEP,
                                    "InstanceType": "node" if k < K_node else "proxy",
                                }
                            )
            if total_numa_nodes_used > 0:
                phys_hosts_rows.append(
                    {
                        "Cluster": c,
                        "HostModel": host_key,
                        "PhysicalHostsNeeded": math.ceil(total_numa_nodes_used / (nodes_per_host or 1)),
                        "TotalNumaNodesUsed": total_numa_nodes_used,
                    }
                )

    patterns_df = pd.DataFrame(patterns_rows)
    phys_hosts_df = pd.DataFrame(phys_hosts_rows)
    return res_df, sizes_df, assign_df, patterns_df, phys_hosts_df
