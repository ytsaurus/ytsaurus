"""
Catalog generation and scarcity pricing.

Imports from: config, utils, data.
"""

import math

import numpy as np
import pandas as pd

from .scripts import shared as cfg
from .scripts.shared import disc_round

# ---------------------------------------------------------------------------
# Scarcity prices
# ---------------------------------------------------------------------------


def compute_resource_coefficients(bundle_data: list, hosts: list):
    demand_cpu = sum(bi.count * bi.cpu_req_int() for bi in bundle_data)
    demand_mem = sum(bi.count * bi.mem_req_int() for bi in bundle_data)
    demand_net = sum(bi.count * bi.net_req_int() for bi in bundle_data)

    supply_cpu = max(1, sum(h.supply_cpu() for h in hosts))
    supply_mem = max(1, sum(h.supply_mem() for h in hosts))
    supply_net = max(1, sum(h.supply_net() for h in hosts))

    raw_p_cpu = demand_cpu / supply_cpu
    raw_p_mem = demand_mem / supply_mem
    raw_p_net = demand_net / supply_net

    # Normalize by average supply per NUMA node so that coefficients are
    # dimensionless (fraction-of-NUMA-node units). This ensures that resources
    # with different physical scales (e.g. 28 CPU cores vs 600 MiB/s network)
    # are weighted correctly: a container consuming 100% of one resource and
    # 0% of another costs the same regardless of the numeric scale of that resource.
    # The formula: w_r = raw_p_r / avg_r_per_numa  (= demand_r / supply_r²  × N)
    # Then: a * cpu_int = raw_p_cpu * (cpu_int / avg_cpu_per_numa) = scarcity × NUMA fraction.
    total_numa_nodes = max(1, sum(h.total_available_numa_nodes for h in hosts))
    avg_cpu_per_numa = supply_cpu / total_numa_nodes
    avg_mem_per_numa = supply_mem / total_numa_nodes
    avg_net_per_numa = supply_net / total_numa_nodes

    w_cpu = raw_p_cpu / avg_cpu_per_numa
    w_mem = raw_p_mem / avg_mem_per_numa
    w_net = raw_p_net / avg_net_per_numa

    mx = max(w_cpu, w_mem, w_net, 1e-12)
    resource_coefficients = {
        "a": float(w_cpu / mx),
        "b": float(w_mem / mx),
        "c": float(w_net / mx),
    }
    debug = {
        "demand_int": {"cpu": demand_cpu, "mem": demand_mem, "net": demand_net},
        "supply_int": {"cpu": supply_cpu, "mem": supply_mem, "net": supply_net},
        "total_numa_nodes": total_numa_nodes,
        "avg_per_numa_int": {"cpu": avg_cpu_per_numa, "mem": avg_mem_per_numa, "net": avg_net_per_numa},
        "raw_prices": {"cpu": raw_p_cpu, "mem": raw_p_mem, "net": raw_p_net},
        "weighted_prices": {"cpu": w_cpu, "mem": w_mem, "net": w_net},
        "resource_coefficients": resource_coefficients,
    }
    return resource_coefficients, debug


def compute_allocation_scarcity_coefficients(bundle_data: list):
    """Цены ресурсов по спросу против уже выделенного динтаблицам.

    То же, что compute_resource_coefficients, но пулом считается сумма гарантий
    текущих контейнеров, а не ресурсы хостов: на MR-кластере хост делится с чужой
    нагрузкой, и весь его объём динтаблицам не принадлежит. Нормировка — на
    средний контейнер, он здесь играет роль NUMA-узла.
    """
    total_instances = max(1, sum(bi.total_count for bi in bundle_data))

    demand_cpu = sum(bi.total_count * bi.cpu_req_int() for bi in bundle_data)
    demand_mem = sum(bi.total_count * bi.mem_req_int() for bi in bundle_data)
    demand_net = sum(bi.total_count * bi.net_req_int() for bi in bundle_data)

    supply_cpu = max(1, sum(bi.total_count * bi.container_type.cpu_limit_int() for bi in bundle_data))
    supply_mem = max(1, sum(bi.total_count * bi.container_type.mem_limit_int() for bi in bundle_data))
    supply_net = max(1, sum(bi.total_count * bi.container_type.net_limit_int() for bi in bundle_data))

    raw_p_cpu = demand_cpu / supply_cpu
    raw_p_mem = demand_mem / supply_mem
    raw_p_net = demand_net / supply_net

    avg_cpu_per_container = supply_cpu / total_instances
    avg_mem_per_container = supply_mem / total_instances
    avg_net_per_container = supply_net / total_instances

    w_cpu = raw_p_cpu / avg_cpu_per_container
    w_mem = raw_p_mem / avg_mem_per_container
    w_net = raw_p_net / avg_net_per_container

    mx = max(w_cpu, w_mem, w_net, 1e-12)
    resource_coefficients = {
        "a": float(w_cpu / mx),
        "b": float(w_mem / mx),
        "c": float(w_net / mx),
    }
    debug = {
        "demand_int": {"cpu": demand_cpu, "mem": demand_mem, "net": demand_net},
        "supply_int": {"cpu": supply_cpu, "mem": supply_mem, "net": supply_net},
        "total_instances": total_instances,
        "utilization": {"cpu": raw_p_cpu, "mem": raw_p_mem, "net": raw_p_net},
        "avg_per_container_int": {
            "cpu": avg_cpu_per_container,
            "mem": avg_mem_per_container,
            "net": avg_net_per_container,
        },
        "resource_coefficients": resource_coefficients,
    }
    return resource_coefficients, debug


def build_host_value_df(hosts: list, resource_coefficients: dict) -> pd.DataFrame:
    rows = []
    for h in hosts:
        nv = h.numa_node_value(resource_coefficients)
        rows.append(
            {
                "cluster": h.cluster,
                "cpu_model": h.cpu_model,
                "host_key": h.host_key,
                "numa_nodes_per_host": h.numa_nodes_per_host,
                "numa_node_cpu": h.numa_node_cpu,
                "numa_node_mem": h.numa_node_mem,
                "numa_node_net": h.numa_node_net,
                "available_physical_hosts": h.available_physical_hosts,
                "numa_node_value": nv,
                "host_value": nv * h.numa_nodes_per_host,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Overhead
# ---------------------------------------------------------------------------


def _compute_overhead_int_per_group(bg) -> tuple:
    if bg.instance_type == "node":
        abs_cpu = cfg.OVERHEAD_CPU_ABS
        abs_mem = cfg.OVERHEAD_MEM_ABS
        abs_net = cfg.OVERHEAD_NET_ABS
        frac_cpu = cfg.OVERHEAD_CPU_FRAC
        frac_mem = cfg.OVERHEAD_MEM_FRAC
        frac_net = cfg.OVERHEAD_NET_FRAC
    else:
        abs_cpu = cfg.PROXY_OVERHEAD_CPU_ABS
        abs_mem = cfg.PROXY_OVERHEAD_MEM_ABS
        abs_net = cfg.PROXY_OVERHEAD_NET_ABS
        frac_cpu = cfg.PROXY_OVERHEAD_CPU_FRAC
        frac_mem = cfg.PROXY_OVERHEAD_MEM_FRAC
        frac_net = cfg.PROXY_OVERHEAD_NET_FRAC

    o_cpu = max(disc_round(abs_cpu, cfg.CPU_STEP), math.ceil(bg.cpu_req_int * frac_cpu))
    o_mem = max(disc_round(abs_mem, cfg.MEM_STEP), math.ceil(bg.mem_req_int * frac_mem))
    o_net = max(disc_round(abs_net, cfg.NET_STEP), math.ceil(bg.net_req_int * frac_net))
    return o_cpu, o_mem, o_net


# ---------------------------------------------------------------------------
# Catalog generation (node — NUMA-divisor based)
# ---------------------------------------------------------------------------


def generate_container_catalog(
    hosts: list,
    instance_type: str,
    resource_coefficients: dict,
    n_limit: int,
    max_cpu: float,
    max_mem: float,
    max_net: float,
    numa_div_max_t: int = 34,
    numa_frac_p_max: int = 11,
    mandatory_sizes: list | None = None,
    neighborhood_spread: float = 0.4,
) -> tuple:
    """NUMA-divisor based catalog. Returns (cat_cpu, cat_mem, cat_net, is_warm_start)."""
    _min_cpu = cfg.MIN_PROXY_CPU if instance_type == "proxy" else cfg.MIN_CONTAINER_CPU
    _min_mem = cfg.MIN_PROXY_MEM if instance_type == "proxy" else cfg.MIN_CONTAINER_MEM
    _min_net = cfg.MIN_PROXY_NET if instance_type == "proxy" else cfg.MIN_CONTAINER_NET
    _cpu_step = cfg.PROXY_CONTAINER_CPU_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_CPU_STEP
    _mem_step = cfg.PROXY_CONTAINER_MEM_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_MEM_STEP
    _net_step = cfg.PROXY_CONTAINER_NET_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_NET_STEP

    def _quantize_and_clip(cpu_r, mem_r, net_r):
        cpu_r = max(_min_cpu, min(max_cpu, math.floor(cpu_r / _cpu_step) * _cpu_step))
        mem_r = max(_min_mem, min(max_mem, math.floor(mem_r / _mem_step) * _mem_step))
        net_r = max(_min_net, min(max_net, math.floor(net_r / _net_step) * _net_step))
        return disc_round(cpu_r, cfg.CPU_STEP), disc_round(mem_r, cfg.MEM_STEP), disc_round(net_r, cfg.NET_STEP)

    def _quantize_only(cpu_r, mem_r, net_r):
        cpu_r = math.floor(cpu_r / _cpu_step) * _cpu_step
        mem_r = math.floor(mem_r / _mem_step) * _mem_step
        net_r = math.floor(net_r / _net_step) * _net_step
        return disc_round(cpu_r, cfg.CPU_STEP), disc_round(mem_r, cfg.MEM_STEP), disc_round(net_r, cfg.NET_STEP)

    from math import gcd

    _fracs = [
        (p, q) for q in range(1, numa_div_max_t + 1) for p in range(1, min(numa_frac_p_max, q) + 1) if gcd(p, q) == 1
    ]
    numa_variants: set = set()
    for h in hosts:
        for p, q in _fracs:
            cpu_r = math.floor(h.numa_node_cpu * p / q / _cpu_step) * _cpu_step
            mem_r = math.floor(h.numa_node_mem * p / q / _mem_step) * _mem_step
            net_r = math.floor(h.numa_node_net * p / q / _net_step) * _net_step
            if (
                cpu_r < _min_cpu
                or cpu_r > max_cpu
                or mem_r < _min_mem
                or mem_r > max_mem
                or net_r < _min_net
                or net_r > max_net
            ):
                continue
            numa_variants.add(
                (disc_round(cpu_r, cfg.CPU_STEP), disc_round(mem_r, cfg.MEM_STEP), disc_round(net_r, cfg.NET_STEP))
            )
    numa_variants_list = list(numa_variants) or [_quantize_and_clip(_min_cpu, _min_mem, _min_net)]

    _range_cpu = max(1, disc_round(max_cpu, cfg.CPU_STEP) - disc_round(_min_cpu, cfg.CPU_STEP))
    _range_mem = max(1, disc_round(max_mem, cfg.MEM_STEP) - disc_round(_min_mem, cfg.MEM_STEP))
    _range_net = max(1, disc_round(max_net, cfg.NET_STEP) - disc_round(_min_net, cfg.NET_STEP))

    def _norm_dist(a, b):
        return abs(a[0] - b[0]) / _range_cpu + abs(a[1] - b[1]) / _range_mem + abs(a[2] - b[2]) / _range_net

    def _nearest_variant(target):
        best, best_d = None, float('inf')
        for v in numa_variants_list:
            d = _norm_dist(v, target)
            if d < best_d:
                best_d = d
                best = v
        return best

    a = resource_coefficients["a"]
    b = resource_coefficients["b"]
    c_coef = resource_coefficients["c"]

    def _cost(t):
        cv, mv, nv = t
        return a * cv + b * mv + c_coef * nv

    def _pick_evenly(lst, k):
        if not lst or k <= 0:
            return []
        if len(lst) <= k:
            return list(lst)
        idxs = np.round(np.linspace(0, len(lst) - 1, k)).astype(int)
        return [lst[i] for i in idxs]

    _LOG_BASE_OFFSETS = [-1.0, -0.7, -0.5, -0.3, -0.15, 0.0, 0.15, 0.3, 0.5, 0.7, 1.0]
    scales = sorted(set(math.exp(o * neighborhood_spread) for o in _LOG_BASE_OFFSETS))

    mandatory_set: set = set()
    mandatory_list: list = []
    if mandatory_sizes:
        for cpu_i, mem_i, net_i in mandatory_sizes:
            entry = _quantize_only(cpu_i * cfg.CPU_STEP, mem_i * cfg.MEM_STEP, net_i * cfg.NET_STEP)
            if entry not in mandatory_set:
                mandatory_set.add(entry)
                mandatory_list.append(entry)

    per_mandatory_candidates: list = []
    for m_entry in mandatory_list:
        cpu_m = m_entry[0] * cfg.CPU_STEP
        mem_m = m_entry[1] * cfg.MEM_STEP
        net_m = m_entry[2] * cfg.NET_STEP
        seen: set = set()
        for s in scales:
            for target_real in [
                (cpu_m * s, mem_m * s, net_m * s),
                (cpu_m * s, mem_m, net_m),
                (cpu_m, mem_m * s, net_m),
                (cpu_m, mem_m, net_m * s),
            ]:
                seen.add(_nearest_variant(_quantize_and_clip(*target_real)))
        cands_with_dist = sorted(((v, _norm_dist(v, m_entry)) for v in seen), key=lambda x: x[1])
        per_mandatory_candidates.append([v for v, _ in cands_with_dist])

    if mandatory_list:
        n_mandatory = len(mandatory_list)
        budget_per = max(1, n_limit // n_mandatory)
        picked: set = set(mandatory_list)
        for cands in per_mandatory_candidates:
            for v in _pick_evenly(cands, budget_per):
                picked.add(v)
        cand = sorted(picked, key=lambda t: (_cost(t), t[0], t[1], t[2]))
        if len(cand) > n_limit:
            non_mandatory = [t for t in cand if t not in mandatory_set]
            n_extra = max(0, n_limit - n_mandatory)
            cand = sorted(
                list(mandatory_set) + _pick_evenly(non_mandatory, n_extra),
                key=lambda t: (_cost(t), t[0], t[1], t[2]),
            )
    else:
        all_variants = sorted(numa_variants_list, key=lambda t: (_cost(t), t[0], t[1], t[2]))
        cand = _pick_evenly(all_variants, n_limit) if len(all_variants) > n_limit else all_variants
        cand.sort(key=lambda t: (_cost(t), t[0], t[1], t[2]))

    if not cand:
        cand = [_quantize_and_clip(_min_cpu, _min_mem, _min_net)]

    is_warm_start = [t in mandatory_set for t in cand]
    return [t[0] for t in cand], [t[1] for t in cand], [t[2] for t in cand], is_warm_start


# ---------------------------------------------------------------------------
# Catalog generation (proxy — log-normal sampling)
# ---------------------------------------------------------------------------


def generate_container_catalog_proxy(
    instance_type: str,
    resource_coefficients: dict,
    n_limit: int,
    max_cpu: float,
    max_mem: float,
    max_net: float,
    mandatory_sizes: list | None = None,
    neighborhood_spread: float = 0.2,
) -> tuple:
    """Log-normal sampling based catalog. Returns (cat_cpu, cat_mem, cat_net, is_warm_start)."""
    _min_cpu = cfg.MIN_PROXY_CPU if instance_type == "proxy" else cfg.MIN_CONTAINER_CPU
    _min_mem = cfg.MIN_PROXY_MEM if instance_type == "proxy" else cfg.MIN_CONTAINER_MEM
    _min_net = cfg.MIN_PROXY_NET if instance_type == "proxy" else cfg.MIN_CONTAINER_NET
    _cpu_step = cfg.PROXY_CONTAINER_CPU_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_CPU_STEP
    _mem_step = cfg.PROXY_CONTAINER_MEM_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_MEM_STEP
    _net_step = cfg.PROXY_CONTAINER_NET_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_NET_STEP

    def _quantize_and_clip(cpu_r, mem_r, net_r):
        cpu_r = max(_min_cpu, min(max_cpu, math.floor(cpu_r / _cpu_step) * _cpu_step))
        mem_r = max(_min_mem, min(max_mem, math.floor(mem_r / _mem_step) * _mem_step))
        net_r = max(_min_net, min(max_net, math.floor(net_r / _net_step) * _net_step))
        return disc_round(cpu_r, cfg.CPU_STEP), disc_round(mem_r, cfg.MEM_STEP), disc_round(net_r, cfg.NET_STEP)

    def _quantize_only(cpu_r, mem_r, net_r):
        cpu_r = math.floor(cpu_r / _cpu_step) * _cpu_step
        mem_r = math.floor(mem_r / _mem_step) * _mem_step
        net_r = math.floor(net_r / _net_step) * _net_step
        return disc_round(cpu_r, cfg.CPU_STEP), disc_round(mem_r, cfg.MEM_STEP), disc_round(net_r, cfg.NET_STEP)

    a = resource_coefficients["a"]
    b = resource_coefficients["b"]
    c_coef = resource_coefficients["c"]

    def _cost(t):
        cv, mv, nv = t
        return a * cv + b * mv + c_coef * nv

    def _pick_evenly(lst, k):
        if not lst or k <= 0:
            return []
        if len(lst) <= k:
            return list(lst)
        idxs = np.round(np.linspace(0, len(lst) - 1, k)).astype(int)
        return [lst[i] for i in idxs]

    rng = np.random.default_rng(seed=42)

    def _sample_around(center_real, n_samples):
        if neighborhood_spread <= 0.0 or n_samples <= 0:
            return []
        cpu_c, mem_c, net_c = center_real
        log_offsets = rng.normal(0.0, neighborhood_spread, size=(n_samples, 3))
        return [
            _quantize_and_clip(
                cpu_c * math.exp(log_offsets[i, 0]),
                mem_c * math.exp(log_offsets[i, 1]),
                net_c * math.exp(log_offsets[i, 2]),
            )
            for i in range(n_samples)
        ]

    mandatory_set: set = set()
    mandatory_list: list = []
    if mandatory_sizes:
        for cpu_i, mem_i, net_i in mandatory_sizes:
            entry = _quantize_only(cpu_i * cfg.CPU_STEP, mem_i * cfg.MEM_STEP, net_i * cfg.NET_STEP)
            if entry not in mandatory_set:
                mandatory_set.add(entry)
                mandatory_list.append(entry)

    if mandatory_list:
        n_mandatory = len(mandatory_list)
        budget_per = max(1, (n_limit - n_mandatory) // n_mandatory)
        oversample = max(budget_per * 10, 200)
        picked: set = set(mandatory_list)
        per_mandatory_pool: list = []
        for m_entry in mandatory_list:
            samples = _sample_around(
                (m_entry[0] * cfg.CPU_STEP, m_entry[1] * cfg.MEM_STEP, m_entry[2] * cfg.NET_STEP),
                oversample,
            )
            unique = list(dict.fromkeys(s for s in samples if s not in mandatory_set))
            per_mandatory_pool.append(unique)

        for pool in per_mandatory_pool:
            for v in _pick_evenly(pool, budget_per):
                picked.add(v)

        if len(picked) < n_limit:
            remaining = n_limit - len(picked)
            all_extra = [v for pool in per_mandatory_pool for v in pool if v not in picked]
            all_extra_sorted = sorted(set(all_extra), key=lambda t: (_cost(t), t[0], t[1], t[2]))
            for v in _pick_evenly(all_extra_sorted, remaining):
                picked.add(v)

        cand = sorted(picked, key=lambda t: (_cost(t), t[0], t[1], t[2]))
        if len(cand) > n_limit:
            non_mandatory = [t for t in cand if t not in mandatory_set]
            n_extra = max(0, n_limit - n_mandatory)
            cand = sorted(
                list(mandatory_set) + _pick_evenly(non_mandatory, n_extra),
                key=lambda t: (_cost(t), t[0], t[1], t[2]),
            )
    else:
        n_samples = n_limit * 20
        log_cpu = rng.uniform(math.log(_min_cpu), math.log(max_cpu), n_samples)
        log_mem = rng.uniform(math.log(_min_mem), math.log(max_mem), n_samples)
        log_net = rng.uniform(math.log(_min_net), math.log(max_net), n_samples)
        all_variants = list(
            dict.fromkeys(
                _quantize_and_clip(math.exp(log_cpu[i]), math.exp(log_mem[i]), math.exp(log_net[i]))
                for i in range(n_samples)
            )
        )
        all_variants.sort(key=lambda t: (_cost(t), t[0], t[1], t[2]))
        cand = _pick_evenly(all_variants, n_limit) if len(all_variants) > n_limit else all_variants
        cand.sort(key=lambda t: (_cost(t), t[0], t[1], t[2]))

    if not cand:
        cand = [_quantize_and_clip(_min_cpu, _min_mem, _min_net)]

    is_warm_start = [t in mandatory_set for t in cand]
    return [t[0] for t in cand], [t[1] for t in cand], [t[2] for t in cand], is_warm_start


def generate_container_catalog_old(
    hosts: list,
    bundle_groups: list,
    instance_type: str,
    resource_coefficients: dict,
    n_limit: int,
    max_cpu: float,
    max_mem: float,
    max_net: float,
    numa_div_max_t: int = 24,
    grid_enabled: bool = True,
    mandatory_sizes: list | None = None,
) -> tuple:
    """
    Generate a catalog of container size candidates (cpu_int, mem_int, net_int).

    Strategy:
    1) NUMA divisors: for each host, divide NUMA node resources by t=1..T
    2) Anchors from bundle group requirements scaled by {1.0, 1.1, 1.25, 1.5, 2.0}
    3) Coarse grid across [min..max] ranges (if enabled)
    4) Dedup, Pareto-prune (remove strictly dominated), downsample to n_limit
    5) mandatory_sizes (e.g. warm start container sizes) are always included after
       downsampling, regardless of n_limit, and are marked in the returned mask.

    Returns (cat_cpu, cat_mem, cat_net, is_warm_start) as lists, where is_warm_start
    is a list of bool indicating entries that came from mandatory_sizes.
    """
    candidates = set()

    _min_cpu = cfg.MIN_PROXY_CPU if instance_type == "proxy" else cfg.MIN_CONTAINER_CPU
    _min_mem = cfg.MIN_PROXY_MEM if instance_type == "proxy" else cfg.MIN_CONTAINER_MEM
    _min_net = cfg.MIN_PROXY_NET if instance_type == "proxy" else cfg.MIN_CONTAINER_NET
    _cpu_step = cfg.PROXY_CONTAINER_CPU_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_CPU_STEP
    _mem_step = cfg.PROXY_CONTAINER_MEM_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_MEM_STEP
    _net_step = cfg.PROXY_CONTAINER_NET_STEP if instance_type == "proxy" else cfg.NODE_CONTAINER_NET_STEP

    def _quantize_and_clip(cpu_r, mem_r, net_r):
        cpu_r = max(_min_cpu, min(max_cpu, math.floor(cpu_r / _cpu_step) * _cpu_step))
        mem_r = max(_min_mem, min(max_mem, math.floor(mem_r / _mem_step) * _mem_step))
        net_r = max(_min_net, min(max_net, math.floor(net_r / _net_step) * _net_step))
        return disc_round(cpu_r, cfg.CPU_STEP), disc_round(mem_r, cfg.MEM_STEP), disc_round(net_r, cfg.NET_STEP)

    # 1) NUMA divisors
    for h in hosts:
        for t in range(1, numa_div_max_t + 1):
            candidates.add(
                _quantize_and_clip(
                    h.numa_node_cpu / t,
                    h.numa_node_mem / t,
                    h.numa_node_net / t,
                )
            )

    # 2) Anchors from bundle group requirements
    scales = [1.0, 1.1, 1.25, 1.5, 2.0]
    for bg in bundle_groups:
        if bg.instance_type != instance_type:
            continue
        for s in scales:
            cpu_r = math.ceil(bg.cpu_req_int * cfg.CPU_STEP * s / _cpu_step) * _cpu_step
            mem_r = math.ceil(bg.mem_req_int * cfg.MEM_STEP * s / _mem_step) * _mem_step
            net_r = math.ceil(bg.net_req_int * cfg.NET_STEP * s / _net_step) * _net_step
            candidates.add(_quantize_and_clip(cpu_r, mem_r, net_r))

    # 3) Neighborhood around mandatory sizes.
    #    a) Proportional: all dims scaled together (along the diagonal).
    #    b) Per-dimension: each dim varied independently, others fixed.
    if mandatory_sizes:
        mandatory_scales = [0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2, 1.3, 1.4, 1.5, 1.6, 1.75, 2.0]
        for cpu_i, mem_i, net_i in mandatory_sizes:
            cpu_r = cpu_i * cfg.CPU_STEP
            mem_r = mem_i * cfg.MEM_STEP
            net_r = net_i * cfg.NET_STEP
            for s in mandatory_scales:
                # a) proportional
                candidates.add(_quantize_and_clip(cpu_r * s, mem_r * s, net_r * s))
                # b) per-dimension
                candidates.add(_quantize_and_clip(cpu_r * s, mem_r, net_r))
                candidates.add(_quantize_and_clip(cpu_r, mem_r * s, net_r))
                candidates.add(_quantize_and_clip(cpu_r, mem_r, net_r * s))

    # 4) Log-spaced grid — denser at small values, sparser at large values
    if grid_enabled:

        def _logspace_int_vals(lo_int, hi_int, n=10):
            if lo_int >= hi_int:
                return [lo_int]
            vals = lo_int + (hi_int - lo_int) * np.linspace(0, 1, n) ** 1.0
            return sorted(set(int(round(v)) for v in vals))

        min_cpu_int = disc_round(_min_cpu, cfg.CPU_STEP)
        min_mem_int = disc_round(_min_mem, cfg.MEM_STEP)
        min_net_int = disc_round(_min_net, cfg.NET_STEP)
        max_cpu_int = disc_round(max_cpu, cfg.CPU_STEP)
        max_mem_int = disc_round(max_mem, cfg.MEM_STEP)
        max_net_int = disc_round(max_net, cfg.NET_STEP)
        cpu_vals = _logspace_int_vals(min_cpu_int, max_cpu_int)
        mem_vals = _logspace_int_vals(min_mem_int, max_mem_int)
        net_vals = _logspace_int_vals(min_net_int, max_net_int)
        for cv in cpu_vals:
            for mv in mem_vals:
                for nv in net_vals:
                    candidates.add(_quantize_and_clip(cv * cfg.CPU_STEP, mv * cfg.MEM_STEP, nv * cfg.NET_STEP))

    candidates = list(candidates)

    # --- 5) Downsample to n_limit while preserving diversity ---
    # We intentionally DO NOT Pareto-prune by (cpu,mem,net), because "larger in all dims"
    # can be beneficial in the new model (scale-in: reducing number of instances).
    cand = list(candidates)

    # Scarcity cost (for stratification)
    a = resource_coefficients["a"]
    b = resource_coefficients["b"]
    c_coef = resource_coefficients["c"]

    # Score helpers: use real units for interpretability, but any monotonic score is OK
    def _cost(t):
        cv, mv, nv = t
        return a * cv + b * mv + c_coef * nv

    # Sorting keys
    by_cost = sorted(cand, key=_cost)
    by_cpu = sorted(cand, key=lambda t: (t[0], t[1], t[2]))
    by_mem = sorted(cand, key=lambda t: (t[1], t[0], t[2]))
    by_net = sorted(cand, key=lambda t: (t[2], t[0], t[1]))

    def _pick_quantiles(sorted_list, q_count):
        if not sorted_list:
            return []
        if q_count <= 0:
            return []
        n = len(sorted_list)
        # Power-spaced indices: denser at the start (smaller values)
        idxs = np.round(np.linspace(0, 1, q_count) ** 1.0 * (n - 1)).astype(int)
        return [sorted_list[i] for i in idxs]

    # We split the budget across multiple "views" of the space.
    # This keeps both small and large sizes and avoids collapsing to only cheap ones.
    if len(cand) > n_limit:
        # budget split (tunable):
        # 40% by cost, 20% by cpu, 20% by mem, 20% by net + extremes
        q_cost = int(round(n_limit * 0.40))
        q_cpu = int(round(n_limit * 0.20))
        q_mem = int(round(n_limit * 0.20))
        q_net = max(0, n_limit - (q_cost + q_cpu + q_mem))

        picked = set()

        # extremes: always keep min/max by each dimension and min/max by cost
        if by_cost:
            picked.add(by_cost[0])
            picked.add(by_cost[-1])
        if by_cpu:
            picked.add(by_cpu[0])
            picked.add(by_cpu[-1])
        if by_mem:
            picked.add(by_mem[0])
            picked.add(by_mem[-1])
        if by_net:
            picked.add(by_net[0])
            picked.add(by_net[-1])

        # quantile picks
        for t in _pick_quantiles(by_cost, q_cost):
            picked.add(t)
        for t in _pick_quantiles(by_cpu, q_cpu):
            picked.add(t)
        for t in _pick_quantiles(by_mem, q_mem):
            picked.add(t)
        for t in _pick_quantiles(by_net, q_net):
            picked.add(t)

        # If still too many (due to overlaps it's usually <=, but can be >),
        # trim by cost quantiles to exactly n_limit.
        picked_list = sorted(list(picked), key=_cost)
        if len(picked_list) > n_limit:
            picked_list = _pick_quantiles(picked_list, n_limit)

        cand = picked_list
    else:
        cand = by_cost  # deterministic ordering

    if not cand:
        # Fallback: single minimum-size entry
        cand = [_quantize_and_clip(_min_cpu, _min_mem, _min_net)]

    # Final deterministic order: sort by cost then (cpu,mem,net)
    cand.sort(key=lambda t: (_cost(t), t[0], t[1], t[2]))

    # 6) Always include mandatory_sizes (e.g. warm start) regardless of n_limit.
    #    Clip them to [min, max] ranges and track which entries are warm-start.
    mandatory_set = set()
    if mandatory_sizes:
        for cpu_i, mem_i, net_i in mandatory_sizes:
            entry = _quantize_and_clip(cpu_i * cfg.CPU_STEP, mem_i * cfg.MEM_STEP, net_i * cfg.NET_STEP)
            mandatory_set.add(entry)

    cand_set = set(cand)
    extra = [e for e in mandatory_set if e not in cand_set]
    cand = cand + extra
    # Re-sort so mandatory entries get their natural cost-based catalog index,
    # not the highest indices. This ensures warm start hints respect the
    # sel_node[0] < sel_node[1] < ... ordering constraint in stage1.
    cand.sort(key=lambda t: (_cost(t), t[0], t[1], t[2]))

    is_warm_start = [t in mandatory_set for t in cand]

    cat_cpu = [t[0] for t in cand]
    cat_mem = [t[1] for t in cand]
    cat_net = [t[2] for t in cand]
    return cat_cpu, cat_mem, cat_net, is_warm_start
