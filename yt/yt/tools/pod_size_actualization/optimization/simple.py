"""
Simplified assignment optimization without host capacity constraints.

Without NUMA hosts, bundle groups are fully independent: for each group,
pick the size from the given candidates that minimizes total container cost
(n_min * resource_value). No CP-SAT needed — plain greedy per group.
"""

import pandas as pd

from .scripts import shared as cfg
from .scripts.shared import disc_round, compute_n_min_from_req


def optimize_assignment(
    bundle_groups: list,
    node_sizes: list,
    proxy_sizes: list,
    resource_coefficients: dict,
    config_names: list | None = None,
) -> tuple:
    """
    Assign each bundle group to the cheapest feasible container size.

    Parameters
    ----------
    bundle_groups : list[BundleGroup]
        Built by build_bundle_groups(); groups share a single size variable.
    node_sizes : list of (cpu_cores, mem_gib, net_mibs)
        K_node candidate sizes for node bundle groups.
    proxy_sizes : list of (cpu_cores, mem_gib, net_mibs)
        K_proxy candidate sizes for proxy bundle groups.
    resource_coefficients : dict
        {"a": float, "b": float, "c": float} — scarcity prices per resource unit.

    Returns
    -------
    (res_df, sizes_df, assign_df)
        res_df     : single-row summary with total container_cost.
        sizes_df   : one row per container type (ContainerTypeID 0..K-1).
        assign_df  : one row per (bundle, cluster) with NewCount and BaseCount;
                     ключ для джойна с метриками — Cluster/BundleName/InstanceType.
                     Base*/New* — размеры инстанса до и после: CPU в ядрах,
                     Memory в GiB, Network в MiB/s, как в sizes_df.
                     Usage* — потребление на инстанс, поклипанное по текущему
                     контейнеру, без margin. GroupID — группа, которой солвер
                     подбирал размер, SubGroupID — её часть, собранная
                     конструктором BundleGroup: слияния частей не объединяют.

    config_names : имена конфигураций бандл-контроллера в порядке
        node_sizes + proxy_sizes; попадают в NewContainerType.
    """
    a = resource_coefficients["a"]
    b = resource_coefficients["b"]
    c_coef = resource_coefficients["c"]

    K_node = len(node_sizes)

    def _to_int(cpu, mem, net):
        return (
            disc_round(cpu, cfg.CPU_STEP),
            disc_round(mem, cfg.MEM_STEP),
            disc_round(net, cfg.NET_STEP),
        )

    node_sizes_int = [_to_int(*s) for s in node_sizes]
    proxy_sizes_int = [_to_int(*s) for s in proxy_sizes]
    all_sizes_int = node_sizes_int + proxy_sizes_int
    all_config_names = list(config_names) if config_names else [None] * len(all_sizes_int)
    if len(all_config_names) != len(all_sizes_int):
        raise ValueError(f"config_names has {len(all_config_names)} entries " f"for {len(all_sizes_int)} sizes")

    def _size_cost(cpu_i, mem_i, net_i):
        return a * cpu_i + b * mem_i + c_coef * net_i

    assign_rows = []
    total_container_cost = 0.0
    part_ids = {}
    for bg in bundle_groups:
        for part in bg.parts:
            part_id = len(part_ids)
            for bundle_key in part:
                part_ids[bundle_key] = part_id

    for group_id, bg in enumerate(bundle_groups):
        if bg.instance_type == 'node':
            candidates = node_sizes_int
            k_offset = 0
        else:
            candidates = proxy_sizes_int
            k_offset = K_node

        best_k = None
        best_cost = float('inf')

        for local_k, (cpu_s, mem_s, net_s) in enumerate(candidates):
            sc = _size_cost(cpu_s, mem_s, net_s)
            total = 0.0
            feasible = True
            for blist in bg.bundles_by_cluster.values():
                for bi in blist:
                    cpu_r, mem_r, net_r = bg.bundle_key_max_req[bi.bundle]
                    n_min, ok = compute_n_min_from_req(
                        bg.instance_type,
                        bi.count,
                        cpu_r,
                        mem_r,
                        net_r,
                        cpu_s,
                        mem_s,
                        net_s,
                    )
                    if not ok:
                        feasible = False
                        break
                    total += n_min * sc
                if not feasible:
                    break

            if feasible and total < best_cost:
                best_cost = total
                best_k = k_offset + local_k

        if best_k is None:
            # All candidates infeasible — fall back to first and accept MAX_EXTRA_RATIO count
            best_k = k_offset

        cpu_s, mem_s, net_s = all_sizes_int[best_k]
        sc = _size_cost(cpu_s, mem_s, net_s)
        for blist in bg.bundles_by_cluster.values():
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
                # Считаем по одной зоне доступности, наружу отдаём все зоны сразу.
                new_total = new_cnt * bi.zones
                total_container_cost += new_total * sc
                assign_rows.append(
                    {
                        "GroupID": group_id,
                        "SubGroupID": part_ids[bi.bundle],
                        "Cluster": bi.cluster,
                        "BundleName": bi.name,
                        "Bundle": bi.bundle,
                        "InstanceType": bg.instance_type,
                        "AssignedContainerTypeID": best_k,
                        "NewCount": new_total,
                        "BaseCount": bi.total_count,
                        "UsageCPU": bi.cpu,
                        "UsageMemory": bi.memory,
                        "UsageNetwork": bi.network,
                        "BaseContainerType": bi.container_type.name,
                        "BaseCPU": bi.container_type.cpu_limit,
                        "BaseMemory": bi.container_type.mem_limit,
                        "BaseNetwork": bi.container_type.net_limit,
                        "NewContainerType": all_config_names[best_k],
                        "NewCPU": cpu_s * cfg.CPU_STEP,
                        "NewMemory": mem_s * cfg.MEM_STEP,
                        "NewNetwork": net_s * cfg.NET_STEP,
                    }
                )

    assign_df = pd.DataFrame(assign_rows)

    sizes_df = pd.DataFrame(
        [
            {
                "ContainerTypeID": k,
                "InstanceType": "node" if k < K_node else "proxy",
                "ConfigName": all_config_names[k],
                "CPU": cpu_i * cfg.CPU_STEP,
                "Memory": mem_i * cfg.MEM_STEP,
                "Network": net_i * cfg.NET_STEP,
            }
            for k, (cpu_i, mem_i, net_i) in enumerate(all_sizes_int)
        ]
    )

    res_df = pd.DataFrame([{"container_cost": total_container_cost}])
    return res_df, sizes_df, assign_df
