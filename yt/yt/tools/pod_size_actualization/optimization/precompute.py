"""
Precomputation of n_min lookup tables.

Split out from model.py so that it can be called from the hermetic binary
without importing ortools (which crashes due to protobuf version conflict).
"""

from .scripts import shared as cfg
from .scripts.shared import compute_n_min_from_req


def precompute_n_min_tables(
    bundle_groups: list,
    clusters: list,
    node_catalog_cpu: list,
    node_catalog_mem: list,
    node_catalog_net: list,
    proxy_catalog_cpu: list,
    proxy_catalog_mem: list,
    proxy_catalog_net: list,
) -> tuple:
    n_min_tables: dict = {}
    infeasible_catalog: dict = {}

    for i, bg in enumerate(bundle_groups):
        if bg.instance_type == 'node':
            cat_cpu, cat_mem, cat_net = node_catalog_cpu, node_catalog_mem, node_catalog_net
        else:
            cat_cpu, cat_mem, cat_net = proxy_catalog_cpu, proxy_catalog_mem, proxy_catalog_net
        N_cat = len(cat_cpu)
        infeasible_i: set = set()

        for c in clusters:
            blist = bg.bundles_by_cluster.get(c, [])
            if not blist:
                continue
            base_total = bg.counts_by_cluster.get(c, 0)
            table = []
            for idx in range(N_cat):
                n_val_total = 0
                all_ok = True
                for bi in blist:
                    cpu_r, mem_r, net_r = bg.bundle_key_max_req[bi.bundle]
                    bi_n_val, bi_ok = compute_n_min_from_req(
                        bg.instance_type,
                        bi.count,
                        cpu_r,
                        mem_r,
                        net_r,
                        cat_cpu[idx],
                        cat_mem[idx],
                        cat_net[idx],
                    )
                    n_val_total += bi_n_val
                    if not bi_ok:
                        all_ok = False
                if not all_ok:
                    infeasible_i.add(idx)
                    n_val_total = cfg.MAX_EXTRA_RATIO * base_total
                table.append(n_val_total)
            n_min_tables[i, c] = table

        infeasible_catalog[i] = infeasible_i

    return n_min_tables, infeasible_catalog
