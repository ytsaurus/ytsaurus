"""
Data loading and preparation: CSV loaders and bundle group builder.

Dataclasses and constants live in scripts/shared.py (single source of truth
shared with the solver subprocess).
"""

import dataclasses
import json
import math
from collections import defaultdict
from pathlib import Path

import pandas as pd

from .scripts import shared as cfg
from .scripts.shared import (
    BundleGroup,
    BundleInstances,
    ContainerType,
    Host,
    cluster_group,
    disc_round,
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_host_data(
    clusters: list,
    file_paths: list[str] | None = None,
    working_dir: str = ".",
) -> pd.DataFrame:
    """
    Load host data for all clusters.

    file_paths: explicit CSV paths (order-independent if files have a 'cluster' column).
                If None, paths are constructed from working_dir + cluster name.
    """
    all_data = []
    if file_paths is not None:
        for csv_file in file_paths:
            df = pd.read_csv(csv_file)
            if 'cluster' not in df.columns:
                raise ValueError(f"Host CSV {csv_file} is missing 'cluster' column")
            cluster = df['cluster'].iloc[0]
            print(f"Loaded {len(df)} hosts from {cluster}")
            all_data.append(df)
    else:
        for cluster in clusters:
            csv_file = Path(working_dir) / "cluster_data" / f"{cluster}_hosts_data_enriched_without_spare.csv"
            if not csv_file.exists():
                print(f"Warning: CSV file not found for cluster {cluster}: {csv_file}")
                continue
            df = pd.read_csv(csv_file)
            if 'cluster' not in df.columns:
                df['cluster'] = cluster
            all_data.append(df)
            print(f"Loaded {len(df)} hosts from {cluster}")
    if not all_data:
        raise ValueError("No data loaded from any cluster")
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal physical hosts loaded: {len(combined_df)}")
    return combined_df


def validate_host_columns(df: pd.DataFrame) -> bool:
    required = [
        'cpu_model',
        'network_total_bandwidth_mib',
        'cluster',
        'numa_cpu_details',
        'numa_memory_details',
        'memory_total_gib',
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required host columns: {missing}")
    return True


def prepare_numa_host_data(hosts_df: pd.DataFrame) -> list:
    processed = []
    for _, row in hosts_df.iterrows():
        try:
            cpu_details = json.loads(row['numa_cpu_details'])
            mem_details = json.loads(row['numa_memory_details'])
            if not cpu_details or not mem_details:
                continue
            numa_node_count = len(cpu_details)
            if numa_node_count == 0:
                continue
            min_cpu_per_node = min(c['total_vcores'] for c in cpu_details)
            min_mem_per_node = min(m['total_gib'] for m in mem_details)

            infra_cpu = max(min_cpu_per_node * cfg.INFRA_CPU_REL_TAX, cfg.INFRA_CPU_ABS_TAX)
            infra_mem = max(min_mem_per_node * cfg.INFRA_MEM_REL_TAX, cfg.INFRA_MEM_ABS_TAX)
            min_cpu_per_node = min_cpu_per_node - infra_cpu
            min_mem_per_node = min_mem_per_node - infra_mem

            if min_mem_per_node > 400:
                min_cpu_per_node /= 2
                min_mem_per_node /= 2
                numa_node_count *= 2

            net_per_node = row['network_total_bandwidth_mib'] / numa_node_count
            processed.append(
                {
                    'cluster': row['cluster'],
                    'cpu_model': row['cpu_model'],
                    'numa_node_cpu': math.floor(min_cpu_per_node) + 1e-9,
                    'numa_node_mem': math.floor(min_mem_per_node) + 1e-9,
                    'numa_node_net': math.floor(net_per_node) + 1e-9,
                    'numa_nodes_per_host': numa_node_count,
                }
            )
        except (json.JSONDecodeError, KeyError, TypeError):
            print(f"Warning: Could not process NUMA data for a host in {row['cluster']}")
            continue

    if not processed:
        raise ValueError("No valid NUMA host data could be processed.")

    from collections import Counter

    groups = Counter(
        (
            r['cluster'],
            r['cpu_model'],
            r['numa_nodes_per_host'],
            r['numa_node_cpu'],
            r['numa_node_mem'],
            r['numa_node_net'],
        )
        for r in processed
    )
    hosts = [
        Host(
            cluster=cluster,
            cpu_model=cpu_model,
            numa_node_cpu=cpu,
            numa_node_mem=mem,
            numa_node_net=net,
            numa_nodes_per_host=numa_count,
            available_physical_hosts=count,
        )
        for (cluster, cpu_model, numa_count, cpu, mem, net), count in groups.items()
        if count >= cfg.MIN_HOSTS_PER_MODEL
    ]

    print(f"\nDistinct host configurations: {len(hosts)}")
    for h in hosts:
        print(
            f"  {h.cluster} {h.cpu_model} numa={h.numa_nodes_per_host} "
            f"cpu={h.numa_node_cpu:.1f} mem={h.numa_node_mem:.1f} "
            f"net={h.numa_node_net:.1f} phys={h.available_physical_hosts}"
        )
    print("\nNUMA host data processed and aggregated successfully.")
    return hosts


# Диагностика, одинаковая во всех периодных выходах.
VALIDITY_COLUMNS = (
    'node_valid_periods',
    'proxy_valid_periods',
    'node_last_config_change',
    'proxy_last_config_change',
    'bundle_spec_loaded_at',
    'node_confidence',
    'proxy_confidence',
    'node_period_invalidation_reason',
    'proxy_period_invalidation_reason',
    'node_confidence_reason',
    'proxy_confidence_reason',
    'node_count',
    'rpc_count',
    'node_type',
    'rpc_type',
)

BUNDLE_ADMINISTRATIVE_COLUMNS = (
    'abc_service_slug',
    'abc_service_path',
    'value_stream_slug',
    'value_stream_name_ru',
    'business_unit_slug',
    'business_unit_name_ru',
    'business_group_slug',
    'business_group_name_ru',
)

# Coverage — единственное, что различается между периодными выходами: в каждом
# из них он относится к своему периоду.
PERIOD_COVERAGE_COLUMNS = (
    'node_spec_coverage',
    'proxy_spec_coverage',
    'node_usage_coverage',
    'proxy_usage_coverage',
)

_VALIDITY_INPUT_COLUMNS = (
    'cluster',
    'bundle',
    'method_name',
    'periods_total',
    *VALIDITY_COLUMNS,
    *PERIOD_COVERAGE_COLUMNS,
    *BUNDLE_ADMINISTRATIVE_COLUMNS,
)


def period_coverage_column(column: str, period: int) -> str:
    """Имя колонки coverage конкретного периода: node_usage_coverage -> node_usage_coverage_period_0."""
    return f"{column}_period_{period}"


def load_bundle_validity(bundle_file_paths: list) -> tuple:
    """Load the bundle diagnostics calculated by YQL.

    YQL repeats the final diagnostics in every period output, so one row per
    bundle is enough here; coverage самого периода, наоборот, разное, поэтому
    раскладывается по колонкам с номером периода. Missing columns are
    intentionally not defaulted: pandas will fail while reading an incompatible
    input schema.
    """
    frames = []
    for path in bundle_file_paths:
        df = pd.read_csv(path, usecols=_VALIDITY_INPUT_COLUMNS)
        if df.empty:
            print(f"  {path}: пустой файл, пропускаем")
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=['cluster', 'bundle', *VALIDITY_COLUMNS, *BUNDLE_ADMINISTRATIVE_COLUMNS]), 0

    data = pd.concat(frames, ignore_index=True)
    periods_total = int(data['periods_total'].max())
    data['_period'] = data['method_name'].str.extract(r'(\d+)$')[0].astype(int)

    validity = data.drop_duplicates(['cluster', 'bundle'], keep='first')[
        ['cluster', 'bundle', *VALIDITY_COLUMNS, *BUNDLE_ADMINISTRATIVE_COLUMNS]
    ]
    for period, group in data.groupby('_period', sort=True):
        renamed = group[['cluster', 'bundle', *PERIOD_COVERAGE_COLUMNS]].rename(
            columns={name: period_coverage_column(name, period) for name in PERIOD_COVERAGE_COLUMNS}
        )
        validity = validity.merge(renamed, how='left', on=['cluster', 'bundle'])
    return validity, periods_total


def _rows_per_cluster(df) -> str:
    by_cluster = df.groupby('cluster').size().to_dict()
    return ", ".join(f"{cluster} {count}" for cluster, count in sorted(by_cluster.items()))


def load_container_specs(
    clusters: list,
    node_spec_paths: list[str] | None = None,
    rpc_spec_paths: list[str] | None = None,
    working_dir: str = ".",
):
    """
    Load node and rpc container specs for all clusters.

    node_spec_paths/rpc_spec_paths: explicit CSV paths, кластеры различаются по
    столбцу 'cluster' внутри файла.
    If None, paths are constructed from working_dir + cluster name.
    """
    all_rpc_specs, all_node_specs = [], []

    if rpc_spec_paths is not None:
        for rpc_file in rpc_spec_paths:
            rpc_df = pd.read_csv(rpc_file)
            if 'cluster' not in rpc_df.columns:
                raise ValueError(f"RPC spec CSV {rpc_file} is missing 'cluster' column")
            all_rpc_specs.append(rpc_df)
            print(f"Loaded RPC specs from {rpc_file}: {_rows_per_cluster(rpc_df)}")
    else:
        for cluster in clusters:
            rpc_file = Path(working_dir) / f"container_specs/rpc_container_specs_{cluster}.csv"
            try:
                rpc_df = pd.read_csv(rpc_file)
                if 'cluster' not in rpc_df.columns:
                    rpc_df['cluster'] = cluster
                all_rpc_specs.append(rpc_df)
                print(f"Loaded RPC specs for {cluster}: {len(rpc_df)} records")
            except FileNotFoundError:
                print(f"Warning: RPC specs file not found for cluster {cluster}: {rpc_file}")

    if node_spec_paths is not None:
        for node_file in node_spec_paths:
            node_df = pd.read_csv(node_file)
            if 'cluster' not in node_df.columns:
                raise ValueError(f"Node spec CSV {node_file} is missing 'cluster' column")
            all_node_specs.append(node_df)
            print(f"Loaded Node specs from {node_file}: {_rows_per_cluster(node_df)}")
    else:
        for cluster in clusters:
            node_file = Path(working_dir) / f"container_specs/node_container_specs_{cluster}.csv"
            try:
                node_df = pd.read_csv(node_file)
                if 'cluster' not in node_df.columns:
                    node_df['cluster'] = cluster
                all_node_specs.append(node_df)
                print(f"Loaded Node specs for {cluster}: {len(node_df)} records")
            except FileNotFoundError:
                print(f"Warning: Node specs file not found for cluster {cluster}: {node_file}")

    node_specs_df = pd.concat(all_node_specs, ignore_index=True) if all_node_specs else None
    rpc_specs_df = pd.concat(all_rpc_specs, ignore_index=True) if all_rpc_specs else None
    return node_specs_df, rpc_specs_df


def _count_per_zone(count: int, zones: int, cluster: str, bundle: str, kind: str) -> int:
    """Инстансы одной зоны доступности cross-dc кластера."""
    if count % zones:
        raise ValueError(f"{cluster}/{bundle}: {kind} count {count} is not divisible by " f"{zones} availability zones")
    return count // zones


def _read_bundle_metrics(clusters, method, bundle_file_paths, working_dir) -> "pd.DataFrame | None":
    """Метрики одного метода по всем кластерам, различаемым столбцом 'cluster'.

    Явные пути — файл на метод. В working_dir сначала ищется такой же
    объединённый файл, потом старая раскладка по кластерам.
    """
    if bundle_file_paths is not None:
        file_path = bundle_file_paths.get(method)
        if file_path is None:
            print(f"Warning: no bundle metrics file for method '{method}'")
            return None
        return pd.read_csv(file_path)

    combined = Path(working_dir) / f"bundle_metrics/bundle_metrics_all_clusters_{method}.csv"
    if combined.exists():
        return pd.read_csv(combined)

    frames = []
    for cluster in clusters:
        file_path = Path(working_dir) / f"bundle_metrics/bundle_metrics_{cluster}_{method}.csv"
        if not file_path.exists():
            print(f"Warning: Bundle metrics file not found for cluster {cluster}: {file_path}")
            continue
        df = pd.read_csv(file_path)
        if 'cluster' not in df.columns:
            df['cluster'] = cluster
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else None


def _load_bundle_data_single_method(
    clusters, method, node_specs_df, rpc_specs_df, bundle_file_paths: dict | None = None, working_dir: str = "."
) -> list:
    data_all = _read_bundle_metrics(clusters, method, bundle_file_paths, working_dir)
    if data_all is None or data_all.empty:
        return []
    if 'cluster' not in data_all.columns:
        raise ValueError(f"Bundle metrics for method '{method}' is missing 'cluster' column")

    bundles = []
    for cluster in clusters:
        data = data_all[data_all['cluster'] == cluster]
        if data.empty:
            print(f"Warning: no bundle metrics for cluster {cluster}, method '{method}'")
            continue

        cluster_node_specs = {
            row['container_type']: row for _, row in node_specs_df[node_specs_df['cluster'] == cluster].iterrows()
        }
        cluster_rpc_specs = {
            row['container_type']: row for _, row in rpc_specs_df[rpc_specs_df['cluster'] == cluster].iterrows()
        }

        for _, row in data.iterrows():
            if str(row['bundle']) in cfg.BUNDLES_TO_SKIP:
                continue
            # Зоны считает кубик окон по карте хостов бандл-контроллера; в старых
            # выгрузках столбца нет — там кластер однозонный.
            zones = int(row.get('availability_zones') or 1)

            node_type = row.get('node_type')
            if (
                pd.notna(row.get('node_cpu_total_p75'))
                and pd.notna(row.get('node_anon_memory_p75'))
                and int(row.get('node_count', 0)) > 0
            ):
                spec = cluster_node_specs.get(node_type)
                if spec is None:
                    raise ValueError(f"No node container spec for type '{node_type}' in {cluster}")
                ct = ContainerType(
                    name=node_type,
                    cpu_limit=float(spec['cpu_cores']),
                    mem_limit=float(spec['memory_bytes']) / (1024**3),
                    net_limit=float(spec['net_bytes']) / (1024**2),
                )
                raw_cpu = float(row['node_cpu_total_p75'])
                raw_mem = float(row['node_anon_memory_p75'])
                raw_net = max(float(row.get('node_net_tx_p75', 0) or 0), float(row.get('node_net_rx_p75', 0) or 0))
                bundles.append(
                    BundleInstances(
                        name=str(row['bundle']),
                        instance_type='node',
                        container_type=ct,
                        count=_count_per_zone(
                            int(row['node_count']), zones, cluster, str(row['bundle']), 'tablet node'
                        ),
                        zones=zones,
                        cpu=min(raw_cpu, ct.cpu_limit),
                        memory=min(raw_mem, float(spec['memory_bytes'])) / (1024**3),
                        network=min(raw_net, float(spec['net_bytes'])) / (1024**2),
                        cluster=cluster,
                        node_type=node_type,
                    )
                )

            rpc_type = row.get('rpc_type')
            if (
                pd.notna(row.get('proxy_cpu_total_p75'))
                and pd.notna(row.get('proxy_anon_memory_p75'))
                and int(row.get('rpc_count', 0)) > 0
            ):
                spec = cluster_rpc_specs.get(rpc_type)
                if spec is None:
                    raise ValueError(f"No rpc container spec for type '{rpc_type}' in {cluster}")
                ct = ContainerType(
                    name=rpc_type,
                    cpu_limit=float(spec['cpu_cores']),
                    mem_limit=float(spec['memory_bytes']) / (1024**3),
                    net_limit=float(spec['net_bytes']) / (1024**2),
                )
                raw_cpu = float(row['proxy_cpu_total_p75'])
                raw_mem = float(row['proxy_anon_memory_p75'])
                raw_net = max(float(row.get('proxy_net_tx_p75', 0) or 0), float(row.get('proxy_net_rx_p75', 0) or 0))
                bundles.append(
                    BundleInstances(
                        name=str(row['bundle']),
                        instance_type='proxy',
                        container_type=ct,
                        count=_count_per_zone(int(row['rpc_count']), zones, cluster, str(row['bundle']), 'rpc proxy'),
                        zones=zones,
                        cpu=min(raw_cpu, ct.cpu_limit),
                        memory=min(raw_mem, float(spec['memory_bytes'])) / (1024**3),
                        network=min(raw_net, float(spec['net_bytes'])) / (1024**2),
                        cluster=cluster,
                        node_type=rpc_type,
                    )
                )
    return bundles


def load_bundle_data(
    clusters,
    methods,
    node_specs_df,
    rpc_specs_df,
    bundle_file_paths: dict | None = None,
    working_dir: str = ".",
    fail_on_method_mismatch: bool = False,
    allow_empty: bool = False,
) -> list:
    """
    Load bundle metrics for one or more methods, taking max across methods.

    bundle_file_paths: dict {method: path} for explicit files, one file per method
                       with all clusters inside (столбец 'cluster').
                       If None, paths are constructed from working_dir + names.

    Порядок methods — от старого к свежему: эталон конфигурации берётся из
    последнего метода, и при расхождении метрики собираются только с совпавшего
    с ним хвоста списка. fail_on_method_mismatch — падать вместо этого.
    """
    if isinstance(methods, str):
        methods = [methods]

    all_method_bundles = []
    for method in methods:
        mb = _load_bundle_data_single_method(
            clusters,
            method,
            node_specs_df,
            rpc_specs_df,
            bundle_file_paths=bundle_file_paths,
            working_dir=working_dir,
        )
        print(f"  Method '{method}': {len(mb)} bundle instances loaded.")
        all_method_bundles.append(mb)

    if len(methods) == 1:
        bundles = all_method_bundles[0]
        if not bundles and not allow_empty:
            raise ValueError("No bundle data found for any cluster.")
        print(f"\nLoaded a total of {len(bundles)} bundle instances.")
        return bundles

    groups: dict = defaultdict(list)
    for method_bundles in all_method_bundles:
        for bi in method_bundles:
            groups[(bi.name, bi.instance_type, bi.cluster)].append(bi)

    merged = []
    for (name, instance_type, cluster), bis in groups.items():
        node_types = {bi.node_type for bi in bis}
        counts = {bi.count for bi in bis}
        inconsistent = False
        if len(node_types) > 1:
            print(
                f"Warning: bundle '{name}' ({instance_type}, {cluster}) has inconsistent "
                f"node_type across methods: {[bi.node_type for bi in bis]}"
            )
            inconsistent = True
        if len(counts) > 1:
            print(
                f"Warning: bundle '{name}' ({instance_type}, {cluster}) has inconsistent "
                f"count across methods: {[bi.count for bi in bis]}"
            )
            inconsistent = True
        if inconsistent and fail_on_method_mismatch:
            raise ValueError(
                f"bundle '{name}' ({instance_type}, {cluster}) differs across methods: "
                f"node_type {[bi.node_type for bi in bis]}, count {[bi.count for bi in bis]}"
            )
        if inconsistent:
            base_node_type = bis[-1].node_type
            base_count = bis[-1].count
            consistent_bis = []
            for bi in reversed(bis):
                if bi.node_type == base_node_type and bi.count == base_count:
                    consistent_bis.append(bi)
                else:
                    break
        else:
            consistent_bis = bis
        merged.append(
            dataclasses.replace(
                bis[-1],
                cpu=max(bi.cpu for bi in consistent_bis),
                memory=max(bi.memory for bi in consistent_bis),
                network=max(bi.network for bi in consistent_bis),
            )
        )

    if not merged and not allow_empty:
        raise ValueError("No bundle data found for any cluster.")
    print(f"\nLoaded {len(merged)} bundle instances merged across {len(methods)} methods.")
    return merged


def get_inconsistent_bundles(bundles: list) -> set:
    """Keys (bundle_name, instance_type, cluster_group) that vary across the
    clusters of their own cluster group. Clusters of different groups are never
    compared with each other — they are optimized independently anyway."""
    tmp = pd.DataFrame(
        [
            {
                'bundle_name': bi.name,
                'instance_type': bi.instance_type,
                'cluster': bi.cluster,
                'cluster_group': cluster_group(bi.cluster),
                'node_type': bi.node_type,
                'count': bi.count,
            }
            for bi in bundles
        ]
    )

    key_cols = ['bundle_name', 'instance_type', 'cluster_group']

    def _rows_of(row):
        return tmp[
            (tmp['bundle_name'] == row['bundle_name'])
            & (tmp['instance_type'] == row['instance_type'])
            & (tmp['cluster_group'] == row['cluster_group'])
        ]

    type_agg = tmp.groupby(key_cols)['node_type'].nunique()
    varying_type = type_agg[type_agg > 1].reset_index()[key_cols]
    print(f"\nBundles with varying node_type across clusters: {len(varying_type)}")
    for _, row in varying_type.iterrows():
        types = _rows_of(row)
        per_cluster = dict(zip(types['cluster'], types['node_type']))
        print(f"  {row['bundle_name']} ({row['instance_type']}, {row['cluster_group']}): {per_cluster}")

    count_agg = tmp.groupby(key_cols)['count'].agg(['min', 'max'])
    varying_count = count_agg[count_agg['max'] - count_agg['min'] > 2].reset_index()
    print(f"Bundles with varying count across clusters: {len(varying_count)}")
    for _, row in varying_count.iterrows():
        counts = _rows_of(row)
        per_cluster = dict(zip(counts['cluster'], counts['count']))
        print(f"  {row['bundle_name']} ({row['instance_type']}, {row['cluster_group']}): {per_cluster}")

    inconsistent_type = set(map(tuple, varying_type[key_cols].values))
    inconsistent_count = set(map(tuple, varying_count[key_cols].values))
    inconsistent = inconsistent_type | inconsistent_count
    print(f"Inconsistent bundles: {len(inconsistent)}")
    return inconsistent


_ENV_SUFFIXES = (
    '-prestable',
    '-production',
    '-preprod',
    '-stable',
    '-prod',
    '_prestable',
    '_production',
    '_preprod',
    '_stable',
    '_prod',
    # '-testing', '-tst', '-test', '-dev',
    # '_testing', '_tst', '_test', '_dev', # Don't merge test with production
)


def _canonical_bundle_name(name: str) -> str:
    for suffix in _ENV_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def build_bundle_groups(bundles: list) -> list:
    """Build BundleGroups from a flat list of BundleInstances.

    Every grouping key below carries the cluster group (see cfg.CLUSTER_GROUPS),
    so bundles are only ever merged across clusters of the same group.
    """
    if not bundles:
        return []

    inconsistent_bundles = get_inconsistent_bundles(bundles)

    consistent_groups: dict = defaultdict(list)
    inconsistent_by_key: dict = defaultdict(list)
    for bi in bundles:
        cgroup = cluster_group(bi.cluster)
        key = (bi.name, bi.instance_type, cgroup)
        if key in inconsistent_bundles:
            inconsistent_by_key[(bi.name, bi.instance_type, cgroup, bi.node_type)].append(bi)
        else:
            consistent_groups[key].append(bi)

    result = []
    for (name, instance_type, cgroup), bis in consistent_groups.items():
        bundles_by_cluster: dict = defaultdict(list)
        for bi in bis:
            bundles_by_cluster[bi.cluster].append(bi)
        counts_by_cluster = {c: sum(bi.count for bi in blist) for c, blist in bundles_by_cluster.items()}
        result.append(
            BundleGroup(
                instance_type=instance_type,
                bundles_by_cluster=dict(bundles_by_cluster),
                counts_by_cluster=counts_by_cluster,
                cpu_req_int=max(bi.cpu_req_int() for bi in bis),
                mem_req_int=max(bi.mem_req_int() for bi in bis),
                net_req_int=max(bi.net_req_int() for bi in bis),
            )
        )

    # Step 1: for each inconsistent (name, instance_type, cluster_group, node_type),
    # merge count-buckets if max-min count <= 1.
    # Step 2: merge those sub-groups into one BundleGroup per
    # (name, instance_type, cluster_group, node_type) using BundleGroup.merge() so
    # that bundle_key_max_req is a union of the parts.
    n_inconsistent_groups = 0
    for (name, instance_type, cgroup, node_type), bis in sorted(inconsistent_by_key.items()):
        count_buckets: dict = defaultdict(list)
        for bi in bis:
            count_buckets[bi.count].append(bi)

        # Merge count-buckets if max-min count <= 1.
        grouped_bis: list = []
        counts = sorted(count_buckets.keys())
        if max(counts) - min(counts) <= 1:
            group = [bi for blist in count_buckets.values() for bi in blist]
            clusters_info = {bi.cluster: (bi.node_type, bi.count) for bi in group}
            print(
                f"  Inconsistent {name} ({instance_type}, {node_type}): group [counts={counts}] " f"→ {clusters_info}"
            )
            grouped_bis.append(group)
        else:
            for count, blist in count_buckets.items():
                clusters_info = {bi.cluster: (bi.node_type, bi.count) for bi in blist}
                print(
                    f"  Inconsistent {name} ({instance_type}, {node_type}): separate [count={count}] "
                    f"→ {clusters_info}"
                )
                grouped_bis.append(blist)

        node_type_groups = []
        for node_bis in grouped_bis:
            bundles_by_cluster: dict = defaultdict(list)
            for bi in node_bis:
                bundles_by_cluster[bi.cluster].append(bi)
            counts_by_cluster = {c: sum(bi.count for bi in blist) for c, blist in bundles_by_cluster.items()}
            node_type_groups.append(
                BundleGroup(
                    instance_type=instance_type,
                    bundles_by_cluster=dict(bundles_by_cluster),
                    counts_by_cluster=counts_by_cluster,
                    cpu_req_int=max(bi.cpu_req_int() for bi in node_bis),
                    mem_req_int=max(bi.mem_req_int() for bi in node_bis),
                    net_req_int=max(bi.net_req_int() for bi in node_bis),
                )
            )

        if len(node_type_groups) == 1:
            result.append(node_type_groups[0])
        else:
            parts = [f"[{', '.join(bg.bundles_by_cluster.keys())}]" for bg in node_type_groups]
            print(
                f"  Inconsistent {name} ({instance_type}, {node_type}): merge {len(node_type_groups)} subgroups → {parts}"
            )
            result.append(BundleGroup.merge(node_type_groups))
        n_inconsistent_groups += 1

    n_consistent = len(result) - n_inconsistent_groups
    print(
        f"\nBuilt {len(result)} bundle groups "
        f"({n_consistent} consistent, {n_inconsistent_groups} inconsistent → merged by node_type)."
    )

    # Step 3: group env-variant bundles (prestable/stable, preprod/prod, tst/prod, etc.)
    # by their canonical name (suffix stripped) within the same (instance_type, node_type).
    canonical_groups: dict = defaultdict(list)
    for bg in result:
        name = bg.all_bundles[0].name
        node_type = bg.all_bundles[0].node_type
        canonical = _canonical_bundle_name(name)
        canonical_groups[(canonical, bg.instance_type, bg.cluster_group, node_type)].append(bg)

    result = []
    n_env_merged_total = 0
    for (canonical, instance_type, cgroup, node_type), groups in sorted(canonical_groups.items()):
        if len(groups) == 1:
            result.append(groups[0])
        else:
            merged = BundleGroup.merge(groups)
            names = sorted({bi.name for bi in merged.all_bundles})
            print(f"  Env-merge {instance_type} ({node_type}, {cgroup}) '{canonical}': {names}")
            result.append(merged)
            n_env_merged_total += len(groups)

    if n_env_merged_total:
        n_env_groups = sum(1 for gs in canonical_groups.values() if len(gs) > 1)
        print(f"Env-merging: {n_env_merged_total} groups → {n_env_groups} merged groups.")
    print(f"After env-merging: {len(result)} bundle groups.")

    if not cfg.MERGE_BELOW_MIN:
        return result

    _min_cpu_by_type = {
        "node": disc_round(cfg.MIN_CONTAINER_CPU, cfg.CPU_STEP),
        "proxy": disc_round(cfg.MIN_PROXY_CPU, cfg.CPU_STEP),
    }
    _min_mem_by_type = {
        "node": disc_round(cfg.MIN_CONTAINER_MEM, cfg.MEM_STEP),
        "proxy": disc_round(cfg.MIN_PROXY_MEM, cfg.MEM_STEP),
    }
    _min_net_by_type = {
        "node": disc_round(cfg.MIN_CONTAINER_NET, cfg.NET_STEP),
        "proxy": disc_round(cfg.MIN_PROXY_NET_FOR_GROUPING, cfg.NET_STEP),
    }

    def _is_below_minimum(bg: BundleGroup) -> bool:
        return (
            bg.cpu_req_int < _min_cpu_by_type[bg.instance_type]
            and bg.mem_req_int < _min_mem_by_type[bg.instance_type]
            and bg.net_req_int < _min_net_by_type[bg.instance_type]
        )

    small_by_type: dict = defaultdict(list)
    final_result = []
    for bg in result:
        if _is_below_minimum(bg):
            small_by_type[(bg.cluster_group, bg.instance_type)].append(bg)
        else:
            final_result.append(bg)

    for (cgroup, instance_type), small_groups in small_by_type.items():
        if len(small_groups) == 1:
            final_result.append(small_groups[0])
        else:
            merged = BundleGroup.merge(small_groups)
            print(f"  Merged {len(small_groups)} below-minimum {instance_type} groups " f"in {cgroup}: {merged.label}")
            final_result.append(merged)

    print(f"Built {len(final_result)} bundle groups after below-minimum merging.")
    return final_result
