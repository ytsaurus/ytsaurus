"""
Defragmentation pipeline entry point.

run_defragmentation() loads cluster data, computes the target pod placement
(either random baseline or greedy + ILP), and returns a statistics text plus
the extra/required pods ratio.
"""

from io import StringIO
from copy import deepcopy
from typing import Optional

from .scripts.shared import ClusterConfig, Cluster
from .cluster import (
    collect_pods_to_remove,
    extract_pod_ids_from_structure,
    remove_tabnodes_and_rpcproxy,
    update_custom_pods,
    raise_network_limits,
    raise_cpu_limits,
    find_greedy_warm_start,
    find_random_placement_max_extra,
    apply_ilp_placement,
    calculate_cluster_resource_utilization,
    calculate_sink_pods_utilization,
    count_current_pod_configurations,
)
from .ilp import call_ilp_solver
from .validation import validate_host_filtering
from .validation import validate_bundles_with_yt  # , get_bundle_hotfix_bundles
from .placement import collect_synthetic_pods_placement, map_pods_with_rack_awareness

from yt.wrapper import YtClient


def _pods_to_remove_to_dict(structure) -> dict:
    def _pod_to_dict(pod):
        return {
            'pod_id': pod.pod_id,
            'yt_pod_name': pod.yt_pod_name,
            'bundle_controller_annotations': pod.bundle_controller_annotations,
            'user_tags': pod.user_tags,
            'decommissioned': pod.decommissioned,
            'proxy_role': pod.proxy_role,
            'config_name': pod.config_name,
            'human_config_name': getattr(pod, 'human_config_name', ''),
            'target_location': None,
        }

    return {
        'tabnodes_by_bundle': {
            bundle: [_pod_to_dict(p) for p in pods] for bundle, pods in structure.tabnodes_by_bundle.items()
        },
        'rpcproxies_by_bundle': {
            bundle: [_pod_to_dict(p) for p in pods] for bundle, pods in structure.rpcproxies_by_bundle.items()
        },
        'skipped_counts': structure.skipped_counts,
        'skipped_pods': [
            {
                'pod_id': p.pod_id,
                'yt_pod_name': p.yt_pod_name,
                'yt_role': p.yt_role,
                'skip_reason': p.skip_reason,
                'bundle_name': p.bundle_name,
            }
            for p in structure.skipped_pods
        ],
    }


def _enriched_pods_to_dict(tabnodes: list, rpcproxies: list) -> dict:
    def _pod_to_dict(pod):
        d = {
            'pod_id': pod.pod_id,
            'yt_pod_name': pod.yt_pod_name,
            'bundle_controller_annotations': pod.bundle_controller_annotations,
            'user_tags': pod.user_tags,
            'decommissioned': pod.decommissioned,
            'proxy_role': pod.proxy_role,
            'config_name': pod.config_name,
            'human_config_name': pod.human_config_name,
            'target_location': None,
        }
        loc = getattr(pod, 'target_location', None)
        if loc is not None:
            d['target_location'] = {
                'hostname': loc.hostname,
                'numa_node_id': loc.numa_node_id,
                'rack': loc.rack,
                'yt_pod_name': loc.yt_pod_name,
            }
        return d

    tabnodes_by_bundle: dict = {}
    for pod in tabnodes:
        bundle = pod.bundle_controller_annotations.get('allocated_for_bundle', 'unknown')
        tabnodes_by_bundle.setdefault(bundle, []).append(_pod_to_dict(pod))

    rpcproxies_by_bundle: dict = {}
    for pod in rpcproxies:
        bundle = pod.bundle_controller_annotations.get('allocated_for_bundle', 'unknown')
        rpcproxies_by_bundle.setdefault(bundle, []).append(_pod_to_dict(pod))

    return {
        'tabnodes_by_bundle': tabnodes_by_bundle,
        'rpcproxies_by_bundle': rpcproxies_by_bundle,
    }


def _print_utilization_by_role(_print, utilization: dict, total_cpu: float, total_mem: float, total_net: float):
    _print("  By role (ytexenode counted by minimal sink pod size):")
    for role, usage in utilization['used_by_role'].items():
        cpu, mem, net = usage['used_cpu'], usage['used_memory'], usage['used_network']
        cpu_pct = cpu / total_cpu * 100 if total_cpu > 0 else 0
        mem_pct = mem / total_mem * 100 if total_mem > 0 else 0
        net_pct = net / total_net * 100 if total_net > 0 else 0
        _print(f"    {role} ({usage['count']:,} pods):")
        _print(f"      CPU:     {cpu/1000:8.0f} / {total_cpu/1000:.0f} k millicores  ({cpu_pct:5.1f}%)")
        _print(f"      Memory:  {mem/1024**3:8.0f} / {total_mem/1024**3:.0f} GiB              ({mem_pct:5.1f}%)")
        _print(f"      Network: {net/1024**2:8.0f} / {total_net/1024**2:.0f} MiB/s            ({net_pct:5.1f}%)")


def run_defragmentation(
    hosts_csv: str,
    pods_csv: str,
    config: ClusterConfig,
    *,
    # Optimization result (JSON-decoded dict with "sizes" and "assignments" keys)
    optimization_result: Optional[dict] = None,
    # cluster_name is used to construct assignment lookup keys
    cluster_name: str = '',
    # If True: random placement baseline; skip ILP
    no_defragmentation: bool = False,
    random_seed: int = 42,
    # How many of the largest racks (by memory) to reserve for failure recovery
    reserved_rack_count: int = 2,
    # ILP time limit
    ilp_time_limit_sec: int = 3600,
    verbose: bool = False,
    # Extra bundles excluded from defragmentation (bundle_hotfix mode).
    # Combined with bundles returned by get_bundle_hotfix_bundles (currently disabled).
    additional_bundle_hotfix_bundles: list = None,
    # If non-empty, only these bundles are reallocated; pods from other bundles are dropped
    # from pods_to_remove_structure before placement.
    bundles_to_reallocate: list = None,
) -> tuple:
    """Run the defragmentation pipeline.

    Returns:
        (stats_text: str, metrics: dict, pods_to_remove: dict, pods_to_create: dict | None)
        where metrics contains: extra_ratio, extra_cpu_cores, extra_cpu_pct,
            extra_memory_gib, extra_memory_pct,
            extra_network_mib, extra_network_pct, ilp_gap (% or None)
        pods_to_create is None when no_defragmentation=True or ILP failed.
    """
    import sys

    # When stdout goes to a file (not a TTY), Python uses 8KB block buffering.
    # Switch to line-buffering so every print() is immediately visible in logs.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass  # reconfigure not available on older Python or non-TextIOWrapper stdout

    out = StringIO()

    def _print(*args, **kwargs):
        kwargs.setdefault('file', out)
        print(*args, **kwargs)
        if verbose:
            import sys

            print(*args, **{**kwargs, 'file': sys.stdout})

    yt_client = YtClient(proxy=config.yt_proxy)

    # Collect bundles to exclude from defragmentation.
    # get_bundle_hotfix_bundles requires bundle_hotfix binary — disabled for now.
    # hotfix_bundles = get_bundle_hotfix_bundles(cluster_name)
    hotfix_bundles = []
    if additional_bundle_hotfix_bundles:
        hotfix_bundles = hotfix_bundles + list(additional_bundle_hotfix_bundles)
    bundle_hotfix_bundles = hotfix_bundles

    # -----------------------------------------------------------------------
    # Load cluster
    # -----------------------------------------------------------------------
    _print("=== CLUSTER DEFRAGMENTATION ANALYSIS ===\n")

    import dataclasses

    _print("Config:")
    for f in dataclasses.fields(config):
        value = getattr(config, f.name)
        if f.name == 'pod_configurations':
            _print(f"  pod_configurations ({len(value)} configs):")
            for cfg_name, cfg in value.items():
                _print(f"    {cfg_name}: {cfg}")
        else:
            _print(f"  {f.name}: {value}")
    _print()

    cluster = Cluster(config)
    cluster.load_from_csv(hosts_csv, pods_csv)

    # -----------------------------------------------------------------------
    # Update bigb to memory_150
    # -----------------------------------------------------------------------
    if config.update_custom_pods:
        update_custom_pods(cluster)

    # -----------------------------------------------------------------------
    # Step 0: Current utilization
    # -----------------------------------------------------------------------
    _print("0. Current cluster resource utilization:")
    baseline = calculate_cluster_resource_utilization(cluster)
    _print(
        f"  CPU: {baseline['used_cpu']/1000:.0f}k / {baseline['total_cpu']/1000:.0f}k millicores "
        f"({baseline['cpu_utilization']:.1f}%)"
    )
    _print(
        f"  Memory: {baseline['used_memory']/1024**3:.0f} / {baseline['total_memory']/1024**3:.0f} GiB "
        f"({baseline['memory_utilization']:.1f}%)"
    )
    _print(
        f"  Network: {baseline['used_network']/1024**2:.0f} / {baseline['total_network']/1024**2:.0f} MiB/s "
        f"({baseline['network_utilization']:.1f}%)"
    )
    sink = calculate_sink_pods_utilization(cluster)
    _print(f"  Sink pods ({sink['sink_pods_count']}):")
    _print(f"    CPU: {sink['used_cpu']/1000:.0f}k millicores ({sink['cpu_utilization']:.1f}%)")
    _print(f"    Memory: {sink['used_memory']/1024**3:.0f} GiB ({sink['memory_utilization']:.1f}%)")
    _print(f"    Network: {sink['used_network']/1024**2:.0f} MiB/s ({sink['network_utilization']:.1f}%)")
    _print_utilization_by_role(
        _print, baseline, baseline['total_cpu'], baseline['total_memory'], baseline['total_network']
    )

    # -----------------------------------------------------------------------
    # Step 1: Current pod counts
    # -----------------------------------------------------------------------
    _print("\n1. Current pod configurations:")
    current_counts = count_current_pod_configurations(cluster)
    for config_name, count in current_counts.items():
        cfg = config.pod_configurations[config_name]
        _print(
            f"  {config_name}: {count} pods (CPU: {cfg['vcpu']/1000}v, "
            f"RAM: {cfg['memory']/1024**3:.0f}GiB, NET: {cfg['network']/1024**2:.0f}MiB/s)"
        )
    _print(f"Total managed pods: {sum(current_counts.values())}")

    # -----------------------------------------------------------------------
    # Step 2: Collect pods to remove
    # -----------------------------------------------------------------------
    _print("\n2. Collecting pods to remove...")
    pods_to_remove_structure = collect_pods_to_remove(cluster, bundle_hotfix_bundles)

    if bundles_to_reallocate:
        _bundles_set = set(bundles_to_reallocate)
        _dropped_tab = {
            b: pods for b, pods in pods_to_remove_structure.tabnodes_by_bundle.items() if b not in _bundles_set
        }
        _dropped_rpc = {
            b: pods for b, pods in pods_to_remove_structure.rpcproxies_by_bundle.items() if b not in _bundles_set
        }
        pods_to_remove_structure.tabnodes_by_bundle = {
            b: pods for b, pods in pods_to_remove_structure.tabnodes_by_bundle.items() if b in _bundles_set
        }
        pods_to_remove_structure.rpcproxies_by_bundle = {
            b: pods for b, pods in pods_to_remove_structure.rpcproxies_by_bundle.items() if b in _bundles_set
        }
        _print(f"  Filtered to bundles_to_reallocate={sorted(_bundles_set)}")
        if _dropped_tab:
            _print(
                f"  Dropped tabnodes bundles: "
                f"{', '.join(f'{b}({len(p)})' for b, p in sorted(_dropped_tab.items()))}"
            )
        if _dropped_rpc:
            _print(
                f"  Dropped rpcproxy bundles: "
                f"{', '.join(f'{b}({len(p)})' for b, p in sorted(_dropped_rpc.items()))}"
            )

        for pod in pods_to_remove_structure.skipped_pods:
            if pod.bundle_name in _bundles_set:
                _print(f"  WARNING: Skipped {pod}")

    pods_to_remove_dict = _pods_to_remove_to_dict(pods_to_remove_structure)
    pod_ids_to_remove = extract_pod_ids_from_structure(pods_to_remove_structure)

    _print(
        f"  Tabnodes to remove: {sum(len(v) for v in pods_to_remove_structure.tabnodes_by_bundle.values())}"
        f" across {len(pods_to_remove_structure.tabnodes_by_bundle)} bundles"
    )
    _print(
        f"  RPC proxies to remove: {sum(len(v) for v in pods_to_remove_structure.rpcproxies_by_bundle.values())}"
        f" across {len(pods_to_remove_structure.rpcproxies_by_bundle)} bundles"
    )
    _print(f"  Skipped: {pods_to_remove_structure.skipped_counts}")
    _print(f"  Total pod IDs to remove: {len(pod_ids_to_remove)}")

    # -----------------------------------------------------------------------
    # Step 2b: Validate bundles with YT (requires live YT client — disabled)
    # -----------------------------------------------------------------------
    if config.validate_bundles:
        validate_bundles_with_yt(pods_to_remove_structure, yt_client)
    else:
        _print(f"\n=== BUNDLE VALIDATION ===\n\nSkipped for {cluster_name}")

    # -----------------------------------------------------------------------
    # Step 2c: Validate host filtering
    # -----------------------------------------------------------------------
    _print("\n2c. Host filtering check:")
    validate_host_filtering(cluster, config, verbose=verbose)

    # -----------------------------------------------------------------------
    # Step 3: Build test cluster — remove pods, reserve racks, raise net limits
    # -----------------------------------------------------------------------
    _print("\n3. Building test cluster...")
    test_cluster = deepcopy(cluster)
    removed_counts = None

    if not no_defragmentation:
        removed_counts = remove_tabnodes_and_rpcproxy(test_cluster, pod_ids_to_remove)
        _print("  Removed pods (old configurations):")
        for config_name, count in removed_counts.items():
            if count > 0:
                _print(f"    {config_name}: {count}")

    # Collect bundle removal info for CHANGE_SIZES path
    pod_ids_set = set(pod_ids_to_remove)
    removed_bundles_nodes = {}
    removed_bundles_proxies = {}
    for bundle_name, pods in pods_to_remove_structure.tabnodes_by_bundle.items():
        count = sum(1 for p in pods if p.pod_id in pod_ids_set)
        if count > 0:
            removed_bundles_nodes[bundle_name] = count
    for bundle_name, pods in pods_to_remove_structure.rpcproxies_by_bundle.items():
        count = sum(1 for p in pods if p.pod_id in pod_ids_set)
        if count > 0:
            removed_bundles_proxies[bundle_name] = count

    _print("Removed bundles nodes: ", removed_bundles_nodes)
    _print("Removed bundles proxies: ", removed_bundles_proxies)

    # Rack analysis + reservation
    rack_stats = {}
    rack_data_nodes = {}
    for hostname, host in test_cluster.hosts.items():
        if not (host.is_alive() and not host.is_master_host() and host.has_complete_resources()):
            continue
        rack = host.rack
        if not rack:
            continue
        if rack not in rack_stats:
            rack_stats[rack] = {'nodes': 0, 'memory_gib': 0.0, 'cpu_vcores': 0.0, 'net_mib': 0.0}
            rack_data_nodes[rack] = []
        rack_stats[rack]['nodes'] += 1
        rack_stats[rack]['memory_gib'] += host.memory_total_gib
        rack_stats[rack]['cpu_vcores'] += host.cpu_total_vcores
        rack_stats[rack]['net_mib'] += host.network_total_bandwidth_mib

        rack_data_nodes[rack].extend(host.get_pods_by_role('ytdatnode'))

    _print(f"\n  Rack distribution ({len(rack_stats)} racks, sorted by memory):")
    _print(f"  {'Rack':<30} {'Nodes':>6} {'Memory GiB':>12} {'CPU vcores':>12} {'Net MiB/s':>12}")
    _print(f"  {'-' * 74}")

    for rack in sorted(rack_stats.keys(), key=lambda r: -rack_stats[r]['memory_gib']):
        s = rack_stats[rack]
        _print(
            f"  {rack:<30} {s['nodes']:>6,} {s['memory_gib']:>12,.0f} "
            f"{s['cpu_vcores']:>12,.0f} {s['net_mib']:>12,.0f}",
            end=" ",
        )

        # def get_disk_types(pod):
        #     disk_types = pod.disk_types
        #     hdd_count += sum(1 for disk_type in disk_types if disk_type == 'HDD')
        #     ssd_count += sum(1 for disk_type in disk_types if disk_type == 'SSD')
        #     nvme_count += sum(1 for disk_type in disk_types if disk_type == 'NVME')

        #     return f"  Pod {pod.pod_id} (HDD: {hdd_count}, SSD: {ssd_count}, NVME: {nvme_count})"

        hdd_count = 0
        ssd_count = 0
        nvme_count = 0
        # if rack in ["SAS-13.1/13.01.21", "SAS-12/12.4.08", "SAS-18.2/18.16.15"]:
        #     _print(f"  Data nodes:")
        for pod in rack_data_nodes[rack]:
            # _print(get_disk_types(pod))
            disk_types = pod.disk_types
            hdd_count += sum(1 for disk_type in disk_types if disk_type == 'HDD')
            ssd_count += sum(1 for disk_type in disk_types if disk_type == 'SSD')
            nvme_count += sum(1 for disk_type in disk_types if disk_type == 'NVME')

        _print(f"(HDD: {hdd_count}, SSD: {ssd_count}, NVME: {nvme_count})")

    reserved_racks = sorted(rack_stats.keys(), key=lambda r: -rack_stats[r]['memory_gib'])[:reserved_rack_count]
    reserved_rack_stats = {
        'nodes': sum(rack_stats[r]['nodes'] for r in reserved_racks),
        'memory_gib': sum(rack_stats[r]['memory_gib'] for r in reserved_racks),
        'cpu_vcores': sum(rack_stats[r]['cpu_vcores'] for r in reserved_racks),
        'net_mib': sum(rack_stats[r]['net_mib'] for r in reserved_racks),
    }
    reserved_hostnames = [
        h
        for h, host in test_cluster.hosts.items()
        if host.rack in reserved_racks
        and host.is_alive()
        and not host.is_master_host()
        and host.has_complete_resources()
    ]

    # In NO_DEFRAGMENTATION mode, still remove pods from reserved racks
    pod_ids_to_remove_from_reserved_racks = []
    if no_defragmentation:
        for hostname in reserved_hostnames:
            host = test_cluster.hosts[hostname]
            for pod in host.pods:
                if pod.pod_id in pod_ids_set:
                    pod_ids_to_remove_from_reserved_racks.append(pod.pod_id)

    if pod_ids_to_remove_from_reserved_racks:
        assert removed_counts is None
        removed_counts = remove_tabnodes_and_rpcproxy(test_cluster, pod_ids_to_remove_from_reserved_racks)
        _print("  Removed from reserved racks:")
        for config_name, count in removed_counts.items():
            if count > 0:
                _print(f"    {config_name}: {count}")

    for hostname in reserved_hostnames:
        del test_cluster.hosts[hostname]

    _print(f"\n  Reserved for rack failure ({reserved_rack_count} largest racks): {', '.join(reserved_racks)}")
    _print(f"    Hosts removed: {len(reserved_hostnames)}")
    _print(f"    CPU:     {reserved_rack_stats['cpu_vcores']:,.0f} vcores")
    _print(f"    Memory:  {reserved_rack_stats['memory_gib']:,.0f} GiB")
    _print(f"    Network: {reserved_rack_stats['net_mib']:,.0f} MiB/s")

    # Raise network limits if configured
    if config.raise_network_limits:
        network_removed = raise_network_limits(test_cluster, seed=random_seed)
        if any(v > 0 for v in network_removed.values()):
            _print("\n  Removed to fit raised network limits:")
            for config_name, count in network_removed.items():
                if count > 0:
                    _print(f"    {config_name}: {count}")
            if removed_counts is None:
                removed_counts = network_removed
            else:
                for config_name, count in network_removed.items():
                    removed_counts[config_name] += count

    if config.raise_cpu_limits:
        cpu_removed = raise_cpu_limits(test_cluster, seed=random_seed)
        if any(v > 0 for v in cpu_removed.values()):
            _print("\n  Removed to fit raised CPU limits:")
            for config_name, count in cpu_removed.items():
                if count > 0:
                    _print(f"    {config_name}: {count}")
            if removed_counts is None:
                removed_counts = cpu_removed
            else:
                for config_name, count in cpu_removed.items():
                    removed_counts[config_name] += count

    # Evict overcommitted pods
    overcommit_removed = {name: 0 for name in config.pod_configurations}
    for host in test_cluster.hosts.values():
        host_removed = host.evict_overcommitted_pods(seed=random_seed)
        for config_name, count in host_removed.items():
            overcommit_removed[config_name] += count
    if any(v > 0 for v in overcommit_removed.values()):
        _print("\n  Removed to fix overcommit:")
        for config_name, count in overcommit_removed.items():
            if count > 0:
                _print(f"    {config_name}: {count}")
        if removed_counts is None:
            removed_counts = overcommit_removed
        else:
            for config_name, count in overcommit_removed.items():
                removed_counts[config_name] += count
    else:
        _print("\n  No overcommit detected.")

    # -----------------------------------------------------------------------
    # Step 3b/3c/3d: CHANGE_SIZES — build new POD_CONFIGURATIONS
    # -----------------------------------------------------------------------
    change_sizes = optimization_result is not None and bool(optimization_result.get('sizes'))
    new_pod_counts = None
    bundle_assignment_found = {}
    container_id_to_config_name = {}

    if change_sizes:
        _print("\n3b. Loading new pod sizes from optimization results...")
        import pandas as pd

        sizes_df = pd.DataFrame(optimization_result['sizes'])
        container_sizes = {}
        container_id_to_human_config_name = {}
        for _, row in sizes_df.iterrows():
            cid = int(row['ContainerTypeID'])
            container_sizes[cid] = {
                'cpu': float(row['CPU']),
                'memory': float(row['Memory']),
                'network': float(row['Network']),
                'instance_type': str(row['InstanceType']),
            }
            container_id_to_human_config_name[cid] = str(row['ConfigName'])

        assignments_df = pd.DataFrame(optimization_result['assignments'])
        bundle_to_container = {}
        for _, row in assignments_df.iterrows():
            bundle_to_container[str(row['Bundle'])] = int(row['AssignedContainerTypeID'])

        bundle_to_new_count = {}
        if 'NewCount' in assignments_df.columns:
            for _, row in assignments_df.iterrows():
                bundle_to_new_count[str(row['Bundle'])] = int(row['NewCount'])

        bundle_to_base_count = {}
        if 'BaseCount' in assignments_df.columns:
            for _, row in assignments_df.iterrows():
                bundle_to_base_count[str(row['Bundle'])] = int(row['BaseCount'])

        _print("Bundle to base count: ", bundle_to_base_count)

        # Determine needed container IDs
        needed_container_ids = set()
        for bundle_name in removed_bundles_nodes:
            key = f"{bundle_name}@node@{cluster_name}"
            if key in bundle_to_container:
                cid = bundle_to_container[key]
                needed_container_ids.add(cid)
                bundle_assignment_found[(bundle_name, 'node')] = cid
            else:
                _print(f"  WARNING: No assignment for node bundle '{key}'")
                continue

            if removed_bundles_nodes[bundle_name] != bundle_to_base_count[key]:
                _print(
                    f" WARNING: removed nodes count != base count from assignments for bundle '{key}'. RemovedCount: {removed_bundles_nodes[bundle_name]}, BaseCount: {bundle_to_base_count[key]}"
                )

        for bundle_name in removed_bundles_proxies:
            key = f"{bundle_name}@proxy@{cluster_name}"
            if key in bundle_to_container:
                cid = bundle_to_container[key]
                needed_container_ids.add(cid)
                bundle_assignment_found[(bundle_name, 'proxy')] = cid
            else:
                _print(f"  WARNING: No assignment for proxy bundle '{key}'")
                continue

            if removed_bundles_proxies[bundle_name] != bundle_to_base_count[key]:
                _print(
                    f" WARNING: removed proxies count != base count from assignments for bundle '{key}'. RemovedCount: {removed_bundles_proxies[bundle_name]}, BaseCount: {bundle_to_base_count[key]}"
                )

        if bundles_to_reallocate:
            for bundle_name in bundles_to_reallocate:
                node_key = f"{bundle_name}@node@{cluster_name}"
                if removed_bundles_nodes.get(bundle_name, 0) != bundle_to_base_count.get(node_key, 0):
                    _print(
                        f" WARNING: removed nodes count != base count from assignments for bundle '{node_key}'. "
                        f"RemovedCount: {removed_bundles_nodes.get(bundle_name, 0)}, "
                        f"BaseCount: {bundle_to_base_count.get(node_key, 0)}"
                    )

                proxy_key = f"{bundle_name}@proxy@{cluster_name}"
                if removed_bundles_proxies.get(bundle_name, 0) != bundle_to_base_count.get(proxy_key, 0):
                    _print(
                        f" WARNING: removed proxies count != base count from assignments for bundle '{proxy_key}'. "
                        f"RemovedCount: {removed_bundles_proxies.get(bundle_name, 0)}, "
                        f"BaseCount: {bundle_to_base_count.get(proxy_key, 0)}"
                    )

        # Build new POD_CONFIGURATIONS
        new_configs_list = sorted(
            [(cid, container_sizes[cid]) for cid in needed_container_ids],
            key=lambda x: (-x[1]['memory'], -x[1]['cpu'], -x[1]['network']),
        )
        new_pod_configurations = {}
        priority = 1
        for cid, size in new_configs_list:
            yt_role = 'yttabnode' if size['instance_type'] == 'node' else 'ytrpcproxy'
            config_name_new = f"new_{size['instance_type']}_id{cid}"
            new_pod_configurations[config_name_new] = {
                'vcpu': int(size['cpu'] * 1000),
                'memory': int(size['memory'] * 1024**3),
                'network': int(size['network'] * 1024**2),
                'disk_capacity': 100 * 1024**3,
                'yt_role': yt_role,
                'priority': priority,
            }
            container_id_to_config_name[cid] = config_name_new
            priority += 1

        # Replace config's pod_configurations with the new ones
        from dataclasses import replace as _replace

        config = _replace(config, pod_configurations=new_pod_configurations)
        test_cluster.update_config(config)

        _print(f"\n  New POD_CONFIGURATIONS ({len(new_pod_configurations)} configs):")
        for cname, cfg in new_pod_configurations.items():
            _print(f"    Priority {cfg['priority']}: '{cname}' ({cfg['yt_role']})")
            _print(
                f"      CPU: {cfg['vcpu']/1000:.1f}v, MEM: {cfg['memory']/1024**3:.0f}GiB, "
                f"NET: {cfg['network']/1024**2:.0f}MiB/s"
            )

        # Compute new pod counts per config
        new_pod_counts = {cname: 0 for cname in new_pod_configurations}
        for bundle_name, count in removed_bundles_nodes.items():
            key = (bundle_name, 'node')
            if key in bundle_assignment_found:
                cid = bundle_assignment_found[key]
                if cid in container_id_to_config_name:
                    cname = container_id_to_config_name[cid]
                    assign_key = f"{bundle_name}@node@{cluster_name}"
                    effective_count = bundle_to_new_count.get(assign_key, count)
                    new_pod_counts[cname] += effective_count

        for bundle_name, count in removed_bundles_proxies.items():
            key = (bundle_name, 'proxy')
            if key in bundle_assignment_found:
                cid = bundle_assignment_found[key]
                if cid in container_id_to_config_name:
                    cname = container_id_to_config_name[cid]
                    assign_key = f"{bundle_name}@proxy@{cluster_name}"
                    effective_count = bundle_to_new_count.get(assign_key, count)
                    new_pod_counts[cname] += effective_count

        _print("\n  New pod counts to settle:")
        for cname, count in new_pod_counts.items():
            if count > 0:
                _print(f"    {cname}: {count}")

    # -----------------------------------------------------------------------
    # Step 4: Placement
    # -----------------------------------------------------------------------
    _print("\n4. Solving placement...")

    if change_sizes and new_pod_counts is not None:
        _placement_pod_counts = new_pod_counts
        _total_w = sum(c for c in new_pod_counts.values() if c > 0)
        proportional_weights = {
            name: (count / _total_w if _total_w > 0 else 0.0) for name, count in new_pod_counts.items()
        }
    elif removed_counts is not None:
        _placement_pod_counts = removed_counts
        _weight_counts = current_counts
        _total_managed = sum(_weight_counts.values())
        proportional_weights = {
            name: (count / _total_managed if _total_managed > 0 else 1.0) for name, count in _weight_counts.items()
        }
    else:
        _placement_pod_counts = {name: 0 for name in config.pod_configurations}
        proportional_weights = {name: 0.0 for name in config.pod_configurations}

    # -----------------------------------------------------------------------
    # 4a. Random placement (NO_DEFRAGMENTATION=True)
    # -----------------------------------------------------------------------
    if no_defragmentation:
        _print("\n--- Random placement baseline ---")
        random_k, random_cluster = find_random_placement_max_extra(
            test_cluster, _placement_pod_counts, proportional_weights, seed=random_seed
        )
        random_total_extra = sum(random_k.values())
        _print(f"  Random: {random_total_extra} extra pods")
        _print(f"\n  {'Config':<25} {'Required':>10} {'Extra':>8} {'Total':>8}")
        _print(f"  {'-' * 55}")
        for cn in config.pod_configurations:
            req_c = _placement_pod_counts.get(cn, 0)
            extra = random_k.get(cn, 0)
            if req_c > 0 or extra > 0:
                _print(f"  {cn:<25} {req_c:>10,} {extra:>8,} {req_c + extra:>8,}")
        _print(f"  {'-' * 55}")
        total_req_r = sum(_placement_pod_counts.get(cn, 0) for cn in config.pod_configurations)
        _print(f"  {'TOTAL':<25} {total_req_r:>10,} {random_total_extra:>8,} {total_req_r + random_total_extra:>8,}")

        _util_r = calculate_cluster_resource_utilization(random_cluster)

        _r_extra_cpu = sum(random_k.get(c, 0) * config.pod_configurations[c]['vcpu'] for c in random_k)
        _r_extra_memory = sum(random_k.get(c, 0) * config.pod_configurations[c]['memory'] for c in random_k)
        _r_extra_network = sum(random_k.get(c, 0) * config.pod_configurations[c]['network'] for c in random_k)

        _r_req_cpu_used = _util_r['used_cpu'] - _r_extra_cpu
        _r_req_mem_used = _util_r['used_memory'] - _r_extra_memory
        _r_req_net_used = _util_r['used_network'] - _r_extra_network

        _r_RESERVED_CPU = reserved_rack_stats['cpu_vcores'] * 1000
        _r_RESERVED_MEM = reserved_rack_stats['memory_gib'] * 1024**3
        _r_RESERVED_NET = reserved_rack_stats['net_mib'] * 1024**2

        _r_T_CPU = _util_r['total_cpu'] + _r_RESERVED_CPU
        _r_T_MEM = _util_r['total_memory'] + _r_RESERVED_MEM
        _r_T_NET = _util_r['total_network'] + _r_RESERVED_NET

        req_cpu_pct_r = _r_req_cpu_used / _r_T_CPU * 100 if _r_T_CPU > 0 else 0
        req_mem_pct_r = _r_req_mem_used / _r_T_MEM * 100 if _r_T_MEM > 0 else 0
        req_net_pct_r = _r_req_net_used / _r_T_NET * 100 if _r_T_NET > 0 else 0
        used_cpu_pct_r = _util_r['used_cpu'] / _r_T_CPU * 100 if _r_T_CPU > 0 else 0
        used_mem_pct_r = _util_r['used_memory'] / _r_T_MEM * 100 if _r_T_MEM > 0 else 0
        used_net_pct_r = _util_r['used_network'] / _r_T_NET * 100 if _r_T_NET > 0 else 0

        total_nodes_and_proxies = _util_r['total_nodes'] + _util_r['total_proxies']
        required_for_ratio = total_nodes_and_proxies - random_total_extra
        extra_ratio = (random_total_extra / required_for_ratio) if required_for_ratio > 0 else 0.0
        _print(f"\n  Extra / Required pods: +{extra_ratio * 100:.2f}%")

        _print("\nCluster resource utilization after random placement (excluding sink pods, full cluster):")
        _print("  Required only:")
        _print(f"    CPU:     {_r_req_cpu_used/1000:8.0f} / {_r_T_CPU/1000:.0f} k millicores  ({req_cpu_pct_r:5.1f}%)")
        _print(
            f"    Memory:  {_r_req_mem_used/1024**3:8.0f} / {_r_T_MEM/1024**3:.0f} GiB              ({req_mem_pct_r:5.1f}%)"
        )
        _print(
            f"    Network: {_r_req_net_used/1024**2:8.0f} / {_r_T_NET/1024**2:.0f} MiB/s            ({req_net_pct_r:5.1f}%)"
        )
        _print("  Required + extra:")
        _print(
            f"    CPU:     {_util_r['used_cpu']/1000:8.0f} / {_r_T_CPU/1000:.0f} k millicores  ({used_cpu_pct_r:5.1f}%)"
        )
        _print(
            f"    Memory:  {_util_r['used_memory']/1024**3:8.0f} / {_r_T_MEM/1024**3:.0f} GiB              ({used_mem_pct_r:5.1f}%)"
        )
        _print(
            f"    Network: {_util_r['used_network']/1024**2:8.0f} / {_r_T_NET/1024**2:.0f} MiB/s            ({used_net_pct_r:5.1f}%)"
        )
        _print("  Extra contribution:")
        _print(f"    CPU:     +{_r_extra_cpu/1000:.0f} k millicores  (+{used_cpu_pct_r - req_cpu_pct_r:.1f}%)")
        _print(f"    Memory:  +{_r_extra_memory/1024**3:.0f} GiB              (+{used_mem_pct_r - req_mem_pct_r:.1f}%)")
        _print(
            f"    Network: +{_r_extra_network/1024**2:.0f} MiB/s            (+{used_net_pct_r - req_net_pct_r:.1f}%)"
        )
        _print(f"  Reserved for rack failure ({', '.join(reserved_racks)}, {len(reserved_hostnames)} nodes):")
        _print(
            f"    CPU:     {_r_RESERVED_CPU/1000:8.0f} / {_r_T_CPU/1000:.0f} k millicores  ({_r_RESERVED_CPU/_r_T_CPU*100:5.1f}%)"
        )
        _print(
            f"    Memory:  {_r_RESERVED_MEM/1024**3:8.0f} / {_r_T_MEM/1024**3:.0f} GiB              ({_r_RESERVED_MEM/_r_T_MEM*100:5.1f}%)"
        )
        _print(
            f"    Network: {_r_RESERVED_NET/1024**2:8.0f} / {_r_T_NET/1024**2:.0f} MiB/s            ({_r_RESERVED_NET/_r_T_NET*100:5.1f}%)"
        )
        _print("  Infra tax:")
        _print(
            f"    CPU:     {_util_r['infra_cpu']/1000:8.0f} / {_r_T_CPU/1000:.0f} k millicores  ({_util_r['infra_cpu']/_r_T_CPU*100:5.1f}%)"
        )
        _print(
            f"    Memory:  {_util_r['infra_memory']/1024**3:8.0f} / {_r_T_MEM/1024**3:.0f} GiB              ({_util_r['infra_memory']/_r_T_MEM*100:5.1f}%)"
        )
        _print_utilization_by_role(_print, _util_r, _r_T_CPU, _r_T_MEM, _r_T_NET)

        metrics = {
            'extra_ratio': extra_ratio,
            'extra_cpu_cores': _r_extra_cpu / 1000,
            'extra_cpu_pct': used_cpu_pct_r - req_cpu_pct_r,
            'extra_memory_gib': _r_extra_memory / 1024**3,
            'extra_memory_pct': used_mem_pct_r - req_mem_pct_r,
            'extra_network_mib': _r_extra_network / 1024**2,
            'extra_network_pct': used_net_pct_r - req_net_pct_r,
            'ilp_gap': None,
        }
        return out.getvalue(), metrics, pods_to_remove_dict, None

    # -----------------------------------------------------------------------
    # 4a/4b. Greedy baseline + ILP
    # -----------------------------------------------------------------------
    _print("\nWeights:")
    for config_name, w in proportional_weights.items():
        req_c = _placement_pod_counts.get(config_name, 0)
        if w > 0 or req_c > 0:
            _print(f"  {config_name}: {w:.4f}  (required: {req_c})")

    _print("\n--- Greedy baseline ---")
    greedy_k, greedy_pl = find_greedy_warm_start(test_cluster, _placement_pod_counts, proportional_weights)
    _print(f"  Greedy: {sum(greedy_k.values())} extra pods")
    _print(f"\n  {'Config':<25} {'Required':>10} {'Extra':>8} {'Total':>8}")
    _print(f"  {'-' * 55}")
    for cn in config.pod_configurations:
        req_cnt = _placement_pod_counts.get(cn, 0)
        extra = greedy_k.get(cn, 0)
        if req_cnt > 0 or extra > 0:
            _print(f"  {cn:<25} {req_cnt:>10,} {extra:>8,} {req_cnt + extra:>8,}")
    _print(f"  {'-' * 55}")
    total_req_g = sum(_placement_pod_counts.get(cn, 0) for cn in config.pod_configurations)
    _print(
        f"  {'TOTAL':<25} {total_req_g:>10,} {sum(greedy_k.values()):>8,} "
        f"{total_req_g + sum(greedy_k.values()):>8,}"
    )

    # ILP
    _print("\n--- ILP ---")
    payload = {
        'cluster_dict': test_cluster.to_dict(),
        'config_dict': config.to_dict(),
        'pod_counts': _placement_pod_counts,
        'weights': proportional_weights,
        'greedy_placement': greedy_pl,
        'time_limit_sec': ilp_time_limit_sec,
        'verbose': verbose,
    }
    ilp_success, k_values, ilp_placement, ilp_gap = call_ilp_solver(payload, verbose=verbose)

    if not ilp_success:
        _print("\n  ILP failed to find a feasible solution!")
        return (
            out.getvalue(),
            {
                'extra_ratio': 0.0,
                'extra_cpu_cores': 0.0,
                'extra_cpu_pct': 0.0,
                'extra_memory_gib': 0.0,
                'extra_memory_pct': 0.0,
                'extra_network_mib': 0.0,
                'extra_network_pct': 0.0,
                'ilp_gap': None,
            },
            pods_to_remove_dict,
            None,
        )

    total_placed = apply_ilp_placement(test_cluster, ilp_placement)
    _print(f"\n  ILP placement: {total_placed} pods placed total")

    utilization = calculate_cluster_resource_utilization(test_cluster)

    extra_cpu = sum(k_values.get(c, 0) * config.pod_configurations[c]['vcpu'] for c in k_values)
    extra_memory = sum(k_values.get(c, 0) * config.pod_configurations[c]['memory'] for c in k_values)
    extra_network = sum(k_values.get(c, 0) * config.pod_configurations[c]['network'] for c in k_values)

    req_cpu_used = utilization['used_cpu'] - extra_cpu
    req_mem_used = utilization['used_memory'] - extra_memory
    req_net_used = utilization['used_network'] - extra_network

    _RESERVED_CPU = reserved_rack_stats['cpu_vcores'] * 1000
    _RESERVED_MEM = reserved_rack_stats['memory_gib'] * 1024**3
    _RESERVED_NET = reserved_rack_stats['net_mib'] * 1024**2

    _T_CPU = utilization['total_cpu'] + _RESERVED_CPU
    _T_MEM = utilization['total_memory'] + _RESERVED_MEM
    _T_NET = utilization['total_network'] + _RESERVED_NET

    _print("\n" + "=" * 72)
    _print("PLACEMENT STATISTICS")
    _print("=" * 72)
    _print(f"\n  {'Config':<25} {'Required':>10} {'Greedy+':>9} {'ILP+':>7} {'Total':>8} {'Weight':>8}")
    _print(f"  {'-' * 69}")
    total_required = total_greedy_extra = total_ilp_extra = 0
    for config_name in config.pod_configurations:
        required = _placement_pod_counts.get(config_name, 0)
        g_extra = greedy_k.get(config_name, 0)
        ilp_extra = k_values.get(config_name, 0)
        weight = proportional_weights.get(config_name, 0.0)
        if required > 0 or ilp_extra > 0:
            if ilp_extra > g_extra:
                improvement = f" (+{ilp_extra - g_extra})"
            elif ilp_extra == g_extra:
                improvement = " (=)"
            else:
                improvement = f" (-{g_extra - ilp_extra})"
            _print(
                f"  {config_name:<25} {required:>10,} {g_extra:>9,} {ilp_extra:>7,}{improvement:<9} "
                f"{required + ilp_extra:>8,} {weight:>8.4f}"
            )
            total_required += required
            total_greedy_extra += g_extra
            total_ilp_extra += ilp_extra
    _print(f"  {'-' * 69}")
    _print(
        f"  {'TOTAL':<25} {total_required:>10,} {total_greedy_extra:>9,} {total_ilp_extra:>7,}"
        f"          {total_required + total_ilp_extra:>8,}"
    )
    if total_ilp_extra > total_greedy_extra:
        _print(f"\n  ILP improved greedy by +{total_ilp_extra - total_greedy_extra} pods")
    elif total_ilp_extra == total_greedy_extra:
        _print("\n  ILP matched greedy result")

    req_cpu_pct = req_cpu_used / _T_CPU * 100 if _T_CPU > 0 else 0
    req_mem_pct = req_mem_used / _T_MEM * 100 if _T_MEM > 0 else 0
    req_net_pct = req_net_used / _T_NET * 100 if _T_NET > 0 else 0
    used_cpu_pct = utilization['used_cpu'] / _T_CPU * 100 if _T_CPU > 0 else 0
    used_mem_pct = utilization['used_memory'] / _T_MEM * 100 if _T_MEM > 0 else 0
    used_net_pct = utilization['used_network'] / _T_NET * 100 if _T_NET > 0 else 0

    total_nodes_and_proxies = utilization['total_nodes'] + utilization['total_proxies']
    required_for_ratio = total_nodes_and_proxies - total_ilp_extra
    extra_ratio = (total_ilp_extra / required_for_ratio) if required_for_ratio > 0 else 0.0
    _print(f"\n  Extra / Required pods: +{extra_ratio * 100:.2f}%")

    _print("\nCluster resource utilization (excluding sink pods, full cluster):")
    _print("  Required only:")
    _print(f"    CPU:     {req_cpu_used/1000:8.0f} / {_T_CPU/1000:.0f} k millicores  ({req_cpu_pct:5.1f}%)")
    _print(f"    Memory:  {req_mem_used/1024**3:8.0f} / {_T_MEM/1024**3:.0f} GiB              ({req_mem_pct:5.1f}%)")
    _print(f"    Network: {req_net_used/1024**2:8.0f} / {_T_NET/1024**2:.0f} MiB/s            ({req_net_pct:5.1f}%)")
    _print("  Required + extra:")
    _print(f"    CPU:     {utilization['used_cpu']/1000:8.0f} / {_T_CPU/1000:.0f} k millicores  ({used_cpu_pct:5.1f}%)")
    _print(
        f"    Memory:  {utilization['used_memory']/1024**3:8.0f} / {_T_MEM/1024**3:.0f} GiB              ({used_mem_pct:5.1f}%)"
    )
    _print(
        f"    Network: {utilization['used_network']/1024**2:8.0f} / {_T_NET/1024**2:.0f} MiB/s            ({used_net_pct:5.1f}%)"
    )
    _print("  Extra contribution:")
    _print(f"    CPU:     +{extra_cpu/1000:.0f} k millicores  (+{used_cpu_pct - req_cpu_pct:.1f}%)")
    _print(f"    Memory:  +{extra_memory/1024**3:.0f} GiB              (+{used_mem_pct - req_mem_pct:.1f}%)")
    _print(f"    Network: +{extra_network/1024**2:.0f} MiB/s            (+{used_net_pct - req_net_pct:.1f}%)")
    _print(f"  Reserved for rack failure ({', '.join(reserved_racks)}, {len(reserved_hostnames)} nodes):")
    _print(
        f"    CPU:     {_RESERVED_CPU/1000:8.0f} / {_T_CPU/1000:.0f} k millicores  ({_RESERVED_CPU/_T_CPU*100:5.1f}%)"
    )
    _print(
        f"    Memory:  {_RESERVED_MEM/1024**3:8.0f} / {_T_MEM/1024**3:.0f} GiB              ({_RESERVED_MEM/_T_MEM*100:5.1f}%)"
    )
    _print(
        f"    Network: {_RESERVED_NET/1024**2:8.0f} / {_T_NET/1024**2:.0f} MiB/s            ({_RESERVED_NET/_T_NET*100:5.1f}%)"
    )
    _print("  Infra tax:")
    _print(
        f"    CPU:     {utilization['infra_cpu']/1000:8.0f} / {_T_CPU/1000:.0f} k millicores  ({utilization['infra_cpu']/_T_CPU*100:5.1f}%)"
    )
    _print(
        f"    Memory:  {utilization['infra_memory']/1024**3:8.0f} / {_T_MEM/1024**3:.0f} GiB              ({utilization['infra_memory']/_T_MEM*100:5.1f}%)"
    )
    _print_utilization_by_role(_print, utilization, _T_CPU, _T_MEM, _T_NET)

    metrics = {
        'extra_ratio': extra_ratio,
        'extra_cpu_cores': extra_cpu / 1000,
        'extra_cpu_pct': used_cpu_pct - req_cpu_pct,
        'extra_memory_gib': extra_memory / 1024**3,
        'extra_memory_pct': used_mem_pct - req_mem_pct,
        'extra_network_mib': extra_network / 1024**2,
        'extra_network_pct': used_net_pct - req_net_pct,
        'ilp_gap': ilp_gap,
    }

    # -----------------------------------------------------------------------
    # Step 5: Pod Placement Mapping with Rack Awareness
    # -----------------------------------------------------------------------
    _print("\n5. Pod placement mapping with rack awareness...")

    # Build the list of pods to create (may differ from pods_to_remove when
    # optimization_result changed bundle instance counts).
    from .cluster import PodToRemove as _PodToRemove

    def _annotations_with_new_resources(old_annotations: dict, cid: int) -> dict:
        """Return a copy of old_annotations with resources updated from container_sizes[cid]."""
        size = container_sizes[cid]
        new_ann = {k: v for k, v in old_annotations.items() if k != 'resources'}
        new_ann['resources'] = {
            'vcpu': int(size['cpu'] * 1000),
            'memory': int(size['memory'] * 1024**3),
            'net_bytes': int(size['network'] * 1024**2),
            'net': int(size['network'] * 1024**2) * 8,
            'type': "",
        }
        return new_ann

    tabnodes_to_create: list = []
    rpcproxies_to_create: list = []

    if change_sizes:
        # Tabnodes: use old pods with new config_name, add stubs if count increased.
        # config_name = ILP solver name (must match synthetic pod names for placement).
        # human_config_name = human-readable name from ConfigName column (for output).
        for bundle_name, old_pods in pods_to_remove_structure.tabnodes_by_bundle.items():
            key = (bundle_name, 'node')
            if key not in bundle_assignment_found:
                _print(f"  WARNING: node bundle '{key}' wouldn't be placed -- no assignment")
                continue

            cid = bundle_assignment_found[key]
            new_config = container_id_to_config_name[cid]
            create_config = container_id_to_human_config_name.get(cid, new_config)
            assign_key = f"{bundle_name}@node@{cluster_name}"
            new_count = bundle_to_new_count[assign_key]
            template = old_pods[0]

            for pod in old_pods[:new_count]:
                tabnodes_to_create.append(
                    _PodToRemove(
                        pod_id=pod.pod_id,
                        yt_pod_name=pod.yt_pod_name,
                        bundle_controller_annotations=_annotations_with_new_resources(
                            pod.bundle_controller_annotations, cid
                        ),
                        user_tags=pod.user_tags.copy(),
                        decommissioned=False,
                        proxy_role=pod.proxy_role,
                        config_name=new_config,
                        human_config_name=create_config,
                    )
                )

            for i in range(len(old_pods), new_count):
                stub_idx = i - len(old_pods)
                tabnodes_to_create.append(
                    _PodToRemove(
                        pod_id=f"new_{bundle_name}_tab_{stub_idx}",
                        yt_pod_name=f"xxxxxxxxx.{bundle_name}.yp-c.yandex.net:9022",
                        bundle_controller_annotations=_annotations_with_new_resources(
                            template.bundle_controller_annotations, cid
                        ),
                        user_tags=template.user_tags.copy(),
                        decommissioned=False,
                        proxy_role=None,
                        config_name=new_config,
                        human_config_name=create_config,
                    )
                )

        # RPC proxies: same logic
        for bundle_name, old_pods in pods_to_remove_structure.rpcproxies_by_bundle.items():
            key = (bundle_name, 'proxy')
            if key not in bundle_assignment_found:
                _print(f"  WARNING: proxy bundle '{key}' wouldn't be placed -- no assignment")
                continue

            cid = bundle_assignment_found[key]
            new_config = container_id_to_config_name[cid]
            create_config = container_id_to_human_config_name.get(cid, new_config)
            assign_key = f"{bundle_name}@proxy@{cluster_name}"
            new_count = bundle_to_new_count[assign_key]
            template = old_pods[0]

            for pod in old_pods[:new_count]:
                rpcproxies_to_create.append(
                    _PodToRemove(
                        pod_id=pod.pod_id,
                        yt_pod_name=pod.yt_pod_name,
                        bundle_controller_annotations=_annotations_with_new_resources(
                            pod.bundle_controller_annotations, cid
                        ),
                        user_tags=pod.user_tags.copy(),
                        decommissioned=False,
                        proxy_role=pod.proxy_role,
                        config_name=new_config,
                        human_config_name=create_config,
                    )
                )

            for i in range(len(old_pods), new_count):
                stub_idx = i - len(old_pods)
                rpcproxies_to_create.append(
                    _PodToRemove(
                        pod_id=f"new_{bundle_name}_proxy_{stub_idx}",
                        yt_pod_name=f"xxxxxxxxx.{bundle_name}.yp-c.yandex.net:9012",
                        bundle_controller_annotations=_annotations_with_new_resources(
                            template.bundle_controller_annotations, cid
                        ),
                        user_tags=template.user_tags.copy(),
                        decommissioned=False,
                        proxy_role=template.proxy_role,
                        config_name=new_config,
                        human_config_name=create_config,
                    )
                )
    else:
        for pods in pods_to_remove_structure.tabnodes_by_bundle.values():
            tabnodes_to_create.extend(pods)
        for pods in pods_to_remove_structure.rpcproxies_by_bundle.values():
            rpcproxies_to_create.extend(pods)

    _print(f"  Pods to create: {len(tabnodes_to_create)} tabnodes, " f"{len(rpcproxies_to_create)} RPC proxies")

    try:
        synthetic_placements = collect_synthetic_pods_placement(test_cluster)
        _print(
            f"  Collected synthetic placements for {len(synthetic_placements)} roles: "
            f"{', '.join(f'{r}: {len(p)}' for r, p in synthetic_placements.items())}"
        )

        enriched_tabnodes, enriched_rpcproxies = map_pods_with_rack_awareness(
            tabnodes_to_create=tabnodes_to_create,
            rpcproxies_to_create=rpcproxies_to_create,
            synthetic_placements=synthetic_placements,
            cluster_name=cluster_name,
            dc=config.dc,
        )

        pods_to_create_dict = _enriched_pods_to_dict(enriched_tabnodes, enriched_rpcproxies)
        _print(
            f"  Pod placement mapping complete: {len(enriched_tabnodes)} tabnodes, "
            f"{len(enriched_rpcproxies)} RPC proxies placed"
        )
    except Exception as e:
        _print(f"  WARNING: Pod placement mapping failed: {e}")
        pods_to_create_dict = None

    return out.getvalue(), metrics, pods_to_remove_dict, pods_to_create_dict
