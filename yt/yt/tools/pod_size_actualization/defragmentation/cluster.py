"""
Main-process-only cluster analysis and placement logic.

Everything here runs only in the hermetic binary (main process), never in the
ILP solver subprocess. Imports pandas and uses deepcopy heavily.
"""

import heapq
import random
from copy import deepcopy
from dataclasses import dataclass, field
from io import StringIO
from typing import Dict, List, Optional, Tuple

from .scripts.shared import (
    AvailableResourcesRequest,
    Cluster,
    ClusterConfig,
    PodResources,
    classify_pod,
    filter_host,
    get_disk_bandwidth_from_storage_class,
)

# ---------------------------------------------------------------------------
# AvailableResourcesResponse
# ---------------------------------------------------------------------------


@dataclass
class AvailableResourcesResponse:
    count: int
    hosts: Dict[str, int]  # hostname -> slots
    numa_info: Dict[str, List[int]] = field(default_factory=dict)  # hostname -> slots per NUMA


# ---------------------------------------------------------------------------
# Pods-to-remove structures
# ---------------------------------------------------------------------------


@dataclass
class PodToRemove:
    pod_id: str
    yt_pod_name: str
    bundle_controller_annotations: Dict
    user_tags: List[str]
    decommissioned: bool
    proxy_role: Optional[str]
    config_name: str = ''
    human_config_name: str = ''

    def __post_init__(self):
        if self.bundle_controller_annotations is None:
            self.bundle_controller_annotations = {}
        if self.user_tags is None:
            self.user_tags = []


@dataclass
class SkippedPod:
    pod_id: str
    yt_pod_name: str
    yt_role: str
    skip_reason: str
    bundle_name: str = ''


@dataclass
class PodsToRemoveStructure:
    tabnodes_by_bundle: Dict[str, List[PodToRemove]]
    rpcproxies_by_bundle: Dict[str, List[PodToRemove]]
    skipped_counts: Dict[str, int]
    skipped_pods: List[SkippedPod]

    def __post_init__(self):
        if self.tabnodes_by_bundle is None:
            self.tabnodes_by_bundle = {}
        if self.rpcproxies_by_bundle is None:
            self.rpcproxies_by_bundle = {}
        if self.skipped_counts is None:
            self.skipped_counts = {}


# ---------------------------------------------------------------------------
# Available-resources calculation
# ---------------------------------------------------------------------------


def _get_resource_slots(free_resource: int, slot_size: int) -> int:
    if slot_size <= 0:
        return float('inf')
    return free_resource // slot_size


def calc_available_resources(
    cluster: Cluster,
    req: AvailableResourcesRequest,
) -> AvailableResourcesResponse:
    response = AvailableResourcesResponse(count=0, hosts={})

    active_hosts = cluster.get_active_hosts()
    filtered_hosts = [h for h in active_hosts if filter_host(h, req, cluster.config.role_specific_host_filter)]

    for host in filtered_hosts:
        eff = host.get_effective_free_resources()
        available_vcpu, available_memory, available_net, available_disk_cap, available_disk_bw = eff

        req_disk_bw = get_disk_bandwidth_from_storage_class(host.disk_storage_class)
        cpu_slots = _get_resource_slots(available_vcpu, req.vcpu)
        memory_slots = _get_resource_slots(available_memory, req.memory)
        net_slots = _get_resource_slots(available_net, req.net)
        disk_cap_slots = _get_resource_slots(available_disk_cap, req.disk_capacity)
        disk_bw_slots = _get_resource_slots(available_disk_bw, req_disk_bw)

        slots = min(cpu_slots, memory_slots, net_slots, disk_cap_slots, disk_bw_slots)

        numa_slots_per_node = []
        if req.numa_enabled:
            numa_resources = host.get_available_numa_resources()
            numa_slots_total = 0
            for numa_slot in numa_resources:
                ns_cpu = _get_resource_slots(numa_slot.vcpu, req.vcpu)
                ns_mem = _get_resource_slots(numa_slot.ram * 1024 * 1024, req.memory)
                ns = min(ns_cpu, ns_mem)
                numa_slots_per_node.append(ns)
                numa_slots_total += ns
            slots = min(slots, numa_slots_total)

        if req.antiaffinity and req.yt_role:
            existing = len(host.get_pods_by_role(req.yt_role))
            slots = min(slots, max(0, req.antiaffinity - existing))

        if slots > 0:
            response.count += slots
            response.hosts[host.hostname] = slots
            if req.numa_enabled and numa_slots_per_node:
                numa_total = sum(numa_slots_per_node)
                if numa_total > slots:
                    slots_to_remove = numa_total - slots
                    while slots_to_remove > 0:
                        max_idx = max(range(len(numa_slots_per_node)), key=lambda i: numa_slots_per_node[i])
                        if numa_slots_per_node[max_idx] <= 0:
                            break
                        numa_slots_per_node[max_idx] -= 1
                        slots_to_remove -= 1
                response.numa_info[host.hostname] = numa_slots_per_node

    return response


# ---------------------------------------------------------------------------
# Pod counting / creation
# ---------------------------------------------------------------------------


def count_current_pod_configurations(cluster: Cluster) -> Dict[str, int]:
    counts = {name: 0 for name in cluster.config.pod_configurations}
    for host in cluster.hosts.values():
        for pod in host.pods:
            name = classify_pod(pod, cluster.config.pod_configurations)
            if name:
                counts[name] += 1
    return counts


def create_pod_instance(
    config_name: str,
    instance_id: int,
    disk_storage_class: str,
    pod_configurations: Dict[str, dict],
    numa_node_id: Optional[int] = None,
) -> PodResources:
    cfg = pod_configurations[config_name]
    return PodResources(
        pod_id=f"synthetic_{config_name}_{instance_id}",
        yt_sink_pod=False,
        vcpu_guarantee=cfg['vcpu'],
        memory_guarantee=cfg['memory'],
        network_guarantee=cfg['network'],
        numa_node_id=numa_node_id,
        yt_role=cfg['yt_role'],
        disk_capacity=cfg['disk_capacity'],
        disk_bandwidth_guarantee=get_disk_bandwidth_from_storage_class(disk_storage_class),
    )


# ---------------------------------------------------------------------------
# Greedy placement
# ---------------------------------------------------------------------------


def _host_heap_key(eff: tuple, req: AvailableResourcesRequest, host_slots: int) -> tuple:
    cpu_slots_raw = eff[0] // req.vcpu if req.vcpu > 0 else 10**9
    mem_slots_raw = eff[1] // req.memory if req.memory > 0 else 10**9
    net_slots_raw = eff[2] // req.net if req.net > 0 else 10**9
    limiting_slots = min(cpu_slots_raw, mem_slots_raw, net_slots_raw)
    return (limiting_slots, -eff[2], -eff[1], -eff[0])


def place_pods_from_calc_response(
    cluster: Cluster,
    config_name: str,
    count: int,
    response: AvailableResourcesResponse,
    req: AvailableResourcesRequest,
) -> None:
    if count <= 0:
        return
    if count > response.count:
        raise ValueError(f"Cannot place {count} pods of {config_name}: only {response.count} slots")

    host_remaining = {hn: slots for hn, slots in response.hosts.items()}
    host_numa = {
        hn: (response.numa_info[hn].copy() if req.numa_enabled and hn in response.numa_info else [slots])
        for hn, slots in response.hosts.items()
    }

    heap: list = []
    for hn, slots in host_remaining.items():
        if slots <= 0:
            continue
        host = cluster.get_host(hn)
        if host is None:
            continue
        eff = host.get_effective_free_resources()
        key = _host_heap_key(eff, req, slots)
        best_numa = max(range(len(host_numa[hn])), key=lambda i: host_numa[hn][i])
        heapq.heappush(heap, (key, hn, best_numa))

    pod_counter = 0
    pods_left = count
    while pods_left > 0 and heap:
        key, hn, numa_idx = heapq.heappop(heap)
        if host_remaining[hn] <= 0:
            continue
        numa_node_id = numa_idx if req.numa_enabled else None
        pod = create_pod_instance(
            config_name,
            pod_counter,
            cluster.get_host(hn).disk_storage_class,
            cluster.config.pod_configurations,
            numa_node_id,
        )
        cluster.add_pod_to_host(hn, pod)
        host_remaining[hn] -= 1
        host_numa[hn][numa_idx] -= 1
        pod_counter += 1
        pods_left -= 1
        if host_remaining[hn] > 0:
            host = cluster.get_host(hn)
            eff = host.get_effective_free_resources()
            new_key = _host_heap_key(eff, req, host_remaining[hn])
            new_numa = max(range(len(host_numa[hn])), key=lambda i: host_numa[hn][i])
            heapq.heappush(heap, (new_key, hn, new_numa))

    if pods_left > 0:
        raise RuntimeError(f"GREEDY BUG: could not place {pods_left} of {count} {config_name} pods")


def extract_greedy_numa_placement(
    cluster_before: Cluster,
    cluster_after: Cluster,
    configs: List[str],
) -> Dict[str, Dict[str, List[Optional[int]]]]:
    result: Dict[str, Dict[str, List[Optional[int]]]] = {c: {} for c in configs}
    for hn, host_after in cluster_after.hosts.items():
        host_before = cluster_before.hosts.get(hn)
        before_counts: Dict[str, int] = {c: 0 for c in configs}
        if host_before:
            for pod in host_before.pods:
                cn = classify_pod(pod, cluster_after.config.pod_configurations)
                if cn in before_counts:
                    before_counts[cn] += 1
        after_by_config: Dict[str, list] = {c: [] for c in configs}
        for pod in host_after.pods:
            cn = classify_pod(pod, cluster_after.config.pod_configurations)
            if cn in after_by_config:
                after_by_config[cn].append(pod)
        for cn in configs:
            new_pods = after_by_config[cn][before_counts[cn] :]
            if new_pods:
                result[cn][hn] = [p.numa_node_id for p in new_pods]
    return result


def _redirect_stdout():
    import sys

    old = sys.stdout
    sys.stdout = StringIO()
    return old


def _restore_stdout(old):
    import sys

    sys.stdout = old


def _try_place_pods_optimally_quiet(cluster: Cluster, pod_counts: Dict[str, int]):
    """Returns (True, modified_cluster) or (False, original_cluster)."""
    test_cluster = deepcopy(cluster)
    sorted_configs = sorted(
        pod_counts.items(),
        key=lambda x: (
            -cluster.config.pod_configurations[x[0]]['memory'],
            -cluster.config.pod_configurations[x[0]]['vcpu'],
        ),
    )
    for config_name, count in sorted_configs:
        if count <= 0:
            continue
        cfg = cluster.config.pod_configurations[config_name]
        req = AvailableResourcesRequest(
            vcpu=cfg['vcpu'],
            memory=cfg['memory'],
            net=cfg['network'],
            disk_capacity=cfg['disk_capacity'],
            yt_role=cfg['yt_role'],
            numa_enabled=True,
            antiaffinity=cluster.config.antiaffinity.get(cfg['yt_role']),
        )
        old_stdout = _redirect_stdout()
        try:
            response = calc_available_resources(test_cluster, req)
        finally:
            _restore_stdout(old_stdout)
        if response.count < count:
            return False, cluster
        place_pods_from_calc_response(test_cluster, config_name, count, response, req)
    return True, test_cluster


def _try_place_pods_random_quiet(cluster: Cluster, pod_counts: Dict[str, int], seed: int):
    """Returns (True, modified_cluster) or (False, original_cluster)."""
    test_cluster = deepcopy(cluster)
    pod_list = []
    for config_name, count in pod_counts.items():
        if count > 0:
            pod_list.extend([config_name] * count)

    rng = random.Random(seed)
    rng.shuffle(pod_list)

    pod_counter = 0
    for config_name in pod_list:
        cfg = cluster.config.pod_configurations[config_name]
        req = AvailableResourcesRequest(
            vcpu=cfg['vcpu'],
            memory=cfg['memory'],
            net=cfg['network'],
            disk_capacity=cfg['disk_capacity'],
            yt_role=cfg['yt_role'],
            numa_enabled=True,
            antiaffinity=cluster.config.antiaffinity.get(cfg['yt_role']),
        )
        old_stdout = _redirect_stdout()
        try:
            response = calc_available_resources(test_cluster, req)
        finally:
            _restore_stdout(old_stdout)
        if response.count == 0:
            return False, cluster

        hostname = rng.choice(list(response.hosts.keys()))
        numa_node_id = None
        if hostname in response.numa_info:
            available_numa = [i for i, s in enumerate(response.numa_info[hostname]) if s > 0]
            if available_numa:
                numa_node_id = rng.choice(available_numa)

        host = test_cluster.get_host(hostname)
        pod = create_pod_instance(
            config_name, pod_counter, host.disk_storage_class, cluster.config.pod_configurations, numa_node_id
        )
        test_cluster.add_pod_to_host(hostname, pod)
        pod_counter += 1

    return True, test_cluster


def _binary_search_max_extra(
    cluster: Cluster,
    required_counts: Dict[str, int],
    weights: Dict[str, float],
    try_fn,
    max_extra: int = 5000,
) -> tuple:
    """Binary search for max extra pods proportional to weights. Returns (extra_k_dict, best_cluster)."""
    configs = list(required_counts.keys())
    total_weight = sum(w for cn, w in weights.items() if w > 0 and cn in required_counts)

    def _distribute(n: int) -> Dict[str, int]:
        counts = required_counts.copy()
        if total_weight > 0:
            for cn, w in weights.items():
                if w > 0 and cn in counts:
                    counts[cn] += int(n * w / total_weight)
        return counts

    left, right = 0, 10
    best_cluster = None
    while right <= max_extra:
        success, c = try_fn(cluster, _distribute(right))
        if success:
            left = right
            best_cluster = c
            right *= 2
        else:
            break

    while left < right:
        mid = (left + right + 1) // 2
        success, c = try_fn(cluster, _distribute(mid))
        if success:
            left = mid
            best_cluster = c
        else:
            right = mid - 1

    if best_cluster is None:
        _, best_cluster = try_fn(cluster, _distribute(0))

    final_counts = _distribute(left)
    extra_k = {cn: max(0, final_counts.get(cn, 0) - required_counts.get(cn, 0)) for cn in configs}
    return extra_k, best_cluster


def find_random_placement_max_extra(
    cluster: Cluster,
    required_counts: Dict[str, int],
    weights: Dict[str, float],
    max_extra: int = 5000,
    seed: int = 42,
) -> tuple:
    def _try(c, counts):
        return _try_place_pods_random_quiet(c, counts, seed)

    return _binary_search_max_extra(cluster, required_counts, weights, _try, max_extra)


def find_greedy_warm_start(
    cluster: Cluster,
    required_counts: Dict[str, int],
    weights: Dict[str, float],
    max_extra: int = 5000,
) -> tuple:
    configs = list(required_counts.keys())

    def _try(c, counts):
        return _try_place_pods_optimally_quiet(c, counts)

    greedy_k_raw, best_cluster = _binary_search_max_extra(cluster, required_counts, weights, _try, max_extra)

    if best_cluster is cluster:  # no solution found even for required
        return {cn: 0 for cn in configs}, {}

    greedy_pl = extract_greedy_numa_placement(cluster, best_cluster, configs)
    greedy_k = {
        cn: max(0, sum(len(v) for v in greedy_pl.get(cn, {}).values()) - required_counts.get(cn, 0)) for cn in configs
    }
    return greedy_k, greedy_pl


# ---------------------------------------------------------------------------
# Defragmentation operations
# ---------------------------------------------------------------------------


def collect_pods_to_remove(
    cluster: Cluster,
    bundle_hotfix_bundles: List[str] = None,
) -> PodsToRemoveStructure:
    if bundle_hotfix_bundles is None:
        bundle_hotfix_bundles = []

    tabnodes_by_bundle: Dict[str, List[PodToRemove]] = {}
    rpcproxies_by_bundle: Dict[str, List[PodToRemove]] = {}
    skipped_counts = {'no_bundle': 0, 'hotfix_bundle': 0, 'no_config_match': 0, 'not_online': 0}
    skipped_pods: List[SkippedPod] = []

    for host in cluster.hosts.values():
        for pod in host.pods:
            if pod.yt_role not in ('yttabnode', 'ytrpcproxy'):
                continue

            bundle_name = ''
            if pod.yt_bundle_controller_annotations:
                bundle_name = pod.yt_bundle_controller_annotations.get('allocated_for_bundle', '')

            if not pod.yt_bundle_controller_annotations or not bundle_name:
                skipped_counts['no_bundle'] += 1
                skipped_pods.append(SkippedPod(pod.pod_id, pod.yt_pod_name, pod.yt_role, 'no_bundle'))
                continue

            if bundle_name in bundle_hotfix_bundles:
                skipped_counts['hotfix_bundle'] += 1
                skipped_pods.append(SkippedPod(pod.pod_id, pod.yt_pod_name, pod.yt_role, 'hotfix_bundle', bundle_name))
                continue

            config_name = classify_pod(pod, cluster.config.pod_configurations)
            if config_name is None:
                skipped_counts['no_config_match'] += 1
                skipped_pods.append(
                    SkippedPod(pod.pod_id, pod.yt_pod_name, pod.yt_role, 'no_config_match', bundle_name)
                )
                continue

            if pod.yt_role == 'yttabnode' and pod.yt_state != 'online':
                skipped_counts['not_online'] += 1
                skipped_pods.append(SkippedPod(pod.pod_id, pod.yt_pod_name, pod.yt_role, 'not_online', bundle_name))
                continue

            if pod.yt_role == 'ytrpcproxy' and not pod.yt_alive:
                skipped_counts['not_online'] += 1
                skipped_pods.append(SkippedPod(pod.pod_id, pod.yt_pod_name, pod.yt_role, 'not_online', bundle_name))
                continue

            pod_info = PodToRemove(
                pod_id=pod.pod_id,
                yt_pod_name=pod.yt_pod_name,
                bundle_controller_annotations=pod.yt_bundle_controller_annotations.copy(),
                user_tags=list(pod.yt_user_tags) if pod.yt_user_tags else [],
                decommissioned=pod.yt_decommissioned,
                proxy_role=pod.yt_proxy_role,
                config_name=config_name,
            )

            if pod.yt_role == 'yttabnode':
                tabnodes_by_bundle.setdefault(bundle_name, []).append(pod_info)
            else:
                rpcproxies_by_bundle.setdefault(bundle_name, []).append(pod_info)

    return PodsToRemoveStructure(
        tabnodes_by_bundle=tabnodes_by_bundle,
        rpcproxies_by_bundle=rpcproxies_by_bundle,
        skipped_counts=skipped_counts,
        skipped_pods=skipped_pods,
    )


def extract_pod_ids_from_structure(structure: PodsToRemoveStructure) -> List[str]:
    ids = []
    for pods in structure.tabnodes_by_bundle.values():
        ids.extend(p.pod_id for p in pods)
    for pods in structure.rpcproxies_by_bundle.values():
        ids.extend(p.pod_id for p in pods)
    return ids


def remove_tabnodes_and_rpcproxy(
    cluster: Cluster,
    pod_ids_to_remove: List[str],
) -> Dict[str, int]:
    pod_ids_set = set(pod_ids_to_remove)
    removed_counts = {name: 0 for name in cluster.config.pod_configurations}

    for host in cluster.hosts.values():
        to_remove = []
        for pod in host.pods:
            if pod.yt_role in ('yttabnode', 'ytrpcproxy'):
                if not pod_ids_to_remove or pod.pod_id in pod_ids_set:
                    config_name = classify_pod(pod, cluster.config.pod_configurations)
                    if config_name:
                        removed_counts[config_name] += 1
                    to_remove.append(pod.pod_id)
        for pod_id in to_remove:
            host.remove_pod(pod_id)

    return removed_counts


def update_custom_pods(cluster: Cluster):
    print("Updating bigb tab nodes -> memory_150...")
    print("Updating fury-fairy-preprod tab nodes -> memory_250 x 3 (old configuration)...")
    print("Updating fury-supermod-preprod tab nodes -> memory_200 x 5 (old configuration)...")
    searchpers_node_count = 0
    searchpers_example = None
    fury_fairy_preprod_pod_count = 0
    caesar_proxy_count = 0
    caesar_example = None
    caesar_prestable_proxy_count = 0
    caesar_prestable_example = None
    for host in cluster.get_active_hosts():
        for pod in host.pods:
            if pod.yt_role == 'yttabnode' and pod.yt_state == 'online':
                if pod.yt_bundle_controller_annotations:
                    bundle_name = pod.yt_bundle_controller_annotations.get('allocated_for_bundle', '')
                    if bundle_name == "bigb":
                        pod.memory_guarantee = 150 * 1024**3

                    if bundle_name == "searchpers":
                        pod.memory_guarantee = 150 * 1024**3
                        searchpers_node_count += 1
                        searchpers_example = pod

                    if bundle_name == "fury-fairy-preprod":
                        fury_fairy_preprod_pod_count += 1

                        if fury_fairy_preprod_pod_count > 3:
                            host.remove_pod(pod.pod_id)

                        pod.memory_guarantee = 250 * 1024**3
                        pod.network_guarantee = 600 * 1024**2
                        pod.vcpu_guarantee = 28 * 1000

                    if bundle_name == "fury-supermod-preprod":
                        pod.memory_guarantee = 200 * 1024**3
                        pod.network_guarantee = 600 * 1024**2
                        pod.vcpu_guarantee = 28 * 1000

            if pod.yt_role == 'ytrpcproxy' and pod.yt_alive:
                if pod.yt_bundle_controller_annotations:
                    bundle_name = pod.yt_bundle_controller_annotations.get('allocated_for_bundle', '')
                    if bundle_name == "caesar-prestable":
                        caesar_prestable_proxy_count += 1
                        pod.memory_guarantee = 20 * 1024**3
                        pod.network_guarantee = 150 * 1024**2
                        pod.vcpu_guarantee = 10 * 1000
                        caesar_prestable_example = pod

                    if bundle_name == "caesar":
                        caesar_proxy_count += 1
                        pod.memory_guarantee = 20 * 1024**3
                        pod.network_guarantee = 150 * 1024**2
                        pod.vcpu_guarantee = 10 * 1000
                        caesar_example = pod

                    if bundle_name == "bigb":
                        pod.memory_guarantee = 20 * 1024**3
                        pod.network_guarantee = 150 * 1024**2
                        pod.vcpu_guarantee = 10 * 1000

    print(
        f"Updating searchpers tab nodes -> memory_150 x 193 (old configuration, adding {193 - searchpers_node_count})..."
    )
    for _ in range(max(193 - searchpers_node_count, 0)):
        for host in cluster.get_active_hosts():
            try:
                pod = deepcopy(searchpers_example)
                pod.pod_id = host.hostname[:9] + pod.pod_id[9:]
                pod.yt_pod_name = host.hostname[:9] + pod.yt_pod_name[9:]
                cluster.add_pod_to_host(host.hostname, pod)
                break
            except Exception:
                continue

    print("Updating bigb rpc_proxies -> medium...")
    print(
        f"Updating caesar-prestable rpc proxies -> medium x 60 (old configuration, adding {60 - caesar_prestable_proxy_count})..."
    )
    for _ in range(max(60 - caesar_prestable_proxy_count, 0)):
        for host in cluster.get_active_hosts():
            try:
                pod = deepcopy(caesar_prestable_example)
                pod.pod_id = host.hostname[:9] + pod.pod_id[9:]
                pod.yt_pod_name = host.hostname[:9] + pod.yt_pod_name[9:]
                cluster.add_pod_to_host(host.hostname, pod)
                break
            except Exception:
                continue

    print(f"Updating caesar rpc proxies -> medium x 127 (old configuration, adding {127 - caesar_proxy_count})...")
    for _ in range(max(127 - caesar_proxy_count, 0)):
        for host in cluster.get_active_hosts():
            try:
                pod = deepcopy(caesar_example)
                pod.pod_id = host.hostname[:9] + pod.pod_id[9:]
                pod.yt_pod_name = host.hostname[:9] + pod.yt_pod_name[9:]
                cluster.add_pod_to_host(host.hostname, pod)
                break
            except Exception:
                continue


def raise_network_limits(cluster: Cluster, seed: int = 42) -> Dict[str, int]:
    print("Raising data node network limits based on disk configuration...")
    cfg = cluster.config
    removed_counts = {name: 0 for name in cfg.pod_configurations}

    modified_hosts = []
    for host in cluster.hosts.values():
        host_modified = False
        for pod in host.pods:
            if pod.yt_role == 'ytdatnode':
                pod.network_guarantee = cfg.data_node_network_guarantee * 1024**2
                host_modified = True
            elif pod.yt_role == 'yttimestampprovider':
                pod.network_guarantee = cfg.timestamp_provider_network_guarantee * 1024**2
                host_modified = True
            elif pod.yt_role == 'ytmastercache':
                pod.network_guarantee = cfg.master_cache_network_guarantee * 1024**2
                host_modified = True
        if host_modified:
            modified_hosts.append(host)

    for host in modified_hosts:
        used = host.get_used_resources_from_pods()
        net_excess = used[2] - host.network_total_bandwidth_mib
        if net_excess > 1.0:
            host_removed = host.evict_overcommitted_pods(seed=seed)
            for cname, cnt in host_removed.items():
                removed_counts[cname] += cnt

    return removed_counts


def raise_cpu_limits(cluster: Cluster, seed: int = 42) -> Dict[str, int]:
    print("Raising data node CPU limits based on disk configuration...")
    cfg = cluster.config
    removed_counts = {name: 0 for name in cfg.pod_configurations}

    modified_hosts = []
    for host in cluster.hosts.values():
        host_modified = False
        for pod in host.pods:
            if pod.yt_role == 'ytdatnode':
                hdd_count = sum(1 for disk_type in pod.disk_types if disk_type == 'HDD')
                ssd_count = sum(1 for disk_type in pod.disk_types if disk_type == 'SSD')
                nvme_count = sum(1 for disk_type in pod.disk_types if disk_type == 'NVME')
                pod.vcpu_guarantee = int((1 + hdd_count * 0.5 + ssd_count * 1.5 + nvme_count * 6) * 1000)
                host_modified = True
        if host_modified:
            modified_hosts.append(host)

    for host in modified_hosts:
        used = host.get_used_resources_from_pods()
        cpu_excess = used[0] - host.cpu_total_vcores
        if cpu_excess > 0.001:
            host_removed = host.evict_overcommitted_pods(seed=seed)
            for cname, cnt in host_removed.items():
                removed_counts[cname] += cnt

    return removed_counts


def apply_ilp_placement(
    cluster: Cluster,
    placement: Dict[str, Dict[str, List[Optional[int]]]],
) -> int:
    counter = 0
    total_placed = 0
    for hostname, configs_placed in placement.items():
        host = cluster.get_host(hostname)
        if host is None:
            continue
        for config_name, numa_ids in configs_placed.items():
            for numa_node_id in numa_ids:
                pod = create_pod_instance(
                    config_name, counter, host.disk_storage_class, cluster.config.pod_configurations, numa_node_id
                )
                cluster.add_pod_to_host(hostname, pod)
                counter += 1
                total_placed += 1
    return total_placed


# ---------------------------------------------------------------------------
# Resource utilization stats
# ---------------------------------------------------------------------------

UTILIZATION_ROLES = ('yttabnode', 'ytrpcproxy', 'ytdatnode', 'ytexenode')

# ytexenode is a sink pod: it can be shrunk down to the minimal sink size,
# so only that minimum is counted as really used.
SINK_UTILIZATION_ROLES = ('ytexenode',)


def _pod_utilization_resources(pod, config: ClusterConfig) -> Tuple[int, int, int]:
    if pod.yt_role not in SINK_UTILIZATION_ROLES:
        return pod.vcpu_guarantee, pod.memory_guarantee, pod.network_guarantee
    return (
        min(pod.vcpu_guarantee, config.min_sink_vcpu),
        min(pod.memory_guarantee, config.min_sink_memory_mib * 1024**2),
        min(pod.network_guarantee, config.min_sink_network),
    )


def calculate_cluster_resource_utilization(cluster: Cluster) -> dict:
    active_hosts = cluster.get_active_hosts()
    total_cpu = sum(h.cpu_total_vcores for h in active_hosts) * 1000
    total_mem = sum(h.memory_total_gib for h in active_hosts) * 1024**3
    total_net = sum(h.network_total_bandwidth_mib for h in active_hosts) * 1024**2
    total_nodes = sum(len(h.get_pods_by_role('yttabnode')) for h in active_hosts)
    total_proxies = sum(len(h.get_pods_by_role('ytrpcproxy')) for h in active_hosts)

    used_by_role = {
        role: {'count': 0, 'used_cpu': 0, 'used_memory': 0, 'used_network': 0} for role in UTILIZATION_ROLES
    }

    used_cpu = used_mem = used_net = 0
    for host in active_hosts:
        for pod in host.pods:
            if not pod.yt_sink_pod:
                used_cpu += pod.vcpu_guarantee
                used_mem += pod.memory_guarantee
                used_net += pod.network_guarantee

            if pod.yt_role in used_by_role:
                pod_cpu, pod_mem, pod_net = _pod_utilization_resources(pod, cluster.config)
                role_usage = used_by_role[pod.yt_role]
                role_usage['count'] += 1
                role_usage['used_cpu'] += pod_cpu
                role_usage['used_memory'] += pod_mem
                role_usage['used_network'] += pod_net

    infra_cpu = infra_mem = 0
    for host in active_hosts:
        h_cpu, h_mem = host.get_infra_tax()
        infra_cpu += h_cpu
        infra_mem += h_mem

    return {
        'total_nodes': total_nodes,
        'total_proxies': total_proxies,
        'total_cpu': total_cpu,
        'total_memory': total_mem,
        'total_network': total_net,
        'used_cpu': used_cpu,
        'used_memory': used_mem,
        'used_network': used_net,
        'used_by_role': used_by_role,
        'infra_cpu': infra_cpu,
        'infra_memory': infra_mem,
        'cpu_utilization': (used_cpu / total_cpu * 100) if total_cpu > 0 else 0,
        'memory_utilization': (used_mem / total_mem * 100) if total_mem > 0 else 0,
        'network_utilization': (used_net / total_net * 100) if total_net > 0 else 0,
    }


def calculate_sink_pods_utilization(cluster: Cluster) -> dict:
    """Resource usage of sink pods only."""
    active_hosts = cluster.get_active_hosts()

    total_cpu = sum(h.cpu_total_vcores for h in active_hosts) * 1000
    total_mem = sum(h.memory_total_gib for h in active_hosts) * 1024**3
    total_net = sum(h.network_total_bandwidth_mib for h in active_hosts) * 1024**2

    used_cpu = used_mem = used_net = 0
    sink_pods_count = 0
    for host in active_hosts:
        for pod in host.pods:
            if pod.yt_sink_pod:
                used_cpu += pod.vcpu_guarantee
                used_mem += pod.memory_guarantee
                used_net += pod.network_guarantee
                sink_pods_count += 1

    return {
        'total_cpu': total_cpu,
        'total_memory': total_mem,
        'total_network': total_net,
        'used_cpu': used_cpu,
        'used_memory': used_mem,
        'used_network': used_net,
        'sink_pods_count': sink_pods_count,
        'cpu_utilization': (used_cpu / total_cpu * 100) if total_cpu > 0 else 0,
        'memory_utilization': (used_mem / total_mem * 100) if total_mem > 0 else 0,
        'network_utilization': (used_net / total_net * 100) if total_net > 0 else 0,
    }
