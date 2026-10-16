"""
Pod Placement Mapping with Rack Awareness.

After ILP places synthetic pods on hosts, this module maps real pods
(from pods_to_remove) to the synthetic pod positions, distributing
them across racks and hosts using a deficit-based algorithm.
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from .cluster import PodToRemove

if TYPE_CHECKING:
    from .scripts.shared import Cluster


_SHORT_CLUSTER_NAMES = {
    'seneca-vla': 'sen-v',
    'seneca-klg': 'sen-k',
    'seneca-sas': 'sen-s',
}

# Matches pod_id format: {host_pt1}-{host_pt2}-{short_bundle_name}-{3hex}-(tab|rpc)-{cluster}
# Group 1: short_bundle_name, Group 2: identifier (3 hex chars).
_POD_ID_RE = re.compile(r'^[^-]+-[^-]+-(.+)-([0-9a-f]{3})-(?:tab|rpc)-.+$')


def _extract_short_bundle_name(pod_id: str) -> Optional[str]:
    m = _POD_ID_RE.match(pod_id)
    return m.group(1) if m else None


@dataclass
class TargetLocation:
    hostname: str
    numa_node_id: Optional[int]
    rack: str
    yt_pod_name: str


@dataclass
class PodPlacement:
    pod_id: str
    hostname: str
    numa_node_id: Optional[int]
    rack: str
    config_name: str
    yt_role: str


@dataclass
class PodWithTargetLocation(PodToRemove):
    target_location: Optional[TargetLocation] = None

    def __post_init__(self):
        super().__post_init__()


def collect_synthetic_pods_placement(cluster: 'Cluster') -> Dict[str, List[PodPlacement]]:
    """Collect placement info for all synthetic pods in the cluster, grouped by role."""
    placements_by_role: Dict[str, List[PodPlacement]] = {}

    for hostname, host in cluster.hosts.items():
        for pod in host.pods:
            if not pod.pod_id.startswith('synthetic_'):
                continue
            if not host.rack:
                raise ValueError(f"Host {hostname} has no rack information")

            parts = pod.pod_id.split('_')
            if len(parts) < 3:
                continue
            config_name = '_'.join(parts[1:-1])

            placement = PodPlacement(
                pod_id=pod.pod_id,
                hostname=hostname,
                numa_node_id=pod.numa_node_id,
                rack=host.rack,
                config_name=config_name,
                yt_role=pod.yt_role,
            )
            placements_by_role.setdefault(pod.yt_role, []).append(placement)

    return placements_by_role


def _distribute_pods_within_rack_by_deficit(
    pods_by_bundle: Dict[str, List[PodToRemove]],
    synthetic_pods_in_rack: List[PodPlacement],
    bundle_names: List[str],
) -> Dict[str, List[Tuple[PodToRemove, PodPlacement]]]:
    """Distribute pods within a single rack across hosts using max-deficit algorithm."""
    pods_by_host: Dict[str, List[PodPlacement]] = {}
    for pod in synthetic_pods_in_rack:
        pods_by_host.setdefault(pod.hostname, []).append(pod)

    slots_by_host = {host: len(pods) for host, pods in pods_by_host.items()}
    total_slots = sum(slots_by_host.values())

    target_quota: Dict[str, Dict[str, float]] = {}
    for bundle_name in bundle_names:
        if bundle_name not in pods_by_bundle:
            continue
        bundle_pod_count = len(pods_by_bundle[bundle_name])
        target_quota[bundle_name] = {
            host: bundle_pod_count * slots / total_slots for host, slots in slots_by_host.items()
        }

    placed_count = {bundle: {host: 0 for host in slots_by_host} for bundle in target_quota}
    remaining_pods = {bundle: list(pods_by_bundle[bundle]) for bundle in target_quota}
    remaining_slots = slots_by_host.copy()
    host_indices = {host: 0 for host in slots_by_host}
    result: Dict[str, List[Tuple[PodToRemove, PodPlacement]]] = {bundle: [] for bundle in target_quota}

    while any(remaining_pods.values()):
        max_deficit = -float('inf')
        best_bundle = None
        best_host = None

        for bundle_name in target_quota:
            if not remaining_pods[bundle_name]:
                continue
            for host in slots_by_host:
                if remaining_slots[host] > 0:
                    deficit = target_quota[bundle_name][host] - placed_count[bundle_name][host]
                    if deficit > max_deficit:
                        max_deficit = deficit
                        best_bundle = bundle_name
                        best_host = host

        if best_bundle is None:
            break

        real_pod = remaining_pods[best_bundle].pop(0)
        synthetic_pod = pods_by_host[best_host][host_indices[best_host]]
        result[best_bundle].append((real_pod, synthetic_pod))

        placed_count[best_bundle][best_host] += 1
        remaining_slots[best_host] -= 1
        host_indices[best_host] += 1

    return result


def _distribute_pods_globally_by_deficit(
    pods_by_bundle: Dict[str, List[PodToRemove]],
    synthetic_pods_by_rack: Dict[str, List[PodPlacement]],
    bundle_names: List[str],
) -> Dict[str, List[Tuple[PodToRemove, PodPlacement]]]:
    """Distribute pods across racks then within racks using max-deficit algorithm."""
    rack_capacity = {rack: len(pods) for rack, pods in synthetic_pods_by_rack.items()}
    total_capacity = sum(rack_capacity.values())

    target_quota: Dict[str, Dict[str, float]] = {}
    for bundle_name in bundle_names:
        if bundle_name not in pods_by_bundle:
            continue
        bundle_pod_count = len(pods_by_bundle[bundle_name])
        target_quota[bundle_name] = {
            rack: bundle_pod_count * capacity / total_capacity for rack, capacity in rack_capacity.items()
        }

    placed_count_by_rack = {bundle: {rack: 0 for rack in rack_capacity} for bundle in target_quota}
    remaining_pods = {bundle: list(pods_by_bundle[bundle]) for bundle in target_quota}
    remaining_capacity = rack_capacity.copy()
    pods_to_place_in_rack = {rack: {bundle: [] for bundle in target_quota} for rack in rack_capacity}

    while any(remaining_pods.values()):
        max_deficit = -float('inf')
        best_bundle = None
        best_rack = None

        for bundle_name in target_quota:
            if not remaining_pods[bundle_name]:
                continue
            for rack in rack_capacity:
                if remaining_capacity[rack] > 0:
                    deficit = target_quota[bundle_name][rack] - placed_count_by_rack[bundle_name][rack]
                    if deficit > max_deficit:
                        max_deficit = deficit
                        best_bundle = bundle_name
                        best_rack = rack

        if best_bundle is None:
            break

        real_pod = remaining_pods[best_bundle].pop(0)
        pods_to_place_in_rack[best_rack][best_bundle].append(real_pod)
        placed_count_by_rack[best_bundle][best_rack] += 1
        remaining_capacity[best_rack] -= 1

    final_result: Dict[str, List[Tuple[PodToRemove, PodPlacement]]] = {bundle: [] for bundle in target_quota}

    for rack, pods_by_bundle_in_rack in pods_to_place_in_rack.items():
        bundles_in_rack = [b for b in bundle_names if pods_by_bundle_in_rack.get(b)]
        if not bundles_in_rack:
            continue
        rack_result = _distribute_pods_within_rack_by_deficit(
            pods_by_bundle=pods_by_bundle_in_rack,
            synthetic_pods_in_rack=synthetic_pods_by_rack[rack],
            bundle_names=bundles_in_rack,
        )
        for bundle_name, placements in rack_result.items():
            final_result[bundle_name].extend(placements)

    return final_result


def _distribute_pods_across_racks(
    real_pods: List[PodToRemove],
    synthetic_pods_by_config_and_rack: Dict[str, Dict[str, List[PodPlacement]]],
    role_name: str,
) -> List[PodWithTargetLocation]:
    """Match real pods to synthetic pod placements using rack-aware deficit distribution."""
    real_pods_by_config: Dict[str, List[PodToRemove]] = {}
    for pod in real_pods:
        real_pods_by_config.setdefault(pod.config_name, []).append(pod)

    enriched_pods: List[PodWithTargetLocation] = []

    for config, pods in real_pods_by_config.items():
        if config not in synthetic_pods_by_config_and_rack:
            raise RuntimeError(f"No synthetic {role_name} with configuration '{config}' available")

        racks_for_config = synthetic_pods_by_config_and_rack[config]

        pods_by_bundle: Dict[str, List[PodToRemove]] = {}
        for pod in pods:
            bundle_name = pod.bundle_controller_annotations.get('allocated_for_bundle', 'unknown')
            pods_by_bundle.setdefault(bundle_name, []).append(pod)

        bundle_names = sorted(pods_by_bundle.keys())

        placement_result = _distribute_pods_globally_by_deficit(
            pods_by_bundle=pods_by_bundle,
            synthetic_pods_by_rack=racks_for_config,
            bundle_names=bundle_names,
        )

        for bundle_name, placements in placement_result.items():
            for real_pod, synthetic_pod in placements:
                enriched_pods.append(
                    PodWithTargetLocation(
                        pod_id=real_pod.pod_id,
                        yt_pod_name=real_pod.yt_pod_name,
                        bundle_controller_annotations=real_pod.bundle_controller_annotations.copy(),
                        user_tags=real_pod.user_tags.copy(),
                        decommissioned=False,
                        proxy_role=real_pod.proxy_role,
                        config_name=real_pod.config_name,
                        human_config_name=real_pod.human_config_name,
                        target_location=TargetLocation(
                            hostname=synthetic_pod.hostname,
                            numa_node_id=synthetic_pod.numa_node_id,
                            rack=synthetic_pod.rack,
                            yt_pod_name=synthetic_pod.hostname[:9] + real_pod.yt_pod_name[9:],
                        ),
                    )
                )

    return enriched_pods


def _update_stub_pod_names(
    enriched_pods: List[PodWithTargetLocation],
    role: str,
    port: int,
    cluster_name: str,
    dc: str,
) -> None:
    """Update pod_id/yt_pod_name for stub pods (pod_id starts with 'new_') after host is assigned.

    Stub pods get names in the same format as real pods:
      tabnode:  {short_hostname}-{short_bundle_name}-{identifier:03x}-tab-{short_cluster}
      rpcproxy: {short_hostname}-{short_bundle_name}-{identifier:03x}-rpc-{short_cluster}
    with yt_pod_name = {pod_id}.{dc}.yp-c.yandex.net:{port}

    short_bundle_name is extracted from an existing (non-stub) pod of the same bundle.
    identifier is a zero-padded 3-digit hex counter unique per bundle.
    """
    short_cluster = _SHORT_CLUSTER_NAMES.get(cluster_name, cluster_name)

    # From existing (non-stub) pods: collect short_bundle_name and used identifiers per bundle.
    short_bundle_name_by_bundle: Dict[str, str] = {}
    used_identifiers: Dict[str, set] = {}
    for pod in enriched_pods:
        if pod.pod_id.startswith('new_'):
            continue
        bundle = pod.bundle_controller_annotations.get('allocated_for_bundle', '')
        if not bundle:
            continue
        m = _POD_ID_RE.match(pod.pod_id)
        if m:
            if bundle not in short_bundle_name_by_bundle:
                short_bundle_name_by_bundle[bundle] = m.group(1)
            used_identifiers.setdefault(bundle, set()).add(m.group(2))

    def _next_identifier(bundle_name: str) -> str:
        used = used_identifiers.setdefault(bundle_name, set())
        for i in range(0x1000):
            candidate = format(i, '03x')
            if candidate not in used:
                used.add(candidate)
                return candidate
        raise RuntimeError(f"Exhausted all 3-hex identifiers for bundle '{bundle_name}'")

    for pod in enriched_pods:
        if not pod.pod_id.startswith('new_'):
            continue
        if pod.target_location is None:
            continue

        hostname = pod.target_location.hostname
        parts = hostname.split('.')
        short_hostname = parts[0]

        bundle_name = pod.bundle_controller_annotations.get('allocated_for_bundle', 'unknown')
        short_bundle_name = short_bundle_name_by_bundle.get(bundle_name, bundle_name)
        identifier = _next_identifier(bundle_name)

        pod.pod_id = f"{short_hostname}-{short_bundle_name}-{identifier}-{role}-{short_cluster}"
        pod.yt_pod_name = f"{pod.pod_id}.{dc}.yp-c.yandex.net:{port}"
        pod.target_location.yt_pod_name = pod.yt_pod_name


def map_pods_with_rack_awareness(
    tabnodes_to_create: List[PodToRemove],
    rpcproxies_to_create: List[PodToRemove],
    synthetic_placements: Dict[str, List[PodPlacement]],
    cluster_name,
    dc,
) -> Tuple[List[PodWithTargetLocation], List[PodWithTargetLocation]]:
    """
    Map real/stub pods to synthetic placement positions with rack awareness.

    Returns:
        (enriched_tabnodes, enriched_rpcproxies) — lists with target_location filled in.
    """
    print("\n=== MAPPING PODS TO SYNTHETIC PLACEMENTS WITH RACK AWARENESS ===\n")

    synthetic_tabnodes = synthetic_placements.get('yttabnode', [])
    synthetic_rpcproxies = synthetic_placements.get('ytrpcproxy', [])

    print("Available synthetic pods:")
    print(f"  Tablet nodes: {len(synthetic_tabnodes)}")
    print(f"  RPC proxies:  {len(synthetic_rpcproxies)}")

    def _group_by_config_and_rack(placements: List[PodPlacement]):
        result: Dict[str, Dict[str, List[PodPlacement]]] = {}
        for p in placements:
            result.setdefault(p.config_name, {}).setdefault(p.rack, []).append(p)
        return result

    synthetic_tabnodes_by_config_and_rack = _group_by_config_and_rack(synthetic_tabnodes)
    synthetic_rpcproxies_by_config_and_rack = _group_by_config_and_rack(synthetic_rpcproxies)

    # Print config/rack distribution
    print("\nSynthetic pods grouped by configuration:")
    print("  Tablet nodes:")
    for config, racks in sorted(synthetic_tabnodes_by_config_and_rack.items()):
        total = sum(len(v) for v in racks.values())
        print(f"    {config}: {total} pods across {len(racks)} racks")
    print("  RPC proxies:")
    for config, racks in sorted(synthetic_rpcproxies_by_config_and_rack.items()):
        total = sum(len(v) for v in racks.values())
        print(f"    {config}: {total} pods across {len(racks)} racks")

    # Distribute tablet nodes
    enriched_tabnodes: List[PodWithTargetLocation] = []
    if tabnodes_to_create:
        print("\n=== DISTRIBUTING TABLET NODES GLOBALLY ===")
        pods_by_bundle: Dict[str, int] = {}
        for pod in tabnodes_to_create:
            bundle = pod.bundle_controller_annotations.get('allocated_for_bundle', 'unknown')
            pods_by_bundle[bundle] = pods_by_bundle.get(bundle, 0) + 1
        for bundle, count in sorted(pods_by_bundle.items()):
            print(f"  Bundle '{bundle}': {count} tablet nodes")
        print(f"Total tablet nodes to distribute: {len(tabnodes_to_create)}")

        enriched_tabnodes = _distribute_pods_across_racks(
            real_pods=tabnodes_to_create,
            synthetic_pods_by_config_and_rack=synthetic_tabnodes_by_config_and_rack,
            role_name="tablet nodes",
        )
        _update_stub_pod_names(enriched_tabnodes, role='tab', port=9022, cluster_name=cluster_name, dc=dc)
        print(f"\nDistributed {len(enriched_tabnodes)} tablet nodes")
        _print_rack_distribution(enriched_tabnodes, "Tablet nodes")

    # Distribute RPC proxies
    enriched_rpcproxies: List[PodWithTargetLocation] = []
    if rpcproxies_to_create:
        print("\n=== DISTRIBUTING RPC PROXIES GLOBALLY ===")
        pods_by_bundle = {}
        for pod in rpcproxies_to_create:
            bundle = pod.bundle_controller_annotations.get('allocated_for_bundle', 'unknown')
            pods_by_bundle[bundle] = pods_by_bundle.get(bundle, 0) + 1
        for bundle, count in sorted(pods_by_bundle.items()):
            print(f"  Bundle '{bundle}': {count} RPC proxies")
        print(f"Total RPC proxies to distribute: {len(rpcproxies_to_create)}")

        enriched_rpcproxies = _distribute_pods_across_racks(
            real_pods=rpcproxies_to_create,
            synthetic_pods_by_config_and_rack=synthetic_rpcproxies_by_config_and_rack,
            role_name="RPC proxies",
        )
        _update_stub_pod_names(enriched_rpcproxies, role='rpc', port=9013, cluster_name=cluster_name, dc=dc)
        print(f"\nDistributed {len(enriched_rpcproxies)} RPC proxies")
        _print_rack_distribution(enriched_rpcproxies, "RPC proxies")

    print("\n=== MAPPING COMPLETE ===\n")
    return enriched_tabnodes, enriched_rpcproxies


def _print_rack_distribution(pods: List[PodWithTargetLocation], label: str) -> None:
    pods_by_rack_host: Dict[str, Dict[str, List]] = {}
    for pod in pods:
        if pod.target_location is None:
            continue
        rack = pod.target_location.rack
        host = pod.target_location.hostname
        pods_by_rack_host.setdefault(rack, {}).setdefault(host, []).append(pod)

    bundle_racks: Dict[str, Dict[str, int]] = {}
    for pod in pods:
        if pod.target_location is None:
            continue
        bundle = pod.bundle_controller_annotations.get('allocated_for_bundle', 'unknown')
        rack = pod.target_location.rack
        bundle_racks.setdefault(bundle, {})
        bundle_racks[bundle][rack] = bundle_racks[bundle].get(rack, 0) + 1

    for bundle_name in sorted(bundle_racks):
        rack_counts = bundle_racks[bundle_name]
        total = sum(rack_counts.values())
        print(f"  Bundle '{bundle_name}': {total} {label.lower()} across {len(rack_counts)} racks")
        for rack in sorted(rack_counts):
            print(f"    {rack}: {rack_counts[rack]} pods")
