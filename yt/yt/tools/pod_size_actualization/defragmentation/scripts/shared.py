"""
Core cluster data model for defragmentation.

Used by BOTH the main process (hermetic binary) and the ILP solver subprocess.
Must only import stdlib (no pandas, no pulp).

Subprocess (solver.py) imports:
    Cluster, ClusterConfig, AvailableResourcesRequest, Host,
    filter_host, get_disk_bandwidth_from_storage_class
"""

import dataclasses
import json
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# ClusterConfig — all cluster-specific constants in one place
# ---------------------------------------------------------------------------


@dataclass
class ClusterConfig:
    """Cluster-specific configuration, passed through the whole pipeline."""

    dc: str  # data center
    pod_configurations: Dict[str, dict]  # name -> {vcpu, memory, network, disk_capacity, yt_role, priority}
    antiaffinity: Dict[str, int]  # yt_role -> max pods per host
    role_specific_host_filter: Dict[str, dict]
    yt_proxy: str  # YT cluster to query
    validate_bundles: bool  # compare pods with bundle controller config in Cypress

    # Sink pod constraints
    min_sink_vcpu: int = 4000  # millicores
    min_sink_memory_mib: int = 8192  # MiB
    min_sink_network: int = 0  # bytes/s
    min_sink_disk_capacity: int = 0  # bytes
    min_sink_disk_bandwidth: int = 0  # bytes/s

    # Per-host resource reserves (subtracted from effective free resources)
    memory_reserve_mib: int = 0  # MiB
    vcpu_reserve: int = 0  # millicores
    network_reserve: int = 0  # bytes/s
    disk_capacity_reserve: int = 0  # bytes
    disk_bandwidth_reserve: int = 0  # bytes/s

    # Infrastructure tax (reserved for system/OS, not available to pods).
    # Applied as max(total * rel_tax, abs_tax * numa_count).
    # abs_tax is per NUMA node; rel_tax is a fraction of total host resources.
    use_new_infra_tax: bool = False

    infra_cpu_rel_tax: float = 0.0  # fraction of total vCPU (0.0–1.0)
    infra_cpu_abs_tax: int = 2000  # millicores per NUMA node
    infra_memory_rel_tax: float = 0.1  # fraction of total memory (0.0–1.0)
    infra_memory_abs_tax: int = 0  # MiB per NUMA node

    # Network guarantee adjustments
    raise_network_limits: bool = False
    data_node_network_guarantee: int = 600  # MiB/s
    timestamp_provider_network_guarantee: int = 25  # MiB/s
    master_cache_network_guarantee: int = 500  # MiB/s

    raise_cpu_limits: bool = False

    update_custom_pods: bool = False

    # Placement settings
    random_seed: int = 42

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'ClusterConfig':
        return cls(**d)


# ---------------------------------------------------------------------------
# Resource dataclasses
# ---------------------------------------------------------------------------


@dataclass
class NumaSlotResources:
    vcpu: int  # millicores
    ram: int  # MiB


@dataclass
class PodResources:
    pod_id: str
    yt_sink_pod: bool
    vcpu_guarantee: int  # millicores
    memory_guarantee: int  # bytes
    network_guarantee: int  # bytes/s
    disk_capacity: int  # bytes
    disk_bandwidth_guarantee: int  # bytes/s
    numa_node_id: Optional[int]
    yt_role: str
    yt_pod_name: str = ''
    yt_bundle_controller_annotations: Dict = field(default_factory=dict)
    yt_decommissioned: bool = False
    yt_proxy_role: Optional[str] = None
    yt_user_tags: List[str] = field(default_factory=list)
    yt_tags: List[str] = field(default_factory=list)
    yt_state: str = ''
    yt_alive: bool = False
    disk_types: List[str] = field(default_factory=list)


@dataclass
class AvailableResourcesRequest:
    vcpu: int  # millicores
    memory: int  # bytes
    net: int = 0  # bytes/s
    disk_capacity: int = 0  # bytes
    antiaffinity: Optional[int] = None
    yt_role: Optional[str] = None
    numa_enabled: bool = False


# ---------------------------------------------------------------------------
# Host
# ---------------------------------------------------------------------------


class Host:
    def __init__(self, hostname: str, host_data: dict, config: 'ClusterConfig'):
        self.hostname = hostname

        self.cpu_model = host_data.get('cpu_model', '')
        self.cpu_total_vcores = host_data.get('cpu_total_vcores', 0)
        self.cpu_used_vcores = host_data.get('cpu_used_vcores', 0)
        self.cpu_free_vcores = host_data.get('cpu_free_vcores', 0)

        self.memory_total_gib = host_data.get('memory_total_gib', 0)
        self.memory_used_gib = host_data.get('memory_used_gib', 0)
        self.memory_free_gib = host_data.get('memory_free_gib', 0)

        self.network_total_bandwidth_mib = host_data.get('network_total_bandwidth_mib', 0)
        self.network_used_bandwidth_mib = host_data.get('network_used_bandwidth_mib', 0)
        self.network_free_bandwidth_mib = host_data.get('network_free_bandwidth_mib', 0)

        self.disk_storage_class = host_data.get('disk_storage_class', '')
        self.disk_total_capacity_gib = host_data.get('disk_total_capacity_gib', 0)
        self.disk_used_capacity_gib = host_data.get('disk_used_capacity_gib', 0)
        self.disk_free_capacity_gib = host_data.get('disk_free_capacity_gib', 0)
        self.disk_total_bandwidth_mib = host_data.get('disk_total_bandwidth_mib', 0)
        self.disk_used_bandwidth_mib = host_data.get('disk_used_bandwidth_mib', 0)
        self.disk_free_bandwidth_mib = host_data.get('disk_free_bandwidth_mib', 0)

        self.walle_state = host_data.get('walle_state', '')
        self.walle_status = host_data.get('walle_status', '')
        self.walle_project = host_data.get('walle_project', '')
        self.rack = host_data.get('rack', '')

        # NUMA info — stored as JSON string in CSV, but may arrive as list if deserializing from dict
        raw_cpu = host_data.get('numa_cpu_details', '[]')
        raw_mem = host_data.get('numa_memory_details', '[]')
        self.numa_cpu_details = self._parse_numa_details(raw_cpu)
        self.numa_memory_details = self._parse_numa_details(raw_mem)

        # NUMA data is raw (no infra tax), so recalculate host totals from it when available.
        # This removes the infra-tax-adjusted values from the dataframe and lets infra tax
        # be applied explicitly via ClusterConfig fields.
        if config.use_new_infra_tax:
            if self.numa_cpu_details:
                self.cpu_total_vcores = sum(node['total_vcores'] for node in self.numa_cpu_details)
            if self.numa_memory_details:
                self.memory_total_gib = sum(node['total_gib'] for node in self.numa_memory_details)

        self.config = config
        self.pods: List[PodResources] = []

    def _parse_numa_details(self, numa_json) -> List[dict]:
        if isinstance(numa_json, list):
            return numa_json
        try:
            return json.loads(numa_json) if numa_json and numa_json != '[]' else []
        except (json.JSONDecodeError, TypeError):
            return []

    def update_config(self, new_config: ClusterConfig):
        assert new_config.use_new_infra_tax == self.config.use_new_infra_tax, (
            f"Cannot change use_new_infra_tax after Host construction "
            f"(host {self.hostname}: {self.config.use_new_infra_tax} -> {new_config.use_new_infra_tax})"
        )
        self.config = new_config

    # --- Pod management ---

    def add_pod(self, pod: PodResources):
        self.pods.append(pod)
        try:
            self._ensure_resource_constraints()
        except RuntimeError:
            self.pods.pop()
            raise

    def _ensure_resource_constraints(self):
        used = self.get_used_resources_from_pods()
        cpu_excess = used[0] - self.cpu_total_vcores
        memory_excess = used[1] - self.memory_total_gib
        network_excess = used[2] - self.network_total_bandwidth_mib
        disk_capacity_excess = used[3] - self.disk_total_capacity_gib
        disk_bandwidth_excess = used[4] - self.disk_total_bandwidth_mib

        numa_excess = self.get_numa_excess()
        if numa_excess:
            raise RuntimeError(f"NUMA resource validation failed on host {self.hostname}: {numa_excess}")

        if (
            cpu_excess <= 0
            and memory_excess <= 0
            and network_excess <= 0
            and disk_capacity_excess <= 0
            and disk_bandwidth_excess <= 0
        ):
            return

        sink_pods = self.get_sink_pods()
        if not sink_pods:
            raise RuntimeError(
                f"RESOURCE VIOLATION: Host {self.hostname} resources exceeded "
                f"(CPU: +{cpu_excess:.2f}v, Memory: +{memory_excess:.2f}GiB, "
                f"Network: +{network_excess:.2f}MiB/s) and no sink pods available"
            )

        sink_pod = sink_pods[0]
        min_sink_vcpu = self.config.min_sink_vcpu
        min_sink_memory = self.config.min_sink_memory_mib * 1024 * 1024
        min_sink_network = self.config.min_sink_network

        if cpu_excess > 0:
            cpu_can_free = max(0, sink_pod.vcpu_guarantee - min_sink_vcpu)
            cpu_to_free = min(cpu_excess * 1000, cpu_can_free)
            if cpu_to_free > 0:
                sink_pod.vcpu_guarantee -= int(cpu_to_free)

        if memory_excess > 0:
            memory_can_free = max(0, sink_pod.memory_guarantee - min_sink_memory)
            memory_to_free = min(memory_excess * 1024**3, memory_can_free)
            if memory_to_free > 0:
                sink_pod.memory_guarantee -= int(memory_to_free)

        if network_excess > 0:
            network_can_free = max(0, sink_pod.network_guarantee - min_sink_network)
            network_to_free = min(network_excess * 1024**2, network_can_free)
            if network_to_free > 0:
                sink_pod.network_guarantee -= int(network_to_free)

        # Re-check
        used = self.get_used_resources_from_pods()
        final_cpu_excess = used[0] - self.cpu_total_vcores
        final_memory_excess = used[1] - self.memory_total_gib
        final_network_excess = used[2] - self.network_total_bandwidth_mib
        final_disk_capacity_excess = used[3] - self.disk_total_capacity_gib
        final_disk_bandwidth_excess = used[4] - self.disk_total_bandwidth_mib

        if (
            final_cpu_excess > 0.01
            or final_memory_excess > 0.01
            or final_network_excess > 1.0
            or final_disk_capacity_excess > 0.01
            or final_disk_bandwidth_excess > 1.0
        ):
            raise RuntimeError(
                f"RESOURCE VIOLATION: Even after shrinking sink pod, host {self.hostname} "
                f"resources still exceeded (CPU: +{final_cpu_excess:.2f}v, "
                f"Memory: +{final_memory_excess:.2f}GiB, Network: +{final_network_excess:.2f}MiB/s, "
                f"Disk capacity: +{final_disk_capacity_excess:.2f}GiB, "
                f"Disk bandwidth: +{final_disk_bandwidth_excess:.2f}MiB/s)"
            )

    def remove_pod(self, pod_id: str) -> bool:
        for i, pod in enumerate(self.pods):
            if pod.pod_id == pod_id:
                self.pods.pop(i)
                return True
        return False

    def get_pod_by_id(self, pod_id: str) -> Optional[PodResources]:
        for pod in self.pods:
            if pod.pod_id == pod_id:
                return pod
        return None

    def get_pods_by_role(self, role: str) -> List[PodResources]:
        return [pod for pod in self.pods if pod.yt_role == role]

    def get_sink_pods(self) -> List[PodResources]:
        return [pod for pod in self.pods if pod.yt_sink_pod]

    def get_used_resources_from_pods(self) -> Tuple[float, float, float, float, float]:
        """Returns (cpu_vcores, memory_gib, net_mib_per_sec, disk_cap_gib, disk_bw_mib_per_sec)."""
        total_cpu_mc = sum(pod.vcpu_guarantee for pod in self.pods)
        total_mem_b = sum(pod.memory_guarantee for pod in self.pods)
        total_net_bps = sum(pod.network_guarantee for pod in self.pods)
        total_disk_cap_b = sum(pod.disk_capacity for pod in self.pods)
        total_disk_bw_bps = sum(pod.disk_bandwidth_guarantee for pod in self.pods)
        return (
            total_cpu_mc / 1000,
            total_mem_b / (1024**3),
            total_net_bps / (1024**2),
            total_disk_cap_b / (1024**3),
            total_disk_bw_bps / (1024**2),
        )

    def get_free_resources_from_pods(self) -> Tuple[float, float, float, float, float]:
        used = self.get_used_resources_from_pods()
        return (
            self.cpu_total_vcores - used[0],
            self.memory_total_gib - used[1],
            self.network_total_bandwidth_mib - used[2],
            self.disk_total_capacity_gib - used[3],
            self.disk_total_bandwidth_mib - used[4],
        )

    def get_sink_resources_usage(self) -> Tuple[int, int, int, int, int]:
        """Returns (vcpu_mc, memory_b, net_bps, disk_cap_b, disk_bw_bps)."""
        sink_pods = self.get_sink_pods()
        return (
            sum(p.vcpu_guarantee for p in sink_pods),
            sum(p.memory_guarantee for p in sink_pods),
            sum(p.network_guarantee for p in sink_pods),
            sum(p.disk_capacity for p in sink_pods),
            sum(p.disk_bandwidth_guarantee for p in sink_pods),
        )

    def get_infra_tax(self) -> Tuple[int, int]:
        """Returns (infra_cpu_mc, infra_memory_bytes) reserved for infrastructure on this host."""
        if not self.config.use_new_infra_tax:
            return 0, 0

        numa_count = max(len(self.numa_cpu_details), len(self.numa_memory_details), 1)
        infra_cpu = max(
            int(self.cpu_total_vcores * 1000 * self.config.infra_cpu_rel_tax),
            self.config.infra_cpu_abs_tax * numa_count,
        )
        infra_memory = max(
            int(self.memory_total_gib * 1024**3 * self.config.infra_memory_rel_tax),
            self.config.infra_memory_abs_tax * numa_count * 1024 * 1024,
        )
        return infra_cpu, infra_memory

    def get_effective_free_resources(self) -> Tuple[int, int, int, int, int]:
        """Effective free resources in raw units: (cpu_mc, memory_b, net_bps, disk_cap_b, disk_bw_bps)."""
        free = self.get_free_resources_from_pods()
        sink = self.get_sink_resources_usage()

        min_sink_vcpu = self.config.min_sink_vcpu
        min_sink_memory = self.config.min_sink_memory_mib * 1024 * 1024
        min_sink_network = self.config.min_sink_network

        effective_cpu = int(free[0] * 1000) - self.config.vcpu_reserve
        sink_cpu_diff = sink[0] - min_sink_vcpu
        if sink_cpu_diff > 0:
            effective_cpu += sink_cpu_diff

        effective_memory = int(free[1] * 1024**3) - self.config.memory_reserve_mib * 1024 * 1024
        sink_mem_diff = sink[1] - min_sink_memory
        if sink_mem_diff > 0:
            effective_memory += sink_mem_diff

        effective_network = int(free[2] * 1024**2) - self.config.network_reserve
        sink_net_diff = sink[2] - min_sink_network
        if sink_net_diff > 0:
            effective_network += sink_net_diff

        effective_disk_cap = int(free[3] * 1024**3) - self.config.disk_capacity_reserve
        effective_disk_bw = int(free[4] * 1024**2) - self.config.disk_bandwidth_reserve

        infra_cpu, infra_memory = self.get_infra_tax()
        effective_cpu -= infra_cpu
        effective_memory -= infra_memory

        return (effective_cpu, effective_memory, effective_network, effective_disk_cap, effective_disk_bw)

    def is_alive(self) -> bool:
        return self.walle_state == 'assigned' and self.walle_status == 'ready'

    def is_master_host(self) -> bool:
        return self.walle_project.endswith('-masters') or self.walle_project.endswith('-ms')

    def has_complete_resources(self) -> bool:
        return all(
            [
                self.cpu_total_vcores > 0,
                self.memory_total_gib > 0,
                self.network_total_bandwidth_mib > 0,
                self.disk_total_capacity_gib > 0,
                self.disk_total_bandwidth_mib > 0,
            ]
        )

    def _compute_numa_slots(self) -> List[NumaSlotResources]:
        """Build NUMA slots after subtracting pod usage and optionally infra tax.

        Does not raise on negative values — use get_available_numa_resources for validated access.
        """
        numa_node_cpu = float('inf')
        numa_node_memory = float('inf')
        numa_node_count = 1

        if self.numa_cpu_details:
            min_cpu = min(node['total_vcores'] for node in self.numa_cpu_details)
            numa_node_cpu = min_cpu * 1000
            numa_node_count = len(self.numa_cpu_details)

        if self.numa_memory_details:
            min_mem = min(node['total_gib'] for node in self.numa_memory_details)
            numa_node_memory = min_mem * 1024
            numa_node_count = len(self.numa_memory_details)

        initial_vcpu = int(numa_node_cpu) if numa_node_cpu != float('inf') else 2**63 - 1
        initial_ram = int(numa_node_memory) if numa_node_memory != float('inf') else 2**63 - 1

        numa_slots = [NumaSlotResources(vcpu=initial_vcpu, ram=initial_ram) for _ in range(numa_node_count)]

        for pod in self.pods:
            if pod.numa_node_id is not None and pod.numa_node_id >= 0:
                numa_id = int(pod.numa_node_id)
                if numa_id >= len(numa_slots):
                    continue
                numa_slots[numa_id].vcpu -= pod.vcpu_guarantee
                numa_slots[numa_id].ram -= int(pod.memory_guarantee / (1024**2))

        if self.config.use_new_infra_tax:
            tax_cpu = max(int(initial_vcpu * self.config.infra_cpu_rel_tax), self.config.infra_cpu_abs_tax)
            # tax_ram = max(int(initial_ram * self.config.infra_memory_rel_tax), self.config.infra_memory_abs_tax)
            for slot in numa_slots:
                slot.vcpu -= tax_cpu
                # slot.ram -= tax_ram

        return numa_slots

    def get_available_numa_resources(self) -> List[NumaSlotResources]:
        """Return available NUMA slots, raising if pods exceed capacity (after infra tax)."""
        slots = self._compute_numa_slots()
        for i, slot in enumerate(slots):
            if slot.vcpu < 0:
                raise RuntimeError(
                    f"NUMA node {i} CPU resources exceeded on host {self.hostname}: "
                    f"{slot.vcpu} millicores available (negative)"
                )
            if slot.ram < 0:
                raise RuntimeError(
                    f"NUMA node {i} Memory resources exceeded on host {self.hostname}: "
                    f"{slot.ram} MiB available (negative)"
                )
        return slots

    def get_numa_excess(self) -> str:
        parts = []
        for i, slot in enumerate(self._compute_numa_slots()):
            if slot.vcpu < 0:
                parts.append(f"NUMA node {i} CPU: {slot.vcpu} millicores (negative)")
            if slot.ram < 0:
                parts.append(f"NUMA node {i} Memory: {slot.ram} MiB (negative)")
        return ", ".join(parts)

    def get_overcommitted_numa_nodes(self) -> Set[int]:
        return {i for i, slot in enumerate(self._compute_numa_slots()) if slot.vcpu < 0 or slot.ram < 0}

    def get_overcommit(self) -> str:
        eff = self.get_effective_free_resources()
        parts = []
        if eff[0] < 0:
            parts.append(f"CPU: +{-eff[0] / 1000:.3f} vcores")
        if eff[1] < 0:
            parts.append(f"Memory: +{-eff[1] / (1024**3):.3f} GiB")
        if eff[2] < -(1024**2):
            parts.append(f"Network: +{-eff[2] / (1024**2):.2f} MiB/s")
        numa_excess = self.get_numa_excess()
        if numa_excess:
            parts.append(f"NUMA: {numa_excess}")
        return ", ".join(parts)

    def evict_overcommitted_pods(self, seed: int = 42) -> Dict[str, int]:
        removed_counts = {name: 0 for name in self.config.pod_configurations}

        if not self.get_overcommit():
            return removed_counts

        rng = random.Random(seed)
        remaining = [pod for pod in self.pods if pod.yt_role in ('yttabnode', 'ytrpcproxy')]
        rng.shuffle(remaining)

        while remaining:
            if not self.get_overcommit():
                break

            overcommitted_numa = self.get_overcommitted_numa_nodes()
            if overcommitted_numa:
                idx = next(
                    (i for i, pod in enumerate(remaining) if pod.numa_node_id in overcommitted_numa),
                    0,
                )
            else:
                idx = 0

            pod = remaining.pop(idx)
            config_name = classify_pod(pod, self.config.pod_configurations)
            if config_name:
                removed_counts[config_name] += 1
            self.remove_pod(pod.pod_id)

        excess = self.get_overcommit()
        if excess:
            raise RuntimeError(
                f"Host {self.hostname} still overcommitted after removing all tab nodes and rpc proxies: {excess}"
            )

        return removed_counts

    def to_dict(self) -> dict:
        return {
            'hostname': self.hostname,
            'cpu_model': self.cpu_model,
            'cpu_total_vcores': self.cpu_total_vcores,
            'cpu_used_vcores': self.cpu_used_vcores,
            'cpu_free_vcores': self.cpu_free_vcores,
            'memory_total_gib': self.memory_total_gib,
            'memory_used_gib': self.memory_used_gib,
            'memory_free_gib': self.memory_free_gib,
            'network_total_bandwidth_mib': self.network_total_bandwidth_mib,
            'network_used_bandwidth_mib': self.network_used_bandwidth_mib,
            'network_free_bandwidth_mib': self.network_free_bandwidth_mib,
            'disk_storage_class': self.disk_storage_class,
            'disk_total_capacity_gib': self.disk_total_capacity_gib,
            'disk_used_capacity_gib': self.disk_used_capacity_gib,
            'disk_free_capacity_gib': self.disk_free_capacity_gib,
            'disk_total_bandwidth_mib': self.disk_total_bandwidth_mib,
            'disk_used_bandwidth_mib': self.disk_used_bandwidth_mib,
            'disk_free_bandwidth_mib': self.disk_free_bandwidth_mib,
            'walle_state': self.walle_state,
            'walle_status': self.walle_status,
            'walle_project': self.walle_project,
            'rack': self.rack,
            # Serialize NUMA details as JSON strings so Host.__init__ can re-parse them
            'numa_cpu_details': json.dumps(self.numa_cpu_details),
            'numa_memory_details': json.dumps(self.numa_memory_details),
            'pods': [dataclasses.asdict(p) for p in self.pods],
        }

    @classmethod
    def from_dict(cls, d: dict, config: 'ClusterConfig') -> 'Host':
        pods_raw = d.pop('pods', [])
        host = cls(d['hostname'], d, config=config)
        host.pods = [PodResources(**p) for p in pods_raw]
        return host


# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------


class Cluster:
    def __init__(self, config: ClusterConfig):
        self.hosts: Dict[str, Host] = {}
        self.config = config

    def update_config(self, new_config: ClusterConfig):
        """Replace cluster config and propagate to all hosts."""
        self.config = new_config
        for host in self.hosts.values():
            host.update_config(new_config)

    def add_host(self, hostname: str, host_data: dict):
        self.hosts[hostname] = Host(hostname, host_data, config=self.config)

    def add_pod_to_host(self, hostname: str, pod: PodResources):
        if hostname in self.hosts:
            self.hosts[hostname].add_pod(pod)
        else:
            raise ValueError(f"Host {hostname} not found in cluster")

    def remove_pod_from_host(self, hostname: str, pod_id: str) -> bool:
        if hostname in self.hosts:
            return self.hosts[hostname].remove_pod(pod_id)
        return False

    def get_host(self, hostname: str) -> Optional[Host]:
        return self.hosts.get(hostname)

    def get_active_hosts(self) -> List[Host]:
        return [
            host
            for host in self.hosts.values()
            if host.is_alive() and not host.is_master_host() and host.has_complete_resources()
        ]

    def load_from_csv(self, hosts_csv: str, pods_csv: str):
        """Load cluster data from CSV files. Requires pandas."""
        import pandas as pd

        print(f"Loading cluster data from {hosts_csv} and {pods_csv}...")

        hosts_df = pd.read_csv(hosts_csv)
        print(f"Loaded {len(hosts_df)} hosts")

        for _, row in hosts_df.iterrows():
            self.add_host(row['hostname'], row.to_dict())

        pods_df = pd.read_csv(pods_csv)
        print(f"Loaded {len(pods_df)} pods")

        for _, row in pods_df.iterrows():
            bca = {}
            if 'yt_bundle_controller_annotations' in row and not _is_na(row['yt_bundle_controller_annotations']):
                bca = json.loads(row['yt_bundle_controller_annotations'])

            yt_user_tags = []
            if 'yt_user_tags' in row and not _is_na(row['yt_user_tags']):
                yt_user_tags = json.loads(row['yt_user_tags'])

            yt_tags = []
            if 'yt_tags' in row and not _is_na(row['yt_tags']):
                yt_tags = json.loads(row['yt_tags'])

            disk_types = []
            if 'disk_types' in row and not _is_na(row['disk_types']):
                disk_types = json.loads(row['disk_types'])

            pod = PodResources(
                pod_id=row['pod_id'],
                yt_sink_pod=bool(row.get('yt_sink_pod', False)),
                vcpu_guarantee=int(row['vcores_guarantee'] * 1000),
                memory_guarantee=int(row['memory_guarantee_gib'] * 1024**3),
                network_guarantee=int(row['network_guarantee_mibs'] * 1024**2),
                numa_node_id=int(row['numa_node_id']) if not _is_na(row.get('numa_node_id')) else None,
                yt_role=row.get('yt_role', 'Unknown'),
                yt_pod_name=row.get('yt_pod_name', ''),
                yt_bundle_controller_annotations=bca,
                yt_decommissioned=bool(row.get('yt_decommissioned', False)),
                yt_proxy_role=row.get('yt_proxy_role') if not _is_na(row.get('yt_proxy_role')) else None,
                yt_user_tags=yt_user_tags,
                yt_tags=yt_tags,
                yt_state=row.get('yt_state', ''),
                yt_alive=bool(row.get('yt_alive', False)),
                disk_types=disk_types,
                disk_capacity=int(row['disk_capacity_gib'] * 1024**3),
                disk_bandwidth_guarantee=int(row['disk_bandwidth_guarantee_mibs'] * 1024**2),
            )

            hostname = row['node_id']
            if hostname in self.hosts:
                self.hosts[hostname].pods.append(pod)
            else:
                print(f"Warning: Pod {pod.pod_id} references unknown host {hostname}")

        print("\nValidating resource consistency...")
        inconsistent = 0
        for hostname, host in self.hosts.items():
            used = host.get_used_resources_from_pods()
            if (
                abs(used[0] - host.cpu_used_vcores) > 0.1
                or abs(used[1] - host.memory_used_gib) > 0.1
                or abs(used[2] - host.network_used_bandwidth_mib) > 1.0
            ):
                inconsistent += 1

        print(f"Cluster loaded: {len(self.hosts)} hosts, " f"{sum(len(h.pods) for h in self.hosts.values())} pods")
        if inconsistent:
            print(f"WARNING: {inconsistent} hosts have resource inconsistencies")
        else:
            print("All host resources are consistent with pod data")

    def to_dict(self) -> dict:
        return {
            'hosts': {hn: h.to_dict() for hn, h in self.hosts.items()},
        }

    @classmethod
    def from_dict(cls, d: dict, config: ClusterConfig) -> 'Cluster':
        cluster = cls(config)
        for hn, hd in d['hosts'].items():
            hd_copy = dict(hd)
            pods_raw = hd_copy.pop('pods', [])
            host = Host(hn, hd_copy, config=config)
            host.pods = [PodResources(**p) for p in pods_raw]
            cluster.hosts[hn] = host
        return cluster


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_na(value) -> bool:
    """Like pd.isna but without pandas dependency."""
    if value is None:
        return True
    try:
        import math

        return isinstance(value, float) and math.isnan(value)
    except Exception:
        return False


def get_disk_bandwidth_from_storage_class(disk_storage_class: str) -> int:
    if disk_storage_class == 'hdd':
        return 10 * 1024**2
    elif disk_storage_class == 'ssd':
        return 50 * 1024**2
    else:
        raise ValueError(f"Unknown disk storage class: {disk_storage_class!r}")


# ---------------------------------------------------------------------------
# Host filtering  (also used by solver.py subprocess)
# ---------------------------------------------------------------------------


def filter_config(
    host: Host, req: AvailableResourcesRequest, role_specific_host_filter: Dict[str, dict], verbose: bool = False
) -> bool:
    if req.yt_role not in role_specific_host_filter:
        return True
    config = role_specific_host_filter[req.yt_role]
    if not config:
        return True

    min_bw = config.get('min_network_bandwidth')
    if min_bw and host.network_total_bandwidth_mib * 1024 * 1024 < min_bw:
        if verbose:
            print(
                f"Node {host.hostname} filtered: low network bandwidth "
                f"({host.network_total_bandwidth_mib} MiB/s < {min_bw / 1024**2:.0f} MiB/s)"
            )
        return False

    required_roles = config.get('require_one_of_neighbor_roles')
    if required_roles:
        if not any(pod.yt_role in required_roles for pod in host.pods):
            if verbose:
                print(f"Node {host.hostname} filtered: no pods with role in {required_roles}")
            return False

    forbidden_tags = config.get('forbid_neighbor_yt_node_tags')
    if forbidden_tags:
        for pod in host.pods:
            for tag in forbidden_tags:
                if tag in pod.yt_tags:
                    if verbose:
                        print(f"Node {host.hostname} filtered: pod {pod.pod_id} has forbidden tag {tag!r}")
                    return False

    return True


def filter_unhealthy_exe_nodes(host: Host, req: AvailableResourcesRequest, verbose: bool = False) -> bool:
    for pod in host.pods:
        if pod.yt_role == 'ytexenode' and 'flavor:exec' in pod.yt_tags and pod.yt_state == 'online':
            return True
    if verbose:
        print(f"Node {host.hostname} filtered: no healthy exe node")
    return False


def filter_host(
    host: Host, req: AvailableResourcesRequest, role_specific_host_filter: Dict[str, dict], verbose: bool = False
) -> bool:
    return filter_config(host, req, role_specific_host_filter, verbose=verbose) and filter_unhealthy_exe_nodes(
        host, req, verbose=verbose
    )


# ---------------------------------------------------------------------------
# Pod classification  (also used by Host.evict_overcommitted_pods)
# ---------------------------------------------------------------------------


def classify_pod(pod: PodResources, pod_configurations: Dict[str, dict]) -> Optional[str]:
    if pod.yt_role not in ('yttabnode', 'ytrpcproxy'):
        return None
    for config_name, cfg in pod_configurations.items():
        if (
            abs(pod.vcpu_guarantee - cfg['vcpu']) <= 100
            and abs(pod.memory_guarantee - cfg['memory']) <= 1024**3
            and abs(pod.network_guarantee - cfg['network']) <= 10 * 1024**2
            and pod.yt_role == cfg['yt_role']
        ):
            return config_name
    return None
