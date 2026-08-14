"""
Shared constants, utilities, and dataclasses for the CP-SAT solver subprocess.

This file is the single source for everything solver.py and model.py need:
  - structural constants
  - utility functions
  - dataclasses BundleGroup / Host / BundleInstances / ContainerType

Kept as one file to avoid cross-imports in the subprocess environment where
these modules are loaded as flat modules from a temp directory.
"""

import dataclasses
import math

import numpy as np

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# --- Discretization steps ---
CPU_STEP = 0.01  # 10m CPU
MEM_STEP = 0.1  # 100 MiB Memory (GiB input)
NET_STEP = 0.1  # 100 KiB Network (MiB/s input)
COST_SCALE = 1000

# --- Container size discretization (for "round" catalog values) ---
NODE_CONTAINER_CPU_STEP = 1.0
NODE_CONTAINER_MEM_STEP = 5.0
NODE_CONTAINER_NET_STEP = 10.0
PROXY_CONTAINER_CPU_STEP = 1.0
PROXY_CONTAINER_MEM_STEP = 1.0
PROXY_CONTAINER_NET_STEP = 10.0

# --- Minimum container sizes (nodes) ---
MIN_CONTAINER_CPU = 4.0
MIN_CONTAINER_MEM = 10.0
MIN_CONTAINER_NET = 100.0

# --- Minimum container sizes (proxies) ---
MIN_PROXY_CPU = 4.0
MIN_PROXY_MEM = 5.0
MIN_PROXY_NET = 50.0

MERGE_BELOW_MIN = False
MIN_PROXY_NET_FOR_GROUPING = 10.0

# --- Availability floor (proxies) ---
# A bundle already running more than one RPC proxy must never be reduced to a
# single one: the bundle would then lose availability, since a failure of that
# one proxy means downtime. Bundles that already run a single proxy keep it.
PROXY_MIN_COUNT_WHEN_REDUNDANT = 2

# --- Overhead per extra (scale-out) instance ---
OVERHEAD_CPU_FRAC = 0.05
OVERHEAD_MEM_FRAC = 0.05
OVERHEAD_NET_FRAC = 0.05
OVERHEAD_CPU_ABS = 2.0  # vcores
OVERHEAD_MEM_ABS = 3.0  # GiB
OVERHEAD_NET_ABS = 0.0  # MiB/s

PROXY_OVERHEAD_CPU_FRAC = 0.05
PROXY_OVERHEAD_MEM_FRAC = 0.05
PROXY_OVERHEAD_NET_FRAC = 0.05
PROXY_OVERHEAD_CPU_ABS = 2.0
PROXY_OVERHEAD_MEM_ABS = 2.0
PROXY_OVERHEAD_NET_ABS = 0.0

# --- Infra tax ---
INFRA_CPU_REL_TAX = 0.0
INFRA_CPU_ABS_TAX = 0.0
INFRA_MEM_REL_TAX = 0.0
INFRA_MEM_ABS_TAX = 0.0


# --- Cluster grouping ---
# Bundles are grouped across clusters (one container size for the whole group)
# only inside a single cluster group. Any cluster not listed here forms its own
# group, i.e. it is never grouped together with another cluster.
# To let more clusters share sizes, just add an entry, e.g.
#   "mr": ["hahn", "arnold", "kolmogorov"],
CLUSTER_GROUPS: dict = {
    "senecas": ["seneca-sas", "seneca-vla", "seneca-klg"],
    "hahn": ["hahn"],
    "arnold": ["arnold"],
    "markov": ["markov"],
    "kolmogorov": ["kolmogorov"],
}


# --- Цены ресурсов ---
# Чем дороже ресурс, тем охотнее солвер экономит именно его. a — за CPU_STEP,
# b — за MEM_STEP, c — за NET_STEP; нормированы так, что максимум равен 1.
# Считаются как дефицит: спрос бандлов против ресурсов хостов кластера
# (compute_resource_coefficients). Марков посчитан по хостам от 2026-08-02 и
# потреблению за 2026-07-30, сенеки — по данным весны. На MR-кластерах хосты
# делятся с MR-нагрузкой, свой пул для них не считался: пока стоят сенечные цены.
SENECA_RESOURCE_COEFFICIENTS: dict = {"a": 0.329465, "b": 1.0, "c": 0.21267}

RESOURCE_COEFFICIENTS: dict = {
    "senecas": SENECA_RESOURCE_COEFFICIENTS,
    "hahn": SENECA_RESOURCE_COEFFICIENTS,
    "arnold": SENECA_RESOURCE_COEFFICIENTS,
    "kolmogorov": SENECA_RESOURCE_COEFFICIENTS,
    "markov": {"a": 0.580251, "b": 1.0, "c": 0.387487},
}


# --- Bundle/host filtering ---
MIN_HOSTS_PER_MODEL = 10
BUNDLES_TO_SKIP = [
    "rtstat_mol",  # heavy_analytics
    # "metrika", # nextgen
    # "metrika-core", # nextgen
    # "searchpers", # nextgen
    # "default", # nextgen
]

# --- Margin settings ---
DEFAULT_MARGIN = 0.15 / (1 - 0.15)
MEM_MARGIN = 0.2 / (1 - 0.2)

NODE_MARGINS: dict = {
    "default": {"cpu": DEFAULT_MARGIN, "memory": MEM_MARGIN, "net": DEFAULT_MARGIN},
}
PROXY_MARGINS: dict = {
    "default": {"cpu": DEFAULT_MARGIN, "memory": MEM_MARGIN, "net": DEFAULT_MARGIN},
}

# --- Model structural constants ---
MAX_EXTRA_RATIO = 2  # max scale-out multiplier
ANTIAFFINITY_PER_NUMA = 5  # max containers of one type per NUMA node
MIN_NUMA_FRAC = 0.2
MIN_USAGE_FRAC_NODE = 0.02
MIN_USAGE_FRAC_PROXY = 0.02

# --- Stage 2 soft-cap slack ---
STAGE2_HOST_COST_SLACK_PCT = 0.001
STAGE2_HOST_COST_SLACK_MIN_INT = 1

# --- Net cost cap for scarcity ---
NET_COST_CAP_GBPS = 30.0

# --- Warm-start hints ---
APPLY_PATTERNS_HINTS = True
HINT_ASSIGNMENTS_DROPOUT_RATE = 0.0  # 0.1   # fraction of bundle groups whose assignment hint to drop
HINT_PATTERNS_DROPOUT_RATE = 0.0  # 0.2      # fraction of (cluster, host, pattern) groups to drop
HINT_DROPOUT_SEED = 42  # set to None for non-reproducible dropout

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def gbps_to_mibs(gbps: float) -> float:
    """1 Gbit/s → MiB/s."""
    return gbps * 1e9 / 8 / (1024**2)


def cluster_to_group_map(cluster_groups: dict | None = None) -> dict:
    """Reverse CLUSTER_GROUPS into {cluster -> group name}."""
    if cluster_groups is None:
        cluster_groups = CLUSTER_GROUPS
    mapping: dict = {}
    for group_name, clusters in cluster_groups.items():
        for cluster in clusters:
            if cluster in mapping:
                raise ValueError(
                    f"Cluster '{cluster}' is listed in several cluster groups: "
                    f"'{mapping[cluster]}' and '{group_name}'"
                )
            mapping[cluster] = group_name
    return mapping


def cluster_group(cluster: str, cluster_groups: dict | None = None) -> str:
    """Cluster group a cluster belongs to; an unlisted cluster is its own group.

    Bundles from different clusters are only ever grouped together when their
    cluster groups match, so unlisted clusters are always optimized on their own.
    """
    return cluster_to_group_map(cluster_groups).get(cluster, cluster)


def disc_floor(values, step: float):
    """Discretize by floor: how many full steps fit (for host capacities)."""
    return np.floor(np.asarray(values, dtype=float) / step).astype(int)


def disc_ceil(values, step: float):
    """Discretize by ceil: how many steps needed to cover (for bundle requirements)."""
    return np.ceil(np.asarray(values, dtype=float) / step).astype(int)


def disc_round(value: float, step: float) -> int:
    """Recover exact integer from stored float z*step, or convert a design value
    that is an exact multiple of step."""
    return int(round(value / step))


def compute_n_min_from_req(
    instance_type: str,
    count: int,
    cpu_req: int,
    mem_req: int,
    net_req: int,
    cpu_s: int,
    mem_s: int,
    net_s: int,
) -> tuple:
    """Return (n_min, feasible) for given explicit requirements and base count."""
    if count <= 0:
        raise ValueError(
            f"compute_n_min_from_req called with count={count} for a "
            f"{instance_type} bundle; bundles are built only from non-empty ones"
        )
    if instance_type == 'node':
        frac_cpu, frac_mem, frac_net = OVERHEAD_CPU_FRAC, OVERHEAD_MEM_FRAC, OVERHEAD_NET_FRAC
        abs_cpu, abs_mem, abs_net = OVERHEAD_CPU_ABS, OVERHEAD_MEM_ABS, OVERHEAD_NET_ABS
    else:
        frac_cpu, frac_mem, frac_net = PROXY_OVERHEAD_CPU_FRAC, PROXY_OVERHEAD_MEM_FRAC, PROXY_OVERHEAD_NET_FRAC
        abs_cpu, abs_mem, abs_net = PROXY_OVERHEAD_CPU_ABS, PROXY_OVERHEAD_MEM_ABS, PROXY_OVERHEAD_NET_ABS
    o_cpu = max(disc_round(abs_cpu, CPU_STEP), math.ceil(cpu_req * frac_cpu))
    o_mem = max(disc_round(abs_mem, MEM_STEP), math.ceil(mem_req * frac_mem))
    o_net = max(disc_round(abs_net, NET_STEP), math.ceil(net_req * frac_net))
    n_min = 1
    for req, O, s in ((cpu_req, o_cpu, cpu_s), (mem_req, o_mem, mem_s), (net_req, o_net, net_s)):
        T = count * req
        if T == 0:
            continue
        n_r = math.ceil(T / s)
        if n_r > count:
            denom = s - O
            if denom <= 0:
                return MAX_EXTRA_RATIO * count, False
            num = T - O * count
            n_r = max(count + 1, math.ceil(num / denom) if num > 0 else count + 1)
        n_min = max(n_min, n_r)
    if instance_type == 'proxy' and count > 1:
        # Never take a redundant bundle down to a single proxy.
        n_min = max(n_min, PROXY_MIN_COUNT_WHEN_REDUNDANT)
    feasible = n_min <= MAX_EXTRA_RATIO * count
    return n_min, feasible


def _make_host_key(cpu_model: str, numa_nodes_per_host: int, C_cpu: int, C_mem: int, C_net: int) -> str:
    return f"{cpu_model}|nnph={int(numa_nodes_per_host)}|cpu={int(C_cpu)}|mem={int(C_mem)}|net={int(C_net)}"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ContainerType:
    name: str  # e.g. "memory_250", "medium"
    cpu_limit: float  # cores
    mem_limit: float  # GiB
    net_limit: float  # MiB/s

    def cpu_limit_int(self, step: float = CPU_STEP) -> int:
        return int(math.floor(self.cpu_limit / step))

    def mem_limit_int(self, step: float = MEM_STEP) -> int:
        return int(math.floor(self.mem_limit / step))

    def net_limit_int(self, step: float = NET_STEP) -> int:
        return int(math.floor(self.net_limit / step))


def _get_margin(instance_type: str, container_type_name: str, resource: str) -> float:
    margins_map = NODE_MARGINS if instance_type == "node" else PROXY_MARGINS
    entry = margins_map.get(container_type_name) or margins_map.get("default") or {}
    return entry.get(resource, DEFAULT_MARGIN)


@dataclasses.dataclass
class BundleInstances:
    name: str
    instance_type: str  # "node" or "proxy"
    container_type: ContainerType
    count: int
    cpu: float  # cores
    memory: float  # GiB
    network: float  # MiB/s
    cluster: str
    node_type: str
    # Инстансы одной зоны доступности: count и метрики уже поделены по зонам.
    zones: int = 1

    @property
    def total_count(self) -> int:
        return self.count * self.zones

    @property
    def bundle(self) -> str:
        return f"{self.name}@{self.instance_type}@{self.cluster}"

    def cpu_req_int(self, step: float = CPU_STEP) -> int:
        margin = _get_margin(self.instance_type, self.container_type.name, "cpu")
        return min(int(math.ceil(self.cpu * (1.0 + margin) / step)), self.container_type.cpu_limit_int(step))

    def mem_req_int(self, step: float = MEM_STEP) -> int:
        margin = _get_margin(self.instance_type, self.container_type.name, "memory")
        return min(int(math.ceil(self.memory * (1.0 + margin) / step)), self.container_type.mem_limit_int(step))

    def net_req_int(self, step: float = NET_STEP) -> int:
        margin = _get_margin(self.instance_type, self.container_type.name, "net")
        return min(int(math.ceil(self.network * (1.0 + margin) / step)), self.container_type.net_limit_int(step))


@dataclasses.dataclass
class BundleGroup:
    """A group of BundleInstances sharing a single optimization variable x[i,k]."""

    instance_type: str
    bundles_by_cluster: dict  # cluster -> list[BundleInstances]
    counts_by_cluster: dict  # cluster -> total instance count
    cpu_req_int: int
    mem_req_int: int
    net_req_int: int
    # Max requirements per bi.bundle key ("name@type@cluster").
    # For a non-merged group: filled by __post_init__ — max across clusters per bundle name
    #   (correct for consistent groups; inconsistent groups are single-cluster so max = itself).
    # For a merged group: filled by merge() as union of constituent dicts — keys include
    #   the cluster, so same-name bundles from different clusters never collide.
    bundle_key_max_req: dict = dataclasses.field(default_factory=dict)
    # Ключи бандлов каждой части, собранной конструктором. merge() их не сливает:
    # части — это то, что считалось вместе по существу, а не по итогу оптимизации.
    parts: list = dataclasses.field(default_factory=list)

    def __post_init__(self):
        if not self.parts:
            self.parts = [frozenset(bi.bundle for bi in self.all_bundles)]
        if not self.bundle_key_max_req:
            name_max: dict = {}
            for blist in self.bundles_by_cluster.values():
                for bi in blist:
                    curr = name_max.get(bi.name)
                    if curr is None:
                        name_max[bi.name] = (bi.cpu_req_int(), bi.mem_req_int(), bi.net_req_int())
                    else:
                        name_max[bi.name] = (
                            max(curr[0], bi.cpu_req_int()),
                            max(curr[1], bi.mem_req_int()),
                            max(curr[2], bi.net_req_int()),
                        )
            self.bundle_key_max_req = {
                bi.bundle: name_max[bi.name] for blist in self.bundles_by_cluster.values() for bi in blist
            }

    @property
    def all_bundles(self) -> list:
        return [bi for blist in self.bundles_by_cluster.values() for bi in blist]

    @property
    def label(self) -> str:
        parts = sorted(f"{bi.name}@{bi.cluster}" for bi in self.all_bundles)
        return f"{self.instance_type}:{','.join(parts)}"

    @property
    def cluster_group(self) -> str:
        """Cluster group this group lives in (all its clusters share one)."""
        return cluster_group(next(iter(self.bundles_by_cluster)))

    @classmethod
    def merge(cls, groups: list) -> "BundleGroup":
        """Merge multiple BundleGroups into one, taking max resource requirements."""
        cluster_groups = {bg.cluster_group for bg in groups}
        if len(cluster_groups) > 1:
            raise ValueError(
                f"Refusing to merge bundle groups from different cluster groups: " f"{sorted(cluster_groups)}"
            )
        merged_by_cluster: dict = {}
        counts_by_cluster: dict = {}
        merged_bundle_key_max_req: dict = {}
        for bg in groups:
            for cluster, blist in bg.bundles_by_cluster.items():
                merged_by_cluster.setdefault(cluster, []).extend(blist)
                counts_by_cluster[cluster] = counts_by_cluster.get(cluster, 0) + bg.counts_by_cluster.get(cluster, 0)
            merged_bundle_key_max_req.update(bg.bundle_key_max_req)
        return cls(
            instance_type=groups[0].instance_type,
            bundles_by_cluster=merged_by_cluster,
            counts_by_cluster=counts_by_cluster,
            cpu_req_int=max(bg.cpu_req_int for bg in groups),
            mem_req_int=max(bg.mem_req_int for bg in groups),
            net_req_int=max(bg.net_req_int for bg in groups),
            bundle_key_max_req=merged_bundle_key_max_req,
            parts=[part for bg in groups for part in bg.parts],
        )


@dataclasses.dataclass
class Host:
    cluster: str
    cpu_model: str
    numa_node_cpu: float
    numa_node_mem: float
    numa_node_net: float
    numa_nodes_per_host: int
    available_physical_hosts: int

    @property
    def total_available_numa_nodes(self) -> int:
        return self.available_physical_hosts * self.numa_nodes_per_host

    @property
    def cpu_capacity(self) -> int:
        return int(math.floor(self.numa_node_cpu / CPU_STEP))

    @property
    def mem_capacity(self) -> int:
        return int(math.floor(self.numa_node_mem / MEM_STEP))

    @property
    def net_capacity(self) -> int:
        return int(math.floor(self.numa_node_net / NET_STEP))

    @property
    def host_key(self) -> str:
        return _make_host_key(
            self.cpu_model,
            self.numa_nodes_per_host,
            self.cpu_capacity,
            self.mem_capacity,
            self.net_capacity,
        )

    def numa_node_value(self, resource_coefficients: dict, net_cap_gbps: float | None = NET_COST_CAP_GBPS) -> float:
        c_net_eff = self.net_capacity
        if net_cap_gbps is not None:
            cap_mibs_host = gbps_to_mibs(net_cap_gbps)
            cap_per_numa = int(math.floor(cap_mibs_host / max(1, self.numa_nodes_per_host) / NET_STEP))
            c_net_eff = min(c_net_eff, cap_per_numa)
        return (
            resource_coefficients["a"] * self.cpu_capacity
            + resource_coefficients["b"] * self.mem_capacity
            + resource_coefficients["c"] * c_net_eff
        )

    def supply_cpu(self) -> int:
        return self.cpu_capacity * self.total_available_numa_nodes

    def supply_mem(self) -> int:
        return self.mem_capacity * self.total_available_numa_nodes

    def supply_net(self, net_cap_gbps: float | None = NET_COST_CAP_GBPS) -> int:
        c_net = self.net_capacity
        if net_cap_gbps is not None:
            cap_mibs_host = gbps_to_mibs(net_cap_gbps)
            cap_per_numa = int(math.floor(cap_mibs_host / max(1, self.numa_nodes_per_host) / NET_STEP))
            c_net = min(c_net, cap_per_numa)
        return c_net * self.total_available_numa_nodes
