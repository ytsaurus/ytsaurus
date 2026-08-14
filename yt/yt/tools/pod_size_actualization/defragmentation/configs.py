"""
Per-cluster configuration constants for defragmentation.

Usage:
    from .configs import get_config
    config = get_config('seneca-klg')
"""

from .scripts.shared import ClusterConfig

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

MEMORY_250 = {
    'vcpu': 28000,
    'memory': 268435456000,
    'network': 629145600,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
MEMORY_250_MARKOV = {
    'vcpu': 25000,
    'memory': 268435456000,
    'network': 786432000,
    'disk_capacity': 80530636800,
    'yt_role': 'yttabnode',
}
HEAVY_ANALYTICS = {
    'vcpu': 56000,
    'memory': 214748364800,
    'network': 629145600,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
NEXTGEN = {
    'vcpu': 50000,
    'memory': 225485783040,
    'network': 629145600,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
SENECA_KLG_SPARE = {
    'vcpu': 50000,
    'memory': 268435456000,
    'network': 629145600,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
MEMORY_200 = {
    'vcpu': 28000,
    'memory': 214748364800,
    'network': 629145600,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
MEMORY_200_MARKOV = {
    'vcpu': 25000,
    'memory': 214748364800,
    'network': 786432000,
    'disk_capacity': 80530636800,
    'yt_role': 'yttabnode',
}
MEMORY_150 = {
    'vcpu': 28000,
    'memory': 161061273600,
    'network': 629145600,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
CPU_INTENSIVE = {
    'vcpu': 28000,
    'memory': 107374182400,
    'network': 629145600,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
CPU_INTENSIVE_MARKOV = {
    'vcpu': 25000,
    'memory': 107374182400,
    'network': 786432000,
    'disk_capacity': 80530636800,
    'yt_role': 'yttabnode',
}
MEDIUM = {
    'vcpu': 14000,
    'memory': 128849018880,
    'network': 314572800,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
TINY = {
    'vcpu': 4000,
    'memory': 21474836480,
    'network': 167772160,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
NEW_TINY = {
    'vcpu': 4000,
    'memory': 21474836480,
    'network': 89128960,
    'disk_capacity': 107374182400,
    'yt_role': 'yttabnode',
}
TINY_MARKOV = {
    'vcpu': 4000,
    'memory': 21474836480,
    'network': 167772160,
    'disk_capacity': 80530636800,
    'yt_role': 'yttabnode',
}
MEDIUM_MARKOV = {
    'vcpu': 14000,
    'memory': 107374182400,
    'network': 335544320,
    'disk_capacity': 80530636800,
    'yt_role': 'yttabnode',
}
RPC_PROXY = {
    'vcpu': 10000,
    'memory': 21474836480,
    'network': 157286400,
    'disk_capacity': 107374182400,
    'yt_role': 'ytrpcproxy',
}
RPC_PROXY_HEAVY_ANALYTICS = {
    'vcpu': 10000,
    'memory': 42949672960,
    'network': 314572800,
    'disk_capacity': 107374182400,
    'yt_role': 'ytrpcproxy',
}

RPC_PROXY_CPU_INTENSIVE = {
    'vcpu': 10000,
    'memory': 10737418240,
    'network': 146800640,
    'disk_capacity': 107374182400,
    'yt_role': 'ytrpcproxy',
}
RPC_PROXY_NETWORK_INTENSIVE = {
    'vcpu': 6000,
    'memory': 10737418240,
    'network': 209715200,
    'disk_capacity': 107374182400,
    'yt_role': 'ytrpcproxy',
}

RPC_PROXY_MEDIUM_MARKOV = {
    'vcpu': 9000,
    'memory': 21474836480,
    'network': 262144000,
    'disk_capacity': 80530636800,
    'yt_role': 'ytrpcproxy',
}
RPC_PROXY_TINY_MARKOV = {
    'vcpu': 2000,
    'memory': 10737418240,
    'network': 52428800,
    'disk_capacity': 80530636800,
    'yt_role': 'ytrpcproxy',
}

LARGE_ZENO = {
    'vcpu': 28000,
    'memory': 214748364800,
    'network': 671088640,
    'disk_capacity': 80530636800,
    'disk_bandwidth': 10485760,
    'yt_role': 'yttabnode',
}

CPU_INTENSIVE_ZENO = {
    'vcpu': 28000,
    'memory': 107374182400,
    'network': 671088640,
    'disk_capacity': 80530636800,
    'disk_bandwidth': 10485760,
    'yt_role': 'yttabnode',
}

MEDIUM_ZENO = {
    'vcpu': 14000,
    'memory': 107374182400,
    'network': 335544320,
    'disk_capacity': 80530636800,
    'disk_bandwidth': 10485760,
    'yt_role': 'yttabnode',
}

SMALL_ZENO = {
    'vcpu': 7000,
    'memory': 53687091200,
    'network': 167772160,
    'disk_capacity': 80530636800,
    'disk_bandwidth': 10485760,
    'yt_role': 'yttabnode',
}

RPC_PROXY_MEDIUM_ZENO = {
    'vcpu': 12000,
    'memory': 21474836480,
    'network': 136314880,
    'disk_capacity': 80530636800,
    'disk_bandwidth': 10485760,
    'yt_role': 'ytrpcproxy',
}
TINY_ZENO = {
    'vcpu': 7000,
    'memory': 21474836480,
    'network': 167772160,
    'disk_capacity': 80530636800,
    'disk_bandwidth': 10485760,
    'yt_role': 'yttabnode',
}

RPC_PROXY_SMALL_ZENO = {
    'vcpu': 6000,
    'memory': 10737418240,
    'network': 68157440,
    'disk_capacity': 80530636800,
    'disk_bandwidth': 10485760,
    'yt_role': 'ytrpcproxy',
    'priority': 7,
}


_SENECA_VLA_POD_CONFIGURATIONS = {
    'memory_250': {**MEMORY_250, 'priority': 1},
    'heavy_analytics': {**HEAVY_ANALYTICS, 'priority': 2},
    'memory_200': {**MEMORY_200, 'priority': 3},
    'memory_150': {**MEMORY_150, 'priority': 4},
    'cpu_intensive': {**CPU_INTENSIVE, 'priority': 5},
    'medium': {**MEDIUM, 'priority': 6},
    'rpc_proxy_heavy_analytics': {**RPC_PROXY_HEAVY_ANALYTICS, 'priority': 7},
    'rpc_proxy_medium': {**RPC_PROXY, 'priority': 7},
    'tiny_deprecated': {**TINY, 'priority': 8},
    'tiny': {**NEW_TINY, 'priority': 9},
    'rpc_proxy_cpu_intensive': {**RPC_PROXY_CPU_INTENSIVE, 'priority': 10},
    'rpc_proxy_network_intensive': {**RPC_PROXY_NETWORK_INTENSIVE, 'priority': 11},
}

SENECA_SAS_POD_CONFIGURATIONS = {
    'memory_250': {**MEMORY_250, 'priority': 1},
    'heavy_analytics': {**HEAVY_ANALYTICS, 'priority': 2},
    'memory_200': {**MEMORY_200, 'priority': 3},
    'memory_150': {**MEMORY_150, 'priority': 4},
    'cpu_intensive': {**CPU_INTENSIVE, 'priority': 5},
    'medium': {**MEDIUM, 'priority': 6},
    'rpc_proxy_medium': {**RPC_PROXY, 'priority': 7},
    'tiny_deprecated': {**TINY, 'priority': 8},
    'tiny': {**NEW_TINY, 'priority': 9},
    'rpc_proxy_cpu_intensive': {**RPC_PROXY_CPU_INTENSIVE, 'priority': 10},
    'rpc_proxy_network_intensive': {**RPC_PROXY_NETWORK_INTENSIVE, 'priority': 11},
}

SENECA_KLG_POD_CONFIGURATIONS = {
    'spare': {
        **SENECA_KLG_SPARE,
        'priority': 1,
    },
    'memory_250': {
        **MEMORY_250,
        'priority': 2,
    },
    'nextgen': {
        **NEXTGEN,
        'priority': 3,
    },
    'memory_200': {
        **MEMORY_200,
        'priority': 4,
    },
    'memory_150': {
        **MEMORY_150,
        'priority': 5,
    },
    'cpu_intensive': {
        **CPU_INTENSIVE,
        'priority': 6,
    },
    'medium': {**MEDIUM, 'priority': 7},
    'rpc_proxy_medium': {**RPC_PROXY, 'priority': 8},
    'tiny_deprecated': {**TINY, 'priority': 9},
    'tiny': {**NEW_TINY, 'priority': 10},
    'rpc_proxy_cpu_intensive': {**RPC_PROXY_CPU_INTENSIVE, 'priority': 11},
    'rpc_proxy_network_intensive': {**RPC_PROXY_NETWORK_INTENSIVE, 'priority': 12},
}

_MARKOV_POD_CONFIGURATIONS = {
    'memory_250': {
        **MEMORY_250_MARKOV,
        'priority': 1,
    },
    'memory_200': {
        **MEMORY_200_MARKOV,
        'priority': 2,
    },
    'cpu_intensive': {
        **CPU_INTENSIVE_MARKOV,
        'priority': 3,
    },
    'medium': {
        **MEDIUM_MARKOV,
        'priority': 4,
    },
    'rpc_proxy_medium': {
        **RPC_PROXY_MEDIUM_MARKOV,
        'priority': 5,
    },
    'tiny': {
        **TINY_MARKOV,
        'priority': 6,
    },
    'rpc_proxy_tiny': {
        **RPC_PROXY_TINY_MARKOV,
        'priority': 7,
    },
}

_ZENO_POD_CONFIGURATIONS = {
    'large': {
        **LARGE_ZENO,
        'priority': 1,
    },
    'cpu_intensive': {
        **CPU_INTENSIVE_ZENO,
        'priority': 2,
    },
    'medium': {
        **MEDIUM_ZENO,
        'priority': 3,
    },
    'small': {
        **SMALL_ZENO,
        'priority': 4,
    },
    'rpc_proxy_medium': {
        **RPC_PROXY_MEDIUM_ZENO,
        'priority': 5,
    },
    'tiny': {
        **TINY_ZENO,
        'priority': 6,
    },
    'rpc_proxy_small': {
        **RPC_PROXY_SMALL_ZENO,
        'priority': 7,
    },
}

_BASE_ROLE_SPECIFIC_HOST_FILTER = {
    'yttabnode': {
        'min_network_bandwidth': 1250000000,
        'require_one_of_neighbor_roles': ['ytexenode'],
        'forbid_neighbor_yt_node_tags': ['disable_hulk_allocations'],
    },
    'ytrpcproxy': {
        'min_network_bandwidth': 1250000000,
        'require_one_of_neighbor_roles': ['ytexenode'],
        'forbid_neighbor_yt_node_tags': ['disable_hulk_allocations'],
    },
}


# ---------------------------------------------------------------------------
# Per-cluster overrides
# ---------------------------------------------------------------------------

_CLUSTER_CONFIGS = {
    'seneca-klg': dict(
        yt_proxy='seneca-klg',
        validate_bundles=True,
        pod_configurations=SENECA_KLG_POD_CONFIGURATIONS,
        antiaffinity={'yttabnode': 8, 'ytrpcproxy': 8},
        data_node_network_guarantee=600,
        min_sink_vcpu=4000,
        min_sink_memory=8192,
        memory_reserve_mib=0,
        use_new_infra_tax=True,
        raise_network_limits=False,
        update_custom_pods=False,
        dc='klg',
    ),
    'seneca-sas': dict(
        yt_proxy='seneca-sas',
        validate_bundles=True,
        pod_configurations=SENECA_SAS_POD_CONFIGURATIONS,
        antiaffinity={'yttabnode': 8, 'ytrpcproxy': 8},
        data_node_network_guarantee=700,
        min_sink_vcpu=1700,
        min_sink_memory=21200,
        memory_reserve_mib=0,
        use_new_infra_tax=True,
        raise_network_limits=False,
        update_custom_pods=False,
        dc='sas',
    ),
    'seneca-vla': dict(
        yt_proxy='seneca-vla',
        validate_bundles=True,
        pod_configurations=_SENECA_VLA_POD_CONFIGURATIONS,
        antiaffinity={'yttabnode': 8, 'ytrpcproxy': 10},
        data_node_network_guarantee=575,
        min_sink_vcpu=4000,
        min_sink_memory=21200,
        memory_reserve_mib=0,
        use_new_infra_tax=True,
        raise_network_limits=False,
        update_custom_pods=False,
        dc='vla',
    ),
    'markov-klg': dict(
        yt_proxy='markov',
        validate_bundles=False,
        pod_configurations=_MARKOV_POD_CONFIGURATIONS,
        antiaffinity={'yttabnode': 8, 'ytrpcproxy': 12},
        data_node_network_guarantee=600,
        min_sink_vcpu=3096,
        min_sink_memory=4096,
        memory_reserve_mib=0,
        use_new_infra_tax=True,
        raise_network_limits=True,
        update_custom_pods=False,
        dc='klg',
    ),
    'markov-sas': dict(
        yt_proxy='markov',
        validate_bundles=False,
        pod_configurations=_MARKOV_POD_CONFIGURATIONS,
        antiaffinity={'yttabnode': 8, 'ytrpcproxy': 12},
        data_node_network_guarantee=600,
        min_sink_vcpu=3096,
        min_sink_memory=4096,
        memory_reserve_mib=0,
        use_new_infra_tax=True,
        raise_network_limits=True,
        update_custom_pods=False,
        dc='sas',
    ),
    'markov-vla': dict(
        yt_proxy='markov',
        validate_bundles=False,
        pod_configurations=_MARKOV_POD_CONFIGURATIONS,
        antiaffinity={'yttabnode': 8, 'ytrpcproxy': 12},
        data_node_network_guarantee=600,
        min_sink_vcpu=3096,
        min_sink_memory=4096,
        memory_reserve_mib=0,
        use_new_infra_tax=True,
        raise_network_limits=True,
        update_custom_pods=False,
        dc='vla',
    ),
    'zeno': dict(
        yt_proxy='zeno',
        validate_bundles=True,
        pod_configurations=_ZENO_POD_CONFIGURATIONS,
        antiaffinity={'yttabnode': 8, 'ytrpcproxy': 8},
        data_node_network_guarantee=600,
        min_sink_vcpu=4000,
        min_sink_memory=8192,
        memory_reserve_mib=2000,
        use_new_infra_tax=False,
        raise_network_limits=False,
        update_custom_pods=False,
        dc='vla',
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_config(cluster_name: str) -> ClusterConfig:
    """Return a ClusterConfig populated with per-cluster constants.

    Args:
        cluster_name: one of 'seneca-klg', 'seneca-sas', 'seneca-vla'.

    Raises:
        ValueError: if the cluster is not known.
    """
    overrides = _CLUSTER_CONFIGS.get(cluster_name)
    if overrides is None:
        known = ', '.join(sorted(_CLUSTER_CONFIGS))
        raise ValueError(f"Unknown cluster {cluster_name!r}. Known clusters: {known}")

    return ClusterConfig(
        dc=overrides['dc'],
        pod_configurations=overrides['pod_configurations'],
        antiaffinity=overrides['antiaffinity'],
        role_specific_host_filter=_BASE_ROLE_SPECIFIC_HOST_FILTER,
        yt_proxy=overrides['yt_proxy'],
        validate_bundles=overrides['validate_bundles'],
        data_node_network_guarantee=overrides['data_node_network_guarantee'],
        min_sink_vcpu=overrides['min_sink_vcpu'],
        min_sink_memory_mib=overrides['min_sink_memory'],
        memory_reserve_mib=overrides['memory_reserve_mib'],
        use_new_infra_tax=overrides['use_new_infra_tax'],
        raise_network_limits=overrides['raise_network_limits'],
        update_custom_pods=overrides['update_custom_pods'],
    )
