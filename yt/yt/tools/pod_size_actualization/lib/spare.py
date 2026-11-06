"""Remove spare pods and release their host resources."""

import json

import pandas as pd

RESOURCE_COLUMNS = (
    ('vcores_guarantee', 'cpu_used_vcores', 'cpu_free_vcores'),
    ('memory_guarantee_gib', 'memory_used_gib', 'memory_free_gib'),
    ('network_guarantee_mibs', 'network_used_bandwidth_mib', 'network_free_bandwidth_mib'),
    ('disk_capacity_gib', 'disk_used_capacity_gib', 'disk_free_capacity_gib'),
)


def get_allocated_for_bundle(annotations_raw):
    if pd.isna(annotations_raw) or annotations_raw == '':
        return ''
    try:
        annotations = json.loads(annotations_raw)
    except (json.JSONDecodeError, TypeError):
        return ''
    return annotations.get('allocated_for_bundle', '') if isinstance(annotations, dict) else ''


def remove_spare_pods(hosts_df, pods_df):
    spare_mask = pods_df['yt_bundle_controller_annotations'].apply(get_allocated_for_bundle) == 'spare'
    spare_pods = pods_df.loc[spare_mask]
    print(f"Found {len(spare_pods)} spare pods")

    hosts_out = hosts_df.copy().set_index('hostname')
    resource_columns = list(RESOURCE_COLUMNS)
    if 'disk_used_bandwidth_mib' in hosts_out.columns:
        resource_columns.append(('disk_bandwidth_guarantee_mibs', 'disk_used_bandwidth_mib', 'disk_free_bandwidth_mib'))

    freed = {pod_column: 0.0 for pod_column, _, _ in resource_columns}
    affected_hosts = set()
    for _, pod in spare_pods.iterrows():
        hostname = pod['node_id']
        if hostname not in hosts_out.index:
            print(f"Warning: host {hostname} not found for spare pod {pod['pod_id']}")
            continue
        affected_hosts.add(hostname)
        for pod_column, used_column, free_column in resource_columns:
            amount = pod.get(pod_column, 0.0)
            hosts_out.loc[hostname, used_column] -= amount
            hosts_out.loc[hostname, free_column] += amount
            freed[pod_column] += amount

    print('Freed resources:')
    for name, amount in freed.items():
        print(f"  {name}: {amount:.2f}")
    print(f"Affected hosts: {len(affected_hosts)}")

    # NUMA resources are derived from the remaining pods and their numa_node_id.
    if 'numa_node_id' in spare_pods.columns:
        numa_spare = spare_pods.loc[spare_pods['numa_node_id'].notna()]
        print(f"Spare pods with NUMA affinity: {len(numa_spare)}")

    return hosts_out.reset_index(), pods_df.loc[~spare_mask].copy()
