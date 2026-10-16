"""
Validation utilities for defragmentation.

get_bundle_hotfix_bundles and validate_bundles_with_yt require external
resources (a binary or a live YT client) and are not called in the normal
pipeline run — enable them manually for pre-flight checks.

validate_host_filtering is called in the normal run (Step 2c).
"""

import subprocess
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from .cluster import PodsToRemoveStructure
    from .scripts.shared import Cluster, ClusterConfig


# ---------------------------------------------------------------------------
# get_bundle_hotfix_bundles
# ---------------------------------------------------------------------------


def get_bundle_hotfix_bundles(cluster_name: str) -> List[str]:
    """Return bundles with bundle_hotfix mode enabled for the given cluster.

    Requires the bundle_hotfix binary to be built:
        ya make -r ~/arcadia/yt/yt/scripts/dynamic_tables/bundle_hotfix --output ./build
    """
    binary = "./build/yt/yt/scripts/dynamic_tables/bundle_hotfix/bundle_hotfix"
    try:
        result = subprocess.run(
            [binary, "list", "--proxy", cluster_name],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        bundles = []
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line and not line.startswith('Listing'):
                bundles.append(line)
        return bundles
    except subprocess.CalledProcessError as e:
        print(f"Error running bundle_hotfix: {e}\nstderr: {e.stderr}")
        return []
    except subprocess.TimeoutExpired:
        print(f"Timeout running bundle_hotfix for cluster {cluster_name}")
        return []
    except FileNotFoundError:
        print(f"bundle_hotfix binary not found at {binary}")
        print("Build with: ya make -r ~/arcadia/yt/yt/scripts/dynamic_tables/bundle_hotfix --output ./build")
        return []


# ---------------------------------------------------------------------------
# validate_bundles_with_yt
# ---------------------------------------------------------------------------


def validate_bundles_with_yt(structure: 'PodsToRemoveStructure', yt_client) -> None:
    """Validate pod counts and resource guarantees against YT Bundle Controller config.

    For each bundle in structure, checks:
    - tablet_node_count / rpc_proxy_count matches bundle_controller_target_config
    - set of pod names in structure matches online nodes / alive proxies in Cypress
    - resource guarantees of each pod match tablet_node_resource_guarantee /
      rpc_proxy_resource_guarantee from the config

    Prints warnings for any mismatches. Requires a live YT client.
    """
    print("\n=== BUNDLE VALIDATION ===\n")

    print("Fetching all tablet nodes and RPC proxies from YT...")
    try:
        nodes = yt_client.list("//sys/tablet_nodes", attributes=["bundle_controller_annotations", "state"])
        proxies = yt_client.list("//sys/rpc_proxies", attributes=["bundle_controller_annotations"])
        print(f"  Found {len(nodes)} tablet nodes and {len(proxies)} RPC proxies on cluster")
    except Exception as e:
        print(f"ERROR: Failed to fetch nodes/proxies from YT: {e}")
        return

    def _normalize_resources(resources: dict) -> dict:
        normalized = {}
        if 'vcpu' in resources:
            normalized['vcpu'] = resources['vcpu']
        if 'memory' in resources:
            normalized['memory'] = resources['memory']
        if 'net_bytes' in resources:
            normalized['net_bytes'] = resources['net_bytes']
        elif 'net' in resources:
            normalized['net_bytes'] = resources['net'] // 8
        return normalized

    def _resources_match(pod_resources: dict, expected: dict):
        pod_norm = _normalize_resources(pod_resources)
        exp_norm = _normalize_resources(expected)
        diffs = []
        for key in exp_norm:
            if key not in pod_norm:
                diffs.append(f"{key}: missing in pod")
            elif pod_norm[key] != exp_norm[key]:
                diffs.append(f"{key}: pod={pod_norm[key]}, expected={exp_norm[key]}")
        return len(diffs) == 0, diffs

    all_bundles = set(structure.tabnodes_by_bundle) | set(structure.rpcproxies_by_bundle)
    warnings_found = False

    for bundle_name in sorted(all_bundles):
        tabnodes = structure.tabnodes_by_bundle.get(bundle_name, [])
        rpcproxies = structure.rpcproxies_by_bundle.get(bundle_name, [])
        if not tabnodes and not rpcproxies:
            continue

        try:
            if bundle_name == 'spare':
                config_path = '//sys/bundle_controller/controller/zones/zone_default/@spare_target_config'
            else:
                config_path = f'//sys/tablet_cell_bundles/{bundle_name}/@bundle_controller_target_config'
            target_config = yt_client.get(config_path)

            expected_node_count = target_config.get('tablet_node_count', 0)
            expected_proxy_count = target_config.get('rpc_proxy_count', 0)
            expected_node_resources = target_config.get('tablet_node_resource_guarantee', {})
            expected_proxy_resources = target_config.get('rpc_proxy_resource_guarantee', {})

            # --- Count checks ---
            nodes_count_ok = len(tabnodes) == expected_node_count or len(tabnodes) == 0
            proxies_count_ok = len(rpcproxies) == expected_proxy_count or len(rpcproxies) == 0

            # --- Tablet node name set vs Cypress (online only) ---
            tabnode_names_in_structure = {pod.yt_pod_name for pod in tabnodes}
            tabnode_names_in_yt = set()
            if tabnodes:
                for node in nodes:
                    ann = node.attributes.get('bundle_controller_annotations', {})
                    if ann.get('allocated_for_bundle') == bundle_name and node.attributes.get('state', '') == 'online':
                        tabnode_names_in_yt.add(str(node))
            tabnode_only_in_structure = tabnode_names_in_structure - tabnode_names_in_yt
            tabnode_only_in_yt = tabnode_names_in_yt - tabnode_names_in_structure
            tabnode_names_ok = not (tabnode_only_in_structure or tabnode_only_in_yt)

            # --- RPC proxy name set vs Cypress (alive only, only when count mismatches) ---
            rpcproxy_names_in_structure = {pod.yt_pod_name for pod in rpcproxies}
            rpcproxy_names_in_yt = set()
            rpcproxy_only_in_structure = set()
            rpcproxy_only_in_yt = set()
            rpcproxy_names_ok = True
            if not proxies_count_ok and rpcproxies:
                for proxy in proxies:
                    ann = proxy.attributes.get('bundle_controller_annotations', {})
                    if ann.get('allocated_for_bundle') == bundle_name:
                        proxy_name = str(proxy)
                        if yt_client.exists(f'//sys/rpc_proxies/{proxy_name}/alive'):
                            rpcproxy_names_in_yt.add(proxy_name)
                rpcproxy_only_in_structure = rpcproxy_names_in_structure - rpcproxy_names_in_yt
                rpcproxy_only_in_yt = rpcproxy_names_in_yt - rpcproxy_names_in_structure
                rpcproxy_names_ok = not (rpcproxy_only_in_structure or rpcproxy_only_in_yt)

            # --- Resource checks ---
            tabnode_resource_issues = []
            if tabnodes and expected_node_resources:
                for i, pod in enumerate(tabnodes):
                    pod_res = pod.bundle_controller_annotations.get('resources', {})
                    ok, diffs = _resources_match(pod_res, expected_node_resources)
                    if not ok:
                        tabnode_resource_issues.append((i, pod.pod_id, diffs))

            rpcproxy_resource_issues = []
            if rpcproxies and expected_proxy_resources:
                for i, pod in enumerate(rpcproxies):
                    pod_res = pod.bundle_controller_annotations.get('resources', {})
                    ok, diffs = _resources_match(pod_res, expected_proxy_resources)
                    if not ok:
                        rpcproxy_resource_issues.append((i, pod.pod_id, diffs))

            # --- Report ---
            has_issues = (
                not nodes_count_ok
                or not proxies_count_ok
                or not tabnode_names_ok
                or not rpcproxy_names_ok
                or tabnode_resource_issues
                or rpcproxy_resource_issues
            )

            if has_issues:
                warnings_found = True
                print(f"WARNING: Bundle '{bundle_name}' has validation issues:")

                if not nodes_count_ok:
                    print(
                        f"  Tablet nodes count: expected {expected_node_count}, "
                        f"found {len(tabnodes)} in structure, "
                        f"{len(tabnode_names_in_yt)} online in Cypress"
                    )

                if not tabnode_names_ok:
                    if nodes_count_ok:
                        print(
                            f"  Tablet nodes count matches ({len(tabnodes)}) but pod sets differ "
                            f"(online in Cypress: {len(tabnode_names_in_yt)})"
                        )
                    if tabnode_only_in_structure:
                        print(f"  Tablet nodes in structure but NOT in Cypress ({len(tabnode_only_in_structure)}):")
                        for name in sorted(tabnode_only_in_structure)[:5]:
                            print(f"    - {name}")
                        if len(tabnode_only_in_structure) > 5:
                            print(f"    ... and {len(tabnode_only_in_structure) - 5} more")
                    if tabnode_only_in_yt:
                        print(f"  Tablet nodes in Cypress but NOT in structure ({len(tabnode_only_in_yt)}):")
                        for name in sorted(tabnode_only_in_yt)[:5]:
                            print(f"    - {name}")
                        if len(tabnode_only_in_yt) > 5:
                            print(f"    ... and {len(tabnode_only_in_yt) - 5} more")

                if not proxies_count_ok:
                    print(
                        f"  RPC proxies count: expected {expected_proxy_count}, "
                        f"found {len(rpcproxies)} in structure, "
                        f"{len(rpcproxy_names_in_yt)} alive in Cypress"
                    )

                if not rpcproxy_names_ok:
                    if rpcproxy_only_in_structure:
                        print(f"  RPC proxies in structure but NOT in Cypress ({len(rpcproxy_only_in_structure)}):")
                        for name in sorted(rpcproxy_only_in_structure)[:5]:
                            print(f"    - {name}")
                        if len(rpcproxy_only_in_structure) > 5:
                            print(f"    ... and {len(rpcproxy_only_in_structure) - 5} more")
                    if rpcproxy_only_in_yt:
                        print(f"  RPC proxies in Cypress but NOT in structure ({len(rpcproxy_only_in_yt)}):")
                        for name in sorted(rpcproxy_only_in_yt)[:5]:
                            print(f"    - {name}")
                        if len(rpcproxy_only_in_yt) > 5:
                            print(f"    ... and {len(rpcproxy_only_in_yt) - 5} more")

                if tabnode_resource_issues:
                    print(f"  Tablet node resource mismatches ({len(tabnode_resource_issues)} pods):")
                    for idx, pod_id, diffs in tabnode_resource_issues[:3]:
                        print(f"    Pod #{idx} ({pod_id}):")
                        for diff in diffs:
                            print(f"      - {diff}")
                    if len(tabnode_resource_issues) > 3:
                        print(f"    ... and {len(tabnode_resource_issues) - 3} more pods with issues")

                if rpcproxy_resource_issues:
                    print(f"  RPC proxy resource mismatches ({len(rpcproxy_resource_issues)} pods):")
                    for idx, pod_id, diffs in rpcproxy_resource_issues[:3]:
                        print(f"    Pod #{idx} ({pod_id}):")
                        for diff in diffs:
                            print(f"      - {diff}")
                    if len(rpcproxy_resource_issues) > 3:
                        print(f"    ... and {len(rpcproxy_resource_issues) - 3} more pods with issues")

                print()
            else:
                print(
                    f"OK Bundle '{bundle_name}': counts and resources match "
                    f"(tabnodes: {len(tabnodes)}, rpcproxies: {len(rpcproxies)})"
                )

        except Exception as e:
            warnings_found = True
            print(f"ERROR: Failed to validate bundle '{bundle_name}': {e}")
            print()

    if warnings_found:
        print("\nSome bundles have validation issues - please review warnings above")
    else:
        print("\nAll bundles validated successfully - counts and resources match target configs")


# ---------------------------------------------------------------------------
# validate_host_filtering
# ---------------------------------------------------------------------------


def validate_host_filtering(cluster: 'Cluster', config: 'ClusterConfig', verbose: bool = False) -> None:
    """Print how many active hosts pass the role-specific host filter for each role.

    Uses the largest pod configuration per role as the most restrictive test.
    When verbose=True, filter_host prints the reason for each filtered-out host.
    """
    from .scripts.shared import AvailableResourcesRequest, filter_host

    active_hosts = cluster.get_active_hosts()
    print("Host filtering check:")

    for role in ('yttabnode', 'ytrpcproxy'):
        role_cfgs = {n: c for n, c in config.pod_configurations.items() if c['yt_role'] == role}
        if not role_cfgs:
            continue
        largest = max(role_cfgs.values(), key=lambda c: c['memory'])
        req = AvailableResourcesRequest(
            vcpu=largest['vcpu'],
            memory=largest['memory'],
            net=largest['network'],
            numa_enabled=True,
            antiaffinity=config.antiaffinity.get(role, 999),
            yt_role=role,
        )
        filtered = [h for h in active_hosts if filter_host(h, req, config.role_specific_host_filter, verbose=verbose)]
        print(f"  {role}: {len(filtered)} / {len(active_hosts)} active hosts pass filter")
