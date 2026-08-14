"""Build the optimizer container catalog from bundle-controller zone config."""

from __future__ import annotations

import datetime
import json
from typing import Callable

from yt.yson import YsonBoolean

from yt.yt.tools.pod_size_actualization.optimization.scripts.shared import CLUSTER_GROUPS

_RESOURCE_GUARANTEE = "resource_guarantee"
_TABLET_NODE_SIZES_PATH = "//sys/bundle_controller/controller/zones/zone_default/@tablet_node_sizes"
_RPC_PROXY_SIZES_PATH = "//sys/bundle_controller/controller/zones/zone_default/@rpc_proxy_sizes"


def _active_configs(configs: dict, *, cluster: str, instance_type: str) -> dict:
    if not isinstance(configs, dict):
        raise TypeError(f"{cluster}: {instance_type} size catalog must be a map, got {type(configs).__name__}")

    active = {}
    for name, config in configs.items():
        if not isinstance(config, dict):
            raise TypeError(f"{cluster}: {instance_type} size {name!r} must be a map")
        deprecated = config.get("deprecated", False)
        if not isinstance(deprecated, (bool, YsonBoolean)):
            raise TypeError(f"{cluster}: {instance_type} size {name!r}: deprecated must be boolean")
        if deprecated:
            reason = config.get("deprecation_reason")
            suffix = f": {reason}" if reason else ""
            print(f"{cluster}: excluding deprecated {instance_type} size {name!r}{suffix}")
            continue
        active[name] = config
    return active


def _normalize_config(name: str, config: dict, *, cluster: str, instance_type: str) -> dict:
    guarantee = config.get(_RESOURCE_GUARANTEE)
    if not isinstance(guarantee, dict):
        raise ValueError(f"{cluster}: {instance_type} size {name!r} has no {_RESOURCE_GUARANTEE} map")

    missing = [key for key in ("vcpu", "memory", "net_bytes") if key not in guarantee]
    if missing:
        raise ValueError(
            f"{cluster}: {instance_type} size {name!r}: resource_guarantee " f"misses {', '.join(missing)}"
        )

    return {
        "InstanceType": instance_type,
        "ConfigName": name,
        "CPU": float(guarantee["vcpu"]) / 1000,
        "Memory": float(guarantee["memory"]) / 2**30,
        "Network": float(guarantee["net_bytes"]) / 2**20,
    }


def build_cluster_instance_sizes(
    tablet_node_sizes: dict,
    rpc_proxy_sizes: dict,
    *,
    cluster: str,
) -> list[dict]:
    """Filter deprecated sizes and convert bundle-controller units for the optimizer."""
    rows = []
    for instance_type, configs in (
        ("node", tablet_node_sizes),
        ("proxy", rpc_proxy_sizes),
    ):
        active = _active_configs(configs, cluster=cluster, instance_type=instance_type)
        rows.extend(
            _normalize_config(name, config, cluster=cluster, instance_type=instance_type)
            for name, config in active.items()
        )

    type_order = {"node": 0, "proxy": 1}
    rows.sort(
        key=lambda row: (
            type_order[row["InstanceType"]],
            row["CPU"],
            row["Memory"],
            row["Network"],
            row["ConfigName"],
        )
    )
    for container_type_id, row in enumerate(rows):
        row["ContainerTypeID"] = container_type_id
    return rows


def ensure_cluster_instance_sizes_match(catalogs: dict[str, list[dict]]) -> list[dict]:
    """Return the common catalog, or fail if a multi-cluster group differs."""
    if not catalogs:
        raise ValueError("cluster group must not be empty")

    reference_cluster, reference = next(iter(catalogs.items()))
    for cluster, catalog in list(catalogs.items())[1:]:
        if catalog != reference:
            reference_by_key = {
                (row["InstanceType"], row["ConfigName"]): {
                    key: value for key, value in row.items() if key != "ContainerTypeID"
                }
                for row in reference
            }
            catalog_by_key = {
                (row["InstanceType"], row["ConfigName"]): {
                    key: value for key, value in row.items() if key != "ContainerTypeID"
                }
                for row in catalog
            }
            missing = sorted(reference_by_key.keys() - catalog_by_key.keys())
            extra = sorted(catalog_by_key.keys() - reference_by_key.keys())
            changed = sorted(
                key
                for key in reference_by_key.keys() & catalog_by_key.keys()
                if reference_by_key[key] != catalog_by_key[key]
            )
            raise ValueError(
                f"instance size catalogs differ between {reference_cluster} and {cluster} "
                f"after filtering deprecated configs: missing={missing}, extra={extra}, "
                f"changed={changed}"
            )
    return reference


def collect_group_instance_sizes(
    cluster_group: str,
    client_factory: Callable[[str], object],
) -> list[dict]:
    """Read and validate the bundle-controller size catalog of one cluster group."""
    try:
        clusters = CLUSTER_GROUPS[cluster_group]
    except KeyError as error:
        raise ValueError(f"unknown cluster group {cluster_group!r}") from error

    catalogs = {}
    for cluster in clusters:
        client = client_factory(cluster)
        catalogs[cluster] = build_cluster_instance_sizes(
            client.get(_TABLET_NODE_SIZES_PATH),
            client.get(_RPC_PROXY_SIZES_PATH),
            cluster=cluster,
        )

    sizes = ensure_cluster_instance_sizes_match(catalogs)
    node_count = sum(row["InstanceType"] == "node" for row in sizes)
    proxy_count = sum(row["InstanceType"] == "proxy" for row in sizes)
    print(
        f"cluster_group={cluster_group}, clusters={clusters}: "
        f"allowed node sizes={node_count}, proxy sizes={proxy_count}"
    )
    return sizes


def _load_cached_group_instance_sizes(cache_client: object, cache_path: str) -> dict:
    payload = json.loads(cache_client.read_file(cache_path).read())
    if not isinstance(payload, dict):
        raise TypeError(f"cached instance size catalog {cache_path!r} must be a map")
    if not isinstance(payload.get("sizes"), list):
        raise TypeError(f"cached instance size catalog {cache_path!r} has no sizes list")
    if not isinstance(payload.get("cluster_group_catalog_loaded_at"), str):
        raise TypeError(f"cached instance size catalog {cache_path!r} has no " "cluster_group_catalog_loaded_at string")
    return payload


def _store_cached_group_instance_sizes(
    cache_client: object,
    cache_path: str,
    payload: dict,
    account: str,
) -> None:
    cache_client.create(
        "file",
        cache_path,
        recursive=True,
        ignore_existing=True,
        attributes={"account": account},
    )
    # create(ignore_existing=True) does not update attributes of an existing
    # cache file, so keep old caches on the account selected for this run too.
    cache_client.set(f"{cache_path}/@account", account)
    cache_client.write_file(cache_path, json.dumps(payload).encode())


def collect_group_instance_sizes_with_fallback(
    cluster_group: str,
    client_factory: Callable[[str], object],
    cache_client: object,
    cache_path: str,
    *,
    account: str = "tmp",
    force_cluster_read: bool = False,
    loaded_at: str | None = None,
) -> dict:
    """Read a group catalog, falling back to its last successfully loaded value.

    Only exceptions raised by a cluster ``get`` are recoverable. Catalog parsing,
    cross-cluster validation and cache I/O errors intentionally propagate.
    """
    try:
        clusters = CLUSTER_GROUPS[cluster_group]
    except KeyError as error:
        raise ValueError(f"unknown cluster group {cluster_group!r}") from error

    catalogs = {}
    for cluster in clusters:
        # Client construction is deliberately outside the recoverable section.
        client = client_factory(cluster)
        try:
            tablet_node_sizes = client.get(_TABLET_NODE_SIZES_PATH)
            rpc_proxy_sizes = client.get(_RPC_PROXY_SIZES_PATH)
        except Exception as error:
            if force_cluster_read:
                raise
            print(
                f"cluster_group={cluster_group}: failed to read instance sizes "
                f"from {cluster}: {error}; loading {cache_path}"
            )
            return _load_cached_group_instance_sizes(cache_client, cache_path)

        # Everything after successful gets is strict: malformed or mismatching
        # catalogs must not be hidden by a stale cache.
        catalogs[cluster] = build_cluster_instance_sizes(
            tablet_node_sizes,
            rpc_proxy_sizes,
            cluster=cluster,
        )

    sizes = ensure_cluster_instance_sizes_match(catalogs)
    payload = {
        "sizes": sizes,
        "cluster_group_catalog_loaded_at": loaded_at or datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    _store_cached_group_instance_sizes(cache_client, cache_path, payload, account)

    node_count = sum(row["InstanceType"] == "node" for row in sizes)
    proxy_count = sum(row["InstanceType"] == "proxy" for row in sizes)
    print(
        f"cluster_group={cluster_group}, clusters={clusters}: "
        f"allowed node sizes={node_count}, proxy sizes={proxy_count}; "
        f"saved {cache_path}"
    )
    return payload
