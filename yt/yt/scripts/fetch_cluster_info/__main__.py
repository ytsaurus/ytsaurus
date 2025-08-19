import argparse
import yt.wrapper as yt
import json
import logging


def try_get_path(path):
    try:
        return yt.get(path)
    except yt.YtError as e:
        if e.is_resolve_error():
            return None
        raise


def fetch_dynamic_configs():
    result = {}
    for key, path in (
        ("master", "//sys/@config"),
        ("controller_agent", "//sys/controller_agents/config"),
        ("scheduler", "//sys/scheduler/config"),
        ("node", "//sys/cluster_nodes/@config"),
        ("rpc_proxy", "//sys/rpc_proxies/@config"),
        ("http_proxy", "//sys/http_proxies/@config"),
    ):
        logging.info(f"Fetching {key.replace('_', ' ')} dynamic config")
        result[key] = try_get_path(path)
    return {"dynamic_config": result}


def fetch_nodes():
    attributes = [
        "banned",
        "decommissioned",
        "disable_write_sessions",
        "disable_scheduler_jobs",
        "disable_tablet_cells",
        "pending_restart",
        "maintenance_requests",
        "rack",
        "data_center",
        "state",
        "multicell_states",
        "user_tags",
        "tags",
        "annotations",
        "version",
        "register_time",
        "statistics",
        "alerts",
        "flavors",
        "tablet_slots",
    ]

    logging.info("Fetching nodes")
    result = yt.get("//sys/cluster_nodes", attributes=attributes)
    logging.info(f"Fetched {len(result)} nodes")

    return {"cluster_nodes": result}


def fetch_bundles():
    attributes = [
        "cell_balancer_config",
        "dynamic_options",
        "health",
        "node_tag_filter",
        "nodes",
        "options",
        "resource_limits",
        "resource_usage",
        "tablet_balancer_config",
        "tablet_cell_ids",
        "user_attributes",
    ]

    logging.info("Fetching tablet cell bundles")
    result = yt.get("//sys/tablet_cell_bundles", attributes=attributes)
    logging.info(f"Fetched {len(result)} tablet cell bundles")

    return {"tablet_cell_bundles": result}


def fetch_static_node_configs():
    logging.info("Fetching static node configs")

    nodes = yt.list("//sys/cluster_nodes", attributes=["state", "flavors"])
    per_flavor_configs = {}
    for node in nodes:
        if node.attributes["state"] != "online":
            continue
        for flavor in node.attributes["flavors"]:
            if flavor in per_flavor_configs:
                continue
            logging.info(f"Will fetch config of node {node} for flavor {flavor}")
            per_flavor_configs[flavor] = yt.get(
                f"//sys/cluster_nodes/{node}/orchid/config")

    return {"static_config": {"nodes": per_flavor_configs}}


def fetch_cluster_info(args):
    if args.all:
        args.nodes = True
        args.bundles = True
        args.dynamic_configs = True
        args.static_node_configs = True

    result = {}

    if args.nodes:
        result |= fetch_nodes()
    if args.bundles:
        result |= fetch_bundles()
    if args.dynamic_configs:
        result |= fetch_dynamic_configs()
    if args.static_node_configs:
        result |= fetch_static_node_configs()

    if not result:
        raise RuntimeError(
            "No flags specified for fetching cluster info, specify "
            "necessary flags or --all")

    return result


def fetch_table_attributes(path):
    attributes = [
        "account",
        "actual_tablet_state",
        "assigned_mount_config_experiments",
        "atomicity",
        "backup_state",
        "chunk_count",
        "chunk_merger_mode",
        "chunk_merger_status",
        "chunk_merger_traversal_info",
        "chunk_row_count",
        "compressed_data_size",
        "compression_codec",
        "compression_ratio",
        "creation_time",
        "data_weight",
        "dynamic",
        "effective_mount_config",
        "enable_consistent_chunk_replica_placement",
        "enable_detailed_profiling",
        "enable_dynamic_store_read",
        "enable_striped_erasure",
        "erasure_codec",
        "estimated_creation_time",
        "expected_tablet_state",
        "external",
        "external_cell_tag",
        "flush_lag_time",
        "foreign",
        "hunk_erasure_codec",
        "id",
        "in_memory_mode",
        "last_commit_timestamp",
        "lock_count",
        "lock_mode",
        "locks",
        "media",
        "mount_config",
        "optimize_for",
        "path",
        "preload_state",
        "primary_medium",
        "queue_agent_stage",
        "remount_needed_tablet_count",
        "replication_factor",
        "replication_progress",
        "resource_usage",
        "retained_timestamp",
        "schema",
        "serialization_type",
        "sorted",
        "tablet_backup_state",
        "tablet_balancer_config",
        "tablet_cell_bundle",
        "tablet_count",
        "tablet_count_by_expected_state",
        "tablet_count_by_state",
        "tablet_error_count",
        "tablet_state",
        "tablet_statistics",
        "treat_as_queue_consumer",
        "treat_as_queue_producer",
        "type",
        "uncompressed_data_size",
        "unflushed_timestamp",
        "unmerged_row_count",
        "update_mode",
        "upstream_replica_id",
        "user_attributes",
        "vital",
    ]

    heavy_attributes = [
        "chunk_format_statistics",
        "chunk_media_statistics",
        "table_chunk_format_statistics",
        "tablets",
        "compression_statistics",
        "erasure_statistics",
        "optimize_for_statistics",
        "hunk_statistics",
    ]

    logging.info(f"Fetching lightweight attributes of table {path}")
    result = yt.get(f"{path}/@", attributes=attributes)
    logging.info("Lightweight attributes fetched")
    for attribute in heavy_attributes:
        logging.info(f'Fetching heavy attribute "{attribute}" of table {path}')
        result |= {attribute: yt.get(f"{path}/@{attribute}")}

    return {path: result}


def fetch_tables(args):
    result = {}
    for path in args.path:
        result |= fetch_table_attributes(path)
    return result


def main():
    parser = argparse.ArgumentParser(description="Fetch various debug info from YT cluster")
    parser.add_argument("--proxy", type=yt.config.set_proxy)
    parser.add_argument("--format", choices=["yson", "json"], default="yson")

    subparsers = parser.add_subparsers(dest="command", required=True)

    config_parser = subparsers.add_parser(
        "cluster_info",
        help="General information about cluster components and their configs")
    config_parser.set_defaults(func=fetch_cluster_info)
    config_parser.add_argument("--nodes", action="store_true")
    config_parser.add_argument("--bundles", action="store_true")
    config_parser.add_argument("--dynamic-configs", action="store_true")
    config_parser.add_argument("--static-node-configs", action="store_true")
    config_parser.add_argument("--all", action="store_true")

    table_parser = subparsers.add_parser(
        "table",
        help="Attributes of certain tables")
    table_parser.set_defaults(func=fetch_tables)
    table_parser.add_argument("path", nargs="+", help="Table paths")

    args = parser.parse_args()
    result = args.func(args)

    if args.format == "yson":
        print(yt.yson.dumps(result, yson_format="pretty").decode())
    elif args.format == "json":
        print(json.dumps(yt.yson.yson_to_json(result)))
    else:
        raise RuntimeError(f'Invalid format "{args.format}"')


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    main()
