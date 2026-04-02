import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson


def get_or_none(path: str) -> Any:
    try:
        return yt.get(path)
    except yt.YtError as e:
        if e.is_resolve_error():
            return None
        raise


class YtClusterDescriber:
    def fetch_dynamic_configs(self) -> Dict[str, Any]:
        result = {}
        configs = (
            ("master", "//sys/@config"),
            ("controller_agent", "//sys/controller_agents/config"),
            ("scheduler", "//sys/scheduler/config"),
            ("node", "//sys/cluster_nodes/@config"),
            ("rpc_proxy", "//sys/rpc_proxies/@config"),
            ("http_proxy", "//sys/http_proxies/@config"),
        )

        for key, path in configs:
            logger.info(f"Fetching {key} dynamic config")
            value = get_or_none(path)
            if value is None:
                logger.warning(f"Config {key} not found at {path}")
            result[key] = value

        return result

    def fetch_nodes(self) -> Dict[str, Any]:
        attributes = (
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
        )

        logger.info("Fetching nodes")
        result = yt.get("//sys/cluster_nodes", attributes=attributes)
        logger.info(f"Fetched {len(result)} nodes")

        return result

    def fetch_bundles(self) -> Dict[str, Any]:
        attributes = (
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
        )

        logger.info("Fetching tablet cell bundles")
        result = yt.get("//sys/tablet_cell_bundles", attributes=attributes)
        logger.info(f"Fetched {len(result)} tablet cell bundles")

        return result

    def fetch_static_configs(self) -> Dict[str, Any]:
        logger.info("Fetching static node configs")

        nodes = yt.list("//sys/cluster_nodes", attributes=["state", "flavors"])
        per_flavor_configs = {}

        for node in nodes:
            if node.attributes["state"] != "online":
                continue

            for flavor in node.attributes["flavors"]:
                if flavor in per_flavor_configs:
                    continue

                logger.info(f"Will fetch config of node {node} for flavor {flavor}")
                per_flavor_configs[flavor] = yt.get(f"//sys/cluster_nodes/{node}/orchid/config")

        return per_flavor_configs

    def describe_cluster(
        self,
        describe_nodes: bool,
        describe_bundles: bool,
        describe_dynamic_configs: bool,
        describe_static_configs: bool,
    ) -> Dict[str, Any]:
        result = {}

        describe_all = not any([describe_nodes, describe_bundles, describe_dynamic_configs, describe_static_configs])

        if describe_all or describe_nodes:
            result["nodes"] = self.fetch_nodes()

        if describe_all or describe_bundles:
            result["bundles"] = self.fetch_bundles()

        if describe_all or describe_dynamic_configs:
            result["dynamic_configs"] = self.fetch_dynamic_configs()

        if describe_all or describe_static_configs:
            result["static_configs"] = self.fetch_static_configs()

        return result


class YtTableDescriber:
    def fetch_table_attributes(self, path: str) -> Dict[str, Any]:
        lightweight_attributes = (
            "account",
            "acl",
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
            "external_cell_tag",
            "external",
            "flush_lag_time",
            "foreign",
            "hunk_erasure_codec",
            "id",
            "in_memory_mode",
            "inherit_acl",
            "last_commit_timestamp",
            "lock_count",
            "lock_mode",
            "locks",
            "media",
            "modification_time",
            "mount_config",
            "optimize_for",
            "path",
            "pivot_keys",
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
            "tablet_count_by_expected_state",
            "tablet_count_by_state",
            "tablet_count",
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
        )

        heavy_attributes = (
            "chunk_format_statistics",
            "chunk_media_statistics",
            "table_chunk_format_statistics",
            "tablets",
            "compression_statistics",
            "erasure_statistics",
            "optimize_for_statistics",
            "hunk_statistics",
        )

        logger.info(f"Fetching lightweight attributes of table {path}")
        result = yt.get(f"{path}/@", attributes=lightweight_attributes)
        logger.info("Lightweight attributes fetched")

        for attribute in heavy_attributes:
            logger.info(f'Fetching heavy attribute "{attribute}" of table {path}')
            result[attribute] = yt.get(f"{path}/@{attribute}")

        return result

    def describe_tables(self, paths: List[str]) -> Dict[str, Any]:
        result = {}
        for path in paths:
            result[path] = self.fetch_table_attributes(path)
        return result


class OutputManager:
    @staticmethod
    def save_to_file(data: Any, filepath: str, output_format: str) -> None:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if output_format == "yson":
            content = yson.dumps(data, yson_format="pretty")
            with open(filepath, "wb") as f:
                f.write(content)
        elif output_format == "json":
            import json

            content = json.dumps(yson.yson_to_json(data), indent=4)
            with open(filepath, "w") as f:
                f.write(content)
        elif output_format == "yaml":
            import yaml

            content = yaml.dump(yson.yson_to_json(data), default_flow_style=False)
            with open(filepath, "w") as f:
                f.write(content)

        logger.info(f"Saved to {filepath}")

    @staticmethod
    def get_cluster_filepath(output_dir: str, output_format: str) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        return os.path.join(output_dir, f"cluster_{timestamp}.{output_format}")

    @staticmethod
    def get_tables_filepath(output_dir: str, output_format: str) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        return os.path.join(output_dir, f"tables_{timestamp}.{output_format}")


def describe_cluster(args: argparse.Namespace) -> None:
    content = YtClusterDescriber().describe_cluster(
        args.nodes,
        args.bundles,
        args.dynamic_configs,
        args.static_configs,
    )

    filepath = OutputManager.get_cluster_filepath(args.output, args.format)
    OutputManager.save_to_file(content, filepath, args.format)


def describe_tables(args: argparse.Namespace) -> None:
    content = YtTableDescriber().describe_tables(args.paths)

    filepath = OutputManager.get_tables_filepath(args.output, args.format)
    OutputManager.save_to_file(content, filepath, args.format)


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", choices=["yson", "json", "yaml"], default="yson")
    parser.add_argument("-o", "--output", type=str, metavar="dir", default="describe", help="Output directory path")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch various debug info from YT cluster", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--proxy", type=yt.config.set_proxy)

    subparsers = parser.add_subparsers(dest="command", required=True)

    cluster_parser = subparsers.add_parser(
        "cluster", help="General information about cluster components and their configs", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    cluster_parser.set_defaults(func=describe_cluster)
    cluster_parser.add_argument("--nodes", action="store_true", help="Fetch cluster nodes information")
    cluster_parser.add_argument("--bundles", action="store_true", help="Fetch tablet cell bundles information")
    cluster_parser.add_argument("--dynamic-configs", action="store_true", help="Fetch dynamic configurations")
    cluster_parser.add_argument("--static-configs", action="store_true", help="Fetch static node configurations")
    add_common_arguments(cluster_parser)

    table_parser = subparsers.add_parser("table", help="Attributes of certain tables", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    table_parser.set_defaults(func=describe_tables)
    table_parser.add_argument("paths", nargs="+", help="Table paths")
    add_common_arguments(table_parser)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
