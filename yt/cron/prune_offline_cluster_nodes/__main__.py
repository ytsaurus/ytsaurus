#!/usr/bin/env python

import yt.wrapper as yt

import datetime
import logging
import time

from argparse import ArgumentParser, BooleanOptionalAction


cluster_node_ttl = "//sys/cluster_nodes/@node_ttl"


def get_node_ttl(node_ttl_default: int, node_ttl_path: str) -> float:
    try:
        return yt.get(node_ttl_path) / 1000
    except yt.YtResponseError as err:
        if err.is_resolve_error():
            logging.warning("Path %s is missing, using %s ttl", node_ttl_path, node_ttl_default)
            return node_ttl_default / 1000


def is_node_flavor_enabled(node, node_flavor_tags: list[str]) -> bool:
    for tag in node.attributes["tags"]:
        if tag in node_flavor_tags:
            return True
    return False


def prune(dry_run: bool, nodes_path: str, node_flavors: list[str], node_ttl: int):
    logging.info("Cleaning up %s nodes which are offline for more than %d seconds (dry run mode enabled: %s)", node_flavors, node_ttl, dry_run)

    nodes_list = yt.list(nodes_path, attributes=["state", "tags", "last_seen_time"])

    unix_now = time.time()

    for node in nodes_list:
        if node.attributes["state"] != "offline":
            continue
        if not is_node_flavor_enabled(node, node_flavors):
            continue

        last_seen_dt = datetime.datetime.fromisoformat(node.attributes["last_seen_time"])

        if last_seen_dt.timestamp() + node_ttl >= unix_now:
            logging.debug("Skip %s node: ttl hasn't expired yet (last seen: %s)", node, node.attributes["last_seen_time"])
            continue

        logging.warning("Removing %s node %s (last seen at %s)", node.attributes["state"], node, last_seen_dt)
        if dry_run:
            continue

        yt.remove("{}/{}".format(nodes_path, node), True)


def main():
    parser = ArgumentParser(description="Prunes offline cluster nodes from Cypress")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--path", default="//sys/cluster_nodes", help="Search path (//sys/cluster_nodes by default)")
    parser.add_argument("--node-flavors", default=["exe", "flavor:exec"], help="Comma separated of node flavors to be processes (exec by default)")
    parser.add_argument("--node-ttl", default=86_400_000, type=int, help="Default offline node TTL (24 hrs by default, in milliseconds)")
    parser.add_argument("--node-ttl-path", default="//sys/cluster_nodes/@node_ttl", help="YT path to node_ttl attribute (//sys/cluster_nodes/@node_ttl by default)")
    parser.add_argument("--dry-run", action=BooleanOptionalAction, default=True, help="Dry run mode (enabled by default)")
    args = parser.parse_args()

    logging.root.setLevel(args.log_level)

    node_ttl = get_node_ttl(args.node_ttl, args.node_ttl_path)

    prune(args.dry_run, args.path, args.node_flavors, node_ttl)


if __name__ == "__main__":
    main()
