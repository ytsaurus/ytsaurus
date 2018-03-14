#!/usr/bin/python

import yt.wrapper as yt
from yt.wrapper.config import get_config
from yt.wrapper.client import Yt

import argparse
import itertools
import logging
import re
import sys


MEMORY_COUNT_THRESHOLD = 1.1


class Bundle:
    def __init__(self, id):
        self.id = id
        self.tablet_cells = []
        self.tablet_slot_count = 0
        self.memory_count = 0
        self.assigned_nodes = []
        self.found_tag_filters = []

    def append_tablet_cells(self, tablet_cells, tablet_cell_memory_size):
        self.tablet_cells += tablet_cells
        self.tablet_slot_count += len(tablet_cells)
        self.memory_count += sum(tablet_cell_memory_size[key] for key in tablet_cells)

    def tablet_slot_filled(self, nodes=None):
        if nodes is None:
            nodes = self.assigned_nodes
        sum_assigned_table_slot_count = sum([node.tablet_slot_count for node in nodes])
        return sum_assigned_table_slot_count >= self.tablet_slot_count

    def memory_count_filled(self, nodes=None):
        if nodes is None:
            nodes = self.assigned_nodes
        if not nodes:
            return False
        min_assigned_memory_count = min([node.memory_count for node in nodes])
        return min_assigned_memory_count >= float(self.memory_count) / len(nodes) * MEMORY_COUNT_THRESHOLD

    def mean_memory_count_assigned(self):
        if not self.assigned_nodes:
            # We need to pick the first node somehow.
            return self.memory_count / 10
        sum_assigned_memory_count = sum([node.memory_count for node in self.assigned_nodes])
        return float(sum_assigned_memory_count) / len(self.assigned_nodes)

    def get_node_comparator(self, maximize=False):
        mean = self.mean_memory_count_assigned()
        multiplier = -1 if maximize else 1
        return lambda node: (node.memory_count - mean)**2 * multiplier

    def was_node_assigned_legacy(self, node):
        for tag_filter in self.found_tag_filters:
            if tag_filter in node.tags:
                logging.warning(
                    "Node `%s` has a legacy tag `%s` which matched a bundle filter, assumed as `%s`.",
                    node, tag_filter, self
                )
                return True
        return False

    def __str__(self):
        return self.id

    def __eq__(self, bundle):
        return self.id == bundle.id


class Node:
    def __init__(self, id, tablet_slots, memory_limit, tags, user_tags):
        self.id = id
        self.tablet_slots = tablet_slots
        self.tablet_slot_count = len(tablet_slots)
        self.memory_count = memory_limit
        self.tags = tags
        self.user_tags = user_tags
        self.assigned_bundle = None

    def __str__(self):
        return self.id

    def __eq__(self, node):
        return self.id == node.id


def quote_list(strs):
    return "[%s]" % ", ".join(map(lambda s: "`%s`" % s, strs))


def get_node_tag_filter(found_tag_filter, global_node_tag, bundle, flush_tag_filter):
    # Check that found node tag filter is not logical expression.
    if not re.match(r"^[\w/-]*$", found_tag_filter):
        msg = "Node tag filter `%s` for bundle `%s` is not plain." % (found_tag_filter, bundle)
        logging.error(msg)
        raise Exception(msg)

    if found_tag_filter.startswith(global_node_tag + "/"):
        return found_tag_filter

    # If bundle does not have explicit node tag filter, bundle name is used.
    if not found_tag_filter or flush_tag_filter:
        found_tag_filter = bundle

    # Global node_tag is prepended to found node tag filter to avoid ambiguity.
    return global_node_tag + "/" + found_tag_filter


def get_tablet_cell_memory_size():
    tablet_cell_attrs = yt.get("//sys/tablet_cells", attributes=["total_statistics"])
    return {
        key: value.attributes["total_statistics"]["memory_size"]
        for key, value in tablet_cell_attrs.iteritems()
    }


def get_bundles(req_bundles, node_tag, tablet_cell_memory_size, flush_tag_filter):
    bundle_attrs = yt.get("//sys/tablet_cell_bundles", attributes=["node_tag_filter", "tablet_cell_ids"])
    bundle_attrs = {key: value.attributes for key, value in bundle_attrs.iteritems()}

    bundle_node_tag_filters = {}
    merged_bundles = {}

    for bundle in req_bundles:
        # Check that requested bundle exists.
        if bundle not in bundle_attrs:
            msg = "Bundle `%s` not found among %s." % (bundle, quote_list(bundle_attrs.keys()))
            logging.error(msg)
            raise Exception(msg)

        found_tag_filter = bundle_attrs[bundle]["node_tag_filter"] if "node_tag_filter" in bundle_attrs[bundle] else ""
        logging.debug("Bundle `%s` has got node tag filter `%s`.", bundle, found_tag_filter)
        node_tag_filter = get_node_tag_filter(found_tag_filter, node_tag, bundle, flush_tag_filter)

        bundle_node_tag_filters[bundle] = node_tag_filter

        if node_tag_filter not in merged_bundles:
            merged_bundles[node_tag_filter] = Bundle(node_tag_filter)

        merged_bundle = merged_bundles[node_tag_filter]
        merged_bundles[node_tag_filter].append_tablet_cells(
            bundle_attrs[bundle]["tablet_cell_ids"], tablet_cell_memory_size
        )
        if found_tag_filter:
            merged_bundles[node_tag_filter].found_tag_filters.append(found_tag_filter)

    return merged_bundles, bundle_node_tag_filters


def is_node_ok(node_attrs):
    return node_attrs["state"] == "online" and not node_attrs["banned"] and not node_attrs["decommissioned"]


def get_nodes(node_tag):
    node_attrs = yt.get("//sys/nodes", attributes=[
        "state", "tablet_slots", "tags", "user_tags", "statistics", "decommissioned", "banned"
    ])
    node_attrs = {
        key: value.attributes for key, value in node_attrs.iteritems() if
            is_node_ok(value.attributes) and node_tag in value.attributes["tags"]
    }
    return {
        node_id: Node(
            node_id,
            node_attrs[node_id]["tablet_slots"],
            int(node_attrs[node_id]["statistics"]["memory"]["tablet_static"]["limit"]),
            node_attrs[node_id]["tags"],
            node_attrs[node_id]["user_tags"],
        ) for node_id in node_attrs
    }


def determine_node_to_bundle(bundles, nodes, tablet_cell_memory_size):
    node_to_bundle = {}
    all_tablet_cells = set(itertools.chain(*[bundles[bundle_id].tablet_cells for bundle_id in bundles]))

    for node_id in nodes:
        node = nodes[node_id]

        # Find mistaken tablet cells.
        bad_tablet_slots = []
        for tablet_slot in node.tablet_slots:
            if tablet_slot["state"] != "none" and tablet_slot["cell_id"] not in all_tablet_cells:
                logging.warning(
                    "Tablet slot on node `%s` contains cell `%s` out of managed bundles.",
                    node_id, tablet_slot["cell_id"]
                )
                node.tablet_slot_count -= 1
                node.memory_count -= tablet_cell_memory_size[tablet_slot["cell_id"]] * MEMORY_COUNT_THRESHOLD

        # Determine the bundle this node is allocated to.
        found_bundles = [
            bundle_id for bundle_id in bundles if
                bundle_id in node.tags or bundles[bundle_id].was_node_assigned_legacy(node)
        ]
        if len(found_bundles) > 1:
            logging.warning(
                "Tags for node `%s` contain multiple filters: %s, assumed as not assigned.",
                node_id, quote_list(found_bundles)
            )
        node_to_bundle[node_id] = found_bundles[0] if len(found_bundles) == 1 else None

    return node_to_bundle


def set_bundle_node_relationships(bundles, nodes, node_to_bundle):
    for node_id in node_to_bundle:
        node = nodes[node_id]

        if node_to_bundle[node_id] is not None:
            bundle = bundles[node_to_bundle[node_id]]
            node.assigned_bundle = bundle
            bundle.assigned_nodes.append(node)
        else:
            node.assigned_bundle = None


def process_filled(bundles, nodes):
    for bundle_id in bundles:
        bundle = bundles[bundle_id]

        if not bundle.tablet_slot_filled() or not bundle.memory_count_filled():
            continue

        logging.info("Bundle `%s` is allright.", bundle)

        mean_memory_count_assigned = bundle.mean_memory_count_assigned()
        assigned_nodes = sorted(bundle.assigned_nodes, key=bundle.get_node_comparator(maximize=True))

        for node in assigned_nodes:
            new_assigned_nodes = list(bundle.assigned_nodes)
            new_assigned_nodes.remove(node)

            if bundle.tablet_slot_filled(new_assigned_nodes) and bundle.memory_count_filled(new_assigned_nodes):
                if node.tablet_slot_count == 0:
                    logging.warning(
                        "Node `%s` is assigned to bundle `%s`, however it has no tablet slots \
                        (they might be used by bundles out of scope).",
                        node, bundle
                    )
                logging.info("Node `%s` is being removed from bundle `%s`.", node, bundle)
                node.assigned_bundle = None
                bundle.assigned_nodes = new_assigned_nodes


def process_not_filled(bundles, nodes):
    empty_nodes = [
        nodes[node] for node in nodes
            if nodes[node].assigned_bundle is None and nodes[node].tablet_slot_count > 0
    ]

    for bundle_id in bundles:
        bundle = bundles[bundle_id]

        if not bundle.tablet_slot_filled():
            logging.info("Bundle `%s` lacks tablet slots.", bundle)
        if not bundle.memory_count_filled():
            logging.info("Bundle `%s` lacks memory count.", bundle)

        failed = False
        while not bundle.tablet_slot_filled() or not bundle.memory_count_filled():
            if not empty_nodes:
                logging.warning("No free nodes to be provided for bundle `%s`.", bundle)
                failed = True
                break

            min_memory = bundle.memory_count / bundle.tablet_slot_count * MEMORY_COUNT_THRESHOLD
            nodes = sorted(empty_nodes, key=bundle.get_node_comparator(maximize=False))
            node = None
            for node_candidate in nodes:
                if node_candidate.memory_count >= min_memory:
                    node = node_candidate
                    break

            if node is None:
                logging.warning("All free nodes have not enough memory for bundle `%s.", bundle)
                failed = True
                break

            logging.info("Node `%s` is being assigned to bundle `%s`.", node, bundle)
            node.assigned_bundle = bundle
            bundle.assigned_nodes.append(node)
            empty_nodes.remove(node)

        if not failed:
            logging.info("Bundle `%s` is allright after performed moves.", bundle)

    logging.info("%d free nodes left.", len(empty_nodes))


def set_node_user_tags(nodes, node_tag, dry_run):
    new_tags = {}
    for node_id in nodes:
        node = nodes[node_id]
        # Save user tags not related to this script.
        tags = [tag for tag in node.user_tags if not tag.startswith(node_tag + "/")]
        if node.assigned_bundle is not None:
            tags.append(node.assigned_bundle.id)
        if tags != node.user_tags:
            new_tags[node_id] = tags

    batch_client = Yt(config=get_config(None)).create_batch_client() if not dry_run else None
    for node_id in new_tags:
        logging.debug("yt set //sys/nodes/%s/@user_tags \"%s\"", node_id, new_tags[node_id])
        if batch_client is not None:
            batch_client.set("//sys/nodes/%s/@user_tags" % node_id, new_tags[node_id])
    if batch_client is not None:
        batch_client.commit_batch()


def set_bundle_node_tag_filters(bundle_node_tag_filters, dry_run):
    batch_client = Yt(config=get_config(None)).create_batch_client() if not dry_run else None
    for bundle_id in bundle_node_tag_filters:
        logging.debug(
            "yt set //sys/tablet_cell_bundles/%s/@node_tag_filter \"%s\"",
            bundle_id, bundle_node_tag_filters[bundle_id]
        )
        if batch_client is not None:
            batch_client.set(
                "//sys/tablet_cell_bundles/%s/@node_tag_filter" % bundle_id,
                bundle_node_tag_filters[bundle_id]
            )
    if batch_client is not None:
        batch_client.commit_batch()


def process(bundles, node_tag, dry_run, flush_tag_filter):
    logging.info(
        "Balancing nodes for bundles %s using nodes with tag `%s`.", quote_list(bundles), node_tag,
    )

    tablet_cell_memory_size = get_tablet_cell_memory_size()
    bundles, bundle_node_tag_filters = get_bundles(
        bundles, node_tag, tablet_cell_memory_size, flush_tag_filter
    )
    nodes = get_nodes(node_tag)

    logging.info("%d online nodes found.", len(nodes))

    node_to_bundle = determine_node_to_bundle(bundles, nodes, tablet_cell_memory_size)
    set_bundle_node_relationships(bundles, nodes, node_to_bundle)

    for node_id in nodes:
        node = nodes[node_id]
        if node.tablet_slot_count == 0:
            continue
        logging.debug(
            "Node `%s` has %d free tablet slots with %d free memory count.",
            node, node.tablet_slot_count, node.memory_count
        )

    for bundle_id in bundles:
        bundle = bundles[bundle_id]
        logging.debug(
            "Bundle `%s` has %d tablet cells with %d tablet static.",
            bundle, bundle.tablet_slot_count, bundle.memory_count
        )

    process_filled(bundles, nodes)
    process_not_filled(bundles, nodes)

    set_node_user_tags(nodes, node_tag, dry_run=dry_run)
    set_bundle_node_tag_filters(bundle_node_tag_filters, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="Bundle Balancing: manages nodes within set of bundles")
    parser.add_argument("bundle", nargs="+", help="Bundles to be managed")
    parser.add_argument("--node_tag", action="store", help="Dedicated node filter tag", required=True)
    parser.add_argument("--proxy", type=yt.config.set_proxy, default=None, help="YT proxy")
    parser.add_argument("--dry-run", action="store_true", default=None, help="Only print output")
    parser.add_argument("--flush-tag-filter", action="store_true", default=None, help="Flush node tag filter for all bundles")

    logging.basicConfig(
        stream=sys.stderr, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        args = parser.parse_args()

        if yt.get_attribute("//sys", "disable_bundle_balance", default=False):
            logging.info("Bundle balancer is disable by //sys/disable_bundle_balance")
            return

        process(args.bundle, args.node_tag, dry_run=args.dry_run, flush_tag_filter=args.flush_tag_filter)
    except KeyboardInterrupt:
        logging.error("Interrupted.")
    finally:
        logging.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
