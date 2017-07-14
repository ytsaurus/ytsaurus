#!/usr/bin/env python

from yt.wrapper.common import parse_bool
import yt.logger as logger

import yt.wrapper as yt

from argparse import ArgumentParser

def safe_get(path, **kwargs):
    try:
        return yt.get(path, **kwargs)
    except yt.YtResponseError as err:
        if err.is_access_denied():
            logger.warning("Failed to get node %s, access denied", path)
        else:
            raise

    return None

def prune(root):
    tables_to_prune = []
    map_nodes_to_prune = []
    ignore_nodes = ["//sys", "//home/qe"]

    requested_attributes = ["type", "opaque", "prune_empty_tables", "prune_empty_map_nodes",
                            "row_count", "count", "revision"]

    def walk(path, object, prune_empty_tables=False, prune_empty_map_nodes=False):
        if path in ignore_nodes:
            return

        if object.attributes["type"] == "table":
            if prune_empty_tables and object.attributes.get("row_count") == 0:
                tables_to_prune.append((path, object.attributes["revision"]))
                return True
        elif object.attributes["type"] == "map_node":
            if parse_bool(object.attributes.get("opaque", False)):
                object = safe_get(path, attributes=requested_attributes)
                if object is None:
                    return False

            attributes = object.attributes
            for key, value in object.iteritems():
                remove = walk(
                    "{0}/{1}".format(path, yt.escape_ypath_literal(key)),
                    value,
                    parse_bool(attributes.get("prune_empty_tables", prune_empty_tables)),
                    parse_bool(attributes.get("prune_empty_map_nodes", prune_empty_map_nodes)))

                if remove:
                    object.attributes["count"] -= 1

            if object.attributes["count"] == 0 and prune_empty_map_nodes:
                map_nodes_to_prune.append(path)
                return True
        else:
            logger.debug("Skipping %s %s", object.attributes["type"], path)

        return False

    root_obj = safe_get(root, attributes=requested_attributes)
    walk(root, root_obj)

    logger.info("Collected %d tables and %d map_node for pruning",
                len(tables_to_prune), len(map_nodes_to_prune))

    for path, revision in tables_to_prune:
        try:
            if yt.exists(path):
                with yt.Transaction():
                    yt.lock(path)
                    if yt.get_attribute(path, "revision") == revision:
                        yt.remove(path, force=True)
                    else:
                        logger.info("Table %s revision mismatch, skipping", path)
        except yt.YtResponseError as err:
            if err.is_concurrent_transaction_lock_conflict():
                logger.warning("Table %s is locked", path)
            else:
                raise

    for path in map_nodes_to_prune:
        try:
            yt.remove(path, force=True)
        except yt.YtResponseError as err:
            if err.is_concurrent_transaction_lock_conflict():
                logger.warning("Map node %s is locked", path)
            elif err.contains_text("non-empty composite"):
                logger.warning("Node %s is non-empty", path)
            else:
                raise

def main():
    parser = ArgumentParser(description="Prunes empty tables and map_node from Cypress")
    parser.add_argument("--path", default="/", help='Search path. Default is cypress root "/"')
    args = parser.parse_args()
    prune(args.path)

if __name__ == "__main__":
    main()
