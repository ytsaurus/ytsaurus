#!/usr/bin/env python

import yt.logger as logger
import yt.wrapper as yt

import argparse
import logging
import time

logger.set_formatter(logging.Formatter("%(asctime)-15s\t{}\t%(message)s".format(yt.config["proxy"]["url"])))

def is_locked(obj):
    return any(map(lambda l: l["mode"] in ["exclusive", "shared"], obj.attributes.get("locks", [])))

def get_destination_path(source_path, destination_dir, timestamp):
    # TODO(ostyakov): Use ypath.split (YT-7663)
    split_path = source_path[2:].split("/")
    split_path[0] = "{0}_{1}".format(split_path[0], timestamp)
    return yt.ypath_join(destination_dir, "/".join(split_path))

def main():
    parser = argparse.ArgumentParser(description="Move tmp nodes to trash.")
    parser.add_argument("--root", default="/")
    parser.add_argument("--trash-dir", required=True)
    parser.add_argument("--account", default="tmp")
    parser.add_argument("--exclude", action="append", default=["//tmp"])
    args = parser.parse_args()

    if not yt.exists(args.root):
        return

    script_start_timestamp = int(time.time())

    objects = []
    for obj in yt.search(args.root, enable_batch_mode=True, exclude=args.exclude,
                         attributes=["locks", "account", "type", "count"]):
        object_type = obj.attributes["type"]
        path = str(obj)

        if is_locked(obj) or obj.attributes.get("account") != args.account:
            continue

        if object_type not in ["map_node", "list_node"]:
            objects.append(path)

    batch_client = yt.create_batch_client()

    move_results = []
    for obj in objects:
        destination_path = get_destination_path(obj, args.trash_dir, script_start_timestamp)
        logger.info("Moving %s to %s", obj, destination_path)

        move_result = batch_client.move(obj, destination_path, recursive=True, force=True, preserve_account=False)
        move_results.append((obj, move_result))

    batch_client.commit_batch()
    for obj, move_result in move_results:
        if not move_result.is_ok():
            error = yt.YtResponseError(move_result.get_error())
            if not error.is_concurrent_transaction_lock_conflict() and not error.is_resolve_error():
                raise error

if __name__ == "__main__":
    main()
