#!/usr/bin/python3

# Move binaries from //sys/clickhouse/bin/* to //sys/bin/{kind}/*.

import argparse
import yt.wrapper
import sys
import yt.wrapper.common


dry_run = False
OLD_PATH = "//sys/clickhouse/bin"
NEW_PATH = "//sys/bin"


def move_file(node, client=None):
    name = str(node)
    params={
        "preserve_creation_time": True,
        "preserve_modification_time": True,
        # The mistake in the naming is intentional as there was the same mistake
        # in YT master. Keep both options for compatibility.
        "preserve_modifcation_time": True,
    }
    yt.wrapper.common.update_inplace(client.COMMAND_PARAMS, params)
    kinds = ("ytserver-clickhouse", "ytserver-log-tailer", "clickhouse-trampoline", "yt-start-clickhouse-clique")
    for kind in kinds:
        if name.startswith(kind):
            old_path = OLD_PATH + "/" + name
            new_path = NEW_PATH + "/" + kind + "/" + name
            print("Moving {} to {}, kind = {}".format(old_path, new_path, kind), file=sys.stderr)
            if not dry_run:
                return client.move(old_path, new_path)
            else:
                return
    print("Node {} is unrecognized".format(name), file=sys.stderr)
    assert False


def move_link(node, client=None):
    name = str(node)
    target_path = node.attributes["target_path"]
    if target_path.startswith(NEW_PATH):
        print("Skipping link node {} -> {} as it already points to the new path".format(node, target_path), file=sys.stderr)
        return None
    target_name = target_path[target_path.rindex("/")+1:]
    kinds = ("ytserver-clickhouse", "ytserver-log-tailer", "clickhouse-trampoline", "yt-start-clickhouse-clique")
    for kind in kinds:
        if name.startswith(kind):
            old_link_path = OLD_PATH + "/" + name
            new_link_path = NEW_PATH + "/" + kind + "/" + name
            new_target_path = NEW_PATH + "/" + kind + "/" + target_name
            print("Linking {} to {}, kind = {}".format(old_link_path, new_link_path, kind), file=sys.stderr)
            print("Linking {} to {}, kind = {}".format(new_link_path, new_target_path, kind), file=sys.stderr)
            if not dry_run:
                client.link(new_link_path, old_link_path, force=True)
                client.link(new_target_path, new_link_path, force=True)
                return
            else:
                return
    print("Node {} is unrecognized".format(name), file=sys.stderr)
    assert False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    global dry_run
    dry_run = args.dry_run

    nodes = yt.wrapper.list(OLD_PATH, attributes=["type", "target_path"])

    print("Node count = {}".format(len(nodes)), file=sys.stderr)

    link_nodes = []
    file_nodes = []

    for node in nodes:
        if node.attributes["type"] == "link":
            link_nodes.append(node)
        elif node.attributes["type"] == "file":
            file_nodes.append(node)
        else:
            print("Unrecognized type {}".format(node.attributes["type"]))
            assert False

    print("Moving files", file=sys.stderr)
    yt.wrapper.batch_apply(move_file, file_nodes)
    print("Files moved", file=sys.stderr)

    print("Moving links", file=sys.stderr)
    yt.wrapper.batch_apply(move_link, link_nodes)
    print("Links moved", file=sys.stderr)

if __name__ == "__main__":
    main()
