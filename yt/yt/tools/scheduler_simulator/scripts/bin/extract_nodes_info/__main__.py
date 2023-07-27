#!/usr/bin/env python3

from yt.tools.scheduler_simulator.scripts.lib import print_info

import yt.yson as yson
import yt.wrapper as yt

from collections import defaultdict
import argparse
import re


def round_up(value, mod):
    if value % mod == 0:
        return value
    return ((value // mod) + 1) * mod


class NodeTagFilter(object):

    def __init__(self, ignore_warnings=False):
        self.white_list_tags = ["main", "internal", "external", "external_vla", "gpu", "xurma",
                                "yamaps", "porto", "nirvana", "core", "sys"]
        self.white_list_patterns = [re.compile(pattern) for pattern in [r"^gpu_.*"]]

        self.black_list_tags = ["max42_was_here", "psushin_test2", "psushin_test", "journals", "in_memory",
                                "toloka", "sbyt", "iss", "man", "iskhakovt_test", "iskhakovt_test/iskhakovt_test"]
        self.black_list_patterns = [re.compile(pattern)
                                    for pattern in [r"^SAS.*", r"^CLOUD.*", r".*\.yandex\.net", r"^tablet_common.*",
                                                    r"^MAN.*", r"^vla.*", r"^VLA.*"]]

        self.suspicious_tags = set()
        self.ignore_warnings = ignore_warnings

    def is_in_white_list(self, tag):
        return tag in self.white_list_tags or any(pattern.match(tag) for pattern in self.white_list_patterns)

    def is_in_black_list(self, tag):
        return tag in self.black_list_tags or any(pattern.match(tag) for pattern in self.black_list_patterns)

    def __call__(self, tag, node_name):
        in_white_list = self.is_in_white_list(tag)
        in_black_list = self.is_in_black_list(tag)

        if in_white_list and in_black_list:
            raise ValueError("Tag '{}' on node '{}' is both in white list and black list".format(tag, node_name))

        if in_black_list:
            return False

        if in_white_list:
            return True

        if tag not in self.suspicious_tags:
            if not self.ignore_warnings:
                print_info("WARNING: "
                           "Tag '{}' on node '{}' is neither in white list nor in black list. "
                           "Not filtered out by default. "
                           "Consider modifying either the white or the black list."
                           .format(tag, node_name))
            self.suspicious_tags.add(tag)
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Extracts information about resource limits of all nodes on the cluster")
    parser.add_argument("--output", default=None,
                        help="Name of output file. nodes_info_PROXY.yson by default.")
    parser.add_argument("--proxy", default=yt.config["proxy"]["url"],
                        help="Name of YT proxy.")
    parser.add_argument("--memory_round_up_mib", default=None, type=int,
                        help="If provided, all memory limits will be rounded up to the specified number of MiB. "
                             "This option is useful to decrease the resulting number of node groups.")
    parser.add_argument("--ignore_unknown_tag_warnings", action="store_true",
                        help="Ignore warnings about unknown tags. "
                             "Unknown tags are not filtered out by default. "
                             "It might drastically increase the resulting number of node groups.")
    args = parser.parse_args()

    if not args.proxy:
        raise EnvironmentError("yt proxy must be specified")
    yt.config["proxy"]["url"] = args.proxy

    if not args.output:
        args.output = "nodes_info_{}.yson".format(args.proxy)

    tag_filter = NodeTagFilter(ignore_warnings=args.ignore_unknown_tag_warnings)

    description_to_node_count = defaultdict(int)
    for node_name, node_object in yt.get("//sys/cluster_nodes", attributes=["resource_limits", "state", "tags"]).items():
        if node_object.attributes["state"] != "online" or "resource_limits" not in node_object.attributes:
            continue

        resources = node_object.attributes["resource_limits"]
        resources = {key: resources[key]
                     for key in resources
                     if key in ("cpu", "gpu", "user_slots", "user_memory", "network")}
        if args.memory_round_up_mib is not None and args.memory_round_up_mib != 0:
            resources["user_memory"] = round_up(resources["user_memory"], args.memory_round_up_mib * 1024 ** 2)
        resources["memory"] = resources["user_memory"]
        del resources["user_memory"]

        if resources["user_slots"] == 0:
            continue

        tags = node_object.attributes["tags"]
        if not tags or any(exclude_tag in tags for exclude_tag in ("cloud", "cloud_vla", "external", "external_vla")):
            continue

        tags = [tag for tag in tags if tag_filter(tag, node_name)]

        node_description = {
            "resource_limits": tuple(sorted(resources.items())),
            "tags": tuple(sorted(tags))
        }
        node_description_tuple = tuple(sorted(node_description.items()))
        description_to_node_count[node_description_tuple] += 1

    nodes_obj = []
    for node_description_tuple, node_count in sorted(description_to_node_count.items(), key=lambda item: item[1]):
        node_description = dict(node_description_tuple)
        node_description["resource_limits"] = dict(node_description["resource_limits"])
        node_description["tags"] = list(node_description["tags"])
        node_description["count"] = node_count
        nodes_obj.append(node_description)

    with open(args.output, "w") as output_file:
        yson.dump(nodes_obj, output_file, yson_format="pretty")


if __name__ == "__main__":
    main()
