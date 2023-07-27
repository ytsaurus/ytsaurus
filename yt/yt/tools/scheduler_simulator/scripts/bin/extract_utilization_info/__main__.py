#!/usr/bin/env python3

from yt.tools.scheduler_simulator.scripts.lib import (
    compose_path_templated,
    create_default_parser,
    extract_timeframe,
    print_info,
    set_default_config,
    time_filter,
)

import yt.wrapper as yt

from functools import partial
from collections import defaultdict


def resources_add(resources, to_add):
    for resource, value in to_add.items():
        resources[resource] += value


def create_utilization_info_mapper(filters=()):
    def mapper(row):
        if row["event_type"] != "nodes_info":
            return
        if not all(predicate(row) for predicate in filters):
            return
        resource_usage = defaultdict(int)
        resource_limits = defaultdict(int)
        for _, node_info in row["nodes"].items():
            resources_add(resource_usage, node_info["resource_usage"])
            resources_add(resource_limits, node_info["resource_limits"])
        assert set(resource_usage.keys()) == set(resource_limits.keys())
        yield {"timestamp": row["timestamp"],
               "resource_usage": resource_usage,
               "resource_limits": resource_limits}

    return mapper


def main():
    set_default_config(yt.config)

    parser = create_default_parser({
        "description": "Extract cluster utilization info from scheduler logs",
        "name": "cluster_utilization"})

    args = parser.parse_args()
    compose_path = partial(compose_path_templated, args)

    logs = compose_path("")

    print_info("Input: {}".format(args.input))
    print_info("Logs: {}".format(logs))

    filters = []
    if args.filter_by_timestamp:
        lower_limit, upper_limit = extract_timeframe(args)
        print_info("Timeframe: {} UTC - {} UTC".format(lower_limit, upper_limit))
        filters.append(partial(time_filter, lower_limit, upper_limit))

    yt.run_map(
        create_utilization_info_mapper(filters),
        source_table=args.input,
        destination_table=logs,
        memory_limit=2 * 1024 ** 3)

    yt.run_sort(logs, sort_by="timestamp")


if __name__ == "__main__":
    main()
