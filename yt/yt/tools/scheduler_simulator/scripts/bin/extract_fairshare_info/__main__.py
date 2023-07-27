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

import itertools

from functools import partial


def create_extract_fairshare_info_mapper(filters=()):
    def mapper(row):
        if not row["event_type"].startswith("fair_share_info"):
            return
        if not all(predicate(row) for predicate in filters):
            return
        yield row

    return mapper


def extract_pools_distribution(params, row):
    timestamp = row["timestamp"]
    target = params["target"]
    metric = params["metric"]
    pools = row[target]
    pools_formatted = {}
    for pool in pools.items():
        name = pool[0]
        fair_share = pool[1][metric]
        pools_formatted[name] = fair_share
    yield {"pools": pools_formatted, "timestamp": timestamp}


def main():
    set_default_config(yt.config)

    parser = create_default_parser({
        "description": "Extract fairness metrics from logs",
        "name": "info"})

    args = parser.parse_args()
    compose_path = partial(compose_path_templated, args)

    targets = ["pools", "operations"]
    metrics = ["fair_share_ratio", "demand_ratio", "usage_ratio"]

    tuples = itertools.product(targets, metrics)
    params = [{"target": target, "metric": metric} for (target, metric) in tuples]

    raw_log = compose_path("raw")
    logs = [compose_path("_".join(param.values())) for param in params]

    print_info("Input: {}".format(args.input))
    print_info("Raw logs: {}".format(raw_log))
    print_info("Params logs:")
    for log in logs:
        print_info(log)

    filters = []
    if args.filter_by_timestamp:
        lower_limit, upper_limit = extract_timeframe(args)
        print_info("Timeframe: {} UTC - {} UTC".format(lower_limit, upper_limit))
        filters.append(partial(time_filter, lower_limit, upper_limit))

    yt.run_map(
        create_extract_fairshare_info_mapper(filters),
        args.input,
        raw_log,
        spec={"data_size_per_job": 2 * 1024 ** 3})

    for param, log in zip(params, logs):
        yt.run_map(partial(extract_pools_distribution, param), raw_log, log)
        yt.run_sort(log, sort_by="timestamp")


if __name__ == "__main__":
    main()
