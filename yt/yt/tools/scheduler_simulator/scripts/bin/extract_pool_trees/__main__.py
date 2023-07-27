#!/usr/bin/env python3

import yt.wrapper as yt
from yt import yson

import argparse


def get_pool_attributes():
    return [
        "aggressive_preemption_satisfaction_threshold",
        "allow_aggressive_starvation_preemption",
        "allowed_profiling_tags",
        "create_ephemeral_subpools",
        "custom_profiling_tag_filter",
        "default_parent_pool",
        "default_tree",
        "enable_aggressive_starvation",
        "enable_operations_profiling",
        "enable_pool_starvation",
        "enable_scheduling_tags",
        "ephemeral_subpool_config",
        "fair_share_preemption_timeout",
        "fair_share_preemption_timeout_limit",
        "fair_share_starvation_tolerance",
        "fair_share_starvation_tolerance_limit",
        "fifo_sort_parameters",
        "forbid_immediate_operations",
        "forbid_immediate_operations_in_root",
        "heartbeat_tree_scheduling_info_log_period",
        "historic_usage_config",
        "infer_children_weights_from_historic_usage",
        "infer_weight_from_min_share_ratio_multiplier",
        "job_count_preemption_timeout_coefficient",
        "max_ephemeral_pools_per_user",
        "max_operation_count",
        "max_operation_count_per_pool",
        "max_running_operation_count",
        "max_running_operation_count_per_pool",
        "max_share_ratio",
        "max_unpreemptible_running_job_count",
        "min_share_ratio",
        "min_share_resources",
        "mode",
        "nodes_filter",
        "packing",
        "preemption_satisfaction_threshold",
        "preemptive_scheduling_backoff",
        "resource_limits",
        "scheduling_tag",
        "scheduling_tag_filter",
        "tentative_tree_saturation_deactivation_period",
        "threshold_to_enable_max_possible_usage_regularization",
        "total_resource_limits_consider_delay",
        "update_preemptible_list_duration_logging_threshold",
        "weight"
    ]


def main():
    parser = argparse.ArgumentParser(description="Extracts information about current state of all pools in the cluster")
    parser.add_argument("--output", default=None)
    parser.add_argument("--proxy", default=yt.config["proxy"]["url"])
    args = parser.parse_args()

    if not args.proxy:
        raise EnvironmentError("yt proxy must be specified")
    yt.config["proxy"]["url"] = args.proxy

    if not args.output:
        args.output = "pool_trees_{}.yson".format(args.proxy)

    attribute_keys = get_pool_attributes()

    pool_trees_obj = yt.get("//sys/pool_trees", attributes=list(attribute_keys))
    with open(args.output, "wb") as output_file:
        yson.dump(pool_trees_obj, output_file, yson_format="pretty")


if __name__ == "__main__":
    main()
