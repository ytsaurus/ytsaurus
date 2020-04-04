import yt.wrapper as yt

import argparse

def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--cloud", action="store_true", help="Specify if cluster has cloud nodes")
    group.add_argument("--no-cloud", action="store_true", help="Specify if cluster does not have cloud nodes")
    args = parser.parse_args()

    yt.create("map_node", "//sys/pool_trees", ignore_existing=True)
    yt.copy("//sys/pools", "//sys/pool_trees/physical")
    yt.set("//sys/pool_trees/physical/@nodes_filter", "internal")
    yt.set("//sys/pool_trees/physical/@opaque", False)
    yt.link("//sys/pool_trees/physical", "//sys/pools", force=True)
    if args.cloud:
        yt.create("map_node", "//sys/pool_trees/cloud", attributes={"nodes_filter": "external"})
    yt.set("//sys/pool_trees/@default_tree", "physical")
    yt.set("//sys/pool_trees/@acl/end",
        {
            "permissions": ["use"],
            "action": "allow",
            "subjects": ["users"],
            "inheritance_mode": "object_and_descendants"
        }
    )

    keys = ["min_share_preemption_timeout", "fair_share_preemption_timeout",
            "fair_share_starvation_tolerance", "min_share_preemption_timeout_limit",
            "fair_share_preemption_timeout_limit", "fair_share_starvation_tolerance_limit",
            "max_unpreemptable_running_job_count", "max_running_operation_count",
            "max_running_operation_count_per_pool", "max_operation_count_per_pool",
            "max_operation_count", "enable_pool_starvation", "default_parent_pool",
            "forbid_immediate_operations_in_root", "job_count_preemption_timeout_coefficient",
            "preemption_satisfaction_threshold", "aggressive_preemption_satisfaction_threshold",
            "enable_scheduling_tags", "heartbeat_tree_scheduling_info_log_period", "max_ephemeral_pools_per_user",
            "update_preemptable_list_duration_logging_threshold", "enable_operations_profiling",
            "threshold_to_enable_max_possible_usage_regularization", "total_resource_limits_consider_delay",
            "preemptive_scheduling_backoff"]

    orchid_config = yt.get("//sys/scheduler/orchid/scheduler/config")

    if yt.exists("//sys/scheduler/config"):
        patch = yt.get("//sys/scheduler/config")
    else:
        patch = {}

    config = yt.common.update(orchid_config, patch)

    for key in keys:
        if key not in config:
            continue

        yt.set("//sys/pool_trees/physical/@" + key, config[key])

if __name__ == "__main__":
    main()
