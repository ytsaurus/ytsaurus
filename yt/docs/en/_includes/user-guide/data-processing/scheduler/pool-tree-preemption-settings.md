- `preemptive_scheduling_backoff`: Limit on how often allocations can be scheduled with preemption on a single node.
- `fair_share_starvation_timeout`: If an operation doesn't receive its resources before the timeout expires, it enters a starving state, and its allocations can be scheduled with preemption.
- `fair_share_starvation_tolerance`: Threshold value indicating that an operation isn't receiving enough resources. An operation is considered starving (`below_fair_share` status) if `usage_share < fair_share * tolerance` for the dominant resource.  
{ #fair-share-starvation-tolerance }
- `preemption_satisfaction_threshold`: Threshold value that determines whether allocations are preemptible (including aggressively preemptible) or non‑preemptible. Allocations where the cumulative `usage_share` exceeds the `fair_share * threshold` are considered preemptible, while all others are either non‑preemptible or aggressively preemptible.
- `non_preemptible_resource_usage_threshold`: The operation will not be subject to preemption if its resource usage is less than this value.
- `allocation_preemption_timeout`: An allocation preemption timeout. If a job in a given allocation supports interruption, the system ensures that the job has enough time to complete successfully before termination.
- `aggressive_preemption_satisfaction_threshold`: Threshold value that determines whether allocations are non-preemptible or aggressively preemptible (or just preemptible). Allocations where the cumulative `usage_share` exceeds the `fair_share * threshold` are considered either aggressively preemptible or just preemptible. This threshold must not exceed `preemption_satisfaction_threshold`.

