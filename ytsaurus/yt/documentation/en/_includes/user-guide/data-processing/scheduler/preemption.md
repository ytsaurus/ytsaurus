# Preemption

When it takes a long time to complete individual operations, an operation to take over the entire cluster and prevent other operations from running jobs is possible. In this case, other operations starve. To solve this problem, the scheduler has a preemption mechanism.

Each operation can be in one of three states:

* normal: The operation gets enough resources.
* starving for fair-share: The operation gets an amount of resources that is not less than its minimum guaranteed share, but significantly less than its fair-share.
* starving for min-share: The operation gets an amount of resources that is significantly less than even the minimum guaranteed share.

If the operation status differs from normal for a long time depending on the operation type, cluster settings, and operation size, the scheduler starts forcibly aborting jobs of other normal operations. In this case, jobs starting with recently run ones are subject to preemption.

## Preemption settings

Preemption is regulated by the following parameters (default values in brackets if set):

* `fair-share_starvation_tolerance`: The maximum allowable relative deviation of the usage_ratio of the operation from its fair-share at which the operation is not yet considered starving. The parameter is intended to overcome problems caused by resource fragmentation.
* `min_share_preemption_timeout`: The time after which an operation with `usage_ratio` less than `min_share_ratio` is considered starving.
* `fair-share_preemption_timeout`: The time after which an operation with `usage_ratio` less than `fair-share_ratio * tolerance` is considered starving.
* `enable_aggressive_starvation` (false): Enables aggressive preemption for the sake of operations in a given pool and in all descendants. Aggressive preemption is the ability to preempt jobs of other operations: all running operations, not just those in a given pool and descendants, up to the point when the operation has only half of its guaranteed resources left. This limit is adjusted in the scheduler settings separately for each cluster. This option enables you to guarantee the start time of large jobs that require a significant share of the cluster node resources.
* `allow_aggressive_starvation_preemption` (true): Allows aggressive preemption of jobs in a given pool. More precisely, preemption of jobs will not be prohibited by a given pool if the `usage_ratio` of the pool is greater than half (the exact limit value is the cluster setting) of the `fair-share_ratio`. When a cluster has aggressively starving operations, it is allowed by default to preempt the jobs of other operations up to half of the share allocated to non-starving operations. If strict timing guarantees are important for the process, this preemption may violate such guarantees. Therefore, aggressive preemption should be disabled for processes that require guarantees.