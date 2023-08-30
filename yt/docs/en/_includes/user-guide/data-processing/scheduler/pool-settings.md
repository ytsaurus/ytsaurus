# Pool characteristics

This section describes the characteristics and settings of pool trees, pools, and operations.

## Pool tree characteristics { #pool_trees }

A pool tree has the following attributes:

- `nodes_filter`: A filter to select cluster nodes belonging to the tree. The filter is a logical expression with & (logical "and"), | (logical "or"), and ! (logical "not") operators and round brackets. The variables in the expression are the possible tag values. If the cluster node has a specific tag, the logical expression takes the "true" value, if not — the "false" value. If the entire expression takes the "true" value, such a cluster node is considered to belong to the tree. Trees must not overlap by cluster nodes, the scheduler monitors that the filter is set correctly and cluster node tags are correct. If there is a query from a cluster node with tags that fall under several trees, the scheduler does not allow the cluster node to register.
- `max_operation_count`: The default value for the maximum number of concurrently started (running + pending) operations in the root pool.
- `max_operation_count_per_pool`: The default value for the maximum number of concurrently started operations in the pool. Can be overwritten by a setting on the pool.
- `max_running_operation_count`: The default value for the maximum number of concurrently running operations in the root pool.
- `max_running_operation_count_per_pool`: The default value for the maximum number of concurrently running operations in the pool. Can be overwritten by a setting on the pool.
- `default_parent_pool`: The default pool, in which operations that do not have a pool indicated in the specification will be started.
- `enable_pool_starvation`: Allow pool "starving". For more information, see [Preemption](../../../../user-guide/data-processing/scheduler/preemption.md).
- `forbid_immediate_operations_in_root`: Allow operations to run in the root pool of the tree.
- `max_ephemeral_pool_per_user`: The maximum number of ephemeral pools for each user. An ephemeral pool is a pool that was indicated in the specification, but for which there is no explicit node in Cypress.
- `fair-share_preemption_timeout`: The time when the operation was below its fair-share, after which preemption will start to run the operation jobs.
- `min_share_preemption_timeout`: The time when the operation was below its min-share, after which preemption will start to run the operation jobs.
- `fair-share_starvation_tolerance`: The tolerance used when comparing usage-ratio with fair-share: an operation is considered starving if `usage-ratio < fair-share * tolerance`.
- `max_unpreemptable_running_job_count`: The operation will not participate in preemption if the number of its jobs is less than this value.

The `fair-share_preemption_timeout`, `min_share_preemption_timeout`, and `fair-share_starvation_tolerance` attributes can be redefined in the operation specification.

The tree is the `scheduler_pool_tree` object at the first level of the `//sys/pool_trees` directory in Cypress. The attributes listed above are the attributes of this object. The `//sys/pool_trees` node has the `default_tree` attribute in which the default tree can be specified. Will be used for operations that do not have the specified `pool_trees` option.

## Pool characteristics { #pools }

Each pool has the following characteristics. The default values are specified in brackets.

* `weight` (1): A real non-negative number, which is responsible for the proportion in which the subtree should be provided with the resources of the parent pool. When a pool has two child pools with weights 2 and 1, the parent's resources will be divided between them in the 2:1 proportion.
* `max_share_ratio`: A real number in the [0, 1] range, indicating the maximum share of the parent pool's resources that should go to the given pool.
* `min_share_resources`: A dict in which the guaranteed resources for the given pool are written (`user_slots`, `cpu`, and `memory`).

{% note info "Note" %}

Note that guarantees will be fulfilled only if the dominant resource of the operation coincides with the guaranteed resource.

{% endnote %}

* `resource_limits`: A dict which describes the limits on different resources of a given pool (`user_slots`, `cpu`, and `memory`).
* `mode` (fair-share): The scheduling mode, can take the `fair-share` or `fifo` values. If there is a FIFO scheduler, jobs are issued to child pools in lexicographic sorting order according to the values of the parameters specified in the `fifo_sort_parameters` attribute for that pool. For example, if the attribute value is `[start_time]`, jobs will first be issued to the operations with the least start time.
* `max_running_operation_count` (8): The limit on the number of concurrently running operations in the pool. Operations above this limit will be queued up and pending.
* `max_operation_count` (50): The limit on the number of concurrently started (running + pending) operations in the pool. If the specified limit is reached, starting new operations in the pool will end with an error.
* `fifo_sort_parameters` (start_time): The order of starting operations in the FIFO pool. By default, operations are sorted first by weight, and then by start time. This parameter enables you to change the order of operations in the queue. Supported values: `start_time` , `weight`, and `pending_job_count`. The `weight` parameter is applied in reverse order: operations with more weight go first and have a higher priority. The `pending_job_count` value enables you to prioritize small operations (with few jobs).
* `forbid_immediate_operations`: Prohibits the start of operations directly in the given pool; does not apply to starting operations in subpools.
* `create_ephemeral_subpools` (false): Activates the mode in which an ephemeral subpool is created for operations in the given pool with the `poolname$username` name where `poolname` is the name of the current pool. The operation is started in the created ephemeral subpool.
* `ephemeral_subpool_config`: A nested configuration of ephemeral subpools created in the current pool. Makes sense only if the `create_ephemeral_subpools` option is specified. You can specify `mode`, `max_running_operation_count`, and `max_operation_count` in the configuration.

Each pool has its _unique_ (within a single tree) name. The name of the pool selected to start the operation is shown in the operation settings.

{% note warning "Attention!" %}

The `max_running_operation_count` and `max_operation_count` parameters are set (explicitly or implicitly) at all levels of the pool hierarchy and are also checked at all levels of the hierarchy. To be specific, starting a new operation in the P pool is possible if and only if both P and all of its parents (up to the root) have fewer started operations than a given limit. The same applies for the `max_running_operation_count` limit: an operation in the P pool will remain in the pending state as long as the number of running operations in that pool or in any of its parents is larger than or equal to the `max_running_operation_count` limit. Oversubscribing these limits is possible at each level of the hierarchy. This feature must be taken into account when ordering and changing user pool limits. In order to guarantee a certain number of operations to the pool, you need to make sure that there is no oversubscription at all levels up to the root.

{% endnote %}

### Operations { #operations }

Some of the settings inherent to the pool are also available for operations and are specified in the operation specification root at start-up. These settings include: `weight`, `max_share_ratio`, and `resource_limits`.

For example, you can specify `resource_limits={user_slots=200}` in the specification. In this case, the operation will not run more than 200 jobs concurrently in each tree.

The `max_share_ratio`, `resource_limits`, and other settings can be specified independently for different pool trees.

## Dynamic characteristics of operations and pools

* `Fair share ratio` is a share of cluster resources that is guaranteed for the given operation (pool) at the moment. The fair share sum of all operations does not exceed one.
* `Guaranteed ratio` is a share of cluster resources that is guaranteed for the operation (pool) at the maximum cluster load.
* `Min share ratio` is a share of cluster resources calculated based on min share resources of this pool.
* `Usage ratio` is a share of cluster resources currently consumed by the operation or pool: all the running operation or pool jobs. The usage ratio sum of all operations can be larger than one, since operations can consume different resources, while the share is calculated by the dominant resource.
* `Starving` is a flag indicating whether the operation is starving. If there is a starving operation, the scheduler will try to abort the jobs of non-starving operations: make preemption to start the jobs of this operation.
* `Demand ratio` is a characteristic that shows the share of all cluster resources that an operation or pool needs to complete computation.
* `Dominant resource` is the dominant resource of an operation or pool - the resource whose share in the resources of the entire cluster is the largest.

Dynamic characteristics of operations are calculated for each tree separately. Consequently, an operation can have several `usage_ratios` and so on.

The amount of resources that an operation needs to be executed is calculated by summing up the needs of all the operation jobs by each resource type.