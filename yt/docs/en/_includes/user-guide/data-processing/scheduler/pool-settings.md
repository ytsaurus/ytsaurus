# Setting up pool trees

This section describes how to set up compute pools and pool trees for allocating computing resources within the cluster.

## Pool tree configuration { #pool_trees }

Trees are represented by `scheduler_pool_tree` objects at the first level of the `//sys/pool_trees` node in Cypress. The only child nodes these pools can have are compute pools. To specify the configuration of a tree, use the `config` attribute.

The `//sys/pool_trees` node also has the `default_tree` attribute — you can use it to set the default tree for operations that don't have a set `pool_trees` option.

The tree configuration has the following options:

- `node_tag_filter` (previously `nodes_filter`): A filter for selecting cluster nodes that belong to the tree. For more information, see [Tagging cluster nodes](../../../../admin-guide/node-tags.md).
- `main_resource`: The main tree resource. If the pool has guarantees, they must contain this resource. Guarantees for non-specified resources are computed proportionally to the main resource.
- `max_operation_count`: The default value for the maximum number of concurrently started (running + pending) operations in the tree.
- `max_operation_count_per_pool`: The default value for the maximum number of concurrently started operations in some pool if `max_operation_count` is not specified for this pool.
- `max_running_operation_count`: The default value for the maximum number of concurrently running operations in the tree.
- `max_running_operation_count_per_pool`: The default value for the maximum number of concurrently running operations in the pool if `max_running_operation_count` is not specified at the pool level.
- `default_parent_pool`: The default pool for running operations that do not explicitly specify a pool in their configuration.
- `forbid_immediate_operations_in_root`: Prohibits operations from running in the root pool of the tree.
- `max_ephemeral_pool_per_user`: The maximum number of ephemeral pools for each user. An ephemeral pool is a pool that was indicated in the specification, but for which there is no explicit node in Cypress.
- `infer_weight_from_guarantees_share_multiplier`: Specifying this multiplier enables inference of the pool weight proportionally to the pool guarantee based on the `main_resource` (more precisely, proportionally to the strong guarantee share). Inference applies only to those pools that do not have an explicitly set weight.
- `integral_guarantees`: Settings for managing [integral guarantees](../../../../user-guide/data-processing/scheduler/integral-guarantees.md) in the given tree.
- `pool_config_presets`: Description of pool configuration presets.

Below are the options associated with the [preemption mechanism](../../../../user-guide/data-processing/scheduler/preemption.md):
- `preemptive_scheduling_backoff`: A limit on the frequency of preemptive allocation scheduling within a single node.
- `fair_share_starvation_timeout`: If an operation has received fewer resources than allocated when this timeout expires, it becomes `starving`, meaning its allocations will be scheduled using preemptive scheduling.
- `fair_share_starvation_tolerance`: A threshold value indicating that an operation is not receiving sufficient resources. An operation is considered starving (`BelowFairShare` status) if `usage_share < fair_share * tolerance` for the dominant resource.
- `preemption_satisfaction_threshold`: A threshold value for classifying allocations as preemptible or non‑preemptible. Allocations are considered preemptible if they lie strictly above the specified threshold in terms of their contribution to the operation’s usage share.
- `non_preemptible_resource_usage_threshold`: An operation will not be subject to preemption if its resource usage is less than this value.
- `allocation_preemption_timeout`: An allocation preemption timeout. If a job in a given allocation supports interruption, the system ensures that the job has enough time to complete successfully before being aborted.

## Pool configuration { #pools }

A pool is an object of the `scheduler_pool` type. Each pool belongs to a specific pool tree. The pool hierarchy is an integral part of Cypress and is located at `//sys/pool_trees/<tree_name>`. When creating a pool, specify the pool tree and the parent pool within the tree. You can set the pool's options using attributes.

Below you can find the options available in the pool configuration (default values are given in parentheses):

* `weight` (none): A real non-negative number specifying the proportion of resources from the parent pool to be allocated to this subtree. For example, if a pool has two child pools with weights 2 and 1, the parent's resources will be split between them in a 2:1 ratio.
* `strong_guarantee_resources`: A dictionary which lists guaranteed resources for the given pool (`user_slots`, `cpu`, `memory`, and `gpu`).

{% note info "Note" %}

For the guarantees to be fulfilled, the dominant resource of the operation has to match the guaranteed resource.

{% endnote %}

* `resource_limits`: A dictionary which describes the limits on different resources for the given pool (`user_slots`, `cpu`, and `memory`).
* `mode` (`fair_share`): The scheduling mode that can take the `fair_share` or `fifo` values. In `fifo` mode, jobs are issued to child operations in lexicographic sorting order according to the values of the parameters specified in the `fifo_sort_parameters` attribute for the pool. For example, if the attribute value is `[start_time]`, jobs will be issued to the operations with the least start time.
* `max_running_operation_count` (8): The limit on the number of concurrently running operations in the pool. Operations above this limit will be queued up and pending.
* `max_operation_count` (50): The limit on the number of concurrently started (running + pending) operations in the pool. If the specified limit is reached, starting new operations in the pool will end with an error.
* `fifo_sort_parameters` (`[weight, start_time]`): The order of starting operations in the FIFO pool. By default, operations are sorted first by weight and then by start time. This parameter enables you to change the order of operations in the queue. Supported values: `start_time`, `weight`, and `pending_job_count`. Sorting by `weight` is done in reverse order: operations with greater weight go first and have a higher priority. The `pending_job_count` value enables you to prioritize smaller operations (with few jobs).
* `forbid_immediate_operations`: Prohibits the start of operations directly in the given pool. This restriction does not apply to starting operations in subpools.
* `create_ephemeral_subpools` (false): Activates the mode in which an ephemeral subpool is created for operations in the given pool. This pool will have the name `poolname$username`, where `poolname` is the name of the current pool. The operation is started in the created ephemeral subpool.
* `ephemeral_subpool_config`: A nested configuration of ephemeral subpools created in the current pool. Makes sense only if the `create_ephemeral_subpools` option is specified. You can specify the `mode`, `max_running_operation_count`, `max_operation_count`, and `resource_limits` in the configuration.
* `offloading_settings`: A setting responsible for offloading some of the pool jobs to another tree along with the tree where the pool resides. Job offloading works for operations started after the setting was enabled. Example: `yt set //sys/pool_trees/<pool_tree>/<pool>/@offloading_settings '{<pool_tree_X>={pool=<pool_name_Y>}}'`.
* `config_presets`: A list of presets applied to the given pool.

Each pool has its _unique_ (in one given tree) name. The name of the pool selected to start the operation is shown in the operation settings.

{% note warning "Attention" %}

The `max_running_operation_count` and `max_operation_count` parameters are set (explicitly or implicitly) at all levels of the pool hierarchy and are also checked at all levels. To be specific, starting a new operation in pool P is possible if and only if both P and all of its parents (up to the root) have fewer started operations than a given limit. The same applies to the `max_running_operation_count` limit: an operation in pool P will remain in pending state as long as the number of running operations in that pool or in any of its parents is larger than or equal to the `max_running_operation_count` limit. You can oversubscribe these limits at each level of the hierarchy. You should keep this in mind when ordering or changing user pool limits. To guarantee a certain number of operations to the pool, make sure that there is no oversubscription at all levels up to the root.

{% endnote %}

### Operation resources configuration { #operations }

Some of the settings inherent to the pool are also available for operations and are specified in the operation specification root at start-up. These settings include `weight` and `resource_limits`.

For example, you can specify `resource_limits={user_slots=200}` in the specification, meaning the operation will not run more than 200 jobs concurrently in each tree.

The `resource_limits` setting and other parameters can be specified independently for different pool trees.

## Pool setup example { #example }

Let's look at the pool structure in a project whose operations can be categorized into three groups: manual requests (ad hoc), batch processing, and backups. The project has a quota of 100 compute cores and standard limits on the [number of operations](../../../../user-guide/data-processing/scheduler/operations-limits.md).

We need to set up pools so that:
- Ad hoc operations are performed as quickly as possible and spend less time in the queue before running.
- Batch processing requires the use of all available resources when there are few or no ad hoc tasks.
- Backup operations require a small resource guarantee to make the process complete within the allotted time.

To do this, we need to create separate pools for each load type and place them in the project's shared root pool (`project-root`). The pool settings are shown in the table.

| pool_name | strong_guarantee_resources/cpu | weight | max_running_operation_count | max_operation_count |
| ----------------------------- | ------------------------------- | ------ | --------------------------- | ------------------- |
| `project-root` | 100 | 1 | 10 | 50 |
| `project-root/project-adhoc` | 80 | 1 | 10 | 50 |
| `project-root/project-batch` | 0 | 10 | 4 | 50 |
| `project-root/project-backup` | 20 | 1 | 2 | 50 |

Note that the sum of guaranteed resources allocated to descendants cannot exceed the parent pool’s guarantee. Note that there is no such restriction for limits on the number of operations; nevertheless, one basic requirement must be met: the `max_running_operation_count` value of each pool can't be greater than `max_operation_count`.

If the sum of `max_running_operation_count` limits of the subpools exceeds the same limit for the parent pool, this may lead to an operation in a subpool not being able to be executed even if the number of running operations in the pool has not reached the limit. This may occur if there are too many running operations in other subpools and their number has reached the `max_running_operation_count` limit of the parent pool. In this case, even if a subpool has a guarantee, it will not preempt operations already running in other pools. Only jobs can be preempted (if the operation has more than one), but not operations themselves.

In the pool structure shown in the table, all guaranteed computing resources go to `project-adhoc` and `project-backup` pools when they are at maximum capacity, and jobs of operations running in `project-batch` are preempted. However, if operations in at least one of the guarantee pools cannot fully utilize their pool, unclaimed resources can be allocated to the `project-batch` pool, which has a higher priority.

## Dynamic characteristics of operations and pools

<!--- TODO: унести в другое место? --->

The scheduling algorithm allocates cluster resources across compute pools and operations.

<!--- TODO: add link to orchid after publishing it in OpenSource [орхидею](../../../../user-guide/storage/orchid.md) --->
You can obtain current values of various characteristics for a specific pool tree via the scheduler Orchid:
* For a pool, this information is available at `//sys/scheduler/orchid/scheduler/pool_trees/<pool-tree>/pools/<pool>`.
* For operations, at `//sys/scheduler/orchid/scheduler/pool_trees/<pool-tree>/operations/<operation-id>`.

This information is also available on the Scheduling page and on the operation page in the UI. For example, you'll find the following characteristics there:
* `Fair share` is the share of cluster resources that is guaranteed for the operation (or pool) at this moment.
* `Usage share` is the share of cluster resources that is currently consumed by the operation or pool: all allocations of the operation or all operations in the pool subtree.
* `Demand share` is the share of cluster resources that the operation or pool needs to run all allocations.
* `Starvation status` is a flag indicating whether the operation is starving. If there is a starving operation, the scheduler will attempt to abort the jobs of non-starving operations (perform [preemption](../../../../user-guide/data-processing/scheduler/preemption.md)) to run the allocations of the starving one.
* `Dominant resource` is the dominant resource of an operation or pool — the resource whose share in the resources of the entire cluster is the largest.

All the shares described above constitute a vector of shares for each cluster resource. If the share is expressed as a single number, this is the share for the dominant resource.

Dynamic characteristics of operations are calculated for each tree separately. Consequently, an operation can have multiple values of `Fair share`, `Usage share`, and other parameters.

