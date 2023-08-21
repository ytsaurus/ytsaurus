# Example of setting up pools

Consider the pool structure of the project, whose operations can be divided into three classes: manual queries (ad_hoc), batch processing, and backups. The project has a quota of 100 computing cores and standard limits on the number of operations. The pools need to be set up so that ad_hoc operations are performed as quickly as possible and there is less waiting in the queue to run. Batch processing requires the use of all available resources when there are few or no ad_hoc tasks. Backup operations require a small resource guarantee so that the process can be complete within the allotted time.

Separate pools must be created for each operation class and placed in the shared project root pool (project-root). The pool settings are shown in the table.

| pool_name | CPU | weight | max_running_operation_count | max_operation_count |
| --------------------------- | ---- | ------ | --------------------------- | ------------------- |
| project-root | 100 | 1 | 10 | 50 |
| project-root/project-ad_hoc | 80 | 1 | 10 | 50 |
| project-root/project-batch | 0 | 10 | 4 | 50 |
| project-root/project-backup | 20 | 1 | 2 | 50 |

The sum of CPU guarantees of all descendants of the pool must not exceed the CPU guarantees of the parent pool. CPU oversubscription is not allowed, such a configuration will be perceived by the scheduler as an error and will not be applied. Oversubscription for both limits on the number of operations is allowed. The following condition must be met:

- The `max_running_operation_count` value of each pool cannot be greater than its `max_operation_count`.

If the `max_running_operation_count` sum of limits of the subpools exceeds its counterpart limit of the parent pool, this may lead to an operation in a subpool not being able to be executed even if the number of running operations in the pool has not reached the limit. This may occur if there are too many running operations in other subpools and their number is equal to the `max_running_operation_count` limit of the parent pool. In this case, even if a subpool has a CPU guarantee, it will not preempt operations already running in other pools. Jobs can be preempted if there are more than one of them in an operation, but not operations themselves.

In the pool structure shown in the table, all guaranteed computing resources go to project-ad_hoc and project-backup pools when they are fully loaded and running operations in project-batch are preempted. But if operations in at least one of the guarantee pools cannot fully utilize their pool, unclaimed resources can be allocated to the project-batch pool with a higher priority.

