## Limits on the number of operations

{{product-name}} has limits on the number of operations running in compute pools. These limits serve as a protective mechanism that controls the load on the scheduler. There are two types of limits on the number of operations:

- `total_operation_count` limits the total number of operations. This reduces the load from maintaining the state of the pool tree and performing periodic recalculations of the fair share.
- `running_operation_count` limits the number of running operations. This reduces the load from scheduling new allocations.

If a pool has reached its limit on the total number of operations, attempting to run the `start_operation` command will result in an error. If an operation starts successfully, but the pool has already reached its maximum number of running operations, the operation enters the `pending` state, gets queued, and waits for the currently running operations to complete. In the pool hierarchy, operations count toward the limits of all their ancestor pools. The described rules apply if at least one ancestor pool reaches its limit. Generally, limits from different pools are independent of each other. There may be cases where the combined limits of all child pools exceed the limit of the parent pool.

### Lightweight operations

The bulk of the scheduler's load comes from processing exec node heartbeats and scheduling new allocations. This load directly depends on the number of operations currently involved in the scheduling process. These are called `schedulable` operations. For example, a running operation may be non-`schedulable` if it already has all the necessary allocations. Since the `schedulable` status can change while an operation is in progress, it's difficult to enforce a limit on the number of such operations. For this reason, the system manages the load by implementing a conservative restriction on the number of currently running operations.

Some operations may be non-`schedulable` for much of their duration. For example, an operation may consist of a single small job that requires minimal time for the scheduler to initiate. In this scenario, the operation continues to count toward the limit on running operations, which can be a valuable resource in large {{product-name}} installations. To address cases like this, the service supports a special type of operations called *lightweight operations*. These operations don't count toward the `running_operation_count`, meaning the limit on the number of running operations doesn't apply to them. However, lightweight operations still count toward the `total_operation_count` and the special `lightweight_running_operation_count` counter, which doesn't have a limit.

For an operation to be considered lightweight, it must meet several conditions:

- The operation must be of the [Vanilla](../../../user-guide/data-processing/operations/vanilla.md) type.
- The pool where the operation is running must be configured in `FIFO` mode.
- The pool must allow lightweight operations. This is controlled by the `enable_lightweight_operations` setting, which can be set by the {{product-name}} cluster administrator.

#### Recommendations for use

You may run non-lightweight operations in a pool that has lightweight operations enabled. These operations count toward the `running_operation_count` and are subject to the associated limit. However, mixing operations of different types is an anti-pattern and is strongly discouraged.

The scheduler algorithm implements special logic to count lightweight operations and expects their jobs to start successfully within a short timeframe. For this reason, we don't recommend abusing this feature by running heavy `Vanilla` operations that consist of more than one job in lightweight pools. While these operations are technically considered lightweight, their launch time may be longer than usual.

