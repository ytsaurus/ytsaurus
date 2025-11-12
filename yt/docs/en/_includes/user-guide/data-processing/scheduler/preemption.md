# Preemption

When certain operations run for too long, they can take too many resources and block other operations from starting new jobs. As a result, the other operations are starved of resources. To solve this problem, the scheduler has a preemption mechanism.

Based on its usage share and fair share, an operation can have one of two scheduling statuses:
* `normal`: The operation gets enough resources.
* `below_fair_share`: The operation receives less resources than its fair share (based on the configured [tolerance](#fair-share-starvation-tolerance)).

If an operation remains in the `below_fair_share` status for too long, it begins to starve. In this case, the scheduler can use preemption to allocate resources to it. In terms of starvation, each operation can be in one of three states:
* `non_starving`: The operation doesn't need preemption to schedule its allocations.
* `starving`: The operation is short on resources, so the standard [_preemption_](#preemption) mechanism is applied when scheduling its allocations.
* `aggressively_starving`: The operation is starving, so the [_aggressive preemption_](#aggressive-preemption) mechanism is applied to schedule its allocations.

At any time, the allocations of each operation are divided into three groups:
* `non_preemptible`: Preemption can't be applied.
* `aggressively_preemptible`: Preemption is applied only as part of aggressive preemption.
* `preemptible`: Preemption is applied unconditionally.

This division occurs as follows: all allocations of an operation are ordered by the start time of the last job. Within this order, non-preemptible allocations come first, followed by aggressively preemptible allocations, and then unconditionally preemptible ones. Allocations are divided into these groups based on the `aggressive_preemption_satisfaction_threshold` and `preemption_satisfaction_threshold`. These thresholds set the cutoff values for the ratio of an operation's usage share to its fair share, calculated from the prefix of the allocations.

## How preemption works { #preemption }

The preemption mechanism is part of allocation scheduling. This means the decision to preempt an allocation is made when processing the heartbeat from the node where that allocation resides.

Preemption can occur for three reasons:
1. To launch an allocation for a starving operation.
2. When the resources consumed by allocations on a node exceed the node's available resources.
3. When an operation or one of its parent pools exceeds the `resource_limits`.

In practice, the vast majority of preemptions occur for the first reason.

The preemption mechanism works according to the following algorithm. When processing a heartbeat, new allocations are scheduled in several stages: regular scheduling, preemptive scheduling, and aggressively preemptive scheduling. While other stages exist, they aren't discussed in this guide. Each stage is defined by a subset of operations involved in scheduling new allocations and the resources available to run them:
* During the regular scheduling stage, the scheduler considers all operations, with the available resources being the free resources on the node.
* During the preemptive scheduling stage, only starving and aggressively starving operations are considered. The available resources include both the free resources on the node and the resources of preemptible allocations on that node.
* During the aggressive preemptive scheduling stage, the scheduler considers only aggressively starving operations. The available resources include the node's free resources plus the resources of preemptible and aggressively preemptible allocations on that node.

Keep in mind that preemptive scheduling uses a configurable backoff, so this stage may not be triggered on every node heartbeat. In addition, only one allocation can be scheduled at the preemptive scheduling stage.

If a new allocation is scheduled at this stage, the scheduler selects the smallest subset of preemptible jobs required to accommodate it, considering allocations in order from newest to oldest.

## Aggressive starvation and preemption { #aggressive-preemption }

This mechanism is necessary to prevent fragmentation and schedule large allocations.

Without this mechanism, most of the resources on each of the cluster nodes may be occupied by non‑preemptible allocations. In this case, an operation that requires a large allocation of resources (for example, half of a node's total resources) is unlikely to be scheduled.

Suppose the aggressive preemption mechanism is enabled, and the `aggressive_preemption_satisfaction_threshold` is set to 0.5. This means at least half of the resources across all allocations on that cluster are preemptible. It also implies that there is a node with half of its total resources aggressively preemptible. This makes it possible to schedule large allocations for aggressively starving operations, provided they don't exceed half of a node's total resources.

Note that the aggressive preemption mechanism has clear drawbacks: it can preempt allocations running within their fair share, which may be unacceptable for production operations with execution‑time SLAs.

## Preemption settings

You can set the main preemption settings in the pool tree configuration. Some parameters can also be overridden in the pool options.

{% include [pool tree preemption settings](pool-tree-preemption-settings.md) %}

{% include [pool preemption settings](pool-preemption-settings.md) %}
