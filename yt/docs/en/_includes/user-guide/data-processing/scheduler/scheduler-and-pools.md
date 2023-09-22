# Scheduler and pools

This section describes the {{product-name}} scheduler, introduces the concepts of a pool and pool tree (pool_tree), reviews the process of scheduling calculations, and provides an example of calculating the fair share ratio and a description of preemption.

## General information

A scheduler is a part of the system responsible for allocating resources between operations and determining the order in which operations will be performed. The {{product-name}} system uses a hierarchical [fair_share](http://en.wikipedia.org/wiki/Fair-share_scheduling) scheduler. This means that resources are dynamically allocated between system users according to their guarantees and priority.

A pool is a container for the CPU and RAM resources that the scheduler uses. When users run tasks in pools, the scheduler allocates resources between the pools. A hierarchical resource allocation system can be built using the pool tree.

## Resources

The scheduler uses the following resources:

- `cpu`: 1 CPU corresponds to one HyperThreading core on the cluster node. By default, all custom jobs order a single core at start-up. If jobs are multi-threaded, you can increase the number of ordered cores in the operation settings. Otherwise, process jobs will be limited (throttling) so that the amount of actual CPU time consumed corresponds to a single core.
- `memory`: The resource to control RAM consumption on the cluster node in bytes.
- `user_slots`: The resource to control the number of jobs on the cluster node. Each job of custom operations always consumes exactly one user_slot.
- `network`: The resource to control consumption of the internal cluster network by operations actively using the network. For example, sorting. The resource unit has no physical analog, and the automatic values set by the scheduler for sorting operations are selected so that jobs running on the same cluster node do not overload the network channel.

For each pool and operation, there is a concept of a dominant resource. The dominant resource among the requested operation or pool resources is the resource type that has the highest share relative to the total volume of the corresponding resource of the entire cluster.

For example: there is an operation that requires 1,000 `cpus` and 2 TiB of `memory`. The entire cluster has 50,000 `cpus` and 180 TiB of `memory`. 1,000 `cpus` are 0.02 (2%) of the total number of `cpus` in the cluster and 2 TiB are 0.011 (1.1%) of the total `memory` capacity in the cluster. Thus, the `cpu` share is larger than the `memory` share, so `cpu` is the dominant resource for a particular operation.

## Scheduler operation description { #scheduler }

### Pools and pool trees

The {{product-name}} scheduler stores multiple pool trees in memory. Each tree consists of operations and compute pools. Compute pools are tree nodes and operations are leaves. Each operation can belong to one or more trees.

Logically, each tree can be regarded as a separate computing cluster — each tree belongs to a number of computing nodes in the cluster, and a cluster node can only belong to one tree.

Pool trees are stored in Cypress and configured by the {{product-name}} administrator. The pool tree is the `scheduler_pool` node at the first level of the `//sys/pool_trees` directory in Cypress. The description of the pools for the `<tree_id>` tree is a set of nested `scheduler_pools` located in the `//sys/pool_trees/<tree_id>` node. The pool characteristics are described in the attributes of the appropriate nodes.

The trees to which the operation will get at start-up are defined by the `pool_trees` parameter in the operation settings. If this parameter is not specified, one `default_pool_tree` will be used. For each of the listed trees, you can specify the pool from that tree in which the operation is to run (the `scheduling_options_per_pool_tree` option).

Each pool tree has the `default_parent_pool` attribute, which specifies the default pool. This pool is used as follows: if the pool was not specified for the tree or for all trees in the operation settings, a temporary ephemeral pool will be created in the tree with the name of the user in {{product-name}} who started the operation. The pools form a hierarchy `//sys/pool_trees/<tree_id>/<default_parent_pool>/<username>` in the end node of which the operation will be run.

Example of the setting for running an operation in two trees with indication of the pool in one of them:

```
pool_trees = [physical; cloud];
scheduling_options_per_pool_tree = {
    physical = {
        pool = cool_pool;
    }
}
```

## Job scheduling process

Each operation consists of a set of jobs. A job is doing some useful work with data on one of the cluster nodes. A job is a scheduling unit.

Job scheduling takes place on each cluster node separately:

1. The cluster node sends a special message (heartbeat) to the scheduler, indicating the type and amount of available resources, information about the completed and currently running jobs.
2. In response to the message, the scheduler decides whether to start new jobs on the cluster node or abort those in progress. Scheduling takes place in the tree to which the cluster node belongs. Preemption is also possible: aborting already running jobs of any operation if it received an amount of resources that is more than guaranteed.
3. All aborted jobs will later be restarted.

To understand which jobs should be started and which should be aborted on a particular cluster node, the scheduler analyzes the pool tree. Each pool in the tree has a number of characteristics that enable the scheduler to understand whether a given subtree needs to start new jobs, and, if it does, how much this subpool needs it. According to the pool characteristics, the scheduler goes down the tree from top to bottom until it reaches a leaf. After reaching the leaf, the scheduler suggests that the operation should start the job. The scheduler makes such bypasses as long as there are unscheduled jobs and the cluster node resources are not exhausted.

There may be operations to which the scheduler allocated less resources than it was obliged to guarantee. Such operations are called "[starving](../../../../user-guide/data-processing/scheduler/preemption.md)" in the system. When the scheduler detects a starving operation, it selects the job with the minimum running time of the non-starving operation on the same cluster node and aborts it.

{% note info "Note" %}

The operation and pool characteristics considered during scheduling are global and do not depend on a particular cluster node. For example, an important characteristic is the share of cluster resources that are already taken up by running operation jobs (usage_ratio). (`usage_ratio`).

{% endnote %}
