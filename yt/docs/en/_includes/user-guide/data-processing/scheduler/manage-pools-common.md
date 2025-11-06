# Managing compute pools

This section explains how to create pools and manage resources and settings of compute pools in the {{product-name}} system using the CLI and web interface as examples.

## General information { #common }

The {{product-name}} system has a mechanism for managing compute pools, enabling pool actions such as:

- Creating.
- Renaming.
- Moving.
- Deleting.
- Changing settings.

You can change the compute pool settings by editing the relevant pool attributes. Attributes that can be edited by the user:

* `weight`: Manages the share of the parent pool's resources that will go to the current pool.
* `max_operation_count`: A limit on the number of running (`running`) operations and operations in the queue (`pending`).
* `max_running_operation_count`: A limit on the number of running (`running`) operations.
* `strong_guarantee_resources`: A dict describing the pool's guaranteed resources.
* `forbid_immediate_operations`: Prohibits operations to be run directly in this pool.
* `mode`: A way of allocating resources between operations in the pool.
* `fifo_sort_parameters`: Managing the order in which operations are started in fifo pools.
* `resource_limits`: A dict that sets the upper limits for the resources available to the pool.
* `create_ephemeral_subpools`: Allows creating [ephemeral pools](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md#puly-i-derevya-pulov).
* `ephemeral_subpool_config`: A dictionary describing the configuration of virtual pools.

For more information about the listed attributes, see [Pool configuration](../../../../user-guide/data-processing/scheduler/pool-settings.md#pools).

### Validating

When validating, {{product-name}} checks attribute types, the `max_operation_count >= max_running_operation_count` limit, the number of tags in the `allowed_profiling_tags` list (no more than 200), consistency of resource guarantees, and whether there are subpools in fifo pools.

Consistency of resource guarantees means that for each resource, the sum of the descendants' guarantees must not exceed the parent's guarantee. If the subpool has a non-zero guarantee for a specific resource, it must also be set for the parent. The pool is allowed to have a non-zero guarantee on more than one resource.
