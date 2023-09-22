# Integral guarantees

The basic mechanism for providing guarantees for computing resources in the {{product-name}} system stipulates issuing a constant amount of resources (strong guarantee), most often in CPU cores (less often in the cluster share). This guarantee type is well suited for tasks that can utilize resources constantly . In reality, there are other scenarios where more flexibility is required.

For example, when a production process needs to perform computations in a relatively short period of time, but needs a lot of computational quota at its peak, whereas the consumption level can be much lower the rest of the time. If the basic mechanism for providing a guarantee to the process is used, you need to allocate resources at the maximum consumption value, and order more resources when scheduling than are actually needed.

There are processes for which it is not so important to get computational resources immediately, but it is important that the processes be completed within a certain extended period of time. If that's the case, you are looking at an average value of the computational quota over a long time: for example, research and analytical tasks - experiments.

The mechanism of integral guarantees enables you to find a balance between the described scenarios.

## Integral pools

In the {{product-name}} system, you can enable the accumulation of resources for the pool. Such pools receive a certain amount of virtual resources at a constant rate — `accumulated_resource_ratio_volume`. The amount of resource is stored in the cluster share per second — `fair_share*sec`, but for simplicity we can assume that the value is given in cores per second — `cpu*sec`. The pool can consume a virtual resource to start operations and can accumulate it to a certain limit.

Example: let there be `accumulated_resource_ratio_volume = 60 fair_share*sec` in the pool, 1000 cores on the cluster, and the dominant resource of the user process is cpu. This volume can be converted to `cores * seconds` — `60,000 cpu*sec`. An operation that consumes 100 cpu cores at a time will use that amount in 10 minutes; one that consumes 50 cpu cores — in 20 minutes. This example does not take into account that the volume of the virtual resource in the pool is replenished over time.

The resource accumulation rate is configured in the `integral-guarantees/resource_flow` pool attribute. The attribute value must specify the resource type and its volume:

```bash
yt set //sys/pools/root-pool/burst/@integral-guarantees/resource_flow "{cpu=100}"
```

Since `resource_flow` is  the rate at which `accumulated_resource_ratio_volume` increases, which is the instantaneous amount of resource multiplied by time, `resource_flow/cpu` is measured in cores, simply put, `cpu*sec/sec = cpu`. For example, `resource_flow/cpu = 100` allows an operation consuming 100 cpu cores at a time to run indefinitely.

Accumulation of `accumulated_resource_ratio_volume` occurs to a certain limit, which is equal to `k*resource_flow_ratio` where k is a single scheduler parameter for all pools and `resource_flow_ratio` is `resource_flow` as a share of all cluster resources. By default, `k=86400`, which equals the number of seconds in a day. This means that the integral pool without operations will accumulate `accumulated_resource_ratio_volume` at a rate of `resource_flow` before it reaches the limit when resource accumulation stops.

The described idea of accumulating and spending resources is much like the [Token bucket](https://en.wikipedia.org/wiki/Token_bucket) algorithm. Not to be mistaken for [Leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket).

## Guarantee types

There are three types of guarantees for computing resources in the {{product-name}} system:

- **strong_guarantee**: A strong guarantee of the share of cluster resources. Set in an absolute value (in cores), automatically recalculated into the cluster share. Enables you to get a guarantee at any time and use it indefinitely.
- **burst_integral**: An immediate guarantee ("peak guarantee") in cores, enables you to get a fixed guarantee over a period of time. For example, get 2000 cores for two hours a day.
- **relaxed_integral**: An integral guarantee (in cores) enables you to get a fixed average amount of resource per day.

### Types of integral pools

To support the two scenarios described at the beginning of this section, two types of integral pools are implemented in the {{product-name}} system:
- **Burst pool**: The pool that has a priority when spending `accumulated_resource_ratio_volume`. Such pools must have `resource_flow` and `burst_guarantee_resources` specified. If there are operations that demand resources in the pool, the scheduler must issue at least `  burst_guarantee_resources` to the pool if the pool has enough `accumulated_resource_ratio_volume`. This pool type provides a burst_integral guarantee.
   A burst pool configuration example:

   ```bash
   yt set //sys/pools/root-pool/burst/@integral-guarantees "
   {
     guarantee_type=burst;
     resource_flow={cpu=1000};
     burst_guarantee_resources={cpu=2000}
   }"
   ```

   In the above example, the user can expect the pool to always be guaranteed 1000 cores, or guaranteed 2000 cores for "half" of the time (for example, 12 hours a day) if nothing is running during the second half.
- **Relaxed pool**: The pool for which there is no guarantee to get all resources for `accumulated_resource_ratio_volume` at a given moment, but there is a guarantee to get them in the end (within a day). You can only set `resource_flow` for a relaxed pool. Such pools are expected to run operations that do not immediately require the resources they are entitled to. This pool type provides a relaxed_integral guarantee.
   A relaxed pool configuration example:

   <small>Listing 3</small>
   ```bash
   yt set //sys/pools/root-pool/relaxed/@integral-guarantees "
   {
     guarantee_type=relaxed;
     resource_flow={cpu=1000}
   }"
   ```

The described pool types complement each other, allowing the use of the same resources, and priority processes that require significant guarantees for limited periods of time and less significant processes in which it is allowable to wait for resources to be allocated.

### Example

Suppose there is a production process that requires 2000 CPUs 12 hours a day and a research process that needs only 1000 CPUs on average. With the mechanism of integral guarantees, burst and relaxed pools can be configured for such processes, and 2000 cores will be needed to provide guarantees. The specified pools must be located in the same pool tree, there are no other restrictions. With the basic mechanism of issuing guarantees (strong guarantee), 3000 cores would have to be ordered to enforce such guarantees.

The figure shows a graph of CPU usage and demand for the burst pool, for which burst_integral guarantee is 500 cpu cores and resource_flow is 100 cpu cores.

The figure below shows a graph of the volume of accumulated resource for the same pool, you can see how the volume of virtual resource decreases when operations are running in the pool, and then the resource is again accumulated to a specified limit.

![](../../../../../images/pool_accumulated_resource_ratio_volume.png)

To configure the integral guarantees, contact the administrator.
## Combination with strong guarantees

Both types of integral pools are naturally compatible with strong guarantees. When issuing resources, the scheduler will first issue resources against strong guarantees. If they are enough to meet the current need of the pool, `accumulated_resource_ratio_volume` will not be spent. If the need for resources exceeds the strong guarantees, `accumulated_resource_ratio_volume` will be spent to meet the exceeding part.

Integral pools along with neighboring pools participate in receiving resources in excess of their guarantees from the pools that are higher in the hierarchy in proportion to the set weights.

## Additional attributes of integral pools

You can request the scheduler for the value of service attributes, which are available in a special subtree of Cypress called Orchid.

- `accumulated_resource_ratio_volume`: Accumulated integral resources in terms of the cluster share * second.
- `accumulated_resource_volume`: `accumulated_resource_ratio_volume` as a dict with all resources. For example, `accumulated_resource_volume/cpu` is a volume of the accumulated integral resource expressed in core * second.
- `integral_pool_capacity`: The limit of accumulating `accumulated_resource_ratio_volume`.
- `specified_burst_ratio`: `burst_guarantee_resources` converted into the cluster share.
- `specified_resource_flow_ratio`: `resource_flow` converted into the cluster share.
- `total_burst_ratio`: The sum of `specified_burst_ratio` for all descendants (including the current pool).
- `total_resource_flow_ratio`: The sum of `specified_resource_flow_ratio` for all descendants (including the current pool).
- `estimated_burst_usage_duration_seconds`: The estimated period of time that the accumulated resource will last (taking into account the continuing inflow of resources at the rate of `resource_flow`) when consuming `burst_guarantee_resources` (only available for burst pools).

An example of requesting an attribute is shown in Listing 4.
<small>Listing 4</small>

```bash
yt get //sys/scheduler/orchid/scheduler/scheduling_info_per_pool_tree/physical/fair_share_info/pools/pool_name/integral_pool_capacity
```

{% note warning "Attention!" %}

The scheduler Orchid is not part of the stable API and can be changed without announcement.

{% endnote %}
