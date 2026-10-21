# Autoscaler

The autoscaler adjusts the partition count of each [Computation](computation.md). Several criteria propose counts; the controller selects the largest, subject to delays and the `min_partition_count` and `max_partition_count` limits. An explicit `desired_partition_count` sets the count manually.

## CPU {#cpu}

The CPU criterion increases the partition count when average partition CPU usage exceeds `desired_average_partition_cpu_load`. This distributes processing across more jobs. As load decreases, the recommendation declines gradually.

## Throughput {#throughput}

The Throughput algorithm adjusts the partition count based on message processing rates. It helps `Computation` nodes that wait for external services while using little CPU.

The algorithm uses two quantities:

- **Demand** is the required rate: new input plus the rate needed to catch up on backlog within the configured catch-up period. Messages from Source, timers and Key Visitor are propagated to each `Computation` using the input/output message ratios recorded in [lineage](lineage.md). Ordinary batching backlog is excluded.
- **Capacity** estimates what the current partitions can process. The total completed-work rate, `Processed`, is divided by the busiest job's busy-time share. Both quantities are averaged over a common window; waiting for input or output distribution is excluded from busy time.

Each `Computation` then follows this cycle:

1. **Measurement.** The algorithm collects statistics from all current jobs over a common window. Job load determines whether Capacity before and after a partition-count change can be compared reliably.
1. **Growth.** If Demand persistently exceeds Capacity and the Computation is sufficiently loaded, the algorithm increases its partition count. The step depends on the capacity shortage; catch-up uses smaller steps than new input.
1. **Capacity comparison.** With sufficiently loaded jobs, the algorithm compares Capacity with the point before growth. If the gain justifies the additional partitions, exploration continues. Two probes without sufficient gain indicate a plateau: the algorithm keeps the larger count.
1. **Bounded exploration.** When capacities before and after growth cannot be compared reliably, the algorithm checks completed throughput, Processed. Growth is bounded by an allowance based on the original partition count; confirmed progress in Processed expands this allowance. Without progress, exploration stops.
1. **Recovery.** After a reliably measured capacity drop, the algorithm restores the original partition count and repeats measurement over a longer window. Repeated degradation appears in the Computation status.
1. **Downsizing.** Once catch-up is complete, the algorithm returns towards the beginning of the plateau while retaining capacity for Demand and twice the historical steady-demand peak. The peak is retained for `peak_demand_window` (nine days by default) and survives restarts; catch-up throughput does not contribute to it. Further reductions also retain spare capacity relative to Processed and are periodically batched across the pipeline. The historical peak only limits downsizing. Unsuccessful probe rollbacks run separately.

At an unchanged plateau, the algorithm waits for confirmed improvement in measurements or an explicit search-state reset. Other criteria can still increase the partition count during Throughput warm-up.

The Throughput algorithm is disabled by default. Enable it with `dynamic_spec.job_manager.partitioning.enable_throughput_autoscaler`. See the [Throughput configuration reference](../generated_docs/all_yson_structs.md#NYT_NFlow_TThroughputSpec).

## Timer memory {#timers}

The timer criterion increases the partition count when the average number of timers per partition exceeds `desired_average_partition_timer_count`. This limits timer accumulation and memory use. A similar criterion tracks average job memory usage through `desired_average_partition_memory_used`.
