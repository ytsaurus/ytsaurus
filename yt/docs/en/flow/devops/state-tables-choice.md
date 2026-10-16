# Choose state tables for {{product-name}} Flow

Flow creates and mounts its [internal tables](../concepts/pipeline-object.md#internal_tables) with the pipeline by default. This guide compares dynamic tables that you create and manage for [external state](../concepts/stateful.md#external-state). Choose regular, replicated, or Chaos tables for that external state before creating them. This choice affects storage cost, read consistency, and behavior when a cluster is unavailable.

## Comparison {#comparison}

| Option | Reads and writes | Cost and use |
| --- | --- | --- |
| Regular dynamic table | One copy on the selected cluster. Availability follows that cluster, and there is no asynchronous replica to lag behind. | The simplest choice when one cluster meets the availability requirement. |
| [Replicated table](../../user-guide/dynamic-tables/replicated-dynamic-tables.md) | A synchronous replica gives current reads; an asynchronous replica can lag. Metadata and synchronous replica selection depend on the meta cluster, whose outage stops writes. | Additional replicas cost more but provide options for available reads. |
| [Chaos table](../../user-guide/dynamic-tables/chaos-dynamic-tables.md) | Metadata lives in chaos cells. Writes can continue when one replica cluster is unavailable. Asynchronous reads can lag. | Each cluster incurs write and replication queue costs; choose this when write availability is critical. |

## Make the choice {#decision}

1. Define acceptable read and write downtime and how fresh reads must be. Use a synchronous replica for the latest committed data; an asynchronous replica has no guaranteed lag bound.
2. Estimate state size, write rate, and the cost of extra replicas and replication queues in your own deployment. Do not reuse another cluster's numerical cost estimate without measurement.
3. Start with regular external state tables if one cluster meets the requirement. Choose replicated tables for additional read availability and Chaos tables when writes must survive an individual replica cluster outage. The linked table guides describe these general table types; this comparison does not change how Flow creates its internal tables.
4. Before changing an external state table's schema or type, stop the pipeline and plan the migration according to the [release rules](vanilla/releases.md#release-and-configure-basic-rules).
