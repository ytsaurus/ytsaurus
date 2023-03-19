# Sort

The Sort operation sorts the data from the selected tables and writes it to one output table. The Reduce operation can be run on a sorted table, and when data is read from the table (in particular, with specified ranges), it appears in a sorted form. It is also possible to use ranges by value, not only by table row number. The comparison is performed first by type, and then, if the types match, in the regular fashion (numbers by value, strings — lexicographically).

{% note info "Note" %}

In the Sort operation, there are no guarantees when it comes to the order of rows with the same key value.

{% endnote %}

One of the main sorting parameters is the number of partitions (`partition_count`). This parameter is calculated in advance. Before the main jobs start, samples are read from the input table. These samples serve to calculate the key ranges for splitting input data into parts (based on `partition_count`) of approximately the same size. Next, things proceed in the following phases:

1. Running first are partition jobs, which split all their input into partitions based on `partition_count`, matching each row to a partition.
2. Second phase is sorting jobs, which sort the data from the partitions. Here, there are two options: if there is little data in the partition, it is sorted whole, and the partition processing ends there. If there is a lot of data and one job is not enough to sort it, sorting jobs are run on fixed-size parts of the partition before the transition to the third phase.
3. The third phase consists of merge jobs, which merge the sorted parts of large partitions.

General parameters for all operation types are described in [Operation options](../../../user-guide/data-processing/operations/operations-options.md).

The Sort operation supports the following additional options (default values, if set, are specified in brackets):

* `sort_by` — list of columns used for sorting (mandatory).
* `input_table_paths` — list of input tables with full paths (cannot be empty).
* `output_table_path` — full path to the output table.
* `partition_count`, `partition_data_size` — options that specify how many partitions are to be made in the sorting process (these are recommendations).
* `partition_job_count`, `data_size_per_partition_job` — specify how many partition jobs should be started (these are recommendations).
* `intermediate_data_replication_factor` (1) — replication factor for intermediate data.
* `intermediate_data_account` (intermediate) — account to whose quota the transaction's intermediate data goes.
* `intermediate_compression_codec` (lz4) — codec used for compressing intermediate data.
* `intermediate_data_medium` (`default`) — type of medium storing chunks of intermediate data produced by sorting.
* `partition_job_io, sort_job_io, merge_job_io` — I/O settings for the respective job types; in the `merge_job_io` option, the `table_writer` section is added for all jobs that write to output tables.
* `schema_inference_mode` (auto) — schema definition mode. Possible values: auto, from_input, from_output. For more information, see the [Data schema](../../../user-guide/storage/static-schema.md#schema_inference) section.
* `samples_per_partition` (1000) — number of keys for samples from the table for each partition (only available for dynamic tables).
* `data_size_per_sorted_merge_job` — determines the amount of input data for merge jobs (it is a recommendation).
* `sort_locality_timeout` (1 min) — time during which the scheduler waits for resources to free up on specific cluster nodes in order to start sorting all the parts of each partition on a node. This is necessary to ensure higher read locality in the course of the subsequent merging of sorted data.

By default, `partition_count` and `partition_job_count` are calculated automatically based on the amount of input data so as to minimize the sorting time.

## Example specification

Example of a Sort operation's specification:

```yaml
{
  data_size_per_partition_job = 1000000;
  sort_by = ["key"; "subkey" ];
  input_table_paths = [ "//tmp/input_table1"; "//tmp/input_table2" ];
  output_table_path = "//tmp/sorted_output_table";
}
```
