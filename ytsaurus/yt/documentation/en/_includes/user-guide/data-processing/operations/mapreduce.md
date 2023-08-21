# MapReduce

How the MapReduce operation works:

1. On cluster nodes, the map phase starts. Each user script in the map phase (mapper) receives a separate part of the input table for input. The output data of the map phase is partitioned based on the hash value of a key. The resulting partitioned data is stored locally on the disk (replicated once), at the nodes where the map jobs were executed.
2. Next comes the reduce phase. Each user script in the reduce phase (reducer) handles one partition: as input, it receives all the records received from mappers whose hashed key has the specified value. Over the network, the reducer fetches the data corresponding to its partition from the cluster nodes where the map jobs were executed. This process is called **shuffle**, and it produces the highest load on the network. Each partition is sorted in memory by key, and if the keys match, then by subkey. The sorted data then becomes input for a reduce job. The reduce job's output is written to the disk and replicated as needed — usually it is two synchronous replicas and one asynchronous, with one synchronous replica being written locally.

![](../../../../../images/mapreduce_op1.png){ .center }

The MapReduce operation is similar to the [Sort](../../../../user-guide/data-processing/operations/sort.md) operation, but allows for executing a mapper user script before partitioning jobs, and a reducer user script after sorting jobs.

{% note warning "Attention!" %}

Functionally, MapReduce is not the same as Map+Sort+Reduce. Even if the user script in the reduce phase does not change key column values, you still cannot get a sorted table as the operation's output. Therefore, using MapReduce in combination with the `sorted_by` attribute on the output table is almost always incorrect. The error will make itself known only on fairly large tables, which do not fit into a single reduce job in their entirety.

{% endnote %}

In the merged MapReduce operation:

1. The user may go without specifying the mapper, in which case it is assumed to be identical, and the input table data is output from the map phase without processing. In terms of {{product-name}}, when there is a user-defined mapper, partition map jobs are run, and in the absence of one, partition jobs.
2. If the partition is too large and cannot be stored in memory, it is impossible to sort it on a single cluster node. In these cases, the partition is divided into parts, each part is sorted separately, and the result is locally written to the disk. After that, [multiway merge](https://en.wikipedia.org/wiki/K-way_merge_algorithm) is launched, which merges the data and feeds it to a reduce job's input. In {{product-name}} terms, sorting a small partition in memory is done through partition reduce jobs. If the partition is large, then partition sorting is performed with partition sort jobs, and the final merge and launch of the reducer is handled by sorted_reduce jobs.
3. Until map jobs finish, you cannot start reduce jobs. This is because the {{product-name}} system must supply reduce jobs with data ordered by subkey, and that sequence cannot be generated until the data (map job outputs) is completed. A barrier of sorts therefore exists between the map and reduce phases — they must not intersect in time.
   If the scheduler detects a large partition that cannot be sorted within one job, it has an opportunity to save time and start processing the partitioned data before all map jobs finish. The scheduler, at its own discretion, divides the partition into several parts. The individual parts are then sorted on different cluster nodes by intermediate sort jobs, which take this data in over the network and sort it. This technique allows for a partial overlap of the phases, shortening the operation time.

## MapReduce vs Map+Sort+Reduce
The merged MapReduce operation has several advantages over the [Map](../../../../user-guide/data-processing/operations/map.md) + [Sort](../../../../user-guide/data-processing/operations/sort.md) + [Reduce](../../../../user-guide/data-processing/operations/reduce.md) combination:

1. **Less I/O operations on intermediate data.** Without the merge phase, map-sort-reduce typically writes intermediate data to disk three times: after the map phase, after the partition sort phase, and after sorting. Some of these writes may have a replication factor other than 1. For a MapReduce operation, a single write to disk will suffice, and the replication factor will be 1.
2. **No unnecessary barriers (synchronization points).** With the map-sort-reduce combination, {{product-name}} is forced to wait for **all** the map jobs (including the longest ones) to finish before starting partitioning. Similarly, sorting needs to be completed before the first reduce jobs can be run. As for the merged MapReduce operation, there is only one inevitable synchronization point (reduce jobs cannot be started before map jobs run their course).
3. **Better fault tolerance.** MapReduce constitutes a single operation, so the scheduler does everything it can to complete it even when some intermediate data becomes unavailable. When it comes to a chain of independent operations, the scheduler is only concerned with the individual phases. The only way to combat this is to ramp up intermediate data replication, which severely impacts speed.

![](../../../../../images/mapreduce_op2.png){ .center }

### Usage notes

To switch from the Map+Sort+Reduce combination to the merged MapReduce operation, just pass both the mapper and reducer options in the operation's specification.


In some cases, users may find the merged MapReduce operation not fast enough on default settings. The reasons may be:

1. **Significant difference between the data volume at the mapper's input and output, which leads to the scheduler misjudging the number of partitions.**

   The scheduler estimates the partition number based on the amount of input data as it aims to ensure the partition fully fits into the sort job's memory (2 GB by default). In doing that, the scheduler assumes that the input for the mapper approximately equals its output.

   In practice, the mapper may be filtering input data with an output-to-input ratio of 1:1000 or less. As a result, each partition's buffer gets something like several records, which leads to a large number of random, small-portioned reads from the disk in the shuffle process at the beginning of the sort phase.

   Example: Let's say a partition map job is input with 2 GB of data, produces an output of 4 MB, with `partition_count` of 1000. This gives us the average pre-compression size of a block written by the partition map job of around 2 KB. Such blocks cause a huge number of instances of random access to the disk.

   To combat this problem, the `map_selectivity_factor` option was added to an operation's specification, allowing users to set the approximate output-to-input ratio at the start of the operation and improve the scheduler's estimates. Alternatively, you can explicitly specify the desired number of reduce jobs using the `partition_count` option.

1. **"Heavy" mappers and a small number of parallel jobs due to memory shortage.**

   The scheduler allocates resources for a job based on the memory required for the user-provided code (`memory_limit`, set by the user in the operation's specification) and the memory for the system elements (JobProxy). For a map job (within the Map operation), the memory is made up of `memory_limit`, the read buffer, and the write buffer.

   Example: Let's say `memory_limit` is 250 MB and the buffer sizes are 100 + 100 MB, totalling 450 MB per job. If the fair-share distribution for the operation is 0.1 and the total amount of the cluster's resources is 11 TB of memory and 7000 CPUs, then the resources at the operation's disposal are 0.1 * 11 TB = 1.1 TB, and 0.1 * 7000 = 700 CPUs. Therefore, min(1100 GB / 450 MB, 700) = min(2500, 700) = 700 jobs can run in parallel.

   For partition map jobs, memory is the sum of `memory_limit`, read buffer, and partition buffer. For operations with many partitions, the scheduler allocates to the partition map job a large amount of memory for the partition buffer so as to avoid small blocks. For example, given 1000 partitions and a 2 GB buffer, the worst-case block size will be 2 GB / 1000 = 2 MB (with even data distribution). Returning to the previous example: Memory amount for a partition map job is 250 MB + 100 MB + 2 GB ≈ 2400 MB, and with 1.1 TB of available memory the scheduler will be able to run only min(1100 GB / 2400 MB, 700) = min(470, 700) = 470 jobs in parallel.

   If the mapper is "heavy" (high CPU usage per unit of output data), then tamping down on concurrent operations will lead to a significant hike in the execution time and cancel out any possible gains from merging the map and partition phases. Because the typical size of the output from the filter mapper is very small, such a mapper should also be considered "heavy".

   The best solution in this situation is to use the Map + MapReduce combination (with a trivial mapper). This already results in higher intermediate data storage metrics compared to the Map+Sort+Reduce combination.

### Measurement results

A good example of the improvement in performance is the classic [WordCount](https://en.wikipedia.org/wiki/Word_count) task. It boils down to a single Map-Reduce operation, wherein the mapper aggregates data by key.

The table shows the different times it took to finish the WordCount task using the merged MapReduce operation, the Map+Sort+Reduce sequence, and Map+MapReduce (the last one used a trivial mapper).

| Operation | Time |
| --------------- | ----- |
| MapReduce | 03:25 |
| Map+MapReduce | 05:30 |
| Map+Sort+Reduce | 08:10 |

These results were collected on a cluster with 200 nodes, each having 48 GB of RAM and 24 cores for jobs. The input data size was 1 TB.

## MapReduce operation options

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

The MapReduce operation supports the following additional options (default values, if set, are specified in brackets):

* `mapper` — user-defined script for the map phase. If not specified, input table data is output by the operation without processing.
* `reducer` — user-defined script for the reduce phase.
* `reduce_combiner` — description of the reduce_combiner script (more details [below](#reduce_combiner)).
* `sort_by` (defaults to `reduce_by`) — list of columns used for sorting data input at the reduce phase. The sequence of the `reduce_by` fields should be set as the prefix of the `sort_by` field sequence.
* `reduce_by` — list of columns used for grouping.
* `input_table_paths` — list of input tables with full paths (must not be empty).
* `output_table_paths` — list of output tables.
* `mapper_output_table_count` — number of tables from `output_table_paths` that will be output at the map phase. For such tables, the job's `table_index` is counted from one, and the intermediate output is a null output table.
* `partition_count`, `partition_data_size` — options that specify how many partitions are to be made in the sorting process.
* `map_job_count`, `data_size_per_map_job` — options that indicate how many jobs should be run at the map phase (these are recommendations).
* `data_size_per_sort_job` — option to control the amount of data at the `reduce_combiner` input (more details [below](#reduce_combiner)).
* `force_reduce_combiners` (false) — forces the launch of `reduce_combiner` (more details [below](#reduce_combiner)).
* `map_selectivity_factor` (1.0) — proportion of the original amount of data that remains after the map phase (the default value is 1.0, which means the expected size of the data remains unchanged during the map phase; 2.0 means the data size doubles, and so on).
* `intermediate_data_replication_factor` (1) — replication factor for intermediate data.
* `intermediate_data_account` (intermediate) — account to whose quota the transaction's intermediate data goes.
* `intermediate_compression_codec` (lz4) — codec used for compressing intermediate data.
* `intermediate_data_acl` `({action=allow;subjects=[everyone];permissions=[read]})` — rights for accessing intermediate data that are set up following the map phase.
* `map_job_io, sort_job_io, reduce_job_io` — I/O settings for the respective job types; in the `reduce_job_io` option, the `table_writer` section is added for all jobs that write to output tables.
* `sort_locality_timeout` (1 min) — time during which the scheduler waits for resources to free up on specific cluster nodes in order to start sorting all the parts of each partition on a node. This is necessary to ensure higher read locality in the course of the subsequent merging of sorted data.
* `ordered` (false) — enables the logic that is similar to ordered map at the partition map phase (input data is divided into jobs in successive segments, with the input of each map phase job being fed rows according to the order contained in the input tables).
* `pivot_keys` — list of keys to be used for data partitioning at the reduce phase. This option is exactly the same as the corresponding reduce [option](../../../../user-guide/data-processing/operations/reduce.md).

The MapReduce operation supports `table_index` only for the mapper. If you want `table_index` for the reducer, you need to add `table_index` to a separate column in a mapper record and use `table_index` in the reducer that way.

## Working with large keys — reduce_combiner { #reduce_combiner }
The reduce phase has a special data processing stage for dealing with large keys. This stage's jobs are called **reduce_combiner** jobs. They run on parts of "large" partitions and facilitate a partial reduce without waiting for all partition parts to get sorted and for reduce with the final merge to start. As input, reduce jobs running on partitions like that receive merged outputs of several `reduce_combiner` jobs.

`Reduce_combiner` is triggered if the partition size exceeds `data_size_per_sort_job`. The amount of data in `reduce_combiner` equals `data_size_per_sort_job`. The default value for `data_size_per_sort_job` is set in the scheduler configuration, but can be overridden via an operation's specification (in bytes).

You can also force-start `reduce_combiner` by setting `force_reduce_combiners` to `true`. `Reduce_combiner` receives a sorted stream of records as input (like a regular reducer). There are several restrictions on the `reduce_combiner` output:

* output must be sorted.
* `reduce_combiner` must not change keys — columns indicated in the `sort_by` field of the specification (if not indicated, `reduce_by`).
* there must only be one output table, as in the map phase of the `MapReduce` operation;
   This means that any commutative and associative reducer can be used as `reduce_combiner` in its original form.

Starting an operation with `reduce_combiner` may look like this:

```
yt map-reduce --reduce-combiner cat --reducer cat --reduce-by cookies --src //statbox/access-log/2013-05-15 --dst //tmp/asd --format dsv
```

## Example specification

Example of a MapReduce operation's specification:

```yaml
{
  partition_count = 100;
  reduce_by = ["key"];
  sort_by = ["key"; "subkey" ];
  input_table_paths = [ "//tmp/input_table" ];
  output_table_paths = [ "//tmp/first_output_table"; //tmp/second_output_table" ];
  mapper = {
    command = "python my_mapper.py";
    file_paths = [ "//tmp/my_mapper.py" ];
    tmpfs_path = ".";
    tmpfs_size = 1000000;
    copy_files = %true;
  };
  reducer = {
    command = "python my_reducer.py";
    file_paths = [ "//tmp/my_reducer.py" ];
  };
}
```
