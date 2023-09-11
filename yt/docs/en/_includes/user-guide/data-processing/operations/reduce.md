# Reduce

The Reduce operation may consist of one or more jobs. Each job receives part of the input table data processed by the user script, and the result is written to the output table. The input data is grouped according to the set of columns indicated in the `reduce_by` option. This grouping guarantees that all data with the same value in the `reduce_by` columns is input to the same job. The guarantee can be eased using the `enable_key_guarantee` option. For the operation to start, each input table must be sorted according to a column set beginning with `reduce_by`.

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

The Reduce operation supports the following parameters (default values, if set, are specified in brackets):

* `reducer` — user script.
* `input_table_paths` — list of input tables with full paths (cannot be empty).
* `output_table_paths` — list of output tables.
* `reduce_by` — set of columns used for grouping.
* `sort_by` (defaults to `reduce_by`) — set of columns used for sorting primary input tables. The option enables an additional check for whether input tables are sorted and provides guarantees the rows are going to be sorted according to the column set within the user script. The `reduce_by` field sequence should be set as the prefix of the `sort_by` field sequence.
* `job_count`, `data_size_per_job` (256 MB) — options specifying how many jobs should be started (these are recommendations). The `job_count` option takes precedence over `data_size_per_job`.
* `pivot_keys` — enables manual data partitioning, so you can determine which jobs get certain data ranges. As its value, this option takes a sorted list of table keys (bordering ones).
   For example, if `pivot_keys = [["a", 3], ["c", -1], ["x", 42]]`, then four jobs (and no more) start:
   * for keys strictly up to `["a", 3]`.
   * for keys from `["a", 3]` (inclusive) to `["c", -1]` (non-inclusive).
   * for keys from `["c", -1]` (inclusive) to`["x", 42]` (non-inclusive).
   * for keys from `["x", 42]` until the end.

   The scheduler will not try to split such jobs into smaller ones. If some of the ranges do not contain keys, then the corresponding jobs may not launch. It is impossible to guarantee which jobs do not run, because the scheduler does not see the data in full, only samples from it.

* `enable_key_guarantee` — enables/disables the guarantee that all records with one key are given as input to one job (`true` by default). [Reduce with foreign tables](../../../../user-guide/data-processing/operations/reduce.md#foreign_tables) describes a special scenario for using this option.
* `auto_merge` — dictionary containing the settings for automatic merge of small-sized output chunks. By default, automatic merge is disabled.

The order of inputs in the Reduce operation is associated with the following guarantees:

* data from each input table is divided into continuous parts that are fed to jobs based on `reduce_by` columns. In other words, one job cannot get keys `A` and `C`, while another job gets key `B`.
* within a job, data from all the input tables is merged and sorted — first according to the `sort_by` fields, and then by input table index (`table_index`). For foreign (`foreign`) tables — within one `join_by` key — sorting is done by external table index. More information about foreign tables [below](../../../../user-guide/data-processing/operations/reduce.md#foreign_tables).

Input tables whose path has the `teleport=%true` attribute get processed by the Reduce operation in a special way. Chunks of such tables without key range overlaps with other input tables' chunks are not fed to the user script. Such unprocessed chunks are written to the output table whose path has the `teleport=%true` attribute. There can be no more than one such output table. You can still specify the `sorted_by` attribute for the output table to make it sorted. In this case, the user must ensure that the key range at the user script output is not wider than the key range at the operation's input.

## Application

A typical use case is updating a directory or aggregates.
Suppose there is a `//some_user/stat` table that stores visit statistics broken down by day. The table has two columns: `time` and `count`, with the data sorted based on the `time` column. The data in the table is stored in two chunks, the first one being dates from 2013-06-01 to 2013-08-15, and the second one — 2013-08-16 to 2013-09-16. Additionally, there is a sorted `//some_user/update` table featuring statistics updates for September. You can run the Reduce operation to update the main table as shown below.

```
yt reduce --src <teleport=%true>//some_user/stat --src //some_user/update --dst <teleport=%true, sorted_by=[time]>//some_user/stat
```
As a result, the first chunk of the `//some_user/stat` table goes to the output table in its original form, without processing.

Another use case is filtering out rows whose keys match those of a large sorted table from a small sorted table. Here, you can add an output table with the `<teleport=%true>` attribute and delete it after execution. The output table will only contain those chunks of the large table that were not present in the small one.
Filtering example:

```
yt reduce --src <teleport=%true>//fat_table --src //small/table --dst //results --dst <teleport=%true>//temp_table_to_throw_out ...; yt remove //temp_table_to_throw_out
```

## Reduce with foreign tables { #foreign_tables }

Reduce with foreign tables is a modified Reduce operation where part of the input tables are marked as foreign (`foreign`) tables and act as directories, with their data being added to the data stream from "regular" (`primary`) tables. For foreign tables, a special set of `join_by` key columns is used, which should be set as the prefix for the `reduce_by` column set. Records with the same `join_by` key, but different `reduce_by` keys may end up in different jobs — in this case, the corresponding records from foreign tables are passed in to all the jobs, and therefore processed more than once.
The reverse is also possible in cases where the directory table can contain more different combinations of values than the main table. If a foreign table record matches none of the main table records, that record may not go to any job at all.

{% note warning "Attention!" %}

Because there are frequent reads on foreign table chunks, [erasure coding](https://en.wikipedia.org/wiki/Erasure_code) for them is strongly discouraged. Indeed, if the foreign table is small, it is best to set a higher replication factor for it. Using `erasure` for a foreign table can lead to increased read loads across cluster nodes and hurt the entire cluster's performance.

{% endnote %}

An important difference between Reduce with foreign tables and the regular Reduce operation is that the granularity of user script calls in the Python wrapper and C++ wrapper. Unlike the regular Reduce operation, a user script (the `IReducer::Do` method in the C++ API and the handler function in Python) is called for each `join_by` key rather than the `reduce_by` key.
Specification requirements for Reduce with foreign tables:

* at least one primary table must be specified (foreign attribute missing).
* at least one foreign table must be specified using the `<foreign=%true>` attribute.
* the `join_by` value must be specified.
* `join_by` must be a prefix of or match `reduce_by`.
* `join_by` cannot be used without foreign tables, and foreign tables cannot be used without `join_by`.
* you cannot set the `teleport` attribute for a foreign table.
* you cannot set multiple ranges for a foreign table, but the same foreign tables can be specified multiple times with different ranges.

Also, Reduce operations with foreign tables have the `consider_only_primary_size` option (`%false` by default), which, if enabled, makes it so only the amount of data in `primary` tables is taken into account in job splitting.

Setting the `enable_key_guarantee=%false` option in Reduce with foreign tables carries special semantics:

* it disables the guarantee that all records with the same key from a primary table end up in the same job. At the same time, there still remains the guarantee that if a record from a primary table with a certain key ends up in a job, then all records with the same key from all foreign tables go to the same job.
* the user must pass in the list of keys to `join_by`, specifying `reduce_by` is not allowed.

We recommend using the Reduce operation with the `enable_key_guarantee=%false` option.

## Working with large keys — reduce_combiner

The reduce phase has a special data processing stage for dealing with large keys. This stage's jobs are called **reduce_combiner** jobs. They run on parts of "large" partitions and facilitate a partial Reduce without waiting for all partition parts to get sorted and for Reduce with the final Merge to start. As input, reduce jobs running on partitions like that receive merged outputs of several `reduce_combiner` jobs.

`Reduce_combiner` is triggered if the partition size exceeds `data_size_per_sort_job`. The amount of data in `reduce_combiner` equals `data_size_per_sort_job`. The default value for `data_size_per_sort_job` is set in the scheduler configuration, but can be overridden via an operation's specification (in bytes).

`Reduce_combiner` can also be forced by setting `force_reduce_combiners` to `%true`.
`Reduce_combiner` receives a sorted stream of records as input (like a regular reducer). There are several restrictions on the `reduce_combiner` output:

* output must be sorted.
* `reduce_combiner` must not change keys — columns specified in the specification's `sort_by` field (if not specified — `reduce_by`).
* there must only be one output table, as in the case of the mapper in the `MapReduce` operation;
   This means that any commutative and associative reducer can be used as `reduce_combiner` in its original form.

Starting an operation with `reduce_combiner` may look like this:

```
yt map-reduce --reduce-combiner cat --reducer cat --reduce-by cookies --src //statbox/access-log/2013-05-15 --dst //tmp/asd --format dsv
```

## Example specification

```yaml
{
  data_size_per_job = 1000000;
  reduce_by = ["key"];
  sort_by = ["key"; "subkey" ];
  input_table_paths = [ "//tmp/input_table" ];
  output_table_paths = [ "//tmp/unsorted_output_table"; <sorted_by = ["key"]> "//tmp/sorted_output_table" ];
  reducer = {
    command = "python my_reducer.py"
    file_paths = [ "//tmp/my_reducer.py" ];
  };
  job_io = {
    table_writer = {
      max_row_weight = 10000000;
    }
  }
}
```
