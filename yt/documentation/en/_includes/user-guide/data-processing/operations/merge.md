# Merge

The Merge operation merges input tables into one. The output is a table whose set of rows is equal to all the rows of the original tables merged together. The `mode` option determines how the merge is performed:

* `mode = unordered` — only merge lists of input table chunks. This the default merge mode.
* `mode = sorted` — merges the input tables while maintaining sorting. The columns based on which the output table is sorted are determined by the `merge_by` option.
* `mode = ordered` — keeps the order of the chunks from the input table list. Meaning if `T1 = (A1, A2), T2 = (B1, B2, B3)`, then `Merge(T1, T2) = (A1, A2, B1, B2, B3)`.

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

The Merge operation supports the following additional options (the default values, if set, are specified in brackets):

* `mode` (unordered) — the type of Merge operation, can take the following values: `unordered`, `sorted`, `ordered`.
* `input_table_paths` — list of input tables with full paths (cannot be empty).
* `output_table_path` — full path to the output table.
* `combine_chunks` (false) — activates the consolidation of chunks.
* `force_transform` (false) — forces the read of all the input table data.
* `merge_by` — list of columns used for sorted merge.
* `job_count`, `data_size_per_job` (256 MB) — options specifying how many jobs should be started (these are recommendations). The `job_count` option takes precedence over `data_size_per_job`. If `job_count` is less than or equal to `total_input_row_count`, the number of jobs for the unordered/ordered merge operation is guaranteed to be met exactly, so long as it does not conflict with the limit on the maximum number of jobs for the operation. In particular, if `job_count` is equal to`total_input_row_count`, then each job gets exactly one row, and if `job_count` equals 1, only one job is launched.
* `schema_inference_mode` (auto) — schema definition mode. Possible values: auto, from_input, from_output. To learn more, see [Data schema](../../../../user-guide/storage/static-schema.md#schema_inference).

If the `force_transform` option is `false`, the operation proceeds in such a way as to minimize the number of running jobs. The best scenario will have the merge carried out on the meta-information level. Otherwise, all chunks will be read.

In addition to disk space, data also uses the chunk quota of the account where it is located. It is a good idea to run the Merge operation even on a single table to “consolidate” the existing chunks. To do this, enable `combine_chunks=true`. To learn more about account quotas, see [Quotas](../../../../user-guide/storage/quotas.md).

## Example specification

```yaml
{
  pool = "my_cool_pool";
  mode = "sorted";
  merge_by = [ "field1"; "field2" ];
  job_count = 100;
  input_table_paths = [ "//tmp/input_table1"; "//tmp/input_table2" ];
  output_table_path = "//tmp/output_table";
  combine_chunks = %true;
}
```
