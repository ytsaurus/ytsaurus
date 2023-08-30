# Map

The Map operation may consist of one or more jobs. Each job receives part of the input table data that is processed by the user script. The result is written to the output table.

The Map operation has two modes: **unordered** (default) and **ordered**. The ordered Map mode is enabled through the `ordered=%true` option in the operation's specification. The mode affects the guarantees that the system provides for jobs' input data. Each of the two modes delivers the basic guarantee of the Map operation: each output table is produced by merging the results of all the successfully completed jobs, with each input data row ending up at the input of precisely one successful job.

- In the unordered Map mode, there are **no** guarantees beyond those described above. In particular:
   - The rows of one input table can end up with random jobs without any continuity (one mapper may get the first and third rows of the table, while another would get the second and fourth).
   - The rows of an input table can be fed to a job in any order, not necessarily based on ascending row numbers.
   - The rows of different input tables can be fed to the same job randomly, including in mixed fashion (first may come a table A row, then a table B row, followed again by a table A row).

- How Ordered Map works:
   - It concatenates the input data according to how it's entered in the input_table_paths section of the input data.
   - The resulting aggregate input is divided into continuous segments, each of which is fed to a separate job.
   - If there are several input tables, some jobs **can** receive rows from several tables for input (the process has to land in between two tables as a result of the above mentioned concatenation).
   - Each job reads its data segment strictly in order within the concatenation described above: first in the order of the tables, and within the tables — based on ascending row numbers.

Both modes have trade-offs: ordered Map mode offers more guarantees that can be used in certain cases, but is less efficient due to having more restrictions. Unordered Map mode makes active use of its ability to adaptively read input data with a high degree of parallelism from different chunks, compensating for the slowdown of individual disks with input data, so it is generally more efficient. Unordered Map mode can regroup rows in a pool of yet unprocessed input data in whichever order. For example, the input of an unsuccessful job (aborted or failed) can be regrouped in an arbitrary fashion and processed within several new jobs.

Note that the `ordered=%true` option determines the way input tables are split into jobs, but **does not** result in any additional sorting of the data therein other than the initial one.

The order of output data is determined both by the operation mode and by the presence of a sorted schema on the output table (or of the `sorted_by` attribute on its path in the operation's specification).

Possible scenarios:

- If the output table is sorted, the following takes place:
   - The output data of each job is checked for being sorted based on the output table schema (or the one specified for the output table path). If the job produces an unsorted row sequence, the operation ends with an error.
   - If all the output chunks are sorted, then at the end of the operation they aim to arrange in accordance with the sort order in the output table. If two chunks happen to overlap on bordering keys and no matter their arrangement the rows do not form a sorted sequence, the operation ends with an error. Chunks consisting of one key are ordered in an arbitrary, non-deterministic fashion.
- If the output table is not sorted and the operation mode is unordered, the output chunks are ordered in an arbitrary, non-deterministic manner.
- If the output table is not sorted and the operation mode is ordered, the output chunks are ordered in accordance with the global job order described above.

The ordered version of the Map operation is characterized by the determinism of output table rows and their order, provided that the mappers' logic is deterministic, regardless of the number of jobs in the operation and the order of their execution. This characteristic is not there for unordered Map.

General parameters for all operation types are described in [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

The Map operation supports the following additional options (the default values, if set, are specified in brackets):

* `mapper` — user script.
* `input_table_paths` — list of input tables with full paths (cannot be empty).
* `output_table_paths` — list of output tables.
* `job_count`, `data_size_per_job` (256 MB) — options specifying how many jobs should be started (these are recommendations). The `job_count` option takes precedence over `data_size_per_job`. If `job_count` is less than or equal to `total_input_row_count`, the number of jobs is guaranteed to be met exactly, so long as it does not conflict with the limit on the maximum number of jobs for the operation. Specifically, if `job_count` is equal to `total_input_row_count`, then each job gets exactly one row, and if `job_count` equals 1, only one job is launched.
* `ordered` — controls the way input is split into jobs.
* `auto_merge` — dictionary containing the settings for automatic merge of small-sized output chunks. By default, automatic merge is disabled.

## Example specification

```yaml
{
  pool = "my_cool_pool";
  job_count = 100;
  input_table_paths = [ "//tmp/input_table" ];
  output_table_paths = [ "//tmp/output_table" ];
  mapper = {
    command = "cat";
    memory_limit = 1000000;
  }
}
```
