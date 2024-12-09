# Debugging MapReduce programs

{% include [Local emulation](../../_includes/user-guide/problems/mapreduce-debug/local-emulation.md) %}

{% include [Stderr running](../../_includes/user-guide/problems/mapreduce-debug/stderr-running.md) %}

## Getting full stderr of all jobs of an operation

In {{product-name}}, you can save full stderr of all jobs to a table. You can export stderr of those jobs that were not aborted.

To enable the described behavior:

{% list tabs %}

- In Python

  Use the `stderr_table` parameter. For example:

  ```python
  yt.wrapper.run_map_reduce( mapper, reducer, '//path/to/input', '//path/to/output', reduce_by=['some_key'], stderr_table='//path/to/stderr/table', )
  ```
- In С++

  Use the [StderrTablePath](https://github.com/ytsaurus/ytsaurus/blob/07c9c385ae116f56d8ecce9fa6765fa1a90e95cc/yt/cpp/mapreduce/interface/operation.h#L563) setting.

- In SDKs in other languages

  You can pass the `stderr_table_path` setting directly to the operation specification. For a description of this option, see [Operation options](../data-processing/operations/operations-options.md).

{% endlist %}

{% include [Stderr table](../../_includes/user-guide/problems/mapreduce-debug/stderr-table.md) %}
