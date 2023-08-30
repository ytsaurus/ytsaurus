# Auto-merging chunks at operation output

This section describes the process of merging chunks automatically at the operation output. We'll discuss the scope of this feature, its settings, and examples.

## Overview { #overview }

One of the most popular data processing scenarios is filtering of a large table consisting of multiple chunks that uses the [Map](../../../../user-guide/data-processing/operations/map.md) operation without special flags (Unordered Map) to produce a small result. Because of the underlying mechanism of such operations, filtering of a table of `n` chunks can at worst create `n` resulting chunks in the output, regardless of how small each of these chunks is. Such a behavior might devour the chunk quota on the user account. For more information on quoting, see [Quotas](../../../../user-guide/storage/quotas.md).

You can merge the resulting chunks using the [Merge](../../../../user-guide/data-processing/operations/merge.md) operation, but this method is not convenient because it requires an additional operation. Still, however, in this case, you need to create a safety stock of quota for `n` chunks to allow for bursts: this might be as high as hundreds of thousands of chunks for large tables (and the chunk quota is quite an expensive resource). That's why {{product-name}} provides automated chunk merging at operation output. This merging is proactive in the sense that it merges output chunks before they have used up your quota.

Automatic merging has the following constraints:

* It's only available for Unordered [Map](../../../../user-guide/data-processing/operations/map.md) and [Sorted Reduce](../../../../user-guide/data-processing/operations/reduce.md) operations.
* It works only with tables that accept **unsorted data**. For the operations with sorted output, the situation is complicated by the fact that you cannot merge output chunks in an arbitrary order.
* Unavailable if `row_count_limit` is set. To learn more about this parameter, see [Operation options](../../../../user-guide/data-processing/operations/operations-options.md).

If the operation goes beyond the constraints, the user has to run the [Merge](../../../../user-guide/data-processing/operations/merge.md) operation manually.

## Automated merging modes { #mode }

Automated merging is set up using the `auto_merge` section that needs to be added to the operation's specification. The section is a dict where the main parameter is the `mode` key that can take upon the `disabled`, `relaxed`, `economy`, `manual` values that set different behaviors in the context of automated merging.

### Relaxed { #relaxed }

The mode that's not attempting to save the quota in real time, but tries to get a table with reasonably sized chunks at the output. Actually, this method relieves the use of the need to run an Unordered Merge on the output table. However, it does not guarantee that the operation will run properly under a quota shortage.

### Economy { #economy }

The mode that optimizes the maximum quota usage by the operation. This mode is suitable for the operations that often hit the quota limit.

Under ideal circumstances, the chunk quota usage in the `economy` mode can be evaluated as `4 * sqrt(n)`, where `n` is the number of chunks that you would have had in the output table without automated merging. Such a result is not guaranteed, it's only a best-effort behavior that requires, for example, that the quantities of chunks output by jobs are burst-free (one job wouldn't generate a thousand chunks at once). In this case, to filter a table of 100,000 chunks, you can safely do with a quota of a couple of thousand chunks.

### Manual { #manual }

The mode in which you can manually set up the automated merge behavior using the following parameters in the `auto_merge` section:

* `max_intermediate_chunk_count`: The scheduler will try to maintain the specified constraint on the intermediate chunk quota. However, you could never maintain this limit exactly because of the internal mechanisms of chunk generation.
* `chunk_count_per_merge_job`: Defines the size of chunk portions that the scheduler will try to merge per job (without exceeding the limit of `desired_chunk_size` in `job_io` for Merge jobs; the default value is 1 GB).
* `chunk_size_threshold` is the limit that allows you to avoid merging the chunks whose size exceeds the given percentage of `desired_chunk_size`. That is, only the chunks whose size is smaller than this percentage (10% by default) will be merged.

### Disabled { #disabled }

The mode with no automated merging. This mode is used by default.

{% note info "Note" %}

The `economy` mode (as well as the `manual`mode at certain parameters), imposes quite a strong constraint on the operation parallelism, which might slow the operation down. The reason for such behavior is that you cannot run 10,000 jobs in parallel, keeping to the constraint of 1000 intermediate chunks because such number of jobs can generate 10,000 chunks at the same time. On the other hand, the `relaxed``` mode doesn't have this issue because it doesn't try to meet the chunk quota constraints in real time.

{% endnote %}


Auto-merging is an additional step in the operation that consumes time. That's why automated merging is disabled by default. If the growth in the operation runtime due to automated merging isn't critical for you, use the `relaxed` mode.  It's assumed that this mode suits the demands of most users.

## Sample specifications { #examples }

Sample specification with the `relaxed` mode:


```yaml
{
  input_table_paths = [ "//tmp/input_table" ];
  output_table_paths = [ "//tmp/output_table" ];
  mapper = {
    command = "cat";
  };
  auto_merge = {
    mode = "relaxed"
  }
}
```
Sample specification with the `manual` mode:

```yaml
{
  input_table_paths = [ "//tmp/input_table" ];
  output_table_paths = [ "//tmp/output_table" ];
  mapper = {
    command = "cat";
  };
  auto_merge = {
    mode = "manual";
    max_intermediate_chunk_count = 1000;
    chunk_count_per_merge_job = 10;
    chunk_size_threshold = 5;
  }
}
```
