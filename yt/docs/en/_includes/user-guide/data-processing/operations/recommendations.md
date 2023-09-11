# Using {{product-name}} system properly and efficiently

This section is a collection of recommendations on using the {{product-name}} for massively parallel data processing scenarios.

## Chunks too small

{% note info "Note" %}

The small chunk scenario is relevant to static tables. Dynamic table use cases imply fine-grained reads to reduce query response times; therefore, small chunks are acceptable for dynamic tables.

{% endnote %}

A [chunk](../../../../user-guide/storage/chunks.md) under 100 MB may be considered small. To prevent there being too many chunks in the system, you should look to have an average chunk size of at least 512 MB.

Small chunks complicate cluster operation for several reasons:

* Small chunks subject master servers to an additional load. The more chunks, the more master server memory is required to store chunk metainformation and the slower their performance: they are slower to write snapshots and longer to recover from them in the event of problems.
* A large number of small chunks results in slower data reads. For instance, if 100 MB are scattered throughout a million chunks, one record each, a million disk `seeks` are required to read all the data which is very slow even if they are done in parallel.

Operation execution or fine-grained data reads may produce tables with many small chunks. To resolve this problem, you need to increase chunk size for such tables. You can increase chunk size by calling the command below:

```bash
yt merge --src //your/table --dst //your/table --spec '{combine_chunks=true;mode=<mode>}'
```

You need to set the `<mode>` attribute to `sorted` for a [sorted table](../../../../user-guide/storage/static-tables.md#sorted_tables) and to `ordered` for an [unsorted](../../../../user-guide/storage/static-tables.md#unsorted_tables) one. For more information on table types, please see [Static tables](../../../../user-guide/storage/static-tables.md).

In both the cases, merge will maintain data ordering. But for `sorted`, it will also maintain the table sort from the system standpoint: the table will remain `sorted` and keep all the relevant attributes.

If you are using a [python library](../../../../api/python/userdoc.md), you can specify the `auto_merge_output={action=merge}` configuration option which will make the library automatically aggregate the resulting tables if their chunks are too small.

## MapReduce vs Map+Sort+Reduce

For more information on MapReduce internals and on why it is normally faster than the Map+Sort+Reduce sequence, please see [MapReduce](../../../../user-guide/data-processing/operations/mapreduce.md).

Below is a description of circumstances when using the combined MapReduce operation is **not recommended**.

### Heavy mapper with strong filter

As a theoretical reference, you can use the fact that a heavy mapper spends over 100 ms of CPU to process a single row. Normally, the number of input rows of a strongly filtering mapper is 5 or more times greater than the number of its output rows or bytes.

This requires there to be as many jobs as possible in the map phase for each to run more quickly. A MapReduce has a constraint on the number of map jobs because if there are too many, the sort phase will become costly to perform following a large number of small random disk reads.

The correct solution in this case is to start a Map first specifying as many jobs as possible. Thereafter, apply a combined MapReduce with an empty mapper to the resulting data.

### Frequent application of MapReduce to same key fields

If you need to process your data through a Reduce more than once, it will, most likely, be more efficient to sort the data first followed by conventional Reduce operations. Logs are a common example of such data.

This arrangement will only work well in two situations:

1. The data does not change.
2. The data is appended. A table is time-sorted, for instance. An additional amount of data comes in with all the key (time) values greater than all the key (time) values in the table. Then, a sort can be applied to this batch of data followed by a Reduce with `teleport=%true`. For more information on the option, please see the [Reduce](../../../../user-guide/data-processing/operations/reduce.md#foreign_tables) section.

### Heavy reducer

Like with a heavy mapper, you need to start as many reduce jobs as possible. But the operation has a limit on the number of partitions; therefore, on the number of reduce jobs.

The right thing to do in this case is to sort the table and then run Reduce with as many jobs as possible.

## Large number of records with identical keys during reduce phase

To solve this problem, {{product-name}} has [Reduce combiners](../../../../user-guide/data-processing/operations/mapreduce#reduce_combiner.md) that help process large keys in several jobs in the reduce phase.

### Map combiners

The idea behind a map combiner is to aggregate data during the map phase.

The [WordCount task](http://wiki.apache.org/hadoop/WordCount) is a classic example of using a map combiner. The map phase of this task requires inserting `(word, count)` rather than `(word, 1)` pairs, where `count` is the number of occurrences of a specific word in this job. Therefore, it is useful to aggregate data both in the reduce phase as well as directly in the map phase.

The {{product-name}} system does not have dedicated map combiner support. Large keys are frequently caused by a failure to aggregate in the map phase. Thus, if aggregation is possible, we recommend aggregating.

## Numerous output tables in operation

A RAM buffer is reserved for each operation output table. When operation launches, all the output table buffer sizes are added together and added to the amount of memory indicated by a user in the operation spec.

Technically, output table buffer size is implicitly counted as part of the `JobProxy` process memory and depends on cluster settings as well as the value of an output table's `erasure_codec` attribute. For more information on `JobProxy`, see the Jobs todo section.

If there are many output tables, the total amount of memory may turn out be fairly large; so, the scheduler will not be able quickly to find a cluster node with the right amount of free memory or may not find one at all. This will cause the operation to be aborted, and the `No online node can satisfy the resource demand` error to be displayed for the user. The error message will indicate the amount of memory requested.

{% note info "Note" %}

We recommend specifying no more than several dozen output tables for an operation.

{% endnote %}
