# YT Pragmas

YT pragmas are a namespace for pragmas that configure {{product-name}}‑specific parameters of YQL queries.

## Syntax

YT pragma names include the `yt` prefix:

```yql
PRAGMA yt.<pragma_name> = '<value>';
```

{% note warning %}

Pass the values of all YT pragmas as strings, in quotes, regardless of type. Use either single `'...'` or double `"..."` quotes.

For example: `PRAGMA yt.QueryCacheUseExpirationTimeout = 'true';`

{% endnote %}

## Scope and features

By scope, YT pragmas can be divided into static and dynamic ones.

**Static pragmas**:

- You initialize them once at the earliest stage of query processing.
- They affect all expressions in the current module where they’re declared.
- If you specify a static pragma multiple times in a query, only its last set value applies.

**Dynamic pragmas**:

- You initialize them at the query execution stage, after optimization and execution plan building.
- They remain in effect until the next identical pragma or until the end of the query.
- Only for a dynamic pragma can you reset its value to the default by assigning `default`.

{% note info %}

All pragmas that affect query optimizers are static, because the values of dynamic pragmas aren’t computed at this stage yet.

{% endnote %}

## Per‑cluster support {#settings}

Some pragmas support a special operating mode — _per‑cluster_. This mode lets you set different pragma values for different clusters in a single query. For example, you can specify: “Run the query on cluster A with settings X, and on cluster B with settings Y.” This is useful in distributed queries where different clusters require different execution conditions.

The _per‑cluster_ mode is available for all dynamic pragmas and for some static ones — in the documentation below, such pragmas are explicitly marked with the “per‑cluster” label.

### How to use

By default, YT pragmas are written with the `yt` prefix — this means the setting applies to the current cluster where the query runs. To apply the same pragma to another cluster with a different value, replace `yt` with the cluster’s name. For example, let’s set different temporary directories for two clusters in a query:


```yql
PRAGMA yt.TmpFolder = "//tmp/my_folder";            -- current cluster
PRAGMA cluster_2.TmpFolder = "//tmp/other_folder";  -- cluster cluster_2

... -- query body
```

As a result, when the query runs, temporary files will be saved in `//tmp/my_folder` on the current cluster and in `//tmp/other_folder` on the `cluster_2` cluster.


{% note warning %}

You can’t use the `yt` prefix and a cluster name at the same time.

Entries like `PRAGMA cluster_2.yt.TmpFolder` or `PRAGMA yt.cluster_2.TmpFolder` are invalid and will cause an error.

{% endnote %}

## yt.Annotations {#annotations}

This lets you set arbitrary structured information related to the operation. It’s useful for searching and identifying operations in the archive (you can search it via the [API](../../../api/python/userdoc.md#operation_and_job_info_commands)). For details, see [Operation settings](../../../user-guide/data-processing/operations/operations-options) section.

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a [YSON Map](../../../user-guide/storage/formats.md#yson) | — | Dynamic |

#### Signature {#annotations-signature}

```yql
PRAGMA yt.Annotations = '{
    "name" = "login";
    "time" = "20.02.2002";
}';
```

#### Result {#annotations-result}

When you search for an operation in the archive, the information set in the Map will be visible.

## yt.Auth {#auth}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

Use authentication data other than the default.

## yt.AutoMerge / yt.TemporaryAutoMerge / yt.PublishedAutoMerge {#auto-merge}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: relaxed / economy / disabled | relaxed | Dynamic |

Manage the {{product-name}} setting [with the same name]({{yt-docs-root}}/user-guide/data-processing/operations/automerge) that helps reduce quota consumption for the number of chunks. `yt.TemporaryAutoMerge` applies to all YT operations except merge inside a YtPublish node.

`yt.PublishedAutoMerge` applies only to merge inside a YtPublish node (if it runs there). `yt.AutoMerge` sets this setting’s value simultaneously for all {{product-name}} operations in the query.

## yt.BatchListFolderConcurrency {#batch-list-folder-concurrency}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 5 | Static |

Set the number of concurrent directory listing operations.

## yt.BinaryExpirationInterval {#binary-expiration-interval}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for `s/m/h/d` suffixes | — | Static |

This lets you manage the [TTL of cached binary artifacts]({{yt-docs-root}}/user-guide/storage/cypress#TTL). It works only together with `yt.BinaryTmpFolder`. Each use of a binary artifact in a query extends its TTL.

## yt.BinaryTmpFolder {#binary-tmp-folder}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path on the cluster | — | Static |

Set a separate path on the cluster where the query’s binary artifacts (UDF and job binary) will be cached. Artifacts are saved in the directory root with a name equal to the artifact’s MD5. Saving and using artifacts in this directory happens outside a transaction, even if the query includes the `yt.ExternalTx` pragma.

## yt.BufferRowCount {#buffer-row-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number, not less than 1 | — | Dynamic |

Limit the number of records that JobProxy can buffer.

## yt.ColumnGroupMode {#column-group-mode}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: disable / single / perusage | disable | Static |

Set the mode for computing column groups for the query’s intermediate tables. In `disable` mode, column groups aren’t used. In `single` mode, one group is created for all table columns. In `perusage` mode, granular column groups are created based on their consumers. All columns in one group are used simultaneously by one or more consumers. For example, if an intermediate table has columns [a, b, c, d, e, f] and two operations use column selections [a, b, c, d] and [c, d, e, f] respectively, the table will have three column groups: [a, b], [c, d], and [e, f]. If the intermediate table is used to publish to an output table (i.e., the consumer is a YtPublish node), column groups aren’t applied, except when you explicitly set the [column_groups modifier](../insert_into.md#hints). In the latter case, the intermediate table uses the modifier’s column groups.

## yt.CombineCoreLimit {#combine-core-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, not less than 1M | 128M | Static |

Set the memory buffer size for running a CombineCore node.

## yt.CommonJoinCoreLimit {#common-join-core-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 128M | Static |

Set the memory buffer size for running a CommonJoinCore node (it runs in a job when you choose the common JOIN strategy).

## `yt.ConvertDynamicTablesToStatic`

| Value type | Default | Static /<br/>Dynamic |
| --- | --- | --- |
| String: disable / join / all | disable; Starting from [2026.02](../../changelog/2026.02.md) — join | Static |

Add preliminary conversion of dynamic tables to static tables. In `join` mode, only tables that are inputs to joins are converted. This lets you use the map join strategy over such tables. In `all` mode, all dynamic tables in the query are converted to static tables. In `disable` mode, no conversion happens.

## yt.CoreDumpPath {#core-dump-path}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path on the cluster | — | Static, [per‑cluster](*per-cluster) |

This lets you save a [coredump](https://en.wikipedia.org/wiki/Core_dump) from failed MapReduce operation jobs to a separate table.

## yt.DataSizePerJob / yt.DataSizePerMapJob {#data-size-per-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 2G | Dynamic |

Manage the splitting of MapReduce operations into jobs: the larger the number, the fewer jobs. For computationally intensive jobs, it’s recommended to decrease the value, and for jobs that quickly scan a lot of data (in particular, user_sessions), increase it.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DataSizePerPartition {#data-size-per-partition}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 1G | Dynamic |

Manage the size of partitions in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DataSizePerSortJob {#data-size-per-sort-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | - | Dynamic |

Manage the splitting of sort jobs in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DefaultCalcMemoryLimit {#default-calc-memory-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 1G | Static |

This sets the memory limit for computations that aren't related to table access.

You can use the K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DefaultLocalityTimeout {#default-locality-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for `s/m/h/d` suffixes | — | Dynamic |

This sets the `locality_timeout` setting in the operation spec (the setting isn't documented yet).

## yt.DefaultMapSelectivityFactor {#default-map-selectivity-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive floating-point number | — | Dynamic |

This sets the approximate output-to-input ratio for the map stage in a combined MapReduce operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/mapreduce).

## yt.DefaultMaxJobFails {#default-max-job-fails}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive integer | 5 | Static |

This is the number of failed MapReduce jobs. When this limit is reached, retry attempts for the query stop, and the query is considered unsuccessful.

## yt.DefaultMemoryDigestLowerBound {#default-memory-digest-lower-bound}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number between 0.0 and 1.0, inclusive | — | Dynamic |

This sets the `user_job_memory_digest_lower_bound` setting in the operation spec. You can read about the setting in the [documentation]({{yt-docs-root}}/user-guide/data-processing/scheduler/memory-digest#nastrojki-digest).

## yt.DefaultMemoryLimit {#default-memory-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 512M | Dynamic |

This sets the memory limit, in bytes, consumed by jobs. This limit is requested when you start MapReduce operations.

You can use the K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DefaultMemoryReserveFactor {#default-memory-reserve-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number between 0.0 and 1.0, inclusive | — | Dynamic |

This sets the memory reservation factor for jobs. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#memory_reserve_factor).

## yt.DefaultOperationWeight {#default-operation-weight}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number | 1.0 | Dynamic |

This sets the weight for all MapReduce operations you start within the selected compute pool.


## yt.Description {#description}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON map | — | Dynamic |

This sets the information that's displayed in the web interface on the operation page. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

## yt.DisableJobSplitting {#disable-job-splitting}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Dynamic |

Use this to prevent the {{product-name}} Scheduler from adaptively splitting long-running user jobs further.

## yt.DisableOptimizers {#disable-optimizers}

| Possible values | Default value | Type |
| --- | --- | --- |
| String with a list of optimizers separated by any of these characters: comma, semicolon, space, or `\|` | — | Static |

This disables the specified optimizers.

## yt.DockerImage {#docker-image}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path to the Docker image | — | Dynamic |

You can specify a Docker image to create the environment where your user jobs will run.

## yt.DontForceTransformForInputTables {#dont-force-transform-for-input-tables}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

This disables forced data transformation for user tables with storage settings (`erasure_codec`, `compression_codec`, `primary_medium`, `media`, column groups) that differ from the default settings.

Forced transformation is applied to input tables if they're used to write to output tables only via the YtMerge operation.

## yt.ErasureCodecCpu {#erasure-codec-cpu}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number, at least 1.0 | 1.0 | Dynamic |

This is a multiplier for estimating CPU consumption when processing tables compressed with an erasure codec. It affects how MapReduce operations are split into jobs.

## yt.EvaluationTableSizeLimit {#evaluation-table-size-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, no more than 10M | 1M | Static |

This sets the maximum total size of tables used at the evaluation stage.

## yt.ExpirationDeadline / yt.ExpirationInterval {#expiration-deadline}

| Possible values | Default value | Type |
| --- | --- | --- |
| ExpirationDeadline: timestamp in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format. ExpirationInterval: time interval with support for `s/m/h/d` suffixes, during which the node must have no access requests. | — | Dynamic |

This lets you manage the [TTL for tables created by the operation]({{yt-docs-root}}/user-guide/storage/cypress#TTL).

## yt.ExternalTx {#external-tx}

This pragma is used to specify an external [transaction](../../../user-guide/storage/transactions.md) in which context the query will run (for example, if the transaction was created via [CLI](../../../api/cli/commands.md#start-tx)). You might need this to perform multiple actions atomically—for example, two YQL queries or a YQL query and a table change.

This pragma also lets you read intermediate data in transactions (for example, in a long operation with temporary files).

{% note info %}

Directories for the query are created within the specified transaction. This can lead to conflicts when two queries with different ExternalTx values try to write data to a directory that doesn't yet exist.

{% endnote %}

| Possible values | Default value | Type |
| --- | --- | --- |
| Transaction ID (string) | — | Static, [per-cluster](*per-cluster) |

#### Example {#external-tx-example}

```yql
PRAGMA yt.ExternalTx = 'a3d149bd-674dfa1-fa68f314-2c22562';   -- Transaction ID (you can view it via CLI)
```

#### Result {#external-tx-result}

The query will run within the specified transaction.

#### Notes and limitations {#external-tx-limitations}

Specifying the pragma without a query body will cause an error.

## yt.ExtraTmpfsSize {#extra-tmpfs-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Dynamic |

This lets you increase the tmpfs size in addition to the total size of all explicitly used files (specified in megabytes). This can be useful if you create new local files from a UDF. This setting is ignored without [UseTmpfs](#use-tmpfs).

## yt.FileCacheTtl {#file-cache-ttl}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for `s/m/h/d` suffixes | 7d | Static |

This lets you manage the TTL for the {{product-name}} [file cache](../../faq/temp.md). A value of 0 disables TTL for the file cache.

## yt.FolderInlineDataLimit {#folder-inline-data-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 100K | Static |

This sets the maximum data size for an inline list obtained as a result of the Folder computation. If the size is larger, a temporary file will be used.

## yt.FolderInlineItemsLimit {#folder-inline-items-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive integer | 100 | Static |

This sets the maximum number of items in an inline list obtained as a result of the Folder computation. If the number is larger, a temporary file will be used.

## yt.ForceJobSizeAdjuster {#force-job-size-adjuster}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Dynamic |

This sets the `"force_job_size_adjuster"` option in the operation settings.


## yt.HybridDqExecution {#hybrid-dq-execution}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Static |

This enables hybrid query execution via DQ.

## yt.IgnoreTypeV3 {#ignore-type-v}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

When you read tables with a type_v3 schema, all fields with complex types will appear in the query as Yson fields. Complex types include all non-data types and data types with optionality greater than one level.

## yt.IgnoreWeakSchema {#ignore-weak-schema}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

This ignores the weak table schema (generated by sorting a schema-less table by a set of fields).

Together with `yt.InferSchema`, this lets you infer the schema from the data for such tables.

## yt.IgnoreYamrDsv {#ignore-yamr-dsv}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

This ignores `_format=yamred_dsv` if it’s specified in the input table’s metadata.

## yt.InferSchema / yt.ForceInferSchema {#infer-schema}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 1000 | — | Static |

This infers the data schema from the content of the first rows of the tables. If you specify the PRAGMA without a value, it means one first row. If you specify several rows and different data types appear for a column, they’re expanded up to Yson.

InferSchema enables schema inference only for tables that don’t have a schema specified in the metadata at all. With ForceInferSchema, the data schema from the metadata is ignored, except for the list of key columns for sorted tables.

In addition to the detected columns, a dictionary column _other (row by row) is generated with the values of columns that weren’t present in the first row but were found later. This lets you use [WeakField](../../builtins/basic.md#weakfield) on such tables.

Because of the wide range of possible issues, this mode isn’t recommended and is disabled by default.

## yt.InferSchemaTableCountThreshold {#infer-schema-table-count-threshold}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 50 | Static |

If the number of tables for which the schema is inferred from the content exceeds the specified value, schema inference runs as a separate operation on {{product-name}}, which can be significantly faster.

## yt.IntermediateAccount {#intermediate-account}

| Possible values | Default value | Type |
| --- | --- | --- |
| Account name in {{product-name}} | intermediate | Dynamic |

This lets you use your own account for intermediate data within a fused MapReduce operation.

By default, a shared account is used, which might overflow at an inconvenient moment.

If you specify [PRAGMA yt.TmpFolder](#tmp-folder), the account specified on the temporary directory is used by default instead of the shared one.

## yt.IntermediateDataMedium {#intermediate-data-medium}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

This sets the medium used for intermediate data in operations (Sort, MapReduce). For details, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/sort).

## yt.IntermediateReplicationFactor {#intermediate-replication-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 10 | — | Dynamic |

This is the replication factor for intermediate data.

## yt.JobEnv {#job-env}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON dictionary | — | Dynamic |

This sets environment variables for the map and reduce jobs of the operations. The keys in the dictionary set the names of the environment variables, and the values in the dictionary set the values of the environment variables.

## yt.JoinAllowColumnRenames {#join-allow-column-renames}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Static |

This enables the use of column renaming when executing the Ordered JOIN strategy (the [rename_columns]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#path_attributes) attribute is used). If the option is disabled, the Ordered JOIN strategy is selected only when the column names on the left and right match.

## yt.CostBasedOptimizerPartial {#yt.costbasedoptimizerpartial}

This controls when the [cost-based optimizer](global.md#costbasedoptimizer) starts for queries with multiple `JOIN`s. The pragma lets you avoid waiting for all inputs to be materialized and preserve computation parallelism if a full recalculation of the join order isn’t required.

| Value type | Default | Static /<br/>dynamic |
| --- | --- | --- |
| Non-negative number | 0 | Static |

The value determines how many `JOIN` inputs must be ready and materialized to disk before reordering:

* `0` — wait for all inputs and perform full reordering. This mode gives the optimizer the most information but reduces parallelism and can interfere with operation merging.
* `1` — don’t wait for inputs and optimize only the already ready subtrees. This mode preserves parallelism but may change a smaller part of the plan.
* `N > 1` — experimental mode: start optimization when at least `N` inputs are ready.

#### Example {#cost-based-optimizer-partial-example}

```yql
PRAGMA CostBasedOptimizer = 'native';
PRAGMA yt.CostBasedOptimizerPartial = '1';
PRAGMA yt.ExtendedStatsMaxChunkCount = '10000';
```

#### Result {#cost-based-optimizer-partial-result}

The cost-based optimizer will reorder the ready `JOIN` subtrees without waiting for the materialization of the other inputs. Extended statistics will be requested only for tables whose total number of chunks on the cluster doesn’t exceed 10,000.

#### Notes and limitations {#cost-based-optimizer-partial-limitations}

* The pragma works only when [`CostBasedOptimizer`](global.md#costbasedoptimizer) is enabled.
* Values greater than `1` enable experimental mode: the threshold semantics and plan construction may change in future YQL versions. For persistent queries, it’s recommended to use `0` or `1`.
* The partial mode preserves more parallelism but may build a less optimal `JOIN` order than full reordering after all inputs are ready.

## yt.ExtendedStatsMaxChunkCount {#yt.extendedstatsmaxchunkcount}

This limits the collection of extended column-wise statistics for the cost-based optimizer. Use this pragma so that getting statistics for tables with a large number of chunks doesn’t delay query optimization.

| Value type | Default | Static /<br/>dynamic |
| --- | --- | --- |
| Positive number | — | Static |

The value sets the maximum total number of chunks of input tables on one cluster:

* If the number of chunks doesn’t exceed the threshold, YQL requests extended statistics.
* If the threshold is exceeded, statistics aren’t requested and the optimizer uses less accurate estimates.
* `0` removes the limit on the number of chunks.
* If the pragma isn’t set, extended statistics aren’t requested.

#### Example {#extended-stats-max-chunk-count-example}

```yql
PRAGMA CostBasedOptimizer = 'native';
PRAGMA yt.ExtendedStatsMaxChunkCount = '10000';
```

#### Result {#extended-stats-max-chunk-count-result}

For input tables with a total number of chunks not exceeding 10,000 on the cluster, the optimizer will get extended column-wise statistics. For larger sets of tables, optimization will continue without it.

#### Notes and limitations {#extended-stats-max-chunk-count-limitations}

* The pragma affects only the collection of statistics for the cost-based optimizer and doesn’t limit the reading of the tables themselves.
* The value `0` may lead to a long time to get statistics for very large tables.
* The recommended initial value is `10000`; you should change it considering the number of chunks in the input tables and the acceptable optimization time.

## yt.JoinCollectColumnarStatistics {#join-collect-columnar-statistics}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: disable / sync / async | async | Static |

This controls the use of column-wise statistics for accurate estimation of JOIN inputs and selection of the corresponding strategy. Async enables the asynchronous collection mode for column-wise statistics.

## yt.JoinColumnarStatisticsFetcherMode {#join-columnar-statistics-fetcher-mode}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: from_nodes / from_master / fallback | fallback | Static |

This controls the mode for requesting column-wise statistics for accurate estimation of JOIN inputs from {{product-name}}. The from_nodes mode provides an accurate estimate but may not meet timeouts for large tables. The from_master mode works very fast but gives coarse statistics. The fallback mode works as a combination of the previous two.

## yt.JoinMergeForce {#join-merge-force}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | - | Static |

This forces the selection of the Ordered JOIN strategy. If the flag is set to True, the Ordered JOIN strategy is selected even if one or both sides of the JOIN aren’t sorted. In this case, the unsorted sides are sorted beforehand. The limits on the maximum size of the unsorted table (see `yt.JoinMergeUnsortedFactor`) are ignored in this case.

## yt.JoinMergeReduceJobMaxSize {#join-merge-reduce-job-max-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 8G | Static |

This is the maximum allowed size of the Reduce job when a small table is selected as the primary in the Ordered JOIN strategy. If the resulting size exceeds the specified value, the Reduce operation is repeated with the larger table as the primary.

## yt.JoinMergeTablesLimit {#join-merge-tables-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 64 | Static |

This is the total allowed number of tables on the left and right to enable the Ordered JOIN strategy.

Setting the value to 0 disables this strategy completely.

## yt.JoinMergeUnsortedFactor {#join-merge-unsorted-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive floating-point number | 0.2 | Static |

This is the minimum ratio of the size of the unsorted side of the JOIN to the sorted side for its additional sorting and selection of the Ordered JOIN strategy.

## yt.JoinMergeUseSmallAsPrimary {#join-merge-use-small-as-primary}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | - | Static |

This explicitly controls the selection of the primary table in the Reduce operation for the Ordered JOIN strategy. If set to true, the smaller side is always selected as the primary table. If the flag value is false, the larger side is selected, except in the case of unique keys on the larger side. Selecting the larger table as the primary is safe even if it contains monster keys, but it works slower. If this pragma isn’t set, the primary is selected automatically based on the maximum size of the resulting jobs (see yt.JoinMergeReduceJobMaxSize).

## yt.LayerPaths {#layer-paths}

| Possible values | Default value | Type |
| --- | --- | --- |
| String with a list of paths to porto layers, separated by any of the following characters: comma, semicolon, space, or `\|` | — | Dynamic |

This lets you specify a sequence of porto layers to form the environment in which user jobs will run.

## yt.LLVMMemSize {#llvm-mem-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 256M | Dynamic |

Set the fixed memory size required for LLVM code compilation in jobs.

## yt.LLVMPerNodeMemSize {#llvm-per-node-mem-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 10K | Dynamic |

Set the memory size per computation graph node required for LLVM code compilation in jobs.

## yt.LookupJoinLimit {#lookup-join-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, no more than 10M | 1M | Static |

You can use a table as a Map in the Lookup JOIN strategy if its size does not exceed the minimum of `yt.LookupJoinLimit` and `yt.EvaluationTableSizeLimit`.

## yt.LookupJoinMaxRows {#lookup-join-max-rows}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number, no more than 1000 | 900 | Static |

This is the maximum number of rows in a table that can act as a Map in the Lookup JOIN strategy.

## yt.MapJoinLimit {#map-join-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 2048M | Static |

This sets the size limit for the smaller table in a JOIN that triggers the Map-side strategy (creating an in-memory Map from the smaller table and using it in the Map over the larger table).

If you set the value to 0, you can completely disable this strategy.

## yt.MapJoinShardCount {#map-join-shard-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 10 | 4 | Static |

The Map-side JOIN strategy can run in a sharded mode: the smaller side splits into N shards (where N is less than or equal to the value of this PRAGMA), each of which joins with the larger side independently and in parallel. The JOIN result is then the concatenation of the JOINs with the shards.

## yt.MapJoinShardMinRows {#map-join-shard-min-rows}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1 | Static |

This sets the minimum number of records per shard in the Map-side JOIN strategy.

## yt.MapLocalityTimeout {#map-locality-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for `s/m/h/d` suffixes | — | Dynamic |

Set the `map_locality_timeout` setting in the operation specification (this setting is not yet documented).

## yt.MaxColumnGroups {#max-column-groups}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 64 | Static |

Set the maximum number of column groups for the intermediate query table. If the calculated number of groups exceeds this limit, no groups are created for this table.

## yt.MaxExtraJobMemoryToFuseOperations {#max-extra-job-memory-to-fuse-operations}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 2G | Static |

This is the maximum memory consumption allowed for jobs after the optimizers merge operations.

## yt.MaxInputTables {#max-input-tables}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1000 | Static |

This limits the number of tables that you can pass as input to each specific MapReduce operation.

## yt.MaxInputTablesForSortedMerge {#max-input-tables-for-sorted-merge}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 100 | Static |

This limits the number of tables that you can pass as input to a sorted merge operation.

## yt.MaxJobCount {#max-job-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive integer | 16384 | Dynamic |

This is the maximum number of jobs within a single {{product-name}} operation. It applies only to single-stage map, reduce, merge, and similar operations. If you specify both [`yt.DataSizePerJob`](#data-size-per-sort-job) and `yt.MaxJobCount`, job slicing runs with [`yt.DataSizePerJob`](#data-size-per-sort-job) taken into account. Even if the resulting value `N` exceeds `yt.MaxJobCount`, `N` jobs will run. `yt.MaxJobCount` only affects whether jobs split after their count reaches a certain threshold.

## yt.MaxKeyWeight {#max-key-weight}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, up to 256K | 16K | Dynamic |

Increase the limit on the maximum length of table keys in {{product-name}} by which the table is sorted.

## yt.MaxOutputTables {#max-output-tables}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 100 | 50 | Static |

This limits the number of output tables for each specific MapReduce operation.

## yt.MaxReplicationFactorToFuseOperations {#max-replication-factor-to-fuse-operations}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number, not less than 1.0 | 20.0 | Static |

This is the maximum data replication factor allowed after the optimizers merge operations.

## yt.MaxRowWeight {#max-row-weight}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, up to 128M | 16M | Dynamic |

Increase the limit on the maximum row length in a yt table.

## yt.MaxSpeculativeJobCountPerTask {#max-speculative-job-count-per-task}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | — | Dynamic |

Set the number of speculative jobs in {{product-name}} operations. By default, the {{product-name}} cluster settings are used.

## yt.MinColumnGroupSize {#min-column-group-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number, not less than 2 | 2 | Static |

Set the minimum size of a column group. If the calculated group contains fewer columns than the pragma value, the group is not created.

## yt.MinLocalityInputDataWeight {#min-locality-input-data-weight}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Dynamic |

Set the `min_locality_input_data_weight` setting in the operation specification (this setting is not yet documented).

## yt.UseQLFilter {#yt.useqlfilter}

Pass the compatible part of the `WHERE` condition to {{product-name}} via [`input_query`]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options). {{product-name}} uses `min`/`max` statistics to avoid reading chunks and blocks where the condition is definitely false. This pragma is useful for selective reading of large static tables with a strict schema.

| Value type | Default | Static /<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

#### Example {#use-ql-filter-example}

```yql
PRAGMA yt.UseQLFilter;

SELECT
    key AS key,
    value AS value
FROM `//path/to/table`
WHERE key >= 1000 AND key < 2000;
```

#### Result {#use-ql-filter-result}

Compatible comparisons from `WHERE` are passed to `input_query`. If the table statistics prove that a chunk or block contains no `key` values within the specified range, {{product-name}} skips it without reading from disk. The rest of the condition continues to be evaluated using YQL.

#### Notes and limitations {#use-ql-filter-limitations}

- You can use numeric types, `Bool`, `String`, `Utf8`, and their `Optional` variants.
- You can use comparisons `<`, `<=`, `>`, `>=`, `==`, `!=` between a column and a constant expression, as well as `AND`, `OR`, `NOT`, `EXISTS`, `COALESCE`.
- The table must have a strict schema.
- Dynamic tables are not supported.
- The pragma does not apply to tables with a custom schema or columns defined via `WITH SCHEMA` or `WITH COLUMNS`.

## yt.MinPublishedAvgChunkSize {#min-published-avg-chunk-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Static |

If the average chunk size in the resulting output table is smaller than the specified setting, an additional {{product-name}} Merge operation runs to enlarge the chunks to the specified size. The value 0 has a special meaning — in this case, merge always runs and enlarges the chunks to 1G.

If the table uses a compression codec, the output chunk size may differ from the specified value by the compression ratio. Essentially, this pragma sets the data size per merge job. After compression, the output size may be significantly smaller. In this case, increase the pragma value by the expected compression ratio.

## yt.MinTempAvgChunkSize {#min-temp-avg-chunk-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Static |

This setting works the same as `yt.MinPublishedAvgChunkSize`, but it applies to intermediate temporary tables.

## yt.NetworkProject {#network-project}


| Possible values | Default value | Type |
| --- | --- | --- |
| String | `yt.StaticNetworkProject` | Dynamic |

Set the use of the specified network project in the jobs for regular operations in the request.


## yt.NightlyCompress {#nightly-compress}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | - | Dynamic |

Manage the process of background table compression so that the tables take up less space.

Setting the value to `true` sets the table's `@force_nightly_compress` attribute to `true`.  
Setting the value to `false` sets the table's `@nightly_compression_settings` attribute with the child value `enabled` to `false`.

The setting applies only to tables that are newly created by a YQL request (and to tables that are overwritten using [INSERT INTO ... WITH TRUNCATE](insert_into)).  
The setting doesn't apply to temporary tables.

## yt.OmitInaccessibleRows {#omit-inaccessible-rows}

Manage the behavior when reading tables with [row-level ACL]({{yt-docs-root}}/user-guide/storage/row-level-security) (RLS).

By default, reading a table with row-level ACL set results in an authorization error if the user doesn't have the `full_read` permission. The `yt.OmitInaccessibleRows` pragma changes this behavior: when enabled, rows without access are skipped, and the request completes successfully. Only the rows allowed by the RLS predicate are included in the result.

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

#### Example {#omit-inaccessible-rows-example}

```yql
PRAGMA yt.OmitInaccessibleRows = "true";

SELECT *
FROM `//path/to/table_with_rls`;
```

#### Result {#omit-inaccessible-rows-result}

The request returns only the rows that are accessible to the current user according to the row-level ACL. Rows that aren't accessible are skipped without an error.

#### Limitations {#omit-inaccessible-rows-restrictions}

- You can't specify `row_index` in `ranges` when reading a table with row-level ACL — the request will result in an error. The row indexes in `ranges` are counted relative to the physical rows on the disk, not the rows accessible to the user. For example, `//path/to/table[:#100]` will return up to 100 rows from the disk, some of which may be inaccessible and will be filtered out.
- RLS isn't supported for dynamic tables — any read operation will result in an error.

## yt.OperationReaders {#operation-readers}

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with a list of logins separated by any of the following characters: comma, semicolon, space, or ` | ` | Dynamic |

Allow you to grant read permissions for the MapReduce operations created in {{product-name}} to users other than the owner of the YQL operation.

## yt.OperationSpec {#operation-spec}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON map | — | Dynamic |

Set a map of operation settings. This lets you specify settings that don't have equivalents as pragmas. Settings specified through specialized pragmas have higher priority and overwrite the values in this map.

## yt.OptimizeFor {#optimize-for}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: lookup / scan | scan | Dynamic |

Manage the `optimize_for` attribute on the tables that are created.

## yt.Owners {#owners}

Allow you to grant access to manage [MapReduce operations](../../../user-guide/data-processing/operations/mapreduce.md) in {{product-name}} (cancel, pause, run-job-shell, etc.) to users other than the one who launched the query.

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with a list of logins separated by any of the following characters: `,`, `;`, ` `, or `\|` | — | Dynamic |


#### Example {#owners-example}

```yql
PRAGMA yt.Owners = 'ivanov petrov';    -- user logins separated by a space
```

#### Result {#owners-result}

The specified users will be able to manage the MapReduce operations.

## yt.ParallelOperationsLimit {#parallel-operations-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| A number, at least 1 | 16 | Static |

Set the maximum number of {{product-name}} operations that can run simultaneously within a request.

## yt.Pool {#pool}

Use this to override the compute pool, which by default is the current user's login or was set by the `yt.StaticPool` pragma.

| Value type | Default value | Type |
| --- | --- | --- |
| String | The value of the `yt.StaticPool` pragma — if it was set earlier; the current user's login — if `yt.StaticPool` wasn't set | Dynamic |

#### Signature {#pool-signature}

```yql
PRAGMA yt.StaticPool = '<pool_1>';
PRAGMA yt.Pool = '<pool_2>';
```

#### Result {#pool-result}

After you specify the `yt.StaticPool` pragma, the request will run in pool `<pool_1>`, and after the `yt.Pool` pragma — in `<pool_2>`.

## yt.PoolTrees {#pool-trees}

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with a list of tree names separated by any of the following characters: comma, semicolon, space, or ` | ` | Dynamic |

Let you choose pool trees that are different from the default one.

## yt.PrimaryMedium {#primary-medium}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

Set the [primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary) for Published and Temporary tables and for intermediate data in operations. This is equivalent to setting the `yt.IntermediateDataMedium`, `yt.PublishedPrimaryMedium`, and `yt.TemporaryPrimaryMedium` pragmas at the same time.

## yt.PublishedCompressionCodec / yt.TemporaryCompressionCodec {#published-compression-codec}

| Possible values | Default value | Type |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/compression) | zstd_5 | Dynamic |

Set the compression settings for tables created via YQL.

Published tables are the tables specified in [INSERT INTO](../insert_into.md); all other tables are considered Temporary. The codec specified as Temporary is also used for intermediate data within a single {{product-name}} operation, for example, a merged MapReduce.

## yt.PublishedErasureCodec / yt.TemporaryErasureCodec {#published-erasure-codec}

| Possible values | Default value | Type |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/replication#erasure) | none | Dynamic |

Erasure coding is disabled by default. To enable it, we recommend using the `lrc_12_2_2` value.

The difference between Published and Temporary is the same as for [CompressionCodec](#published-compression-codec).

## yt.PublishedMedia / yt.TemporaryMedia {#published-media}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON map | — | Dynamic |

Set the `@media` attribute on the tables that are newly created. If present, this attribute specifies [the media in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#naznachenie-mediuma) where the table's chunks will be stored.

Published tables are the tables specified in [INSERT INTO](../insert_into.md); all other tables are considered Temporary.

## yt.PublishedPrimaryMedium / yt.TemporaryPrimaryMedium {#published-primary-medium}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

Set the `@primary_medium` attribute on the tables that are newly created. If present, this attribute specifies [the primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary) where the chunks will be written. By default, {{product-name}} sets the primary medium to `"default"`.

Published tables are the tables specified in [INSERT INTO](../insert_into.md); all other tables are considered Temporary.

## yt.PublishedReplicationFactor / yt.TemporaryReplicationFactor {#published-replication-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| A number from 1 to 10 | — | Dynamic |

Set the replication factor for tables created via YQL.

Published tables are the tables specified in [INSERT INTO](../insert_into.md); all other tables are considered Temporary.

## yt.PythonCpu {#python-cpu}

| Possible values | Default value | Type |
| --- | --- | --- |
| A floating-point number, at least 1.0 | 4.0 | Dynamic |

Set a multiplier for estimating the CPU consumption of [Python UDF](../../udf/python.md). This affects how MapReduce operations are split into jobs.

## yt.QueryCacheChunkLimit {#query-cache-chunk-limit}

Use this pragma to control how tables are written to the cache, depending on the number of chunks in the table: with the `concatenate` command (as‑is) or with the `merge` command (with chunk merging).

| Possible values | Default | Type |
| --- | --- | --- |
| A string containing the number of chunks — `<Uint64>` | `'0'` | Dynamic |

#### Example { #query-cache-chunk-limit-example }

```yql
PRAGMA yt.QueryCacheChunkLimit = '100000';
```

#### Result { #query-cache-chunk-limit-result }

If the number of chunks in the table is less than the set limit, the table is written to the cache with the `concatenate` command as‑is.

If the number of chunks exceeds the set limit, the table is written to the cache using the `merge` command (with chunk merging).

So, with the default value of `0`, tables are written to the cache using the `merge` command.

#### Notes and limitations { #query-cache-chunk-limit-constraint }

The `concatenate` command — writing as‑is — runs faster than `merge`, which needs time to merge the chunks and then write them. You’ll get better performance when caching tables with a relatively small number of chunks by using `concatenate`.

## yt.QueryCacheIgnoreTableRevision {#query-cache-ignore-table-revision}

Use this pragma to avoid flushing the cache when data in a table changes. This speeds up debugging complex queries on large, mutable tables when the query logic doesn’t depend on data changes.

By default, when calculating the hash from the table metadata, the revision number — {{product-name}} revision — is taken into account along with other data. The revision is a non‑negative integer that increases each time the table changes. For more details about the `revision` attribute, see the [Metadata Tree]({{yt-docs-root}}/user-guide/storage/cypress#time_attributes) section.

With this pragma, you can control cache flushing: you can include or exclude the revision number when calculating the hash from the table metadata.

| Possible values | Default | Type |
| --- | --- | --- |
| A string containing `true` or `false` | `false` | Static |

#### Example { #query-cache-ignore-table-revision-example }

```yql
PRAGMA yt.QueryCacheIgnoreTableRevision = 'true';
```

#### Result { #query-cache-ignore-table-revision-result }

If you set the flag to `true`, the {{product-name}} revision number is excluded when calculating the hash from the table metadata, and the Query Cache isn’t flushed when the content of the input tables changes.

#### Notes and limitations { #query-cache-ignore-table-revision-constraint }

{% note warning %}

Don’t use this pragma in production. Use it only when debugging complex queries to reduce their execution time.

{% endnote %}

## yt.QueryCacheMode {#query-cache-mode}

Use the `yt.QueryCacheMode` pragma to control caching of MapReduce operation results in {{product-name}}.

The cache stores results of previous operations: if the same operation has already run in a previous request, it won’t be launched again. Instead, {{product-name}} will take the ready result from the cache. This speeds up requests that contain computations identical to those in other requests.

Query Cache is also useful when you’re debugging or making relatively small changes to a query: in this case, the results of most operations are taken from the cache.

| Possible values | Default | Type |
| --- | --- | --- |
| `'disable'` / `'readonly'` / `'refresh'` / `'normal'` | `'normal'` | Static |

#### Example { #query-cache-mode-example }

```yql
PRAGMA yt.QueryCacheMode = 'disable';
```

#### Result { #query-cache-mode-result }

Depending on the selected mode, {{product-name}} will use, ignore, or update the cache:

- `disable` — the cache is disabled. {{product-name}} doesn’t check the cache and doesn’t write results to it. This mode is suitable for production where data changes frequently: it saves resources and avoids unnecessary read and write operations to the cache.
- `readonly` — read‑only mode. {{product-name}} takes the result from the cache if it exists, but doesn’t save new results to it. This mode is good for testing or debugging: you can use old results and avoid cluttering the cache with new entries.
- `refresh` — write‑only mode. {{product-name}} saves results to the cache but doesn’t read from it. Use this mode to refresh the cache: run a query without reading from the cache and save its result for future use.
- `normal` — the default mode. {{product-name}} both reads from and writes to the cache. This mode is especially useful during development and debugging when you run the same query multiple times to speed up iterations.

In `normal` and `refresh` modes, the operation results are additionally saved at the path `//<tmp_folder>/query_cache/<hash>`, where:

- `tmp_folder` — the temporary directory. By default, it’s `tmp/<login>`; you can set it using the [yt.TmpFolder](#tmp-folder) pragma.
- `hash` — the hash of the significant metadata and data of the input tables and the logical program that run in the operation.

{% note info %}

In `normal` mode, when you start a MapReduce operation, the system looks for the cache at the path `//<tmp_folder>/query_cache/<hash>`. If there’s no cache, {{product-name}} checks whether another operation that’s calculating the same cache is running in parallel. If such an operation exists but hasn’t written the result yet, the first operation waits for it to finish and then takes the ready cache.

Keep in mind that the second request’s operation, which is calculating the cache, might be allocated fewer resources than the first request’s operation, which is waiting. So the first request will run slower than it would if it executed without reading from the cache.

{% endnote %}

## yt.QueryCacheTtl {#query-cache-ttl}

Use this pragma to set the storage time for tables created by an operation in the query cache — [TTL (Time to Live)]({{yt-docs-root}}/user-guide/storage/cypress) — in the directory `<tmp_folder>/query_cache/<hash>`, where `tmp_folder` is the temporary directory. By default, it’s `tmp/<login>`; you can set it using the [yt.TmpFolder](#tmp-folder) pragma.

| Possible values | Default | Type |
| --- | --- | --- |
| A string containing a time interval in the specified format: a number and a suffix `s/m/h/d` (seconds, minutes, hours, days) | `'7d'` | Static |

#### Example { #query-cache-ttl-example }

```yql
PRAGMA yt.QueryCacheTtl = '3h';
```

#### Result { #query-cache-ttl-result }

If you explicitly specify a value in the pragma, the Query Cache will be cleared after the set interval. The interval is counted from the moment the table is created in the query cache or from the moment the table was last used (see the [yt.QueryCacheUseExpirationTimeout](#query-cache-use-expiration-timeout) pragma).

If you don’t set an interval, the Query Cache will be cleared automatically after 7 days (by default).

## yt.QueryCacheUseExpirationTimeout {#query-cache-use-expiration-timeout}

This pragma defines the mode for counting the TTL interval for tables in the query cache.

| Possible values | Default | Type |
| --- | --- | --- |
| A string containing `true` or `false` | `false` | Static |

#### Example { #query-cache-use-expiration-timeout-example }

```yql
PRAGMA yt.QueryCacheUseExpirationTimeout = 'true';
```

#### Result { #query-cache-use-expiration-timeout-result }

With the default value of `false`, the TTL is counted from the moment the table is created in the query cache.

If you set the value to `true`, the TTL is counted from the moment the table was last used.

#### Notes and limitations { #query-cache-use-expiration-timeout-constraint }

{% note warning %}

Use this pragma only together with the [yt.QueryCacheTtl](#query-cache-ttl) pragma: without specifying a TTL interval, it has no effect.

{% endnote %}

## yt.ReduceLocalityTimeout {#reduce-locality-timeout}

| Possible values | Default | Type |
| --- | --- | --- |
| A time interval with support for the `s/m/h/d` suffixes | — | Dynamic |

This pragma sets the `reduce_locality_timeout` setting in the operation specification (the setting isn’t documented yet).

## yt.ReleaseTempData {#release-temp-data}

| Possible values | Default | Type |
| --- | --- | --- |
| A string: `immediate` / `finish` / `never` | `immediate` | Static |

Use this pragma to control when to delete temporary objects (for example, tables) that are created during query execution:

- `immediate` — delete the objects as soon as they’re no longer needed.
- `finish` — delete them at the end of the entire YQL query execution.
- `never` — never delete them.


## yt.SamplingIoBlockSize {#sampling-io-block-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Dynamic |

Set the minimum block size for coarse-grained sampling.


## yt.ScriptCpu {#script-cpu}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number, not less than 1.0 | 1.0 | Dynamic |

This is a multiplier for estimating CPU consumption of script UDFs (including [Python UDF](../../udf/python.md)). It affects how MapReduce operations are split into jobs. You can override it with specialized pragmas `yt.PythonCpu` / `yt.JavascriptCpu` for a specific UDF type.

## yt.SortLocalityTimeout {#sort-locality-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for `s/m/h/d` suffixes | — | Dynamic |

Set the `sort_locality_timeout` setting in the operation specification (this setting is not documented yet).

## yt.StartedBy {#started-by}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON map | — | Dynamic |

Set a map that describes the client through which the operation was started. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).


## yt.StaticPool {#static-pool}

Use this to override the compute pool, which by default is the current user's login.

You can set only one new value for `yt.StaticPool`. If you specify the static pragma multiple times, its last value will be used. If you need to override the value for the next query, set it using the dynamic pragma `yt.Pool`.

| Value type | Default value | Type |
| --- | --- | --- |
| String | Current user's login | Static, [per-cluster](*per-cluster) |

#### Signature {#static-pool-signature}

```yql
PRAGMA yt.StaticPool = '<pool_1>';
```

#### Result {#static-pool-result}

After you set the pragma, the query will run in the `<pool_1>` pool.

#### Notes and limitations {#static-pool-limitations}

- Specifying a non-existent pool will cause a query execution error.
- If you specify `yt.StaticPool` multiple times, the last pragma value is used for all queries. For example, both queries written after the pragma with the `pool_1` value and queries after the pragma with `pool_2` will run in `pool_2`:

    ```yql
    PRAGMA yt.StaticPool = '<pool_1>';
    PRAGMA yt.StaticPool = '<pool_2>';
    ```

## yt.SuspendIfAccountLimitExceeded {#suspend-if-account-limit-exceeded}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Dynamic |

Suspend the operation if the "Account limit exceeded" error occurs in the jobs. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

## yt.SwitchLimit {#switch-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, not less than 1M | 128M | Static |

Set the memory buffer size for executing the Switch node.

## yt.TableContentCompressLevel {#table-content-compress-level}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number, up to and including 11 | 8 | Dynamic |

Configure the compression level for table content delivered via a file (when `yt.TableContentDeliveryMode="file"`).

## yt.TableContentDeliveryMode {#table-content-delivery-mode}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: native / file | native | Dynamic |

If you set the value to `native`, the table content is delivered to jobs using the native {{product-name}} mechanisms. If you set the value to `file`, the table content is first downloaded on the YQL server and then delivered to jobs as a regular file.

## yt.TableContentMaxChunksForNativeDelivery {#table-content-max-chunks-for-native-delivery}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number, up to and including 1000 | 1000 | Static |

Set the maximum number of chunks in a table for delivering it to jobs using the native {{product-name}} mechanisms. If this number is exceeded, the table is delivered via a file.

## yt.TableContentMaxInputTables {#table-content-max-input-tables}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number, up to and including 1000 | 1000 | Static |

Set the maximum number of tables for delivering them to jobs using the native {{product-name}} mechanisms. If this number is exceeded, a preliminary merge is inserted.

## yt.TableContentMinAvgChunkSize {#table-content-min-avg-chunk-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 1GB | Static |

Set the minimum average chunk size in a table for delivering it to jobs using the native {{product-name}} mechanisms. A preliminary merge is inserted for chunks that are not large enough.

## yt.TableContentTmpFolder {#table-content-tmp-folder}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path on the cluster | — | Dynamic |

Set the directory where temporary files for tables delivered via a file (when `yt.TableContentDeliveryMode="file"`) will be stored. If you don't set it, the standard {{product-name}} file cache is used.

## yt.TableContentUseSkiff {#table-content-use-skiff}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Dynamic |

Enable the Skiff format for delivering the table to operation jobs.

## yt.TablesTmpFolder {#tables-tmp-folder}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | `//tmp/yql/<login>` | Static, [per-cluster](*per-cluster) |

Set the directory for storing [temporary tables](../../faq/temp.md). This has priority over `yt.TmpFolder`.

## yt.TempTablesTtl {#temp-tables-ttl}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for `s/m/h/d` suffixes | — | Static |

Use this to manage the TTL for [temporary tables](../../faq/temp.md). It affects tables with the full result; other temporary tables are unconditionally deleted when the query finishes, regardless of this pragma.

## yt.TentativePoolTrees {#tentative-pool-trees}

| Possible values | Default value | Type |
| --- | --- | --- |
| String with a list of tree names separated by any of the following characters: comma, semicolon, space, or `\|` | — | Dynamic |

You can cautiously distribute operations to pool trees that differ from the standard ones.

## yt.TentativeTreeEligibilityMaxJobDurationRatio {#tentative-tree-eligibility-max-job-duration-ratio}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number | — | Dynamic |

This takes effect only if the `yt.TentativePoolTrees` pragma is present. It sets the acceptable job slowdown ratio in the alternative pool tree. 

## yt.TentativeTreeEligibilityMinJobDuration {#tentative-tree-eligibility-min-job-duration}

| Possible values | Default value | Type |
| --- | --- | --- |
| Milliseconds | — | Dynamic |

This takes effect only if the `yt.TentativePoolTrees` pragma is present. It sets the minimum average job duration in the alternative pool tree.

## yt.TentativeTreeEligibilitySampleJobCount {#tentative-tree-eligibility-sample-job-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | — | Dynamic |

This takes effect only if the `yt.TentativePoolTrees` pragma is present. It sets the number of jobs in the sample.

## yt.TmpFolder {#tmp-folder}

This pragma is used to specify a directory for storing temporary tables and files. For more details, see the [Temporary data](../../faq/temp.md) section.

| Possible values | Default value | Type |
| --- | --- | --- |
| Directory path (string) | Current user's directory — `//tmp/yql/<login>` | Static, [per-cluster](*per-cluster) |

#### Example {#tmp-folder-example}

```yql
PRAGMA yt.TmpFolder = '//tmp/yql/ivanov/folder';
```

#### Result {#tmp-folder-result}

Temporary tables and files will be saved in the specified directory.

## yt.TopSortMaxLimit {#top-sort-max-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1000 | Static |

This is the maximum LIMIT value used in combination with ORDER BY that triggers the TopSort optimization.

## yt.TopSortRowMultiplierPerJob {#top-sort-row-multiplier-per-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number not less than 1 | 10 | Static |

You set the expected number of records per job in a TopSort operation. The value is calculated as `LIMIT * yt.TopSortRowMultiplierPerJob`.

## yt.TopSortSizePerJob {#top-sort-size-per-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, not less than 1 | 128M | Static |

You set the expected data volume per job in a TopSort operation.

## yt.UseColumnarStatistics {#use-columnar-statistics}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: disable / auto / force / 0 (=disable) / 1 (=force) | force | Dynamic |

This enables the use of columnar statistics to accurately estimate job sizes when you run operations on tables with columnar selections. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

In auto mode, the system automatically disables the use of statistics for operations that have tables with `optimize_for=lookup` as input.

## yt.UseDefaultTentativePoolTrees {#use-default-tentative-pool-trees}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | — | Dynamic |

You set the value of the `use_default_tentative_pool_trees` option in the operation specification.

## yt.UseNativeYtTypes {#use-native-yt-types}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Static |

This enables writing values of complex types to tables using the native support for complex types in {{product-name}}.


## yt.UserSlots {#user-slots}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | Unlimited | Dynamic |

This sets an upper limit on the number of jobs that can run in parallel within a MapReduce operation.

## yt.UseSkiff {#use-skiff}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Dynamic |

This enables the Skiff format for input/output in operation jobs.

## yt.UseTmpfs {#use-tmpfs}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Dynamic |

This mounts [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) to the `_yql_tmpfs` folder in the MapReduce job sandbox. We don’t recommend using this.

[*per-cluster]: You can configure all dynamic and some static pragmas so that their effect applies only to a specific cluster. For more details, see the [per-cluster support](#settings) section.

<style>
    .dc-doc-page__aside {
        width: 235px !important;
    }
</style>
