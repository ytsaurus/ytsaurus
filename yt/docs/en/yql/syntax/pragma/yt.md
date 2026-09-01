# YT pragmas

YT pragmas are a namespace for pragmas that configure {{product-name}}‑specific parameters for YQL queries.

## Syntax

YT pragma names include the `yt` prefix:

```yql
PRAGMA yt.<pragma_name> = '<value>';
```

{% note warning %}

Pass the values of all YT pragmas as strings, in quotes, regardless of the type. Use single `'...'` or double `"..."` quotes.

For example: `PRAGMA yt.QueryCacheUseExpirationTimeout = 'true';`

{% endnote %}

## Scope and features

By scope, you can divide YT pragmas into static and dynamic ones.

**Static pragmas:**

- They are initialized once at the earliest stage of query processing.
- They apply to all expressions in the current module where they are declared.
- If you specify a static pragma multiple times in a query, only its last set value will be applied.

**Dynamic pragmas:**

- They are initialized at the query execution stage, after optimization and execution plan creation.
- They are valid until the next identical pragma or until the end of the query.
- Only for a dynamic pragma can you reset its value to the default by assigning `default`.

{% note info %}

All pragmas that affect query optimizers are static, because the values of dynamic pragmas are not yet computed at this stage.

{% endnote %}

## Per‑cluster support {#settings}

Some pragmas support a special operating mode — _per‑cluster_. This mode lets you set different pragma values for different clusters in a single query. For example, you can specify: “Run the query on cluster A with settings X, and on cluster B with settings Y.” This is useful for distributed queries where different clusters require different execution conditions.

The _per‑cluster_ mode is available for all dynamic pragmas and for some static ones — in the documentation below, such pragmas are explicitly marked with the “per‑cluster” label.

### How to use

By default, YT pragmas are written with the `yt` prefix — this means the setting will apply to the current cluster where the query is running. To apply the same pragma to another cluster with a different value, replace `yt` with the name of that cluster. For example, let’s set different temporary directories for two clusters in a query:

{% if audience == "internal" %}

```yql
PRAGMA yt.TmpFolder = "//tmp/my_folder";          -- current cluster
PRAGMA arnold.TmpFolder = "//tmp/other_folder";   -- cluster arnold

... -- query body
```

As a result, when the query runs, temporary files will be saved to `//tmp/my_folder` on the current cluster and to `//tmp/other_folder` on the `arnold` cluster.

{% else %}

```yql
PRAGMA yt.TmpFolder = "//tmp/my_folder";            -- current cluster
PRAGMA cluster_2.TmpFolder = "//tmp/other_folder";  -- cluster cluster_2

... -- query body
```

As a result, when the query runs, temporary files will be saved to `//tmp/my_folder` on the current cluster and to `//tmp/other_folder` on the `cluster_2` cluster.

{% endif %}

{% note warning %}

You can’t use the `yt` prefix and a cluster name at the same time.

Entries like {% if audience == "internal" %} `PRAGMA arnold.yt.TmpFolder` {% else %} `PRAGMA cluster_2.yt.TmpFolder` {% endif %} or {% if audience == "internal" %} `PRAGMA yt.arnold.TmpFolder` {% else %} `PRAGMA yt.cluster_2.TmpFolder` {% endif %} are invalid and will cause an error.

{% endnote %}

## yt.Annotations {#annotations}

This lets you set arbitrary structured information related to the operation. It’s useful for searching and identifying operations in the archive (you can search it via [API](../../../api/python/userdoc.md#operation_and_job_info_commands)). For more details, see the [Operation settings](../../../user-guide/data-processing/operations/operations-options) section.

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

Use authentication data other than the default ones.

## yt.AutoMerge / yt.TemporaryAutoMerge / yt.PublishedAutoMerge {#auto-merge}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: relaxed / economy / disabled | relaxed | Dynamic |

Control the [ одноименной setting of {{product-name}}]({{yt-docs-root}}/user-guide/data-processing/operations/automerge) that helps reduce quota consumption for the number of chunks. `yt.TemporaryAutoMerge` applies to all YT operations, except for merge inside a YtPublish node.

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

Let you manage the [TTL of cached binary artifacts]({{yt-docs-root}}/user-guide/storage/cypress#TTL). It works only together with `yt.BinaryTmpFolder`. Each use of a binary artifact in a query extends its TTL.

## yt.BinaryTmpFolder {#binary-tmp-folder}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path on the cluster | — | Static |

Set a separate path on the cluster where the query’s binary artifacts (UDF and job binary) will be cached. Artifacts are saved in the root of the directory with a name equal to the artifact’s md5. Saving and using artifacts in this directory happens outside the transaction, even if the query includes the `yt.ExternalTx` pragma.

## yt.BufferRowCount {#buffer-row-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number, not less than 1 | — | Dynamic |

Limit the number of records that JobProxy can buffer.{% if audience == "internal" %} For more details, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/jobs).{% endif %}

## yt.ColumnGroupMode {#column-group-mode}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: disable / single / perusage | disable | Static |

Set the mode for computing column groups for the query’s intermediate tables. In `disable` mode, column groups aren’t used. In `single` mode, one group is created for all columns in the table. In `perusage` mode, granular column groups are created based on their consumers. All columns in one group are used simultaneously by one or more consumers. For example, if an intermediate table has columns [a, b, c, d, e, f] and two operations use it with column selections [a, b, c, d] and [c, d, e, f] respectively, the table will have three column groups: [a, b], [c, d], and [e, f]. If the intermediate table is used for publishing to an output table (i.e., the consumer is a YtPublish node), column groups aren’t applied, except when you explicitly set the [column_groups modifier](../insert_into.md#hints). In the latter case, the intermediate table uses the modifier’s column groups.

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

| Value type | Default | Static / Dynamic |
| --- | --- | --- |
| String: disable / join / all | disable; Starting from [2026.02](../../changelog/2026.02.md) - join | Static |

Add preliminary conversion of dynamic tables to static tables. In `join` mode, only tables that are inputs to joins are converted. This lets you use the map join strategy on such tables. In `all` mode, all dynamic tables in the query are converted to static tables. In `disable` mode, no conversion happens.

## yt.CostBasedOptimizerPartial {#yt.costbasedoptimizerpartial}

Controls when the [cost-based optimizer](global.md#costbasedoptimizer) starts for queries with multiple `JOIN` inputs. Use this pragma to avoid waiting for every input to be materialized when preserving computation parallelism is more important than fully reordering the joins.

| Possible values | Default value | Type |
| --- | --- | --- |
| Non-negative number | 0 | Static |

The value specifies how many `JOIN` inputs must be ready and materialized before reordering:

* `0`: Wait for all inputs and perform full reordering. This gives the optimizer the most information, but reduces parallelism and may prevent operation fusion.
* `1`: Do not wait; optimize only ready subtrees. This preserves parallelism, but may reorder a smaller part of the plan.
* `N > 1`: Experimental mode that starts optimization when at least `N` inputs are ready.

#### Example {#cost-based-optimizer-partial-example}

```yql
PRAGMA CostBasedOptimizer = 'native';
PRAGMA yt.CostBasedOptimizerPartial = '1';
PRAGMA yt.ExtendedStatsMaxChunkCount = '10000';
```

#### Result {#cost-based-optimizer-partial-result}

The optimizer reorders ready `JOIN` subtrees without waiting for the remaining inputs. Extended statistics are requested only when the total number of table chunks on a cluster does not exceed 10,000.

#### Features and limitations {#cost-based-optimizer-partial-limitations}

* The pragma applies only when [`CostBasedOptimizer`](global.md#costbasedoptimizer) is enabled.
* Values greater than `1` enable an experimental mode: the threshold semantics and query planning behavior may change in future YQL versions. For long-lived queries, use `0` or `1`.
* Partial optimization preserves more parallelism, but may produce a less efficient `JOIN` order than full reordering after every input is ready.

## yt.CoreDumpPath {#core-dump-path}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path on the cluster | — | Static, [per-cluster](*per-cluster) |

Let you save the [coredump](https://en.wikipedia.org/wiki/Core_dump) from failed MapReduce operation jobs to a separate table.

## yt.DataSizePerJob / yt.DataSizePerMapJob {#data-size-per-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 2G | Dynamic |

Control how MapReduce operations are split into jobs; the larger the number, the fewer the jobs. For computationally expensive jobs, it’s recommended to decrease the value, and for jobs that quickly scan lots of data (in particular, user_sessions) — to increase it.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DataSizePerPartition {#data-size-per-partition}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 1G | Dynamic |

Control the size of partitions in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DataSizePerSortJob {#data-size-per-sort-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | - | Dynamic |

Control how sort jobs are split in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DefaultCalcMemoryLimit {#default-calc-memory-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 1G | Static |

This sets the memory limit for computations that aren’t related to table access.

You can use the K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

{% if audience == "internal" %}

## yt.DefaultCluster {#default-cluster}

This pragma sets the cluster where computations not related to data in tables run by default. For example, this applies to computations with custom UDFs: in such cases, the pragma will launch a Map operation.

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with the cluster name | `{{production-cluster}}` | Static |

{% note warning %}

Specify the cluster name in lowercase. For example: `'{{ testing-cluster }}'`, not `'{{ testing-cluster-name }}'`.

{% endnote %}

#### Example {#default-cluster-example}

```yql
PRAGMA yt.DefaultCluster = 'watt';
SELECT 1 + 2;
```

#### Result {#default-cluster-result}

Operations that don’t use tables will run on the specified cluster.

{% endif %}

## yt.DefaultLocalityTimeout {#default-locality-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| A time interval with support for the `s/m/h/d` suffixes | — | Dynamic |

This sets the `locality_timeout` setting in the operation spec (this setting isn’t documented yet).

## yt.DefaultMapSelectivityFactor {#default-map-selectivity-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| A positive floating-point number | — | Dynamic |

This sets the approximate ratio of output to input for the map stage in a combined MapReduce operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/mapreduce).

## yt.DefaultMaxJobFails {#default-max-job-fails}

| Possible values | Default value | Type |
| --- | --- | --- |
| A positive number | 5 | Static |

This is the number of failed MapReduce jobs that, once reached, stops further retry attempts for the request. The request is then considered unsuccessful.

## yt.DefaultMemoryDigestLowerBound {#default-memory-digest-lower-bound}

| Possible values | Default value | Type |
| --- | --- | --- |
| A floating-point number from 0.0 to 1.0, inclusive | — | Dynamic |

This sets the `user_job_memory_digest_lower_bound` setting in the operation spec. You can read about this setting in the [documentation]({{yt-docs-root}}/user-guide/data-processing/scheduler/memory-digest#nastrojki-digest).

## yt.DefaultMemoryLimit {#default-memory-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 512M | Dynamic |

This sets the memory limit, in bytes, that jobs consume. The limit is requested when you start MapReduce operations.

You can use the K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DefaultMemoryReserveFactor {#default-memory-reserve-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| A floating-point number from 0.0 to 1.0, inclusive | — | Dynamic |

This sets the memory reservation factor for jobs. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#memory_reserve_factor).

## yt.DefaultOperationWeight {#default-operation-weight}

| Possible values | Default value | Type |
| --- | --- | --- |
| A floating-point number | 1.0 | Dynamic |

This sets the weight of all MapReduce operations you launch within the selected compute pool.

{% if audience == "internal"%}

## yt.DefaultRuntimeCluster {#default-runtime-cluster}

This pragma sets the cluster that will be used by default for `yt.RuntimeCluster`.

The `yt.DefaultRuntimeCluster` value is used if `yt.RuntimeCluster` isn’t set in the request or its value isn’t available in the current context (for example, at the [stage](../../misc/exec_steps.md) evaluation).

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with the cluster name | watt | Static |

{% endif %}

## yt.Description {#description}

| Possible values | Default value | Type |
| --- | --- | --- |
| A string representation of a YSON map | — | Dynamic |

This sets the information that’s displayed in the web interface on the operation page. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

## yt.DisableJobSplitting {#disable-job-splitting}

| Possible values | Default value | Type |
| --- | --- | --- |
| A flag | false | Dynamic |

Use this to prevent the {{product-name}} Scheduler from adaptively splitting long-running user jobs further.

## yt.DisableOptimizers {#disable-optimizers}

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with a list of optimizers, separated by any of the following characters: a comma, a semicolon, a space, or `\|` | — | Static |

This disables the specified optimizers.

## yt.DockerImage {#docker-image}

| Possible values | Default value | Type |
| --- | --- | --- |
| The path to a Docker image | — | Dynamic |

You can specify a Docker image to create the environment where user jobs run.

## yt.DontForceTransformForInputTables {#dont-force-transform-for-input-tables}

| Possible values | Default value | Type |
| --- | --- | --- |
| A flag | false | Static |

This disables the forced data transformation for user tables with storage settings (`erasure_codec`, `compression_codec`, `primary_medium`, `media`, column groups) that differ from the default settings.

Forced transformation is applied to input tables if they’re used to write to output tables only via the YtMerge operation.

## yt.ErasureCodecCpu {#erasure-codec-cpu}

| Possible values | Default value | Type |
| --- | --- | --- |
| A floating-point number, at least 1.0 | 1.0 | Dynamic |

This is the multiplier for estimating CPU consumption when processing tables that are compressed with an erasure codec. It affects how MapReduce operations are split into jobs.

## yt.EvaluationTableSizeLimit {#evaluation-table-size-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, no more than 10M | 1M | Static |

This sets the maximum total size of tables used at the evaluation stage.

## yt.ExpirationDeadline / yt.ExpirationInterval {#expiration-deadline}

| Possible values | Default value | Type |
| --- | --- | --- |
| ExpirationDeadline: a point in time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format. ExpirationInterval: a time interval with support for the `s/m/h/d` suffixes, during which there must be no calls to the node. | — | Dynamic |

This lets you manage the [TTL of tables created by the operation]({{yt-docs-root}}/user-guide/storage/cypress#TTL).

## yt.ExtendedStatsMaxChunkCount {#yt.extendedstatsmaxchunkcount}

Limits collection of extended column statistics for the cost-based optimizer. Use this pragma to prevent statistics collection for tables with many chunks from delaying query optimization.

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | — | Static |

The value specifies the maximum total number of input-table chunks on one cluster:

* At or below the threshold, YQL requests extended statistics.
* Above the threshold, statistics are not requested and the optimizer uses less accurate estimates.
* `0` removes the chunk-count limit.
* If the pragma is not set, extended statistics are not requested.

#### Example {#extended-stats-max-chunk-count-example}

```yql
PRAGMA CostBasedOptimizer = 'native';
PRAGMA yt.ExtendedStatsMaxChunkCount = '10000';
```

#### Result {#extended-stats-max-chunk-count-result}

The optimizer receives extended column statistics when the input tables have no more than 10,000 chunks in total on a cluster. For larger input sets, optimization continues without these statistics.

#### Features and limitations {#extended-stats-max-chunk-count-limitations}

* The pragma affects only statistics collection and does not restrict reading the tables themselves.
* A value of `0` may make statistics collection slow for very large tables.
* The recommended initial value is `10000`; adjust it according to the number of input chunks and acceptable optimization time.

## yt.ExternalTx {#external-tx}

Use this to specify an external [transaction](../../../user-guide/storage/transactions.md) in the context of which the request will run. For example, you might use this if the transaction was created via the [CLI](../../../api/cli/commands.md#start-tx). This can be useful to perform several actions atomically, such as two YQL requests or a YQL request and a table change.

This pragma also lets you read intermediate data in transactions, for example, in a long operation that has temporary files.

{% note info %}

The directories for the request are created within the specified transaction. This can lead to conflicts if you try to write data to a directory that didn’t exist before using two requests with different ExternalTx values.

{% endnote %}

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with the transaction ID | — | Static, [per-cluster](*per-cluster) |

#### Example {#external-tx-example}

```yql
PRAGMA yt.ExternalTx = 'a3d149bd-674dfa1-fa68f314-2c22562';   -- Transaction ID (you can view it via the CLI)
```

#### Result {#external-tx-result}

The request will run within the specified transaction.

#### Features and limitations {#external-tx-limitations}

Specifying the pragma without a request body will cause an error.

## yt.ExtraTmpfsSize {#extra-tmpfs-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Dynamic |

This lets you increase the tmpfs size in addition to the total size of all explicitly used files (specify the value in megabytes). This can be useful if you create new files locally from a UDF. This is ignored without [UseTmpfs](#use-tmpfs).

## yt.FileCacheTtl {#file-cache-ttl}

| Possible values | Default value | Type |
| --- | --- | --- |
| A time interval with support for the `s/m/h/d` suffixes | 7d | Static |

This lets you manage the TTL of the {{product-name}} [file cache](../../faq/temp.md). A value of 0 disables the use of TTL for the file cache.

## yt.FolderInlineDataLimit {#folder-inline-data-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 100K | Static |

This sets the maximum data size for an inline list that’s the result of a Folder computation. If the size is larger, a temporary file is used.

## yt.FolderInlineItemsLimit {#folder-inline-items-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| A positive number | 100 | Static |

This sets the maximum number of items in an inline list that’s the result of a Folder computation. If the number is larger, a temporary file is used.

## yt.ForceJobSizeAdjuster {#force-job-size-adjuster}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Dynamic |

This sets the `"force_job_size_adjuster"` option in the operation settings.

{% if audience == "internal"%}

## yt.GeobaseDownloadUrl {#geobase-download-url}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

This sets the URL from which the geobase (the geodata6.bin file) will be downloaded if the query uses a Geo UDF.

{% endif %}

## yt.HybridDqExecution {#hybrid-dq-execution}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Static |

This enables hybrid query execution via DQ.

## yt.IgnoreTypeV3 {#ignore-type-v}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

When you read tables with a type_v3 schema, all fields with complex types appear as Yson fields in the query. Complex types include all non-data types and data types with more than one level of optionality.

## yt.IgnoreWeakSchema {#ignore-weak-schema}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

This tells the system to ignore the table’s weak schema (which is generated by sorting a non-schema table by a set of fields).

Used together with `yt.InferSchema`, this lets you infer the schema from the data for such tables.

## yt.IgnoreYamrDsv {#ignore-yamr-dsv}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Static |

This tells the system to ignore `_format=yamred_dsv` if it’s specified in the input table’s metadata.

## yt.InferSchema / yt.ForceInferSchema {#infer-schema}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 1000 | — | Static |

This infers the data schema from the content of the table’s first rows. If you specify the PRAGMA without a value, the system assumes one first row. If you specify multiple rows and the column has different data types, the types expand up to Yson.

InferSchema only infers the data schema for tables that don’t have a schema specified in the metadata. ForceInferSchema ignores the data schema from the metadata, except for the list of key columns for sorted tables.

In addition to the detected columns, the system generates an _other dictionary column (string per row) with values from columns that weren’t present in the first row but were found later. This lets you use [WeakField](../../builtins/basic.md#weakfield) on such tables.

Because of the wide range of possible issues, this mode isn’t recommended and is disabled by default.

## yt.InferSchemaTableCountThreshold {#infer-schema-table-count-threshold}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 50 | Static |

If the number of tables for which the schema is inferred from the content exceeds this value, schema inference runs as a separate operation on {{product-name}}, which can be much faster.

## yt.IntermediateAccount {#intermediate-account}

| Possible values | Default value | Type |
| --- | --- | --- |
| Account name in {{product-name}} | intermediate | Dynamic |

This lets you use your own account for intermediate data within a fused MapReduce operation.

By default, the system uses a shared account, which might fill up at an inconvenient time.

If you specify the [PRAGMA yt.TmpFolder](#tmp-folder), the system uses the account specified on the temporary directory instead of the shared one by default.

## yt.IntermediateDataMedium {#intermediate-data-medium}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

This sets the medium used for intermediate data in operations (Sort, MapReduce). For details, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/sort).

## yt.IntermediateReplicationFactor {#intermediate-replication-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 10 | — | Dynamic |

This sets the replication factor for intermediate data.

## yt.JobEnv {#job-env}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a Yson map | — | Dynamic |

This sets the environment variables for the map and reduce jobs in the operation. The keys in the map set the environment variable names, and the values in the map set the environment variable values.

## yt.JoinAllowColumnRenames {#join-allow-column-renames}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Static |

This enables column renaming when you use the Ordered JOIN strategy (it uses the [rename_columns]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#path_attributes) attribute). If you disable this option, the Ordered JOIN strategy is only selected when the column names on the left and right sides match.

## yt.JoinCollectColumnarStatistics {#join-collect-columnar-statistics}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: disable / sync / async | async | Static |

This controls the use of columnar statistics to accurately estimate JOIN inputs and choose the appropriate strategy. Async enables asynchronous collection of columnar statistics.

## yt.JoinColumnarStatisticsFetcherMode {#join-columnar-statistics-fetcher-mode}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: from_nodes / from_master / fallback | fallback | Static |

This controls the mode for requesting columnar statistics from {{product-name}} to accurately estimate JOIN inputs. The from_nodes mode gives an accurate estimate but might miss timeouts for large tables. The from_master mode is very fast but gives coarse statistics. The fallback mode works as a combination of the previous two.

## yt.JoinMergeForce {#join-merge-force}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | - | Static |

This forces the selection of the Ordered JOIN strategy. If you set the flag to True, the Ordered JOIN strategy is selected even if one or both JOIN sides aren’t sorted. In this case, the unsorted sides are sorted beforehand. The limits on the maximum size of an unsorted table (see `yt.JoinMergeUnsortedFactor`) are ignored in this case.

## yt.JoinMergeReduceJobMaxSize {#join-merge-reduce-job-max-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 8G | Static |

This sets the maximum allowed size of the Reduce job when a small table is selected as the primary one in the Ordered JOIN strategy. If the resulting size exceeds this value, the Reduce operation repeats with a larger table as the primary one.

## yt.JoinMergeTablesLimit {#join-merge-tables-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 64 | Static |

This sets the total allowed number of tables on the left and right sides to enable the Ordered JOIN strategy.

You can completely disable this strategy by setting the value to 0.

## yt.JoinMergeUnsortedFactor {#join-merge-unsorted-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive floating-point number | 0.2 | Static |

This sets the minimum ratio of the unsorted JOIN side’s size to the sorted side’s size for its additional sorting and the selection of the Ordered JOIN strategy.

## yt.JoinMergeUseSmallAsPrimary {#join-merge-use-small-as-primary}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | - | Static |

This explicitly controls the selection of the primary table in the Reduce operation for the Ordered JOIN strategy. If you set it to true, the smaller side is always selected as the primary table. If the flag’s value is false, the larger side is selected, except when the larger side has unique keys. Selecting the larger table as the primary one is safe even if it has monster keys, but it’s slower. If you don’t set this pragma, the primary table is selected automatically based on the maximum size of the resulting jobs (see yt.JoinMergeReduceJobMaxSize).

## yt.LayerPaths {#layer-paths}

| Possible values | Default value | Type |
| --- | --- | --- |
| String with a list of paths to porto layers, separated by any of these characters: comma, semicolon, space, or `\|` | — | Dynamic |

This lets you specify the sequence of porto layers to form the environment in which user jobs will run.{% if audience == "internal" %} For more details, see [Etushka]({{yql.pages.syntax.pragma.at-launch-jobs}}).{% endif %}

## yt.LLVMMemSize {#llvm-mem-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 256M | Dynamic |

This sets the fixed memory size required for compiling LLVM code in jobs.

## yt.LLVMPerNodeMemSize {#llvm-per-node-mem-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 10K | Dynamic |

This sets the memory size per computation graph node required for compiling LLVM code in jobs.

## yt.LookupJoinLimit {#lookup-join-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, no more than 10M | 1M | Static |

A table can be used as a map in the Lookup JOIN strategy if its size doesn’t exceed the minimum of `yt.LookupJoinLimit` and `yt.EvaluationTableSizeLimit`.

## yt.LookupJoinMaxRows {#lookup-join-max-rows}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number, no more than 1000 | 900 | Static |

This sets the maximum number of rows in a table that can act as a map in the Lookup JOIN strategy.

## yt.MapJoinLimit {#map-join-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 2048M | Static |

This limits the size of the smaller table in a JOIN, which triggers the Map-side strategy (creating an in-memory map from the smaller table and using it in the Map over the larger table).

You can completely disable this strategy by setting the value to 0.

## yt.MapJoinShardCount {#map-join-shard-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 10 | 4 | Static |

The Map-side JOIN strategy can run in a sharded mode: the smaller side splits into N shards (where N is less than or equal to this PRAGMA’s value), each of which independently and in parallel joins with the larger side. The JOIN result is then the concatenation of the JOINs with the shards.

## yt.MapJoinShardMinRows {#map-join-shard-min-rows}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1 | Static |

This sets the minimum number of records per shard in the Map-side JOIN strategy.

## yt.MapLocalityTimeout {#map-locality-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for the `s/m/h/d` suffixes | — | Dynamic |

This sets the `map_locality_timeout` setting in the operation specification (the setting is not yet documented).

## yt.MaxColumnGroups {#max-column-groups}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 64 | Static |

This sets the maximum number of column groups for the intermediate request table. If the calculated number of groups exceeds this limit, no groups are created for this table.

## yt.MaxExtraJobMemoryToFuseOperations {#max-extra-job-memory-to-fuse-operations}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 2G | Static |

This is the maximum amount of memory consumption by jobs allowed after operations are fused by optimizers.

## yt.MaxInputTables {#max-input-tables}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1000 | Static |

This limits the number of tables provided as input to each specific MapReduce operation.

## yt.MaxInputTablesForSortedMerge {#max-input-tables-for-sorted-merge}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 100 | Static |

This limits the number of tables provided as input to a sorted merge operation.

## yt.MaxJobCount {#max-job-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive integer | 16384 | Dynamic |

This is the maximum number of jobs within a single {{product-name}} operation. It applies only to single-stage map, reduce, merge, and similar operations. If you specify both [`yt.DataSizePerJob`](#data-size-per-sort-job) and `yt.MaxJobCount`, job slicing will take [`yt.DataSizePerJob`](#data-size-per-sort-job) into account. Even if the resulting value `N` exceeds `yt.MaxJobCount`, `N` jobs will be launched. `yt.MaxJobCount` will only affect whether jobs are split after their number reaches a certain threshold.

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
| Floating-point number not less than 1.0 | 20.0 | Static |

This is the maximum data replication factor allowed after operations are fused by optimizers.

## yt.MaxRowWeight {#max-row-weight}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, up to 128M | 16M | Dynamic |

Increase the limit on the maximum length of a table row in yt.

## yt.MaxSpeculativeJobCountPerTask {#max-speculative-job-count-per-task}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | — | Dynamic |

This sets the number of {% if audience == "internal" %}[speculative jobs]({{yql.pages.syntax.pragma.speculative-job}}){% else %}speculative jobs{% endif %} in {{product-name}} operations. By default, the {{product-name}} cluster settings are used.

## yt.MinColumnGroupSize {#min-column-group-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number not less than 2 | 2 | Static |

This sets the minimum size of a column group. If the calculated group contains fewer columns than the pragma value specifies, the group is not created.

## yt.MinLocalityInputDataWeight {#min-locality-input-data-weight}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Dynamic |

This sets the `min_locality_input_data_weight` setting in the operation specification (the setting is not yet documented).

## yt.MinPublishedAvgChunkSize {#min-published-avg-chunk-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Static |

If the average chunk size in the resulting output table is smaller than the specified setting, an additional {{product-name}} Merge operation starts to enlarge the chunks to the specified size. A value of 0 has a special meaning — in this case, the merge always starts and enlarges the chunks to 1G.

If a compression codec is used for the table, the output chunk size may differ from the specified value by the compression ratio. Essentially, this pragma sets the data size per merge job. After compression, the output size may be significantly smaller. In this case, you should increase the pragma value by the expected compression ratio.

## yt.MinTempAvgChunkSize {#min-temp-avg-chunk-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Static |

This setting is similar to `yt.MinPublishedAvgChunkSize`, but it works for intermediate temporary tables.

## yt.NetworkProject {#network-project}

{% if audience == "internal" %}

Specifies the network project for jobs of regular operations created after the pragma. Use this dynamic pragma when the network project must change only for part of the query without affecting the evaluation stage or the part of the plan created earlier.

{% endif %}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | `yt.StaticNetworkProject` | Dynamic |

{% if audience == "internal" %}

#### Example {#network-project-example}

```yql
PRAGMA yt.NetworkProject = 'my-network-project';

SELECT
    key AS key,
    value AS value
FROM `//path/to/table`;
```

#### Result {#network-project-result}

Jobs of regular operations in the part of the query after the pragma will run with the `my-network-project` network project.

#### Features and limitations {#network-project-limitations}

* The pragma is dynamic and affects only the following part of the query.
* Setting `yt.NetworkProject` partially disables DQ. DQ may still be used for the evaluation stage or for the part of the query before the pragma.
* If the pragma is not set, the `yt.StaticNetworkProject` value is used.

{% else %}

This sets the use of the specified network project in jobs for regular operations in the request.

{% endif %}

## yt.NightlyCompress {#nightly-compress}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | - | Dynamic |

This controls the process of {% if audience == "internal" %}[background table compression]({{yt-docs-root}}/user-guide/storage/regular-system-processes#nightly_compress){% else %}background table compression{% endif %} so that tables take up less space.

A `true` value sets the table attribute `@force_nightly_compress` to `true`.
A `false` value sets the table attribute `@nightly_compression_settings` with the child value `enabled` to `false`.

This setting applies only to tables newly created by a YQL request (and to tables overwritten using [INSERT INTO ... WITH TRUNCATE](insert_into)).
This setting does not apply to temporary tables.

## yt.OmitInaccessibleRows {#omit-inaccessible-rows}

This controls the behavior when reading tables with [row-level ACL]({{yt-docs-root}}/user-guide/storage/row-level-security) (RLS).

By default, reading a table with row-level ACL set results in an authorization error if the user doesn’t have the `full_read` permission. The `yt.OmitInaccessibleRows` pragma changes this behavior: when enabled, rows without access are skipped, and the query completes successfully. Only rows allowed by the RLS predicate are included in the result.

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

The query will return only the rows accessible to the current user according to the row-level ACL. Rows without access will be skipped without an error.

#### Restrictions {#omit-inaccessible-rows-restrictions}

- You can’t specify `row_index` in `ranges` when reading a table with row-level ACL — the query will result in an error. Row indexes in `ranges` are counted relative to the physical rows on disk, not the rows accessible to the user. For example, `//path/to/table[:#100]` will return up to 100 rows from the disk, some of which may be inaccessible and will be filtered out.
- RLS is not supported for dynamic tables — any read will return an error.

## yt.OperationReaders {#operation-readers}

| Possible values | Default value | Type |
| --- | --- | --- |
| String with a list of logins separated by any of the following characters: comma, semicolon, space, or ` | ` | Dynamic |

This lets you grant read access to created MapReduce operations in {{product-name}} to other users besides the YQL operation owner.

## yt.OperationSpec {#operation-spec}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON map | — | Dynamic |

This sets a map of operation settings. It lets you specify settings that don’t have pragma equivalents. Settings defined via specialized pragmas have higher priority and override values in this map.

## yt.OptimizeFor {#optimize-for}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: lookup / scan | scan | Dynamic |

This controls the `optimize_for` attribute on created tables.

## yt.Owners {#owners}

This lets you grant access to manage [MapReduce operations](../../../user-guide/data-processing/operations/mapreduce.md) in {{product-name}} (cancel, pause, run-job-shell, etc.) to other users besides the user who launched the request.

| Possible values | Default value | Type |
| --- | --- | --- |
| String with a list of logins separated by any of the following characters: `,`, `;`, ` ` or `\|` | — | Dynamic |

{% if audience == "internal" %}
For example, if YQL operations are launched under a [robot](https://wiki.yandex-team.ru/tools/support/zombik/), you should add the employees responsible for it to the list.
{% endif %}

#### Example {#owners-example}

```yql
PRAGMA yt.Owners = 'ivanov petrov';    -- user logins separated by a space
```

#### Result {#owners-result}

The specified users will be able to manage MapReduce operations.

## yt.ParallelOperationsLimit {#parallel-operations-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number not less than 1 | 16 | Static |

This sets the maximum number of {{product-name}} operations that can run simultaneously within a request.

## yt.Pool {#pool}

This is used to override the compute pool, which by default is the current user’s login or was set by the `yt.StaticPool` pragma.

| Value type | Default value | Type |
| --- | --- | --- |
| String | Value of the `yt.StaticPool` pragma — if it was set earlier; the current user’s login — if `yt.StaticPool` was not set | Dynamic |

#### Signature {#pool-signature}

```yql
PRAGMA yt.StaticPool = '<pool_1>';
PRAGMA yt.Pool = '<pool_2>';
```

#### Result {#pool-result}

After specifying the `yt.StaticPool` pragma, the request will run in pool `<pool_1>`, and after the `yt.Pool` pragma — in `<pool_2>`.

## yt.PoolTrees {#pool-trees}

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with a list of pool tree names, separated by any of the following characters: comma, semicolon, space, or ` | ` | Dynamic |

You can choose pool trees that differ from the default one.{% if audience == "internal" %} For more details, see the [documentation]({{yql.pages.syntax.pragma.cloud-nodes}}).{% endif %}

## yt.PrimaryMedium {#primary-medium}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

This sets the [primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary) for Published and Temporary tables, as well as for intermediate data in operations. It’s the same as setting the `yt.IntermediateDataMedium`, `yt.PublishedPrimaryMedium`, and `yt.TemporaryPrimaryMedium` pragmas at the same time.

## yt.PublishedCompressionCodec / yt.TemporaryCompressionCodec {#published-compression-codec}

| Possible values | Default value | Type |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/compression) | zstd_5 | Dynamic |

These settings configure the compression for tables created via YQL.

Published tables are those specified in [INSERT INTO](../insert_into.md); all others are considered Temporary. The codec specified as Temporary is also used for intermediate data within a single {{product-name}} operation, for example, a fused MapReduce.

## yt.PublishedErasureCodec / yt.TemporaryErasureCodec {#published-erasure-codec}

| Possible values | Default value | Type |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/replication#erasure) | none | Dynamic |

Erasure coding is disabled by default. To enable it, we recommend using the lrc_12_2_2 value.

The difference between Published and Temporary is the same as for [CompressionCodec](#published-compression-codec).

## yt.PublishedMedia / yt.TemporaryMedia {#published-media}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON map | — | Dynamic |

This sets the `@media` attribute on newly created tables. If present, it specifies [which media in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#naznachenie-mediuma) will store the table’s chunks.

Published tables are those specified in [INSERT INTO](../insert_into.md); all others are considered Temporary.

## yt.PublishedPrimaryMedium / yt.TemporaryPrimaryMedium {#published-primary-medium}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

This sets the `@primary_medium` attribute on newly created tables. If present, it specifies the [primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary) where the chunks will be written. By default, {{product-name}} sets the primary medium to `"default"`.

Published tables are those specified in [INSERT INTO](../insert_into.md); all others are considered Temporary.

## yt.PublishedReplicationFactor / yt.TemporaryReplicationFactor {#published-replication-factor}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number from 1 to 10 | — | Dynamic |

This sets the replication factor for tables created via YQL.

Published tables are those specified in [INSERT INTO](../insert_into.md); all others are considered Temporary.

## yt.PythonCpu{%if audience == "internal" %} / yt.JavascriptCpu{% endif %} {#python-cpu}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number, at least 1.0 | 4.0 | Dynamic |

This is a multiplier for estimating CPU consumption for [Python UDF](../../udf/python.md){%if audience == "internal" %} and [JavaScript UDF](../../udf/javascript.md), respectively{% endif%}. It affects how MapReduce operations are split into jobs.

## yt.QueryCacheChunkLimit {#query-cache-chunk-limit}

Use this pragma to control how tables are written to the cache, depending on the number of chunks in the table: with the `concatenate` command (as-is) or with the `merge` command (with merging).

| Possible values | Default value | Type |
| --- | --- | --- |
| String containing the number of chunks — `<Uint64>` | `'0'` | Dynamic |

#### Example { #query-cache-chunk-limit-example }

```yql
PRAGMA yt.QueryCacheChunkLimit = '100000';
```

#### Result { #query-cache-chunk-limit-result }

If the number of chunks in the table is less than the set limit, the table is written to the cache with the `concatenate` command, as-is.

If the number of chunks exceeds the set limit, the table is written to the cache with the `merge` command (with chunk merging).

So, with the default value of `0`, tables are written to the cache using the `merge` command.

#### Features and limitations { #query-cache-chunk-limit-constraint }

The `concatenate` command — writing as-is — is faster than `merge`, which needs time to merge the chunks and then write them. It’s more efficient to write tables with a relatively small number of chunks to the cache using `concatenate`.

## yt.QueryCacheIgnoreTableRevision {#query-cache-ignore-table-revision}

Use this pragma to avoid clearing the cache when table data changes. This speeds up debugging complex queries on large, changing tables when the query logic doesn’t depend on data changes.

By default, when calculating the hash from table metadata, the revision number — the {{product-name}} revision, a non-negative integer that increases with each table change — is included along with other data. For more details about the `revision` attribute, see the [Metadata Tree]({{yt-docs-root}}/user-guide/storage/cypress#time_attributes) section.

With this pragma, you can control cache clearing: include or exclude the revision number when calculating the hash from table metadata.

| Possible values | Default value | Type |
| --- | --- | --- |
| String containing `true` or `false` | `false` | Static |

#### Example { #query-cache-ignore-table-revision-example }

```yql
PRAGMA yt.QueryCacheIgnoreTableRevision = 'true';
```

#### Result { #query-cache-ignore-table-revision-result }

If the flag is set to `true`, the {{product-name}} revision number is excluded when calculating the hash from table metadata, and the Query Cache isn’t cleared when the content of input tables changes.

#### Features and limitations { #query-cache-ignore-table-revision-constraint }

{% note warning %}

Don’t use this pragma in production. Use it only for debugging complex queries to reduce their execution time.

{% endnote %}

## yt.QueryCacheMode {#query-cache-mode}

Use the yt.QueryCacheMode pragma to control caching of MapReduce operation results in {{product-name}}.

The cache stores results of previous operations: if the same operation was already run in a previous query, it won’t be started again. Instead, {{product-name}} will take the ready result from the cache. This speeds up queries that include calculations identical to those in other queries.

Query Cache is also useful when you’re debugging or making relatively small changes to a query: in this case, the result of a large part of the operations is taken from the cache.

| Possible values | Default value | Type |
| --- | --- | --- |
| `'disable'` / `'readonly'` / `'refresh'` / `'normal'` | `'normal'` | Static |

#### Example { #query-cache-mode-example }

```yql
PRAGMA yt.QueryCacheMode = 'disable';
```

#### Result { #query-cache-mode-result }

Depending on the selected mode, {{product-name}} will use, ignore, or update the cache:

- `disable` — the cache is disabled. {{product-name}} doesn’t check the cache and doesn’t write results to it. This mode is suitable for production where data changes often: it saves resources and avoids unnecessary cache read and write operations;
- `readonly` — read-only mode. {{product-name}} takes the result from the cache if it exists, but doesn’t save new results to it. This is suitable for testing or debugging: you can use old results and avoid cluttering the cache with new entries;
- `refresh` — write-only mode. {{product-name}} saves results to the cache but doesn’t read from it. This is used to update the cache: run a query without reading from the cache and save its result for future use;
- `normal` — the default mode. {{product-name}} both reads from and writes to the cache. This is especially useful during development and debugging when you run the same query multiple times to speed up iterations.

In `normal` and `refresh` modes, the operation result is also saved to the path `//<tmp_folder>/query_cache/<hash>`, where:

- `tmp_folder` — the temporary directory. By default, it’s `tmp/<login>`; you can set it using the [yt.TmpFolder](#tmp-folder) pragma;
- `hash` — the hash of the significant metadata and data of the input tables and the logical program that was run in the operation.

{% note info %}

In `normal` mode, when you start a MapReduce operation, the system looks for the cache at the path `//<tmp_folder>/query_cache/<hash>`. If the cache doesn’t exist, {{product-name}} checks whether another operation that calculates the same cache is running in parallel. If such an operation exists but hasn’t written the result yet, the first operation waits for it to finish and then takes the ready cache.

Keep in mind that the second query’s operation, which calculates the cache, might be allocated fewer resources than the first one, which is waiting. So, the first query will run slower than if it were executed without reading from the cache.

{% endnote %}

## yt.QueryCacheTtl {#query-cache-ttl}

Use this pragma to set the time that operation-created tables are stored in the query cache — [TTL (Time to Live)]({{yt-docs-root}}/user-guide/storage/cypress) — in the directory `<tmp_folder>/query_cache/<hash>`, where `tmp_folder` is the temporary directory. By default, it’s `tmp/<login>`; you can set it using the [yt.TmpFolder](#tmp-folder) pragma.

| Possible values | Default value | Type |
| --- | --- | --- |
| String containing a time interval in the specified format: a number and the suffix `s/m/h/d` (seconds, minutes, hours, days) | `'7d'` | Static |

#### Example { #query-cache-ttl-example }

```yql
PRAGMA yt.QueryCacheTtl = '3h';
```

#### Result { #query-cache-ttl-result }

If you explicitly set a value in the pragma, the Query Cache will be cleared after the specified interval. The interval is counted from the moment the table is created in the query cache or from the moment the table was last used (see the [yt.QueryCacheUseExpirationTimeout](#query-cache-use-expiration-timeout) pragma).

If you don’t set an interval, the Query Cache will be cleared automatically after 7 days (by default).

## yt.QueryCacheUseExpirationTimeout {#query-cache-use-expiration-timeout}

This pragma defines the mode for counting the TTL interval for tables in the query cache.

| Possible values | Default value | Type |
| --- | --- | --- |
| String containing `true` or `false` | `false` | Static |

#### Example { #query-cache-use-expiration-timeout-example }

```yql
PRAGMA yt.QueryCacheUseExpirationTimeout = 'true';
```

#### Result { #query-cache-use-expiration-timeout-result }

With the default value of `false`, the TTL is counted from the moment the table is created in the query cache.

If you set the value to `true`, the TTL is counted from the moment the table was last used.

#### Features and limitations { #query-cache-use-expiration-timeout-constraint }

{% note warning %}

Use this pragma only together with the [yt.QueryCacheTtl](#query-cache-ttl) pragma: it has no effect without specifying the TTL interval.

{% endnote %}

## yt.ReduceLocalityTimeout {#reduce-locality-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for the `s/m/h/d` suffixes | — | Dynamic |

This sets the `reduce_locality_timeout` setting in the operation specification (the setting isn’t documented yet).

## yt.ReleaseTempData {#release-temp-data}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: immediate / finish / never | immediate | Static |

Use this pragma to control when temporary objects (for example, tables) that are created during query execution are deleted:

- `immediate` — delete objects as soon as they are no longer needed.
- `finish` — delete at the end of the entire YQL query execution.
- `never` — never delete.

{% if audience == "internal"%}

## yt.RuntimeCluster {#runtime-cluster}

This pragma sets the current cluster value for running MapReduce operations. It’s useful for selecting a cluster when the query includes tables located on different clusters.

The cluster selection mode is set by the [`yt.RuntimeClusterSelection`](#runtime-cluster-selection) pragma.

| Possible values | Default value | Type |
| --- | --- | --- |
| String with the cluster name | — | Dynamic |

#### Signature {#runtime-cluster-signature}

```yql
PRAGMA yt.RuntimeCluster = '<cluster_name>';
```

{% note warning %}

The cluster name must be specified in lowercase. For example: `'{{ testing-cluster }}'`, not `'{{ testing-cluster-name }}'`.

{% endnote %}

#### Example {#runtime-cluster-example}

```yql
PRAGMA yt.RuntimeClusterSelection = 'force';
PRAGMA yt.RuntimeCluster = 'watt';
```

#### Result {#runtime-cluster-result}

MapReduce operations will run strictly on the Watt cluster, because the `force` mode is set for the `yt.RuntimeClusterSelection` pragma above.

#### Features and limitations {#runtime-cluster-limitations}

The `yt.RuntimeCluster` pragma is dynamic — you can use different clusters within a single query, for example:

```yql
PRAGMA yt.RuntimeClusterSelection = 'force';
PRAGMA yt.RuntimeCluster = "cluster1";

INSERT into cluster2.@tmp -- temporary table on cluster2
SELECT ... ;  -- SELECT will run on cluster1

pragma yt.RuntimeCluster = "cluster2";

INSERT into cluster1.@tmp -- temporary table on cluster1
SELECT ... ;  -- SELECT will run on cluster2

commit;

PRAGMA yt.RuntimeCluster = "cluster3";
SELECT ... from cluster1.@tmp as t1 JOIN cluster2.@tmp as t2; -- SELECT will run on cluster3
```

## yt.RuntimeClusterSelection {#runtime-cluster-selection}

| Possible values | Default | Static / Dynamic |
| --- | --- | --- |
| String: disable / auto / force | disable | Static |

This pragma controls the mode for selecting the cluster where MapReduce operations run. In `disable` mode, MapReduce operations run only if all input tables for the operation (as well as tables used inside the operation’s lambdas) are on the same cluster.

In `auto` mode, the operation runs either on the cluster with the input tables (if all input tables are on the same cluster) or on the cluster specified by the current value of the dynamic pragma.

In `force` mode, the operation always runs on the cluster specified by the current value of the dynamic `yt.RuntimeCluster` pragma.

Note that the output tables for the operation are always located on the cluster where the operation runs.

In `auto`/`force` mode, tables attached explicitly or implicitly (for example, when choosing the MapJoin strategy) to the operation can be on different clusters and do not affect the selection of the cluster where the operation will run. The cluster is determined only by the location of the input tables and the `yt.RuntimeClusterSelection` and `yt.RuntimeCluster` settings.

{% endif %}

## yt.SamplingIoBlockSize {#sampling-io-block-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | — | Dynamic |

This pragma sets the minimum block size for large-block sampling.

{% if audience == "internal" %}

## yt.SchedulingTag / yt.SchedulingTagFilter {#scheduling-tag}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Dynamic |

You can enable “YT in the cloud” by specifying `external` as the value, or set any other valid value for this `yt` setting. For more details, see the [YT documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

{% endif %}

## yt.ScriptCpu {#script-cpu}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number, at least 1.0 | 1.0 | Dynamic |

This is a multiplier for estimating CPU consumption by script UDFs (including [Python UDF](../../udf/python.md){%if audience == "internal" %} and [JavaScript UDF](../../udf/javascript.md)){% endif %}. It affects how MapReduce operations are split into jobs. You can override it with specialized pragmas `yt.PythonCpu` / `yt.JavascriptCpu` for a specific UDF type.

## yt.SortLocalityTimeout {#sort-locality-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for `s/m/h/d` suffixes | — | Dynamic |

This pragma sets the `sort_locality_timeout` setting in the operation specification (this setting is not yet documented).

## yt.StartedBy {#started-by}

| Possible values | Default value | Type |
| --- | --- | --- |
| String representation of a YSON map | — | Dynamic |

This pragma sets a map that describes the client through which the operation was started. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

{% if audience == "internal" %}

## yt.StaticNetworkProject {#static-network-project}

Specifies the network project for jobs of all MapReduce operations in the query, including the evaluation stage. Use this static pragma when the entire query must run in one network project.

| Possible values | Default value | Type |
| --- | --- | --- |
| String | - | Static, [per-cluster](*per-cluster) |

#### Example {#static-network-project-example}

```yql
PRAGMA yt.StaticNetworkProject = 'my-network-project';

SELECT
    key AS key,
    value AS value
FROM `//path/to/table`;
```

#### Result {#static-network-project-result}

Jobs of all MapReduce operations in the query, including the evaluation stage, will run with the `my-network-project` network project.

#### Features and limitations {#static-network-project-limitations}

* The pragma is static and configured separately for each cluster.
* Setting `yt.StaticNetworkProject` completely disables DQ for the query.
* For details about network projects, see the [{{product-name}} documentation]({{yt-docs-root}}/user-guide/data-processing/operations/mtn).

{% endif %}

## yt.StaticPool {#static-pool}

Use this pragma to override the compute pool, which by default is the current user’s login.

You can set only one new value for `yt.StaticPool`. If you specify the static pragma multiple times, its last value will be used. If you need to override the value for the next query, set it using the dynamic `yt.Pool` pragma.

| Value type | Default value | Type |
| --- | --- | --- |
| String | Current user’s login | Static, [per-cluster](*per-cluster) |

#### Signature {#static-pool-signature}

```yql
PRAGMA yt.StaticPool = '<pool_1>';
```

#### Result {#static-pool-result}

After you specify the pragma, the query will run in the `<pool_1>` pool.

#### Features and limitations {#static-pool-limitations}

- Specifying a non-existent pool will cause a query execution error.

- If you specify `yt.StaticPool` multiple times, the last pragma value is used for all queries. For example, both queries written after the pragma with the `pool_1` value and queries after the pragma with the `pool_2` value will run in `pool_2`:

    ```yql
    PRAGMA yt.StaticPool = '<pool_1>';
    PRAGMA yt.StaticPool = '<pool_2>';
    ```

## yt.SuspendIfAccountLimitExceeded {#suspend-if-account-limit-exceeded}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Dynamic |

Suspend the operation if the “Account limit exceeded” error occurs in the jobs, [see the documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

## yt.SwitchLimit {#switch-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, at least 1M | 128M | Static |

This pragma sets the memory buffer size for running the Switch node.

## yt.TableContentCompressLevel {#table-content-compress-level}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number, up to 11 inclusive | 8 | Dynamic |

This pragma sets the compression level for table content delivered via a file (when `yt.TableContentDeliveryMode="file"`).

## yt.TableContentDeliveryMode {#table-content-delivery-mode}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: native / file | native | Dynamic |

If you set the value to `native`, the table content is delivered to jobs using the native {{product-name}} mechanisms. If you set the value to `file`, the table content is first downloaded on the YQL server and then delivered to jobs as a regular file.

## yt.TableContentMaxChunksForNativeDelivery {#table-content-max-chunks-for-native-delivery}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number, up to 1000 inclusive | 1000 | Static |

This pragma sets the maximum number of chunks in a table for delivering it to jobs using the native {{product-name}} mechanisms. If this number is exceeded, the table is delivered via a file.

## yt.TableContentMaxInputTables {#table-content-max-input-tables}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number, up to 1000 inclusive | 1000 | Static |

This pragma sets the maximum number of tables for delivering them to jobs using the native {{product-name}} mechanisms. If this number is exceeded, a preliminary merge is inserted.

## yt.TableContentMinAvgChunkSize {#table-content-min-avg-chunk-size}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes | 1GB | Static |

This pragma sets the minimum average chunk size in a table for delivering it to jobs using the native {{product-name}} mechanisms. A preliminary merge is inserted for chunks that are not large enough.

## yt.TableContentTmpFolder {#table-content-tmp-folder}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path on the cluster | — | Dynamic |

This pragma specifies the directory where temporary files for tables delivered via a file (when `yt.TableContentDeliveryMode="file"`) will be stored. If you don’t specify it, the default {{product-name}} file cache is used.

## yt.TableContentUseSkiff {#table-content-use-skiff}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Dynamic |

This pragma enables the Skiff format for delivering a table to operation jobs.

## yt.TablesTmpFolder {#tables-tmp-folder}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | `//tmp/yql/<login>` | Static, [per-cluster](*per-cluster)|

This pragma specifies the directory for storing [temporary tables](../../faq/temp.md). It takes precedence over `yt.TmpFolder`.

## yt.TempTablesTtl {#temp-tables-ttl}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval with support for suffixes `s/m/h/d` | — | Static |

Use this pragma to manage the TTL of [temporary tables](../../faq/temp.md). It affects tables with the full result; all other temporary tables are unconditionally deleted when the query finishes, regardless of this pragma.

## yt.TentativePoolTrees {#tentative-pool-trees}

| Possible values | Default value | Type |
| --- | --- | --- |
| A string with a list of tree names, separated by any of the following characters: comma, semicolon, space, or `\|` | — | Dynamic |

This pragma lets you cautiously extend operations to pool trees that differ from the standard ones.{% if audience == "internal" %} For more details, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.TentativeTreeEligibilityMaxJobDurationRatio {#tentative-tree-eligibility-max-job-duration-ratio}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number | — | Dynamic |

This pragma takes effect only if you set the `yt.TentativePoolTrees` pragma. It sets the allowed slowdown ratio for jobs in an alternative pool tree. {% if audience == "internal" %} For more details, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.TentativeTreeEligibilityMinJobDuration {#tentative-tree-eligibility-min-job-duration}

| Possible values | Default value | Type |
| --- | --- | --- |
| Milliseconds | — | Dynamic |

This pragma takes effect only if you set the `yt.TentativePoolTrees` pragma. It sets the minimum average job duration in an alternative pool tree. {% if audience == "internal" %} For more details, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.TentativeTreeEligibilitySampleJobCount {#tentative-tree-eligibility-sample-job-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | — | Dynamic |

This pragma takes effect only if you set the `yt.TentativePoolTrees` pragma. It sets the number of jobs in the sample.{% if audience == "internal" %} For more details, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.TmpFolder {#tmp-folder}

Use this pragma to specify the directory for storing temporary tables and files. For more details, see the [Temporary data](../../faq/temp.md) section.

| Possible values | Default value | Type |
| --- | --- | --- |
| Directory string | Current user’s directory — `//tmp/yql/<login>` | Static, [per-cluster](#settings) |

#### Example {#tmp-folder-example}

```yql
PRAGMA yt.TmpFolder = '//tmp/yql/ivanov/folder';
```

#### Result {#tmp-folder-result}

Temporary tables and files will be saved to the specified directory.

## yt.TopSortMaxLimit {#top-sort-max-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1000 | Static |

This is the maximum LIMIT value used in combination with ORDER BY that triggers the TopSort optimization.

## yt.TopSortRowMultiplierPerJob {#top-sort-row-multiplier-per-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Number, at least 1 | 10 | Static |

This pragma sets the expected number of rows per job in a TopSort operation, calculated as `LIMIT * yt.TopSortRowMultiplierPerJob`.

## yt.TopSortSizePerJob {#top-sort-size-per-job}

| Possible values | Default value | Type |
| --- | --- | --- |
| Bytes, at least 1 | 128M | Static |

This pragma sets the expected data size per job in a TopSort operation.

## yt.UseColumnarStatistics {#use-columnar-statistics}

| Possible values | Default value | Type |
| --- | --- | --- |
| String: disable / auto / force / 0 (=disable) / 1 (=force) | force | Dynamic |

Enable the use of columnar statistics to accurately estimate job sizes when running operations on tables with columnar selections. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

In auto mode, the system automatically disables statistics usage for operations that include tables with `optimize_for=lookup`.

## yt.UseDefaultTentativePoolTrees {#use-default-tentative-pool-trees}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | — | Dynamic |

This pragma sets the `use_default_tentative_pool_trees` option in the operation specification.

## yt.UseNativeYtTypes {#use-native-yt-types}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true{% if audience == "internal" %}*{% endif %} | Static |

Enable writing values of complex types to tables using the native support for complex types in {{product-name}}.

{% if audience == "internal" %}
{% note info %}

In Query Tracker, the `yt.UseNativeYtTypes` pragma is enabled by default. On the [{{yql.link}}]({{yql.link}}) service, you must enable it explicitly by running the `PRAGMA yt.UseNativeYtTypes` command.

{% endnote %}
{% endif %}

## yt.UseQLFilter {#yt.useqlfilter}

Passes the compatible part of a `WHERE` condition to {{product-name}} through [`input_query`]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options). Using `min`/`max` statistics, {{product-name}} can skip chunks and blocks where the condition is known to be false. The pragma is useful for selective reads from large static tables with a strict schema.

| Possible values | Default value | Type |
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

Compatible comparisons from `WHERE` are passed in `input_query`. If table statistics prove that a chunk or block has no `key` values in the specified range, {{product-name}} skips it without reading from disk. The remaining part of the condition is still evaluated by YQL.

#### Features and limitations {#use-ql-filter-limitations}

* Supported types are numeric types, `Bool`, `String`, `Utf8`, and their `Optional` variants.
* Supported expressions are `<`, `<=`, `>`, `>=`, `==`, and `!=` comparisons between a column and a constant expression, and `AND`, `OR`, `NOT`, `EXISTS`, and `COALESCE`.
* The table must have a strict schema.
* Dynamic tables are not supported.
* The pragma does not apply to tables with a custom schema or columns specified through `WITH SCHEMA` or `WITH COLUMNS`.

## yt.UserSlots {#user-slots}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | Unlimited | Dynamic |

This pragma sets an upper limit on the number of jobs that can run in parallel within a MapReduce operation.

## yt.UseSkiff {#use-skiff}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | true | Dynamic |

Enable the Skiff format for input and output in operation jobs.

## yt.UseTmpfs {#use-tmpfs}

| Possible values | Default value | Type |
| --- | --- | --- |
| Flag | false | Dynamic |

Mount [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) to the `_yql_tmpfs` folder in the MapReduce job sandbox. This pragma isn’t recommended for use.

[*per-cluster]: You can configure all dynamic and some static pragmas so that they apply only to a specific cluster. For more details, see the [per-cluster support](#settings) section.

<style>
    .dc-doc-page__aside {
        width: 235px !important;
    }
</style>
