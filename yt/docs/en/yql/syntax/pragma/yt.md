# YT pragmas {#yt}

YT pragmas may be defined as static or dynamic based on their lifetimes. Static pragmas are initialized one time at the earliest query processing step. If a static pragma is specified multiple times in a query, it accepts the latest value set for it. Dynamic pragma values are initialized at the query execution step after its optimization and execution plan preparation. The specified value is valid until the next identical pragma is found or until the query is completed. For dynamic pragmas only, you can reset their values to the default by assigning a `default`.

All pragmas that affect query optimizers are static because dynamic pragma values are not yet calculated at this step.

## yt.InferSchema / yt.ForceInferSchema {#inferschema}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 1,000 | — | Static |

Outputting the data schema based on the contents of the table's first rows. If PRAGMA is specified without a value, the contents of only the first row is used. If multiple rows are specified and column data types differ, they are extended to Yson.

InferSchema includes outputting data schemas for those tables only where it's not specified in metadata at all. When using ForceInferSchema, the data schema from metadata is ignored except for the list of key columns for sorted tables.

In addition to the detected column, dictionary column _other (row-on-row) is generated, which contains values for those columns that weren't in the first row but were found somewhere else. This lets you use [WeakField](../../builtins/basic.md#weakfield) for such tables.

Due to a wide range of issues that may arise, this mode isn't recommended for use and is disabled by default.

## yt.InferSchemaTableCountThreshold

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 50 | Static |

If the number of tables for which the schema is outputted based on their contents exceeds the specified value, then schema outputting is initiated as a separate operation on {{product-name}}, which may happen much faster.

## yt.IgnoreWeakSchema

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

Ignore the table's weak schema (produced by sorting a non-schematized table based on a set of fields).

Together with `yt.InferSchema`, you can output data-based schemas for such tables.

## yt.IgnoreYamrDsv

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

Ignore `_format=yamred_dsv` if it is specified in the input table's metadata.

## yt.IgnoreTypeV3 {#ignoretypev3}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

 When reading tables with type_v3 schema, all fields containing complex types will be displayed as Yson fields in the query. Complex types include all non-data types and data types with more than one level of optionality.

## yt.StaticPool

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | Current user login | Static |

Selecting a computing pool in the scheduler for operations performed at the optimization step.

## yt.Pool

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | `yt.StaticPool` if set, or the current user login | Dynamic |

Selecting a computing pool in the scheduler for regular query operations.

## yt.Owners

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A string containing the list of logins separated by any of these symbols: comma, semicolon, space or `|` | — | Dynamic |

Lets you grant management permissions for operations created by MapReduce in {{product-name}} (cancel, pause, run-job-shell, etc.) to any users other than the YQL operation owner.

{% if audience == "internal" %}
For instance, if YQL operations are initiated from a [robot user account]({{yql.pages.syntax.pragma.zombik}}), then you should add employees responsible for it to this list.
{% endif %}

## yt.OperationReaders

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A string containing the list of logins separated by any of these symbols: comma, semicolon, space or `|` | — | Dynamic |

Lets you grant read permissions for operations created by MapReduce in {{product-name}} to any users other than the YQL operation owner.

## yt.Auth

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Static |

Use authentication data other than the default data.

## yt.DefaultMaxJobFails

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 5 | Static |

The number of failed MapReduce jobs, upon reaching which query execution retries are stopped and the query is considered failed.

## yt.DefaultMemoryLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 512M | Dynamic |

Limitation of memory utilization (bytes) by jobs, which is ordered when launching MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DataSizePerJob / yt.DataSizePerMapJob {#datasizeperjob}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 2G | Dynamic |

Controls the splitting of MapReduce operations into jobs: the larger the number, the fewer jobs. Use a lower value for compute-intensive jobs. Use a higher value for jobs that scan through a large amount of data (namely, user_sessions).

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DataSizePerSortJob

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | - | Dynamic |

Controls the splitting of sort jobs in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DataSizePerPartition

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 1G | Dynamic |

Controls the size of partitions in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.DockerImage

| Value type | Default | Static /<br/>dynamic |
| --- | --- | --- |
| Path to the Docker image | — | Dynamic |

The ability to specify a Docker image to create an environment in which user jobs will be executed.

## yt.MaxJobCount

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive integer | 16384 | Dynamic |

Maximum number of jobs within a single {{product-name}} operation. It is used only for single-stage map, reduce, merge, and other operations. If [`yt.DataSizePerJob`](#datasizeperjob) and `yt.MaxJobCount` are both specified, job splitting is done with account for [`yt.DataSizePerJob`](#datasizeperjob), and even if the resulting value `N` exceeds `yt.MaxJobCount`, `N` jobs will be run, and `yt.MaxJobCount` will only affect whether the jobs will be split after their number reaches a particular threshold.

## yt.UserSlots

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | No limits | Dynamic |

Upper limit on the number of concurrent jobs within a MapReduce operation.

## yt.DefaultOperationWeight

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating-point number | 1.0 | Dynamic |

Weight of all launched MapReduce operations in a selected computing pool.

## yt.TmpFolder

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | `//tmp/yql/<login>` | Static |

Directory for storing temporary tables and files.

## yt.TablesTmpFolder

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | `//tmp/yql/<login>` | Static |

Directory for storing temporary tables. Takes priority over `yt.TmpFolder`.

## yt.TempTablesTtl

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Static |

Allows management of TTL for temporary tables. Effective for tables containing a full result, while the other temporary tables are unconditionally removed upon completion of the query regardless of this pragma.

## yt.FileCacheTtl

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | 7d | Static |

Allows management of TTL for {{product-name}} file cache. Value of 0 disables use of TTL for file cache.

## yt.IntermediateAccount

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Account name in {{product-name}} | intermediate | Dynamic |

Allows use of your account for intermediate data in a unified MapReduce operation.

The common account, which can overflow at an unfortunate time, is the default.

If [PRAGMA yt.TmpFolder](#yt.tmpfolder) is set, then instead of the common account you can use the one specified in the temporary directory.

## yt.IntermediateReplicationFactor

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 10 | — | Dynamic |

Intermediate data replication factor.

## yt.PublishedReplicationFactor / yt.TemporaryReplicationFactor {#replicationfactor}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 10 | — | Dynamic |

Replication factor for tables created through YQL.

Tables specified in [INSERT INTO](../insert_into.md) are Published. All other tables are Temporary.

## yt.ExternalTx

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Static |

Running an operation in a transaction that has already been launched outside YQL. Its identifier is passed to the value.

All directories required for running the query are created in a specified transaction. This may cause conflicts when attempting to write data from two queries with different ExternalTx into a previously non-existent directory.

## yt.OptimizeFor

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: lookup/scan | scan | Dynamic |

Controls the `optimize_for` attribute for the tables being created.

## yt.PublishedCompressionCodec / yt.TemporaryCompressionCodec {#compressioncodec}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/compression) | zstd_5 | Dynamic |

Compression settings for tables created through YQL.

Tables specified in [INSERT INTO](../insert_into.md) are Published. All other tables are Temporary. Also, a codec specified as Temporary is used for intermediate data in a single {{product-name}} operation (e.g. unified MapReduce).

## yt.PublishedErasureCodec / yt.TemporaryErasureCodec

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/replication#erasure) | none | Dynamic |

Erasure coding is always disabled by default. To enable it, you should use a value of lrc_12_2_2.

The difference between Published and Temporary is similar to [CompressionCodec](#compressioncodec).

## yt.NightlyCompress

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | - | Dynamic |

Manages the background table compression process so that the tables take up less space.

The `true` value sets the `@force_nightly_compress` attribute equal to `true` on the table.
The `false` value sets the `@nightly_compression_settings` attribute with the `enabled` child value equal to `false` on the table.

The setting applies only to tables newly created by the YQL query (as well as to tables overwritten using [INSERT INTO ... WITH TRUNCATE](insert_into)).
The setting doesn't apply to temporary tables.

## yt.ExpirationDeadline / yt.ExpirationInterval

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| ExpirationDeadline: point in time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format. ExpirationInterval: time interval supporting `s/m/h/d` suffixes during which the node shouldn't be accessed. | — | Dynamic |

Allows management of [TTL for tables created by the operation]({{yt-docs-root}}/user-guide/storage/cypress#TTL).

## yt.MaxRowWeight

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, up to 128M | 16M | Dynamic |

Increase the maximum table row length limit in yt.

## yt.MaxKeyWeight

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, up to 256K | 16K | Dynamic |

Increase the maximum table key length limit in {{product-name}}, based on which the table is sorted.

## yt.UseTmpfs

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Dynamic |

Connects [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) to the `_yql_tmpfs` folder in the sandbox with MapReduce jobs. Its use is not recommended.

## yt.ExtraTmpfsSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Dynamic |

Ability to increase the size of tmpfs in addition to the total size of all expressly used files (specified in megabytes). It can be useful if you create new files in UDF locally. Without [UseTmpfs](#yt.usetmpfs) is ignored.

## yt.PoolTrees

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing a list of tree names separated by any of the following symbols: comma, semicolon, space, or `|` | — | Dynamic |

Ability to select pool trees different from the standard ones.{% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.cloud-nodes}}).{% endif %}

## yt.TentativePoolTrees

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing a list of tree names separated by any of the following symbols: comma, semicolon, space, or `|` | — | Dynamic |

Ability to "gently" spread operations across pool trees different from the standard ones.{% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.TentativeTreeEligibilitySampleJobCount

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | — | Dynamic |

Effective only when the `yt.TentativePoolTrees` pragma is present. Sets the number of jobs in a sample.{% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.TentativeTreeEligibilityMaxJobDurationRatio

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating-point number | — | Dynamic |

Effective only when the `yt.TentativePoolTrees` pragma is present. Sets the permissible job slowdown factor in an alternative pool tree. {% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.TentativeTreeEligibilityMinJobDuration

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Milliseconds | — | Dynamic |

Effective only when the `yt.TentativePoolTrees` pragma is present. Sets the minimum average job duration in an alternative pool tree. {% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

## yt.UseDefaultTentativePoolTrees

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | — | Dynamic |

Sets the value for the `use_default_tentative_pool_trees` option in the operation spec.

## yt.QueryCacheMode {#querycache}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: disable / readonly / refresh / normal | normal | Static |

Cache operates at the level of MapReduce operations:
* Cache is disabled in **disable** mode.
* **readonly** — read permissions only. No writes allowed.
* **refresh** — write-only. No reads are allowed. A query error is generated if an error occurs during parallel write to the cache from another transaction.
* **normal** — read and write permissions. If an error occurs during parallel write to the cache from another transaction, assume that the same data was written and continue your work.
In **normal** and **refresh** mode, the output for each operation is additionally stored in `//<tmp_folder>/query_cache/<hash>`, where:
 * tmp_folder — defaults to `tmp/<login>` or [PRAGMA yt.TmpFolder](#yt.tmpfolder) value;
 * hash — hash of input tables' meaningful metadata and the logical program that ran in the operation.
In **normal** and **readonly** mode, this path is calculated for the MapReduce operation just before its launch. Depending on the selected caching mode, the operation may either be launched or instantly marked as successful using the prepared table instead of its outcome. If an expression contains nondeterministic functions like Random/RandomNumber/RandomUuid/CurrentUtcDate/CurrentUtcDatetime/CurrentUtcTimestamp, the cache for this operation is disabled. All UDFs are currently considered deterministic, meaning they don't interfere with caching. If a non-deterministic UDF must be used, you should specify an additional Uint64-type argument and pass `CurentUtcTimestamp()` to it. Use of arguments is not mandatory in this case.

## yt.QueryCacheIgnoreTableRevision

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

If the flag is set, {{product-name}} [revision is excluded from metadata during hash calculation]({{yt-docs-root}}/user-guide/storage/cypress). Therefore, QueryCache is not invalidated when modifying input table contents.

The mode is primarily intended for speeding up the complex queries debugging process in large, modifiable tables where query logic can't ignore these modifications.

## yt.QueryCacheSalt

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Random string | — | Static |

Salt to be mixed into the hash values calculation process for the query cache

## yt.QueryCacheTtl

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes that counts from table creation time in the query cache or from the time of last table use. | 7d | Static |

Allows management of [TTL for tables created by the operation in the query cache]({{yt-docs-root}}/user-guide/storage/cypress).

## yt.AutoMerge / yt.TemporaryAutoMerge / yt.PublishedAutoMerge

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: relaxed/economy/disabled | relaxed | Dynamic |

Management of the [same-named setting{{product-name}}]({{yt-docs-root}}/user-guide/data-processing/operations/automerge) that helps reduce the utilization quota for chunk quantity. `yt.TemporaryAutoMerge` is valid for all YT operations except for merge inside the YtPublish node.

`yt.PublishedAutoMerge` is only valid for merge inside the YtPublish node (if it's launched there). `yt.AutoMerge` sets the value for this setting simultaneously for all {{product-name}} query operations.

## yt.ScriptCpu

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 1.0 | Dynamic |

Multiplier for evaluating utilization of the script UDF processor (including Python UDF and JavaScript UDF). Affects splitting of MapReduce operations to jobs. May be redefined with special-purpose `yt.PythonCpu` / `yt.JavascriptCpu` pragmas for a specific UDF type.

## yt.PythonCpu / yt.JavascriptCpu

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 4.0 | Dynamic |

Multiplier for evaluating utilization of the Python UDF and JavaScript UDF processors, respectively. Affects splitting of MapReduce operations into jobs.

## yt.ErasureCodecCpu

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 1.0 | Dynamic |

Multiplier for evaluating utilization of the processor used for processing tables compressed with the erasure codec. Affects splitting of MapReduce operations to jobs.

## yt.ReleaseTempData

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: immediate/finish/never | immediate | Static |

Allows management of the removal time of temporary objects (e.g. tables) created when running the query:

* `immediate` — remove objects as soon as they're no longer required.
* `finish` — remove after running the entire YQL query.
* `never` — never remove.

## yt.CoreDumpPath

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Path on cluster | — | Static |

Allows the [coredump](https://en.wikipedia.org/wiki/Core_dump) of dropped jobs for MapReduce operations to be saved to a separate table.

## yt.MaxInputTables

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 1000 | Static |

Limit of the number of delivered input tables for each specific MapReduce operation.

## yt.MaxInputTablesForSortedMerge

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 100 | Static |

Limit of the number of delivered input tables for a sorted merge operation.

## yt.MaxOutputTables

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number from 1 to 100 | 50 | Static |

Limit of the number of output tables for each specific MapReduce operation.

## yt.JoinCollectColumnarStatistics

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: disable/sync/async | async | Static |

Manages the use of columnar statistics in order to precisely evaluate JOIN inputs and select the optimal strategy. Async includes the asynchronous columnar statistics collection mode.

## yt.JoinColumnarStatisticsFetcherMode

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: from_nodes/from_master/fallback | fallback | Static |

Manages the columnar statistics query mode in order to precisely evaluate JOIN inputs from {{product-name}}. From_nodesmode ensures precise evaluation but may fail to fit timeouts for large tables. From_master mode works very fast but provides simplified statistics. Fallback mode works as a combination of the previous two.

## yt.MapJoinLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 2048M | Static |

Limit of a smaller table in JOIN, which ensures the Map-side strategy (creating a dictionary in the memory based on the smaller table and using it in the Map for a larger one).

You can disable the strategy completely by specifying 0 as the value.

## yt.MapJoinShardCount

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 10 | 4 | Static |

Map-side JOIN strategy may run in a sharded manner. The smaller side is split into N shards (where N is less than or equal to the value of this PRAGMA), and all shards are independently and simultaneously joined with the larger side. Thus, concatenation of JOIN with shards is considered to be the outcome of JOIN.

## yt.MapJoinShardMinRows

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 1 | Static |

Minimum number of writes to the shard in map-side JOIN strategy.

## yt.JoinMergeTablesLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 64 | Static |

Total permissible number of tables in the left and right sides for enabling Ordered JOIN strategy.

You can disable the strategy completely by specifying 0 as the value.

## yt.JoinMergeUseSmallAsPrimary

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | - | Static |

Explicit management in selecting the primary table in a Reduce operation with the Ordered JOIN strategy. If the value is set as True, then the smaller side will always be selected as the primary table. If the flag value is False, the larger side will be selected, except when unique keys are available on the larger side. Selecting a larger table as the primary table is safe, even if the table contains monster keys. However, it will run slower. If this pragma isn't set, the primary table is selected automatically based on the maximum size of the resulting jobs (see yt.JoinMergeReduceJobMaxSize).

## yt.JoinMergeReduceJobMaxSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 8G | Static |

Maximum acceptable size of Reduce job when selecting a small table as the primary table with the Ordered JOIN strategy. If the resulting size exceeds the specified value, the Reduce operation is repeated for the larger table as the primary table.

## yt.JoinMergeUnsortedFactor

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive floating point number | 0.2 | Static |

Minimum ratio of the unsorted JOIN side to the sorted one for its additional sorting and selection of the Ordered JOIN strategy.

## yt.JoinMergeForce

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | - | Static |

Forces selection of the Ordered JOIN strategy. If the flag is set to True, the Ordered JOIN strategy is selected even if a single JOIN side or both JOIN sides are unsorted. In this case, unsorted sides are pre-sorted. The maximum size of the unsorted table (see `yt.JoinMergeUnsortedFactor`) is unlimited in this case.

## yt.JoinAllowColumnRenames

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Static |

Enables column renaming when executing the Ordered JOIN strategy ([rename_columns]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#path_attributes) attribute is used). If the option is disabled, then the Ordered JOIN strategy is selected only when the left and right column names match.

## yt.UseColumnarStatistics

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: disable/auto/force/0 (=disable)/1 (=force) | force | Dynamic |

Includes the use of columnar statistics to precisely evaluate job sizes when launching operations on top of the tables containing columnar data selections. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

Auto mode automatically disables the use of statistics for operations that use tables with `optimize_for=lookup` as input.


## yt.MinPublishedAvgChunkSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Static |

If the average chunk size in the resulting output table is smaller than the specified setting, launch an additional {{product-name}} Merge operation to enlarges the chunks to reach the specified size. The value of 0 has a special meaning, causing merge to always launch and enlarge the chunks up to 1G.

If the table uses the compression codec, then the chunk output size may differ from the specified one by the compression factor value. Essentially, this pragma sets the data size per merge job. The output size may be significantly smaller after compression. In this case, you should increase the pragma value by the expected compression factor value.

## yt.MinTempAvgChunkSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Static |

The setting is similar to `yt.MinPublishedAvgChunkSize`, but it works for intermediate temporary tables.

## yt.TableContentDeliveryMode

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: native/file | native | Dynamic |

If the native value is set, then the table contents are delivered to jobs via native {{product-name}} mechanisms. If the file value is set, the table contents are first downloaded to the YQL server and then delivered to jobs as a regular file.

## yt.TableContentMaxChunksForNativeDelivery

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number, up to and including 1,000 | 1000 | Static |

Maximum number of chunks in the table for it to be delivered to jobs via native {{product-name}} mechanisms. If this number is exceeded, the table is delivered via a file.

## yt.TableContentCompressLevel

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number, up to and including 11 | 8 | Dynamic |

Setting the compression level for the table contents delivered via a file (if yt.TableContentDeliveryMode="file").

## yt.TableContentTmpFolder

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Path on cluster | — | Dynamic |

Directory where temporary files for tables delivered via file (if yt.TableContentDeliveryMode="file") will be added. If not set, then the standard {{product-name}} file cache is used.

## yt.TableContentMinAvgChunkSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 1GB | Static |

Minimum average size of chunks in the table for it to be delivered to jobs via native {{product-name}} mechanisms. If the chunk size isn't large enough, then preliminary merge is inserted.

## yt.TableContentMaxInputTables

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number, up to and including 1,000 | 1000 | Static |

Maximum number of tables for delivery to jobs via native {{product-name}} mechanisms. If this number is exceeded, then preliminary merge is inserted.

## yt.TableContentUseSkiff

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Dynamic |

Enables the skiff format for delivering the table to operation jobs.

## yt.LayerPaths

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing the list of paths to porto layers separated by any of the following symbols: comma, semicolon, space, or `|` | — | Dynamic |

Ability to specify the sequence of porto layers in order to create an environment for executing custom jobs.{% if audience == "internal" %} To learn more, see [Atushka]({{yql.pages.syntax.pragma.at-launch-jobs}}).{% endif %}

## yt.UseSkiff

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Dynamic |

Enables the skiff format for inputting and outputting in operation jobs.

## yt.DefaultCalcMemoryLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 1G | Static |

Limit on memory utilization for calculations that aren't related to table access.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

## yt.ParallelOperationsLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, minimum 1 | 16 | Static |

Sets the maximum number of concurrently launched {{product-name}} operations in a query.

## yt.DefaultCluster

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | hahn | Static |

Sets the default cluster for performing computations that aren't related to table access.

## yt.DefaultMemoryReserveFactor

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number from 0.0 to 1.0 | — | Dynamic |

Sets the job memory reservation factor. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#memory_reserve_factor).

## yt.DefaultMemoryDigestLowerBound

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number from 0.0 to 1.0 | — | Dynamic |

Sets the `user_job_memory_digest_lower_bound` setting in the operation spec. To learn more about the setting, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/scheduler/memory-digest#nastrojki-digest).

## yt.BufferRowCount

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, minimum 1 | — | Dynamic |

Limit on the number of records that JobProxy can buffer.{% if audience == "internal" %} To learn more, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/jobs).{% endif %}

## yt.DisableJobSplitting

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Dynamic |

Prohibit the {{product-name}} scheduler from adaptively splitting long-running custom jobs.

## yt.DefaultLocalityTimeout

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `locality_timeout` setting in the operation spec (the setting isn't yet documented).

## yt.MapLocalityTimeout

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `map_locality_timeout` setting in the operation spec (the setting isn't yet documented).

## yt.ReduceLocalityTimeout

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `reduce_locality_timeout` setting in the operation spec (the setting isn't yet documented).

## yt.SortLocalityTimeout

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `sort_locality_timeout` setting in the operation spec (the setting isn't yet documented).

## yt.MinLocalityInputDataWeight

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Dynamic |

Sets the `min_locality_input_data_weight` setting in the operation spec (the setting isn't yet documented).

## yt.DefaultMapSelectivityFactor

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive floating point number | — | Dynamic |

Sets the approximate output-input ratio for the map stage in the joint MapReduce operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/mapreduce).

## yt.SuspendIfAccountLimitExceeded

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Dynamic |

Pause the operation if the "Account limit exceeded" error occurs in jobs. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

## yt.CommonJoinCoreLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 128M | Static |

Sets the memory buffer size for CommonJoinCore node execution (executed in the job when the common JOIN strategy is selected).

## yt.CombineCoreLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, minimum 1M | 128M | Static |

Sets the memory buffer size for CombineCore node execution.

## yt.SwitchLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, minimum 1M | 128M | Static |

Sets the memory buffer size for Switch node execution.

## yt.EvaluationTableSizeLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, maximum 10M | 1M | Static |

Sets the maximum total volume of tables used at the evaluation step.

## yt.LookupJoinLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, maximum 10M | 1M | Static |

A table may be used as a map in the Lookup JOIN strategy if it doesn't exceed the minimum size specified in `yt.LookupJoinLimit` and `yt.EvaluationTableSizeLimit`.

## yt.LookupJoinMaxRows

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, maximum 1,000 | 900 | Static |

Maximum number of table rows at which the table may be used as a dictionary in the Lookup JOIN strategy.

## yt.MaxExtraJobMemoryToFuseOperations

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 2G | Static |

Maximum memory utilization for jobs permitted after operations are merged by optimizers.

## yt.MaxReplicationFactorToFuseOperations

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 20.0 | Static |

Maximum data replication factor permitted after operations are merged by optimizers.

## yt.TopSortMaxLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 1000 | Static |

Maximum LIMIT value used together with ORDER BY at which TopSort optimization is launched.

## yt.TopSortSizePerJob

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, minimum 1 | 128M | Static |

Sets the expected data volume per job in a TopSort operation.

## yt.TopSortRowMultiplierPerJob

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, minimum 1 | 10 | Static |

Sets the expected number of records per job in a TopSort operation, calculated as `LIMIT * yt.TopSortRowMultiplierPerJob`.

## yt.DisableOptimizers

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing the list of optimizers separated by any of the following symbols: comma, semicolon, space, or `|` | — | Static |

Disables the set optimizers.

## yt.JobEnv

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets environment variables for map and reduce jobs in operations. Map keys set the environment variable names, and map values set the values for these variables.

## yt.OperationSpec

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets the operation settings map. Lets you set the settings that have no counterparts in the form of pragmas. Settings that were set via special-purpose pragmas have priority and redefine the values in this map.

## yt.Annotations

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets arbitrary structured information related to the operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

## yt.StartedBy

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets the map describing the client that started the operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

## yt.Description

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets human-readable information displayed on the operation page in the web interface. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

{% if audience == "internal" %}
## yt.GeobaseDownloadUrl

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Sets a URL for downloading the geobase (geodata6.bin file) if the query uses Geo UDF.
{% endif %}
## yt.MaxSpeculativeJobCountPerTask

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | — | Dynamic |

Sets the number of speculatively executed jobs in {{product-name}} operations. {{product-name}} cluster settings are used by default.

## yt.LLVMMemSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 256M | Dynamic |

Sets the fixed memory size required for compiling the LLVM code in jobs.

## yt.LLVMPerNodeMemSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 10K | Dynamic |

Sets the required memory size per computation graph node for compiling the LLVM code in jobs.

## yt.SamplingIoBlockSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Dynamic |

Sets the minimum size of a block for coarse-grain sampling.

## yt.BinaryTmpFolder

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Path on cluster | — | Static |

Sets a separate path on the cluster for caching binary query artifacts (UDF and job binary). Artifacts are saved to the directory root with the same name as the artifact's md5. Artifacts are saved and used in this directory outside of transactions even if a `yt.ExternalTx` pragma is set in the query.

## yt.BinaryExpirationInterval

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Static |

Allows management of [TTL for cached binary artifacts]({{yt-docs-root}}/user-guide/storage/cypress#TTL). Only works together with `yt.BinaryTmpFolder`. Each use of a binary artifact in the query extends the lifetime of its TTL.

## yt.FolderInlineDataLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 100K | Static |

Sets the maximum amount of data for the inline list obtained as a result of the Folder computation. If a greater size is selected, a temporary file will be used.

## yt.FolderInlineItemsLimit

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 100 | Static |

Sets the maximum number of elements in the inline list obtained as a result of the Folder computation. If a greater size is selected, a temporary file will be used.

## `yt.UseNativeYtTypes` {#yt-usenativeyttypes}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

Allows values of composite data types to be written to tables through native support of composite types in {{product-name}}.

## yt.PublishedMedia` / `yt.TemporaryMedia`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Set the `@media` attribute for newly created tables. If available, assigns [mediums in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#naznachenie-mediuma), where table chunks will be stored.

Tables specified in [INSERT INTO](../insert_into.md) are Published. All other tables are Temporary.

## yt.PublishedPrimaryMedium / yt.TemporaryPrimaryMedium

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Set the `@primary_medium` attribute for newly created tables. If available, assigns the [primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary), where chunks will be recorded. By default, {{product-name}} sets the primary medium to `"default"`.

Tables specified in [INSERT INTO](../insert_into.md) are Published. All other tables are Temporary.

## yt.IntermediateDataMedium

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Set the medium used for intermediate data in operations (Sort, MapReduce). To learn more, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/sort).

## yt.PrimaryMedium

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Sets the [primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary) for Published and Temporary tables and intermediate data in operations. Amounts to simultaneous setting of `yt.IntermediateDataMedium`, `yt.PublishedPrimaryMedium`, and `yt.TemporaryPrimaryMedium` pragmas.

## yt.HybridDqExecution

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Static |

Includes hybrid query execution via DQ

## yt.NetworkProject

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | - | Dynamic |

Sets the use of a specified network project in jobs. <!--See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/mtn).-->

## yt.BatchListFolderConcurrency

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 5 | Static |

Sets the number of concurrent directory listing operations.

## yt.ColumnGroupMode

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: disable, single or perusage | disable | Static |

Sets the columnar group computation mode for intermediate query tables. In `disable` mode, columnar groups aren't used. In `single` mode, one group is created for all table columns. In `perusage` mode, granular column groups by their consumers are created. All columns of the same group are simultaneously used in one or more consumers. For example, if the intermediate table has columns [a, b, c, d, e, f] and it is used by two operations with column selections [a, b, c, d] and [c, d, e, f] respectively, three column groups ([a, b], [c, d], and [e, f]) will be created in the table. If the intermediate table is used for publishing to an output table (that is, the consumer is the YtPublish node), columnar groups aren't used, except for the explicitly specified [column_groups modifier](../insert_into.md#hints). In the latter case, the intermediate table uses the modifier's columnar groups.

## yt.MinColumnGroupSize

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number not less than 2 | 2 | Static |

Sets the minimum size of a columnar group. If a computed group contains the number of columns that is less than the specified pragma value, the group isn't created.

## yt.MaxColumnGroups

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 64 | Static |

Sets the maximum number of columnar groups per intermediate query table. If the computed number of groups exceeds this limit, groups aren't created for this table.

## yt.ForceJobSizeAdjuster

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Dynamic |

Sets the `"force_job_size_adjuster"` option in the operation settings.

