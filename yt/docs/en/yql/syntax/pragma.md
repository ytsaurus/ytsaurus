# PRAGMA

## Definition

Redefinition of settings.

### Syntax

`PRAGMA x.y = "z";` or `PRAGMA x.y("z", "z2", "z3");`:

* `x`: (optional) The category of the setting.
* `y`: The name of the setting.
* `z`: (optional for flags) The value of the setting. The following suffixes are acceptable:

  * `k`, `m`, `g`, `t`: For the data amounts.
  * `w`,`d`, `h`, `m`, `s`, `ms`, `us`, `ns` : For the time values.

For [dynamic yt pragmas](#yt), you can revert the settings values to their default states using `PRAGMA my_pragma = default;`. Please note that you can't reset the settings for other pragmas.

### Examples

```yql
PRAGMA AutoCommit;
```

```yql
PRAGMA TablePathPrefix = "home/yql";
```

```yql
PRAGMA Warning("disable", "1101");
```

For the complete list of available settings, [see the table below](#pragmas).

### Scope {#pragmascope}

Unless otherwise specified, a pragma affects all the subsequent expressions up to the end of the module where it's used. If necessary and logically possible, you can change the value of this setting several times within a given query to make it different at different execution steps.

There are also special scoped pragmas with the scope defined by the same rules as the scope of [named expressions](expressions.md#named-nodes).

Unlike scoped pragmas, regular pragmas can only be used in the global scope (not inside [DEFINE ACTION](action.md#define-action) and [DEFINE SUBQUERY](subquery.md#define-subquery)).

## Global {#pragmas}

### AutoCommit {#autocommit}

| Value type | By default |
| --- | --- |
| Flag | false |

Automatically perform [COMMIT](commit.md) after each expression.

### TablePathPrefix {#table-path-prefix}

| Value type | By default |
| --- | --- |
| String | — |

Add the specified prefix to the cluster table paths. Works on the same principle as merging paths in a file system: supports references to the parent catalog `..` and doesn't require adding a slash to the right. For example,

```yql
PRAGMA TablePathPrefix = "home/yql";
SELECT * FROM test;
```

The prefix is not added if the table name is an absolute path (starts with /).

### UDF {#udf}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Static |
| String, the name of the prefix appended to all modules | "" | Static |

Importing all UDFs from the specified library. For the pragma to work, the library must be attached to the query using the ![ico](../../../images/qt-ui-attach.png =30x30) icon. Please note that the library must be shared library (.so) and compiled for Linux x64.

When setting a prefix, it's appended before the names of all loaded modules, e.g. CustomPrefixIp::IsIPv4 instead of Ip::IsIPv4. Setting the prefix lets you use  different versions of the same UDF.

### RuntimeLogLevel {#runtime-log-level}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String, one of the following: `Trace`, `Debug`, `Info`, `Notice`, `Warn`, `Error`, or `Fatal` | `Info` | Static |

Lets you change the computation logging level (for example, for UDFs) at query runtime or at the UDF signature declaration stage.

### UseTablePrefixForEach {#use-table-prefix-for-each}

| Value type | By default |
| --- | --- |
| Flag | false |

`EACH` uses [TablePathPrefix](#table-path-prefix) for each list item.

### Warning {#warning}

| Value type | By default |
| --- | --- |
| 1. Action<br/>2. Warning code or "*" | — |

Action:

* `disable`: Disable.
* `error`: Treat as an error.
* `default`: Revert to the default behavior.

The warning code is returned with the text itself (it's displayed on the right side of the web interface).

Examples:

`PRAGMA Warning("error", "*");`
`PRAGMA Warning("disable", "1101");`
`PRAGMA Warning("default", "4503");`

In this case, all the warnings are treated as errors, except for warnings `1101` (will be disabled) and `4503` (will be processed by default, that is, remain a warning). Since warnings may be added in new YQL releases, use `PRAGMA Warning("error", "*");` with caution (at least cover such queries with autotests).

[List of warning and error codes]({{yql.pages.syntax.pragma.error-code-list}})

### Greetings {#greetings}

| Value type | By default |
| --- | --- |
| Text | — |

Issue the specified text as the query's Info message.

Example: `PRAGMA Greetings("It's a good day!");`

### WarningMsg {#warningmsg}

| Value type | By default |
| --- | --- |
| Text | — |

Issue the specified text as the query's Warning message.

Example: `PRAGMA WarningMsg("Attention!");`

### DqEngine {#dqengine}

| Value type | By default |
| --- | --- |
| disable/auto/force string | "auto" |

When set to "auto", it enables a new compute engine. Computing is made, whenever possible, without creating map/reduce operations. The "force" value unconditionally routes computations to the new engine.

### SimpleColumns {#simplecolumns}

`SimpleColumns` / `DisableSimpleColumns`

| Value type | By default |
| --- | --- |
| Flag | true |

If using `SELECT foo.* FROM ... AS foo`, delete the `foo.`  prefix from the names of the resulting columns.

It also works for [JOIN](join.md), but then it may crash if there's a name conflict (which can be resolved through [WITHOUT](select/without.md) and renaming columns). For JOIN in SimpleColumns mode, an implicit Coalesce is made for key columns: the query `SELECT * FROM T1 AS a JOIN T2 AS b USING(key)` in the SimpleColumns mode works same as `SELECT a.key ?? b.key AS key, ... FROM T1 AS a JOIN T2 AS b USING(key)`

### CoalesceJoinKeysOnQualifiedAll

`CoalesceJoinKeysOnQualifiedAll` / `DisableCoalesceJoinKeysOnQualifiedAll`

| Value type | By default |
| --- | --- |
| Flag | true |

Controls implicit Coalesce for the key `JOIN` columns in the SimpleColumns mode. If the flag is set, key columns are coalesced if there is at least one expression in the format `foo.*` or `*` in SELECT: for example, `SELECT a.* FROM T1 AS a JOIN T2 AS b USING(key)`. If the flag isn't set, JOIN keys are coalesced only if there is an asterisk '*' after `SELECT`.

### StrictJoinKeyTypes

`StrictJoinKeyTypes` / `DisableStrictJoinKeyTypes`

| Value type | By default |
| --- | --- |
| Flag | false |

If the flag is set, [JOIN](join.md) will require a strict match of key types.
By default, JOIN preconverts keys to a shared type, which might result in performance degradation.

StrictJoinKeyTypes is a [scoped](#pragmascope) setting.


### AnsiInForEmptyOrNullableItemsCollections

| Value type | By default |
| --- | --- |
| Flag | false |

This pragma brings the behavior of the `IN` operator in accordance with the standard when there's `NULL` in the left or right side of `IN`. The behavior of `IN` when on the right side there is a Tuple with elements of different types also changed. Examples:

`1 IN (2, 3, NULL) = NULL (was Just(False))`
`NULL IN () = Just(False) (was NULL)`
`(1, null) IN ((2, 2), (3, 3)) = Just(False) (was NULL)`

For more information about the `IN` behavior when operands include NULLs, see [here](expressions.md#in). You can explicitly select the old behavior by specifying the pragma `DisableAnsiInForEmptyOrNullableItemsCollections`. If no pragma is set, you'll see a warning, and the original variant will be used.

### AnsiRankForNullableKeys

| Value type | By default |
| --- | --- |
| Flag | false |

Aligns the RANK/DENSE_RANK behavior with the standard if there are optional types in the window sort keys or in the argument of such window functions. It means that:

* The result type is always Uint64 rather than Uint64?;
* NULLs in keys are treated as equal to each other (the current implementation returns NULL).

You can explicitly select the old behavior by specifying the pragma `DisableAnsiRankForNullableKeys`. If no pragma is set, you'll see a warning, and the original variant will be used.

### AnsiCurrentRow

| Value type | By default |
| --- | --- |
| Flag | false |

Aligns the implicit setting of a window frame with the standard if there is [ORDER BY](select/order_by.md).

If AnsiCurrentRow is not set, the `(ORDER BY key)` window is equivalent to `(ORDER BY key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`. The standard requires that such window behaves as `(ORDER BY key RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

The difference is in `CURRENT ROW` interpretation. In `ROWS` mode, `CURRENT ROW` is interpreted literally: the current string in the partition.
And in `RANGE` mode, the end of the `CURRENT ROW` frame means "the last row in the partition with the sorting key equal to the current row".

### OrderedColumns {#orderedcolumns}

`OrderedColumns` / `DisableOrderedColumns`

Output the [sequence of columns](select/index.md#orderedcolumns) to SELECT/JOIN/UNION ALL and save it when recording the results. The order of columns is undefined by default.

### PositionalUnionAll {#positionalunionall}

Enable the standard columnar execution for [UNION ALL](select/union.md#union-all). This automatically enables [ordered columns](#orderedcolumns).

### RegexUseRe2

| Value type | By default |
| --- | --- |
| Flag | false |

Use Re2 UDF instead of Pcre for executing `REGEX`,`MATCH`,`RLIKE`, and SQL operators. Re2 UDF supports correct processing of Unicode symbols, unlike Pcre UDF, which is used by default.

### ClassicDivision

| Value type | By default |
| --- | --- |
| Flag | true |

In the classical version, the result of integer division remains integer (by default). If disabled, the result is always Double.

`ClassicDivision` is a [scoped](#pragmascope) setting.

### CheckedOps

| Value type | By default |
| --- | --- |
| Flag | false |

When enabled: if aggregate functions SUM/SUM_IF, binary operations `+`, `-`, `*`, `/`, `%`, or unary operation `-` on integers result in an overflow beyond the range of the target argument or result type, return `NULL`. When disabled: overflow isn't checked.

Has no effect on floating point or `Decimal` numbers.

`CheckedOps` is a [scoped](#pragmascope) setting.

### UnicodeLiterals

`UnicodeLiterals`/`DisableUnicodeLiterals`

| Value type | By default |
| --- | --- |
| Flag | false |

When the mode is enabled, string literals without suffixes like "foo"/'bar'/@@multiline@@ will have the `Utf8` type. If disabled, then `String`.

`UnicodeLiterals` is a [scoped](#pragmascope) setting.

### WarnUntypedStringLiterals

`WarnUntypedStringLiterals`/`DisableWarnUntypedStringLiterals`

| Value type | By default |
| --- | --- |
| Flag | false |

When the mode is enabled, string literals without a suffix like "foo"/'bar'/@@multiline@@ will prompt a warning. It can be suppressed if you explicitly select the `s` suffix for the `String` type or the `u` suffix for the `Utf8` type.

`WarnUntypedStringLiterals` is a [scoped](#pragmascope) setting.

### AllowDotInAlias

| Value type | By default |
| --- | --- |
| Flag | false |

Enable dot in names of result columns. This behavior is disabled by default, since the further use of such columns in JOIN is not fully implemented.

### WarnUnnamedColumns

| Value type | By default |
| --- | --- |
| Flag | false |

Generate a warning if a column name (in the format `column[0-9]+`) was automatically generated for an unnamed expression in `SELECT`.

### GroupByLimit

| Value type | By default |
| --- | --- |
| Positive number | 32 |

Increasing the limit on the number of groups in [GROUP BY](group_by.md).

### GroupByCubeLimit

| Value type | By default |
| --- | --- |
| Positive number | 5 |

Increasing the limit on the number of dimensions in [GROUP BY](group_by.md#rollup-cube-group-sets).

Use this option with care: the computational complexity of the query grows exponentially with the number of dimensions.


## Yson

Management of Yson UDF default behavior. To learn more, see the [documentation](../udf/list/yson.md), in particular, [Yson::Options](../udf/list/yson.md#ysonoptions).

### `yson.AutoConvert`

| Value type | By default |
| --- | --- |
| Flag | false |

Automatic conversion of values to the required data type in all Yson UDF calls, including implicit calls.

### `yson.Strict`

| Value type | By default |
| --- | --- |
| Flag | true |

Strict mode control in all Yson UDF calls, including implicit calls. Empty or `"true"` value enables strict mode. If the value is `"false"`, strict mode is disabled.

### `yson.DisableStrict`

| Value type | By default |
| --- | --- |
| Flag | false |

An inverted version of `yson.Strict`. Empty or `"true"` value disables strict mode. If the value is `"false"`, strict mode is enabled.


## Working with files

### File

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments — alias, URL, and optional token name | — | Static |

Attach a file to the query by URL. For attaching files you can use the built-in functions [FilePath and FileContent](../builtins/basic.md#filecontent). This `PRAGMA` is a universal alternative to attaching files using built-in mechanisms of web or console clients.

YQL reserves the right to cache files at the URL for an indefinite period, hence, if there is a significant change in the content behind it, we strongly recommend to modify the URL by adding or modifying dummy parameters.

If the token name is specified, its value will be used to access the target system.

### FileOption

| Value type | Default value | Static/<br/>dynamic |
|-------------------------------------------------|--------------|--------------------------------|
| Three string arguments: alias, key, value | — | Static |

Set the option by the specified key for the specified file to the specified value. The file with this alias should already be declared through [PRAGMA File](#file) or attached to the query.

Currently supported options:

| Key | Value range | Description |
|-------------------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `bypass_artifact_cache` | `true`/`false` | Manages [caching]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#fajly) |

Example:

```yql
PRAGMA FileOption("<file-name>", "bypass_artifact_cache", "true");
```

{% if audience == "internal" %}
### Folder

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments — prefix, URL, and optional token name | — | Static |

Attach a set of files to the query by URL. Functions similar to adding a set of files using [PRAGMA File](#file) via direct links to files with aliases obtained by joining a prefix with the file name via `/`.

If the token name if specified, its value will be used to access the target system.
{% endif %}

### Library

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| One or two arguments: file name and optional URL | — | Static |

Treat the specified attached file as a library from which you can do [IMPORT](export_import.md). The syntax type for the library is determined from the file extension:
* `.sql`: For the YQL dialect of SQL <span style="color: green;">(recommended)</span>.
* `.yqls`: For {% if audience == "internal"  %}[s-expressions]({{yql.s-expressions-link}}){% else %}s-expressions{% endif %}.

Example with a file attached to the query:

```yql
PRAGMA library("a.sql");
IMPORT a SYMBOLS $x;
SELECT $x;
```

If the URL is specified, the library is downloaded from the URL rather than from the previously attached file as in the following example:
{% if audience == "internal"%}
```yql
PRAGMA library("a.sql","{{ corporate-paste }}/5618566/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "5618566"
PRAGMA library("a.sql","{{ corporate-paste }}/{$_ver}/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```
{% else %}

```yql
PRAGMA library("a.sql","https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docs/code-examples/yql/pragma-library-example");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "pragma-library-example"
PRAGMA library("a.sql","https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docs/code-examples/yql/{$_ver}");
IMPORT a SYMBOLS $x;
SELECT $x;
```
{% endif %}

### Package

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three arguments: package name, URL, and optional token | — | Static |

Attach a hierarchical set of files to the query by URL, treating them as a package with the specified name, an interrelated set of libraries.

The package name is expected in ``project_name.package_name`` format; package libraries can then be used to make [IMPORT](export_import.md) with a module name like ``pkg.project_name.package_name.maybe.nested.module.name``.

Example for a package with a flat hierarchy consisting of two libraries, foo.sql and bar.sql:

```yql
PRAGMA package({{yql.pages.syntax.pragma.package}});
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_path AS STRING; -- "path"
PRAGMA package({{yql.pages.syntax.pragma.package-var}});
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

### OverrideLibrary

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| One argument: file name | — | Static |

Interpret the specified attached file as a library and override one of the package libraries with it.

The file name should be in the format ``project_name/package_name/maybe/nested/module/name.EXTENSION``, [PRAGMA Library](#library) extensions are supported.

Example:

```yql
PRAGMA package({{yql.pages.syntax.pragma.package}});
PRAGMA override_library("project/package/maybe/nested/module/name.sql");

IMPORT pkg.project.package.foo SYMBOLS $foo;
SELECT $foo;
```

{% if audience == "internal" %}

  {% note warning %}

  For PRAGMA `Folder`, only links to {{yql.pages.syntax.pragma.folder-note}} with resources containing the directory are supported.

  {% endnote %}

{% endif %}

  {% note warning %}

  For PRAGMA `Package`, only links to directories on {{product-name}} clusters are supported.

  {% endnote %}


## YT {#yt}

YT pragmas may be defined as static or dynamic based on their lifetimes. Static pragmas are initialized one time at the earliest query processing step. If a static pragma is specified multiple times in a query, it accepts the latest value set for it. Dynamic pragma values are initialized at the query execution step after its optimization and execution plan preparation. The specified value is valid until the next identical pragma is found or until the query is completed. For dynamic pragmas only, you can reset their values to the default by assigning a `default`.

All pragmas that affect query optimizers are static because dynamic pragma values are not yet calculated at this step.

### `yt.InferSchema` / `yt.ForceInferSchema` {#inferschema}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 1,000 | — | Static |

Outputting the data schema based on the contents of the table's first rows. If PRAGMA is specified without a value, the contents of only the first row is used. If multiple rows are specified and column data types differ, they are extended to Yson.

InferSchema includes outputting data schemas for those tables only where it's not specified in metadata at all. When using ForceInferSchema, the data schema from metadata is ignored except for the list of key columns for sorted tables.

In addition to the detected column, dictionary column _other (row-on-row) is generated, which contains values for those columns that weren't in the first row but were found somewhere else. This lets you use [WeakField](../builtins/basic.md#weakfield) for such tables.

Due to a wide range of issues that may arise, this mode isn't recommended for use and is disabled by default.

### `yt.InferSchemaTableCountThreshold`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 50 | Static |

If the number of tables for which the schema is outputted based on their contents exceeds the specified value, then schema outputting is initiated as a separate operation on {{product-name}}, which may happen much faster.

### `yt.IgnoreWeakSchema`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

Ignore the table's weak schema (produced by sorting a non-schematized table based on a set of fields).

Together with `yt.InferSchema`, you can output data-based schemas for such tables.

### `yt.IgnoreYamrDsv`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

Ignore `_format=yamred_dsv` if it is specified in the input table's metadata.

### `yt.IgnoreTypeV3` {#ignoretypev3}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

 When reading tables with type_v3 schema, all fields containing complex types will be displayed as Yson fields in the query. Complex types include all non-data types and data types with more than one level of optionality.

### `yt.StaticPool`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | Current user login | Static |

Selecting a computing pool in the scheduler for operations performed at the optimization step.

### `yt.Pool`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | `yt.StaticPool` if set, or the current user login | Dynamic |

Selecting a computing pool in the scheduler for regular query operations.

### `yt.Owners`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A string containing the list of logins separated by any of these symbols: comma, semicolon, space or `|` | — | Dynamic |

Lets you grant management permissions for operations created by MapReduce in {{product-name}} (cancel, pause, run-job-shell, etc.) to any users other than the YQL operation owner.

{% if audience == "internal" %}
For instance, if YQL operations are initiated from a [robot user account]({{yql.pages.syntax.pragma.zombik}}), then you should add employees responsible for it to this list.
{% endif %}

### `yt.OperationReaders`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A string containing the list of logins separated by any of these symbols: comma, semicolon, space or `|` | — | Dynamic |

Lets you grant read permissions for operations created by MapReduce in {{product-name}} to any users other than the YQL operation owner.

### `yt.Auth`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Static |

Use authentication data other than the default data.

### `yt.DefaultMaxJobFails`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 5 | Static |

The number of failed MapReduce jobs, upon reaching which query execution retries are stopped and the query is considered failed.

### `yt.DefaultMemoryLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 512M | Dynamic |

Limitation of memory utilization (bytes) by jobs, which is ordered when launching MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

### `yt.DataSizePerJob` / `yt.DataSizePerMapJob` {#datasizeperjob}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 2G | Dynamic |

Controls the splitting of MapReduce operations into jobs: the larger the number, the fewer jobs. Use a lower value for compute-intensive jobs. Use a higher value for jobs that scan through a large amount of data (namely, user_sessions).

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

### `yt.DataSizePerSortJob`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | - | Dynamic |

Controls the splitting of sort jobs in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

### `yt.DataSizePerPartition`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 1G | Dynamic |

Controls the size of partitions in MapReduce operations.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

### `yt.MaxJobCount`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive integer | 16384 | Dynamic |

Maximum number of jobs within a single {{product-name}} operation. It is used only for single-stage map, reduce, merge, and other operations. If [`yt.DataSizePerJob`](#datasizeperjob) and `yt.MaxJobCount` are both specified, job splitting is done with account for [`yt.DataSizePerJob`](#datasizeperjob), and even if the resulting value `N` exceeds `yt.MaxJobCount`, `N` jobs will be run, and `yt.MaxJobCount` will only affect whether the jobs will be split after their number reaches a particular threshold.

### `yt.UserSlots`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | No limits | Dynamic |

Upper limit on the number of concurrent jobs within a MapReduce operation.

### `yt.DefaultOperationWeight`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating-point number | 1.0 | Dynamic |

Weight of all launched MapReduce operations in a selected computing pool.

### `yt.TmpFolder`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | `//tmp/yql/<login>` | Static |

Directory for storing temporary tables and files.

### `yt.TablesTmpFolder`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | `//tmp/yql/<login>` | Static |

Directory for storing temporary tables. Takes priority over `yt.TmpFolder`.

### `yt.TempTablesTtl`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Static |

Allows management of TTL for temporary tables. Effective for tables containing a full result, while the other temporary tables are unconditionally removed upon completion of the query regardless of this pragma.

### `yt.FileCacheTtl`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | 7d | Static |

Allows management of TTL for {{product-name}} file cache. Value of 0 disables use of TTL for file cache.

### `yt.IntermediateAccount`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Account name in {{product-name}} | intermediate | Dynamic |

Allows use of your account for intermediate data in a unified MapReduce operation.

The common account, which can overflow at an unfortunate time, is the default.

If [PRAGMA yt.TmpFolder](#yt.tmpfolder) is set, then instead of the common account you can use the one specified in the temporary directory.

### `yt.IntermediateReplicationFactor`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 10 | — | Dynamic |

Intermediate data replication factor.

### `yt.PublishedReplicationFactor` / `yt.TemporaryReplicationFactor` {#replicationfactor}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 10 | — | Dynamic |

Replication factor for tables created through YQL.

Tables specified in [INSERT INTO](insert_into.md) are Published. All other tables are Temporary.

### `yt.ExternalTx`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Static |

Running an operation in a transaction that has already been launched outside YQL. Its identifier is passed to the value.

All directories required for running the query are created in a specified transaction. This may cause conflicts when attempting to write data from two queries with different ExternalTx into a previously non-existent directory.

### `yt.OptimizeFor`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: lookup/scan | scan | Dynamic |

Controls the `optimize_for` attribute for the tables being created.

### `yt.PublishedCompressionCodec` / `yt.TemporaryCompressionCodec` {#compressioncodec}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/compression) | zstd_5 | Dynamic |

Compression settings for tables created through YQL.

Tables specified in [INSERT INTO](insert_into.md) are Published. All other tables are Temporary. Also, a codec specified as Temporary is used for intermediate data in a single {{product-name}} operation (e.g. unified MapReduce).

### `yt.PublishedErasureCodec` / `yt.TemporaryErasureCodec`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String, see the [documentation]({{yt-docs-root}}/user-guide/storage/replication#erasure) | none | Dynamic |

Erasure coding is always disabled by default. To enable it, you should use a value of lrc_12_2_2.

The difference between Published and Temporary is similar to [CompressionCodec](#compressioncodec).

### `yt.NightlyCompress`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | - | Dynamic |

Manages the [background table compression]({{yt-docs-root}}/user-guide/storage/regular-system-processes#nightly_compress) process so that the tables take up less space.

The `true` value sets the `@force_nightly_compress` attribute equal to `true` on the table.
The `false` value sets the `@nightly_compression_settings` attribute with the `enabled` child value equal to `false` on the table.

The setting applies only to tables newly created by the YQL query (as well as to tables overwritten using [INSERT INTO ... WITH TRUNCATE](insert_into)).
The setting doesn't apply to temporary tables.

### `yt.ExpirationDeadline` / `yt.ExpirationInterval`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| ExpirationDeadline: point in time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format. ExpirationInterval: time interval supporting `s/m/h/d` suffixes during which the node shouldn't be accessed. | — | Dynamic |

Allows management of [TTL for tables created by the operation]({{yt-docs-root}}/user-guide/storage/cypress#TTL).

### `yt.MaxRowWeight`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, up to 128M | 16M | Dynamic |

Increase the maximum table row length limit in yt.

### `yt.MaxKeyWeight`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, up to 256K | 16K | Dynamic |

Increase the maximum table key length limit in {{product-name}}, based on which the table is sorted.

### `yt.UseTmpfs`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Dynamic |

Connects [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) to the `_yql_tmpfs` folder in the sandbox with MapReduce jobs. Its use is not recommended.

### `yt.ExtraTmpfsSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Dynamic |

Ability to increase the size of tmpfs in addition to the total size of all expressly used files (specified in megabytes). It can be useful if you create new files in UDF locally. Without [UseTmpfs](#yt.usetmpfs) is ignored.

### `yt.PoolTrees`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing a list of tree names separated by any of the following symbols: comma, semicolon, space, or `|` | — | Dynamic |

Ability to select pool trees different from the standard ones.{% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.cloud-nodes}}).{% endif %}

### `yt.TentativePoolTrees`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing a list of tree names separated by any of the following symbols: comma, semicolon, space, or `|` | — | Dynamic |

Ability to "gently" spread operations across pool trees different from the standard ones.{% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

### `yt.TentativeTreeEligibilitySampleJobCount`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | — | Dynamic |

Effective only when the `yt.TentativePoolTrees` pragma is present. Sets the number of jobs in a sample.{% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

### `yt.TentativeTreeEligibilityMaxJobDurationRatio`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating-point number | — | Dynamic |

Effective only when the `yt.TentativePoolTrees` pragma is present. Sets the permissible job slowdown factor in an alternative pool tree. {% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

### `yt.TentativeTreeEligibilityMinJobDuration`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Milliseconds | — | Dynamic |

Effective only when the `yt.TentativePoolTrees` pragma is present. Sets the minimum average job duration in an alternative pool tree. {% if audience == "internal" %} To learn more, see the [documentation]({{yql.pages.syntax.pragma.pooltrees}}).{% endif %}

### `yt.UseDefaultTentativePoolTrees`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | — | Dynamic |

Sets the value for the `use_default_tentative_pool_trees` option in the operation spec.

### `yt.QueryCacheMode` {#querycache}

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

### `yt.QueryCacheIgnoreTableRevision`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

If the flag is set, {{product-name}} [revision is excluded from metadata during hash calculation]({{yt-docs-root}}/user-guide/storage/cypress). Therefore, QueryCache is not invalidated when modifying input table contents.

The mode is primarily intended for speeding up the complex queries debugging process in large, modifiable tables where query logic can't ignore these modifications.

### `yt.QueryCacheSalt`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Random string | — | Static |

Salt to be mixed into the hash values calculation process for the query cache

### `yt.QueryCacheTtl`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes that counts from table creation time in the query cache or from the time of last table use. | 7d | Static |

Allows management of [TTL for tables created by the operation in the query cache]({{yt-docs-root}}/user-guide/storage/cypress).

### `yt.AutoMerge` / `yt.TemporaryAutoMerge` / `yt.PublishedAutoMerge`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: relaxed/economy/disabled | relaxed | Dynamic |

Management of the [same-named setting{{product-name}}]({{yt-docs-root}}/user-guide/data-processing/operations/automerge) that helps reduce the utilization quota for chunk quantity. `yt.TemporaryAutoMerge` is valid for all YT operations except for merge inside the YtPublish node.

`yt.PublishedAutoMerge` is only valid for merge inside the YtPublish node (if it's launched there). `yt.AutoMerge` sets the value for this setting simultaneously for all {{product-name}} query operations.

### `yt.ScriptCpu`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 1.0 | Dynamic |

Multiplier for evaluating utilization of the script UDF processor (including Python UDF and JavaScript UDF). Affects splitting of MapReduce operations to jobs. May be redefined with special-purpose `yt.PythonCpu` / `yt.JavascriptCpu` pragmas for a specific UDF type.

### `yt.PythonCpu` / `yt.JavascriptCpu`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 4.0 | Dynamic |

Multiplier for evaluating utilization of the Python UDF and JavaScript UDF processors, respectively. Affects splitting of MapReduce operations into jobs.

### `yt.ErasureCodecCpu`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 1.0 | Dynamic |

Multiplier for evaluating utilization of the processor used for processing tables compressed with the erasure codec. Affects splitting of MapReduce operations to jobs.

### `yt.ReleaseTempData`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: immediate/finish/never | immediate | Static |

Allows management of the removal time of temporary objects (e.g. tables) created when running the query:

* `immediate` — remove objects as soon as they're no longer required.
* `finish` — remove after running the entire YQL query.
* `never` — never remove.

### `yt.CoreDumpPath`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Path on cluster | — | Static |

Allows the [coredump](https://en.wikipedia.org/wiki/Core_dump) of dropped jobs for MapReduce operations to be saved to a separate table.

### `yt.MaxInputTables`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 1000 | Static |

Limit of the number of delivered input tables for each specific MapReduce operation.

### `yt.MaxInputTablesForSortedMerge`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 100 | Static |

Limit of the number of delivered input tables for a sorted merge operation.

### `yt.MaxOutputTables`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number from 1 to 100 | 50 | Static |

Limit of the number of output tables for each specific MapReduce operation.

### `yt.JoinCollectColumnarStatistics`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: disable/sync/async | async | Static |

Manages the use of columnar statistics in order to precisely evaluate JOIN inputs and select the optimal strategy. Async includes the asynchronous columnar statistics collection mode.

### `yt.JoinColumnarStatisticsFetcherMode`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: from_nodes/from_master/fallback | fallback | Static |

Manages the columnar statistics query mode in order to precisely evaluate JOIN inputs from {{product-name}}. From_nodesmode ensures precise evaluation but may fail to fit timeouts for large tables. From_master mode works very fast but provides simplified statistics. Fallback mode works as a combination of the previous two.

### `yt.MapJoinLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 2048M | Static |

Limit of a smaller table in JOIN, which ensures the Map-side strategy (creating a dictionary in the memory based on the smaller table and using it in the Map for a larger one).

You can disable the strategy completely by specifying 0 as the value.

### `yt.MapJoinShardCount`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| A number from 1 to 10 | 4 | Static |

Map-side JOIN strategy may run in a sharded manner. The smaller side is split into N shards (where N is less than or equal to the value of this PRAGMA), and all shards are independently and simultaneously joined with the larger side. Thus, concatenation of JOIN with shards is considered to be the outcome of JOIN.

### `yt.MapJoinShardMinRows`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 1 | Static |

Minimum number of writes to the shard in map-side JOIN strategy.

### `yt.JoinMergeTablesLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 64 | Static |

Total permissible number of tables in the left and right sides for enabling Ordered JOIN strategy.

You can disable the strategy completely by specifying 0 as the value.

### `yt.JoinMergeUseSmallAsPrimary`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | - | Static |

Explicit management in selecting the primary table in a Reduce operation with the Ordered JOIN strategy. If the value is set as True, then the smaller side will always be selected as the primary table. If the flag value is False, the larger side will be selected, except when unique keys are available on the larger side. Selecting a larger table as the primary table is safe, even if the table contains monster keys. However, it will run slower. If this pragma isn't set, the primary table is selected automatically based on the maximum size of the resulting jobs (see yt.JoinMergeReduceJobMaxSize).

### `yt.JoinMergeReduceJobMaxSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 8G | Static |

Maximum acceptable size of Reduce job when selecting a small table as the primary table with the Ordered JOIN strategy. If the resulting size exceeds the specified value, the Reduce operation is repeated for the larger table as the primary table.

### `yt.JoinMergeUnsortedFactor`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive floating point number | 0.2 | Static |

Minimum ratio of the unsorted JOIN side to the sorted one for its additional sorting and selection of the Ordered JOIN strategy.

### `yt.JoinMergeForce`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | - | Static |

Forces selection of the Ordered JOIN strategy. If the flag is set to True, the Ordered JOIN strategy is selected even if a single JOIN side or both JOIN sides are unsorted. In this case, unsorted sides are pre-sorted. The maximum size of the unsorted table (see `yt.JoinMergeUnsortedFactor`) is unlimited in this case.

### `yt.JoinAllowColumnRenames`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Static |

Enables column renaming when executing the Ordered JOIN strategy ([rename_columns]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#path_attributes) attribute is used). If the option is disabled, then the Ordered JOIN strategy is selected only when the left and right column names match.

### `yt.UseColumnarStatistics`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: disable/auto/force/0 (=disable)/1 (=force) | force | Dynamic |

Includes the use of columnar statistics to precisely evaluate job sizes when launching operations on top of the tables containing columnar data selections. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

Auto mode automatically disables the use of statistics for operations that use tables with `optimize_for=lookup` as input.


### `yt.MinPublishedAvgChunkSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Static |

If the average chunk size in the resulting output table is smaller than the specified setting, launch an additional {{product-name}} Merge operation to enlarges the chunks to reach the specified size. The value of 0 has a special meaning, causing merge to always launch and enlarge the chunks up to 1G.

If the table uses the compression codec, then the chunk output size may differ from the specified one by the compression factor value. Essentially, this pragma sets the data size per merge job. The output size may be significantly smaller after compression. In this case, you should increase the pragma value by the expected compression factor value.

### `yt.MinTempAvgChunkSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Static |

The setting is similar to `yt.MinPublishedAvgChunkSize`, but it works for intermediate temporary tables.

### `yt.TableContentDeliveryMode`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: native/file | native | Dynamic |

If the native value is set, then the table contents are delivered to jobs via native {{product-name}} mechanisms. If the file value is set, the table contents are first downloaded to the YQL server and then delivered to jobs as a regular file.

### `yt.TableContentMaxChunksForNativeDelivery`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number, up to and including 1,000 | 1000 | Static |

Maximum number of chunks in the table for it to be delivered to jobs via native {{product-name}} mechanisms. If this number is exceeded, the table is delivered via a file.

### `yt.TableContentCompressLevel`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number, up to and including 11 | 8 | Dynamic |

Setting the compression level for the table contents delivered via a file (if yt.TableContentDeliveryMode="file").

### `yt.TableContentTmpFolder`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Path on cluster | — | Dynamic |

Directory where temporary files for tables delivered via file (if yt.TableContentDeliveryMode="file") will be added. If not set, then the standard {{product-name}} file cache is used.

### `yt.TableContentMinAvgChunkSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 1GB | Static |

Minimum average size of chunks in the table for it to be delivered to jobs via native {{product-name}} mechanisms. If the chunk size isn't large enough, then preliminary merge is inserted.

### `yt.TableContentMaxInputTables`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number, up to and including 1,000 | 1000 | Static |

Maximum number of tables for delivery to jobs via native {{product-name}} mechanisms. If this number is exceeded, then preliminary merge is inserted.

### `yt.TableContentUseSkiff`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Dynamic |

Enables the skiff format for delivering the table to operation jobs.

### `yt.LayerPaths`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing the list of paths to porto layers separated by any of the following symbols: comma, semicolon, space, or `|` | — | Dynamic |

Ability to specify the sequence of porto layers in order to create an environment for executing custom jobs.{% if audience == "internal" %} To learn more, see [Atushka]({{yql.pages.syntax.pragma.at-launch-jobs}}).{% endif %}

### `yt.UseSkiff`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Dynamic |

Enables the skiff format for inputting and outputting in operation jobs.

### `yt.DefaultCalcMemoryLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 1G | Static |

Limit on memory utilization for calculations that aren't related to table access.

You can use K, M, and G suffixes to specify values in kilobytes, megabytes, and gigabytes, respectively.

### `yt.ParallelOperationsLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, minimum 1 | 16 | Static |

Sets the maximum number of concurrently launched {{product-name}} operations in a query.

### `yt.DefaultCluster`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | hahn | Static |

Sets the default cluster for performing computations that aren't related to table access.

### `yt.DefaultMemoryReserveFactor`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number from 0.0 to 1.0 | — | Dynamic |

Sets the job memory reservation factor. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#memory_reserve_factor).

### `yt.DefaultMemoryDigestLowerBound`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number from 0.0 to 1.0 | — | Dynamic |

Sets the `user_job_memory_digest_lower_bound` setting in the operation spec. To learn more about the setting, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/scheduler/memory-digest#nastrojki-digest).

### `yt.BufferRowCount`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, minimum 1 | — | Dynamic |

Limit on the number of records that JobProxy can buffer.{% if audience == "internal" %} To learn more, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/jobs).{% endif %}

### `yt.DisableJobSplitting`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Dynamic |

Prohibit the {{product-name}} scheduler from adaptively splitting long-running custom jobs.

### `yt.DefaultLocalityTimeout`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `locality_timeout` setting in the operation spec (the setting isn't yet documented).

### `yt.MapLocalityTimeout`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `map_locality_timeout` setting in the operation spec (the setting isn't yet documented).

### `yt.ReduceLocalityTimeout`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `reduce_locality_timeout` setting in the operation spec (the setting isn't yet documented).

### `yt.SortLocalityTimeout`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Dynamic |

Sets the `sort_locality_timeout` setting in the operation spec (the setting isn't yet documented).

### `yt.MinLocalityInputDataWeight`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Dynamic |

Sets the `min_locality_input_data_weight` setting in the operation spec (the setting isn't yet documented).

### `yt.DefaultMapSelectivityFactor`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive floating point number | — | Dynamic |

Sets the approximate output-input ratio for the map stage in the joint MapReduce operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/mapreduce).

### `yt.SuspendIfAccountLimitExceeded`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Dynamic |

Pause the operation if the "Account limit exceeded" error occurs in jobs. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#common_options).

### `yt.CommonJoinCoreLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 128M | Static |

Sets the memory buffer size for CommonJoinCore node execution (executed in the job when the common JOIN strategy is selected).

### `yt.CombineCoreLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, minimum 1M | 128M | Static |

Sets the memory buffer size for CombineCore node execution.

### `yt.SwitchLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, minimum 1M | 128M | Static |

Sets the memory buffer size for Switch node execution.

### `yt.EvaluationTableSizeLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, maximum 10M | 1M | Static |

Sets the maximum total volume of tables used at the evaluation step.

### `yt.LookupJoinLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, maximum 10M | 1M | Static |

A table may be used as a map in the Lookup JOIN strategy if it doesn't exceed the minimum size specified in `yt.LookupJoinLimit` and `yt.EvaluationTableSizeLimit`.

### `yt.LookupJoinMaxRows`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, maximum 1,000 | 900 | Static |

Maximum number of table rows at which the table may be used as a dictionary in the Lookup JOIN strategy.

### `yt.MaxExtraJobMemoryToFuseOperations`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 2G | Static |

Maximum memory utilization for jobs permitted after operations are merged by optimizers.

### `yt.MaxReplicationFactorToFuseOperations`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Floating point number, minimum 1.0 | 20.0 | Static |

Maximum data replication factor permitted after operations are merged by optimizers.

### `yt.TopSortMaxLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 1000 | Static |

Maximum LIMIT value used together with ORDER BY at which TopSort optimization is launched.

### `yt.TopSortSizePerJob`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes, minimum 1 | 128M | Static |

Sets the expected data volume per job in a TopSort operation.

### `yt.TopSortRowMultiplierPerJob`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Number, minimum 1 | 10 | Static |

Sets the expected number of records per job in a TopSort operation, calculated as `LIMIT * yt.TopSortRowMultiplierPerJob`.

### `yt.DisableOptimizers`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String containing the list of optimizers separated by any of the following symbols: comma, semicolon, space, or `|` | — | Static |

Disables the set optimizers.

### `yt.JobEnv`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets environment variables for map and reduce jobs in operations. Map keys set the environment variable names, and map values set the values for these variables.

### `yt.OperationSpec`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets the operation settings map. Lets you set the settings that have no counterparts in the form of pragmas. Settings that were set via special-purpose pragmas have priority and redefine the values in this map.

### `yt.Annotations`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets arbitrary structured information related to the operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

### `yt.StartedBy`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets the map describing the client that started the operation. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

### `yt.Description`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Sets human-readable information displayed on the operation page in the web interface. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options).

{% if audience == "internal" %}
### `yt.GeobaseDownloadUrl`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Sets a URL for downloading the geobase (geodata6.bin file) if the query uses Geo UDF.
{% endif %}
### `yt.MaxSpeculativeJobCountPerTask`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | — | Dynamic |

Sets the number of [speculatively executed jobs]({{yql.pages.syntax.pragma.speculative-job}}) in {{product-name}} operations. {{product-name}} cluster settings are used by default.

### `yt.LLVMMemSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 256M | Dynamic |

Sets the fixed memory size required for compiling the LLVM code in jobs.

### `yt.LLVMPerNodeMemSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 10K | Dynamic |

Sets the required memory size per computation graph node for compiling the LLVM code in jobs.

### `yt.SamplingIoBlockSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | — | Dynamic |

Sets the minimum size of a block for coarse-grain sampling.

### `yt.BinaryTmpFolder`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Path on cluster | — | Static |

Sets a separate path on the cluster for caching binary query artifacts (UDF and job binary). Artifacts are saved to the directory root with the same name as the artifact's md5. Artifacts are saved and used in this directory outside of transactions even if a `yt.ExternalTx` pragma is set in the query.

### `yt.BinaryExpirationInterval`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Time interval supporting `s/m/h/d` suffixes | — | Static |

Allows management of [TTL for cached binary artifacts]({{yt-docs-root}}/user-guide/storage/cypress#TTL). Only works together with `yt.BinaryTmpFolder`. Each use of a binary artifact in the query extends the lifetime of its TTL.

### `yt.FolderInlineDataLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Bytes | 100K | Static |

Sets the maximum amount of data for the inline list obtained as a result of the Folder computation. If a greater size is selected, a temporary file will be used.

### `yt.FolderInlineItemsLimit`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 100 | Static |

Sets the maximum number of elements in the inline list obtained as a result of the Folder computation. If a greater size is selected, a temporary file will be used.

### `yt.UseNativeYtTypes`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | false | Static |

Allows values of composite data types to be written to tables through native support of composite types in {{product-name}}.

### `yt.PublishedMedia` / `yt.TemporaryMedia`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String representation of a yson map | — | Dynamic |

Set the `@media` attribute for newly created tables. If available, assigns [mediums in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#naznachenie-mediuma), where table chunks will be stored.

Tables specified in [INSERT INTO](insert_into.md) are Published. All other tables are Temporary.

### `yt.PublishedPrimaryMedium` / `yt.TemporaryPrimaryMedium`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Set the `@primary_medium` attribute for newly created tables. If available, assigns the [primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary), where chunks will be recorded. By default, {{product-name}} sets the primary medium to `"default"`.

Tables specified in [INSERT INTO](insert_into.md) are Published. All other tables are Temporary.

### `yt.IntermediateDataMedium`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Set the medium used for intermediate data in operations (Sort, MapReduce). To learn more, see the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/sort).

### `yt.PrimaryMedium`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Dynamic |

Sets the [primary medium in {{product-name}}]({{yt-docs-root}}/user-guide/storage/media#primary) for Published and Temporary tables and intermediate data in operations. Amounts to simultaneous setting of `yt.IntermediateDataMedium`, `yt.PublishedPrimaryMedium`, and `yt.TemporaryPrimaryMedium` pragmas.

### `yt.HybridDqExecution`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Static |

Includes hybrid query execution via DQ

### `yt.NetworkProject`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | - | Dynamic |

Sets the use of a specified network project in jobs. See the [documentation]({{yt-docs-root}}/user-guide/data-processing/operations/mtn).

### `yt.BatchListFolderConcurrency`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 5 | Static |

Sets the number of concurrent directory listing operations.

### `yt.ColumnGroupMode`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String: disable, single or perusage | disable | Static |

Sets the columnar group computation mode for intermediate query tables. In `disable` mode, columnar groups aren't used. In `single` mode, one group is created for all table columns. In `perusage` mode, granular column groups by their consumers are created. All columns of the same group are simultaneously used in one or more consumers. For example, if the intermediate table has columns [a, b, c, d, e, f] and it is used by two operations with column selections [a, b, c, d] and [c, d, e, f] respectively, three column groups ([a, b], [c, d], and [e, f]) will be created in the table. If the intermediate table is used for publishing to an output table (that is, the consumer is the YtPublish node), columnar groups aren't used, except for the explicitly specified [column_groups modifier](insert_into.md#hints). In the latter case, the intermediate table uses the modifier's columnar groups.

### `yt.MinColumnGroupSize`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number not less than 2 | 2 | Static |

Sets the minimum size of a columnar group. If a computed group contains the number of columns that is less than the specified pragma value, the group isn't created.

### `yt.MaxColumnGroups`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Positive number | 64 | Static |

Sets the maximum number of columnar groups per intermediate query table. If the computed number of groups exceeds this limit, groups aren't created for this table.

### `yt.ForceJobSizeAdjuster`

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Flag | true | Static |

Sets the `"force_job_size_adjuster"` option in the operation settings.

