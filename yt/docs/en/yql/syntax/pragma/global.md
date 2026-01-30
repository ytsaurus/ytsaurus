# Global

## Scope {#pragmascope}

Unless otherwise specified, a global pragma affects all the subsequent expressions up to the end of the module where it's used. If necessary and logically possible, you can change the value of this setting several times within a given query to make it different at different execution steps.

There are also special scoped pragmas with the scope defined by the same rules as the scope of [named expressions](../expressions.md#named-nodes). Their scope is limited to certain SQL blocks, including ACTION, SUBQUERY, and lambda.

Unlike scoped pragmas, global pragmas can only be used in the global scope (not inside [DEFINE ACTION](../action.md#define-action) and [DEFINE SUBQUERY](../subquery.md#define-subquery)).

## AutoCommit {#autocommit}

| Value type | By default |
| --- | --- |
| Flag | false |

Automatically perform [COMMIT](../commit.md) after each expression.

## TablePathPrefix {#table-path-prefix}

| Value type | By default |
| --- | --- |
| String | — |

Add the specified prefix to the cluster table paths. Works on the same principle as merging paths in a file system: supports references to the parent catalog `..` and doesn't require adding a slash to the right. For example,

```yql
PRAGMA TablePathPrefix = "home/yql";
SELECT * FROM test;
```

The prefix is not added if the table name is an absolute path (starts with /).

## UDF {#udf}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String | — | Static |
| String, the name of the prefix appended to all modules | "" | Static |

Importing all UDFs from the specified library. For the pragma to work, the library must be attached to the query using the ![ico](../../../../images/qt-ui-attach.png =30x30) icon. Please note that the library must be shared library (.so) and compiled for Linux x64.

When setting a prefix, it's appended before the names of all loaded modules, e.g. CustomPrefixIp::IsIPv4 instead of Ip::IsIPv4. Setting the prefix lets you use  different versions of the same UDF.

## RuntimeLogLevel {#runtime-log-level}

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| String, one of the following: `Trace`, `Debug`, `Info`, `Notice`, `Warn`, `Error`, or `Fatal` | `Info` | Static |

Lets you change the computation logging level (for example, for UDFs) at query runtime or at the UDF signature declaration stage.

## UseTablePrefixForEach {#use-table-prefix-for-each}

| Value type | By default |
| --- | --- |
| Flag | false |

`EACH`/`PARTITION_LIST` use [TablePathPrefix](#table-path-prefix) for each list item.

## Warning {#warning}

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

## Greetings {#greetings}

| Value type | By default |
| --- | --- |
| Text | — |

Issue the specified text as the query's Info message.

Example: `PRAGMA Greetings("It's a good day!");`

## WarningMsg {#warningmsg}

| Value type | By default |
| --- | --- |
| Text | — |

Issue the specified text as the query's Warning message.

Example: `PRAGMA WarningMsg("Attention!");`

## DqEngine {#dqengine}

| Value type | By default |
| --- | --- |
| disable/auto/force string | "auto" |

When set to "auto", it enables a new compute engine. Computing is made, whenever possible, without creating map/reduce operations. The "force" value unconditionally routes computations to the new engine.

## SimpleColumns {#simplecolumns}

`SimpleColumns` / `DisableSimpleColumns`

| Value type | By default |
| --- | --- |
| Flag | true |

If using `SELECT foo.* FROM ... AS foo`, delete the `foo.`  prefix from the names of the resulting columns.

It also works for [JOIN](../join.md), but then it may crash if there's a name conflict (which can be resolved through [WITHOUT](../select/without.md) and renaming columns). For JOIN in SimpleColumns mode, an implicit Coalesce is made for key columns: the query `SELECT * FROM T1 AS a JOIN T2 AS b USING(key)` in the SimpleColumns mode works same as `SELECT a.key ?? b.key AS key, ... FROM T1 AS a JOIN T2 AS b USING(key)`

## CoalesceJoinKeysOnQualifiedAll

`CoalesceJoinKeysOnQualifiedAll` / `DisableCoalesceJoinKeysOnQualifiedAll`

| Value type | By default |
| --- | --- |
| Flag | true |

Controls implicit Coalesce for the key `JOIN` columns in the SimpleColumns mode. If the flag is set, key columns are coalesced if there is at least one expression in the format `foo.*` or `*` in SELECT: for example, `SELECT a.* FROM T1 AS a JOIN T2 AS b USING(key)`. If the flag isn't set, JOIN keys are coalesced only if there is an asterisk '*' after `SELECT`.

## StrictJoinKeyTypes

`StrictJoinKeyTypes` / `DisableStrictJoinKeyTypes`

| Value type | By default |
| --- | --- |
| Flag | false |

If the flag is set, [JOIN](../join.md) will require a strict match of key types.
By default, `JOIN` preconverts keys to a shared type, which might result in performance degradation.

`StrictJoinKeyTypes` is a [scoped](#pragmascope) setting.


## AnsiInForEmptyOrNullableItemsCollections

| Value type | By default |
| --- | --- |
| Flag | false |

This pragma brings the behavior of the `IN` operator in accordance with the standard when there's `NULL` in the left or right side of `IN`. The behavior of `IN` when on the right side there is a Tuple with elements of different types also changed. Examples:

`1 IN (2, 3, NULL) = NULL (was Just(False))`
`NULL IN () = Just(False) (was NULL)`
`(1, null) IN ((2, 2), (3, 3)) = Just(False) (was NULL)`

For more information about the `IN` behavior when operands include NULLs, see [here](../expressions.md#in). You can explicitly select the old behavior by specifying the pragma `DisableAnsiInForEmptyOrNullableItemsCollections`. If no pragma is set, you'll see a warning, and the original variant will be used.

## AnsiRankForNullableKeys

| Value type | By default |
| --- | --- |
| Flag | false |

Aligns the ``RANK`/`DENSE_RANK` behavior with the standard if there are optional types in the window sort keys or in the argument of such window functions. It means that:

* The result type is always Uint64 rather than Uint64?;
* NULLs in keys are treated as equal to each other (the current implementation returns NULL).

You can explicitly select the old behavior by specifying the pragma `DisableAnsiRankForNullableKeys`. If no pragma is set, you'll see a warning, and the original variant will be used.

## AnsiCurrentRow

| Value type | By default |
| --- | --- |
| Flag | false |

Aligns the implicit setting of a window frame with the standard if there is [ORDER BY](../select/order_by.md).

If `AnsiCurrentRow` is not set, the `(ORDER BY key)` window is equivalent to `(ORDER BY key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`. The standard requires that such window behaves as `(ORDER BY key RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

The difference is in `CURRENT ROW` interpretation. In `ROWS` mode, `CURRENT ROW` is interpreted literally: the current string in the partition.
And in `RANGE` mode, the end of the `CURRENT ROW` frame means "the last row in the partition with the sorting key equal to the current row".

## OrderedColumns {#orderedcolumns}

`OrderedColumns` / `DisableOrderedColumns`

Preserve the [order of columns](../select/index.md#orderedcolumns) to `SELECT` / `JOIN` / `UNION [ALL]` / `INTERSECT [ALL]` / `EXCEPT [ALL]` and save it when writing the results. The order of columns is undefined by default.

## PositionalUnionAll {#positionalunionall}

Enable the standard columnar execution for [UNION [ALL]](../select/operators.md#union), [INTERSECT [ALL]](../select/operators.md#intersect), and [EXCEPT [ALL]](../select/operators.md#except). This automatically enables [ordered columns](#orderedcolumns).

## RegexUseRe2

| Value type | By default |
| --- | --- |
| Flag | false |

Use Re2 UDF instead of Pcre for executing `REGEX`,`MATCH`,`RLIKE`, and SQL operators. Re2 UDF supports correct processing of Unicode symbols, unlike Pcre UDF, which is used by default.

## ClassicDivision

| Value type | By default |
| --- | --- |
| Flag | true |

In the classical version, the result of integer division remains integer (by default). If disabled, the result is always Double.

`ClassicDivision` is a [scoped](#pragmascope) setting.

## CheckedOps

| Value type | By default |
| --- | --- |
| Flag | false |

When enabled: if aggregate functions SUM/SUM_IF, binary operations `+`, `-`, `*`, `/`, `%`, or unary operation `-` on integers result in an overflow beyond the range of the target argument or result type, return `NULL`. When disabled: overflow isn't checked.

Has no effect on floating point or `Decimal` numbers.

`CheckedOps` is a [scoped](#pragmascope) setting.

## UnicodeLiterals

`UnicodeLiterals`/`DisableUnicodeLiterals`

| Value type | By default |
| --- | --- |
| Flag | false |

When the mode is enabled, string literals without suffixes like "foo"/'bar'/@@multiline@@ will have the `Utf8` type. If disabled, then `String`.

`UnicodeLiterals` is a [scoped](#pragmascope) setting.

## WarnUntypedStringLiterals

`WarnUntypedStringLiterals`/`DisableWarnUntypedStringLiterals`

| Value type | By default |
| --- | --- |
| Flag | false |

When the mode is enabled, string literals without a suffix like "foo"/'bar'/@@multiline@@ will prompt a warning. It can be suppressed if you explicitly select the `s` suffix for the `String` type or the `u` suffix for the `Utf8` type.

`WarnUntypedStringLiterals` is a [scoped](#pragmascope) setting.

## SimplePg

`SimplePg`/`DisableSimplePg`

| Value type | By default |
| --- | --- |
| Flag | false |

When the mode is enabled, all functions from the [SimplePg module](../../udf/list/simple_pg.md) are imported into the global function space.
When the mode is disabled, these functions require the `SimplePg::` prefix to be called.

`SimplePg` is a [scoped](#pragmascope) setting.

## AllowDotInAlias

| Value type | By default |
| --- | --- |
| Flag | false |

Enable dot in names of result columns. This behavior is disabled by default, since the further use of such columns in `JOIN` is not fully implemented.

## WarnUnnamedColumns

| Value type | By default |
| --- | --- |
| Flag | false |

Generate a warning if a column name (in the format `column[0-9]+`) was automatically generated for an unnamed expression in `SELECT`.

## GroupByLimit

| Value type | By default |
| --- | --- |
| Positive number | 32 |

Increasing the limit on the number of groups in [GROUP BY](../group_by.md).

## GroupByCubeLimit

| Value type | By default |
| --- | --- |
| Positive number | 5 |

Increasing the limit on the number of dimensions in [GROUP BY](../group_by.md#rollup-cube-group-sets).

Use this option with care: the computational complexity of the query grows exponentially with the number of dimensions.
