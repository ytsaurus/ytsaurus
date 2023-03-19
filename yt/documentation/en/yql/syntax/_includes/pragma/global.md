---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/pragma/global.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/pragma/global.md
---
## Global {#pragmas}

### AutoCommit {#autocommit}

| Value type | By default |
| --- | --- |
| Flag | false |

Automatically perform [COMMIT](../../select.md#commit) after each expression.

### TablePathPrefix {#table-path-prefix}

| Value type | By default |
| --- | --- |
| String | — |

Add the specified prefix to the cluster table paths. Works on the same principle as merging paths in a file system: supports references to the parent catalog `..` and doesn't require adding a slash to the right. For example,

`PRAGMA TablePathPrefix = "home/yql";
SELECT * FROM test;`

The prefix is not added if the table name is an absolute path (starts with /).

### UseTablePrefixForEach {#use-table-prefix-for-each}

| Value type | By default |
| --- | --- |
| Flag | false |

EACH uses [TablePathPrefix](#table-path-prefix) for each list item.

### Warning {#warning}

| Value type | By default |
| --- | --- |
| 1. Action<br>2. Warning code or "*" | — |

Action:
* `disable`: Disable.
* `error`: Treat as an error.
* `default`: Revert to the default behavior.

The warning code is returned with the text itself (it's displayed on the right side of the web interface).

Example:
`PRAGMA Warning("error", "*");`
`PRAGMA Warning("disable", "1101");`
`PRAGMA Warning("default", "4503");`

In this case, all the warnings are treated as errors, except for the warning `1101` (that will be disabled) and `4503` (that will be processed by default, that is, remain a warning). Since warnings may be added in new YQL releases, use `PRAGMA Warning("error", "*");` with caution (at least cover such queries with autotests).

{% include [issue_protos.md](issue_protos.md) %}

### Greetings {#greetings}

| Value type | By default |
| --- | --- |
| Text | — |

Issue the specified text as the query's Info message.

Example:
`PRAGMA Greetings("It's a good day!");`

### WarningMsg {#warningmsg}

| Value type | By default |
| --- | --- |
| Text | — |

Issue the specified text as the query's Warning message.

Example:
`PRAGMA WarningMsg("Attention!");`

### DqEngine {#dqengine}

| Value type | By default |
| --- | --- |
| disable/auto/force string | "auto" |

When set to "auto", it enables a new compute engine. Computing is made, whenever possible, without creating map/reduce operations. The "force" value unconditionally routes calculations to the new engine.

### SimpleColumns {#simplecolumns}

`SimpleColumns` / `DisableSimpleColumns`

| Value type | By default |
| --- | --- |
| Flag | true |

If using `SELECT foo.* FROM ... AS foo`, delete the `foo.`  prefix from the names of the resulting columns.

It also works for [JOIN](../../join.md), but in this case it may crash if there's a name conflict (which can be resolved through [WITHOUT](../../select.md#without) and renaming columns). For JOIN in SimpleColumns mode, an implicit Coalesce is made for key columns: the query `SELECT * FROM T1 AS a JOIN T2 AS b USING(key)` in the SimpleColumns mode works same as `SELECT a.key ?? b.key AS key, ... FROM T1 AS a JOIN T2 AS b USING(key)`

### CoalesceJoinKeysOnQualifiedAll

`CoalesceJoinKeysOnQualifiedAll` / `DisableCoalesceJoinKeysOnQualifiedAll`

| Value type | By default |
| --- | --- |
| Flag | true |

Controls implicit Coalesce for the key `JOIN` columns in the SimpleColumns mode. If the flag is set, the Coalesce is made for key columns if there is at least one expression in the format `foo.*` or `*` in SELECT: for example, `SELECT a.* FROM T1 AS a JOIN T2 AS b USING(key)`. If the flag is not set, then Coalesce for JOIN keys is made only if there is an asterisk '*' after `SELECT`

### StrictJoinKeyTypes
`StrictJoinKeyTypes` / `DisableStrictJoinKeyTypes`

| Value type | By default |
| --- | --- |
| Flag | false |

If the flag is set, [JOIN](../../join.md) will require a strict match of key types.
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
`2147483648u IN (1, 2147483648u) = True (was False)`

For more information about the `IN` behavior when operands include `NULL`s, see [here](../../expressions.md#in). You can explicitly select the old behavior by specifying the pragma `DisableAnsiInForEmptyOrNullableItemsCollections`. If no pragma is set, then a warning is issued and the old version works.

### AnsiRankForNullableKeys

| Value type | By default |
| --- | --- |
| Flag | false |

Aligns the RANK/DENSE_RANK behavior with the standard if there are optional types in the window sort keys or in the argument of such window functions. It means that:
* The result type is always Uint64 rather than Uint64?;
* NULLs in keys are treated as equal to each other (the current implementation returns NULL).
   You can explicitly select the old behavior by using the `DisableAnsiRankForNullableKeys` pragma. If no pragma is set, then a warning is issued and the old version works.

### AnsiCurrentRow

| Value type | By default |
| --- | --- |
| Flag | false |

If ORDER BY is present, the implicit window frame task is brought into conformity with the standard.
If AnsiCurrentRow is not set, the `(ORDER BY key)` window is equivalent to `(ORDER BY key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
The standard requires that such window behaves as `(ORDER BY key RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
The difference is in `CURRENT ROW` interpretation. In `ROWS` mode, `CURRENT ROW` is interpreted literally: the current string in the partition.
And in `RANGE` mode, the end of the `CURRENT ROW` frame means "the last row in the partition with the sorting key equal to the current row".

### OrderedColumns {#orderedcolumns}
`OrderedColumns` / `DisableOrderedColumns`

Output the [sequence of columns](../../select.md#orderedcolumns) to SELECT/JOIN/UNION ALL and save it when recording the results. The order of columns is undefined by default.

### PositionalUnionAll {#positionalunionall}

Enable the standard column-by-column execution for [UNION ALL](../../select.md#unionall). In this case,
[column ordering](#orderedcolumns) is enabled automatically.

### RegexUseRe2

| Value type | By default |
| --- | --- |
| Flag | false |

Use Re2 UDF instead of Pcre for executing `REGEX`,`MATCH`,`RLIKE`, and SQL operators. Re2 UDF supports correct processing of Unicode symbols, unlike Pcre UDF, which is used by default.

### ClassicDivision

| Value type | By default |
| --- | --- |
| Flag | true |

In the classical version, the result of integer division remains integer (by default).
If disabled, the result is always Double.
ClassicDivision is a [scoped](#pragmascope) setting.

### CheckedOps

| Value type | By default |
| --- | --- |
| Flag | false |

When the mode is enabled, if integers go beyond the limits of the target argument type or result when performing SUM/SUM_IF, `+`,`-`,`*`,`/`,`%` binary operations, or unary operation`-`, then `NULL` is returned.
If disabled, overflow is not checked.
Has no effect on floating point or `Decimal` numbers.
CheckedOps is a [scoped](#pragmascope) setting.

### AllowDotInAlias

| Value type | By default |
| --- | --- |
| Flag | false |

Enable dot in names of result columns. This behavior is disabled by default, since the further use of such columns in JOIN is not fully implemented.

### WarnUnnamedColumns

| Value type | By default |
| --- | --- |
| Flag | false |

Generate a warning if a column name was automatically generated for an unnamed expression in `SELECT` (in the format `column[0-9]+`).

### GroupByLimit

| Value type | By default |
| --- | --- |
| Positive number | 32 |

Increasing the limit on the number of dimensions in [GROUP BY](../../group_by.md).


### GroupByCubeLimit

| Value type | By default |
| --- | --- |
| Positive number | 5 |

Increasing the limit on the number of dimensions in [GROUP BY](../../group_by.md#rollup-cube-group-sets).

Use this option with care, because the computational complexity of the query grows exponentially with the number of dimensions.

