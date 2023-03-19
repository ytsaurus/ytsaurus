---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/execution.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/select/execution.md
---
## SELECT execution procedure {#selectexec}

The `SELECT` query result is calculated as follows:

* a set of input tables is defined and expressions after [FROM](#from) are calculated.
* [SAMPLE](#sample)/[TABLESAMPLE](#sample) is applied to input tables.
* [FLATTEN COLUMNS](../../flatten.md#flatten-columns) or [FLATTEN BY](../../flatten.md) is executed. Aliases set in `FLATTEN BY` become visible after this point.
* all [JOIN](../../join.md)s are executed.
* Add to (or replace in) the data the columns listed in [GROUP BY ... AS ...](../../group_by.md);
* [WHERE](#where) &mdash; is executed. All data that don't match the predicate are filtered out.
* [GROUP BY](../../group_by.md) is executed and aggregate function values are calculated.
* [HAVING](../../group_by.md#having) is filtered.
* [window function](../../window.md) values are calculated.
* Evaluate expressions in `SELECT`;
* Assign names set by aliases to expressions in `SELECT`.
* top-level [DISTINCT](#distinct) is applied to columns obtained this way.
* all subqueries in [UNION ALL](#unionall) are calculated the same way, then they get joined (see [PRAGMA AnsiOrderByLimitInUnionAll](../../pragma.md#pragmas)).
* sorting based on [ORDER BY](#order-by) is performed.
* [OFFSET and LIMIT](#limit-offset) are applied to the obtained result.
