
## Table expressions {#table-contexts}

A table expression is an expression that returns a table. Table expressions in YQL are as follows:
* Subqueries: `(SELECT key, subkey FROM T)`
* [named subqueries](#named-nodes): `$foo = SELECT * FROM T;` (use of a `$foo` named subquery is a table expression)
* [subquery templates](../../subquery.md#define-subquery): `DEFINE SUBQUERY $foo($name) AS ... END DEFINE;` (`$foo("InputTable")` call is a table expression).

Semantics of a table expression depends on the context where it is used. In YQL, table expressions can be used in the following contexts:
* Table context: after [FROM](../../select.md#from).
   In this case, table expressions work as expected: for example, `$input = SELECT a, b, c FROM T; SELECT * FROM $input` returns a table with three columns.
   The table context also appears after [UNION ALL](../../select.md#unionall), [JOIN](../../join.md#join), [PROCESS](../../process.md#process), and [REDUCE](../../reduce.md#reduce);
* the vector context appears after [IN](#in). In this context, the table expression must contain exactly one column (the name of this column doesn't affect the expression result in any way).
   A table expression in a vector context is typed as a list (the type of the list element is the same as the column type in this case). Example: `SELECT * FROM T WHERE key IN (SELECT k FROM T1)`;
* A scalar context arises _in all the other cases_. As in a vector context,
   a table expression must contain exactly one column, but the value of the table expression is a scalar, that is,
   an arbitrarily selected value of this column (if no rows are returned, the result is `NULL`). Example: `$count = SELECT COUNT(*) FROM T; SELECT * FROM T ORDER BY key LIMIT $count / 2`;

The order of rows in a table context, the order of elements in a vector context, and the rule for selecting a value from a scalar context (if multiple values are returned), aren't defined. This order also cannot be affected by `ORDER BY`: `ORDER BY` without `LIMIT` is ignored in table expressions with a warning, and `ORDER BY` with `LIMIT` defines a set of elements rather than the order within that set.


There is an exception to this rule. A named expression containing [PROCESS](../../process.md#process), when used in scalar context, behaves as if it existed in table context:

```yql
$input = SELECT 1 AS key, 2 AS value;
$process = PROCESS $input;

SELECT FormatType(TypeOf($process)); -- $process used in scalar context,
                                     -- but the result of SELECT in this case is - List<Struct<'key':Int32,'value':Int32>>

SELECT $process[0].key; -- returns 1

SELECT FormatType(TypeOf($input)); -- error: $input in scalar context must contain one column
```
{% note warning "Attention!" %}

A common error is to use an expression in a scalar context rather than a table context or vector context. For example:

```yql
$dict = SELECT key, value FROM T1;

DEFINE SUBQUERY $merge_dict($table, $dict) AS
SELECT * FROM $table LEFT JOIN $dict USING(key);
END DEFINE;

SELECT * FROM $merge_dict("Input", $dict); -- $dict here is used in scalar context.
                                           -- an error: exactly one column is expected in a scalar context

```

A correct notation in this case is:

```yql
DEFINE SUBQUERY $dict() AS
SELECT key, value FROM T1;
END DEFINE;

DEFINE SUBQUERY $merge_dict($table, $dict) AS
SELECT * FROM $table LEFT JOIN $dict() USING(key); -- use of the table expression $dict()
                                                   -- (subquery template call) in table context
END DEFINE;

SELECT * FROM $merge_dict("Input", $dict); -- $dict - subquery template (not a table expression)
                                           -- passed as a table expression argument
```

{% endnote %}
