---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/named-nodes.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/named-nodes.md
---
## Named expressions {#named-nodes}

Complex queries may be sophisticated, containing lots of nested levels and/or repeating parts. In YQL, you can use named expressions to assign a name to an arbitrary expression or subquery. Named expressions can be referenced in other expressions or subqueries. In this case, the original expression/subquery is actually substituted at point of use.

A named expression is defined as follows:
```
<named-expr> = <expression> | <subquery>;
```
Here `<named-expr>` consists of a $ character and an arbitrary non-empty identifier (for example, `$foo`).

If the expression on the right is a tuple, you can automatically unpack it by specifying several named expressions separated by commas on the left:
```
<named-expr1>, <named-expr2>, <named-expr3> ... = <expression-returning-tuple>;
```
In this case, the number of expressions must match the tuple size.

Each named expression has a scope. It starts immediately after defining the named expression and ends at the end of the closest enveloping scope of names (e.g. at the end of the query or at the end of the [lambda function](#lambda) body, [ACTION](../../action.md#define-action), [SUBQUERY](../../subquery.md#define-subquery) or [EVALUATE FOR](../../action.md#evaluate-for)) cyclic path.
Redefining a named expression with the same name hides the previous expression from the current scope.

If the named expression has never been used, a warning is issued. To avoid such a warning, use the underscore as the first character in the ID (for example, `$_foo`).
`$_` named expression is called an anonymous named expression and processed in a special way. It works as if `$_` was automatically replaced with `$_<some_uniq_name>`.
Anonymous named expressions are convenient when you don't need the expression value. For example, to fetch the second element from a tuple of three elements, you can write:
```yql
$_, $second, $_ = AsTuple(1, 2, 3);
select $second;
```

An attempt to reference an anonymous named expression results in an error:
```yql
$_ = 1;
select $_; --- error: Unable to reference anonymous name $_
export $_; --- error: Can not export anonymous name $_
```
Also, you can't import a named expression using an anonymous alias:
```yql
import utils symbols $sqrt as $_; --- error: Can not import anonymous name $_
```
Anonymous argument names are also supported for [lambda functions](#lambda), [ACTION](../../action.md#define-action), [SUBQUERY](../../subquery.md#define-subquery) and in [EVALUATE FOR](../../action.md#evaluate-for).

{% note info %}

If named expression substitution results in completely identical subgraphs in the query execution graph, the graphs are combined to execute a subgraph only once.

{% endnote %}

**Examples**

```yql
$multiplier = 712;
SELECT
  a * $multiplier, -- $multiplier is 712
  b * $multiplier,
  (a + b) * $multiplier
FROM abc_table;
$multiplier = c;
SELECT
  a * $multiplier -- $multiplier is column c
FROM abc_table;
```

```yql
$intermediate = (
  SELECT
    value * value AS square,
    value
  FROM my_table
);
SELECT a.square * b.value
FROM $intermediate AS a
INNER JOIN $intermediate AS b
ON a.value == b.square;
```

```yql
$a, $_, $c = AsTuple(1, 5u, "test"); -- tuple unpacking
SELECT $a, $c;
```

```yql
$x, $y = AsTuple($y, $x); -- swap expression values
```
