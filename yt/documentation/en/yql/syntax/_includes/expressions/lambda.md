---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/lambda.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/lambda.md
---
## Lambda functions {#lambda}

Let you combine multiple expressions into a single callable value.

List arguments in round brackets, following them by the arrow and lambda function body. The body of the lambda consists of an expression enclosed in parentheses or of curly brackets enclosing the optional chain of expressions and assigns [named expressions](#named-nodes) and the output of a call after the `RETURN` keyword in the last expression.

The scope for the lambda body: first the local named expressions, then arguments, then named expressions defined above by the lambda function at the top level of the query.

Only use pure expressions inside the lambda body (those might also be other lambdas, possibly passed through arguments). But you can't use [SELECT](../../select.md), [INSERT INTO](../../insert_into.md) and other top-level statements inside lambdas.

One or more of the last lambda parameters can be marked with a question mark as optional: if they haven't been specified when calling lambda, they are assigned the `NULL` value.

**Examples**

```yql
$f = ($y) -> {
    $prefix = "x";
    RETURN $prefix || $y;
};

$g = ($y) -> ("x" || $y);

$h = ($x, $y?) -> ($x + ($y ?? 0));

SELECT $f("y"), $g("z"), $h(1), $h(2, 3); -- "xy", "xz", 1, 5
```

```yql
-- if the lambda output is calculated as a single expression, you can use a more compact syntax variant:
$f = ($x, $_) -> ($x || "suffix"); -- the second argument isn't used by
SELECT $f("prefix_", "whatever");
```
