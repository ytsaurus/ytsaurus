---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/case.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/case.md
---
## CASE {#case}

Conditional expressions and branching. It's similar to `if`, `switch` and ternary operators in the imperative programming languages.

If the result of the `WHEN` expression is `true`, the value of the `CASE` expression becomes the result
following the condition, and the rest of the `CASE` expression isn't calculated. If the condition is not met,
all the `WHEN` clauses that follow are checked. If none
of the `WHEN` conditions is fulfilled, the result recorded in the `ELSE` sentence becomes the `CASE` value.
The `ELSE` branch is mandatory in the `CASE` expression. Expressions in `WHEN` are checked sequentially, from top to bottom.

Since its syntax is quite sophisticated, it's often more convenient to use the built-in `IF` function.

**Examples**
```yql
SELECT
  CASE
    WHEN value > 0
    THEN "positive"
    ELSE "negative"
  END
FROM my_table;
```

```yql
SELECT
  CASE value
    WHEN 0 THEN "zero"
    WHEN 1 THEN "one"
    ELSE "not zero or one"
  END
FROM my_table;
```
