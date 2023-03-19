---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/nanvl.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/nanvl.md
---
## NANVL {#nanvl}

Replaces the values `NaN` (not a number) in the expressions that have the type `Float`, `Double`, or [Optional](../../../types/optional.md).

**Signature**
```
NANVL(Float, Float)->Float
NANVL(Double, Double)->Double
```

Arguments:

1. The expression where you want to make a replacement.
2. The value to replace `NaN`.

If one of the arguments is `Double`, the result `isDouble`, otherwise, it's `Float`. If one of the arguments is `Optional`, then the result is `Optional`.

**Examples**
```yql
SELECT
  NANVL(double_column, 0.0)
FROM my_table;
```
