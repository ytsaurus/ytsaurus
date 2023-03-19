---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/assume_strict.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/assume_strict.md
---
## AssumeStrict {#assumestrict}

**Signature**
```
AssumeStrict(T)->T
```

The `AssumeStrict` function returns its argument. Using this function is a way to tell the YQL optimizer that the expression in the argument is _strict_, i.e. free of runtime errors.
Most of the built-in YQL functions and operators are strict, but there are exceptions like [Unwrap](#optional-ops) and [Ensure](#ensure).
Besides that, a non-strict expression is considered a UDF call.

If you are sure that no runtime errors actually occur when computing the expression, we recommend using `AssumeStrict`.

**Example**
```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE AssumeStrict(Unwrap(CAST(a.key AS Int32))) == 1;
```

In this example, we assume that all values of the `a.key` text column in table `T1` are valid numbers, so Unwrap does not cause an error.
If there is `AssumeStrict`, the optimizer will be able to perform filtering first and then JOIN.
Without `AssumeStrict`, such optimization is not performed: the optimizer must take into account the situation when there are non-numeric values in the `a.key` column which are filtered out by `JOIN`.
