---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/likely.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/likely.md
---
## Likely {#likely}

**Signature**
```
Likely(Bool)->Bool
Likely(Bool?)->Bool?
```

The `Likely` function returns its argument. This function is a hint for the optimizer that tells it that in most cases its argument will be `True`.
For example, if such a function is used in `WHERE`, it means that the filter is weekly selective.

**Example**
```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE Likely(a.amount > 0)  -- This condition is almost always true
```

When you use `Likely`, the optimizer won't attempt to perform filtering before `JOIN`.
