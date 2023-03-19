---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/callable.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/callable.md
---
## Callable {#callable}

Create a callable value with the given signature from a lambda function. Usually used to place callable values in containers.

**Signature**
```
Callable(Type<Callable<(...)->T>>, lambda)->Callable<(...)->T>
```

Arguments:

1. Type.
2. Lambda function.

**Examples:**
```yql
$lambda = ($x) -> {
    RETURN CAST($x as String)
};

$callables = AsTuple(
    Callable(Callable<(Int32)->String>, $lambda),
    Callable(Callable<(Bool)->String>, $lambda),
);

SELECT $callables.0(10), $callables.1(true);
```
