---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/current_utc.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/current_utc.md
---
## CurrentUtc... {#current-utc}

`CurrentUtcDate()`, `CurrentUtcDatetime()` and `CurrentUtcTimestamp()`: Getting the current date and/or time in UTC. The result data type is specified at the end of the function name.

**Signatures**
```
CurrentUtcDate(...)->Date
CurrentUtcDatetime(...)->Datetime
CurrentUtcTimestamp(...)->Timestamp
```

The arguments are optional and work the same as [RANDOM](#random).

**Examples**
```yql
SELECT CurrentUtcDate();
```
```yql
SELECT CurrentUtcTimestamp(TableRow()) FROM my_table;
```
