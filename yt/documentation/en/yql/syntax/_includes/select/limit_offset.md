## LIMIT and OFFSET {#limit-offset}

`LIMIT` limits the output to the specified number of rows. If the limit value is `NULL` or no `LIMIT` is specified, the output is unlimited.

`OFFSET`: specifies the offset from the beginning (in rows). If the offset value is `NULL` or no `OFFSET` is specified, the null value is used.

**Examples**

```yql
SELECT key FROM my_table
LIMIT 7;
```

```yql
SELECT key FROM my_table
LIMIT 7 OFFSET 3;
```

```yql
SELECT key FROM my_table
LIMIT 3, 7; -- equivalent to previous example
```

```yql
SELECT key FROM my_table
LIMIT NULL OFFSET NULL; -- equivalent to SELECT key FROM my_table
```
