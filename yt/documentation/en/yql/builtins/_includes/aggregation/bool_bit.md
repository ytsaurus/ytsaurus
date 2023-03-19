---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/bool_bit.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/aggregation/bool_bit.md
---
## BOOL_AND, BOOL_OR and BOOL_XOR {#bool-and-or-xor}

**Signature**
```
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Applying the appropriate logical operation (`AND`/`OR`/`XOR`) to all values of a boolean column or expression.

These functions **do not skip** the `NULL` value during aggregation, a single `NULL` value will turn the result into  `NULL`. The `MIN`/`MAX` or `BIT_AND`/`BIT_OR`/`BIT_XOR` functions can be used for aggregation with `NULL` skips.

**Examples**
```yql
SELECT
  BOOL_AND(bool_column),
  BOOL_OR(bool_column),
  BOOL_XOR(bool_column)
FROM my_table;
```

## BIT_AND, BIT_OR, and BIT_XOR {#bit-and-or-xor}

Applying the appropriate bitwise operation to all values of a numeric column or expression.

**Examples**
```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```
