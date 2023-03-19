---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/bitops.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/bitops.md
---
## ...Bit {#bitops}

`TestBit()`, `ClearBit()`, `SetBit()`, and `FlipBit()`: Test, clear, set, or flip a bit in an unsigned number by the specified bit sequence number.

**Signatures**
```
TestBit(T, Uint8)->Bool
TestBit(T?, Uint8)->Bool?

ClearBit(T, Uint8)->T
ClearBit(T?, Uint8)->T?

SetBit(T, Uint8)->T
SetBit(T?, Uint8)->T?

FlipBit(T, Uint8)->T
FlipBit(T?, Uint8)->T?
```

Arguments:

1. An unsigned number to perform the desired operation on. TestBit is also implemented for strings.
2. Bit number.

TestBit returns `true/false`. Other functions return a copy of their first argument with the corresponding transformation performed.

**Examples:**
```yql
SELECT
    TestBit(1u, 0), -- true
    SetBit(8u, 0); -- 9
```
