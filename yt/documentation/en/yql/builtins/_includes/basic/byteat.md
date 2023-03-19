---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/byteat.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/byteat.md
---
## ByteAt {#byteat}

Getting the byte value in the string by the index from its beginning. If the index is invalid, `NULL` is returned.

**Signature**
```
ByteAt(String, Uint32)->Uint8
ByteAt(String?, Uint32)->Uint8?

ByteAt(Utf8, Uint32)->Uint8
ByteAt(Utf8?, Uint32)->Uint8?
```

Arguments:

1. String: `String` or `Utf8`.
2. Index: `Uint32`.

**Examples**
```yql
SELECT
    ByteAt("foo", 0), -- 102
    ByteAt("foo", 1), -- 111
    ByteAt("foo", 9); -- NULL
```
