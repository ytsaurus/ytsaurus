---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/optional_ops.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/optional_ops.md
---
## Just, Unwrap, Nothing {#optional-ops}

`Just()`: Change the data type of the value to an [optional](../../../types/optional.md) type derived from the current data type (for example, `T` becomes `T?`).

**Signature**
```
Just(T)->T?
```

**Examples**
```yql
SELECT
  Just("my_string"); --  String?
```

`Unwrap()`: Converting the [optional](../../../types/optional.md) value of the data type to the corresponding non-optional type, raising a runtime error if the data is `NULL`. This means that `T?` becomes `T`.

If the value is not [optional](../../../types/optional.md), the function returns its first argument without change.

**Signature**
```
Unwrap(T?)->T
Unwrap(T?, Utf8)->T
Unwrap(T?, String)->T
```

Arguments:

1. Value to be converted.
2. An optional string with a comment for the error text.

The reverse operation is [Just](#just).

**Examples**
```yql
$value = Just("value");

SELECT Unwrap($value, "Unexpected NULL for $value");
```

`Nothing()`: Create an empty value for the specified [Optional](../../../types/optional.md) data type.

**Signature**
```
Nothing(Type<T?>)->T?
```

**Examples**
```yql
SELECT
  Nothing(String?); -- A NULL value with the String? type
```

[Learn more about ParseType and other functions for data types](../../types.md).
