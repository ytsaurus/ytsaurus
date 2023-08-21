
## COALESCE {#coalesce}

Goes through the arguments from left to right and returns the first non-empty argument found. For the result to be guaranteed non-empty (not [optional type](../../../types/optional.md)), the rightmost argument must be of this type (a literal is often used). If there is one argument, returns it without change.

**Signature**
```
COALESCE(T?, ..., T)->T
COALESCE(T?, ..., T?)->T?
```

Enables you to pass potentially empty values to functions that cannot process them.

A short write format in the form of the `??` operator which has a low priority (below boolean operations) is available. The `NVL` alias can be used.

**Examples**
```yql
SELECT COALESCE(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

```yql
SELECT
  maybe_empty_column ?? "it's empty!"
FROM my_table;
```

```yql
SELECT NVL(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

<span style="color: gray;">(all three examples above are equivalent)</span>
