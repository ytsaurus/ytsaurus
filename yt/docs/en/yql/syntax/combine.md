# COMBINE

Groups rows from two input tables by a common key and applies a UDF or [lambda function](expressions.md#lambda) to each group. Unlike [JOIN](join.md), `COMBINE` does not build a Cartesian product of matching rows: the function receives all rows with the same key as two lists and can implement arbitrary matching logic.

## Syntax {#syntax}

```yql
COMBINE input1 AS alias1 [PRESORT presort_expression1 [ASC | DESC], ...]
WITH input2 AS alias2 [PRESORT presort_expression2 [ASC | DESC], ...]
ON alias1.key_expression = alias2.key_expression [AND ...]
USING function(item_expression1, item_expression2)
```

## Availability {#availability}

`COMBINE` is available starting with language version [2026.02](../changelog/2026.02.md).

## Description {#description}

The `ON` clause contains one or more equality conditions joined with `AND`. Multiple predicates produce a composite tuple key, while one predicate produces a scalar key.

For every key present in at least one input, `COMBINE` calls the function with three arguments:

1. The common key from the `ON` clause.
2. A list of values of the first `USING` argument for rows from the first input.
3. A list of values of the second `USING` argument for rows from the second input.

If a key occurs in only one input, the list for the other input is empty. At the group level, `COMBINE` therefore has `FULL JOIN` semantics.

Use `TableRow()` to pass the entire row. Other expressions can select columns or calculate a value. The optional `PRESORT` clause specifies row order within a group; without it, element order is undefined.

The function in `USING` can return the same types as [PROCESS](process.md): a structure, optional structure, list, or stream of structures. The result is converted into a flat table.

{% note info "Note" %}

`COMBINE` is useful when rows with the same key must be processed together without the row multiplication of a regular join, for example when matching time intervals.

{% endnote %}

## Examples {#examples}

```yql
$count_rows = ($key, $left_rows, $right_rows) -> {
    RETURN <|
        key: $key,
        left_count: ListLength($left_rows),
        right_count: ListLength($right_rows)
    |>;
};

COMBINE my_table1 AS L
WITH my_table2 AS R
ON L.key = R.key
USING $count_rows(TableRow(), TableRow());
```

```yql
$zip_rows = ($key, $left_rows, $right_rows) -> {
    RETURN <|
        key: $key.0,
        subkey: $key.1,
        rows: ListZipAll($left_rows, $right_rows)
    |>;
};

COMBINE my_table1 AS L PRESORT L.timestamp
WITH my_table2 AS R PRESORT R.timestamp
ON L.key = R.key AND L.subkey = R.subkey
USING $zip_rows(TableRow(), TableRow());
```
