---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/flatten/flatten_by.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/flatten/flatten_by.md
---
# FLATTEN

## FLATTEN BY {#flatten-by}

Converts the rows in the source table using vertical unpacking of [containers](../../../types/containers.md) of variable length (lists or dictionaries).

For example:

* Original table:
   |[a, b, c]|1|
   | --- | --- |
   |[d]|2|
   |[]|3|

* The table resulting from `FLATTEN BY` on the left column:
   |a|1|
   | --- | --- |
   |b|1|
   |c|1|
   |d|2|


**Example**
```(sql)
$sample = AsList(
    AsStruct(AsList('a','b','c') AS value, CAST(1 AS Uint32) AS id),
    AsStruct(AsList('d') AS value, CAST(2 AS Uint32) AS id),
    AsStruct(AsList() AS value, CAST(3 AS Uint32) AS id)
);

SELECT value, id FROM as_table($sample) FLATTEN BY (value);
```

This conversion can be convenient in the following cases:
* When you need to display statistics (e.g. via [`GROUP BY`](../../group_by.md)) based on the container column cells.
* When the container column cells store identifiers from another table that must be joined via [`JOIN`](../../join.md).

**Syntax**

* `FLATTEN BY` is specified after `FROM`, but before `GROUP BY`, if `GROUP BY` is present in the query.
* The type of the result column depends on the type of the source column:

| Container type | Result type | Comment |
| --- | --- | --- |
| `List<X>` | `X` | List cell type |
| `Dict<X,Y>` | `Tuple<X,Y>` | Tuple of two elements containing key-value pairs |
| `Optional<X>` | `X` | The result is almost equivalent to the clause `WHERE foo IS NOT NULL`, but the `foo` column type is changed to `X` |

* By default, the result column replaces the source column. Use `FLATTEN BY foo AS bar` to keep the source container. As a result, the source container is still available as `foo` and the output container is available as `bar`.
* To build a Cartesian product of multiple container columns, use the clause `FLATTEN BY (a, b, c)`. Parentheses are mandatory to avoid grammar conflicts.
* Inside `FLATTEN BY`, you can only use column names from the input table. To apply `FLATTEN BY` to the calculation result, use a subquery.
* In `FLATTEN BY` you can use both columns and arbitrary named expressions (unlike columns, `AS` is required in this case). To avoid grammatical ambiguities of the expression after `FLATTEN BY`, make sure to use parentheses with the following: `... FLATTEN BY (ListSkip(col, 1) AS col) ...`
* If the source column had nested containers, for example, `List<Dict<X,Y>>`, `FLATTEN BY` unpacks only the outer level. To completely unpack the nested containers, use a subquery.

{% note info %}

`FLATTEN BY` interprets [optional data types](../../../types/optional.md) as lists with a length of 0 or 1. The table rows with `NULL` are skipped, and the column type changes to a similar non-optional type.

`FLATTEN BY` performs only one conversion at a time. This is why you should use `FLATTEN LIST BY` or `FLATTEN OPTIONAL BY` on optional containers, e.g. `Optional<List<String>>`.

{% endnote %}