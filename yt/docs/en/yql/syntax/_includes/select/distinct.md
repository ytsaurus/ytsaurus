## DISTINCT {#distinct}

Selecting unique rows.

{% note info %}

Applying `DISTINCT` to calculated values is not currently implemented. For this purpose, use a subquery or the clause [`GROUP BY ... AS ...`](../../group_by.md).

{% endnote %}

**Example**

```yql
SELECT DISTINCT value -- only unique values from the table
FROM my_table.
```

Also, the `DISTINCT` keyword can be used to apply aggregate functions only to unique values. For more information, see the documentation for [GROUP BY](../../group_by.md).