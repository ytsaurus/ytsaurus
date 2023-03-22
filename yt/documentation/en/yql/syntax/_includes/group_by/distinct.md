
## DISTINCT {#distinct}

Applying aggregate functions only to distinct values of the column.

{% note info %}

Applying `DISTINCT` to calculated values is not currently implemented. For this purpose, use a [subquery](../../select.md#from) or the clause `GROUP BY ... AS ...`.

{% endnote %}

**Example**

```sql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- top 3 keys based on the number of unique values
FROM my_table
GROUP BY key
ORDER BY count DESC
LIMIT 3.
```

You can also use `DISTINCT` to fetch unique rows using [`SELECT DISTINCT`](../../select.md#distinct).

