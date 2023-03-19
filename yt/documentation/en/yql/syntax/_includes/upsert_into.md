---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/upsert_into.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/upsert_into.md
---
# UPSERT INTO

UPSERT (which stands for UPDATE or INSERT) updates or inserts multiple rows to a table based on a comparison by the primary key. Missing rows are added. For the existing rows, the values of the specified columns are updated, but the values of the other columns are preserved.

Search for the table by name in the database specified by the [USE](../use.md) operator.

`UPSERT` is the only data modification operation that doesn't need them to be preliminarily read. Due to this, it works faster and is cheaper than other operations.

Column mapping when using `UPSERT INTO ... SELECT` is done by names. Use `AS` to fetch a column with the desired name in `SELECT`.

**Examples**

```yql
UPSERT INTO my_table
SELECT pk_column, data_column1, col24 as data_column3 FROM other_table  
```

```yql
UPSERT INTO my_table ( pk_column1, pk_column2, data_column2, data_column5 )
VALUES ( 1, 10, 'Some text', Date('2021-10-07')),
       ( 2, 10, 'Some text', Date('2021-10-08'))
```
