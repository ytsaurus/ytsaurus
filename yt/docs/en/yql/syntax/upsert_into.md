
# UPSERT INTO

UPSERT (which stands for UPDATE or INSERT) updates or inserts multiple rows to a table based on a comparison by the primary key. Missing rows are added. For the existing rows, the values of the specified columns are updated, but the values of the other columns are preserved.

Search for the table by name in the database specified by the [USE](use.md) operator.

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



## UPSERT in {{product-name}}

{% note warning "Attention!" %}

This operation is not supported in {{product-name}} itself. However, it can be used for publishing data in Statface.

{% endnote %}

{% if audience == internal %}

In this case, a specified scale of (an existing) report acts as a table, the report's dimensions being a collection of key columns, and the (`stat_beta` or `stat`) cluster defines the Statface interface that will be used for publication.

{% endif %}

**Examples:**

```yql
UPSERT INTO stat.`Adhoc/My/Report/daily`
SELECT fielddate, hits FROM my_table_source;
```

```yql
UPSERT INTO
  stat_beta.`Adhoc/My/Report/daily` (fielddate, hits)
VALUES
  ("2018-09-01", 1),
  ("2018-09-02", 2);
```

{% note warning "Attention!" %}

We recommended pretesting corrections on a test interface to avoid data loss when production calculations are corrected.

{% endnote %}

### Removing data from a report before uploading

By default, `UPSERT INTO` performs point-specific record updates in a report in accordance with the set of report dimensions.

{% if audience == internal %}

You can also remove all records with values from a certain dimension set from a report before uploading. To do this, just add the following modifier: `UPSERT INTO ... ERASE BY (A, B, C)`, where `A, B, C` define the required dimension set.
To learn more about semantics, read the [replace_mask](https://wiki.yandex-team.ru/statbox/statface/externalreports/#replacemask) parameter description in Statface documentation.

{% endif %}


**Examples:**
```yql
UPSERT INTO stat.`Adhoc/My/Report/daily` ERASE BY (fielddate)
SELECT fielddate, country, hits FROM my_table_source;
```

In this example, all data related to newly calculated calculation days (where the fielddate field for the daily scale contains days) will be removed. After this, the calculated data will be uploaded.

```yql
UPSERT INTO stat.`Adhoc/My/Report/daily` ERASE BY (fielddate, country)
SELECT fielddate, country, hits FROM my_table_source;
```

In this case, a full dimension set is specified for the report. Thus, in terms of semantics, the behavior is the same as the default behavior of the `UPSERT INTO` operation and there's no need to specify a modifier.
