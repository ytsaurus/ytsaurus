# Spark SQL

You can work with {{product-name}} tables from [Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html). This SQL dialect is used for queries in [Query Tracker](../../../../user-guide/query-tracker.md) using SPYT.

When working with {{product-name}}, `yt` is used as a database ID, and `ytTable:/` is used as a file system. The latter can be omitted, so the below pair of queries is equivalent:

```sql
SELECT `key` FROM yt.`ytTable:///home/service/data`
```

```sql
SELECT `key` FROM yt.`//home/service/data`
```

Queries to other systems are executed in the same way as in the original Spark:

```sql
SELECT * FROM json.`s3a://bucket/file.json`
```

{% note info "Note" %}

Spark doesn't support running multiple sequential commands in a single query. All commands (`CREATE`, `INSERT`, and others) must be executed by separate queries.

{% endnote %}

## Working with tables

Creating a table:

```sql
CREATE TABLE yt.`//tmp/users` (
    id INT,
    name STRING
) USING yt
```

```sql
CREATE TABLE yt.`//tmp/users_copy`
    USING yt AS
    SELECT * FROM yt.`//tmp/users`
```

Deleting a table:

```sql
DROP TABLE yt.`//tmp/users`
```

When a query is executed, table metainformation is cached into the Spark session memory to be reused for subsequent accesses. If the table has been modified by external processes after the query, the cached information becomes irrelevant, and new queries will be executed with errors. In this case, you need to clear the cache manually:

```sql
CREATE TABLE yt.`//tmp/users` (id INT, name STRING) USING yt
-- Any changes to the table by external processes
REFRESH TABLE yt.`//tmp/users` -- Clearing cache
SELECT * FROM yt.`//tmp/users`
```

## Working with data

Reading static tables:

```sql
SELECT t1.value
    FROM yt.`//home/service/table1` t1
    JOIN yt.`//home/service/table2` t2
    ON t1.id == t2.id
```

To read dynamic tables, you need to specify a data slice timestamp:

```sql
SELECT * FROM yt.`//home/service/dynamic_data/@latest_version`
```

```sql
SELECT * FROM yt.`//home/service/dynamic_data/@timestamp_XXX`
```

Inserting into an existing table:

```sql
INSERT INTO TABLE yt.`//home/service/copy`
    SELECT * FROM yt.`//home/service/origin`
```

```sql
INSERT OVERWRITE TABLE yt.`//home/service/copy`
    VALUES (-1, "Existed data is overwritten")
```

Apache Spark doesn't support update queries.
