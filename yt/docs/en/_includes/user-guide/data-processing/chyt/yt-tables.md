# Working with {{product-name}} tables

The main task solved by CHYT is processing tables that are already in {{product-name}} without having to copy them to any third-party storages.

Thus, the main difference of CHYT from ClickHouse is that it does not have the variety of engines from regular ClickHouse. Tables are stored according to the logic of [table storage in {{product-name}}](../../../../user-guide/storage/static-tables.md). Creating tables using standard ClickHouse engines is prohibited. Instead, you can read any of the existing tables in the corresponding {{product-name}} cluster. You can also save results as {{product-name}} tables.

"Under the hood", working with {{product-name}} tables is arranged similarly to the [Distributed](https://clickhouse.com/docs/ru/engines/table-engines/special/distributed) engine from ClickHouse. Exception: you do not have to think about manual or automatic distribution of data into shards, because as a storage {{product-name}} solves this task in a transparent, seamless, and reliable way, distributing data to nodes.

CHYT works only with [schematized tables](../../../../user-guide/storage/static-schema.md). The table must have the filled in `/@schema` attribute, or the **Schema** tab in the {{product-name}} web interface must have a schema (i.e. at least one column is mentioned), which is the same thing.

{{product-name}} tables are denoted by their full paths in Cypress wrapped in backticks or in double quotes: `` `//path/to/table` `` or `"//path/to/table"`:

```sql
SELECT * FROM `//tmp/sample_table`
```

CHYT can read and write static and sorted dynamic tables, but note that due to the data storage peculiarities, reading from [dynamic tables](../../../../user-guide/dynamic-tables/overview.md) may take by several times more time than reading from static tables.

## Reading multiple tables { #many }

- Concatenation of several {{product-name}} tables: `concatYtTables(table1, [table2, [...]])`.

   Example:
   ```sql
   SELECT * FROM concatYtTables("//tmp/sample_table", "//tmp/sample_table2")
   ```

- Concatenation of all tables along `cypressPath` (without a slash at the end): `concatYtTablesRange(cypressPath, [from, [to]])`.

   Example:
   ```sql
   SELECT * FROM concatYtTablesRange("//tmp/sample_tables")
   SELECT * FROM concatYtTablesRange('//tmp/sample_tables','2019-01-01')
   SELECT * FROM concatYtTablesRange('//tmp/sample_tables', '2019-08-13T11:00:00')
   ```

## Saving results { #save }

CHYT can create static tables, insert data into them, and save query results in {{product-name}}. Below is a list of supported queries:

* `INSERT INTO … VALUES`;
* `INSERT INTO … SELECT`;
* `CREATE TABLE`;
* `CREATE TABLE AS SELECT`.

Examples:

```sql
-- Inserting data into a table
INSERT INTO `//tmp/sample_table`(a) VALUES (10), (11);
```

```sql
-- Inserting data from another table
INSERT INTO `//tmp/sample_table`
SELECT a+100 FROM `//tmp/sample_table` ORDER BY a;
```

```sql
-- Creating a table
CREATE TABLE IF NOT EXISTS `//tmp/sample_table`
(
   a String,
   b Int32
) ENGINE = YtTable();
```

```sql
-- Creating a table with sorting by key b
CREATE TABLE IF NOT EXISTS `//tmp/sample_table_with_pk`
(
   a String,
   b Int32
) ENGINE = YtTable() order by b;
```

```sql
-- Creating a table based on a select query
CREATE TABLE `//tmp/sample_table` ENGINE = YtTable()
AS SELECT a FROM `//tmp/sample_table` ORDER BY a;
```

```sql
-- Creating a table based on a select query with sorting by column b
CREATE TABLE `//tmp/sample_table_with_pk` ENGINE = YtTable() order by b
AS SELECT b,a FROM `//tmp/sample_table` order by b;
```

To overwrite the existing data, just add the `<append=%false>` option before the path:

Example:

```sql
INSERT INTO `<append=%false>//tmp/sample_table`
SELECT a+100 FROM `//tmp/sample_table` ORDER BY a.
```

When creating a table, you can specify additional attributes. To do this, just pass them to Engine in the form of [YSON](../../../../user-guide/storage/data-types.md#yson).

Example:

```sql
CREATE TABLE `//tmp/sample_table`(i Int64) engine = YtTable('{compression_codec=snappy}');

CREATE TABLE `//tmp/sample_table`(i Int64) engine = YtTable('{optimize_for=lookup}');

CREATE TABLE `//tmp/sample_table`(i Int64) engine = YtTable('{primary_medium=ssd_blobs}').
```

There are restrictions when saving results: default values, expressions for TTL, and compression formats for columns are ignored.

## Table manipulation queries { #ddl }

CHYT supports the following operations on tables:

* `TRUNCATE TABLE [IF EXISTS]`.
* `RENAME TABLE … TO …`;
* `EXCHANGE TABLES … AND …`;
* `DROP TABLE [IF EXISTS]`.

Examples:

```sql
-- Deleting all data from a table
TRUNCATE TABLE `//tmp/sample_table`;
TRUNCATE TABLE IF EXISTS `//tmp/sample_table`;

-- Renaming a table
RENAME TABLE `//tmp/sample_table` TO `//tmp/sample_table_renamed`;

-- Exchanging the names of two tables
EXCHANGE TABLES `//tmp/sample_table` AND `//tmp/other_sample_table`;

-- Deleting a table
DROP TABLE `//tmp/sample_table`;
DROP TABLE IF EXISTS `//tmp/sample_table`;
```

## Virtual columns { #virtual_columns }

Each {{product-name}} table has virtual columns. These columns are not visible in the `DESCRIBE TABLE` and `SELECT * FROM` queries and you can only get them by explicitly accessing them.

3 virtual columns are currently available for each table:
- `$table_index Int64`: The table number in the order of enumeration in `concatYtTables` (or in the sort order in `concatYtTablesRange`). When reading a single table, the index is always zero.
- `$table_path String`: The full path to the table in {{product-name}}.
- `$table_name String`: The table name, i.e. the last literal of the table path.

Input tables can be filtered quite effectively by described virtual columns, since their values are known before reading any data from the tables. This property can be used to read almost any subset of tables from the directory.

However, take into account that although the tables themselves will not be read, the *table metainformation* (for example, the schema) will still be loaded. Besides that, even if the table does not fall under the condition and will not be read, the user's access permissions to all tables specified in `concatYtTables/concatYtTablesRange` will still be checked. Therefore, we recommend specifying restrictions using the arguments of the `concatYtTablesRange` function for directories with a large number of tables.

Query examples:

```sql
SELECT $table_name FROM "//home/dev/username/t1";
-- Result
-- #	$table_name
-- 1	"t1"
```

```sql
SELECT * FROM "//home/dev/username/t1";
--Result
--#	a	b	c
--1	0	1	1
```

```sql
SELECT *, $table_name, $table_path, $table_index
FROM concatYtTables("//home/dev/username/t0", "//home/dev/username/t1");
-- Result
--#	a	b	c	$table_name	$table_path	$table_index
--1	0	0	0	"t0"	"//home/dev/username/t0"	0
--2	0	1	1	"t0"	"//home/dev/username/t0"	0
--3	0	1	1	"t1"	"//home/dev/username/t1"	1
```

## Working with dynamic tables { #dynamic }

You can read [dynamic](../../../../user-guide/dynamic-tables/overview.md) tables, including dynamic stores (fresh data in memory), from CHYT.

{% note warning "Attention!" %}

To make data in dynamic stores visible when reading from CHYT and Map-Reduce, the table must be remounted and the `enable_dynamic_store_read = %true` attribute must be set.

{% endnote %}

By default, if you try to read a dynamic table without the `enable_dynamic_store_read = %true` attribute, the query will end with an error. If you really need data only from chunk stores (with a delay of tens of minutes), then you can set `chyt.dynamic_table.enable_dynamic_store_read` to `0`, after which CHYT will forcibly start reading only chunk stores (see the [query settings](../../../../user-guide/data-processing/chyt/reference/settings.md) page).


