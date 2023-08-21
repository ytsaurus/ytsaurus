
## TablePath {#tablepath}

Access to the current table name, which might be needed when using [CONCAT](../../../syntax/select.md#concat), [RANGE](../../../syntax/select.md#range), and other related mechanisms.

**Signature**
```
TablePath()->String
```

No arguments. Returns a string with the full path or an empty string and warning when used in an unsupported context (for example, when working with a subquery or a range of 1000+ tables).

{% note info "Note" %}

The [TablePath](#tablepath), [TableName](#tablename), and [TableRecordIndex](#tablerecordindex) functions do no support temporary and anonymous tables (they return an empty string or 0 for [TableRecordIndex](#tablerecordindex)).
These functions are calculated when the `SELECT` projection is [executed](../../../syntax/select.md#selectexec), and the current table might already be temporary at that point.
To avoid such a situation, create a subquery for calculating these functions, as shown in the second example below.

{% endnote %}

**Examples**
```yql
SELECT TablePath() FROM CONCAT(table_a, table_b);
```

```yql
SELECT key, tpath_ AS path FROM (SELECT a.*, TablePath() AS tpath_ FROM RANGE(`my_folder`) AS a)
WHERE key IN $subquery;
```

## TableName {#tablename}

Get the table name based on the table path. You can obtain the path using the [TablePath](#tablepath) function or as the `Path` column when using the table function [FOLDER](../../../syntax/select.md#folder).

**Signature**
```
TableName()->String
TableName(String)->String
TableName(String, String)->String
```

Optional arguments:

* Path to the table, `TablePath()` is used by default (see also its limitations).
* Specifying the system ("yt") whose rules are used to determine the table name. You need to specify the system only if [USE](../../../syntax/select.md#use) doesn't specify the current cluster.

**Examples**
```yql
USE hahn;
SELECT TableName() FROM CONCAT(table_a, table_b);
```

```yql
SELECT TableName(Path, "yt") FROM hahn.FOLDER(folder_name);
```

## TableRecordIndex {#tablerecordindex}

Access to the current sequence number of a row in the physical source table, **starting from 1** (depends on the storage implementation).

**Signature**
```
TableRecordIndex()->Uint64
```

No arguments. When used together with [CONCAT](../../../syntax/select.md#concat), [RANGE](../../../syntax/select.md#range), and other related mechanisms, numbering is reset for each input table. If used in an incorrect context, it returns 0.

**Example**
```yql
SELECT TableRecordIndex() FROM my_table;
```
