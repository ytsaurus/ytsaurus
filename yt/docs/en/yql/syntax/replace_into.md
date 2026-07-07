# REPLACE INTO

Writes rows to a dynamic table. For existing keys, it overwrites entire rows; for new keys, it adds values to the table.

To use `REPLACE INTO`, the following conditions must be met:
- The target table must exist.
- The target table must be a sorted dynamic table<!--(../../user-guide/dynamic-tables/sorted-dynamic-tables.md)-->.
- The target table must be mounted<!--(../../user-guide/dynamic-tables/operations.md#mount_table)-->.
- Data being written must be unique by key.
- Data sorting by key is not required. YQL can automatically sort unsorted data using the `sort` operation.

## Description

`REPLACE INTO` supports the same operations as [INSERT INTO](insert_into.md), but with one crucial difference: `REPLACE INTO` allows inserting only into dynamic tables.

You can use it when the data you're inserting only partially overwrites the existing records, and you don't want to wipe them with `INSERT INTO WITH TRUNCATE`.

## Using modifiers

The logic for using modifiers in the same as with [INSERT INTO](insert_into.md).

### Full list of supported write modifiers
* `USER_ATTRS=yson` — Sets user attributes that will be added to the table. Any existing attributes will be overwritten. Attributes must be set as a YSON dictionary.

{% note info %}

We do not recommend using system attributes in `USER_ATTRS`, as applying them to written data can lead to unpredictable or unreliable results.

{% endnote %}

#### Examples

```yql
REPLACE INTO my_dynamic_table
SELECT key FROM my_table_source;

REPLACE INTO my_dynamic_table WITH USER_ATTRS="{attr1=value1; attr2=value2;}"
SELECT key FROM my_table_source;
```
