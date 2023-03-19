# Ordered dynamic tables

This section reviews the structure of ordered dynamic tables and describes the actions that can be performed with this type of table.

## Data model { #model }

Ordered dynamic tables are a table type in {{product-name}} that is a simple ordered sequence of rows. Each row of an ordered table consists of columns and is subordinate to the table schema specified in the process of creation. There are no key columns in such tables.

Ordered dynamic tables support adding new rows to the end within a transaction, as well as reading rows by their indexes without transaction isolation.

The closest analog of ordered dynamic tables is [Apache Kafka](https://kafka.apache.org).

As with [sorted dynamic tables](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md), the key space of an ordered dynamic table is divided into [tablets](../../../user-guide/dynamic-tables/overview.md#tablets).

Dividing data into tablets is random. Each tablet contains an ordered sequence of table rows. When the rows are written to the tablet, they get to the end of this sequence. That way, orderliness is only guaranteed within the tablet.

An ordered dynamic table initially consists of one tablet. Multiple tablets can be changed using the `reshard_table` command. It enables you to change the structure for a set of consecutive tablets. A new number of tablets to replace the original tablets is specified in ordered dynamic tables when they are resharded. Specifying `pivot_keys` is not required. Existing data is redistributed between the new tablets in an unspecified way.

## Supported operations { #methods }

### Creating

To create an ordered dynamic table, run the `create table` command and specify the schema and `dynamic=True` setting in the attributes. The schema must match the schema of the ordered table: there must be no key columns among its columns.

CLI
```bash
yt create table //path/to/table --attributes \
'{dynamic=%true;schema=[{name=first_name;type=string};{name=last_name;type=string}]}'
```

### Writing rows

The `insert_rows` command is used to write to an ordered dynamic table. By default, data is written to a random mounted tablet. To manage data distribution in manual mode, you can use a special `$tablet_index` system column of the `int64` type in the rows to be written. The values in this column must be numbers from 0 to N − 1 where N is the number of tablets in the table. The relevant rows will be written strictly to the specified tablet. Rows for which there is no such annotation will be written to a random mounted tablet.

The write within a transaction is transactional: if the transaction succeeds, the rows appear in the relevant tables; if not, they do not. You can handle both sorted and ordered tables in a single transaction.

### Reading rows

You can read data from ordered tables using an SQL-like query language and the `select_rows` command. Each ordered dynamic table appears as a sorted one with system key columns `($tablet_index, $row_index)` (both of the `int64` type) and all data columns specified in the table schema.

For example, this is what a query that reads a range of rows from a fixed tablet of an ordered table looks like:

```sql
* from [//path/to/table] where [$tablet_index] = 10 and [$row_index] between 100 and 200
```

### Changing a table schema and type

You can change the schema of an existing dynamic table using the `alter-table` command. For the command to be successful, the table must be [unmounted](../../../user-guide/dynamic-tables/overview.md#mount_table) and the new schema must be compatible with the old one. There are no changes to the data written to the disk, because the old data is suitable for the new schema.

Use `alter-table` to convert a dynamic table into a static table. For more information, see [MapReduce for dynamic tables](../../../user-guide/dynamic-tables/mapreduce.md#convert_table).

### Trim

{% note warning "Attention!" %}

Use of `reshard` together with `trim` is prohibited, because it can lead to unforeseen consequences.

{% endnote %}

In general, data cannot be deleted from an ordered dynamic table. But there is an exception: you can delete the starting row segment in each tablet . To do this, use the `trim_rows` command. The table path and tablet number are transmitted to it as arguments, as well as the `trimmed_row_count` argument showing how many rows in the table will be deleted after the command is executed. The row numbering is retained. For example, when first called with `trimmed_row_count = 10`, rows with numbers from 0 to 9 inclusive will be deleted. Then, when called with `trimmed_row_count = 30` — rows from 10 to 29 inclusive, etc., `trimmed_row_count` does not have a relative sense, but an absolute one and indicates not the number of rows that will be additionally deleted in case of the next call, but which initial rows will be deleted after the call.

The `trim_rows` command is executed outside of transactions. Once it is complete, the deleted data can no longer be read by the `select_rows` command. As soon as it appears that so many rows were deleted in the tablet that they form an entire initial chunk, the cluster node serving the tablet sends a signal to the master server and the given chunk is deleted as a whole. This is the moment when disk space is freed up.

You can find out the number of deleted rows in any tablet from the `trimmed_row_count` attribute of the tablet. This parameter is updated asynchronously, i.e. some time may elapse between `trim_rows` execution and its change.

When a tablet is unmounted and then mounted, the number of initial deleted rows is retained, ensuring that the numbering is unchanged.

{% note warning %}

When you convert a dynamic table into a static table using the `alter_table` command, information about which rows were deleted is lost. The order of rows in the table is lost as well. The order of rows within the chunks remains unchanged, but it is difficult to rely on this. Indeed, some initial rows of some chunks may be marked as deleted in a dynamic table, and this information cannot be retained when the table is converted into a static table. As a result, when converting a dynamic table into a static table, some of the previously deleted rows may appear in the table.

{% endnote %}

### Resharding

If the number of tablets increases during sharding, the existing tablets are not changed and new tablets are created empty. If the number of tablets needs to be reduced during resharding, the tablets at the end of the range are merged with the last of the resulting list of tablets that is not in the range.

The system tries to retain the invariant "what is deleted is no longer available". As the system adds the chunks from the deleted tablets to the end of the last retained tablet during resharding, it is difficult to perform resharding if there are deleted rows in the deleted tablets. Successful completion of resharding requires that the deleted rows form a chunk prefix in the tablets to be deleted.

In practice, this limitation is difficult to observe, so you can consider that resharding with a reduced number of tablets is impossible if there are deleted rows in the tablets to be deleted. To bypass this limitation, you can use two `alter_table` calls, first making the table static and then dynamic. Remember that some of the deleted rows will become visible in the table again when you use this method.

### Automatic deletion of old rows (TTL)

The same old data deletion settings apply to ordered tables as to sorted tables. For more information, see [Deleting old data](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#remove_old_data). The significant difference is that a row always has only one version in ordered tables. The cleanup settings can then be interpreted as follows:

- If `min_data_versions > 0` (the default value is 1), no automatic deletion occurs.
- {{product-name}} does not delete rows written less than `min_data_ttl` before the current moment.
- If `max_data_versions = 0`, you can delete rows written later than `min_data_ttl`.
- If `max_data_versions > 0`, you can delete rows written later than `max_data_ttl`.

{% note info "Note" %}

Automatic deletion (`trim`) applies to the entire chunk. As long as there are rows in the chunk that cannot be deleted, all rows from the chunk will be available.

{% endnote %}

## $timestamp column

A special `$timestamp` system column of the `uint64` type can be specified in the ordered dynamic table schema. The value in this column is automatically generated by the system when writing and is equal to `commit timestamp` for the transaction in which the rows were added to the table.

## $cumulative_data_weight column

A special `$cumulative_data_weight` system column of the `uint64` type can be specified in the ordered dynamic table schema. The value in this column is generated automatically when you write. It is equal to the total logical weight in bytes of rows in the tablet, counting from the initial row with index zero to the current one, inclusive. The weight of the `$cumulative_data_weight` column itself is also counted in this value.

When you add this column to the schema of an existing table (via unmounting and the `alter_table` query), the initial value of `$cumulative_data_weight` is taken from the table chunk metadata.

## Change visibility, strong/weak commit ordering

The consistency level of ordered dynamic tables is fundamentally lower than that of sorted dynamic tables: committed data may not generally be visible immediately after a commit and may not be added in the order in which the commits occurred.

When a distributed commit is used — and several tablet cells are involved — direct appearance of data in the tablets of different tablet cells occurs at different points in time. Indeed, the second phase of the protocol (commit) is performed by physically distributed participants. Moreover, the same tablet cell may be a participant of several distributed transactions, and the values of their `commit-ts` and the actual sequence in which that tablet cell performs a commit are generally not subject to any conditions. The participant can perform the commit of the `A` transaction before the commit of the `B` transaction even when `commit-ts(A) > commit-ts(B)`.

In the case of dynamic sorted tables, this implementation detail is hidden from the user by snapshot isolation: even though the data from the `A` transaction has already been added to the table, it can only be read by ordering `ts` not less than `commit-ts(A)`. In addition, the row locking system ensures that such inversion will not occur on the same key when the `[start-ts,commit-ts]` intervals for transactions overlap.

The situation is different for ordered dynamic tables: they usually do not store commit timestamps, except in the case of the `$timestamp` field, and do not support snapshot isolation. Therefore, the order in which writes will be added to the end of the ordered dynamic table has nothing to do with the order for commit timestamps.

When an ordered dynamic table is involved in a two-phase commit, i.e. a commit affects more than one tablet cell, confirmation of a successful commit from the coordinator does not mean that all participants have also performed a commit and that data has indeed been added to the end of the table. For example, this is true for all commits in which the ordered dynamic table is a synchronous replica. In the above two-phase commit scenario, immediately after a successful commit, an attempt to find the committed rows on a participant may fail: these rows will not be visible until some time later.

You can obtain certain monotony guarantees for added rows. A table has a `commit_ordering` system attribute that manages the order of adding rows to it:

- `weak`: The default mode. The rows get into the ordered table immediately at the time of the commit of the participant, which is late relative to the coordinator, and the order relative to commit timestamps is not guaranteed.
- `strong`: It is guaranteed that the rows get into the table in the order of commit timestamps.

In `strong` mode, if there is a `$timestamp` field, the table appears ordered by this field, but this does not make it key.

In fact, `strong` mode is implemented as follows: each tablet cell tracks a special barrier-ts, which increases monotonically all the time and about which it is known that no transaction can get `commit-ts` less than `barrier-ts`. The rows written within a transaction to an ordered dynamic table with `strong` mode do not get into it immediately at the time of the commit when barrier-ts becomes larger than `commit-ts` of this transaction. Thus, the system serializes all transactions by `commit-ts`, but only those for which `commit-ts < barrier-ts`. For transactions with `commit-ts > barrier-ts`, the system can define a relative order, but cannot guarantee that there will not be a new transaction in the future that violates the established order.

For static tables, there is also a `commit_ordering` attribute, but it is always `weak`.
