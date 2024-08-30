# Sharding

This section describes ways of sharding dynamic tables. A description of the automatic sharding algorithm is given.

Dynamic tables are divided into [tablets](../../../user-guide/dynamic-tables/overview.md#tablets) (shards), which are the unit of concurrency.

#### Sorted tables

Each tablet of a [sorted table](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) is responsible for a certain range of keys. Tablet boundary keys (also called pivot keys) are available in the `pivot_keys` table attribute. A tablet with the *k* index is responsible for the keys between the *k* boundary key (inclusive) and *k*+1 boundary key (not inclusive).

Each boundary key consists of a certain prefix of the table's key columns. For example, for a table with three key columns of the types int64, string, and double, the acceptable boundary keys could be `[]`, `[10]`, `[50; foo]`, `[100; bar; 1.234]`. The boundary keys of tablets must be strictly ascending. The first boundary key must be empty.

When resharding a sorted table, you can either specify the desired boundary keys explicitly or pass the `tablet_count` parameter. In the latter case, the system will attempt to split the table evenly into the required number of tablets.

#### Ordered tables

Each tablet of an [ordered table](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md) is an independent queue. To reshard an ordered table, you must specify the desired number of tablets. For more information, see [Sharding ordered tables](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md).

#### Replicated tables
Physically, each tablet of a [replicated table](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md) is a queue and stores the replication log. Despite this, the sharding logic is the same as for regular tables.
- Each tablet of a sorted replicated table is responsible for a particular key range and stores the replication log of keys from that range.
- Each tablet of an ordered replicated table is an independent queue. For the tablets of a replicated table and the replicas to match, use the [preserve_tablet_index](../../../user-guide/dynamic-tables/replicated-dynamic-tables#sohranenie-indeksa-tableta-pri-replikacii-uporyadochennoj-tablicy) attribute.

The sharding of a sorted replicated table doesn't have to match the sharding of replicas. Different replicas of the same table can also be sharded in different ways.

You can't reshard a non-empty replicated table. To do that, you must recreate the table with the required boundary keys.

## Manual sharding

Use the [reshard-table](../../../api/commands.md#reshard_table) command to reshard a table. Before running the command, the table must be unmounted.

{% list tabs %}

- CLI
   ```bash
   yt unmount-table //path --sync

   # Reshard with tablet count
   yt reshard-table //path --tablet-count 10 --sync
   # --enable-slicing is used to pick pivots more precisely in case of small tables
   yt reshard-table //path --tablet-count 100 --enable-slicing --sync
   # Reshard with pivot keys
   yt reshard-table //path '[]' '[10;foo]' '[20]' --sync

   yt mount-table //path --sync
   ```

- Python
   ```python
   yt.unmount_table("//path", sync=True)

   # Reshard with tablet count
   yt.reshard_table("//path", tablet_count=10, sync=True)

   # enable_slicing is used to pick pivots more precisely in case of small tables
   yt.reshard_table("//path", tablet_count=10, enable_slicing=True, sync=True)

   # Reshard with pivot keys
   yt.reshard_table("//path", [[], [10, "foo"], [20]], sync=True)

   yt.mount_table("//path", sync=True)
   ```

{% endlist %}

{% note info "About the "sync" parameter" %}

The "unmount-table", "reshard-table", and "mount-table" commands are asynchronous, so we recommend waiting until execution is complete when using them. For that, you can use the `--sync` flag or the `sync=True` parameter. In practice, "unmount-table" can take a long time to execute, so waiting is required for this command. Typically, "reshard-table" and "mount-table" are completed instantly, so waiting can be omitted for manual runs, but with scripts it's recommended to use sync.

{% endnote %}

When the `tablet_count` parameter is specified, the system attempts to automatically select appropriate boundary keys. If the table is relatively small (less than 200 MB per tablet), there may be fewer total tablets than requested. In this case, we recommend using the [enable_slicing](../../../api/commands.md#reshard_table) option to refine the boundary key search.

You can find out the number of tablets via the `@tablet_count` attribute.

## Sharding by hash { #hash }

To distribute the load evenly, you can use the standard approach: prepend an auxiliary key column that contains the hash of the part of the key to perform sharding for (for example, the hash from the first column). The resulting table will have its keys evenly distributed in the [0, 2<sup>64</sup> – 1] range.

To split it into *k* tablets, simply split the [0, 2<sup>64</sup>-1] range into *k* parts. To do this, you can use the "reshard-table" command with the `--uniform` flag: when `tablet_count` is specified, the system will set the boundary keys [], [2<sup>64</sup> / *k*], [2<sup>64</sup> * 2 / *k*], and so on.

The value of the key column can be calculated on the client side and transmitted when writing to the table, but computed columns can be used as an alternative.

### Computed columns { #expression }

The {{product-name}} system supports the feature that automatically calculates the value of a key column using a formula. You must specify this formula in the schema of this column in the `expression` field. The key column can only depend on non-calculated key columns. When writing a row or searching for a row by key, the computed columns must be skipped.

For even distribution, better specify `"expression" = "farm_hash(key)"` where `key` is the prefix of the source key (`farm_hash` is a built-in function that calculates [FarmHash](https://code.google.com/p/farmhash) from arguments).

When using automatically computed columns, consider that the `select_rows` operation infers the range of affected keys out of the predicate to optimize performance. If some of the values of the computed columns are not specified explicitly in the predicate, {{product-name}} will try to supplement the condition with the value of the computed columns. In the current implementation, the result will be successful if those columns on which the calculated one depends are defined in the predicate by an equality or using the `IN` operator. Calculation of the explicitly specified values of the computed columns in the range output does not take place.

#### Example of using compucolumns

Let's assume there is a table with `hash, key, value` columns, `hash` and `key` are key columns, and the `expression = "farm_hash(key)"` formula is specified in the schema for `hash`. Then for insert, delete, and read operations by key, you only need to specify `key` and `value`. In order for the `select_rows` operation to work efficiently, `key` must be exactly specified in the predicate so that the {{product-name}} system can calculate which `hash` values to consider. For example, you can specify `WHERE key = key_value` or `WHERE key IN (key_value1, key_value2)` in the query.

If you specify `WHERE key > key_lower_bound and key < key_upper_bound`, the range for `hash` cannot be inferred. In some cases, enumeration of  the values of the computed columns is possible. Enumeration occurs in the following cases:

1. The expression of the computed column is `expression = "f(key / divisor)"`  where `key` and `divisor` are integers. In this case, all such `key` values are enumerated and they generate different `expression` values. This behavior is generalized in case there are multiple computed columns and multiple `key` occurrences with different divisors.
2. The expression is `expression = "f(key) % mod"`. In this case, the `expression` values within the `mod` value are enumerated, and the `null` value is also included in the enumeration.

If both enumeration methods can be applied, the one that generates the least number of values is chosen. The total number of keys generated by the enumeration is limited by the `range_expansion_limit` parameter.
