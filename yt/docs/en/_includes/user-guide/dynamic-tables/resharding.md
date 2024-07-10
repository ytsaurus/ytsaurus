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

## Automatic sharding { #auto }

Sharding is needed to distribute the load evenly across the cluster. Automatic sharding includes:

1. Table sharding.
2. Redistributing tablets between [tablet cells](../../../user-guide/dynamic-tables/overview.md#tablet_cells).

Sharding is needed for the table tablets to become approximately of the same size. Redistributing between tablet cells is needed for tablet cells to have an approximately equal amount of data. This is especially important for in-memory tables (with `@in_memory_mode` other than `none`), because the cluster memory is a very limited resource and some cluster nodes may become overloaded if the distribution fails.

You can configure balancing both on a per-table basis and for each [tablet cell bundle](../../../user-guide/dynamic-tables/overview.md#tablet_cell_bundles).

A list of available bundle settings is available at `//sys/tablet_cell_bundles/<bundle_name>/@tablet_balancer_config` and represented in the table below:

| Name | Type | Default value | Description |
| -------------------------------- | --------- | ---------------------- | ------------------------------------------------------------ |
| min_tablet_size | int | 128 MB | Minimum tablet size |
| desired_tablet_size | int | 10 GB | Desired tablet size |
| max_tablet_size | int | 20 GB | Maximum tablet size |
| min_in_memory_tablet_size | int | 512 MB | Minimum in-memory table tablet size |
| desired_in_memory_tablet_size | int | 1 GB | Desired in-memory table tablet size |
| max_in_memory_tablet_size | int | 2 GB | Maximum in-memory table tablet size |
| enable_tablet_size_balancer | boolean | %true | Enabling/disabling sharding |
| enable_in_memory_cell_balancer | boolean | %true | Enabling/disabling moving in-memory tablets between cells |
| enable_cell_balancer | boolean | %false | Enabling/disabling moving not-in-memory tablets between cells |

A list of available table settings (`//path/to/table/@tablet_balancer_config`) is given in the table below:

| Name | Type | Default value | Description |
| ------------------------- | --------- | ---------------------- | ------------------------------------------------------------ |
| enable_auto_reshard | boolean | %true | Enabling/disabling sharding |
| enable_auto_tablet_move | boolean | %true | Enabling/disabling moving table tablets between cells |
| min_tablet_size | int | - | Minimum tablet size |
| desired_tablet_size | int | - | Desired tablet size |
| max_tablet_size | int | - | Maximum tablet size |
| desired_tablet_count | int | - | Desired number of tablets |
| min_tablet_count | int | - | Minimum number of tablets (see explanation in the text) |

{% cut  "Устаревшие атрибуты таблицы" %}

Previously, instead of `//path/to/table/@tablet_balancer_config`, the balancer settings were set directly on the table. The following attributes could be found in the table:

- enable_tablet_balancer;
- disable_tablet_balancer;
- min_tablet_size;
- desired_tablet_size;
- max_tablet_size;
- desired_tablet_count.

These attributes are declared **deprecated**. They are linked to the corresponding values from `//path/to/table/@tablet_balancer_config`, but their use is **not recommended**.

Previously, the `enable_tablet_balancer` could either not exist or could take one of the true/false values. It now unambiguously corresponds to the `enable_auto_reshard` option and either contains `false` (balancing disabled) or does not exist (balancing enabled, default value).

{% endcut %}


If the `desired_tablet_count` parameter is specified in the table settings, the balancer will attempt to shard the table by the specified number of tablets. Otherwise, if all three `min_tablet_size`, `desired_tablet_size`, and `max_tablet_size` parameters are specified in the table settings and their values are allowable (i.e. `min_tablet_size < desired_tablet_size < max_tablet_size` is true), the values of the specified parameters will be used instead of the default settings. Otherwise, the tablet cell bundle settings will be used.

The automatic sharding algorithm is as follows: the background process monitors the mounted tablets and as soon as it detects a tablet smaller than `min_tablet_size` or larger than `max_tablet_size`, it tries to bring it to `desired_tablet_size`, possibly affecting adjacent tablets. If the `desired_tablet_count` parameter is specified, the custom table size settings will be ignored and values will be calculated based on table size and `desired_tablet_count`.

If the `min_tablet_count` parameter is set, the balancer will not combine tablets if the resulting number of tablets is less than the limit. However, this option does not guarantee that the balancer will increase the number tablets if there are fewer now: if you use it, you need to pre-shard the table by the desired number of tablets yourself.

### Disabling automatic sharding:

On the table:

- `yt set //path/to/table/@tablet_balancer_config/enable_auto_reshard %false`: Disable sharding.
- `yt set //path/to/table/@tablet_balancer_config/enable_auto_tablet_move %false`: Disable moving tablets between cells.

On the tablet cell bundle:

- `yt set //sys/tablet_cell_bundles//@tablet_balancer_config/enable_tablet_size_balancer %false`: Disable sharding.
- `yt set //sys/tablet_cell_bundles//@tablet_balancer_config/enable_{in_memory_,}cell_balancer %false`: Disable moving between in_memory/not-in_memory tablet cells.

## Automatic sharding schedule

Sharding will inevitably unmount some of the tablets. To make the process more predictable, you can set up the balancer schedule. The setting can be per-cluster and per-bundle. The bundle schedule is in the `//sys/tablet_cell_bundles//@tablet_balancer_config/tablet_balancer_schedule` attribute. Any arithmetic formula from the `hours` and `minutes` integer variables can be specified as a format. The bundle table balancing will only occur when the formula value is true (i.e. different from zero).

The background balancing process runs once every few minutes, so you should expect that the tablets can be in the unmounted state for 10 minutes after the formula becomes true.

Examples:

- `minutes % 20 == 0`: Balancing at the 0th, 20th, and 40th minute of each hour.
- `hours % 2 == 0 && minutes == 30`: Balancing at 00:30, 02:30, ...

If no attribute value is specified for the bundle, the default value for the cluster is used. Example of a cluster-level balancing setup is given in the table below:

| Cluster | Schedule |
| ---------- | ------------------------------------ |
| first-cluster | ` (hours * 60 + minutes) % 40 == 0` |
| second-cluster | ` (hours * 60 + minutes) % 40 == 10` |
| third-cluster | ` (hours * 60 + minutes) % 40 == 20` |
