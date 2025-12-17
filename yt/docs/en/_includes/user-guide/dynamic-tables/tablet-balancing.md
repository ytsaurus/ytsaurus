# Automatic sharding and dynamic table balancing

Sharding and balancing help ensure even load distribution across the cluster. This includes:

* Table sharding.
* Redistributing tablets between [tablet cells](../../../user-guide/dynamic-tables/concepts.md#tablet_cells).

Sharding is needed for the table tablets to become approximately of the same size, while redistribution between tablet cells helps ensure that tablet cells have an approximately even amount of data and/or load. Even data distribution is especially important for in-memory tables (with `@in_memory_mode` other than `none`), because cluster memory is a rather limited resource and a poor distribution can overload some cluster nodes. Load distribution, on the other hand, is essential for cases where the load on table keys is uneven, or if a certain type of load is a limited resource in your bundle.

You can configure balancing both on a per-table basis and for each [tablet cell bundle](../../../user-guide/dynamic-tables/concepts.md#tablet_cell_bundles). You can also configure balancing for groups of tables. Table configuration has a higher priority than group configuration, which in turn has a higher priority than bundle configuration.

The system has two types of balancers: the legacy balancer, which lives in the master process and supports only balancing by tablet size and a limited number of options, and the standalone balancer that also supports balancing by load. {% if audience == "public" %}The external balancer is not currently supported in opensource.{% endif %}

{% if audience == "internal" %}

A list of available production clusters with a standalone balancer: {{clusters-with-new-tablet-balancer}}.
\* — Cluster bundles are moved to the standalone balancer on request via [{{tracker-queue}}](https://{{tracker-domain}}.{{internal-domain}}/{{tracker-queue}}). Bundles can only be moved if they don't contain tables on the primary master (with `external=%false`) that need to be balanced.

{% endif %}

## Balancing strategies

 * **Resharding by size**. To evenly distribute table tablets across tablet cells, each tablet needs to be of a reasonable size. Large tablets are split into several smaller ones, and small tablets are combined with the neighboring ones.
 * **Resharding by parameter and size** (available only for clusters with a standalone balancer). To evenly balance the table by parameter, all table tablets need to be of approximately the same size and have approximately the same load. Tablets with a higher load or of a larger size are resharded, and smaller tablets with a smaller load are combined with the neighboring ones.
 * **Balancing between tablet cells**
   * **In-memory tables**. In-memory tables are balanced by minimizing the load on the most loaded cell (for tables with hunks, this is done on their non-hunk part). Only the total node memory usage across all tables is taken into account, the tablet distribution within each individual table is not optimized.
   * **Disk tables**. Disk tables are balanced by the number of table tablets by the bundle's tablet cells.
 * **Parameterized balancing between nodes** (aka balancing by load, available only for clusters with a standalone balancer). Minimizes the dispersion of the selected metric by node and, to a lesser extent, by cell. Per-table distribution is not taken into account, all tablets are considered equivalent.

## Quick start for parameterized balancing { #quick-start }

> Available only for clusters with a standalone balancer.

If the bundle doesn't have parameterized balancing enabled and dedicated balancing groups set up, all you need to do is create a group with the desired settings and put the tables into that group.

1. To verify whether these instructions apply to you, make sure that the bundle config doesn't contain a list of groups (the `//sys/tablet_cell_bundles/<bundle_name>/@tablet_balancer_config` map doesn't include the `groups` field).
2. Select a metric for balancing the tables. For the meta cluster, if writes to replicated tables are evenly distributed across their tablets, we recommend balancing the load based on the number of tablets (`metric = "1"`). The list of available metrics is provided in the [section on parameterized balancing configuration](#parameterized). You can add metrics using the + operator in the formula. If the bundle contains groups of tables with varying types of load, you may want to balance them using different metrics. To do that, create multiple groups with custom settings. For more information, see the [section on table groups](#group).
3. For large balancing groups with more than 50,000 tablets, you may need to adjust the `max_action_count` parameter, which specifies the number of movements per balancing iteration. For more information, see the section on [parameterized balancing configuration](#parameterized). If you enable parameterized balancing for the entire bundle, you can view the total number of tablets across all tables on the bundle page.
4. Create a `default` [group](#system-groups) with the desired config for parameterized balancing and enter the desired metric in the creation command. If needed, use a similar approach to set the `max_action_count` parameter.
    ```(bash)
    yt set //sys/tablet_cell_bundles/<bundle_name>/@tablet_balancer_config/groups
    '{default = {parameterized = {metric = "<metric_formula>"}}}'
    ```
5. Enable parameterized balancing for the desired tables.
    - To enable it for the entire bundle:
      `yt set //sys/tablet_cell_bundles/<bundle_name>/@tablet_balancer_config/enable_parameterized_by_default %true`
    - To enable it only for specific tables, enable parameterized balancing for each table individually:
      `yt set //path/to/table/@tablet_balancer_config/enable_parameterized %true`
6. You can monitor the balancer's actions on the bundle page: go to Monitoring → Maintenance.

## Balancing configuration

### Table groups { #group }

> Available only for clusters with a standalone balancer.

To make configuration easier, the service supports balancing groups. Their configs are located in the `groups` field in the bundle config. If this parameter doesn't exist (it isn't created automatically), you need to create it. Inside the group, all tables are balanced together.

{% cut "Creating a balancing group with certain values" %}

You need to create an empty map in the `groups` field along with configs for balancing groups, subconfigs, and so on
`yt set //sys/tablet_cell_bundles/<bundle_name>/@tablet_balancer_config/groups '{}'`
, or create a group with the desired configuration right away
`yt set //sys/tablet_cell_bundles/<bundle_name>/@tablet_balancer_config/groups '{alpha={parameterized={max_action_count=5}; schedule="minutes % 45 == 0"}}'`

{% endcut %}

To put a table into a group named `alpha`, set the `group` field of the `@tablet_balancer_config` attribute to `alpha`.

#### Group configuration parameters

| **Name** | **Type** | **Default value** | **Description** |
|--|--|--|--|
|type| `legacy` / `parameterized` | `legacy` for `legacy` and `legacy_in_memory` groups, `parameterized` for other group types | Balancing type. `legacy` can't be set for new groups |
|parameterized|yson||Parameterized balancing configuration. Learn more in the section on parameterized balancing configuration|
|schedule|Arithmetic formula that includes `minutes`, `hours`, and numeric values|If the value is set, it is taken from the bundle's config. Otherwise, it depends on the cluster configuration|Balancing schedule. Once the formula value becomes true, this triggers the resharding of all the group's tables, and after that (usually within 10 minutes), movement between cells. See the examples in the [Schedule](#schedule) section|
|enable_move|boolean|%true|Enables movement between tablet cells. Balancing type depends on the group|
|enable_reshard|boolean|%true|Enables automatic resharding. Balancing type depends on the group|

{% cut "Sample group configuration" %}

{% note warning "Attention" %}

This is not a recommendation. The example only lists possible options with random values.

{% endnote %}

```bash
"in_memory_stable" = {
    "enable_move" = %true;
    "enable_reshard" = %true;
    "schedule" = "minutes % 30 == 5";
    "parameterized" = {
        "max_action_count" = 100;
        "metric" = "double([/statistics/memory_size])";
    };
};
```

{% endcut %}

#### System groups { #system-groups }

From the balancer's point of view, system groups exist by default even if `@tablet_balancer_config/groups` doesn't explicitly list their configs. The default values for these groups are taken from the above table. All system groups are listed below:
* `legacy`: Balancing between the tablet cells of disk tables.
* `legacy_in_memory`: Balancing between the tablet cells of in-memory tables.
* `default`: Includes all tables with enabled parameterized balancing where the group isn't explicitly indicated.

Tables are put into the first and second groups automatically if the group type and parameterized balancing for them are not indicated. Tables are put into the `default` group if:
* The `default` group is explicitly indicated for the table.
* The group isn't explicitly indicated but `enable_parameterized=true` is set in the balancing config.
* The group isn't explicitly indicated, and parameterized balancing is enabled for the bundle by default (`enable_parameterized_by_default=%true`).

If the bundle's config indicates the default group name for in-memory table balancing (the `default_in_memory_group` attribute), the in-memory tables that were supposed to be put into the `default` category are put into the group specified in the config instead.

### Per-table settings

A list of available table settings (`//path/to/table/@tablet_balancer_config`) is given in the table:

| **Name**     | **Type** | **Default value** | **Description** |
| -- | -- | -- | -- |
| enable_auto_reshard   | boolean | %true         | Enables/disables resharding             |
| enable_auto_tablet_move | boolean | %true         | Enables/disables moving table tablets between cells |
| min_tablet_size     | int   | -           | Minimum tablet size for resharding by size |
| desired_tablet_size   | int   | -           | Desired tablet size for resharding by size |
| max_tablet_size     | int   | -           | Maximum tablet size for resharding by size |
| desired_tablet_count  | int   | -           | The desired number of tablets for resharding |
| min_tablet_count | int | - | The minimum number of tablets for resharding by size, [see note in text](#reshard-config)|
| group* | str | - | Puts the table into a balancing group, see more in [Table groups](#group)|
| enable_parameterized* | boolean | - | Enables/disables moving tablets by load|

\* — Available only for clusters with a standalone balancer.

{% cut "Sample table configuration" %}

{% note warning "Attention" %}

This is not a recommendation. The example only lists possible options with random values.

{% endnote %}

```bash
{
    "enable_auto_reshard" = %true;
    "enable_auto_tablet_move" = %true;
    "group" = "writes";

# use the rest wisely
    "enable_verbose_logging" = %false;
    "enable_parameterized" = %true;
    "desired_tablet_count" = 100;
    "min_tablet_count" = 5;
    "min_tablet_size" = 1000;
    "desired_tablet_size" = 5000;
    "max_tablet_size" = 10000;
}
```

{% endcut %}

### Per-bundle settings

The settings are specified in `//sys/tablet_cell_bundles/<bundle_name>/@tablet_balancer_config`.
If the bundle and the table have conflicting settings, the table settings have a higher priority and will be used for balancing.

| **Name**       | **Type** | **Default value** | **Description** |
| -- | -- | -- | -- |
| min_tablet_size        | int   | 128 MB         | Minimum disk table tablet size                   |
| desired_tablet_size      | int   | 10 GB         | Desired disk table tablet size                   |
| max_tablet_size        | int   | 20 GB         | Maximum disk table tablet size                 |
| min_in_memory_tablet_size   | int   | 512 MB         | Minimum in-memory table tablet size         |
| desired_in_memory_tablet_size | int   | 1 GB          | Desired in-memory table tablet size         |
| max_in_memory_tablet_size   | int   | 2 GB          | Maximum in-memory table tablet size       |
| enable_tablet_size_balancer  | boolean | %true         | Enables/disables resharding by size |
| enable_in_memory_cell_balancer | boolean | %true         | Enables/disables in-memory table tablet balancing between tablet cells |
| enable_cell_balancer      | boolean | %false         | Enables/disables disk table tablet balancing between tablet cells |
| tablet_to_cell_ratio | double | 5. | Resharding by size limits the number of tablets in a table to the number of tablet cells multiplied by the value of this parameter |
| enable_parameterized_by_default* | boolean | %false | Puts tables with no explicit `group` indication into the `default` group for parameterized balancing |
| groups* | dictionary | - | Balancing group configs |
| default_in_memory_group* | str | - | Default group name for in-memory tables for parametrized balancing |

\* — Available only for clusters with a standalone balancer.

{% cut "Sample bundle configuration" %}

{% note warning "Attention" %}

This is not a recommendation. The example only lists possible options with random values.

{% endnote %}

```bash
{
    "enable_cell_balancer" = %true;
    "enable_in_memory_cell_balancer" = %true;
    "enable_tablet_size_balancer" = %true;
    "min_tablet_size" = 500;
    "desired_tablet_size" = 1000;
    "max_tablet_size" = 2000;
    "min_in_memory_tablet_size" = 50;
    "desired_in_memory_tablet_size" = 100;
    "max_in_memory_tablet_size" = 1000;
    "hard_in_memory_cell_balance_threshold" = 0.15;
    "soft_in_memory_cell_balance_threshold" = 0.05;
    "tablet_balancer_schedule" = "(hours * 60 + minutes) % 80 == 10";
    "tablet_to_cell_ratio" = 5.;
    "enable_verbose_logging" = %false;
    "enable_parameterized_by_default" = %true;
    "groups" = {
        "in_memory_stable" = {
            # ... parameterized balancing only
        };
        "legacy" = {
            # ...
        };
        "default" = {
            # ... parameterized balancing only
        };
        "legacy_in_memory" = {
            # ...
        };
    };
}
```

{% endcut %}

### Parameterized balancing configuration { #parameterized }

> Available only for clusters with a standalone balancer.

| **Name**       | **Type** | **Default value** | **Description** |
| -- | -- | -- | -- |
| metric      | str | write data weight | Balancing metric (parameter) |
| max_action_count | int | - | Maximum number of tablets to be moved in one iteration |
| enable_reshard        | boolean   | - | Enables the resharding algorithm by parameter and size instead of resharding by size |
| per_table_uniform        | boolean   | - | Enables the movement balancing algorithm, which among other functions aims to ensure an even distribution of tables across nodes and cells. Recommended for groups of no more than 50 tables |

The metric for parameterized balancing is set by an arithmetic formula using per-table metrics. The metric must be positive for any given tablet. For the correct operation of the balancing algorithm, the metric size must be in direct ratio to the tablet load. The formula may include tablet size metrics. Each of the load metrics can have one of the following two suffixes: `_10m_rate` or `_1h_rate`, which are calculated as the exponential decay of requests within 10 minutes and 1 hour, respectively. In the table, all metrics are given for the 10-minute window.

Below you will find per-table metrics that can prove useful.

- Write data weight
  `write_10m`
- Amount of data read by lookup requests, including reads from hunk chunks
  `lookup_10m`
- Amount of data read by select requests, including reads from hunk chunks
  `read_10m`
- Amount of data read by lookup requests, excluding reads from hunk chunks
  `double([/performance_counters/dynamic_row_lookup_data_weight_10m_rate]) + double([/performance_counters/static_chunk_row_lookup_data_weight_10m_rate])`
- Amount of data read by select requests, excluding reads from hunk chunks
  `double([/performance_counters/dynamic_row_read_data_weight_10m_rate]) + double([/performance_counters/static_chunk_row_read_data_weight_10m_rate])`
- Uncompressed size
  `double([/statistics/uncompressed_data_size])`
- Compressed size
  `double([/statistics/compressed_data_size])`
- Memory size (the amount of data occupied by an in-memory table, equal to either compressed or uncompressed size depending on `in_memory_mode`)
  `double([/statistics/memory_size])`
- CPU time consumed by lookup requests
  `lookup_cpu_10m`
- CPU time consumed by select requests
  `select_cpu_10m`
- Number of tablets (can be used if tablets need to be distributed equally across nodes without taking the load into account)
  `1`

You may need to set the `max_action_count` parameter, which represents the number of movements per balancing iteration. This parameter determines how many tablets will be inaccessible during balancing. If your bundle is fairly large or, for some reason, the default values don't suit your needs, you can apply the following principle to estimate an initial value for this parameter:
- For bundles of up to 100 tablet nodes: No less than the number of nodes and no less than 10.
- For groups of up to 100 tablets: 10.
- For groups of up to 1000 tablets: 50.
- For groups of up to 10,000 tablets: 100.
- For groups of up to 50,000 tablets: 250.
- For groups of up to 100,000 tablets: 500.
If all the bundle tables use parameterized balancing and there is only one parameterized balancing group, then the total number of tablets across all tables is displayed on the bundle page.

{% cut "Full list of balancing metric aliases" %}

- `write_10m`, `write_1h`
- `lookup_10m`, `lookup_1h`
- `read_10m`, `read_1h`
- `lookup_cpu_10m`, `lookup_cpu_1h`
- `select_cpu_10m`, `select_cpu_1h`

{% endcut %}

### Resharding configuration

#### Resharding by size { #reshard-config }

- If the `desired_tablet_count` parameter is specified in the table settings, the balancer will attempt to shard the table by the specified number of tablets.
- Otherwise, if all three parameters (`min_tablet_size`, `desired_tablet_size`, `max_tablet_size`) are specified in the table settings and their values are valid (i.e. `min_tablet_size < desired_tablet_size < max_tablet_size` is true), the specified parameter values will be used.
- Otherwise, the bundle's tablet cell settings will be used.

{% note warning "Note" %}

If all three tablet size parameters (min, desired, and max) are explicitly indicated, the following recommendation needs to be observed for the algorithm to work correctly: `max` / `min` must be equal to at least 2.5 or, better yet, 3. With smaller ratios, the algorithm won't be able to reshard the table to the optimal size and will continually change the size of tablets, interfering with your work.

{% endnote %}

The automatic sharding algorithm is as follows: the background process monitors the mounted tablets and as soon as it detects a tablet smaller than `min_tablet_size` or larger than `max_tablet_size`, it tries to bring it to `desired_tablet_size`, possibly affecting adjacent tablets. If the `desired_tablet_count` parameter is specified, the custom table size settings will be ignored and values will be calculated based on table size and `desired_tablet_count`.

If the `min_tablet_count` parameter is set, the balancer won't combine tablets if the resulting number of tablets is below the limit. However, this option doesn't guarantee that the balancer will reduce the size of tablets if their current number is too little: if you use it, you need to manually pre-shard the table to the desired number of tablets.

#### Resharding by size and parameter { #parameterized-reshard-config }

This type of resharding is intended for collaborative work with parameterized balancing. Both algorithms use the same metric (parameter) that is specified in the balancing group's config. To enable this type of resharding, you need to specify the desired number of tablets (`desired_tablet_count`) in the table's config. Without this value, the algorithm can't predict the desired parameter values for each tablet's metric. You also need to set the `enable_reshard` flag value to `%true` in the group's parameterized balancing config.

The algorithm is similar to that for the resharding by size, except for the conditions at which the tablets are split and combined. When the algorithm detects a tablet that doesn't meet at least one of the min/max size or parameter requirements, this tablet is either split or combined with the neighboring tablets to bring it to the required size and parameter values if possible. The above-mentioned limits are calculated based on the desired number of table tablets.

{% note info "Note" %}

Unlike resharding by size, this type of resharding doesn't take the `min_tablet_count` parameter into account, which means that the number of tablets in the table can drop down below the specified value.

{% endnote %}

### Disabling { #shutdown }

To disable a certain type of balancing, set the value of the corresponding attribute to `%false` (see the table below). You need to specify this value in the following configs:
- For bundles: `//sys/tablet_cell_bundles/<bundle>/@tablet_balancer_config/<attribute>`
- For groups: `//sys/tablet_cell_bundles/<bundle>/@tablet_balancer_config/groups/<group>/<attribute>`
- For tables: `//path/to/table/@tablet_balancer_config/<attribute>`

| **Balancing type** | **Bundle** | **Group** | **Table** |
| --- | --- | --- | --- |
| [in-memory move](*in-memory-move) | enable_in_memory_cell_balancer | enable_move | enable_auto_tablet_move |
| [ordinary move](*ordinary-move) | enable_cell_balancer | enable_move | enable_auto_tablet_move |
| [parameterized move](*parameterized-move) | - | enable_move | enable_auto_tablet_move |
| [reshard](*reshard) | enable_tablet_size_balancer | enable_reshard | enable_auto_reshard |
| [parameterized reshard](*parameterized-reshard) | - | enable_reshard | enable_auto_reshard |

### Automatic sharding schedule { #schedule }

Sharding will inevitably unmount some of the tablets. To make the process more predictable, you can set up the balancer schedule. The setting can be per-cluster, per-bundle, and per-group.

The bundle schedule is located in the `//sys/tablet_cell_bundles//@tablet_balancer_config/tablet_balancer_schedule` attribute. The group schedule is located in the `schedule` attribute of the [group's](#group) config.

Any arithmetic formula from the `hours` and `minutes` integer variables can be specified as a format. The balancing of the bundle's tables will only occur when the formula value is true (i.e. different from zero).

The background balancing process runs once every few minutes, so you should expect that the tablets can be in the unmounted state for 10 minutes after the formula becomes true.

Examples:

- `minutes % 20 == 0`: Balancing at the 0th, 20th, and 40th minute of each hour.
- `hours % 2 == 0 && minutes == 30`: Balancing at 00:30, 02:30, ...

If no attribute value is specified for the bundle, the default value for the cluster is used. Example of a cluster-level balancing setup is given in the table below:

| **Cluster** | **Schedule** |
| ---------- | ------------------------------------ |
| first-cluster | `(hours * 60 + minutes) % 40 == 0` |
| second-cluster | `(hours * 60 + minutes) % 40 == 10` |
| third-cluster | `(hours * 60 + minutes) % 40 == 20` |

## Implementation details { #details }

### Movement balancing

#### Parameterized balancing

{% note info "Attention" %}

Per-table distribution is not taken into account, all the group's tablets are considered equivalent.

{% endnote %}

Balancing is applied to tables in [groups](#system-groups) **other than** `legacy_in_memory` and `legacy`.

##### Algorithm overview

The algorithm calculates the `metric` for each tablet and then, depending on the tablet distribution by cells and nodes, the metrics for cells and nodes are calculated as the sum of metrics of the tablets that they host, followed by step-by-step minimization
$\sum_{node}{NodeMetric^2} + \sum_{cell}{CellMetric^2}$

In one parameterized balancing step, a tablet can be moved from one cell to another.

The algorithm won't run if tablet distribution by cells and nodes is adequate.

#### Balancing between disk tablet cells

{% note info "Attention" %}

The tablet size isn't taken into account, all the table's tablets are considered equivalent.

{% endnote %}

Balancing [group](#system-groups): `legacy`

##### Algorithm overview

Tablets are evenly distributed across bundle cells independently for each table. If the bundle has empty cells, the tablets are distributed so that each cell contains approximately the same number of tablets.

#### Balancing between in-memory tablet cells

{% note info "Attention" %}

Per-table distribution is not taken into account, all the group's tablets are considered equivalent.

{% endnote %}

Balancing [group](#system-groups): `legacy_in_memory`

##### Algorithm overview

Tablets are evenly distributed across bundle cells by `memory_size`. Per-table distribution is not taken into account.

### Sharding

#### Resharding by size

Performed by splitting and combining neighboring tablets. Each table is resharded independently.

##### Algorithm overview

The algorithm splits large tablets and combines neighboring tablets independently in each table. A tablet is considered too large if its size exceeds `max_tablet_size`. In this case, it can be split into several smaller tablets separately or together with the neighboring tablets so that the size of new tablets is approximately equal to `desired_tablet_size`. Tablets smaller than `min_tablet_size` are combined.

If `min_tablet_count` is specified for the table, and the number of tablets is less than the specified value, they won't be combined even if they are smaller than `min_tablet_size`. The parameter doesn't affect `min/desired/max tablet_size`.

If `desired_tablet_count` is specified for the table, the limits are calculated as follows:
- `desired_tablet_size = table_size / desired_tablet_count`
- `min_tablet_size = desired_tablet_size / 1.9`
- `max_tablet_size = desired_tablet_size * 1.9`

For the algorithm to work properly, **max_tablet_size / min_tablet_size > 2** needs to be true. Otherwise, the balancer will repeatedly perform useless actions that will hamper the table operation. Other configuration recommendations can be found in the [relevant section](#reshard-config).

#### Resharding by size and metric

Resharding by size and metric is performed by splitting and combining neighboring tablets. Tables are resharded independently, except for the stage when planned actions are sorted by relevance.

##### Algorithm overview

The algorithm requires `desired_tablet_count`. This value is used in each table to calculate the following values:
- `desired_tablet_size = table_size / desired_tablet_count`
- `min_tablet_size = desired_tablet_size / 1.9`
- `max_tablet_size = desired_tablet_size * 1.9`
- `desired_tablet_metric = table_metric / desired_tablet_count`
- `min_tablet_metric = desired_tablet_metric / 1.9`
- `max_tablet_metric = desired_tablet_metric * 1.9`

The algorithm splits large tablets and combines neighboring tablets. A tablet is considered too large if its size exceeds `max_tablet_size` or its metric exceeds `max_tablet_metric`. In this case, the tablet can be split into several smaller tablets so that the size of new tablets fits the specified limits. The algorithm will split a large tablet even if its value for the remaining parameter is smaller than the desired one. Tablets are combined if they are smaller than `min_tablet_size` or `min_tablet_metric`.

If table resharding creates too many actions, exceeding the maximum number of actions per iteration, all planned reshards of this group are sorted by revelance, and only the most useful ones are performed. Other configuration recommendations can be found in the [relevant section](#parameterized-reshard-config).

[*in-memory-move]: In-memory table balancing between tablet cells.

[*ordinary-move]: Disk table balancing between tablet cells.

[*parameterized-move]: Table balancing by load.

[*reshard]: Resharding by size.

[*parameterized-reshard]: Resharding by size and load.
