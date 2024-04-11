# CHYT instance configuration

The entire clique configuration is described by a single [YSON document](../../../../../user-guide/storage/yson-docs.md) called a *speclet*.

The clique speclet is stored in Cypress at `//sys/strawberry/chyt/<alias>/speclet`. You can also retrieve it by running the CLI command `yt clickhouse ctl get-speclet`.

A speclet is a set of options that are interpreted by the controller at clique startup. In particular, the controller generates the final configuration of CHYT instances based on these options.

## Available options { #options }

Here are the clique options that can be set by the `set-option` command (the default values are in square brackets):

- `active` [`%false`]: If the option is enabled, the controller will try to run the Vanilla operation assigned to the clique. When the value is `%false`, the clique is inactive: it doesn't process queries or use any compute pool resources. Use this option, if you want to disable the clique for a while, preserving its configuration.

- `pool`: Name of the compute pool where you want to run the clique operation. To set this option, the user needs the `Use` permission for the specified compute pool. Setting this option is required if you want the clique operation to be run and administered by the controller.

- `enable_geodata` [`%true`]: Automatically set up the system dictionaries needed for [certain geo features in ClickHouse](https://clickhouse.com/docs/en/sql-reference/functions/ym-dict-functions/).

- `restart_on_speclet_change` [`%true`]: If this option is set, the clique will automatically restart on any reconfiguration (upon speclet modification). Otherwise, you will need to restart the clique manually to apply its settings.

- `query_settings`: Dictionary storing the default settings for all the queries sent to the clique. For more information about query settings, see the [relevant documentation section](../../../../../user-guide/data-processing/chyt/reference/settings).

- `yt_config`: Dictionary with the {{product-name}} configuration for certain CHYT instances. This part of the configuration will be written to the generated instance configuration as it is. This setting is intended for advanced users. We don't recommend using this option, as this configuration has a pretty sophisticated structure. Use other options, whenever possible, to set changes to the configuration in a more straightforward way.

- `clickhouse_config`: Dictionary with the ClickHouse configuration for certain CHYT instances. This setting is intended for advanced users. Same case as with `yt_config`, we don't recommend using this option.

- `instance_count` [1]: Number of clique instances.

- `preemption_mode` [`normal`]: Preemption mode applied when running a clique's {{product-name}} operation. This setting is intended only for advanced users. If you are not sure if you need it, we recommend keeping the default value.


{% if audience == "internal" %}

- `network_project` [`cluster_node_nets` (will later be replaced by a dedicated `chyt` project)]: A network project whose network macro will run the clique instances. This setting is intended only for advanced users. If you are not sure if you need it, we recommend keeping the default value. If you're using a separate network project, make sure to read the article on Starting jobs in a separate network environment, which covers the network accesses and permissions that are required to start them.{% endif %}

- `instance_cpu` [16]: CPU resources to be allocated to each clique's instance. This setting is intended only for advanced users. If you are not sure if you need it, we recommend keeping the default value.

- `instance_total_memory` [71940702208 (67 GiB)]: RAM (in bytes) to be allocated to each instance of the clique. This setting is intended only for advanced users. If you are not sure if you need it, we recommend keeping the default value.

## Advanced configuration of the instance's {{product-name}} part { #yt_config }

The {{product-name}} part of the instance configuration resides in the `yt_config` option. With this option, you can configure advanced settings that don't exist as individual speclet options. The following sub-options are supported:

- `settings`: CHYT-specific default query [settings](../../../../../user-guide/data-processing/chyt/reference/settings.md). Changing the setting in this section changes the default value for this setting for all queries in the clique.

- `table_writer`: [Table Writer configuration](../../../../../user-guide/storage/io-configuration.md#table_writer).

- `table_attribute_cache`: Table attribute cache configuration. This cache significantly improves the responsiveness of CHYT, but at the moment it can potentially lead to non-consistent reads (data has already appeared in the table, but CHYT does not see it yet). To disable this cache, use the following configuration:
   ```
   {read_from=follower;expire_after_successful_update_time=0;expire_after_failed_update_time=0;refresh_time=0;expire_after_access_time=0}
   ```
- `create_table_default_attributes` [`{optimize_for = scan}`]: The default attributes with which tables will be created during `CREATE` queries in CHYT.

- `health_checker`: The Health Checker configuration{% if audience == "internal" %}, see also the [corresponding section](../../../../../user-guide/data-processing/chyt/cliques/dashboard.md#health_checker) on the CHYT dashboard{% endif %}. Consists of 3 fields:

   — `queries` [``["select * from `//sys/clickhouse/sample_table`"]``]: A list of test queries whose performance will be checked regularly.

   {% note warning "Attention" %}

   Queries from Health Checker are currently executed on behalf of user `yt-clickhouse` who must have the `read` permissions to access the table.


   {% endnote %}

   — `period` [60000]:The test triggering period in milliseconds.

   — `timeout` [`0.9 * period / len(queries)`]: The triggering timeout for each of the configured queries. If the query does not fit into the timeout, it is considered `failed`.

- `subquery`: The configuration of the main system part which coordinates the execution of ClickHouse queries on top of {{product-name}} tables. Before configuring this part, read the article about [query execution within a clique](../../../../../user-guide/data-processing/chyt/queries/anatomy.md#query-execution).

   — `min_data_weight_per_thread` [64 MiB]: (in bytes) When splitting the input into subqueries, the coordinator will try to give no less than the given amount to each core of each instance.

   — `max_data_weight_per_subquery` [50 GiB]: (in bytes) The maximum allowable amount of data to be processed on one core of one instance. This restriction is protective and protects clique users from accidentally running a huge query that processes petabytes. The constant of 50 GiB is selected, because such amount is processed on a single core in about dozens of minutes.

- `show_tables`: The `SHOW TABLES` query behavior configuration. Enables you to configure the list of directories in which `SHOW TABLES` will show a list of tables.

   — `roots`: The list of YPath paths of directories in Cypress from which the tables will be collected for `SHOW TABLES`.

{% note warning "Attention" %}

The `max_data_weight_per_subquery` limit uses columnar statistics to account for column sampling from processed tables. Columnar statistics may be missing for old tables created before the statistics became available. For such tables, column orientation is not taken account in the limit. This means that when processing a very narrow slice of columns which forms 1% of the total table volume and is 1 GiB per core, CHYT will calculate that 100 GiB per core are processed. CHYT will not run such a query with the default settings.

If this scenario is required or you need to process large amounts of data, configure this setting to a random large value.

{% endnote %}

## Advanced configuration of the instance's ClickHouse part { #clickhouse_config }

The `clickhouse_config` setting is used to customize the default configuration of the ClickHouse server.

The rules for converting a ClickHouse XML configuration into a CHYT YSON configuration can be described as follows:

- Any non-multiple configuration node in the ClickHouse configuration is a dict (`map`).

- A multiple node is represented by a list (`list`).

Below is an example of converting an artificial XML configuration into a YSON configuration.


```xml
<foo>42</foo>
<bar>qwe</bar>
<baz>
    <quux>3.14</quux>
</baz>
<baz></baz>
<baz>hi!</baz>
```


```json
{
    foo = 42;
    bar = "qwe";
    baz = [
        {quux = 3.14};
        {};
        "hi!";
    ];
}
```

Let's examine the `clickhouse_config` sub-options that may be of use in CHYT:

- `dictionaries` (`[]`): The configuration of external dicts. The value must be a list of dict configurations. Each dict is configured by the map with the following fields that retain the meaning of the original ClickHouse configuration:
   - `name`: The name of the external dict.
   - `source`: The [data source](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources/) for the external dict.
   - `layout`: [Representation](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout/) of the external dict in the instance memory.
   - `structure`: The [schema of data](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure/) stored in the dict.
   - `lifetime`: The dict [lifetime](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime/).

## External dicts { #external-dict }

CHYT supports all `layout`, `structure`, and `lifetime` settings of regular ClickHouse.

{% note info "Note" %}

You can specify sources from original ClickHouse as the `source`, but they are not guaranteed to work. In particular, sources that require networking should technically work, but may face a lack of network access in practice.

{% endnote %}

There is also an additional data source type in which you can use static tables in {{product-name}}. This type is called `yt` and has one parameter:

- `path`: The path to the static table on the cluster which will serve as a data source for the dict.

As an example of connecting a {{product-name}} table as an external ClickHouse dict, you can look at:
- The CHYT [configuration example](../../../../../user-guide/data-processing/chyt/reference/configuration.md#configuration_example) that connects this table as an external ClickHouse dict.
- Example of using an external dict in queries:

```sql
select dictGet('OS', 'OS', toUInt64(38)) as os_name,
    dictGetHierarchy('OS', toUInt64(38)) as hierarchy,
    dictGetChildren('OS', toUInt64(101)) as children
```

## Configuration example { #configuration_example }

Below is an example of a complete configuration that can be set by the `set-speclet` command.

```json
{
    instance_count = 1;
    query_settings = {
        extremes = 1;
    };
    yt_config = {
        subquery = {
            max_data_weight_per_subquery = 1000000000000;
        };
    };
    clickhouse_config = {
        dictionaries = [
            {
                name = OS;
                layout = {flat = {}};
                structure = {
                    id = {name = Id};
                    attribute = [
                        {
                            name = "OS";
                            type = "Nullable(String)";
                            null_value = "NULL";
                        };
                        {
                            name = "ParentId";
                            type = "Nullable(UInt64)";
                            null_value = "NULL";
                            hierarchical = %true;
                        };
                        {
                            name = "RootId";
                            type = "Nullable(UInt64)";
                            null_value = "NULL";
                        };
                    ];
                };
                lifetime = 0;
                source = {yt = {path = "//sys/clickhouse/dictionaries/OS"}};
            };
        ];
    };
}
```
