# Advanced settings configuration

This section describes how to work with advanced clique settings. In the web interface, they are located in the *Advanced settings* section. The complete list of advanced settings is provided in the [Advanced settings](../../../../../user-guide/data-processing/chyt/cliques/configs.md#options) section.

{% note warning %}

The settings mentioned in this section are intended for advanced users only. If you’re not sure you need them, we recommend keeping the default values.

{% endnote %}

Advanced settings include:

- Configuring queries within a clique — [Query settings](#query-settings) block;
- Configuring server settings — [{{clickhouse}} config](#ch-config) block;
- Configuring the YT part of instances — [YT config](#yt-config) block;
- Memory allocation — [Instance memory](#memory) block;
- Query caching — [Sticky query distribution](#sticky-query) block.

## Query settings { #query-settings }

The parameters in the *Query settings* section are a subset of [{{clickhouse}} session settings](https://clickhouse.com/docs/ru/operations/settings/settings) that will be applied to all queries within the clique.

To change query behavior, override the default parameter values using:

{% list tabs %}

- Web interface

    1. In the [{{clickhouse}} documentation](https://clickhouse.com/docs/ru/operations/settings/settings), find the required settings and copy their names.
    1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
    1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block or the button **Edit speclet** on the **Speclet** tab in the [Tab panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
    1. Select the **Advanced** section on the left.
    1. Find the **Query settings** section.
    1. In the *Use JSON syntax* field, enter parameters and their values in JSON syntax, as `key: value` pairs enclosed in curly braces `{ }`. For example:

        ```json
        {
            "max_execution_time": 200000,
            "max_insert_threads": 32,
            "max_threads": 32,
            "parallel_distributed_insert_select": 2
        }
        ```

    1. To apply the changes, click **Confirm**.

- CLI

    1. Install the [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package, if you haven’t done so already.
    1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.
    
        ```bash
        export YT_PROXY=<cluster_name>
        ```
    
    1. Set an environment variable with the controller address:
    
        ```bash
        export CHYT_CTL_ADDRESS=<address>
        ```
    
        , where `<address>` is the controller address. For example, the address for a demo cluster looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
        You can get the controller address from the `controller` field in the output of the following command:
    
        ```bash
        yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
        ```
    
    1. Save the cluster name to an environment variable:
    
        ```bash
        export CLUSTER_NAME=<cluster_name>
        ```
    
        , where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.
    1. In the [{{clickhouse}} documentation](https://clickhouse.com/docs/ru/operations/settings/settings), find the required settings and copy their names.
    1. Set the required parameters using the `query_settings` option. For example:

        ```bash
        yt clickhouse ctl set-option query_settings "{\"max_execution_time\": 200000,\"max_insert_threads\": 32,\"max_threads\": 32,\"parallel_distributed_insert_select\": 2}"
        ```

{% endlist %}

## {{clickhouse}} config { #ch-config }

The *Clickhouse config* setting is used to manage the {{clickhouse}} part configuration. Configure it to match the standard {{clickhouse}} XML configuration.

The rules for converting a {{clickhouse}} XML configuration to a CHYT YSON configuration are as follows:

- Any non‑repeating node in the {{clickhouse}} configuration is a dictionary (`map`).
- A repeating node is represented as a list (`list`).

{% note info "Example of converting a sample XML configuration to a YSON configuration" %}

#|
|| **XML** | **YSON** ||
||

```xml
<foo>42</foo>
<bar>qwe</bar>
<baz>
    <quux>3.14</quux>
</baz>
<baz></baz>
<baz>hi!</baz>
```

|

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

 ||
|#

{% endnote %}

### {{clickhouse}} config options that may be useful in CHYT { #ch-options }

The main option is `dictionaries` — the external dictionaries configuration. The value must be a list of dictionary configurations.
Each dictionary is configured as a map with the following fields, which preserve the meaning of the original {{clickhouse}} configuration:

- `name` — the name of the external dictionary;
- `source` — [data source](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources/) for the external dictionary;
- `layout` — [representation](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout/) of the external dictionary in the instance memory;
- `structure` — [data schema](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure/) stored in the dictionary;
- `lifetime` — [lifetime](https://clickhouse.com/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime/) of the dictionary.

Other options can be found in the {{clickhouse}} config in the [repository](https://github.com/ytsaurus/ytsaurus/blob/main/yt/chyt/server/clickhouse_config.h).

{% note info %}

The {{clickhouse}} config also has a `settings` option that contains the settings described in the [**Query settings**](#query-settings) section. Thus, query settings can also be defined inside the {{clickhouse}} config.

{% endnote %}

### How to change the {{clickhouse}} configuration { #ch-instruction }

{% list tabs %}

- Web interface

    1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
    1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block or the button **Edit speclet** on the **Speclet** tab in the [Tab panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
    1. Select the **Advanced** section on the left.
    1. Find the **Clickhouse config** section.
    1. In the *Use JSON syntax* field, enter the parameters and their values as a JSON configuration.
        Example of configuring a simple dictionary and setting [**Query settings**](#query-settings) in the `settings` part:

        ```json
        {
            "settings": {
                "max_execution_time": 30
            },
            "dictionaries": [
                {
                    "name": "dict",
                    "layout": {"flat": {}},
                    "structure": {
                        "id": {"name": "key"},
                        "attribute": [
                            {"name": "value_str", "type": "String", "null_value": "n/a"},
                            {"name": "value_i64", "type": "Int64", "null_value": 42}
                        ]
                    },
                    "lifetime": 0,
                    "source": {"yt": {"path": "//home/user/table"}}
                }
            ]
        }
        ```

    1. To apply the changes, click **Confirm**.

- CLI

    1. Install the [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package, if you haven’t done so already.
    1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.
    
        ```bash
        export YT_PROXY=<cluster_name>
        ```
    
    1. Set an environment variable with the controller address:
    
        ```bash
        export CHYT_CTL_ADDRESS=<address>
        ```
    
        , where `<address>` is the controller address. For example, the address for a demo cluster looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
        You can get the controller address from the `controller` field in the output of the following command:
    
        ```bash
        yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
        ```
    
    1. Save the cluster name to an environment variable:
    
        ```bash
        export CLUSTER_NAME=<cluster_name>
        ```
    
        , where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.
    1. Find the required parameters in the list of [{{clickhouse}} settings](https://clickhouse.com/docs/ru/operations/settings/settings) and copy their names.
    1. Set the required parameters using the `clickhouse_config` option. For example:

        ```bash
        yt clickhouse ctl set-option clickhouse_config "{ ... some JSON params}"
        ```

{% endlist %}

## YT config { #yt-config }

The YT part of the instance configuration is set in the `yt_config` option. This option lets you specify advanced settings that aren’t exposed as separate speclet options.

You can view the list of available YT configuration parameters in the [Instance configuration](../../../../../user-guide/data-processing/chyt/reference/configuration.md#yt_config) section.

### How to change the YT configuration { #yt-instruction }

{% list tabs %}

- Web interface

    1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
    1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block or the button **Edit speclet** on the **Speclet** tab in the [Tab panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
    1. Select the **Advanced** section on the left.
    1. Find the **YT config** section.
    1. In the *Use JSON syntax* field, enter the parameters and their values as a JSON configuration, for example:

        ```json
        {
            "subquery": {
                "max_data_weight_per_subquery": 12942417591810
            }
        }
        ```

    1. To apply the changes, click **Confirm**.

- CLI

    1. Install the [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package if you haven’t done so already.
    1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.

        ```bash
        export YT_PROXY=<cluster_name>
        ```

    1. Set an environment variable with the controller address:

        ```bash
        export CHYT_CTL_ADDRESS=<address>
        ```

        where `<address>` is the controller address. For example, the address for a demo cluster looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
        You can get the controller address from the `controller` field in the output of the following command:

        ```bash
        yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
        ```

    1. Save the cluster name to an environment variable:

        ```bash
        export CLUSTER_NAME=<cluster_name>
        ```

        where `<cluster_name>` is the cluster name. For example, the name of a demo cluster is `ytdemo`.
    1. Set the required parameters using the `yt_config` option. For example:

        ```bash
        yt clickhouse ctl set-option yt_config "{\"subquery\":{\"max_data_weight_per_subquery\": 12942417591810}}"
        ```

{% endlist %}

## Instance memory { #memory }

To fine‑tune memory allocation, it’s helpful to understand the memory allocation scheme in a CHYT instance, learn which parameters control each memory amount, and how to change them.

{% note info %}

In memory management terms:

- Watermark — the amount of memory measured from the upper limit of the allocated memory. When this limit is reached, the CHYT instance initiates shutdown according to one of the predefined scenarios.
- Window — a time interval (15 minutes by default) during which the *average* value of watermark lower boundary exceedances is calculated. This helps to ignore random memory spikes.
- RSS (Resident Set Size) — the actual amount of RAM used by the process.
- OOM (out of memory) — memory exhaustion.

{% endnote %}

### Memory allocation of a CHYT instance { #memory-allocation }


The diagram shows how memory is allocated within a CHYT instance: which amounts make up the total limit and which thresholds determine the system’s behavior when memory is low.

![memory](../../../../../../images/chyt_memory.png)

Below is a description of each parameter in the scheme — the memory amounts included in the instance’s total limit and the thresholds:


- `MaxServerMemoryUsage` — the total memory usage limit within a CHYT instance, the sum of the following amounts:

  - `Reader` — the amount of memory allocated for reader caches to pre‑load and speed up data reading.
  - `Caches` — a schematic designation of the memory amount for caches, which includes:

    - `CompressedBlockCache` — a cache for *compressed* data blocks. It holds large amounts because data is stored in compressed form (default behavior), but it takes time to decompress again. It’s used when re‑reading *large* data blocks.
    - `UncompressedBlockCache` — a cache for *uncompressed* data blocks. It’s useful when there are many sequential queries to the same data because it saves resources not only for reading but also for decompressing data.
    - `ChunkMetaCache` — a cache for chunk metadata. Reading chunk metadata precedes any read operation. It’s useful for re‑reading data and saves resources by avoiding repeated retrieval of the same metadata.

  - `CH Memory` — a special memory amount reserved for {{clickhouse}}’s internal needs.
  - `Footprint` — a special reserve “buffer” of memory that allows the process to exceed thresholds without collectively exceeding critical values.

- `MemoryLimit` — a conditional hard limit on the RSS (memory usage) of a CHYT instance, used for the `WatchdogOomWatermark` and `WatchdogOomWindowWatermark` categories.
- `WatchdogOomWatermark` — an additional memory amount used in checks to see if an OOM has occurred: the sum of the current RSS value and `WatchdogOomWatermark` is compared to the specified `MemoryLimit`.
- `WatchdogOomWindowWatermark` — an additional memory amount used in OOM checks. The check follows this algorithm:

  - The average RSS value is calculated over the `Window` time interval (15 minutes by default).
  - `WatchdogOomWindowWatermark` is added to the calculated value.
  - The sum is compared to the specified `MemoryLimit`.

- `ClickHouseWatermark` — an additional memory amount to separate `MaxServerMemoryUsage` from the overall `MemoryLimit`.


### Critical RSS (memory usage) value ranges { #critical-limits }

The RSS scale in the memory allocation diagram marks critical value ranges with numbers — when these are reached, the system terminates the process.


![memory](../../../../../../images/chyt_memory_rss.png)

If the process’s physical RAM amount reaches the following ranges:

- `1` (the average RSS value over more than 15 minutes exceeds the lower boundary of the `WatchdogOomWindowWatermark` range) — the instance performs a graceful shutdown: it stops accepting new queries, waits for active ones to complete, and then shuts down.
- `2` (the average RSS exceeds the lower threshold of `ClickHouseWatermark`) — {{clickhouse}} stops allocating memory for any operations, and the system reports a memory shortage.
- `3` (RSS exceeds the lower boundary of the `WatchdogOomWatermark` range) — the instance shuts down abruptly, and active queries fail with an error.
- `4` (RSS exceeds `MemoryLimit`) — {{product-name}} deletes the instance.


### How to set memory thresholds { #memory-instruction }

We recommend using the web interface:

1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block or the button **Edit speclet** on the **Speclet** tab in the [Tab panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
1. Select the **Advanced** section on the left.
1. Find the **Instance memory** section.
1. In the *Use JSON syntax* field, enter the parameters and their values as a JSON configuration. You can use the following configuration example as a template:

    ```json
    {
        "clickhouse": 10500000000,
        "chunk_meta_cache": 100000000,
        "compressed_cache": 4000000000,
        "uncompressed_cache": null,
        "reader": 500000000,
        "clickhouse_watermark": 10,
        "watchdog_oom_watermark": 0,
        "watchdog_oom_window_watermark": 0,
        "footprint": 2000000000
    }
    ```

1. To apply the changes, click **Confirm**.

## Sticky query distribution { #sticky-query }

Advanced options **Enable sticky query distribution** and **Query sticky group size** control query distribution across instances with caching in mind.

If queries repeat at a given interval, it’s useful to cache their results. The query cache is stored on the instance where it was executed.

By default, queries are distributed across instances at random. To redirect queries to instances where they were previously executed and where their cache is stored, use the following settings:

- **Enable sticky query distribution** — this option enables query distribution across instances based on the hashes of these queries.
- **Query sticky group size** — this option sets the size of the group of instances selected deterministically by the query hash. A coordinator for executing the query is chosen uniformly from among these instances. This is only relevant when the **Enable sticky query distribution** option is enabled.

### How to enable redirecting queries to instances with cache { #cache-instruction }


We recommend using the web interface:

1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block or the button **Edit speclet** on the **Speclet** tab in the [Tab panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
1. Select the **Advanced** section on the left.
1. Find the **Enable sticky query distribution** option and enable it.
1. Find the **Query sticky group size** option and enter a number in the field — the size of the instance group. The query will be randomly directed by the coordinator to one of the instances in the group.
1. To apply the changes, click **Confirm**.
