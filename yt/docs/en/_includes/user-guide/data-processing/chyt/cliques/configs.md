# Clique settings

This article describes clique [options](#options) and how to manage them: how to view and change the configuration via Cypress, CLI, or web interface. The article will be useful for anyone who wants to configure a clique or change its behavior.

## Speclet { #speclet }

A clique’s configuration is described by a single [YSON document](../../../../../user-guide/storage/yson-docs.md), called a *speclet*.

The speclet is stored in Cypress at the path `//sys/strawberry/chyt/<alias>/speclet`. It is a set of [options](#options) that are interpreted by the [controller](../../../../../user-guide/data-processing/chyt/controller.md) when the clique starts. Based on these options, the controller generates the final configuration for CHYT instances.

Example of a speclet for the `ch_public` clique on the `ytdemo` cluster:

```yson
{
    "active": true,
    "enable_geodata": false,
    "family": "chyt",
    "instance_count": 1,
    "instance_cpu": 1,
    "instance_memory": {
        "chunk_meta_cache": 100000000,
        "clickhouse": 100000000,
        "clickhouse_watermark": 10,
        "compressed_cache": 100000000,
        "footprint": 500000000,
        "log_tailer": 100000000,
        "reader": 100000000,
        "watchdog_oom_watermark": 0,
        "watchdog_oom_window_watermark": 0
    },
    "pool": "chyt",
    "stage": "production"
}
```

### View clique options { #where }

You can view clique options via:

- Viewing the speclet in Cypress at the path `//sys/strawberry/chyt/<alias>/speclet`;
- Running the following CLI commands: `yt clickhouse ctl get-speclet` and `yt clickhouse ctl status`;
- Viewing the **Speclet** tab in the [web interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui) on the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).

### Change clique options { #how-to-change }

To change clique options, use:

- Editing the file in Cypress: open the `speclet` file at the path `//sys/strawberry/chyt/<alias>/speclet` and click **Edit**;
- The CLI utility: run the `yt clickhouse ctl set-option` command with the needed option from the list of [available options](#options);
- The web interface: click ![](../../../../../../images/edit-btn.png){width=24 height=24} **Edit speclet** in the upper‑right corner, in the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui) block, or the button **Edit Speclet** tab on the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui).

For step‑by‑step instructions and practical examples of setting options, see the [Adding compute resources](../../../../../user-guide/data-processing/chyt/how-to-guides/manage-resources.md) and [Configuring advanced settings](../../../../../user-guide/data-processing/chyt/how-to-guides/manage-advanced-settings.md) sections.

### Available options { #options }

Below are the clique options available for configuration via the [web interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md) or via the CLI with `set-option` command (default values are shown in square brackets).

Options are roughly grouped into three sections corresponding to the tabs in the speclet settings dialog.

- **Basic options** — *General* tab

    #|
    || **Option** | **Description**||
    || `active` [`false`] | If enabled, the controller will try to run the Vanilla operation assigned to the clique. If set to `false`, the clique will be inactive — it won’t process requests or consume compute pool resources. This option can be useful for temporarily disabling a clique while preserving its configuration.

    In the web interface, use the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui) to start and stop the clique ||

    || `pool` | The name of the compute pool where the clique operation should be run. To set this option, you must have the `Use` permission for the specified compute pool. Setting this option is required to run a clique operation under the controller ||

    || `preemption_mode` [`normal`] | The preemption mode in which the {{product-name}} clique operation will be started. This setting is intended for advanced users only. If you’re unsure whether you need it, we recommend keeping the default value.

    For more information about preemption, see the [Resources and preemption](../../../../../user-guide/data-processing/chyt/resources.md) section ||

    || `restart_on_controller_change` [`true`] | If enabled, the clique will automatically restart whenever the controller changes. Otherwise, you’ll need to manually restart the clique to apply its settings ||

    || `restart_on_speclet_change` [`true`] | If enabled, the clique will automatically restart whenever the speclet changes. Otherwise, you’ll need to manually restart the clique to apply its settings ||

    |#

- **Compute resources** — *Resources* tab

    #|
    || **Option** | **Description**||
    || `instance_count` [1] | The number of clique instances ||

    || `instance_cpu` | The amount of CPU allocated to each clique instance. This setting is intended for advanced users only. If you’re unsure whether you need it, we recommend keeping the default value ||

    || `instance_total_memory` | The amount of Random Access Memory (RAM, in bytes) allocated to each clique instance. This setting is intended for advanced users only. If you’re unsure whether you need it, we recommend keeping the default value ||

    |#

- **Advanced options** — *Advanced* tab


    {% note warning %}

    These settings are intended for advanced users only. If you’re unsure whether you need them, we recommend keeping the default values. For more information, see the [Configuring advanced settings](../../../../../user-guide/data-processing/chyt/how-to-guides/manage-advanced-settings.md) section.

    {% endnote %}

    #|
    || **Option** | **Description**||

    || `enable_geodata` [`true`] | Automatic configuration of system dictionaries required for [certain ClickHouse Geo functions](https://clickhouse.com/docs/en/sql-reference/functions/ym-dict-functions/) ||

    || `query_settings` | A dictionary with default settings applied for all queries executed within the clique ||

    || `clickhouse_config` | A dictionary with the {{clickhouse}}-part configuration of CHYT instances. These settings affect the logic of the ClickHouse compute engine and are identical to the original {{clickhouse}} engine settings ||

    || `yt_config` | A dictionary with the {{product-name}}-part configuration of CHYT instances ||

    || `enable_sticky_query_distribution` [`false`] | Distribute queries across instances based on the hashes of those queries. This increases the efficiency of cache use ||

    || `query_sticky_group_size` [2] | The size of the group of instances selected deterministically based on the query hash. A coordinator for executing the query will be selected uniformly from among these instances. This is only relevant when `enable_sticky_query_distribution=true` ||

    |#

## Useful links

[Adding compute resources](../../../../../user-guide/data-processing/chyt/how-to-guides/manage-resources.md)  
[Configuring advanced settings](../../../../../user-guide/data-processing/chyt/how-to-guides/manage-advanced-settings.md)
