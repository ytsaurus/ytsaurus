# Controller

Controller is a service responsible for administering and maintaining the operability of private cliques.

## Tasks solved { #why }

- Store the clique configuration persistently.
- Generate the appropriate [instance configurations](../../../../../user-guide/data-processing/chyt/reference/configuration.md) based on the clique configuration and {{product-name}} cluster configuration.
- Start and restart Vanilla operations on the clique, if they have been terminated because of failures or maintenance works on the cluster.

{% if audience == "internal" %}
The system can also be integrated with IDM to [issue rights for the clique](../../../../../user-guide/data-processing/chyt/cliques/administration.md#access)).
{% else %}{% endif %}

## Controller CLI { #cli }

To access the controller, you can use the Command Line Interface (CLI) provided with the `ytsaurus-client` package.

{% note info %}

We're continuously adding new [commands](#commands) for the controller, so some of them might not be in the documentation yet. To view all the currently available commands, use `yt clickhouse ctl --help`.

{% endnote %}

All the commands have the optional `--proxy` parameter for setting the {{product-name}} cluster to access the cliques from:

```bash
yt --proxy <cluster_name> clickhouse ctl list
```
If you omit this argument, the default value from the `YT_PROXY` environment variable will be used.

## Right for the clique { #access }

{% if audience == "internal" %}
Within a couple of minutes since [clique creation](#create), the creator will be granted the **Responsible**, **Use**, and **Manage** roles for the clique in IDM.
{% else %}
The rights for the clique are stored at the path `//sys/access_control_object_namespaces/chyt/<alias>/@principal_acl`. Replace the `@principal_acl` attribute with the appropriate rights:

- `use`: Use the clique (run queries).
- `read`: Read the clique's configuration.
- `manage`: Edit the clique's configuration
- `remove`: Remove the clique.

{% endif %}

## Commands { #commands }

### Create { #create }

`yt clickhouse ctl create <alias>`: Create a clique with the specified alias (name). The clique's alias must meet the ID requirements, that is, include only Latin characters, digits, and underscores (`_`) and never begin with a digit. You can also use `*` in the beginning (however, it would be ignored) and the `-` character.

Once the command is completed, the `//sys/clickhouse/strawberry/<alias>/speclet` node is created in Cypress to store the clique's configuration.

Moreover, in the web interface, the `//sys/clickhouse/strawberry/<alias>` node will now show the current status of the clique, including possible errors preventing the Vanilla operation from running on the clique's instances.

Cliques are created in an inactive state in which they cannot accept or process queries. To activate your clique, first set it up using appropriate commands.

### Remove { #remove }

`yt clickhouse ctl remove <alias>`: Fully delete the clique and its persistent state. Once the operation is completed, you can't recover the clique anymore. To execute this operation, you only need the **Manage** role for this clique.

### Exists { #exists }

`yt clickhouse ctl exists <alias>`: Make sure that the clique exists on the cluster. The result of the command is a `bool` value.

Result example:

```bash
yt clickhouse ctl exists <clique_name>
%true
```

### Status { #status }

`yt clickhouse ctl status <alias>`: Return the current clique status.

Result example:
```bash
yt clickhouse ctl status <clique_name>
{
    "status" = "Waiting for restart: speclet changed";
    "operation_state" = "running";
    "operation_url" = "https://ytsaurus.ru/<cluster_name>/operations/de569537-f8cded7d-3fe03e8-b5278252";
    "speclet_diff" = {
        "enable_geodata" = {
            "new_value" = %true;
            "old_value" = %false;
        };
    };
}
```

### List { #list }

`yt clickhouse ctl list`: Look up all the aliases of all the cliques running in the cluster.

Result example:

```bash
yt clickhouse ctl list
[
    "<clique_name_1>";
    "<clique_name_2>";
    "<clique_name_3>";
]
```

### Set-option { #set_option }

`yt clickhouse ctl set-option <key> <value> [--alias <alias>]`: Set the speclet's `key` option to `value`. To execute this command, you only need the **Manage** role for this clique.

An arbitrary YSON value is used for `value`. For example, the value can be:
- A number:
```bash
yt clickhouse ctl set-option instance_count 1
```
- A boolean value. Such values begin with `%` in YSON syntax:
```bash
yt clickhouse ctl set-option enable_geodata %false
```
- An ID string:
```bash
yt clickhouse ctl set-option query_settings/chyt.execution.join_policy local
```
- An arbitrary string. If the string includes something different from alphanumeric characters, underscores `_` and dashes `-`, to be sure that it's parsed correctly as a string, enclose it in double quotes (`"like this"`). Most of the popular command line implementations, such as bash, zsh, and others, parse `"` as a control character and might deliver values without the enclosing double quotes to the CLI. To overcome this issue, enclose the argument additionally in single quotes:
```bash
yt clickhouse ctl set-option chyt_version '"stable-2.08"'
```

The optional `--alias <alias>` argument sets the alias for the clique to be updated. You can omit this argument; in this case, its value will be taken from `CHYT_ALIAS`. If it is empty, an error will be returned.

To look up all the assigned options, access the speclet node at the `//sys/clickhouse/strawberry/<alias>/speclet` path.

### Remove-option { #remove_option }

`yt clickhouse ctl remove-option <key> [--alias <alias>]`: Remove a previously assigned option value from the clique configuration. The options omitted in the clique configuration will be taken from their defaults when the clique's operation is started.

## Available options { #options }

Here are the clique's options that can be set by the `set-option` command (the default values are in square brackets):

- `active` [`%false`]: If the option is enabled, the controller will try to run the Vanilla operation assigned to the clique. At `%false`, the clique will be inactive: it won't process queries or use resources of the compute pool. Use this option, if you want to disable the clique for a while, preserving its configuration.

- `pool`: Name of the compute pool where you want to run the clique's operation. To enable this option, the user needs the **Use** rights for the specified compute pool. To run the clique's operation, be sure to enable this option first.

- `enable_geodata` [`%true`]: Automatically set up the system dictionaries needed for [certain geo features in ClickHouse](https://clickhouse.com/docs/en/sql-reference/functions/ym-dict-functions/).

- `restart_on_speclet_change` [`%true`]: If this option is enabled, the clique will be automatically restarted on any reconfiguring (speclet change). Otherwise, you will need to restart the clique manually to apply its settings.

- `query_settings`: Dictionary storing the default settings for all the queries sent to the clique.

- `yt_config`: Dictionary with the {{product-name}} configuration for certain CHYT instances. This part of the configuration will be written to the generated instance configuration as it is. We strongly discourage you from using this option because the structure of this configuration is pretty sophisticated. Use other options, whenever possible, to set changes to the configuration in a more straightforward way.

- `clickhouse_config`: Dictionary with the ClickHouse configuration for certain CHYT instances. It's similar to the `yt_config` option. We strongly discourage you from using this option.

- `instance_count` [1]: Count of the clique's instances.

- `instance_cpu` [16]: CPU resources to be allocated to each clique's instance.
- `instance_total_memory` [71940702208 (67 Gib)]: RAM (in bytes) to be allocated to each clique's instance.
- `clique_cpu`: Total CPU resources allocated to the clique. You can't use this option together with `instance_cpu`.
- `clique_memory`: Total memory allocated to the clique. You can't use this option together with `instance_total_memory`.
