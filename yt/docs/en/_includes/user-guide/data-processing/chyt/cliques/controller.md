# Controller

Controller is a service responsible for administering and maintaining the operability of private cliques.

## Tasks solved { #why }

- Store the clique configuration persistently.
- Generate the appropriate [instance configurations](../../../../../user-guide/data-processing/chyt/reference/configuration.md) based on the clique configuration and {{product-name}} cluster configuration.
- Start and restart Vanilla operations on the clique, if they have been terminated because of failures or maintenance works on the cluster.

{% if audience == "internal" %}
The system also supports integration with IDM to [issue clique permissions](../../../../../user-guide/data-processing/chyt/cliques/access.md).
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

## Clique permissions { #access }

{% if audience == "internal" %}
Within a couple of minutes of [creating a clique](#create), the creator will be granted the roles **responsible**, **use**, and **manage** for the clique in IDM.
{% else %}
Clique permissions are stored at `//sys/access_control_object_namespaces/chyt/<alias>/@principal_acl`. Replace the `@principal_acl` attribute with the necessary permissions:

- `use`: Use the clique (run queries).
- `read`: Read the clique's configuration.
- `manage`: Edit the clique's configuration
- `remove`: Remove the clique.

{% endif %}

## Commands { #commands }

### Create { #create }

`yt clickhouse ctl create <alias>`: Create a clique with the specified alias (name). The clique's alias must meet the ID requirements, that is, include only Latin characters, digits, and underscores (`_`) and never begin with a digit. You can also use `*` in the beginning (however, it would be ignored) and the `-` character.

Once the command is successfully executed, the `//sys/strawberry/chyt/<alias>/speclet` node is created in Cypress to store the clique's configuration.

In the web interface, the `//sys/strawberry/chyt/<alias>` node will now additionally show the current state of the clique, including possible errors preventing the Vanilla operation from running on the clique's instances.

Cliques are created in an inactive state in which they cannot accept or process queries. To activate your clique, first set it up using appropriate commands.

### Remove { #remove }

`yt clickhouse ctl remove <alias>`: Fully delete the clique and its persistent state. Once the operation is completed, you can't recover the clique anymore. To execute this operation, you only need the **Manage** role for this clique.

### Exists { #exists }

`yt clickhouse ctl exists <alias>`: Make sure that the clique exists on the cluster. The result of the command is a `bool` value.

Result example:

```bash
$ yt clickhouse ctl exists test_clique_name
%true
```

### Status { #status }

`yt clickhouse ctl status <alias>`: Return the current clique status.

Result example:
```bash
$ yt clickhouse ctl status test_clique_name
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
$ yt clickhouse ctl list
[
    "<clique_name_1>";
    "<clique_name_2>";
    "<clique_name_3>";
]
```

### Set-option { #set_option }

`yt clickhouse ctl set-option <key> <value> [--alias <alias>]`: Set the speclet's `key` option to `value`. To execute this command, you only need the **manage** role for this clique.

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

To view all the set options, run the [get-speclet](#get-speclet) command. For a description of all available options, see [Configuration](../../../../../user-guide/data-processing/chyt/reference/configuration.md#options).

### Remove-option { #remove_option }

`yt clickhouse ctl remove-option <key> [--alias <alias>]`: Remove a previously assigned option value from the clique configuration. The options omitted in the clique configuration will be taken from their defaults when the clique's operation is started.

### Get-speclet { #get-speclet }

`yt clickhouse ctl get-speclet [--alias <alias>]`: Get all the options set for the clique.

Result example:
```bash
$ yt clickhouse ctl get-speclet --alias test_clique_name
{
    "family" = "chyt";
    "stage" = "production";
    "instance_count" = 8;
    "active" = %true;
    "pool" = "chyt";
    ...
}
```

### Set-speclet { #set-speclet }

`yt clickhouse ctl set-speclet <speclet> [--alias <alias>]`: Set the clique's speclet in YSON string format. To execute this command, you only need the **manage** role for this clique.

Example:
```bash
yt clickhouse ctl set-speclet '{instance_count=2;pool="chyt";}' --alias test_clique_name
```

### Start { #start }

`yt clickhouse ctl start <alias> [--untracked]`: Start a previously created clique.

With the `untracked` option, you can start the clique without linking it to the controller and without any guarantees. This option is intended for starting test cliques without a compute pool. We strongly advise against running production cliques without specifying a compute pool, as this may make the clique unstable. The clique will start under the user's name. The controller doesn't monitor the state of such cliques or restart them if there are any errors. If the clique's operation ends in an error, you'll need to restart the clique manually by running the command again.

To execute this command, you only need the **manage** role for this clique.

Result example:
```bash
$ yt clickhouse ctl start test_clique_name
{
    "status" = "Ok";
    "operation_state" = "initializing";
    "operation_url" = ...;
}
```

### Stop { #stop }

`yt clickhouse ctl stop <alias>`: Stop the clique. The clique will be stopped. All pool resources allocated to the clique will be released. The clique configuration won't be deleted. The user can restart the clique at any time using the `start` command. To execute this command, you only need the **manage** role for this clique.

## Available options { #options }

For a list of the clique options that can be set by the `set-option` command, see [Configuration](../../../../../user-guide/data-processing/chyt/reference/configuration.md#options).

