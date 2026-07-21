# Adding compute resources

Sometimes, when working with cliques, you may experience performance issues due to full utilization of available resources and, as a result, query degradation. In such cases, you need to increase the amount of compute resources.

Compute resources are {{clickhouse}} instances: servers with dedicated CPUs and RAM. Most of the time, users only need to adjust the number of instances.

{% note warning %}

To configure a clique, you must have the `manage` permission for that clique. Check whether you have this permission type on the **ACL** tab in the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs) under **Object permissions**.

{% endnote %}

You can add compute resources to a clique in two ways:

{% list tabs %}

- Through the web interface

    1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
    1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) section or the button **Edit speclet** on the **Speclet** tab in the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
    1. Select the **Resources** tab on the left.
    1. In the _Instances_ field, enter the number of instances for the clique (from 1 to 100).

        {% note info %}

        The default field value is `1`. There is no universal recommendation for choosing the number of instances: the parameter depends on your tasks and is determined experimentally. We recommend starting with the default value.

        {% endnote %}
    1. To apply the changes, click **Confirm**.

- Using the CLI

    1. Install the [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package, if you haven’t done so already.
    1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.

        ```bash
        export YT_PROXY=<cluster_name>
        ```

    1. Set an environment variable with the controller address:

        ```bash
        export CHYT_CTL_ADDRESS=<address>
        ```

        where `<address>` is the controller address. For example, for a demo cluster, the address looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.

        You can get the controller address from the `controller` field in the output of the following command:

        ```bash
        yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
        ```

    1. Save the cluster name to an environment variable:

        ```bash
        export CLUSTER_NAME=<cluster_name>
        ```

        where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.

    1. Set the required number of instances using the `instance_count` option:

        ```bash
        yt clickhouse ctl set-option instance_count COUNT --alias chyt_example_clique
        ```

        where `COUNT` is the number of instances.

{% endlist %}

## Advanced compute resource settings { #advanced }

{% note warning %}

These options are intended for advanced users. If you’re unsure whether you need them, keep the default values.

{% endnote %}

Advanced settings include CPU and RAM parameters for each instance. When choosing CPU and RAM values, consider the physical resources of your servers and the requirements of your clique workloads.

Recommended values:

- 1–100 CPU cores per instance;
- 20–300 GB of RAM per instance.

{% note warning %}

Choose option values so that they don’t exceed the physical resources of the servers. For example, on a server with 10 CPU cores and 40 GB of memory, you can’t allocate an instance with 16 cores and 65 GB.

{% endnote %}

You can set the `Instance CPU` and `Instance Total Memory` options via:

{% list tabs %}

- Web interface

    1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
    1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) section or the button **Edit speclet** on the **Speclet** tab in the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
    1. Select the **Resources** tab on the left.
    1. In the _Instance CPU_ field, specify the number of CPU cores per instance.
    1. In the _Instance Total Memory_ field, specify the amount of RAM per instance.
    1. To save the settings, click **Confirm**.

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

        where `<address>` is the controller address. For example, for a demo cluster, the address looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.

        You can get the controller address from the `controller` field in the output of the following command:

        ```bash
        yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
        ```

    1. Save the cluster name to an environment variable:

        ```bash
        export CLUSTER_NAME=<cluster_name>
        ```

        where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.

    1. Set the amount of CPU to be allocated for each clique instance using the `instance_cpu` option:


        ```bash
        yt clickhouse ctl set-option instance_cpu CPU_AMOUNT --alias chyt_example_clique
        ```

        where `CPU_AMOUNT` is the number of CPU cores.

    1. Set the amount of RAM to be allocated for each clique instance using the `instance_total_memory` option:

        ```bash
        yt clickhouse ctl set-option instance_total_memory RAM_AMOUNT --alias chyt_example_clique
        ```

        where `RAM_AMOUNT` is the amount of RAM in GB.

{% endlist %}

Note that with the **Restart on speclet change** option enabled, any setting changes will trigger an automatic clique restart, and the restart will take some time. You can enable the **Restart on speclet change** option on the **General** tab in the speclet interface dialog box.
