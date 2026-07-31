# Creating, Starting, and Stopping a Clique

{% note info "Note" %}

The operations and actions described below do not start computations — they only prepare the clique for use.

{% endnote %}

## How to create a new clique { #create }

You can create a new clique in two ways:

{% list tabs %}

- Via the web interface

  1. In the main menu of {{product-name}}, go to the **Cliques** section.
  1. In the opened **CHYT cliques** section, click **Create clique** in the upper‑right corner.
  1. Fill in the form fields:
     - _Alias name_ — the clique name;
     - _Instance count_ — the number of clique instances (from 1 to 100);
     - _Pool tree_ — pool tree, select a value from the list or keep the default value;
     - _Pool_ — the name of the compute [pool](../../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md#scheduler) where you want to run the clique operation.

        {% note info %}

        This option is optional at the clique creation stage. You must [set it](#set-up-options) before starting the clique.

        {% endnote %}
  1. To complete the creation, click **Confirm**.

  After confirmation, you’ll be forward to the created clique’s interface, and it will become available in the clique list in the **CHYT cliques** section.

- Using the CLI

  1. Install [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package, if you haven’t done so yet.
  1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.

      ```bash
      export YT_PROXY=<cluster_name>
      ```

  1. Set an environment variable with the controller address for the same reason:

      ```bash
      export CHYT_CTL_ADDRESS=<address>
      ```

      , where `<address>` is the controller address. For example, for the demo cluster, the address looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
      You can get the controller address from the `controller` field in the output of the command:

      ```bash
      yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
      ```

  1. Also save the cluster name to an environment variable:

      ```bash
      export CLUSTER_NAME=<cluster_name>
      ```

      , where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.
  1. Create a clique using a command from [CHYT Controller CLI](../../../../../user-guide/data-processing/chyt/controller.md):

      ```bash
      yt clickhouse ctl create chyt_example_clique
      ```

{% endlist %}

## Setting up options for starting { #set-up-options }

After creating a clique, you need to configure it — set the required [options](../../../../../user-guide/data-processing/chyt/cliques/configs.md#options). The main option for starting a clique is `pool`; you need to pass the name of the compute pool where the clique instances will run.

Before setting the `pool` option, make sure you have the `Use` permission for the specified pool. Check your permissions on the **ACL** tab in the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui) in the **Object permissions** section. If you’re unsure which pool to use, contact the {{product-name}} cluster administrator.


You can set the `pool` option via:

{% list tabs %}

- Web interface

  1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
  1. Click ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} in the upper‑right corner, in the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui) block, or the button **Edit speclet** on the **Speclet** tab in the [Tabs panel](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui).
  1. In the opened settings window, enter the compute pool name in the _Pool_ field.
  1. To finish, click **Confirm**.

  {% note info %}

  Since clique operations are run under the system robot `robot-strawberry-controller`, grant this robot the `Use` permission for the specified pool in advance.

  {% endnote %}

- CLI

  1. Install [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package, if you haven’t done so yet.
  1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.

      ```bash
      export YT_PROXY=<cluster_name>
      ```

  1. Set an environment variable with the controller address for the same reason:

      ```bash
      export CHYT_CTL_ADDRESS=<address>
      ```

      , where `<address>` is the controller address. For example, for the demo cluster, the address looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
      You can get the controller address from the `controller` field in the output of the command:

      ```bash
      yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
      ```

  1. Also save the cluster name to an environment variable:

      ```bash
      export CLUSTER_NAME=<cluster_name>
      ```

      , where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.
  1. Set the option using the command:

      ```bash
      yt clickhouse ctl set-option pool chyt_example_pool --alias chyt_example_clique
      ```

Now the clique is configured and all settings are saved in the [speclet](../../../../../user-guide/data-processing/chyt/cliques/configs.md#speclet) in Cypress.


{% endlist %}

## How to start a clique { #start }

You can start a configured clique via:

{% list tabs %}

- Web interface

  1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
  1. Click ![start](../../../../../../images/start-btn.png){width=24 height=24} in the upper‑right corner, in the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block.
  1. Make sure that:
     - the `Health` parameter changed to the `Pending` state and then, after some time, to `Good`;
     - the `State` parameter has the `Active` value.

     You can view the statuses of these parameters in the clique interface — in the block with [characteristics](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui).
  1. Check the clique’s functionality. To do this, make a test query in the **Query Tracker** interface:
     - click ![sql](../../../../../../images/sql-btn.png){width=24 height=24} in the upper‑right corner, in the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block;
     - in the opened window, enter and run an SQL query.

- CLI

  1. Install [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package, if you haven’t done so yet.
  1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.

      ```bash
      export YT_PROXY=<cluster_name>
      ```

  1. Set an environment variable with the controller address for the same reason:

      ```bash
      export CHYT_CTL_ADDRESS=<address>
      ```

      , where `<address>` is the controller address. For example, for the demo cluster, the address looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
      You can get the controller address from the `controller` field in the output of the command:

      ```bash
      yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
      ```

  1. Also save the cluster name to an environment variable:

      ```bash
      export CLUSTER_NAME=<cluster_name>
      ```

      , where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.
  1. Start the clique using the command:

      ```bash
      yt clickhouse ctl start chyt_example_clique
      ```

  1. Check the clique status using the `status` command. When the clique operation is started, the `status` field value should become `Ok`, and `operation_state` should change to `running`:

      ```bash
        $ yt clickhouse ctl status chyt_example_clique
        {
            "status" = "Waiting for restart: oplet does not have running yt operation";
        }
        # a few moments later
        $ yt clickhouse ctl status chyt_example_clique
        {
            "status" = "Ok";
            "operation_state" = "running";
            "operation_url" = "https://domain.com/<cluster_name>/operations/<some_hash>";
        }
      ```

  1. Make sure the clique is working. To do this, run a test query, for example:

      ```bash
      yt clickhouse execute --alias chyt_example_clique 'select 42'
      ```

{% endlist %}

{% note warning %}

If the сlique has not started within 10 minutes, check the YT operation status:

- in the web interface — via the link from the `YT operation id` parameter;
- in the CLI — via the `operation_url` link from the output of the `status` command.

{% if audience == "public" %}
If you can’t resolve the issue yourself, ask for help in the [{{product-name}} chat](https://t.me/ytsaurus_ru) or in your personal support channel.
{% endif %}


{% endnote %}

## How to stop a clique { #stop }


You can stop a clique via:

{% list tabs %}

- Web interface

  1. Open the clique interface as described in the [How to open the clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) section.
  1. Click ![stop](../../../../../../images/stop-btn.png){width=24 height=24} in the upper‑right corner, in the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block.
  1. Make sure that:
     - the `Health` parameter changed to the `Pending` state and then, after some time, to `Failed`;
     - the `State` parameter has the `Inactive` value.

     You can view the statuses of these parameters in the clique interface — in the block with [characteristics](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui).


- CLI

  1. Install [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) as part of the `ytsaurus-client` package, if you haven’t done so yet.
  1. Save the proxy address to an environment variable. This allows you to avoid specifying the {{product-name}} cluster using the `--proxy` argument in every subsequent command.

      ```bash
      export YT_PROXY=<cluster_name>
      ```

  1. Set an environment variable with the controller address for the same reason:

      ```bash
      export CHYT_CTL_ADDRESS=<address>
      ```

      , where `<address>` is the controller address. For example, for the demo cluster, the address looks like this: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
      You can get the controller address from the `controller` field in the output of the command:

      ```bash
      yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
      ```

  1. Also save the cluster name to an environment variable:

      ```bash
      export CLUSTER_NAME=<cluster_name>
      ```

      , where `<cluster_name>` is the cluster name. For example, the demo cluster name is `ytdemo`.
  1. Stop the clique using the command:

      ```bash
      yt clickhouse ctl stop chyt_example_clique
      ```

{% endlist %}
