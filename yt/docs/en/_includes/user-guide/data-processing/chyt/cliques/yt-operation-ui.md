# Clique as a YT operation

In the {{product-name}} system, a CHYT clique is not a separate entity but a standard distributed [Vanilla operation](../../../../../user-guide/data-processing/operations/vanilla.md) that consists of a set of tasks — jobs.

The YT operation interface helps you track the click’s status, analyze jobs, and troubleshoot issues if something goes wrong.

## How to open the YT operation web interface { #get-to-section }

You can open the YT operation interface in three ways:

- Using the link in the `YT Operation Id` parameter from the [clique interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui);
- Using the link from the *Current operation id* field in the Strawberry operation description, which is stored in Cypress at the path `//sys/strawberry/chyt/<alias>`;
- Via the CLI, using the link from the output of the `yt clickhouse ctl status` command.

## Key interface blocks { #ui-main-sections }

![UI](../../../../../../images/chyt-yt-operation.png){ .center }

_1. Basic information about the YT operation._  
_2. Job summary._  
_3. Tabs panel._  
_4. **Description** section._  
_5. **Tasks** section._

Blocks that are useful for practical tasks:

1. **Description** section (4). The section is located on the **Details** tab in the Tabs panel (3). It contains system information about the clique and a number of useful parameters — for example, CHYT and {{clickhouse}} versions. To learn how to view component versions, see the [Get CHYT and {{clickhouse}} versions](../../../../../user-guide/data-processing/chyt/how-to-guides/versions.md) section.
1. Job summary section (2). It is located to the right of the basic operation information (1). It displays information about job statuses: *Running*, *Completed*, *Failed*. The statuses are links. They lead to the **Jobs** tab in the tabs panel (3), where you can view data for each job and filter the job list by various parameters. For more information on job diagnostics, see the [Check failed jobs](../../../../../user-guide/data-processing/chyt/how-to-guides/failed-jobs.md) section. Keep in mind that one job corresponds to one instance in the clique — that is, to one {{clickhouse}} server.
1. The **Tasks** section (5) displays job statistics. In the context of CHYT instances, the following terms are used:

    #|
    || **Status** | **Meaning** | **What to look for** ||
    || `total` | Total number of instances | — ||
    || `running` | The instance is up and running | If `running` = `total`, everything is fine ||
    || `pending` | The instance has not been launched yet | A non‑zero value means the clique lacks resources to launch all instances ||
    || `completed` | The instance was shut down normally — it completed all the queries | — ||
    || `failed` | The instance crashed | Common causes: out‑of‑memory (OOM) or an error in the CHYT / {{clickhouse}} code ||
    || `aborted` | The instance was aborted | If the reason is `preemption`, it indicates a lack of resources ||
    || `lost` | — | A failure outside CHYT, in other components ||
    |#

