# Getting CHYT and {{clickhouse}} versions

Component versions in {{product-name}} are important parameters for stable and consistent operation of the entire system. Version incompatibilities can cause errors during operations. Therefore, identifying the current component versions helps you quickly find the source of the problem.

You can get the versions via:

{% list tabs %}

- Web interface

    1. Go to the [web interface](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where) of the clique.
    1. In the clique characteristics block, find the **YT operation Id** parameter and click the link provided in it.
    1. In the opened Vanilla operation interface, on the left, in the *Tabs panel*, select the **Details** tab.
    1. In the **Description** section, find the **Artifacts** block.
    1. To see the full list of parameters, click the **Show more**.
    1. In the list of parameters, find the `ytserver-clickhouse` field. It contains:
        - `ch_version` — {{clickhouse}} version;
        - `version` — CHYT clique version.

- Query to the clique

    1. Go to the Query Tracker web interface: click ![sql](../../../../../../images/sql-btn.png){width=24 height=24} in the upper-right corner of the [Action buttons](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) block.
    1. Make sure that the CHYT engine is selected in the panel at the top.
    1. In the opened window, enter and run the SQL query:

    ```sql
    SELECT chytVersion(), version();
    ```

{% endlist %}

{% note tip "Tip" %}

If you contact technical support about issues with the clique, check the component versions in advance and provide them to the support specialist. This will speed up diagnostics and help find a solution faster.

{% endnote %}
