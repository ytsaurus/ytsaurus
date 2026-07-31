# The clique web interface in {{product-name}}

The web interface is a convenient way to manage [cliques](../../../../../user-guide/data-processing/chyt/general.md). It is suitable for:

- Quick one‑off tasks.
- Visual monitoring of the clique’s status.
- Configuration without writing scripts.
- Quick parameter review enabled by a clear visual structure.

## How to open the clique interface { #where }

1. In the main menu of {{product-name}}, on the left, select **Cliques**.
1. In the **CHYT cliques** section, select the required clique from the list or [create a new one](../../../../../user-guide/data-processing/chyt/how-to-guides/create-start.md#create).

    {% note info %}

    To get familiar with the web interface, use the public clique `ch_public`. This is a publicly accessible clique that runs on every {{product-name}} cluster.

    {% endnote %}

## Main sections of the web interface { #ui }

![UI](../../../../../../images/clique-ui-os.png){ .center }

_1. [Header](#header) — displays the clique name._  
_2. [Action buttons](#action-menu) — use them to manage cliques._  
_3. [Clique characteristics block](#params) — here you can view the clique’s status._  
_4. [Tabs panel](#tabs) — links to the speclet (configuration), ACL, and logs tabs._

### Header {#header}

The header (1) displays basic information:

- The name of the cluster where the clique is located, and a button to change it.
- The name of section — **CHYT cliques**.
- Buttons:
  - ![add to favourites](../../../../../../images/add-to-favourites-btn.png){width=24 height=24} _Add to favourites_ — add the clique to your favourites.
  - ![view favourites](../../../../../../images/view-favourites-btn.png){width=24 height=24} _View favourites_ — view your favourite cliques.
- The clique name.
- The **Create clique** button for [creating a new clique](../../../../../user-guide/data-processing/chyt/how-to-guides/create-start.md#create).

### Action buttons {#action-menu}

On the right, there is an action buttons block (2):

- ![sql](../../../../../../images/sql-btn.png){width=24 height=24} _SQL_ — go to the web interface for running queries — [Query Tracker](../../../../../user-guide/query-tracker/about.md).
- ![start](../../../../../../images/start-btn.png){width=24 height=24} _Start_ — start the clique.
- ![stop](../../../../../../images/stop-btn.png){width=24 height=24} _Stop_ — stop the clique.
- ![remove](../../../../../../images/remove-btn.png){width=24 height=24} _Remove_ — delete the clique.
- ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} _Edit speclet_ — edit the [speclet](../../../../../user-guide/data-processing/chyt/cliques/configs.md#speclet) — the clique’s configuration file.

### Clique characteristics {#params}

Below the header is the clique characteristics block (3). Key parameters:

- `Health` — shows the operational status of the clique. The parameter can have the following values:
  - `Good` — the clique is healthy and ready to accept queries.
  - `Pending` — this is the state before `Good`, indicating that the clique is waiting to start.
  - `Failed` — the clique is unavailable due to a failure.

  {% note info %}
  
  When the `Health` parameter is set to `Failed`, the [Strawberry Controller](../../../../../user-guide/data-processing/chyt/controller.md) restarts the Vanilla operation. If the issue is resolved, `Health` will change to `Pending` and then to `Good`, or back to `Failed`, in which case the controller will try to restart the Vanilla operation again.

  {% endnote %}
  
- `State` — the clique state: `Active` / `Inactive`. Shows whether the clique is running or stopped.
- `Pool` — the name of the compute [pool](../../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md#scheduler), which is a link to pool's web interface.
- `Instances`, `Cores`, `Memory` — the number of instances and computing resources (CPU cores and RAM) allocated to the clique.
- `YT operation Id` — a link to the [YT operation](../../../../../user-guide/data-processing/chyt/cliques/yt-operation-ui.md) interface that corresponds to the clique.

Other characteristics are for reference only and help to clarify the main metrics and identify the causes of failures.

### Tabs panel {#tabs}

Key information about the clique is displayed in block (4) on the tabs:

- **Speclet** — shows the contents of the YSON document with the clique’s settings ([speclet](../../../../../user-guide/data-processing/chyt/cliques/configs.md#speclet)).
- **ACL** — shows which access permissions to the clique have been granted and for which user groups. For more details, see the [Access permissions](../../../../../user-guide/data-processing/chyt/cliques/access.md) section.
- **Query Logs** — opens the query logs table. For more details, see [Getting Query Logs](../../../../../user-guide/data-processing/chyt/how-to-guides/query-logs.md).

## Useful links

[Clique settings](../../../../../user-guide/data-processing/chyt/cliques/configs.md)
