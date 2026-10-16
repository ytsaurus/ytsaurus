# Update strategies for {{product-name}}

This article describes the strategies for updating {{product-name}} cluster components, their differences, and configuration procedures. Choose a strategy based on your cluster availability requirements during the update.

## Overview of strategies {#strategies}

An update strategy determines whether the operator restarts component pods simultaneously or sequentially, one by one. This affects cluster availability during the update.

To select a strategy, define update rules in the `updatePlan` field. In each rule, you specify which components to update and which strategy to use. A single rule can cover the entire cluster, individual components, or groups of components. For more details, see the [Configuring the updatePlan field](../../../admin-guide/update/update_strategy.md#updateplan) section.

The operator supports three update strategies:

#|
|| **Strategy** | **Description** | **Cluster availability** ||
|| `BulkUpdate` | The operator uses this strategy by default if you don’t explicitly specify one. The operator simultaneously deletes and recreates all component pods. When updating master servers, the operator enables safe mode and creates snapshots. Schedule this update during periods of minimal load. For more details, see [Default strategy: `BulkUpdate`](#default) | Cluster is unavailable ||
|| `RollingUpdate` | Available starting from operator version **0.31.0**. The operator updates pods one by one or in groups and guarantees a minimum number of available instances. Not suitable for all components: master servers can only be updated using `BulkUpdate` or `OnDelete`. Other components require at least 2 instances. For more details, see [Strategy `RollingUpdate`](#configure-rolling) | Cluster is available, except when updating master servers ||
|| `OnDelete` | Available starting from operator version **0.31.0**. The operator updates the `StatefulSet` specification but doesn’t automatically restart pods — you delete them manually. For master servers, delete pods one by one; otherwise, the cluster will become unavailable | Depends on user actions ||
|#

## Configuring the updatePlan field {#updateplan}

Add the `updatePlan` field to the {{product-name}} cluster specification. In it, list the rules: in each rule, select components by the `class` or `component` field and specify the update strategy in the `strategy` field. The operator checks the rules from top to bottom and applies the first matching one. Therefore, place specific rules by the `component` field before general rules by the `class` field.

#### Field descriptions {#fields}

#|
||
**Field**
|
**Description**
||
||
`class`
|
Select components by class. Available values:

- `Everything` — the operator updates all components;
- `Stateless` — the operator updates all [stateless components](*stateless), except master servers, Data nodes, and Tablet nodes;
- `Nothing` — the operator doesn’t update any components.
||
||
`component.type`
|
Select by component type: `Master`, `HttpProxy`, `RpcProxy`, `DataNode`, `ExecNode`, `TabletNode`, `Scheduler`, `ControllerAgent`, `Discovery`, and others
||
||
`component.name`
|
Select a specific instance group by name. Optional field
||
||
`concurrency`
|
Maximum number of instance groups that the operator updates simultaneously
||
||
`strategy.rollingUpdate`
|
`RollingUpdate` strategy: the operator updates pods one by one
||
||
`strategy.onDelete`
|
`OnDelete` strategy: you delete pods manually
||
||
`strategy.runPreChecks`
|
Whether to run pre‑checks before updating each pod
||
|#

For examples of configuring the `updatePlan` field for the `RollingUpdate` strategy, see the [Updating without stopping the cluster](../../../admin-guide/update/update-ytsaurus.md#rolling-update-scenario) section.

## Image preloading {#image-heater}

By default, the operator downloads the new image during the update, when the pod is already stopped. This makes the component unavailable for a longer period. To download images in advance, enable preloading — the operator will download the required images before the update starts, and pod switching will be faster.

Enable preloading for all components using the `enableImageHeater` flag in the `clusterFeatures` block:

```yaml
spec:
  clusterFeatures:
    enableImageHeater: true
```

Preloading is especially useful with the `RollingUpdate` strategy when minimal downtime is important. For a combined configuration example, see the [Updating without stopping the cluster](../../../admin-guide/update/update-ytsaurus.md#rolling-update-scenario) section.

The preloading progress is reflected in the `WaitingForImageHeater` status of the `UPDATESTATE` field.

## Migrating from enableFullUpdate to updatePlan {#migration-from-enablefullupdate}

Starting from operator version **0.32.0**, the `updatePlan` field replaces the `enableFullUpdate` flag, so this flag is no longer valid. For information about operator releases, see the [Releases page](../../../admin-guide/releases.md#kubernetes-operator).

The `enableFullUpdate` flag only enabled or disabled a full update. The `updatePlan` field specifies a list of rules: in each rule, you select components and their update strategy.

Direct correspondence between the old flag and the new field:

#|
|| **Flag `enableFullUpdate`** | **Field `updatePlan`** | **Result** ||
|| `enableFullUpdate: true` | Rule with `class: Everything` | Update all server components ||
|| `enableFullUpdate: false` | Empty `updatePlan` or rule with `class: Nothing` | Don’t update anything ||
|#

{% cut "Example: full update" %}

```yaml
# Operator 0.31.0 and earlier
spec:
  enableFullUpdate: true
```

```yaml
# Operator 0.32.0 and later
spec:
  updatePlan:
    - class: Everything
```

{% endcut %}

In addition to the direct correspondence, `updatePlan` provides capabilities that `enableFullUpdate` didn’t have:

- **Component set selection.** A rule with `class: Stateless` updates all [stateless components](*stateless), except master servers, Data nodes, and Tablet nodes. You can also select an individual component by type using the `component` field.
- **Update strategy selection.** In the `strategy` field of each rule, specify the `RollingUpdate` or `OnDelete` strategy. For more details, see the [Configuring the updatePlan field](#updateplan) section.

{% cut "Example: update only HTTP proxies" %}

```yaml
# Operator 0.32.0 and later
spec:
  updatePlan:
    - component:
        type: HttpProxy
```

The `enableFullUpdate` flag didn’t allow selecting an individual component — it either updated all server components or none. You could update a single component only by overriding its image in the `image` field. This method still works — for more details, see the [Updating individual components](../../../admin-guide/update/update-ytsaurus.md#partial-update) section.

{% endcut %}

The operator only updates components explicitly listed in `updatePlan`. It leaves other components unchanged, even if their image has changed: such a component enters the `UpdateBlocked` state and waits for you to add it to the plan.

If `updatePlan` is empty or not specified, the operator doesn’t perform any updates. If `coreImage` changes, the cluster enters the `UpdateBlocked` state — the update is prepared but blocked.

## Default strategy: BulkUpdate {#default}

If you don’t specify the `updatePlan` field or specify it without the `strategy` field, the operator applies the `BulkUpdate` strategy. The update proceeds in three phases:

Preparation.

:   The operator enables safe mode and disables writing to the cluster, saves and deletes tablet cells — dynamic tables become unavailable. Then the operator creates snapshots of master servers and puts them in `read‑only` mode.

Pod replacement.

:   The operator simultaneously deletes pods of all components — at this stage, the cluster is completely unavailable. After that, the operator recreates pods with the new image.

Recovery.

:   The operator waits for master servers to exit `read‑only` mode, restores tablet cells, and disables safe mode.

The cluster is completely unavailable from the start of preparation to the end of recovery. The duration depends on the cluster size — from several minutes to tens of minutes.

## Strategy: RollingUpdate {#configure-rolling}


With the `RollingUpdate` strategy, the operator updates pods one by one or in groups and guarantees a minimum number of available instances.

### Component availability with RollingUpdate {#component-availability}


#|
|| **Component** | **What the operator does** | **Availability** ||
|| HTTP/RPC proxies | The operator updates pods one by one. Kubernetes Service automatically redirects traffic to ready pods | Clients can access the cluster ||
|| Data nodes | The operator updates pods one by one. Data is available via replicas on other nodes | Read and write operations work ||
|| Exec nodes | Before updating a pod, the operator disables scheduler jobs; after the update, it enables them again | Remaining nodes perform operations ||
|| Tablet nodes | The operator updates pods one by one | Dynamic tables are available via remaining nodes ||
|| Scheduler, Controller agent | The operator updates pods one by one | Operation scheduling works ||
|| Master servers | `RollingUpdate` isn’t implemented for master servers yet. The operator updates them using `BulkUpdate` or `OnDelete` | With `BulkUpdate`, the cluster is unavailable ||
|#

{% note warning "Important" %}

For the `RollingUpdate` strategy to work correctly, each component must have at least 2 instances. If there’s only one instance, the component will be unavailable during its update — just like with `BulkUpdate`.

{% endnote %}

### Recommended number of instances {#instance-count}

#|
|| **Component** | **Minimum for availability** | **Recommended** ||
|| HTTP/RPC proxies | 2 | 3 or more ||
|| Data nodes | 3 — for data replication | 3 or more ||
|| Exec nodes | 2 | 2 or more ||
|| Master servers | 3 — for quorum | 3 ||
|| Tablet nodes | 2 | 3 or more ||
|#

For examples of configuring `RollingUpdate` for a cluster, see the [Additional scenarios](../../../admin-guide/update/update-ytsaurus.md#rolling-update-scenario) section of the update guide.

### Configuring pod availability {#budget}

By default, with the `RollingUpdate` strategy, the operator updates one pod at a time. To specify a different number, use the `minReadyInstanceCount` field:

```yaml
spec:
  httpProxies:
    - instanceCount: 5
      minReadyInstanceCount: 3  # always at least 3 pods are available
      role: default
```

The operator calculates the number of pods updated simultaneously using the formula:

```
maxUnavailable = max(1, instanceCount - minReadyInstanceCount)
```

In the example above, the operator updates up to 2 pods simultaneously, while 3 pods always serve requests: `maxUnavailable = max(1, 5 - 3) = 2`.

{% cut "Examples of calculating maxUnavailable" %}

#|
|| **instanceCount** | **minReadyInstanceCount** | **maxUnavailable** | **What happens** ||
|| 1 | not specified → 0 | 1 | The operator updates the only pod, the component is unavailable ||
|| 3 | not specified → 2 | 1 | The operator updates 1 pod at a time, 2 pods are always available ||
|| 5 | 3, specified explicitly | 2 | The operator updates 2 pods at a time, 3 pods are always available ||
|| 5 | not specified → 4 | 1 | The operator updates 1 pod at a time, 4 pods are always available ||
|#

{% endcut %}

[*stateless]: Stateless components don’t store data — these include HTTP and RPC proxies, the scheduler, Controller agent, and others. Master servers, Data nodes, and Tablet nodes are stateful components: they are responsible for data storage, so the `Stateless` strategy doesn’t update them.
