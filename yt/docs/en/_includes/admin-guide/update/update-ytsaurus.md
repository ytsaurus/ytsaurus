# Updating server components of {{product-name}}

{% note warning "Important" %}

This describes how to perform an update using the operator version **0.32.0** or later. For differences with older versions, see the section [Migrating from enableFullUpdate to updatePlan](../../../admin-guide/update/update_strategy.md#migration-from-enablefullupdate).

{% endnote %}

This article describes the process of fully updating the server components of a {{product-name}} cluster via the Kubernetes operator: how to prepare for the update, update the operator, modify the `Ytsaurus` specification, monitor the update progress, and verify the result.

## What you need to know before updating {#important-things-to-know}

Update only to the next major version.
: Update the cluster sequentially by major versions, without skipping intermediate ones: first `25.1 → 25.2`, then `25.2 → 25.3`. Skipping a version may disrupt cluster operation. Within the target major version, you can select the latest minor version to get the most recent updates — for example, `25.2.0 → 25.3.2`. {% if audience == "public" %}See available versions in the [release list](../../../admin-guide/releases.md#server).{% endif %}

The update cannot be undone.
: Once the master servers exit `read-only` mode and apply irreversible changes, rolling back to the previous version is not supported.

Update the operator first, then the cluster.
: Before updating the cluster, update the operator. Its version must be compatible with the target cluster version. {% if audience == "public" %}Check compatibility in the [compatibility table](../../../admin-guide/compatibility.md).{% endif %}


The cluster may be unavailable during the update.
: By default, the cluster is fully unavailable during the update — all operations and requests are interrupted. Schedule the update during a period of minimal load. If the cluster must remain available, choose a different strategy — see [Update strategies](../../../admin-guide/update/update_strategy.md).

{% note info "Updating the cluster without downtime" %}

If you need to update the cluster without downtime, configure the `RollingUpdate` strategy via the `updatePlan` field. See an example in the section [Updating the cluster without downtime via RollingUpdate](#rolling-update-scenario).

{% endnote %}

## Cluster update {#update}

The update process consists of four steps:

1. [Prepare the cluster](#before-update)
1. [Update the operator](#update-operator)
1. [Start the update](#change-spec)
1. [Verify the result](#verify)

### 1. Prepare the cluster for the update {#before-update}

Before changing the `Ytsaurus` specification, check the current state of the cluster and make sure the operator is ready to start a new update cycle.

1. Find out the current cluster version:

   ```bash
   $ kubectl get {{product-name}} <cluster_name> -n <namespace> -o jsonpath='{.spec.coreImage}'
   
   ghcr.io/ytsaurus/ytsaurus:stable-25.1.0-relwithdebinfo
   ```

1. Check the cluster state:

   ```bash
   $ kubectl get {{product-name}} <cluster_name> -n <namespace>
   
   NAME       CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS   BLOCKEDCOMPONENTS
   {{product-name}}   Running
   ```

   Before the update, the cluster must be in the `Running` state. If the cluster is in another state, fix the issues first — see the [Troubleshooting](#troubleshooting) section.

1. Make sure the operator is ready for the update. To do this, check its logs:

   ```bash
   kubectl logs -n <namespace> deployment/ytsaurus-ytop-chart-controller-manager --tail=100
   
   # If the deployment name is different, get the list
   
   kubectl get deployments -n <namespace>
   ```

   This is an important step: the operator must not be busy with pending actions. Proceed to the next step only when the logs contain the message `INFO {{product-name}} is running and happy`. This means the operator considers the cluster consistent and is not performing any pending actions.

   {% note warning "Important" %}
   
   If the operator does not report `running and happy`, do not start the update. This may lead to unpredictable cluster behavior.
   
   {% endnote %}

1. Save a backup copy of the specification:

   The file is required to enable rollback before passing the point of no return:

   ```bash
   kubectl get {{product-name}} <cluster_name> -n <namespace> -o yaml > ytsaurus-spec-backup.yaml
   ```

   After the master servers exit the `read‑only` mode and apply irreversible changes, rollback to the previous version is not supported.

If all checks are passed, you can proceed with updating the operator.

### 2. Update the operator {#update-operator}

Update the operator first. New versions of server components may not work correctly with an older version of the operator.

{% note warning "Warning" %}

The new version of the operator may trigger a cluster update immediately — without you changing `coreImage`. This happens if the new operator version generates static configs differently from the old one: the operator detects a discrepancy and starts reconciliation. For more details on the reasons and how to stop an unwanted update, see the section [Automatic update after operator update](#auto-update-operator-scenario).

{% endnote %}

Check the current operator version:

```bash
helm list -n <namespace>
```

{% if audience == "public" %}
See the list of releases and compatible versions on the [releases page](../../../admin-guide/releases.md#kubernetes-operator) and in the [compatibility table](../../../admin-guide/compatibility.md).{% endif %}


Update the operator:

```bash
helm upgrade {{product-name}} --install oci://ghcr.io/ytsaurus/ytop-chart --version <new_version>
```

Verify that the operator has been updated:

```bash
kubectl get pods -n <namespace>
```

Right after the update, you may see both the old and new pods at the same time:

```bash
NAME                                                      READY   STATUS        RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running       0          21s
ytsaurus-ytop-chart-controller-manager-7478f9b6cb-qr8wd   2/2     Terminating   0          23h
```

After some time, the old pod will be removed:


```bash
NAME                                                      READY   STATUS    RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running   0          25s
```

### 3. Start the update {#change-spec}

When you change the `coreImage` field in the `Ytsaurus` specification, the operator recreates pods for all [server components from this specification](#server-components-ref), even if the image of a specific component hasn’t changed. This ensures version compatibility across all server components.


[Components with their own release cycle](#additional-components-ref) — Query Tracker, YQL agent, and others — are not updated in this process. To learn how to update them, see the section [Updating individual components](#partial-update). Before updating, check the compatibility of these components with the target cluster version{% if audience == "public" %} in the [compatibility table](../../../admin-guide/compatibility.md).{% endif %}


Save the current specification to a file for editing:


```bash
kubectl get {{product-name}} <cluster_name> -n <namespace> -o yaml > ytsaurus-spec-new.yaml
```

{% note warning "Warning" %}


Only save the specification from your running cluster using the `kubectl get ytsaurus` command. Do not use a specification from GitHub or another template: it won’t include your current settings — such as marked disks, encryption parameters, and custom settings. Applying a clean specification may break the cluster.


{% endnote %}

Make the following changes to the `ytsaurus-spec-new.yaml` file:

- Set the `updatePlan` field with a list of components to update. For a full update, specify the `Everything` class — all [server components](#server-components-ref) will be updated.
- Change `coreImage` to the next version.


{% if audience == "public" %}
- Select the target version on the [releases page](../../../admin-guide/releases.md#server). Open the required release and copy the image tag `ghcr.io/ytsaurus/ytsaurus`.{% endif %}


```yaml
spec:
  coreImage: ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo # Next version
  updatePlan:
    - class: Everything # Full update of all server components
```

{% note warning "Important" %}


Only update to the stable version following your current one. Do not use `dev` images from the `main` branch. After a successful update, rolling back to the previous version is not supported.


{% endnote %}

By default, the operator uses the `BulkUpdate` strategy — the cluster will be completely unavailable during the update. If the cluster must stay available, configure the `RollingUpdate` strategy via the `updatePlan` field in the `ytsaurus-spec-new.yaml` file before applying the changes. See an example in the section [Updating without cluster downtime using RollingUpdate](#rolling-update-scenario).


Apply the modified specification:

```bash
kubectl apply -f ytsaurus-spec-new.yaml
```

Expected output:

```bash
ytsaurus.cluster.ytsaurus.tech/<cluster_name> configured
```

Monitor the status in the `UPDATESTATE` field of the `Ytsaurus` resource:


```bash
kubectl get {{product-name}} <cluster_name> -n <namespace>
```

During the update, the output may look like this:

```bash
NAME       CLUSTERSTATE   UPDATESTATE            UPDATINGCOMPONENTS
ytsaurus   Updating       WaitingForPodsCreation   {ms hp ds dnd rp end tnd sch ca}
```

For a description of update stages and pod states, see the [status reference](#statuses-reference) below.

Check the pod statuses:

```bash
kubectl get pods -n <namespace>
```

{% note info %}

The update may take some time, as the operator first downloads the new Docker image and then starts the updated components. To have images ready in advance and speed up the update, configure [image pre‑loading](../../../admin-guide/update/update_strategy.md#image-heater).


{% endnote %}

If the update takes too long, check the events for the problematic pod:


```bash
kubectl describe pod <pod_name> -n <namespace> | grep -A 10 "Events:"
```

If the events show an image pull error `ImagePullBackOff`, verify that the image tag is correct and that this version exists in the registry. If the pod is in the `Pending` state, the issue is related to resources — check their availability in the cluster.


### 4. Verify the result {#verify}

The update is complete when the following conditions are met:


- The cluster is in the `Running` state.
- All main pods are in the `Running` state.
- There are no pods in the cluster in the `Init`, `Pending`, `ImagePullBackOff`, and `CrashLoopBackOff` states.


Check the final cluster state:

```bash
kubectl get {{product-name}} <cluster_name> -n <namespace>
```

Expected output:

```bash
NAME       CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS   BLOCKEDCOMPONENTS
ytsaurus   Running
```

Check pod statuses:

```bash
kubectl get pods -n <namespace>
```

Example of expected output:

```bash
NAME                                      READY   STATUS     RESTARTS   AGE
ca-0                                      1/1     Running    0          5m
dnd-0                                     1/1     Running    0          5m
dnd-1                                     1/1     Running    0          5m
dnd-2                                     1/1     Running    0          5m
ds-0                                      1/1     Running    0          10m
end-0                                     2/2     Running    0          5m
hp-0                                      1/1     Running    0          5m
ms-0                                      1/1     Running    0          10m
qt-0                                      1/1     Running    15         75d
rp-0                                      1/1     Running    0          5m
sch-0                                   1/1     Running    0          5m
tnd-0                                     1/1     Running    0          10m
```

Verify that the new version is specified in the specification:

```bash
kubectl get {{product-name}} <cluster_name> -n <namespace> -o jsonpath='{.spec.coreImage}'
```

Expected output:

```bash
ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo
```

Make sure the operator is again logging `Ytsaurus is running and happy`:


```bash
kubectl logs -n <namespace> deployment/ytsaurus-ytop-chart-controller-manager --tail=100
```

After a successful update, clear the `updatePlan` field in the `ytsaurus-spec-new.yaml` file — leave it empty:


```yaml
spec:
  updatePlan: []
```

You can achieve the same result with an explicit rule using the `Nothing` class — the operator won’t update any component:


```yaml
spec:
  updatePlan:
    - class: Nothing
```

Apply the change:

```bash
kubectl apply -f ytsaurus-spec-new.yaml
```

The operator doesn’t clear `updatePlan` automatically — reset it yourself. An empty `updatePlan` means “don’t update anything” and protects against accidental component restarts during further specification changes. If you leave `- class: Everything`, the next change to `coreImage` will trigger a full update of all components again.


Updating to the next version: if you need to update to another version, repeat all steps starting from the [Preparing for update](#before-update) section. Each time, save the specification anew via `kubectl get ytsaurus` — the operator might have changed it during the update, and the local file from last time is no longer up to date.


## Troubleshooting updates {#troubleshooting}


Use this section if the update doesn’t start, takes too long, fails, or behaves unpredictably.


{% cut "Version conflict when applying the specification" %}


You get a version conflict error when trying to apply the specification:


```bash
Error from server (Conflict): error when applying patch: the object has been modified; please apply your changes to the latest version and try again
```

This means another process has modified the specification. Save the current version of the specification and retry the change:


```bash
kubectl get {{product-name}} <cluster_name> -n <namespace> -o yaml > ytsaurus-spec-new.yaml
```

{% endcut %}


{% cut "Image not found" %}


Image pull error:

```bash
Failed to pull image "ghcr.io/ytsaurus/ytsaurus:stable-X.X.X-relwithdebinfo": not found
```

Check that:

1. The image tag is specified correctly.
2. This version exists in the registry.

{% endcut %}

{% cut "Pod stuck in Init state for a long time" %}


This behavior is possible when Kubernetes is still downloading the new image.

```bash
kubectl describe pod <pod_name> -n <namespace> | grep -A 10 "Events:"
```

Example of normal process:

```bash
Events:
  Type    Reason     Age    From               Message
  ----    ------     ----   ----               -------
  Normal  Scheduled  3m10s  default-scheduler  Successfully assigned ytsaurus/ms-0 to docker-desktop
  Normal  Pulling    3m9s   kubelet            Pulling image "ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo"
```

If the pod remains in the `Init` state longer than expected, check the pod logs and events to identify the issue.


{% endcut %}

{% cut "Job is in CrashLoopBackOff state" %}


For example, you might see `yt-scheduler-init-job-op-archive` in the `CrashLoopBackOff` state.


During an update, this may be a temporary state. The job might start before the cluster fully prepares tablet cells. Because of this, the job enters a restart loop but usually converges later — this is normal.


First, check the overall cluster status:

```bash
kubectl get {{product-name}} <cluster_name> -n <namespace>
```

If the cluster is still updating, wait for the process to complete and check the job status again. If the cluster isn’t updating or the job doesn’t converge after a long time, check the job logs to identify the cause of the problem.


{% endcut %}

{% cut "Operator doesn’t start the update" %}


Check that:

1. The `updatePlan` field lists the components to update — for example, `- class: Everything`. If `updatePlan` is empty and you changed `coreImage`, the cluster will enter the `UpdateBlocked` state and the update won’t start.
2. The operator has completed previous actions and the logs show `running and happy`.
3. The cluster was in the `Running` state before the update started.
4. Check the operator logs:


```bash
kubectl logs -n <namespace> deployment/ytsaurus-ytop-chart-controller-manager --tail=100
```

{% endcut %}


{% cut "Update entered the ImpossibleToStart state" %}

The Kubernetes operator may stop the update if it considers it unsafe. For example, if some tablet cell bundles are in poor condition — tablet cells aren’t working or have errors. In this case, the update won’t start until you fix the issue.


Check the resource status:

```bash
kubectl get {{product-name}} -n <namespace>
```

Example output:

```bash
NAME         CLUSTERSTATE   UPDATESTATE           UPDATINGCOMPONENTS
minisaurus   Updating       ImpossibleToStart
```

To find the reason, run the command:

```bash
kubectl describe {{product-name}} -n <namespace>
```

And check the `Conditions` block in `UpdateStatus`.


{% cut "Example of why an update can’t start" %}

```bash
kubectl describe {{product-name}} -n <namespace>
...
  Update Status:
    Conditions:
      Last Transition Time:  2023-09-26T09:18:11Z
      Message:               Tablet cell bundles ([sys default]) aren't in 'good' health
      Reason:                Update
      Status:                True
      Type:                  NoPossibility
    State:                   ImpossibleToStart
```

{% endcut %}

If you can’t start the update, revert the specification to its previous value. After that, the operator will cancel the update and return the cluster to the `Running` state.


{% endcut %}


{% cut "Unrecognized master options after update" %}


After updating the cluster, master server configs may still contain unrecognized options that are no longer used in the new version of {{product-name}}. This often happens during major updates. In this case, you’ll see a warning:


```bash
Found unrecognized options in dynamic cluster config
```

You can remove unrecognized master options in two ways:


- **Using the CLI command** — [yt admin remove-master-unrecognized-options](../../../admin-guide/cli-admin.md#remove-master-unrecognized-options). This method supports the `--dry` flag for previewing changes.
- **Using a standalone script** — [remove_master_unrecognized_options](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/scripts/remove_master_unrecognized_options) from the GitHub repository.


{% endcut %}

## Advanced scenarios {#advanced-scenarios}


### Updating individual components {#partial-update}


{% note warning "Warning" %}


Use partial updates only in rare cases, such as fixing an error. We recommend discussing this decision with the [{{product-name}} team](https://ytsaurus.tech/#contact) first.


A newer version of one component may require a specific version of master servers. If dependencies aren’t met, the cluster may fail to start. To update the cluster, we recommend using the update via the `coreImage` field. This guarantees version compatibility across all server components.


{% endnote %}


In the `Ytsaurus` specification, you can manage component images individually. How the operator handles such changes depends on the component type:


- Components with their own release cycle — Query Tracker, YQL agent, Strawberry, and others. For more information, see the section [Components with their own release cycle](#additional-components-ref).


  To update these components, change the `image` field in the `Ytsaurus` specification. This is the only way to update these components. When you change their image, the operator updates only their pods without stopping cluster operation.


  When fully updating the cluster via the `coreImage` field, these components’ pods will restart, but their image will remain the same — the one set in the `image` field.


  Example: updating Query Tracker to version `0.0.7`:


  ```yaml
  spec:
    queryTrackers:
      image: ghcr.io/ytsaurus/query-tracker:0.0.7
  ```

  ```bash
  kubectl apply -f y
