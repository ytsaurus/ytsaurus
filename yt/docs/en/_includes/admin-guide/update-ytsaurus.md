# Updating {{product-name}} server components

## Selecting an image and starting the update

The service regularly receives new releases as well as fixes to the existing stable releases. For more information about existing releases, see [this section](../../admin-guide/releases.md). Docker images with stable releases follow the format `ytsaurus/ytsaurus:23.N.M` (for example, `ytsaurus/ytsaurus:23.1.0`).

A Kubernetes operator supports updates to Docker images used for server components. To update them, change `coreImage` in the {{product-name}} specification and push it to K8s using the command `kubectl apply -f my_ytsaurus_spec.yaml -n <namespace>`.

{% note warning "Attention" %}

You can only update to more recent versions. If you try to roll back to an older image, you won't be able to deploy the masters. You also can't roll back successful upgrades to the latest version.

We strongly advise against upgrading to a `dev` version compiled from the current `main` branch since those {{product-name}} versions aren't stable and haven't been properly tested. Moreover, rolling back to the latest stable release will be impossible for the reasons described above.

{% endnote %}

{% note warning "Important" %}

Before updating {{product-name}}, you first need to update the operator to the latest release. Functionality isn't guaranteed for new stable images with outdated operators or for operators with unstable {{product-name}} versions like `dev`. You can find a list of stable images on the [Releases](../../admin-guide/releases) page.

{% endnote %}

## Update status

You can monitor the update status using the `Ytsaurus` resource status:
```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE      UPDATESTATE            UPDATINGCOMPONENTS
minisaurus   Updating          WaitingForPodsRemoval
```

Once the update starts, the cluster enters the `Updating` state (`CLUSTERSTATE`), at which point you can start monitoring the update state (`UPDATESTATE`). You can get more detailed information about the update progress by running `kubectl describe ytsaurus -n <namespace>`.

After the update finishes, the cluster goes back to the `Running` state:

```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Running        None
```

## Full and partial updates

In the `Ytsaurus` specification, you can set one image for all server components (`coreImage`) or different images for different components (in the components' `image` field). You only need to set individual images in rare cases, and it's recommended that you discuss this with the [{{product-name}} team](https://ytsaurus.tech/#contact) beforehand. If a component has its own image, it will be used. Modifying the `image` initiates a cluster update, though this only updates some of the components.

## Updating static configs { #configs }

The operator generates static component configs according to the `Ytsaurus` specification. Some modifications (like a change to `locations`, for example) may require you generating the config again and restarting the pods for that component. For this reason, config modifications also trigger an update.

## Inability to update { #impossible }

There are a number of situations where it may not be safe to run an update. Because of this, the operator performs a number of checks before initiating an update, including checking the health of all tablet cell bundles. If the operator decides the update is impossible based on the check results, it sets the update state to `ImpossibleToStart`.

```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE           UPDATINGCOMPONENTS
minisaurus   Updating       ImpossibleToStart
```

You can find out why in the `Ytsaurus` status by running `kubectl describe ytsaurus -n<namespace>` and checking `Conditions` in `UpdateStatus`.

{% cut "Sample reason why the update couldn't start" %}
```bash
$ kubectl describe ytsaurus -n <namespace>
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


If that happens, restore the previous specification value to prevent the component images from changing and to ensure that the operator generates the same static configs according to the specification as before. Next, the operator cancels the update and rolls {{product-name}} back to the `Running` state.

## Manual intervention

There are various issues that can potentially occur during an update, which may require manual intervention. When that happens, set the `isManaged=false` flag in the `Ytsaurus` specification. This prevents the operator from performing any operations on the cluster, so you can take whatever manual measures are necessary.

{% note warning "Attention" %}

If unforeseen problems occur during the update process, we recommend consulting with the [{{product-name}} team](https://ytsaurus.tech/#contact) before doing anything manually.

{% endnote %}

## Updating the operator { #operator }

{% note warning "Attention" %}

Before updating the operator, make sure the cluster is healthy. For example, it shouldn't have any [LVCs](../../admin-guide/problems/#lvc), and all [tablet cell bundles](../../admin-guide/problems/#tabletcellbundles) must be alive.

{% endnote %}

### Instructions

1. Launch the chart update: `helm upgrade ytsaurus --install oci://docker.io/ytsaurus/ytop-chart --version <new-version>`
2. Make sure the old operator's substations have been deleted and new ones have been created:
```bash
$ kubectl get pod -n <namespace>
NAME                                                      READY   STATUS        RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running       0          21s
ytsaurus-ytop-chart-controller-manager-7478f9b6cb-qr8wd   2/2     Terminating   0          23h

$ kubectl get pod -n <namespace>
NAME                                                      READY   STATUS    RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running   0          25s
```

### Possible automated cluster update

Different operator versions may generate different configs for the same components (for example, a new field may be added in the new operator version). In that case, the cluster update is initiated immediately after starting the operator.

If an update is [impossible](#impossible), the cluster remains in the `Updating` state, and the update status is set to `ImpossibleToStart`. If that happens, you can roll back the operator to cancel the update, and the cluster will enter the `Running` state. Alternatively, you can set the `enableFullUpdate = false` flag in the `Ytsaurus` specification, which also cancels the update and stops the new operator from trying to initiate another cluster update. You can then restore the cluster to a healthy state and retry the update by setting the `enableFullUpdate = true` flag.

