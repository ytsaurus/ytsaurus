# Getting the {{product-name}} specification ready

You can find an example of the minimum specification [here](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_demo.yaml).

{% cut "Sample specification" %}

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: ytdemo
spec:
  coreImage: ghcr.io/ytsaurus/ytsaurus:stable-{{yt-server-version}}-relwithdebinfo
  uiImage: ghcr.io/ytsaurus/ui:stable

  adminCredentials:
    name: ytadminsec

  discovery:
    instanceCount: 1

  primaryMasters:
    instanceCount: 3
    cellTag: 1
    volumeMounts:
      - name: master-data
        mountPath: /yt/master-data
    locations:
      - locationType: MasterChangelogs
        path: /yt/master-data/master-changelogs
      - locationType: MasterSnapshots
        path: /yt/master-data/master-snapshots

    volumeClaimTemplates:
      - metadata:
          name: master-data
        spec:
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: 20Gi

  httpProxies:
    - serviceType: NodePort
      instanceCount: 3

  rpcProxies:
    - serviceType: LoadBalancer
      instanceCount: 3

  dataNodes:
    - instanceCount: 3
      volumeMounts:
        - name: node-data
          mountPath: /yt/node-data

      locations:
        - locationType: ChunkStore
          path: /yt/node-data/chunk-store

      volumeClaimTemplates:
        - metadata:
            name: node-data
          spec:
            accessModes: [ "ReadWriteOnce" ]
            resources:
              requests:
                storage: 50Gi

  execNodes:
    - instanceCount: 3
      resources:
        limits:
          cpu: 3
          memory: 5Gi

      volumeMounts:
        - name: node-data
          mountPath: /yt/node-data

      volumes:
        - name: node-data
          emptyDir:
            sizeLimit: 40Gi

      locations:
        - locationType: ChunkCache
          path: /yt/node-data/chunk-cache
        - locationType: Slots
          path: /yt/node-data/slots

  tabletNodes:
    - instanceCount: 3

  queryTrackers:
    instanceCount: 1

  yqlAgents:
    instanceCount: 1

  schedulers:
    instanceCount: 1

  controllerAgents:
    instanceCount: 1

  ui:
    serviceType: NodePort
    instanceCount: 1
```

{% endcut %}

Table 1 shows some general `Ytsaurus` settings. Full description: [YtsaurusSpec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec).

<small>Table 1 — Basic `Ytsaurus` specification fields </small>

| **Field** | **Type** | **Description** |
| ------------------- | --------------- | ------------------------------------------------------------ |
| `coreImage` | `string` | Image for the main server components (for example, `ghcr.io/ytsaurus/ytsaurus:stable-{{yt-server-version}}-relwithdebinfo`). |
| `uiImage` | `string` | Image for the UI (for example, `ghcr.io/ytsaurus/ui:stable`). |
| `imagePullSecrets` | `array<LocalObjectReference>` | Secrets needed to pull images from a private registry. Learn more [here](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/). |
| `configOverrides` | `optional<LocalObjectReference>` | A ConfigMap for overriding generated static configs. It should only be used in rare cases. |
| `adminCredentials` | `optional<LocalObjectReference>` | A secret with the login/password for the admin account. |
| `isManaged` | `bool` | A flag that lets you disable all operator actions on this cluster in order to manually work with the cluster where needed. |
| `enableFullUpdate` | `bool` | A flag that lets you prohibit the launch of a full cluster update. |
| `useIpv6` | `bool` | Use IPv6 or IPv4 |
| `bootstrap` | BootstrapSpec | Settings for initially deploying the cluster (for example, [tablet cell bundle parameters](../../user-guide/dynamic-tables/concepts#tablet_cell_bundles)) |

## Selecting a set of components

Clusters can be created with different sets of components. Let's take a brief look at the components that can be configured in the `Ytsaurus` specification.

You can read more about components in [this section](../../admin-guide/components.md).

At a minimum, the cluster must have masters and discovery services configured in the `primaryMasters` and `discovery` fields, respectively.

Schedulers and controller agents are required to start operations, and they are configured respectively in the `schedulers` and `controllerAgents` fields.

To make requests to the cluster from the `CLI` and `SDKs`, you need proxies. There are two types of proxies: `HTTP` and `RPC`. They are configured in the `httpProxies` and `rpcProxies` fields, respectively.

For a convenient UI you can use to work with a cluster, you need to configure it in the `ui` field.

`dataNodes` are used to store data, while `execNodes` launch operation jobs.

If you plan to query data using an SQL-like [query language](../../yql), add `queryTrackers` and `yqlAgents` to the specification.

To use [CHYT](../../user-guide/data-processing/chyt/about-chyt), you need to run a special controller. It can be configured in the `strawberry` field.

For dynamic tables to work (they're needed for system tables of components like query tracker), `tabletNodes` needs to be raised.

## Docker image

At the first step, select the main Docker image for the server components.

Most of the server components are released from a separate (release) branch. Currently, the latest stable branch is `stable/{{yt-stable-branch}}`. We strongly recommend using an image compiled from a stable release branch.

The names of Docker images built from the release branch follow the format `ghcr.io/ytsaurus/ytsaurus:stable-{{yt-stable-branch}}.N` or `ghcr.io/ytsaurus/ytsaurus:stable-{{yt-stable-branch}}.N-relwithdebinfo`. The difference between the images is that all the binary files in the second one are built with debug symbols.

If the server components crash, the stack trace is printed to the stderr components, and a `coredump` is retrieved from the K8s node (if this is configured in your K8s cluster). This provides detailed information on what exactly happened to the component. With that in mind, we recommend using `relwithdebinfo` images even though they take up more space. Without debug symbols, the {{product-name}} team probably won't be able to help you if you encounter any problems.

The image provided has everything you need for almost every component. For components not included in the main Docker image, we push separate images. Table 2 shows the recommended image for each component.

The `image` for each component is taken primarily from the component's image field. If an image isn't listed, `coreImage` is taken from the top level of the specification.


<small>Table 2 — Component images </small>

| **Field** | **Docker repository** | **Recommended stable release tag** |
| ------------------- | --------------- | ----------------------------- |
| `discovery` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `primaryMasters` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `httpProxies` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `rpcProxies` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `dataNodes` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `execNodes` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `tabletNodes` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `schedulers` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `controllerAgents` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `queryTrackers` | [ghcr.io/ytsaurus/query-tracker](https://github.com/ytsaurus/ytsaurus/pkgs/container/query-tracker) | `{{qt-version}}-relwithdebinfo` |
| `yqlAgents` | [ghcr.io/ytsaurus/query-tracker](https://github.com/ytsaurus/ytsaurus/pkgs/container/query-tracker) | `{{qt-version}}` |
| `strawberry` | [ghcr.io/ytsaurus/strawberry](https://github.com/ytsaurus/ytsaurus/pkgs/container/strawberry) | `{{strawberry-version}}` |
| `ui` | [ghcr.io/ytsaurus/ui](https://github.com/ytsaurus/ytsaurus-ui/pkgs/container/ui) | `stable` |

In addition, a common image is provided for all server components at once (except `ui`). You only need to specify it once in `coreImage`, without explicitly specifying anything in the component's `image` field.

## Logging
A proper logging configuration is essential for diagnosing problems and facilitating support. For recommendations on setting up logging, see [this page](../../admin-guide/logging.md).

## Locations

There are recommendations for disk layout and location configuration on a separate [page](../../admin-guide/locations.md).

## Operation execution environment {#job-environment}

`Exec nodes` can run jobs in isolated containers to handle the `docker_image` operation option. You can find the required settings under `jobResources` and `jobEnvironment` in [ExecNodeSpec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#execnodesspec). See the sample [cluster configuration](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_cri.yaml).

## Setting up tablet cell bundles

The operator automatically creates multiple [tablet cell bundles](../../user-guide/dynamic-tables/concepts#tablet_cell_bundles): `sys` and `default`.

For tablet cell bundles, you can set up media for storing logs and snapshots. By default, logs and snapshots are stored in the `default` medium.

We recommend setting bundles up such that logs and snapshots are stored on the `SSD`, otherwise the bundles could become inoperable.

For bundles that have already been created, you can set the `@options/snapshot_primary_medium` and `@options/changelog_primary_medium` attributes:

```bash
yt set //sys/tablet_cell_bundles/<bundle-name>/@options/snapshot_primary_medium '<medium-name>'
yt set //sys/tablet_cell_bundles/<bundle-name>/@options/changelog_primary_medium '<medium-name>'
```

When initializing a cluster, the operator can set up media for bundles automatically. To configure a bundle, enter the names of the media in the `bootstrap` section at the top level of the specification. In the same section, specify the number of tablet cells in the bundle. You can change the number of tablet cells after initializing the cluster by setting the attribute `//sys/tablet_cell_bundles/<bundle-name>/@tablet_cell_count`.

Sample `bootstrap` sections:

```yaml
bootstrap:
    tabletCellBundles:
        sys:
            snapshotMedium: ssd_medium
            changelogMedium: ssd_medium
            tabletCellCount: 3
        default:
            snapshotMedium: ssd_medium
            changelogMedium: ssd_medium
            tabletCellCount: 5
```

Once the cluster is deployed, the operator won't process changes in the `bootstrap` field. Further configuration must be done manually using the specified attributes.
