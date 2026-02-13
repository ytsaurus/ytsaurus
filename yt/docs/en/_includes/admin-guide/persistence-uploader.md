## Uploading snapshots to Cypress { #uploading-snapshots-to-cypress }

Some parts of the system require master snapshots.

To do this, the {{product-name}} k8s operator, starting from version 0.28.0, supports uploading master snapshots to Cypress. A special sidecar container is used for this — [Hydra Persistence Uploader](https://github.com/ytsaurus/ytsaurus/tree/main/yt/docker/sidecars/hydra_persistence_uploader).

{% note warning %}

The sidecar will upload the following files to Cypress, which will consume cluster disk space:

- Master binary (with replication, at least ~15 GB)
- Master snapshots

Make sure that the `sys` account has enough free space to store this data.

{% endnote %}

In the cluster specification, in the `primaryMasters` section, you need to specify the image for `hydraPersistenceUploader`:

{% note info %}

Applying these changes will trigger a full cluster update with downtime, similar to a regular master update.

{% endnote %}

```yaml
spec:
  primaryMasters:
    hydraPersistenceUploader:
      image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
```

### Normal operation of the sidecar

- The status of the container named `hydra-persistence-uploader` is `Running`.

```
kubectl -n <namespace> get pod ms-0 \
  -o jsonpath='{.status.containerStatuses[?(@.name=="hydra-persistence-uploader")].state}'
```

- The master binary is uploaded to `//sys/admin/snapshots/meta`:

```
yt list //sys/admin/snapshots/meta | grep master_binary
```

- Snapshots and changelogs of the master are uploaded to `//sys/admin/snapshots/<cell_id>/snapshots`:

```
yt list //sys/admin/snapshots/1/snapshots
yt list //sys/admin/snapshots/1/changelogs
```

### Troubleshooting

To view the sidecar logs, run:

```
kubectl -n <namespace> logs ms-0 -c hydra-persistence-uploader
```
