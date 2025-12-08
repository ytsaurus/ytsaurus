Kubernetes operator {{product-name}}, starting from version 0.28.0, supports the delivery of structured master logs, which can be configured directly in the cluster specification.

The delivery is implemented using a sidecar with [timbertruck](https://github.com/ytsaurus/ytsaurus/tree/main/yt/admin/timbertruck) running in it.

To make it work, you need to do the following:

1. Launch HTTP Proxy and Queue Agent on the cluster.

2. Enable the recording of the required structured master log:

```yaml
spec:
  primaryMasters:
    structuredLoggers:
      - name: access
        minLogLevel: info
        category: Access
        format: json
        rotationPolicy:
          maxTotalSizeToKeep: 5_000_000_000
          rotationPeriodMilliseconds: 900000
```

3. Specify a location of the `Logs` type:

```yaml
spec:
  primaryMasters:
    locations:
      - locationType: Logs
        path: /yt/master-logs
    volumeMounts:
      - name: master-logs
        mountPath: /yt/master-logs
    volumeClaimTemplates:
      - metadata:
          name: master-logs
        spec:
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: 5Gi
```

4. Specify the image for `timbertruck`:

{% note warning %}

Applying these changes will trigger a full cluster update with downtime, similar to a regular master update.

{% endnote %}

```yaml
spec:
  primaryMasters:
    timbertruck:
      image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
```

5. Apply the changes:

```
kubectl apply -f your_config.yaml -n <namespace>
```

#### Normal operation of the sidecar

- Queues have been created for each of the structured logs:

```bash
yt exists //sys/admin/logs/master-access/producer
yt exists //sys/admin/logs/master-access/queue
```

- The `producer` has a line with information about the delivery from the masters:

```bash
yt select-rows "SELECT session_id FROM [//sys/admin/logs/master-access/producer]" --format json
```

Example of expected output:
```json
{
  "session_id": "ms-0.masters.ytsaurus-dev.svc.cluster.local:/yt/master-logs/timbertruck/master-access/staging/2025-12-01T11:07:46_ino:46696359.access.log.json"
}
```

- Export of the queue `//sys/admin/logs/master-access/queue` to static tables has been configured:

```bash
yt get //sys/admin/logs/master-access/queue/@static_export_config
```

The expected configured export to an existing node:
```yson
{
    "default" = {
        "export_directory" = "//sys/admin/logs/export/master-access";
        "export_ttl" = 1209600000;
        "export_period" = 1800000;
        "export_name" = "default";
    };
}
```

#### Troubleshooting

To view the sidecar logs, run:

```bash
kubectl -n <namespace> logs ms-0 -c timbertruck
```
