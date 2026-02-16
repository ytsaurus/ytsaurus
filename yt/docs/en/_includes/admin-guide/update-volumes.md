# Updating volumes in {{product-name}} services

This guide describes how to update volume settings for {{product-name}} services running in Kubernetes.

## Important considerations {#note}

When updating volumes, keep in mind the following:

1. **The operator does not automatically apply updates.** It does not monitor changes in the configuration of volumes, so modifying the resource specification will not automatically initiate an update.

2. **Persistent Volume Claims (PVCs) are attached to pods.** Even if you delete the pods or the StatefulSet, the PVCs will remain attached and prevent the new volume settings from being applied.

{% note warning %}

The procedure described below will result in data loss for updated volumes, because they will be recreated. Proceed with caution.

{% endnote %}

## Update procedure {#steps}

To update volume settings, follow the steps:

### Step 1. Disable the operator {#disable-operator}

Set the `isManaged` field to `false` in your {{product-name}} resource specification. This disables the operator and prevents it from interfering with manual changes.

```yaml
spec:
  isManaged: false
```

Apply this change in your cluster.

### Step 2. Delete the StatefulSet {#delete-statefulset}

Delete the StatefulSet for the service whose volumes you want to update:

```bash
kubectl delete statefulset <statefulset-name> -n <namespace>
```

### Step 3. Delete the Persistent Volume Claims {#delete-pvc}

Delete the PVCs associated with the pods:

```bash
kubectl delete pvc <pvc-name> -n <namespace>
```

If your StatefulSet has multiple replicas, delete PVCs for each of them. To find the PVCs, run the following command:

```bash
kubectl get pvc -n <namespace>
```

### Step 4. Re-enable the operator {#enable-operator}

Set the `isManaged` field back to `true` in your {{product-name}} resource specification:

```yaml
spec:
  isManaged: true
```

Apply this change. The operator will now recreate the StatefulSet and PVCs with the new volume settings.

## Example: increasing disk space for scheduler logs {#example}

A common use case for updating volumes is to increase disk space for logs. For example, you need to increase disk space for scheduler logs from 10 GiB to 50 GiB.

1. Specify the new volume size in your {{product-name}} resource specification:

```yaml
spec:
  schedulers:
    volumeClaimTemplates:
      - spec:
          resources:
            requests:
              storage: 50Gi  # increased from 10Gi
```

2. Follow the update procedure described above: set `isManaged: false`, delete the StatefulSet and PVCs, and then set `isManaged: true`.

{% note warning %}

As a result, **current logs are lost**, because the old PVCs are deleted and new ones are created. If you need to preserve logs, make sure to back them up before initiating an update.

{% endnote %}