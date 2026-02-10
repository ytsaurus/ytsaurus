# Updating volumes in {{product-name}} services

This guide describes how to update volume settings for {{product-name}} services running in Kubernetes.

## Important considerations {#note}

When working with volume updates in {{product-name}} services, keep in mind the following:

1. **Updating volume settings does not trigger any updates by the operator**. The operator does not automatically detect or apply changes to volume configurations.

2. **Persistent Volume Claims (PVCs) remain attached to pods**. Even if you delete the pods or the StatefulSet, the PVCs will remain attached and prevent the new volume settings from being applied.

{% note warning %}

This procedure will result in data loss for recreted volumes, use it with caution.

{% endnote %}

## Update procedure {#steps}

To successfully update volume settings, follow these steps:

### Step 1: Disable the operator {#disable-operator}

Set the `isManaged` field to `false` in your {{product-name}} resource specification. This disables the operator and prevents it from interfering with manual changes.

```yaml
spec:
  isManaged: false
```

Apply this change to your cluster.

### Step 2: Delete the StatefulSet {#delete-statefulset}

Delete the StatefulSet for the service whose volumes you want to update:

```bash
kubectl delete statefulset <statefulset-name> -n <namespace>
```

### Step 3: Delete the Persistent Volume Claims {#delete-pvc}

Delete the PVCs associated with the pods:

```bash
kubectl delete pvc <pvc-name> -n <namespace>
```

You may need to delete multiple PVCs if your StatefulSet has multiple replicas. List all PVCs to identify which ones need to be deleted:

```bash
kubectl get pvc -n <namespace>
```

### Step 4: Re-enable the operator {#enable-operator}

Set the `isManaged` field back to `true` in your {{product-name}} resource specification:

```yaml
spec:
  isManaged: true
```

Apply this change. The operator will now recreate the StatefulSet and PVCs with the new volume settings.

## Example: Increasing disk space for scheduler logs {#example}

A common use case for updating volumes is to increase disk space for logs. For example, if you want to increase the log volume size from 10Gi to 50Gi for schedulers.

1. Update your {{product-name}} resource specification to increase the log volume size:

```yaml
spec:
  schedulers:
    volumeClaimTemplates:
      - spec:
          resources:
            requests:
              storage: 50Gi  # increased from 10Gi
```

2. Follow the update procedure described above (set `isManaged: false`, delete StatefulSet, delete PVCs, set `isManaged: true`).

{% note warning %}

After this procedure, **current logs will be lost** because the old PVCs are deleted and new ones are created. If you need to preserve logs, make sure to back them up before proceeding with the update.

{% endnote %}