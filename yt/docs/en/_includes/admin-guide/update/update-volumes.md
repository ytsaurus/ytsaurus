# Updating volumes in {{product-name}} services

This document explains how to change volume settings for {{product-name}} services in Kubernetes.

## Important considerations {#note}

When updating volumes:

1. **The operator doesn’t apply updates automatically** — it ignores changes to volume configurations. Changing the resource specification alone won’t trigger an update.
2. **PVCs remain after deleting pods or StatefulSets**, blocking new volume settings.

{% note warning %}

Updating volumes this way causes data loss because the system re‑creates them. Don’t apply this procedure to stateful components (e.g., masters and data nodes) — their volumes store cluster data that must be migrated beforehand. Otherwise, data will be permanently lost.

{% endnote %}

## Procedure {#steps}

To update volume settings:

### Step 1. Disable the operator {#disable-operator}

Set `isManaged: false` in the {{product-name}} resource specification to stop the operator from interfering with manual changes:

```yaml
spec:
  isManaged: false
```

Apply the change to the cluster.

### Step 2. Delete the StatefulSet {#delete-statefulset}

Delete the StatefulSet for the service whose volumes you’re updating:

```bash
kubectl delete statefulset <statefulset-name> -n <namespace>
```

### Step 3. Delete Persistent Volume Claims {#delete-pvc}

If your StatefulSet has multiple replicas, delete each associated PVC. To find relevant PVCs, run:

```bash
kubectl get pvc -n <namespace>
```

Then delete them:

```bash
kubectl delete pvc <pvc-name> -n <namespace>
```

### Step 4. Enable the operator {#enable-operator}

Set `isManaged: true` in the {{product-name}} resource specification:

```yaml
spec:
  isManaged: true
```

Apply the change. The operator re‑creates the StatefulSet and PVCs with the new volume settings.

## Example: Increasing space for scheduler logs {#example}

Suppose you need to increase scheduler log storage from 10 GB to 50 GB.

1. To specify the new volume size, update the {{product-name}} resource specification:

```yaml
spec:
  schedulers:
    volumeClaimTemplates:
      - spec:
          resources:
            requests:
              storage: 50Gi  # increased from 10Gi
```

2. Complete the update by:
   1. Setting `isManaged: false`.
   2. Deleting the StatefulSet and PVCs.
   3. Setting `isManaged: true` again.

{% note warning %}

This operation deletes current logs because old PVCs are removed and new ones are created. Back up your logs before starting the update.

{% endnote %}