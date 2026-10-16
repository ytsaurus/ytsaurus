# Updating Cron

This article describes how to update Cron. Use Helm to perform the update.

## What you need to know before updating

When you update Cron, the {{product-name}} cluster keeps running normally, and you can perform all data operations.

During the Cron update, the following changes occur:

- The {{product-name}} cluster continues to run in normal mode.
- {{product-name}} server components don’t need to be restarted.
- Access to data via the {{product-name}} API remains available.
- Cronjobs might be temporarily unavailable during the update.

{% if audience == "public" %}Available Cron versions and their changes are described in the [Cron releases](../../../admin-guide/releases.md#cron).{% endif %}

## Preparing for the update {#before-update}

#### 1. Check the current Cron version

Check the current Cron version in Helm:

```bash
helm list -n <namespace> | grep cron
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
ytsaurus-cron   {{product-name}}    1           2026-05-27 13:23:50.855024 +0300 MSK   deployed    cron-chart-0.0.2
```

#### 2. Check Cron status

Make sure Cronjobs are running correctly:

```bash
kubectl get cronjobs -l app.kubernetes.io/name=cron-chart -n <namespace>
```

Example output:
```
NAME                                                   SCHEDULE       SUSPEND   ACTIVE   LAST SCHEDULE   AGE
ytsaurus-cron-cron-chart-clear-tmp-files               */15 * * * *   True      0        <none>          13s
ytsaurus-cron-cron-chart-clear-tmp-location            */15 * * * *   False     0        <none>          13s
ytsaurus-cron-cron-chart-clear-tmp-trash               */15 * * * *   False     0        <none>          13s
ytsaurus-cron-cron-chart-prune-offline-cluster-nodes   */15 * * * *   True      0        <none>          13s
```

#### 3. Save a backup of the Cron configuration

Save the current Cron configuration:

```bash
helm get values ytsaurus-cron -n <namespace> > cron-backup.yaml
```

## Updating Cron {#update-process}

Update Cron sequentially through all versions. For each version, follow these steps.

### 1. Get the current Cron configuration

Get the current Cron configuration:

```bash
helm get values ytsaurus-cron -n <namespace> > cron-values.yaml
```

### 2. Update Cron to the new version

Update Cron to the new version:

```bash
helm upgrade ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version <new-version> \
  -f cron-values.yaml \
  -n <namespace>
```

Example update:
```bash
helm upgrade ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version 0.0.4 \
  -f cron-values.yaml \
  -n ytsaurus
```

### 3. Monitor the update

Check Cron’s status during the update:

```bash
helm status ytsaurus-cron -n <namespace>
```

### 4. Verify the result

Make sure Cron updated successfully:

```bash
helm list -n <namespace> | grep cron
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
ytsaurus-cron   {{product-name}}    2           2026-05-27 13:24:38.855024 +0300 MSK   deployed    cron-chart-0.0.4
```

Check the chart version:
```bash
helm history ytsaurus-cron -n <namespace>
```

Example output:
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Wed May 27 13:23:50 2026	superseded	cron-chart-0.0.2	           	Install complete
2       	Wed May 27 13:24:38 2026	deployed  	cron-chart-0.0.4	           	Upgrade complete
```

Check for new cronjobs:
```bash
kubectl get cronjobs -l app.kubernetes.io/name=cron-chart -n <namespace>
```

Example output:
```
NAME                                                   SCHEDULE       SUSPEND   ACTIVE   LAST SCHEDULE   AGE
ytsaurus-cron-cron-chart-clear-tmp-files               */15 * * * *   True      0        <none>          94s
ytsaurus-cron-cron-chart-clear-tmp-location            */15 * * * *   False     0        <none>          94s
ytsaurus-cron-cron-chart-clear-tmp-trash               */15 * * * *   False     0        <none>          94s
ytsaurus-cron-cron-chart-process-master-snapshot       0 * * * *      True      0        <none>          45s
ytsaurus-cron-cron-chart-prune-offline-cluster-nodes   */15 * * * *   True      0        <none>          94s
```

## Rolling back to the previous version {#rollback}

Roll back Cron to the previous version without losing data.

#### Steps to roll back

1. Check the update history:

```bash
helm history ytsaurus-cron -n <namespace>
```

Example output:
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Wed May 27 13:23:50 2026	superseded	cron-chart-0.0.2	           	Install complete
2       	Wed May 27 13:24:38 2026	deployed  	cron-chart-0.0.4	           	Upgrade complete
```

2. Roll back to the previous version:

```bash
helm rollback ytsaurus-cron -n <namespace>
```

3. Verify the result:

```bash
helm list -n <namespace> | grep cron
```

## Managing release history {#history-management}

Helm stores the history of all release updates, which lets you roll back to previous versions. The release history doesn’t consume cluster resources and is stored in Kubernetes Secrets.

### Viewing the history

```bash
helm history ytsaurus-cron -n <namespace>
```

### Clearing the history

If you’re sure you won’t roll back to older versions, you can clear the release history.

{% note warning "Warning" %}

Clearing the release history is irreversible. After you delete the history, you can’t roll back to previous versions.

{% endnote %}

To clear the history, delete and reinstall the release:

```bash
helm uninstall ytsaurus-cron -n <namespace>
helm install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version <version> \
  -f cron-values.yaml \
  -n <namespace>
```
