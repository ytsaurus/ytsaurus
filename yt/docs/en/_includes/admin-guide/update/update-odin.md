# Updating Odin

This article describes how to update Odin. Perform the update using Helm.

## What you need to know before updating

When you update Odin, the {{product-name}} cluster keeps running normally, and you can perform all data operations.

During the Odin update, the following changes occur:

- The {{product-name}} cluster continues to run in standard mode.
- {{product-name}} server components don’t need to be restarted.
- Access to data via the {{product-name}} API remains available.
- Odin checks might be temporarily unavailable during the update.

{% if audience == "public" %}Available Odin versions and their changes are described in the [Odin releases](../../../admin-guide/releases.md#odin).{% endif %}

## Preparing for the update {#before-update}

#### 1. Check the current Odin version

Check the current Odin version in Helm:

```bash
helm list -n <namespace> | grep odin
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
odin            {{product-name}}    1           2026-05-28 12:29:52.273705 +0300 MSK   deployed    odin-chart-0.0.7
```

#### 2. Check Odin’s status

Make sure that the Odin pods are running correctly:

```bash
kubectl get pods,svc,deploy -n <namespace> | grep -i odin
```

Example output:
```
pod/odin-odin-chart-686c5bfbb5-t5c89                             1/1     Running            0             49s
pod/odin-odin-chart-web-75c78c8498-dzgv2                         1/1     Running            0             49s
service/odin-odin-chart-web              ClusterIP   10.100.248.191   <none>        9002/TCP         49s
deployment.apps/odin-odin-chart          1/1     1            1           49s
deployment.apps/odin-odin-chart-web      1/1     1            1           49s
```

#### 3. Save a backup of the Odin configuration

Save the current Odin configuration:

```bash
helm get values odin -n <namespace> > odin-backup.yaml
```

## Updating Odin {#update-process}

Perform the update to the target version. {% if audience == "public" %}Available Odin versions and their changes are described in the [Odin releases](../../../admin-guide/releases.md#odin).{% endif %}

### 1. Get the current Odin configuration

Get the current Odin configuration:

```bash
helm get values odin -n <namespace> > odin-values.yaml
```

### 2. Update Odin to the new version

Update Odin to the new version:

```bash
helm upgrade odin oci://ghcr.io/{{product-name}}/odin-chart \
  --version <new-version> \
  -f odin-values.yaml \
  -n <namespace>
```

Example update:
```bash
helm upgrade odin oci://ghcr.io/{{product-name}}/odin-chart \
  --version 0.0.9 \
  -f odin-values.yaml \
  -n {{product-name}}
```

### 3. Monitor the update

Keep an eye on Odin’s status during the update:

```bash
helm status odin -n <namespace>
```

### 4. Verify the result

Check that Odin updated successfully:

```bash
helm list -n <namespace> | grep odin
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
odin            {{product-name}}    2           2026-05-28 12:42:06.910572 +0300 MSK   deployed    odin-chart-0.0.9
```

Check the chart version:
```bash
helm history odin -n <namespace>
```

Example output:
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Thu May 28 12:29:52 2026	superseded	odin-chart-0.0.7	           	Install complete
2       	Thu May 28 12:42:06 2026	deployed  	odin-chart-0.0.9	           	Upgrade complete
```

Check the pods’ status after the update:
```bash
kubectl get pods,svc,deploy -n <namespace> | grep -i odin
```

Example output:
```
pod/odin-odin-chart-556fcb9db7-5fs75                             1/1     Running            0             4m20s
pod/odin-odin-chart-web-66bf9849dd-pvgrk                         1/1     Running            0             4m20s
service/odin-odin-chart-web              ClusterIP   10.100.248.191   <none>        9002/TCP         15m
deployment.apps/odin-odin-chart          1/1     1            1           15m
deployment.apps/odin-odin-chart-web      1/1     1            1           15m
```

## Rolling back to the previous version {#rollback}

Roll back Odin to the previous version without losing data.

#### Steps to roll back

1. Check the update history:

```bash
helm history odin -n <namespace>
```

Example output:
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Thu May 28 12:29:52 2026	superseded	odin-chart-0.0.7	           	Install complete
2       	Thu May 28 12:42:06 2026	deployed  	odin-chart-0.0.9	           	Upgrade complete
```

2. Roll back to the previous version:

```bash
helm rollback odin -n <namespace>
```

Example output:
```
Rollback was a success! Happy Helming!
```

3. Verify the result:

```bash
helm list -n <namespace> | grep odin
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
odin            {{product-name}}    3           2026-05-28 13:11:01.812794 +0300 MSK   deployed    odin-chart-0.0.7
```

4. Check the pods’ status:

```bash
kubectl get pods -n <namespace> | grep odin
```

Example output:
```
odin-odin-chart-686c5bfbb5-txx2k                             1/1     Running            0             2m3s
odin-odin-chart-web-75c78c8498-2xkz9                         1/1     Running            0             2m3s
```

{% note info "Note" %}

When you roll back, Helm automatically creates new pods with the previous chart version, and the old pods are deleted. Odin starts successfully and runs checks after the rollback.

{% endnote %}

## Managing release history {#history-management}

Helm stores the history of all release updates, which lets you roll back to previous versions. The release history doesn’t consume cluster resources and is stored in Kubernetes Secrets.

### Viewing the history

```bash
helm history odin -n <namespace>
```

### Clearing the history

If you’re sure you won’t roll back to older versions, you can clear the release history.

{% note warning "Warning" %}

Clearing the release history is irreversible. After you delete the history, you won’t be able to roll back to previous versions.

{% endnote %}

To clear the history, delete and reinstall the release:

```bash
helm uninstall odin -n <namespace>
helm install odin oci://ghcr.io/{{product-name}}/odin-chart \
  --version <version> \
  -f odin-values.yaml \
  -n <namespace>
```
