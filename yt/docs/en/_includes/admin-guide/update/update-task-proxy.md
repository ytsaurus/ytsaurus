# Updating Task-proxy

This article explains how to update Task-proxy using Helm.

## What you need to know before the update {#important}

When you update Task-proxy, the {{product-name}} cluster keeps running normally, and all data operations remain available.

During the Task-proxy update, the following changes happen:

- The {{product-name}} cluster runs in standard mode.
- The {{product-name}} server components aren’t restarted.
- Access to data via the {{product-name}} API is preserved.
- Task-proxy is temporarily unavailable while the pods are being updated.

You can find available Task-proxy versions and their changes in the [Task-proxy releases](../../../admin-guide/releases.md#task-proxy).

## Preparing for the update {#before-update}

#### 1. Check the current Task-proxy version

Check the current Task-proxy version:

```bash
helm list -n <namespace> | grep task-proxy
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART                   APP VERSION
task-proxy      ytsaurus    1           2026-05-28 12:29:52.273705 +0300 MSK   deployed    task-proxy-chart-0.2.2
```

#### 2. Check the Task-proxy status

Make sure the Task-proxy pods are in the `Running` state:

```bash
kubectl get pods -n <namespace> | grep task-proxy
```

Example output:
```
task-proxy-d9fcb9485-txx2k                             2/2     Running            0             49s
```

If the Task-proxy pods aren’t in the `Running` state, wait for them to start or fix any issues.

#### 3. Save a backup of the Task-proxy configuration

Save the current Task-proxy configuration:

```bash
helm get values task-proxy -n <namespace> -o yaml > task-proxy-backup.yaml
```

## Updating Task-proxy {#update-process}

You can skip intermediate versions if the releases don’t require a sequential update.

### 1. Get the current Task-proxy configuration

Get the current Task-proxy configuration:

```bash
helm get values task-proxy -n <namespace> -o yaml > task-proxy-values.yaml
```

### 2. Update Task-proxy to the new version

Update Task-proxy to the new version:

```bash
helm upgrade task-proxy oci://ghcr.io/ytsaurus/task-proxy-chart \
  --version <new-version> \
  -f task-proxy-values.yaml \
  -n <namespace>
```

Example of updating from version 0.2.2 to 0.3.0:
```bash
helm upgrade task-proxy oci://ghcr.io/ytsaurus/task-proxy-chart \
  --version 0.3.0 \
  -f task-proxy-values.yaml \
  -n ytsaurus
```

### 3. Monitor the update

Watch the Task-proxy pod status during the update:

```bash
kubectl get pods -n <namespace> -w | grep task-proxy
```

Task-proxy goes through the following update states:

| Status | Description |
| --- | --- |
| `ContainerCreating` | New pods with the new version are being created |
| `Running` | New pods are successfully running |
| `Terminating` | Old pods are being terminated |

### 4. Verify the result

Confirm that Task-proxy was updated successfully:

```bash
helm list -n <namespace> | grep task-proxy
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART                   APP VERSION
task-proxy      ytsaurus    2           2026-05-28 12:42:06.910572 +0300 MSK   deployed    task-proxy-chart-0.3.0
```

Check the pod status:

```bash
kubectl get pods -n <namespace> | grep task-proxy
```

Example output:
```
task-proxy-6655cf875b-dmwcf                                  2/2     Running            0             4m20s
```

Check the logs:

```bash
kubectl logs -n <namespace> <task-proxy-pod-name> --tail=20
```

## Rolling back to the previous version {#rollback}

Roll back Task-proxy to the previous version without losing data.

#### Steps to roll back

1. Check the update history

```bash
helm history task-proxy -n <namespace>
```

Example output:
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Thu May 28 12:29:52 2026	superseded	task-proxy-chart-0.2.2	           	Install complete
2       	Thu May 28 12:42:06 2026	deployed  	task-proxy-chart-0.3.0	           	Upgrade complete
```

2. Roll back to the previous version

```bash
helm rollback task-proxy -n <namespace>
```

Example output:
```
Rollback was a success! Happy Helming!
```

3. Verify the result

```bash
helm list -n <namespace> | grep task-proxy
```

Example output:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART                   APP VERSION
task-proxy      ytsaurus    3           2026-05-28 13:11:01.812794 +0300 MSK   deployed    task-proxy-chart-0.2.2
```

4. Check the pod status

```bash
kubectl get pods -n <namespace> | grep task-proxy
```

Example output:
```
task-proxy-d9fcb9485-txx2k                             2/2     Running            0             2m3s
```

5. Check the logs

```bash
kubectl logs -n <namespace> <task-proxy-pod-name> --tail=5
```

{% note info "Note" %}

During a rollback, Helm automatically creates new pods with the previous chart version and deletes the old pods. Task-proxy starts successfully and performs its functions after the rollback.

{% endnote %}
