# Cron Installation

## Description

For {{product-name}}, there is a set of cron jobs that are useful for cluster operations, such as cleaning up temporary directories or removing inactive nodes. Both built-in and custom jobs are supported.

List of built-in scripts:

* `clear_tmp` – a script that cleans temporary files on the cluster. The script’s source code lives [here](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cron/clear_tmp).
* `prune_offline_cluster_nodes` – a script that removes offline nodes from Cypress. The script’s source code lives [here](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cron/prune_offline_cluster_nodes).
* `validate_master_snapshot` and `export_master_snapshot` — scripts for validating and exporting master snapshots to a static table. These scripts are used together. For more details, see [here](#process_master_snapshot).

## Prerequisites

At this point, you should have:

* Helm 3.x;
* a running {{product-name}} cluster and the internal HTTP proxy address;
* a special robot user `robot-cron` with a token issued for it (see the [Token management](../../user-guide/storage/auth.md#token-management) section):

  ```
  yt create user --attr "{name=robot-cron}"
  yt issue-token robot-cron
  ```

## Basic Installation

```bash
helm install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  --set yt.proxy="http://http-proxies.default.svc.cluster.local" \
  --set yt.token="<ROBOT-CRON-TOKEN>" \
  -n <namespace>
```

After this, you can check the presence of the corresponding cronjobs:

```bash
kubectl get cronjobs -l app.kubernetes.io/name=cron-chart -n <namespace>
```

You can manually run a cron job using the built-in `kubectl` tools:

```bash
kubectl create job --from=cronjob/<cron-job-name> <your-job-name> -n <namespace>
```

## Configuration

All chart parameters can be set via `-f values.yaml` or overridden with `--set` on the command line.

## Authentication in YTsaurus

Specify the token directly:

```yaml
yt:
  proxy: "http://http-proxies.default.svc.cluster.local"
  token: "<ROBOT-CRON-TOKEN>"
```

Or use Kubernetes Secret:

1. Create a secret:

  ```bash
  kubectl create secret generic yt-robot-cron-token --from-literal=token="$(yt issue-token robot-cron)"
  ```

2. In `values.yaml`, enable the use of unmanaged secrets and specify where to get the token from:

  ```yaml
  unmanagedSecret:
    enabled: true
    secretKeyRef:
      name: yt-robot-cron-token
      key: token
  ```

## Defining tasks { #jobs }

Each cron job is defined in `values.yaml` with a unique name. Built-in tasks are defined in `.jobs`. It is recommended to define additional user tasks in `.additionalJobs`.

### Simple tasks { #simple-jobs }

Runs a single script with the specified arguments.

Each job is defined by the following structure:
- `enabled`: whether the job is enabled;
- `args`: command-line arguments;
- `schedule`: cron-format schedule;
- `restartPolicy`: restart policy (recommended: `Never`).

For diagnostics, you can check the status of the pods:

```
kubectl get pods -n <namespace> -l job-name=<job-name>
```

To view the logs of a specific pod, just specify its name. Since the pod contains only one container, there is no need to specify the container name:

```
kubectl logs <pod-name> -n <namespace>
```

For example:

```
kubectl logs ytsaurus-cron-cron-chart-clear-tmp-trash-29409690-vqsh5 -n <namespace>
```

### Sequential tasks { #sequential-jobs }

Allows you to run several scripts strictly in sequence within a single run. This is useful for related operations, for example, first validate a snapshot and then export it to a static table.

Each task is defined by the following structure:
- `enabled`, `schedule`, `restartPolicy`: Similar to a simple task.
- `jobs`: A list of subtask names that will be executed in the specified sequence.
- `jobDescriptions`: A dictionary where the key is the name of the subtask from the `jobs` list, and the value is its configuration (it is possible to configure only command-line arguments via `jobDescriptions.jobName.args`).

Since all tasks are executed in the same pod, to view the logs of the corresponding job, you need to specify the name of the container, which corresponds to the names from `jobs.X.jobs`. For the `process_master_snapshot` task, the specification of which is described below, the container names will be `validate` and `export`.

```yaml
jobs:
  process_master_snapshot:
    enabled: false
    jobs:
      - validate
      - export
    jobDescriptions:
      validate:
        args:
          - validate_master_snapshot
      export:
        args:
          - export_master_snapshot
    schedule: "0 * * * *"
    restartPolicy: Never
```

To view the validation logs, respectively:

```
kubectl logs ytsaurus-cron-cron-chart-process-master-snapshot-29409720-9dmps -c validate -n <namespace>
```

The container name will correspond to the job name from `jobs`.

## Built-in tasks { #enabling-builtin-jobs }

Example of enabling a task in `values.yaml`:

```yaml
jobs:
  clear_tmp_location:
    enabled: true
    schedule: "*/30 * * * *"
```

### Processing master snapshots { #process_master_snapshot }

Corresponds to the `process_master_snapshot` key in `values.yaml`.

To work, it requires:
- Enabled [loading of master snapshots into Cypress](../../admin-guide/persistence-uploader.md#uploading-snapshots-to-cypress).
- `read`, `write`, and `remove` permissions on `//sys/admin/snapshots`:

  ```bash
  yt set //sys/admin/snapshots/@acl/end '{action=allow; subjects=[robot-cron]; permissions=[read; write; remove;]}'
  ```

- `use` permission on the account that owns `//sys/admin/snapshots` (`sys` by default):

  ```bash
  yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-cron]; permissions=[use]}'
  ```

Raw master snapshots are delivered to the `//sys/admin/snapshots/<cell_id>/snapshots` node. This process validates them and exports them in a human-readable format: the Cypress snapshot is saved to `//sys/admin/snapshots/snapshot_exports`, and the user snapshot and related ACL groups are saved to `//sys/admin/snapshots/user_exports`.

By default, processing is disabled.

To override command-line arguments, you can pass the corresponding arguments in `values.yaml`:

```yaml
jobs:
  process_master_snapshot:
    enabled: true
    jobDescriptions:
      export:
        args:
          - "export_master_snapshot"
          - "--memory-limit-gbs"
          - "4"
```

## Custom Jobs { #additionalJobs }

You can define your own jobs:

```yaml
additionalJobs:
  my_cleanup:
    enabled: true
    args:
      - clear_tmp
      - --directory
      - //my/custom/path
    schedule: "0 */6 * * *"
    restartPolicy: Never

  my_new_process:
    enabled: true
    jobs:
      - process_one
      - process_two
    jobDescriptions:
      process_two:
        args:
          - process_two
      process_one:
        args:
          - process_one
    schedule: "0 * * * *"
    restartPolicy: Never
```

## Example `values.yaml`

```yaml
yt:
  proxy: http://http-proxies.default.svc.cluster.local
  token: my-secret-token

jobs:
  clear_tmp_files:
    enabled: true
    args:
      - clear_tmp
      - --directory "//tmp/yt_wrapper/file_storage"
      - --account "tmp_files"
    schedule: "*/30 * * * *"
    restartPolicy: Never

unmanagedSecret:
  enabled: false
```

To deploy:

```bash
helm upgrade --install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  -f values.yaml
```

## Commonly Used Parameters

| Parameter                      | Description                                         |
|--------------------------------|-----------------------------------------------------|
| `yt.proxy`                     | HTTP proxy for accessing {{product-name}}           |
| `yt.token`                     | Access token (if `unmanagedSecret` is disabled)     |
| `unmanagedSecret`              | Use a Kubernetes Secret                             |
| `image.repository`             | Docker image repository                             |
| `image.tag`                    | Docker image tag                                    |
| `schedule`                     | Default schedule (if not specified per job)         |
| `concurrencyPolicy`            | `Allow`, `Forbid`, or `Replace`                     |
| `successfulJobsHistoryLimit`   | Number of successful job records to keep            |
| `failedJobsHistoryLimit`       | Number of failed job records to keep                |
| `ttlSecondsAfterFinished`      | How long job objects will be stored in Kubernetes   |

