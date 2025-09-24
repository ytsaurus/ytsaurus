# Cron Installation

## Description

For {{product-name}}, there is a set of cron jobs that are useful for cluster operations, such as cleaning up temporary directories or removing inactive nodes. Both built-in and custom jobs are supported.

List of built-in scripts:

* `clear_tmp` – a script that cleans temporary files on the cluster. The script’s source code lives [here](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cron/clear_tmp).
* `prune_offline_cluster_nodes` – a script that removes offline nodes from Cypress. The script’s source code lives [here](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cron/prune_offline_cluster_nodes).

## Prerequisites

At this point, you should have:

* Helm 3.x
* a running {{product-name}} cluster and the HTTP proxy address (`http_proxy`)
* a dedicated robot user for Cron with a token issued to it (see [Token Management](../../user-guide/storage/auth.md#token-management))

## Basic Installation

```bash
helm install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  --set yt.proxy="http_proxy" \
  --set yt.token="<ROBOT-CRON-TOKEN>"
```

## Configuration

All chart parameters can be set via `values.yaml` or overridden with `--set` on the command line.

## Authentication in YTsaurus

Specify the token directly:

```yaml
yt:
  proxy: yt.company.com
  token: Qwerty123!
```

Or configure it via a Kubernetes Secret:

```yaml
unmanagedSecret:
  enabled: true
  secretKeyRef:
    name: ytadminsec
    key: token
```

## Built-in Jobs (`jobs`)

Each job is defined by the following structure:
- `name`: a unique job name
- `enabled`: whether the job is enabled
- `args`: command-line arguments
- `schedule`: cron-format schedule
- `restartPolicy`: restart policy (recommended: `Never`)

Example of enabling a job:

```bash
helm upgrade --install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  --set jobs[1].enabled=true \
  --set jobs[1].args[5]="tmp_files"
```

Array indexing of `jobs` starts at zero—keep track of the job order.

## Custom Jobs

You can define your own jobs:

```yaml
additionalJobs:
  - name: my_cleanup
    enabled: true
    args:
      - clear_tmp
      - --directory "//my/custom/path"
    schedule: "0 */6 * * *"
    restartPolicy: Never
```

## Example `values.yaml`

```yaml
yt:
  proxy: yt.mycompany.com
  token: my-secret-token

jobs:
  - name: clear_tmp_files
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
  -f my-values.yaml
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


