# Installing microservices

## Description

{{product-name}} has a set of additional microservices that expand the functionality of the web interface and provide useful information for cluster administrators and users.

Currently, the following components are supported:

- **Resource Usage**: Allows you to track and analyze disk space usage by accounts. [Microservice code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/resource_usage/json_api_go), [preprocessing code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/resource_usage_roren).
- **Access Log Viewer**: Enables you to view master access logs in the UI. [Microservice code](https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/access_log_viewer/http_service), [preprocessing code](https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/access_log_viewer/preprocessing), [raw log parser code](https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/access_log_viewer/raw_access_log_preprocessing).
- **Bulk ACL Checker**: Provides an optimized way to check user access rights (ACL) for multiple paths. Reduces the load on the master. [Microservice code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/bulk_acl_checker/http_service_go), [preprocessing code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/bulk_acl_checker_roren).
- **Id To Path Updater**: A background process that parses the master's access logs and creates a mapping of `Node ID` → `Path` in a dynamic table. It is a dependency for other microservices. [Preprocessing code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/id_to_path_mapping/id_to_path_updater).

## Architecture

{{product-name}} microservices typically consist of two parts:

1. **Preprocessing (CronJob)**: A background process launched on a schedule. It collects and processes data (for example, from logs or master snapshots) and saves them in a prepared form in tables.
2. **API (Deployment)**: A constantly running web service that provides convenient access to the data prepared during the Preprocessing stage.

## Prerequisites

Before installation, make sure the following conditions are met:

1. Helm 3.x is installed.
2. The version of the {{product-name}} Kubernetes operator is not less than 0.28.0.
3. UI version from 3.6.0 and higher.
4. Loading of master snapshots [into Cypress](../../admin-guide/persistence-uploader.md#uploading-snapshots-to-cypress) and master access logs [into Cypress](../../admin-guide/logging.md#structured_log_delivery) is enabled.

Configuring both processes requires a master reboot with downtime. To minimize downtime, it is recommended to apply the settings simultaneously.

To enable these features, add the appropriate sidecar and logging settings to `spec.primaryMasters`:

```yaml
spec:
  primaryMasters:
    hydraPersistenceUploader:
      image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
    timbertruck:
      image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
    structuredLoggers:
      - name: access
        minLogLevel: info
        category: Access
        format: json
        rotationPolicy:
          maxTotalSizeToKeep: 5_000_000_000
          rotationPeriodMilliseconds: 900000
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

1. The [Cron Helm chart](../../admin-guide/install-cron.md#process_master_snapshot) is installed, with the `process_master_snapshot` task enabled. This task must run at least once and create the necessary nodes and tables, namely: `//sys/admin/snapshots/snapshot_exports` and `//sys/admin/snapshots/user_exports`.
1. [CHYT](../../admin-guide/install-chyt.md) is installed and [a clique is created](../../user-guide/data-processing/chyt/try-chyt.md). Save the clique name to the `CHYT_CLIQUE_NAME` environment variable. For the sake of clarity, our example uses the default clique `ch_public`.

```bash
export CHYT_CLIQUE_NAME=ch_public
```

Check the status of the `process_master_snapshot` task:

```bash
kubectl -n <namespace> get cronjobs ytsaurus-cron-cron-chart-process-master-snapshot -o jsonpath='{.status}'
```

We expect to see `lastSuccessfulTime` corresponding to `lastScheduleTime`:

```json
{
  "lastScheduleTime": "2025-12-01T10:00:00Z",
  "lastSuccessfulTime": "2025-12-01T10:05:09Z"
}
```

Check for the presence of the necessary nodes and tables:

```bash
yt list //sys/admin/snapshots/snapshot_exports
yt list //sys/admin/snapshots/user_exports
```

We should see at least one snapshot with a name like `000000068.snapshot_3163fafb_unified_export` and `000000068.snapshot_3163fafb_user_export`, respectively.

## Preparation and installation

### Step 1: Preparing users and granting permissions (ACL)

Each microservice requires its own robot user:

- `robot-msvc-access-log-viewer` for Access Log Viewer
- `robot-msvc-acl-checker` for Bulk ACL Checker
- `robot-msvc-id-to-path` for Id To Path Updater
- `robot-msvc-resource-usage` for Resource Usage

Create users [according to the instructions](../../user-guide/storage/auth.md):

```bash
yt create user --attr "{name=robot-msvc-access-log-viewer}"
yt create user --attr "{name=robot-msvc-acl-checker}"
yt create user --attr "{name=robot-msvc-id-to-path}"
yt create user --attr "{name=robot-msvc-resource-usage}"
```

Create working nodes for microservices in `//sys/admin/yt-microservices`:

```bash
yt create map_node //sys/admin/yt-microservices/access_log_viewer --recursive
yt create map_node //sys/admin/yt-microservices/bulk_acl_checker
yt create map_node //sys/admin/yt-microservices/node_id_dict
yt create map_node //sys/admin/yt-microservices/raw_master_access_log_processing
yt create map_node //sys/admin/yt-microservices/resource_usage
```

Create directories for parsed master access logs:

```bash
yt create map_node //sys/admin/logs/export/master-access-parsed/1d --recursive
yt create map_node //sys/admin/logs/export/master-access-parsed/30min
```

Create working directories of temporary files for microservices:

```bash
yt create map_node //sys/admin/yt-microservices/tmp/access_log_viewer/30min --recursive
yt create map_node //sys/admin/yt-microservices/tmp/access_log_viewer/1d
yt create map_node //sys/admin/yt-microservices/tmp/id_to_path_updater
```

{% note info %}

It is recommended to create a separate account for microservice working directories and grant `use` ACL to it for the respective robots. For simplicity, in this example, we will work with the `sys` account.

{% endnote %}

Grant access permissions for each user.

- For Resource Usage:

Grant read permissions for snapshots, access to microservice working directories, and account usage.

```bash
yt set //sys/admin/snapshots/snapshot_exports/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read]}'
yt set //sys/admin/yt-microservices/node_id_dict/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read]}'
yt set //sys/admin/yt-microservices/resource_usage/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read; write; create; remove; mount]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[use]}'
```

- For Bulk ACL Checker:

Grant read permissions for user exports, access to microservice nodes, and account usage.

```bash
yt set //sys/admin/snapshots/@acl/end '{action=allow; subjects=[robot-msvc-acl-checker]; permissions=[read]}'
yt set //sys/admin/yt-microservices/bulk_acl_checker/@acl/end '{action=allow; subjects=[robot-msvc-acl-checker]; permissions=[read; write; create; remove; mount]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-acl-checker]; permissions=[use]}'
```

- For Id To Path Updater:

Enable `bulk_insert` for the user and for the entire cluster (if not already enabled).

```bash
yt set //sys/users/robot-msvc-id-to-path/@enable_bulk_insert %true
yt set //sys/@config/tablet_manager/enable_bulk_insert %true
```

Grant read and marking permissions for logs, write access to the microservice node, and account usage.

```bash
yt set //sys/admin/logs/export/master-access/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[read; write]}'
yt set //sys/admin/yt-microservices/node_id_dict/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[read; write; create; remove; mount]}'
yt set //sys/admin/yt-microservices/tmp/id_to_path_updater/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[read; write; create; remove]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[use]}'
yt set //sys/tablet_cell_bundles/sys/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[use]}'
```

- For Access Log Viewer:

  Grant permissions to read and mark logs, write to the working directory of the microservice, use the account, and access the clique.

  ```bash
  yt set //sys/admin/logs/export/master-access/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write]}'
  yt set //sys/admin/yt-microservices/node_id_dict/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read]}'
  yt set //sys/admin/yt-microservices/tmp/access_log_viewer/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; create; remove]}'
  yt set //sys/admin/yt-microservices/access_log_viewer/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; create; remove; mount]}'
  yt set //sys/admin/yt-microservices/raw_master_access_log_processing/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; create; remove]}'
  yt set //sys/admin/logs/export/master-access-parsed/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; remove]}'
  yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[use]}'
  yt set //sys/access_control_object_namespaces/chyt/$CHYT_CLIQUE_NAME/principal/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[use]}'
  ```

  Specify a [dictionary](../../user-guide/data-processing/chyt/reference/configuration.md#clickhouse_config) for the CHYT clique that will access Bulk ACL Checker:

  1. Add the `ACL` dictionary:

    ```
    if [ $(yt exists //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/dictionaries) == "false" ]; then
        yt set --format json //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/dictionaries '[]' --recursive
    fi

    yt set --format json //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/dictionaries/end '{
        "layout": {
            "complex_key_cache": {
                "size_in_cells": 1000000
            }
        },
        "name": "ACL",
        "lifetime": 300,
        "structure": {
            "attribute": [
                {
                    "name": "action",
                    "type": "String",
                    "null_value": "NoAnswer"
                }
            ],
            "key": {
                "attribute": [
                    {
                        "name": "cluster",
                        "type": "String"
                    },
                    {
                        "name": "subject",
                        "type": "String"
                    },
                    {
                        "name": "path",
                        "type": "String"
                    }
                ]
            }
        },
        "source": {
            "http": {
                "url": "http://ytsaurus-msvc-ytmsvc-chart-bulk-acl-checker-service.default.svc.cluster.local:80/clickhouse-dict",
                "format": "JSONEachRow"
            }
        }
    }'
    ```

  2. Enable `cancel_http_readonly_queries_on_client_close`:

    ```bash
    yt set -r //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/settings/cancel_http_readonly_queries_on_client_close 1
    ```

  The URL for Bulk ACL Checker, which will be deployed in the `default` namespace, is specified in `dictionaries[].source.http.url`. If you're using a different one, specify it.

### Step 2: Preparing a Kubernetes Secret with tokens

By default, a single shared secret named `ytsaurus-msvc` is used. Each microservice and its preprocessing use a token from their own environment variable.

- For Resource Usage: `YT_RESOURCE_USAGE_TOKEN`.
- For Bulk ACL Checker: `YT_BULK_ACL_CHECKER_TOKEN`.
- For Id To Path Updater: `YT_ID_TO_PATH_TOKEN`.
- For Access Log Viewer: `YT_ACCESS_LOG_VIEWER_TOKEN`.

Example of creating a secret. The command will issue the necessary tokens and insert them into the secret:

```bash
kubectl create secret generic ytsaurus-msvc \
  --from-literal=YT_RESOURCE_USAGE_TOKEN="$(yt issue-token robot-msvc-resource-usage)" \
  --from-literal=YT_BULK_ACL_CHECKER_TOKEN="$(yt issue-token robot-msvc-acl-checker)" \
  --from-literal=YT_ID_TO_PATH_TOKEN="$(yt issue-token robot-msvc-id-to-path)" \
  --from-literal=YT_ACCESS_LOG_VIEWER_TOKEN="$(yt issue-token robot-msvc-access-log-viewer)" \
  --from-literal=CRYPTO_SECRET="$(openssl rand -hex 32)" \
  -n <namespace>
```

If necessary, you can create separate secrets by specifying them in `.microservices.<name>.secretRefs`.

### Step 3: Preparing `values.yaml`

- Specify the proxy and cluster name:

```yaml
cluster:
  proxy: "http-proxies.default.svc.cluster.local" # Internal HTTP proxy address for the `default` namespace
  name: "<cluster-name>" # Your cluster name (see in `//sys/@cluster_connection/cluster_name`)
```

- Configure Access Log Viewer:

  Replace `ch_public` with your clique from `CHYT_CLIQUE_NAME`.

  ```yaml
  microservices:
    accessLogViewer:
      api:
        config:
          chytAlias: "ch_public"
  ```

Other parameters work out of the box. For more details, see the [detailed configuration](#detailed-config).

- Example of a minimal `values.yaml`:

  ```yaml
  cluster:
    proxy: "http-proxies.default.svc.cluster.local"
    name: "<cluster-name>"

  microservices:
    accessLogViewer:
      api:
        config:
          chytAlias: "ch_public"
  ```

The full list of parameters is available in [values.yaml](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/charts/ytmsvc-chart/values.yaml) in the chart repository. A detailed description is provided below in the [Detailed configuration](#detailed-config) section.

### Step 4: Installing the Helm chart

```bash
helm install ytsaurus-msvc oci://ghcr.io/ytsaurus/ytmsvc-chart \
  --version {{ytmsvc-version}} \
  -f values.yaml \
  -n <namespace>
```

View the list of pods and cronjobs created by the chart:
```bash
kubectl get pod -l app.kubernetes.io/name=ytmsvc-chart -n <namespace>
kubectl get cronjobs -l app.kubernetes.io/name=ytmsvc-chart -n <namespace>
```

Manually start a cron job using built-in `kubectl` tools:
```bash
kubectl create job --from=cronjob/<cron-job-name> <your-job-name> -n <namespace>
```

View logs for processing and the microservice:
```bash
kubectl logs ytsaurus-msvc-resource-usage-preprocessing-29390072-nrhj6 -n <namespace>
kubectl logs deployment/ytsaurus-msvc-ytmsvc-chart-resource-usage-api -n <namespace>
```

### Step 5: Integrating with the UI

To integrate microservices with the UI, specify their internal addresses in the configuration. Microservice requests pass through the UI backend, so internal Kubernetes addresses are used.

Create `//sys/@ui_config/ui_settings` if it doesn't exist and specify the microservice addresses:

```bash
if [ $(yt exists //sys/@ui_config/ui_settings) == "false" ]; then
    yt set //sys/@ui_config/ui_settings '{}'
fi

yt set //sys/@ui_config/ui_settings/accounts_usage_base_path '"http://ytsaurus-msvc-ytmsvc-chart-resource-usage-service.default.svc.cluster.local"'
yt set //sys/@ui_config/ui_settings/access_log_base_path '"http://ytsaurus-msvc-ytmsvc-chart-access-log-viewer-service.default.svc.cluster.local"'
```

{% note info %}

If microservices are deployed in a namespace different from `default`, replace `default` with the name of the namespace.

{% endnote %}

#### Verifying the result

- Resource Usage operation:

  1. Make sure that `cronjobs` completed and the `//sys/admin/yt-microservices/resource_usage` directory contains preprocessing tables.
  2. Open the {{product-name}} web interface.
  3. Go to `Accounts` and select any account.
  4. Open the `Detailed usage` tab at the top.

- Access Log Viewer operation:

  1. Make sure that `cronjobs` completed and the `//sys/admin/yt-microservices/access_log_viewer/<cluster_name>` directory contains log tables. If the cluster is new, wait 1–2 hours from the moment it was created.
  2. Open the {{product-name}} web interface.
  3. Go to `Navigation` and open the object you want to look at.
  4. Open the `Access log` tab at the top.

{% note info %}

It may take some time (up to five minutes) for the `Detailed usage` or `Access log` tab to appear, and you may need to refresh the page.

If the tab does not appear or something goes wrong, check:
- The correctness of addresses in `//sys/@ui_config/ui_settings/accounts_usage_base_path` and `//sys/@ui_config/ui_settings/access_log_base_path`.
- Microservice logs: `kubectl logs deployment/ytsaurus-msvc-ytmsvc-chart-resource-usage-api -n <namespace>`.

{% endnote %}


## Detailed configuration {#detailed-config}

### General parameters

  ```yaml
  cluster:
    # Internal HTTP proxy address
    proxy: "http-proxies-lb.default.svc.cluster.local"
    # Cluster name
    name: "ytsaurus"

  # User authentication cookie
  authCookieName: "YTCypressCookie"

  # Secrets that will be available from pods
  secretRefs: [ytsaurus-msvc]
  ```

### Resource Usage API

  Configuration of the Resource Usage API service:

  ```yaml
  microservices:
    resourceUsage:
      api:
        config:
          # Address and port for the main API HTTP server
          httpAddr: "[::]:80"

          # Timeout for HTTP request handlers
          httpHandlerTimeout: 120s

          # Address and port for metric collection
          debugHttpAddr: "[::]:81"

          # Authorization cookie name (must match the UI settings)
          authCookieName: YTCypressCookie

          # Path for storing processed snapshots
          snapshotRoot: //sys/admin/yt-microservices/resource_usage

          # List of fields to exclude from API responses
          excludedFields: []
  ```

### Resource Usage: cleaning up old snapshots

Configuration for automatic cleaning of old snapshots:

```yaml
microservices:
  resourceUsage:
    removeExcessive:
      # Enable cleaning of old snapshots
      enabled: true

      config:
        # Thinning frequency factor: the higher the value, the fewer old snapshots will remain
        denominator: 1.4

        # Factor for increasing the time interval when going further back in time
        stepSizeIncrease: 2.86

        # Period during which recent snapshots are not deleted
        ignoreWindowSize:
          days: 3

        # Size of the first time window for thinning
        firstStepWindowSize:
          weeks: 1

        # Minimum interval between snapshots in the first window
        firstStepAllowedFrequency:
          hours: 6
```

### Id To Path Updater

Configuration of the Id To Path Updater microservice:

```yaml
microservices:
  idToPathUpdater:
    config:
      # Path to master access logs
      inputTablesSource: //sys/admin/logs/export/master-access

      # Dynamic table for the cluster → node_id → path mapping (created automatically)
      outputTable: //sys/admin/yt-microservices/node_id_dict/data

      # Node for temporary files
      tmpPath: //sys/admin/yt-microservices/tmp/id_to_path_updater
```

### Access Log Viewer API

  Configuration of the Access Log Viewer API service:

  ```yaml
  microservices:
    accessLogViewer:
      api:
        enabled: true
        config:
          # Name of the clique used
          chytAlias: "ch_public"
  ```

### Raw Log Preprocessing

  Raw access log parser configuration:

  ```yaml
  microservices:
    accessLogViewer:
      rawLogPreprocessing:
        config:
          # Directory containing temporary preprocessing files
          workPath: "//sys/admin/yt-microservices/raw_master_access_log_processing"
          # Number of concurrently processed tables with raw logs
          maxInputTables: 16
          # Settings for each log processing type
          processings:
            30min:
              # Storage timeout for output tables.
              # If not specified, no timeout is set.
              expirationTimeout: 168h
            # 1d:
            #   expirationTimeout: 672h
  ```

### Access Log Viewer Preprocessing

  Access Log Viewer preprocessing configuration:

  ```yaml
  microservices:
    accessLogViewer:
      preprocessing:
        config:
          # Number of concurrently running operations
          maxParallelOps: 1
          # Number of concurrently processed tables
          maxTables: 100
  ```

You can view all parameters in the [source code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/charts/ytmsvc-chart/values.yaml).

