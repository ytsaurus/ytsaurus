# Installing microservices

## Description

{{product-name}} has a set of additional microservices that expand the functionality of the web interface and provide useful information for cluster administrators and users.

Currently, the following components are supported:

- **Resource Usage**: Allows you to track and analyze disk space usage by accounts. [Microservice code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/resource_usage/json_api_go), [preprocessing code](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/resource_usage_roren).
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
3. Loading of master snapshots [into Cypress](../../admin-guide/persistence-uploader.md#uploading-snapshots-to-cypress) and master access logs [into Cypress](../../admin-guide/logging.md#structured_log_delivery) is enabled.

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

4. The [Cron Helm chart](../../admin-guide/install-cron.md#process_master_snapshot) is installed, with the `process_master_snapshot` task enabled. This task must run at least once and create the necessary nodes and tables, namely: `//sys/admin/snapshots/snapshot_exports` and `//sys/admin/snapshots/user_exports`.

Check the status of the `process_master_snapshot` task:

```
kubectl -n <namespace> get cronjobs ytsaurus-cron-cron-chart-process-master-snapshot -o jsonpath='{.status}'
```

We expect to see `lastSuccessfulTime` corresponding to `lastScheduleTime`:
```
{
  "lastScheduleTime": "2025-12-01T10:00:00Z",
  "lastSuccessfulTime": "2025-12-01T10:05:09Z"
}
```

Check for the presence of the necessary nodes and tables:

```
yt list //sys/admin/snapshots/snapshot_exports
yt list //sys/admin/snapshots/user_exports
```

We should see at least one snapshot with a name like `000000068.snapshot_3163fafb_unified_export` and `000000068.snapshot_3163fafb_user_export`, respectively.

## Preparation and installation

### Step 1: Preparing users and granting permissions (ACL)

Each microservice requires its own robot user:

- `robot-msvc-resource-usage` for Resource Usage
- `robot-msvc-acl-checker` for Bulk ACL Checker
- `robot-msvc-id-to-path` for Node ID Dict

Create users [according to the instructions](../../user-guide/storage/auth.md):

```bash
yt create user --attr "{name=robot-msvc-resource-usage}"
yt create user --attr "{name=robot-msvc-acl-checker}"
yt create user --attr "{name=robot-msvc-id-to-path}"
```

Create working nodes for microservices in `//sys/admin/yt-microservices`:

```bash
yt create map_node //sys/admin/yt-microservices/resource_usage --recursive
yt create map_node //sys/admin/yt-microservices/bulk_acl_checker
yt create map_node //sys/admin/yt-microservices/node_id_dict
```

{% note info %}

It is recommended to create a separate account for microservice nodes and grant `use` ACL to it for the respective robots. For simplicity, in this example, we will work with the `sys` account.

{% endnote %}

Grant access permissions for each user.

- For Resource Usage:

Grant read permissions for snapshots, access to microservice nodes, and account usage.

```bash
yt set //sys/admin/snapshots/snapshot_exports/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read]}'
yt set //sys/admin/yt-microservices/node_id_dict/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read]}'
yt set //sys/admin/yt-microservices/resource_usage/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read; write; create; remove; mount]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[use]}'
```

- For Bulk ACL Checker:

Grant read permissions for user exports, access to microservice nodes, and account usage.

```bash
yt set //sys/admin/snapshots/user_exports/@acl/end '{action=allow; subjects=[robot-msvc-acl-checker]; permissions=[read]}'
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
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[use]}'
```

### Step 2: Preparing a Kubernetes Secret with tokens

By default, a single shared secret named `ytsaurus-msvc` is used. Each microservice and its preprocessing use a token from their own environment variable.

- For Resource Usage — `YT_RESOURCE_USAGE_TOKEN`;
- For Bulk ACL Checker — `YT_BULK_ACL_CHECKER_TOKEN`;
- For ID To Path Updater — `YT_ID_TO_PATH_TOKEN`.

Example of creating a secret. The command will issue the necessary tokens and insert them into the secret:

```bash
kubectl create secret generic ytsaurus-msvc \
  --from-literal=YT_RESOURCE_USAGE_TOKEN="$(yt issue-token robot-msvc-resource-usage)" \
  --from-literal=YT_BULK_ACL_CHECKER_TOKEN="$(yt issue-token robot-msvc-acl-checker)" \
  --from-literal=YT_ID_TO_PATH_TOKEN="$(yt issue-token robot-msvc-id-to-path)" \
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

- Configure Resource Usage:

You need to specify `allowedHostSuffixes` or `allowedHosts` of your UI in `microservices.resourceUsage.api.config.cors`. More details will be provided below. Other parameters work out of the box. For more details, see the [detailed configuration section](#detailed-config).

- Example of a minimal `values.yaml`:

```yaml
cluster:
  proxy: "http-proxies.default.svc.cluster.local"
  name: "<cluster-name>"

microservices:
  resourceUsage:
    api:
      config:
        cors:
          allowedHostSuffixes:
            - "<your-ui-domain-suffix>"
```

The full list of parameters is available in [values.yaml](https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docker/charts/ytmsvc-chart/values.yaml) in the chart repository. A detailed description is provided below in the [Detailed configuration](#detailed-config) section.

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

### Step 5: Configuring network access and integrating with the UI

The Resource Usage API is used by the {{product-name}} web interface directly from the user's browser. Choose an access method depending on your environment.

#### Production

Recommended method for production. The microservice will be available on the same domain as the UI, at the path `/resource-usage/`.

1. Configure `apiPrefix` in `values.yaml`:

```yaml
microservices:
  resourceUsage:
    api:
      config:
        apiPrefix: "/resource-usage/"
```

2. Apply the changes:

```bash
helm upgrade ytsaurus-msvc oci://ghcr.io/ytsaurus/ytmsvc-chart \
  --version {{ytmsvc-version}} \
  -f values.yaml \
  -n <namespace>
```

3. Modify the Ingress:

Modify the current UI Ingress manifest so that it redirects requests `/resource-usage` to the service:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ui-ingress
  namespace: <namespace>
spec:
  rules:
    - host: <your-ui-domain> # Specify your UI domain here
      http:
        paths:
          - backend:
              service:
                name: ytsaurus-msvc-ytmsvc-chart-resource-usage-service
                port:
                  name: http
            path: /resource-usage
            pathType: Prefix
          - backend:
              service:
                name: ytsaurus-ui
                port:
                  name: http
            path: /
            pathType: Prefix
```

Apply the manifest:

```bash
kubectl apply -f ui-ingress.yaml
```

4. Specify the URL in the UI configuration:

```bash
yt set //sys/@ui_config/resource_usage_base_url '"https://<your-ui-domain>/resource-usage/"' 
```

#### Testing

This method is suitable for quick functionality testing.

1. Configure CORS in `values.yaml`:

Since the UI (e.g., `http://localhost:8080`) will access `http://localhost:3000`, the browser will block the request without explicit CORS permission. Allow requests from the UI:

```yaml
microservices:
  resourceUsage:
    api:
      config:
        apiPrefix: "/"

        cors:
          allowedHosts:
            - "localhost:8080"
```

2. Apply the changes:

```bash
helm upgrade ytsaurus-msvc oci://ghcr.io/ytsaurus/ytmsvc-chart \
  --version {{ytmsvc-version}} \
  -f values.yaml \
  -n <namespace>
```

3. Start port forwarding:

```bash
kubectl port-forward service/ytsaurus-msvc-ytmsvc-chart-resource-usage-service 3000:80 -n <namespace>
```

4. Specify the local address in the UI configuration:

```bash
yt set //sys/@ui_config/resource_usage_base_url '"http://localhost:3000/"'
```

#### Verifying the result

1. Open the {{product-name}} web interface.
2. Go to the `Accounts` section and select any account.
3. Open the `Detailed usage` tab at the top.

{% note info %}

It may take some time (no more than a few minutes) for the `Detailed usage` tab to appear, and you may need to refresh the page.

If the tab does not appear or something goes wrong, check:
- The correctness of `resource_usage_base_url` (should include the protocol and end with `/`)
- Microservice logs: `kubectl logs deployment/ytsaurus-msvc-ytmsvc-chart-resource-usage-api -n <namespace>`
- Ingress operation, if used: `kubectl get ingress -n <namespace>`

{% endnote %}

## Detailed configuration {#detailed-config}

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

        # Prefix for HTTP server handlers
        apiPrefix: "/"

        cors:
          # Lists of hosts/suffixes for CORS requests (required for UI integration)
          allowedHosts: []
          allowedHostSuffixes: []

        # Cookie name for authorization (must match UI settings)
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
      tmpPath: //tmp/microservices/id_to_path_updater
```
