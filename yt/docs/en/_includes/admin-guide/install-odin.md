# Installing Odin

## Overview

Odin is a service that provides qualitative monitoring of a {{product-name}} cluster. For more details, see the [Monitoring](../../admin-guide/monitoring.md#odin) section.

## Prerequisites

Before proceeding, you should have:

* Helm 3.x;
* a running {{product-name}} cluster and internal address of its HTTP proxy;
* a dedicated service user `robot-odin` with an issued token (see [Token Management](../../user-guide/storage/auth.md#token-management)):

```
yt create user --attr "{name=robot-odin}"
yt issue-token robot-odin
```

## Configuration

Odin can be deployed to monitor multiple clusters. In this case, you must select one cluster as the primary, where Odin will store its state, and create the corresponding service user on each cluster.

#### Granting Permissions

Assign the minimal required ACLs to the service user (assuming the user is `robot-odin`):

```bash
yt set //sys/@acl/end '{action=allow; subjects=[robot-odin]; permissions=[read; write; create; remove; mount]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-odin]; permissions=[use]}'
yt set //sys/tablet_cell_bundles/sys/@acl/end '{subjects=[robot-odin];permissions=[use];action=allow}'
yt set //sys/operations/@acl/end '{subjects=[robot-odin];permissions=[read];action=allow}'
```

#### Creating a Kubernetes Secret with Tokens

Create a secret with tokens for Odin’s access to {{product-name}}:

```bash
kubectl create secret generic odin-secrets \
    --from-literal=YT_TOKEN="<robot-odin-token>" \
    -n <namespace>
```

#### Preparing `values.yaml` {#prepare-values}

A minimal example of Odin configuration for connecting to {{product-name}} via HTTP proxy and exposing Odin’s web service on port 9002:

```yaml
config:
  odin:
    db:
      proxy: "http://http-proxies.default.svc.cluster.local"
    clusters:
      - proxy: "http://http-proxies.default.svc.cluster.local"
        cluster: minisaurus
        tokenEnvVariable: "YT_TOKEN"

webservice:
  service:
    port: 9002
```

**Parameter descriptions:**

* `config.odin.db.proxy` — HTTP proxy address of the {{product-name}} cluster where Odin will store its state.
* `config.odin.clusters[]` — list of {{product-name}} clusters monitored by Odin. For each cluster:
  * `proxy` — HTTP proxy address inside Kubernetes (or external address if outside).
  * `cluster` — {{product-name}} cluster name.
  * `tokenEnvVariable` — environment variable name in the container to read the token from (see secrets above).
* `webservice.service.port` — Odin web service port.

> Verify DNS names of the services: `http-proxies.default.svc.cluster.local` is an example for a `http-proxies` service in the `default` namespace.

By default, an Init Job will run to create the necessary tables for storing state. You can disable it by setting `config.odin.db.initialize: false`.

Odin can expose metrics in Prometheus format. Services for monitoring are created by default (you can disable them by setting `metrics.enable`). By default, a ServiceMonitor is not created, but you can enable it by setting `metrics.serviceMonitor.enable`. For more details, see the [Monitoring](../../admin-guide/monitoring.md) section.

## Installing the Helm Chart

```bash
helm install odin oci://ghcr.io/ytsaurus/odin-chart \
    --version {{odin-version}} \
    -f values.yaml \
    -n <namespace>
```

The Helm chart first runs an Init Job to create the required table for Odin’s state in the cluster, and then deploys two Deployments — one with Odin itself and one with its web service.

## Post-Installation Checks

1. **Check resource status:**

```bash
kubectl get pods,svc,deploy,cm,secret -n <namespace> | grep -i odin
```

2. **Check pod logs:**

```bash
kubectl logs deploy/odin-odin-chart -n <namespace> --tail=200
```

## UI Integration

{{product-name}} UI includes a page that displays Odin check results. To enable it, specify the Odin web service address in the UI configuration.

The UI must be installed as a Helm chart (see [installation guide](install-ytsaurus#ui)).

Specify the Odin web service address in the `values.yaml` file under `.settings.odinBaseUrl`. Example address when Odin is deployed in the `default` namespace and exposed on port 9002 (default):
`"http://odin-odin-chart-web.default.svc.cluster.local:9002"`.

## Enabling and Disabling Checks

The list of checks is configured in the `config.checks` section of `values.yaml`.
Each check is described by a structure of the following form:

```yaml
sort_result:
  displayName: Sort Result
  enable: true
  config: {...}
```

- `enable: true` — the check is included in the final configuration and executed by Odin;
- `enable: false` — the check is skipped (not included in `checks/config.json`).

> Some checks also support an internal `config.enable` flag.
> If `enable: true` but `config.enable: false`, the check will be present in the configuration and visible in the Odin UI, but it will not be executed.

#### Example: disabling a check

```yaml
suspicious_jobs:
  displayName: Suspicious Jobs
  enable: false
  config:
    options:
      critical_suspicious_job_inactivity_timeout: 420
```

In this case, the **Suspicious Jobs** check is completely excluded from the configuration.

#### Example: partial disable with `config.enable`

```yaml
operations_snapshots:
  displayName: Operations Snapshots
  enable: true
  config:
    enable: false
    options:
      critical_time_without_snapshot_threshold: 3600
```

Here, the check is included in the configuration, but its execution is disabled inside Odin.

## Updating and Uninstalling

**Update configuration:**

```bash
helm upgrade odin oci://ghcr.io/ytsaurus/odin-chart \
    --version {{odin-version}} \
    -f values.yaml \
    -n <namespace>
```

**Uninstall release:**

```bash
helm uninstall odin -n <namespace>
```

## Common Issues and Troubleshooting

* **Invalid `proxy` address:** connection/authentication errors in Odin logs. Check service DNS name, namespace, and availability of {{product-name}} HTTP proxy;
* **Token issues:** 401/403 errors in Odin logs. Verify that the environment variable points to the correct secret key and that ACLs are properly granted to `robot-odin`;
* **Insufficient ACLs:** `create/remove/mount` operations fail. Recheck ACLs for `//sys` and/or required directories/tables;
* **Service port conflict:** when exposing via Ingress/NodePort, ensure port `9002` is available and corresponding resources exist.

## Quick Token Self-Check (outside Helm)

```bash
curl -sS -H "Authorization: OAuth $YT_TOKEN" http://http-proxies.default.svc.cluster.local/auth/whoami
```

## Security Notes

* Follow the principle of least privilege when assigning rights to `robot-odin`; create dedicated ACLs for target paths when necessary;
* Rotate tokens according to your internal policies and update secrets promptly (via `kubectl apply -f` or `kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`).

