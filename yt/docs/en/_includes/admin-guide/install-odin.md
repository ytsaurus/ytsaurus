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


Odin can expose metrics in Prometheus format. By default, services that publish metrics are created (you can disable them by setting `metrics.enable: false`), and a `ServiceMonitor` is created for the Prometheus Operator to automatically collect these metrics (you can disable it by setting `metrics.serviceMonitor.enable: false`). The list of metrics and Odin checks is provided in the [Monitoring](../../admin-guide/monitoring.md) section. In addition to collecting metrics, the chart can generate alerting rules (`PrometheusRule`) based on them — see the [Alerting (Prometheus)](#alerting) section.

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

## Alerting (Prometheus) {#alerting}

In addition to collecting metrics, starting from version 0.0.10, the chart can create a `PrometheusRule` resource (requires the [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator) to be installed) with alerting rules based on Odin check metrics. By default, this feature is disabled (`rule.enable: false`). To use it, you must enable metric collection (`metrics.enable: true`).

#### Enabling

```yaml
rule:
  enable: true
  # Labels that the Prometheus Operator uses to pick up the rule.
  labels:
    release: kube-prom
    yt_alerts: "true"
  # Rule group name.
  groupName: odin.checks
```

#### Which alerts are generated

For each enabled check, up to three alerts are created based on its status:

- `OdinCheck<Name>Failed` — the check failed (status `0`, `2`, or `3`);
- `OdinCheck<Name>Partial` — the check is partially available (status `0.5`);
- `OdinCheck<Name>Absent` — the check metric is missing. Created separately for each cluster in `config.odin.clusters`.


#### Default configuration (`rule.defaults`)


All alert parameters are set in the `rule.defaults` section. Below are the default values:


```yaml
rule:
  defaults:
    # Time the condition must hold before the alert fires.
    failureFor: 5m   # for *Failed alerts
    partialFor: 5m   # for *Partial alerts
    absentFor: 3m    # for *Absent alerts
    # Severity level (severity label of the alert).
    severityFail: critical
    severityPartial: warning
    severityAbsent: warning
    # Additional labels added to all alerts.
    extraLabels: {}
    # Summary and description templates for alerts.
    summaryFail: 'Odin check "{{ .displayName }}" failed on cluster {{`{{ $labels.cluster }}`}}'
    summaryPartial: 'Odin check "{{ .displayName }}" is partially available on cluster {{`{{ $labels.cluster }}`}}'
    summaryAbsent: 'Odin is not reporting check "{{ .displayName }}" on cluster {{ .cluster }}'
    descriptionFail: |
      yt_odin_{{ .name }} state is {{`{{ $value }}`}} on cluster {{`{{ $labels.cluster }}`}}.
    descriptionPartial: |
      yt_odin_{{ .name }} state is 0.5 on cluster {{`{{ $labels.cluster }}`}}.
    descriptionAbsent: |
      No samples for yt_odin_{{ .name }}{cluster="{{ .cluster }}"}.
```

Chart variables are available in `summary*` / `description*` templates:

- `{{ .name }}` — technical name of the check (e.g., `scheduler`);
- `{{ .displayName }}` — display name of the check (`displayName`);
- `{{ .cluster }}` — cluster name (available only in `*Absent` templates).

Constructs like `{{`{{ $labels.cluster }}`}}` and `{{`{{ $value }}`}}` are escaped and included in the final `PrometheusRule` as‑is — Prometheus substitutes them during alerting.

#### Overriding for a specific check (`<check>.alert`)


You can override any parameter from `rule.defaults` in the `alert` block of a specific check, and also fully or partially disable its alerts:


```yaml
config:
  checks:
    scheduler:
      displayName: Scheduler
      enable: true
      alert:
        # Override default values for this check.
        failureFor: 1m
        partialFor: 1m
        absentFor: 5m
        severityFail: critical
        severityPartial: warning
        severityAbsent: warning
        extraLabels: {}
        # Override text templates (optional).
        summaryFail: "Scheduler is down on {{`{{ $labels.cluster }}`}}"
        # similarly for summaryPartial / summaryAbsent / descriptionFail / descriptionPartial / descriptionAbsent.
    lost_vital_chunks:
      displayName: Lost Vital Chunks
      enable: true
      alert:
        failureFor: 1m
        # Flags to disable individual alerts:
        disable: false          # true — disable all check alerts
        disableFail: false      # true — do not create *Failed alert
        disablePartial: false   # true — do not create *Partial alert
        disableAbsent: true     # true — do not create *Absent alerts
```

Values in the `config.checks.<check>.alert` block take precedence over `rule.defaults`. See [`values.yaml`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/charts/odin-chart/values.yaml) for the full list of parameters and their default values.


## Updating and Uninstalling

**Update configuration:**

```bash
helm upgrade odin oci://ghcr.io/ytsaurus/odin-chart \
    --version {{odin-version}} \
    -f values.yaml \
    -n <namespace>
```

> We do not recommend using the `--reuse-values` flag when upgrading a release. This flag preserves old parameter values (in particular, `$Values.image.tag`) from previous chart versions, which may lead to undefined behavior, version conflicts, and unexpected errors. Always explicitly pass the up‑to‑date configuration file using `-f` or `--set`.

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