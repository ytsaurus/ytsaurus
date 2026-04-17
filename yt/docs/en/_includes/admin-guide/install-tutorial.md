# Installing Tutorial

## Overview

Tutorial is a set of data and query examples for learning how to work with {{product-name}}. Installation is performed using a Helm chart. Installing this Helm chart creates tables with demo data in the {{product-name}} cluster and loads query examples into the Queries section for learning various system capabilities.

## Prerequisites

Before proceeding, you should have:

* Helm 3.x;
* a running {{product-name}} cluster and the address of its HTTP proxy (`http_proxy`);
* a dedicated user with an issued token (see [Token Management](../../user-guide/storage/auth.md#token-management));
* Running Query Tracker and YQL Agent.

> Typically, http-proxy address has template: http://http-proxies.default.svc.cluster.local

## Configuration

### Prepare directories

```bash
yt create map_node //home/tutorial
```

### Granting Permissions

Assign the minimal required ACLs to the user (assuming the user has permissions to create tables and execute queries):

```bash
yt add-member robot-tutorial superusers
yt set //home/tutorial/@acl/end '{action=allow; subjects=[robot-tutorial]; permissions=[read; write; create; remove; mount]}'
yt set //sys/tablet_cell_bundles/sys/@acl/end '{action=allow; subjects=[robot-tutorial];permissions=[use]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-tutorial]; permissions=[use]}'
```

> It is assumed that you have created a user `robot-tutorial`.

### Creating a Kubernetes Secret with Token

Create a secret with a token for accessing YT:

```bash
kubectl create secret generic yt-tutorial-secret \
  --from-literal=yt-token="<yt**-****-****************>" \
  -n <namespace>
```

> By default, the chart expects a secret named `yt-tutorial-secret` with a key `yt-token`. These values can be changed in `values.yaml` (see the `tutorial.secret` section) or overridden via `--set`.

## Installing the Helm Chart

Typically, installation can be performed as follows:

```bash
helm install ytsaurus-tutorial oci://ghcr.io/ytsaurus/tutorial-chart \
     --version {{tutorial-version}} \
     --set tutorial.args.proxy="http://http-proxies.<namespace>.svc.cluster.local" \
     -n <namespace>
```

List of all available parameters for overriding either via `--set` or via `values.yaml`:

```yaml
tutorial:
  secret:
    name: "yt-tutorial-secret"
    value: "yt-token"
  args:
    proxy: "http-proxies.default.svc.cluster.local"
    ytDirectory: "//home/tutorial"
    nomenclatureCount: 5000
    daysToGenerate: 14
    desiredOrderSize: 3000
    force: true
```

**Parameter descriptions:**

* `tutorial.secret.name` — name of the Kubernetes Secret with the YT access token;
* `tutorial.secret.value` — key in the secret containing the token;
* `tutorial.args.proxy` — {{product-name}} HTTP proxy address (inside Kubernetes cluster or external address);
* `tutorial.args.ytDirectory` — directory in {{product-name}} where tutorial tables will be created (default: `//home/tutorial`);
* `tutorial.args.nomenclatureCount` — number of records to generate (default: `5000`). Affects the size of the `nomenclature` table;
* `tutorial.args.daysToGenerate` — number of days of data to generate in the `prices` and `orders` tables (default: `14`);
* `tutorial.args.desiredOrderSize` — desired number of rows in the `orders` table (default: `3000`);
* `tutorial.args.force` — force overwrite of existing tables (default: `true`).

## Post-Installation Checks

1. **Check Init Job status:**

```bash
kubectl get jobs -n <namespace> | grep tutorial
```

2. **Check Init Job logs:**

```bash
kubectl logs job/ytsaurus-tutorial-tutorial-chart-init -n <namespace> --tail=200
```

3. **Check created tables in {{product-name}}:**

```bash
yt list //home/tutorial
```

## Updating and Uninstalling

**Update configuration:**

```bash
helm upgrade ytsaurus-tutorial oci://ghcr.io/ytsaurus/tutorial-chart \
  --version <new_version>> \
  -f values.yaml \
  -n <namespace>
```

> When updating, the Init Job will be run again (if `force: true`, existing tables will be overwritten).

**Uninstall release:**

```bash
helm uninstall ytsaurus-tutorial -n <namespace>
```

> **Warning:** Uninstalling the Helm chart does not remove the created tables in {{product-name}}. If you need to remove tutorial data, run:

```bash
yt remove //home/tutorial
```

## Common Issues and Troubleshooting

* **Invalid `proxy` address:** connection errors in Init Job logs. Check service DNS name, namespace, and availability of {{product-name}} HTTP proxy;
* **Token issues:** 401/403 errors in logs. Verify that the environment variable points to the correct secret key and that ACLs are properly granted to the service user;
* **Insufficient ACLs:** `create/remove` operations fail. Recheck ACLs for `//home/tutorial`;
* **Directory not empty:** if `force: false` and the `ytDirectory` directory is not empty, the Init Job will fail. Set `force: true` or clear the directory manually.

## Quick Token Self-Check (outside Helm)

```bash
curl -sS -H "Authorization: OAuth $YT_TOKEN" http://http-proxies.default.svc.cluster.local/auth/whoami
```

## Security Notes

* Follow the principle of least privilege when assigning rights to the service user; grant permissions only to necessary directories;
* Rotate tokens according to your internal policies and update secrets promptly (via `kubectl apply -f` or `kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`).

