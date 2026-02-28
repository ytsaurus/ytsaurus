# Installing Task proxy

## Description {#task-proxy-desc}

Task proxy provides access to web services deployed in {{product-name}} operations. For more details, see the [Task proxy](../../user-guide/proxy/task.md) section.

## Prerequisites

At this stage, you should have:

- Helm 3.x;
- a running {{product-name}} cluster and the internal HTTP proxy address;
- a special robot user `robot-task-proxy` with an issued token (see the [token management](../../user-guide/storage/auth.md#token-management) section):
   ```sh
   yt create user --attr "{name=robot-task-proxy}"
   yt issue-token robot-task-proxy > robot-task-proxy-token
   ```

## Configuration {#setup}

### Granting permissions {#permissions}

Assign the minimum required permissions (ACL) for the robot user (commands assume the user is `robot-task-proxy`):

```sh
yt set //sys/operations/@acl/end '{subjects=[robot-task-proxy];permissions=[read];action=allow}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-task-proxy]; permissions=[use]}'

yt create map_node //sys/task_proxies
yt set //sys/task_proxies/@acl/end '{action=allow; subjects=[robot-task-proxy]; permissions=[read; write; create; remove;]}'
```

### Creating Kubernetes secret with tokens {#create-secret}

Create a secret with tokens for Task proxy access to {{product-name}}.

```bash
kubectl create secret generic task-proxy-token \
    --from-file robot-task-proxy-token \
    -n ${NAMESPACE}
```

### Preparing `values.yaml` {#prepare-values}

Example Task proxy configuration:

```yaml
baseDomain: my-cluster.ytsaurus.example.net
replicas: 2
proxy:
  resources:
    requests:
      cpu: "1"
      memory: 1Gi
server:
  resources:
    requests:
      cpu: "1"
      memory: 1Gi
nodeSelector:
  yt-group: tp
```

**Parameter explanations:**

- Basic parameters:
  - `dirPath` – path to the Task proxy directory in Cypress. By default, the path to the table with service data will be `//sys/task_proxies/services`.
  - `baseDomain` – service domains use this as the base, adding a service hash, for example, `645236d8.my-cluster.ytsaurus.example.net` for the configuration example above.
  - `tokenSecretRef` – name of the k8s secret with the token for Task proxy access, default name is `task-proxy-token`.
  - `discoveryPeriodSeconds` – frequency of service discovery (task discovery) launch in seconds.
- Security parameters:
  - `auth`
    - `auth.enabled` – enable access control to the operation
    - `auth.cookieName` – authentication cookie name, default is `YTCypressCookie`
  - `tls`
    - `tls.enabled` – enable TLS for Task proxy
    - `tls.certSecretRef` – name of the k8s secret with TLS certificate
- Deployment parameters:
  - Task proxy consists of two applications located in one pod:
    - [Envoy](https://www.envoyproxy.io/), which proxies requests to jobs and controls access.
    - Server that discovers web services and supplies Envoy with a routing table via the xDS protocol.
  - `replicas` – number of Task proxy replicas (pods)
  - `proxy`
    - `proxy.resources` – resources for Envoy
    - `proxy.image`
      - `proxy.image.repository` – Envoy image repository
      - `proxy.image.tag` – Envoy image tag
  - `server`
    - `server.resources` – resources for the Task proxy server
    - `server.image`
      - `server.image.repository` – Task proxy server image repository
      - `server.image.tag` – Task proxy server image tag
  - `nodeSelector` – k8s node selector, specify your labels in it
  - `affinity` – you can set pods anti-affinity to prevent pod allocation on the same k8s nodes.

Default parameter values are specified in [`values.yaml`](https://github.com/ytsaurus/ytsaurus-task-proxy/blob/main/chart/values.yaml).

## Installing Helm chart {#helm-chart-install}

```bash
helm install task-proxy oci://ghcr.io/ytsaurus/task-proxy-chart  \
    --version ${VERSION} \
    -f values.yaml \
    -n ${NAMESPACE}
```

## Configuring ingress controller {#ingress-install}

Below is an example of configuring your ingress controller for Task proxy, specifically the gateway and routes objects.

Add routes for HTTP and gRPC protocols:
```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: task-proxy-http-https-route
  namespace: {{.Namespace}}
spec:
  hostnames:
    - "{{.TaskProxyPublicFQDN}}"
  parentRefs:
    - name: gateway
      sectionName: yt-task-proxy-http-https-listener
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - kind: Service
          name: task-proxy
          port: 80
---
apiVersion: gateway.networking.k8s.io/v1
kind: GRPCRoute
metadata:
  name: task-proxy-grpc-https-route
  namespace: {{.Namespace}}
spec:
  hostnames:
    - "{{.TaskProxyPublicFQDN}}"
  parentRefs:
    - name: gateway
      sectionName: yt-task-proxy-grpc-https-listener
  rules:
    - backendRefs:
        - kind: Service
          name: task-proxy
          port: 80
```

Add listeners corresponding to the routes in the `spec.listeners` section of the gateway object:

```yaml
...
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: gateway
  ...
spec:
  listeners:
    ...
    - name: yt-task-proxy-http-https-listener
      protocol: HTTPS
      port: 443
      hostname: "{{.TaskProxyPublicFQDN}}"
      tls:
        certificateRefs:
          - ...
    - name: yt-task-proxy-grpc-https-listener
      protocol: HTTPS
      port: 9090
      hostname: "{{.TaskProxyPublicFQDN}}"
      tls:
        certificateRefs:
          - ...
```

## TLS support {#tls}

If you need Task proxy to work with TLS, add the following parameters for the Helm chart:

```yaml
tls:
  enabled: true
  secretName: yt-domain-cert
```

Below is an example of creating the `yt-domain-cert` secret using private key and certificate chain files:

```sh
kubectl create secret tls yt-domain-cert -n yt --cert=cert.pem --key=key.pem
```