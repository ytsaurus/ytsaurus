# How to split traffic

By default, all proxies in a cluster have the `default` role. This creates a "noisy neighbor" problem: one user running a heavy read operation can overload a proxy and slow down work for everyone else, including web interface requests.

The solution is to split proxies into groups using [roles](../../../admin-guide/cluster-access-proxy/index.md#proxy-role). This lets you distribute load between light and heavy requests and isolate resources for different projects and teams.

This article shows how to:

- [Split proxies for light and heavy operations](#split-traffic)
- [Allocate proxies for a specific project](#projects)

## Splitting light and heavy traffic (for HTTP) {#split-traffic}

In practice, HTTP proxies are usually divided into two functional groups:

- Control proxies (typically with the `control` role) — serve "light" requests: browsing the web interface, directory listing, creating/deleting tables, working with metadata.
- Data proxies (typically with the `default` role) — serve "heavy" requests: reading and writing tables or files.

For high availability, it's recommended to run several proxy replicas per role.

You can set roles in the Ytsaurus specification:

```yaml
spec:
  httpProxies:
    # Heavy group (accepts Data traffic by default)
    - role: default
      instanceCount: 5
      serviceType: NodePort
    # Control group (UI and metadata)
    - role: control
      instanceCount: 2
      serviceType: LoadBalancer # Convenient for Ingress
```

{% note info %}

The role name becomes part of the K8s pod name (for example, `hp-control-0`). Therefore, use only DNS-compatible names (lowercase letters, numbers, and hyphens). The underscore (`_`) cannot be used.

{% endnote %}

How it works:

- The operator creates two independent pod groups (StatefulSets) with labels `control` and `default`.
- The operator automatically creates a Kubernetes Service for each group. The `http-proxies-control` service is configured with a selector that targets only pods with the `control` label.
- You configure Ingress/DNS to point to the `control` group's service. Clients will automatically get Data proxy addresses through the [Discovery](../../../admin-guide/cluster-access-proxy/index.md#discovery) mechanism and send heavy requests directly to them.

{% note info %}

If you create a Service or Ingress manually (without the operator), make sure the service's Label Selector has the correct filter for the role label. You can check labels on running pods using the command `kubectl get pods --show-labels`. Without this filter, the load balancer will send light requests to all proxies in turn, including Data proxies.

{% endnote %}

### HTTP access and Ingress specifics {#ingress}

For HTTP proxies, Ingress controllers are often used for SSL termination and routing. If Ingress is used only for Control proxies, and Data proxies are exposed via NodePort (as described above), the scheme works efficiently.

If you need to route **all** traffic through Ingress (including Data), you'll have to solve several problems:
- Double load balancing and reduced efficiency.
- Complex Ingress rules for each Data proxy pod individually (for per-pod routing).
- Configuring [sticky sessions](#sticky-sessions) for correct transaction handling.

### The sticky session problem {#sticky-sessions}

When using Ingress for Data proxies, it's important to consider how transactions work. Transactions in {{product-name}} are created on a specific proxy, and all subsequent requests within a single transaction must go to that same proxy. Requests routed through the Ingress controller may intermittently result in errors like "Transaction not found" or "Invalid transaction state". To avoid this, configure [sticky sessions](*sticky-session) in your Ingress controller.

Configuring sticky sessions depends on the Ingress controller used:

- NGINX Ingress: use the annotation `nginx.ingress.kubernetes.io/affinity: "cookie"`
- Traefik: configure `stickiness` in the service configuration
- HAProxy Ingress: use `haproxy.org/cookie-persistence`

{% cut "Example for NGINX Ingress with sticky sessions" %}

This Ingress configures NGINX to mark the client with a special cookie and always send them to the same proxy pod.

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: yt-data-proxies
  annotations:
    # Enable sticky sessions: NGINX will remember which pod to send the client to
    nginx.ingress.kubernetes.io/affinity: "cookie"
    nginx.ingress.kubernetes.io/session-cookie-name: "yt-proxy-id"
    nginx.ingress.kubernetes.io/session-cookie-max-age: "14400"
spec:
  rules:
  - host: yt-data.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: http-proxies-default
            port:
              number: 80
```

{% endcut %}

### About TLS {#tls}

{% note warning "Important" %}

When exposing Data proxies directly (via NodePort), traffic does not pass through Ingress, meaning it's transmitted **without TLS encryption**.

{% endnote %}

For high-load installations, if there are no strict encryption requirements, the optimal scheme remains split:
- Use Ingress only for Control traffic (for convenient access to the UI, light API requests, and HTTPS).
- Publish Data proxies directly via NodePort or LoadBalancer with the Discovery mechanism configured (traffic will be sent in clear text but with maximum performance).

If security requirements mandate encryption for external clients, there are two options:
- Route all traffic through Ingress (with sticky sessions configured), taking into account the potential performance degradation,
- Or configure load balancers (Network Load Balancer) or internal TLS within the cluster itself.

## Project isolation (for RPC) {#projects}

For RPC proxies, splitting by projects is more common. For example, you can create a dedicated proxy group with the role `project-a` and grant access to it only to a specific user.

```yaml
spec:
  rpcProxies:
    # Pods for general needs
    - role: default
      instanceCount: 2
      serviceType: NodePort
    # Dedicated pods for Project A
    - role: project-a
      instanceCount: 2
      serviceType: NodePort
```

For a client to start working with a dedicated proxy group, you need to explicitly specify the `proxy_role` in the application code (or in the CLI settings). If you don't do this, the client will use the `default` role.

{% cut "Example for Python" %}

```python
# Example of a Python client hard-bound to a group
client = yt.YtClient(proxy="yt.cluster", config={"proxy_role": "project-a"})
```
{% endcut %}

{% cut "Example for CLI" %}

```bash
# Can be passed via environment variable before running scripts
export YT_CONFIG_PATCHES='{proxy_role="project-a"}'
yt list //home
```
{% endcut %}

[*sticky-session]: Sticky sessions — a load balancing mechanism where the Ingress controller binds a client to a specific server (pod) for the entire session duration. On the first request, the load balancer gives the client a special cookie tag, ensuring all subsequent requests are guaranteed to go to the same pod.
