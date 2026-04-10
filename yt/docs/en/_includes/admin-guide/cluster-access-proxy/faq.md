# FAQ

{% cut "Can a Data proxy role have a name other than `default` (for example, `heavy`)?" %}

Yes, but this requires additional configuration.

By default, client libraries (SDKs) send all heavy requests (read/write) to the proxy group with the role `default`. If you create a group with the `heavy` role in the operator but do not reconfigure the cluster, the SDK won't know about it and will continue routing requests to the `default` role, for which no proxies are configured.

To redirect standard client traffic to the new group, change the `@default_role_filter` attribute in Cypress:

```bash
# Now, clients that don't explicitly specify a role will be directed to proxies with the 'heavy' role.
yt set //sys/http_proxies/@default_role_filter heavy
```

{% endcut %}

{% cut "Can all traffic (including Data) be routed through Ingress?" %}

Technically, yes, but it's not recommended for high-load installations. If you configure clients to work without [Discovery](../../../admin-guide/cluster-access-proxy/index.md#discovery) (only through Ingress), three problems arise:

- Double load balancing. All traffic passes through an intermediate Ingress controller, increasing latency.
- Configuration complexity. To make Discovery work through Ingress, you'll need to create a separate Ingress resource and DNS name for each Data proxy pod.
- Transaction issues. You must configure [sticky sessions](../../../admin-guide/cluster-access-proxy/manage-traffic.md#sticky-sessions).

For high-load installations, a combined approach is recommended:
- L7 (Ingress): Use only for Control traffic (role `control`). This provides a clear domain name, SSL, and convenience for users.
- L4 (NodePort/LoadBalancer): Use for Data traffic (role `default`). This allows clients to connect directly to pods (via Discovery and Advertised Addresses), ensuring maximum throughput without a bottleneck from the Ingress controller.

{% endcut %}

{% cut "Why do `list` and `create` commands work, but `write-table` fails with a network error?" %}

This indicates misconfigured (or unconfigured) Discovery.

Lightweight commands go through control proxies. Heavy commands (`read-table`, `write-table`) try to connect directly to Data proxies. A "Name resolution failure" or "Connection refused" error on write means that Discovery is returning the pods' internal addresses to the client.

Configure address substitution via the `//sys/http_proxies/@balancers` attribute (see the section [Configure address substitution](../../../admin-guide/cluster-access-proxy/network-isolation.md#set-discovery-step)).

{% endcut %}

{% cut "Can Discovery be temporarily disabled for debugging?" %}

<!--If you need to force all traffic through a single entry point (for example, via `port-forward` or Ingress), disable Discovery on the client side.-->

Yes, the Discovery mechanism can be disabled on the client side (config option `enable_proxy_discovery=%false`). This is convenient for quick debugging and testing: traffic stops being split and is directed to a single entry point.

Examples of how to disable Discovery:

{% list tabs %}

- Python SDK

  ```python
  client = yt.YtClient(proxy="localhost:8080", config={"proxy": {"enable_proxy_discovery": False}})
  ```
- CLI

  ```bash
  export YT_USE_HOSTS=0
  # Or:
  # export YT_CONFIG_PATCHES='{proxy={enable_proxy_discovery=%false}}'
  ```

{% endlist %}

{% note warning %}

Do not use this mode in production when transferring large amounts of data—it will create a bottleneck at the cluster entry point.

{% endnote %}

{% endcut %}

{% cut "Can a proxy's role be specified via an attribute in Cypress?" %}

Yes, the role of a specific instance can be changed on the fly by modifying the `@role` attribute in Cypress. This is useful for temporarily removing a proxy from the active pool without restarting it.

Example of changing a role via the CLI:

```bash
# Assign the 'control' role to a specific proxy
yt set //sys/http_proxies/hp-0.http-proxies.default.svc.cluster.local:80/@role control
```

{% note alert %}

When the pod restarts, its role will revert to the value specified in the operator's specification.

{% endnote %}

{% endcut %}
