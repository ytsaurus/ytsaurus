# Configure access to SPYT

For SPYT to work in [Standalone](../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone) mode, the external Spark driver must connect directly to the Spark workers. These workers run not as separate Kubernetes pods, but as processes inside [Vanilla jobs](../../../user-guide/data-processing/operations/vanilla.md) in {{product-name}}. Standard Kubernetes network abstractions (Services) can't route traffic to such processes.

To proxy these connections, `tcp_proxy` is used. This is a {{product-name}} component that knows which node and port a specific worker process runs on, and forwards external traffic to it using routing tables stored in Cypress at `//sys/tcp_proxies/routes`.

To set up `tcp_proxy`:

1. Ensure the ports on which `tcp_proxy` runs (32000–32019 by default) are exposed via Kubernetes services (NodePort or LoadBalancer).
2. Specify the external addresses of these ports in the attribute `//sys/tcp_proxies/routes/<proxy_role>/@external_addresses` in Cypress.

This allows the Spark driver to use `tcp_proxy` as an intermediary to communicate with the internal workers.

#### Example

```bash
yt set //sys/tcp_proxies/routes/default/@external_addresses '["node1.example.com:32000"; "node2.example.com:32000"]'
```
