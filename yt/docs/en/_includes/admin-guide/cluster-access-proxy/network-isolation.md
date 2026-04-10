# How to resolve network isolation issues

If you've configured Ingress but encounter errors when trying to write data, despite `list` or `create` commands working, you're facing an isolation issue.

You can resolve this problem in several ways:

- [Use flat network routing](#use-flat-network)
- [Use host network](#use-host-network)
- [Configure Kubernetes Services and address substitution](#k8s-services)

## Use flat network routing {#use-flat-network}

If your network infrastructure allows direct access to pods (for example, you're using Calico CNI with BGP routing or AWS VPC CNI), no additional {{product-name}} configuration is required. The [Discovery](../../../admin-guide/cluster-access-proxy/index.md#discovery) mechanism will return the pods' internal FQDNs (for example, `hp-0.http-proxies.default.svc.cluster.local`).

However, the external client must be able to:
- Resolve the pod DNS names. Internal FQDNs like `*.svc.cluster.local` are only known to the DNS server inside the Kubernetes cluster by default—an external client can't resolve them via regular public DNS servers.
- Establish a network connection. Even if the name resolves to an IP address, the client must have network connectivity to that IP (routing).

### Configuration for AWS

In AWS, if pods already have network connectivity in dual-stack mode, the simplest approach is to configure CoreDNS for pod name resolution. To do this:

1. Make CoreDNS in the cluster accessible to external clients.
2. Configure CoreDNS to respond to queries like `*.cluster.domain.name`, where `cluster.domain.name` is your cluster's domain name.

{% cut "CoreDNS ConfigMap configuration example" %}

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns
  namespace: kube-system
data:
  Corefile: |
    .:53 {
        errors
        health {
           lameduck 5s
        }
        ready
        kubernetes cluster.local in-addr.arpa ip6.arpa {
           pods insecure
           fallthrough in-addr.arpa ip6.arpa
           ttl 30
        }
        # ATTENTION: Replace 'cluster.domain.name' with your cluster's actual domain.
        # This template allows external clients to find pod IP addresses by their names.
        template IN A cluster.domain.name {
            match "^([^.]+)\.http-proxies\.default\.svc\.cluster\.domain\.name\.$"
            answer "{{ .Name }} 60 IN A {{ .Group 1 | replace \"-\" \".\" }}"
        }
        prometheus :9153
        forward . /etc/resolv.conf {
           max_concurrent 1000
        }
        cache 30
        loop
        reload
        loadbalance
    }
```

After this, external clients will be able to resolve pod FQDNs to their real IP addresses and establish direct connections.

{% endcut %}


## Use host network {#use-host-network}

In this mode, proxy pods don't receive a dedicated pod IP from the Kubernetes cluster's internal network. Instead, they use the physical server's (node's) network interface on which they are running.

To enable the mode, add the `hostNetwork: true` field at the root level of the [operator](install-ytsaurus#operator) specification:

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: my-cluster
spec:
  # Enable host network usage for all components.
  hostNetwork: true

  httpProxies:
    - serviceType: ClusterIP # With hostNetwork, an external K8s service is not required.
      instanceCount: 1
  # ... other settings ...
```

In this case, proxies will register in Cypress under the names of the Kubernetes cluster nodes. They will provide these K8s node names to clients during Discovery. External clients must then be able to resolve the DNS names of these nodes.

{% note warning %}

In `hostNetwork` mode, proxies will occupy ports (80 and 443 by default) directly on the cluster nodes. Make sure no other conflicting web services are running on these nodes, or change the ports in the configuration.

{% endnote %}

## Configure K8S Services and address substitution {#k8s-services}

This method is the most widely applicable for cloud environments. Proxies are published via services (NodePort or LoadBalancer), and {{product-name}} is configured so that the [Discovery](../../../admin-guide/cluster-access-proxy/index.md#discovery) mechanism returns these external addresses to clients, not the pods' internal names (FQDNs).

This approach requires two steps:

1. [Open ports](#external-access-step) via K8s services (NodePort or LoadBalancer).
1. [Configure address substitution](#set-discovery-step) (Advertised Addresses).

### Step 1: Open ports {#external-access-step}

Specify the service type for the required proxy groups in the operator specification. For example:

```yaml
spec:
  # HTTP proxies
  httpProxies:
    - role: control
      serviceType: LoadBalancer   # Entry point for lightweight requests
      instanceCount: 1
    - role: default
      serviceType: NodePort       # Entry points for heavy requests (Data)
      instanceCount: 3

  # RPC proxies
  rpcProxies:
    - role: project-a
      serviceType: LoadBalancer
      instanceCount: 2
```

[Full specification example](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_local.yaml)

### Step 2: Configure address substitution (Discovery) {#set-discovery-step}

Even if the services are created, the `discover_proxies` command will still return the pods' internal FQDNs. You need to configure {{product-name}} to return the external addresses of the created services.

To do this, set configuration for the `//sys/http_proxies/@balancers` and `//sys/rpc_proxies/@balancers` attributes:

```yson
{
    "<proxy_role>" = {
        "<address_type>" = {
            "<network_name>" = ["<external_addr_1>"; "<external_addr_2>"]
        }
    }
}
```

- `proxy_role` — proxy role (for example, `default` or `project-a`).
- `address_type` — protocol type. Use `http` for HTTP proxies and `internal_rpc` for RPC proxies.
- `network_name` — network name. {{product-name}} server components support multiple interfaces (historically for separating real-time and bulk traffic). In Kubernetes, a pod usually has one network interface, so the standard value is `default`.

{% note warning %}

The `internal_rpc` value for the `address_type` parameter is historical. It refers to internal code implementation and doesn't mean that addresses must be internal. Specify **external** addresses (or FQDNs) accessible to clients in this block.

Note that the configuration format is YSON, so list items are separated not by a comma, but by a semicolon (`;`).

{% endnote %}

{% note info %}

You must configure addresses for each role separately.

{% endnote %}

### Configuration example

Suppose the cluster has two configured proxy roles:

- Control proxies: accessible via a common LoadBalancer (or Ingress) at `yt.example.com`.
- Data proxies: three instances with direct access via NodePort. When using NodePort, the port is the same for all K8s nodes, so addresses will look like: `node1.example.com:30001`, `node2.example.com:30001`, and `node3.example.com:30001`.

To make Discovery return these addresses correctly for each role, run the commands:

```bash
# 1. Configure addresses for Data proxies (role default)
# These addresses will be used for heavy operations (read/write).
$ yt set //sys/http_proxies/@balancers/default \
  '{"http"={"default"=[
      "node1.example.com:30001";
      "node2.example.com:30001";
      "node3.example.com:30001"
  ]}}'

# 2. Configure address for control proxies (role control)
# This address will be returned if the client explicitly requests discovery for the control group.
$ yt set //sys/http_proxies/@balancers/control \
  '{"http"={"default"=[
      "yt.example.com"
  ]}}'
```

{% note tip %}

In the example, the `set` command is applied to specific paths (`@balancers/default` and `@balancers/control`), not to the root `@balancers` attribute. This approach is safer: each role is configured independently, without the risk of accidentally overwriting other roles' configuration.

{% endnote %}