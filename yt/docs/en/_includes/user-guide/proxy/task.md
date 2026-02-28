# Task proxy

{{product-name}} operations often require deploying web services. These can be debugging UIs (such as Spark UI in [SPYT](../../../user-guide/data-processing/spyt/overview.md)), ML model inference servers, or APIs inside jobs.

Operation jobs run on cluster exec nodes, so services bind to network ports on these nodes &mdash; to receive incoming traffic. However, when attempting to directly access services inside a job, difficulties arise:

- Network isolation: the user may not have direct network access to exec node IP addresses (they may be in a closed perimeter).
- Dynamic addressing: even with network access, jobs can move between nodes, so the host and port of services constantly change.
- Security: direct access to a port on a node bypasses {{product-name}} authentication mechanisms. Access control to the operation is not enforced.

_Task proxy_ solves these problems by providing a single entry point. It allocates stable domains (FQDN) for each service and verifies user access rights before redirecting the request inside the job.

## How it works { #how }

Task proxy is based on the _service discovery_ mechanism. It automatically finds operations with web services in the cluster and [generates](#access) domain names for them.

Each service is described by entities:

- _Operation_: The main unit of work in the {{product-name}} cluster.
- _Task_: Type of work within an operation. An operation can contain multiple tasks (for example, driver and executor).
- _Job_: A specific process. Each task runs in one or more instances — jobs.
- _Service_: A specific network service of a task. One task can expose multiple web services (for example, HTTP UI and REST API).

Task proxy balances incoming traffic between all running jobs of one task, directing the request to a specific service of that job. If one of the jobs fails or moves to another node, the proxy automatically redirects traffic to available instances.

![](../../../../images/task-proxy.png)

Automatic discovery (service discovery) works for:

- Vanilla operations with configured annotations.
- SPYT operations (Standalone cluster and Direct Submit) — they are discovered automatically without additional configuration.

## Launching HTTP service {#quickstart}

To launch an operation with a web service accessible through Task proxy:

1. Specify the `port_count` parameter in the task specification. {{product-name}} will allocate free ports on the exec node and pass their numbers to environment variables `YT_PORT_0`, `YT_PORT_1`, etc.
2. Add `task_proxy={enabled=%true}` to the operation specification.

Example command to launch two instances (`job_count=2`) of an HTTP server on one port (`port_count=1`):

```sh
yt vanilla \
    --tasks '{example_http_server={job_count=2; command="python3 -m http.server ${YT_PORT_0}"; port_count=1}}' \
    --spec '{annotations={task_proxy={enabled=%true;}}}'
```

With this configuration, Task proxy will automatically discover the service, assign it the `HTTP` protocol and the name `port_0`.

## How to find out the service address {#access}

After launching the operation, Task proxy will generate a unique domain for each service. The domain name consists of `baseDomain` (specified by the cluster administrator), to which a unique hash from the service descriptor is added.

The service discovery mechanism scans operations and writes information about available entry points to the `//sys/task_proxies/services` table (the path can be changed by the administrator in the `dirPath` parameter).

Below is an example of the table contents. Each row describes its own service:

| __domain__                               | __operation_id__                  |  __task_name__      | __service__ | __protocol__ |
|------------------------------------------|-----------------------------------|---------------------|-------------|--------------|
| 645236d8.my-cluster.ytsaurus.example.net | a6e04b98-bf982394-5103e8-55754a49 | example_http_server | port_0      | http         |
| ae5cf6f5.my-cluster.ytsaurus.example.net | a8ef7695-3de07913-5103e8-e29a6707 | example_grpc_server | server      | grpc         |
| 2ef4261c.my-cluster.ytsaurus.example.net | a6e04b98-bf982394-5103e8-55754a49 | master              | ui          | http         |
| 51a6d485.my-cluster.ytsaurus.example.net | a6e04b98-bf982394-5103e8-55754a49 | history             | ui          | http         |
| 37a5f11c.my-cluster.ytsaurus.example.net | 6699a5a9-37c731e3-5103e8-b05d7dd0 | driver              | ui          | http         |

- The first row represents a [vanilla](../../../user-guide/data-processing/operations/vanilla.md) operation with an HTTP server example.
- The second is a [gRPC server example](https://github.com/ytsaurus/ytsaurus-task-proxy/tree/main/examples/grpc-service). On how to launch it using the extended Task proxy annotation format, read [further](#exp).
- The third and fourth rows correspond to a SPYT [standalone](../../../user-guide/data-processing/spyt/cluster/cluster-start.md) cluster launched with history server.
- The fifth row relates to SPYT [direct submit](../../../user-guide/data-processing/spyt/direct-submit/desc.md).

## Methods of connecting to services {#connection-methods}

To access a service, authentication is required. Supported:

- **Tokens:** OAuth token or IAM token (in the `Authorization` header).
- **Cookie:** Authentication cookie (cookie name is set by the administrator).

### Through browser {#browser}

To access the UI (for example, Spark UI), simply open the service domain (`https://645236d8...`) in a browser. If you are authorized in the {{product-name}} interface, access will be granted automatically via cookie.

### Through Ingress (public access) {#ingress}

If an ingress controller is configured in the cluster, you can access the domain directly via the internet. In this case, DNS and TLS settings are not performed within Task proxy, but are carried out separately by your installation administrators.

```sh
curl \
  -H "Authorization: Bearer ${IAM_TOKEN}" \
  "https://645236d8.my-cluster.ytsaurus.example.net"
```

### Directly to Task proxy (inside K8s) {#k8s}

If you are inside the network perimeter (for example, in Kubernetes where Task proxy is deployed), you can access the K8s service directly. In this case, you **must** specify the service domain in the `Host` header.

```sh
curl \
  -H "Host: 645236d8.my-cluster.ytsaurus.example.net" \
  -H "Authorization: OAuth ${YT_TOKEN}" \
  "http://task-proxy.${NAMESPACE}.svc.cluster.local:80"
```

### Alternative to wildcard domains {#wildcard-domains}

If the infrastructure does not support wildcard domains (like `*.ytsaurus.example.net`), use the `x-yt-taskproxy-id` header. Pass the left part of the domain (hash) as the value.

```sh
curl \
  -H "Authorization: Bearer ${IAM_TOKEN}" \
  -H "x-yt-taskproxy-id: 645236d8" \
  "https://task-proxy.my-cluster.ytsaurus.example.net"
```

## Extended annotation format {#exp}

The `task_proxy={enabled=%true}` annotation applies default settings. This means that when using it, each service receives:

- name `port_${PORT_INDEX}`;
- port with an index corresponding to the order of task appearance in the operation specification;
- HTTP protocol.

If you want to set other settings — for example, use the gRPC protocol, set a custom service name, or select a specific port from the list — use the extended annotation format. To do this, you need to specify the `tasks_info` field in it. Examples can be found below.

### Launching gRPC service {#grpc}

Let's launch a [gRPC server example](https://github.com/ytsaurus/ytsaurus-task-proxy/tree/main/examples/grpc-service) using the extended annotation:

```sh
yt vanilla \
    --tasks '{example_grpc_server={job_count=2; command="./yt-sample-grpc-service"; file_paths = ["//home/yt-sample-grpc-service"]; port_count=1}}' \
    --spec '{annotations={task_proxy={enabled=%true; tasks_info={example_grpc_server={server={protocol="grpc"; port_index=0}}}}}}'
```

Breakdown of the `tasks_info` parameter:

- `example_grpc_server` — task name.
    - `server` — service name (in this example there is one, but one task can contain multiple services).
        - `protocol="grpc"` — protocol specification.
        - `port_index=0` — use the first port from `port_count` (corresponds to `YT_PORT_0`).


### Accessing gRPC service {#grpcurl}

gRPC requests also require authorization.

#### Through Ingress (public access) {#grpc-ingress}

Note that the port for gRPC on the ingress controller may differ (in the example below — `9090`).

```sh
grpcurl \
  -H "Cookie: YTCypressCookie ${AUTH_COOKIE_VALUE}" \
  "ae5cf6f5.my-cluster.ytsaurus.example.net:9090" \
  "helloworld.Greeter/SayHello"
```

#### Directly to Task proxy (inside K8s) {#grpc-task-proxy}

```sh
grpcurl \
  -plaintext \
  -authority "ae5cf6f5.my-cluster.ytsaurus.example.net" \
  -H "Authorization: Bearer ${IAM_TOKEN}" \
  "task-proxy.${NAMESPACE}.svc.cluster.local:80" \
  "helloworld.Greeter/SayHello"
