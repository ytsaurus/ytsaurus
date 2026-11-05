# Run a pipeline in a Vanilla operation

This is the simplest way to run Flow: you don’t need a separate long-running deployment of controllers and workers — they start inside a single {{product-name}} [Vanilla operation](../../../user-guide/data-processing/operations/vanilla.md). To enable this type of run, just add the `vanilla` block to `pipeline.yson`.

## What you’ll need {#prerequisites}

You need a configuration file:

* `pipeline.yson` — [runner config](../../concepts/spec.md#runner-config) with the pipeline spec. For the pipeline to run in a Vanilla operation, it must include a `vanilla` block with `enable = %true` (see [How to enable](#enable)). You don’t need a separate `config.yson` — the node config inside the jobs is built automatically.

And binaries — their roles depend on the language:

{% list tabs %}

- C++

  * `pipeline` — your pipeline binary. It also acts as `flow_server` (via `TSimpleRunnerProgram`) and works as a controller, worker, and runner.

- Python

  * `pipeline` — a lightweight Python binary: launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin` or the `YT_FLOW_BIN` environment variable. The companion is delivered to the job automatically. Leave `entrypoint` out of the companion resource parameters: the runner keeps a declared `executable` other than `./py_companion` as is and assumes the companion is already in the job environment.

- Java

  * `run.sh` — the Java launcher script for the jar and companion; it takes the fully qualified main class as its first argument.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin` or the `YT_FLOW_BIN` environment variable. The companion is delivered to the job automatically. Leave `classpath` out of the companion resource parameters: the runner keeps a declared `classpath` as is and assumes the jars are already in the job environment.

- Go

  * `pipeline` — a Go binary: launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin` or the `YT_FLOW_BIN` environment variable. The companion is delivered to the job automatically. Leave `entrypoint` out of the companion resource parameters: the runner keeps a declared `executable` other than `./go_companion` as is and assumes the companion is already in the job environment.

{% endlist %}

## How to enable {#enable}

Add the `vanilla` block to your pipeline config:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 5};
};
```

The required fields are `pool` and `worker.count`. By default, the controller has one job, and each controller and worker job gets `cpu_limit = 6` and `memory_limit = 18 GiB`. Without a network project, the launcher asks {{product-name}} to allocate job ports. With a network project, each job has its own IP and uses fixed ports unless its launcher requests job ports: `rpc_port = 10080`, `monitoring_port = 10081`, and `companion.port = 10082`. See [Additional parameters](#advanced-config) for port allocation details.

{% if audience == "internal" %}

Internal Vanilla jobs use the `yt_flow_common` network project by default. Set `vanilla.network_project` when the pipeline needs a dedicated project or access to other services. Setting it to `#` disables that default; without a network project, use the allocated port settings described in [Additional parameters](#advanced-config) to avoid collisions on a shared host.

{% endif %}

If needed, you can override the resources explicitly:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "controller" = {"count" = 1; "cpu_limit" = 2; "memory_limit" = "8g"};
    "worker" = {"count" = 5; "cpu_limit" = 8; "memory_limit" = "32g"};
};
```

For the full list of fields, see [TVanillaConfig](../../generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) and [TVanillaTaskConfig](../../generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig).

When you run it, `flow_server` validates the spec itself, creates a Vanilla operation with two tasks (controller and worker), sets the pipeline spec, and starts it.

## Additional parameters {#advanced-config}

The less frequently used fields of the `vanilla` block:

#|
|| **Parameter** | **Description** ||
|| `runtime_proxy_role` | The RPC proxy role for `runtime_cluster` (the pipeline cluster’s role may not exist there). Taken into account only when `runtime_cluster` differs from the pipeline cluster; on the pipeline cluster its own `proxy_role` is used ||
|| `cache_path` | The {{product-name}} file cache the job files are uploaded to (shared by all flow operations on the cluster). Non-empty, `//tmp/yt_wrapper/file_storage/new_cache` by default ||
|#

And the task fields (`controller`/`worker`):

#|
|| **Parameter** | **Description** ||
|| `layers` | Cypress paths of the porto layers mounted into the task’s root filesystem. A non-empty list on at least one task enables porto jobs for the whole operation ||
|| `system_layer_path` | The task’s base OS layer; overrides the default system layer ||
|| `set_container_cpu_limit` | Request a container CPU ceiling equal to `cpu_limit` on execution backends that support it. Defaults to `%false`; when disabled, the runner leaves the operation field unset. ||
|| `port_count` | Number of ports requested from {{product-name}} instead of using fixed ports. Without a network project, the default is 2 for the controller and 3 for the worker; `0` keeps fixed ports unless a companion launcher raises `worker.port_count` ||
|#

On a shared-network host, fixed ports of neighboring jobs can collide. The launcher therefore sets `port_count = 2` for the controller and `port_count = 3` for the worker when no network project is configured. An explicit `0` selects fixed ports again unless a companion launcher raises `worker.port_count`. Allocated ports take priority over the node config: `YT_PORT_0` is `rpc_port` and `bus_server.port`, `YT_PORT_1` is `monitoring_port`, and `YT_PORT_2` is `companion.port`. A controller or a worker without a [companion](../../concepts/companion.md) needs two ports; a worker with a companion needs three. With fewer ports, some remain fixed and can still collide.

The Go and Python companion launchers raise `worker.port_count` to at least `3` when Vanilla is enabled, including when the value is omitted or explicitly set to `0`. Their worker ports are allocated even with a network project; the controller follows the rules above.

## Accessing {{product-name}} through job proxy {#job-proxy-readers}

Use job proxy for Flow requests to the cluster running the vanilla operation. This option applies to bulk static table reads, queue reads, loading and saving state, dynamic table writes, and transaction commits.

Requests bypass the shared RPC proxy: a local Unix socket replaces the network hop to that proxy. This reduces load on shared RPC proxies and can reduce latency and increase throughput when those proxies are the bottleneck. Vanilla jobs need spare CPU and memory: RPC request processing and buffers move to each job's job proxy. Changing the route does not by itself remove bottlenecks in storage servers or user computations.

Add [`enable_rpc_proxy_in_job_proxy`](../../../user-guide/data-processing/operations/operations-options.md#user_script_options) to `vanilla.worker`:

```yson
enable_rpc_proxy_in_job_proxy = %true;
```

Data path:

```text
Flow → RPC client → Unix socket → job proxy → native client → cluster servers
```

Job proxy is a separate process on a {{product-name}} execution node. It starts the job's user process, monitors its resources, and reports statistics to the node. In a vanilla Flow operation, the user process is a Flow worker. Job proxy already exists without this option; the flag enables its RPC API and exports the local socket path in `YT_JOB_PROXY_SOCKET_PATH`.

The Flow launcher passes the execution cluster in `YT_FLOW_JOB_PROXY_CLUSTER`. All clients from the shared Flow cache use the local socket for this cluster, including table reads, state access, writes, and transaction commits. The authenticator client also uses job proxy. Clients with identical settings within one cache share a connection while retaining their authentication options. An explicit proxy role such as `my-cluster/my-proxy-role` is also replaced by the local socket for the execution cluster. Requests to other clusters keep using their regular RPC proxies.

This option is incompatible with a custom root clients cache factory (`SetRootClientsCacheFactory`). This combination fails at startup to preserve the custom client semantics.

### Separating RPC traffic into transport bands {#job-proxy-multiplexing}

With many partitions, service RPCs can compete with data streams, such as `ReadTable`, for processing and data transfer through job proxy. [`enable_control_multiplexing_band`](../../generated_docs/all_yson_structs.md#NYT_NApi_NRpcProxy_TConnectionConfig) routes methods that support it, including `CommitTransaction`, `PingTransaction`, `AbortTransaction`, and `DetachTransaction`, to the `control` band. `ReadTable` stays in `default`. Bands use separate transport connections and queues, but share the host's CPU, memory, and bandwidth.

These are Flow client-side settings. The `enable_control_multiplexing_band` flag belongs to an individual RPC connection and selects the band for supported methods; it applies to both job proxy and regular RPC proxy connections. The `tcp_dispatcher.multiplexing_bands` settings control transport connection parallelism across the Flow process. They do not configure the dispatcher in the separate job proxy process.

Separation helps when service requests wait behind data streams in a shared transport queue. Additional connections consume resources and do not eliminate CPU contention or storage-server overload. Choose parallelism based on service RPC latency, throughput, and resource use.

The following config fragment enables job proxy for workers, selects `control` for supported RPCs, and sets parallelism to 4 in `default` and 1 in `control` for each transport channel. These values are an example to tune for your workload, not new defaults.

```yson
{
    vanilla = {
        worker = {enable_rpc_proxy_in_job_proxy = %true;};
        node_config = {
            clients_cache = {
                default_connection = {
                    enable_control_multiplexing_band = %true;
                };
            };
        };
    };
    dynamic_spec = {
        singletons = {
            tcp_dispatcher = {
                multiplexing_bands = {
                    default = {
                        min_multiplexing_parallelism = 4;
                        max_multiplexing_parallelism = 4;
                    };
                    control = {
                        min_multiplexing_parallelism = 1;
                        max_multiplexing_parallelism = 1;
                    };
                };
            };
        };
    };
}
```

See the [transport band reference](../../generated_docs/all_yson_structs.md#NYT_NBus_NTcp_TMultiplexingBandConfig) for `min_multiplexing_parallelism` and `max_multiplexing_parallelism`.

If the cluster has a `clients_cache.per_cluster_connection` entry, set `enable_control_multiplexing_band` in that connection too: it does not inherit `default_connection`.

## Run the pipeline {#run}

{% list tabs %}

- C++

  ```bash
  ./pipeline --config pipeline.yson
  ```

- Python

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server
  ```

- Java

  ```bash
  ./run.sh com.example.pipeline.PipelineMain --config pipeline.yson --flow-bin flow_server
  ```

- Go

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server
  ```

{% endlist %}

After the start, the runner by default (`YT_FLOW_WAIT=1`) waits until the pipeline reaches the `completed` state, printing new records of the controller’s public log all the while — those that appeared since the wait began; earlier ones are not shown. You can interrupt the runner; that does not affect the running operation. With `YT_FLOW_WAIT=0`, the runner exits right after the launch.

### Validate the spec only {#validate-only}

The C++ and Java runners support `--validate-only`: they parse and validate the static and dynamic specs locally without sending them to the controller. An invalid spec makes the runner fail; a valid one exits successfully. The Python and Go launchers do not forward this flag to `flow_server` and may start or update the pipeline instead. The command below uses the C++ runner.

```bash
./pipeline --config pipeline.yson --validate-only
```

## See also

- [Basic rollout rules](releases.md#release-and-configure-basic-rules)
- [Basic pipeline operations](pipeline-operations.md)
- [Updates and releases](releases.md)
- [Security and access](security.md)
- [Logs](diagnostics/logs.md)
- [Spec and DynamicSpec](../../concepts/spec.md)
