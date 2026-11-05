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

- Kotlin

  * `run.sh` — the JVM launcher generated for the Kotlin program; it takes the fully qualified main class as its first argument. For the Word Count module, deploy it together with `wordcount_kotlin_bin.jar` and the dependency directory `wordcount_kotlin_bin/`, preserving their relative layout. The module targets JDK 21 and also produces `jdk.tar`: `WITH_JDK()` packs this archive but does not automatically unpack or deploy a `jdk/` directory. On the launch host, provide Java 21 through `JAVA_HOME` or a leading `run.sh -j <path/to/java>` option; `-j` takes priority. Run the script from the directory containing this artifact layout because its paths are relative to the current directory. The separate `flow-java` docker example uses Java 17, so its image is not a guaranteed runtime for this JDK 21 module. Kotlin uses the same Java SDK; see [Getting started (Kotlin)](../../../flow/kotlin/getting-started.md).
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin` or the `YT_FLOW_BIN` environment variable. The companion is delivered to the job automatically. Leave `classpath` out of companion resource parameters when the runner should ship the jars. A declared `classpath` is preserved; if every Java companion resource declares one, the runner ships no companion jars, while a mixed spec still ships jars for resources without `classpath`.

- Go

  * `pipeline` — a Go binary: launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin` or the `YT_FLOW_BIN` environment variable. The companion is delivered to the job automatically. Leave `entrypoint` out of the companion resource parameters: the runner keeps a declared `executable` other than `./go_companion` as is and assumes the companion is already in the job environment.

{% endlist %}

The JVM runner applies JDK and classpath enrichment only for this enabled Vanilla mode. In docker jobs, the JDK path precedence is resource `jdk_bin_path`, then `YT_FLOW_JDK_BIN_PATH`, then the launcher JVM path; the selected path must exist in the job image and provide a compatible runtime. In porto jobs that use the supplied JDK layer, `YT_FLOW_JDK_BIN_PATH` overrides the layer path and a different resource path is ignored. A worker with explicit layers does not use the supplied JDK layer, so it needs a compatible resource or environment JDK path. Direct custom deployments must provide their own JDK, jars, and companion parameters.

## How to enable {#enable}

Add the `vanilla` block to your pipeline config:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 5};
};
```

The required fields are `pool` and `worker.count`. By default, the controller has one job, and each controller and worker job gets `cpu_limit = 6` and `memory_limit = 18 GiB`. Without a network project, the launcher asks {{product-name}} to allocate job ports. With a network project, each job has its own IP and uses fixed ports unless its launcher requests job ports: `rpc_port = 10080`, `monitoring_port = 10081`, `companion.port = 10082`, and `companion.monitoring_port = 10083`. See [Additional parameters](#advanced-config) for port allocation details.

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
|| `port_count` | Number of ports requested from {{product-name}} instead of using fixed ports. Without a network project, native C++ and JVM launches default to 2 for the controller and 4 for the worker; Python and Go launchers set an omitted worker value to 3. `0` keeps fixed ports unless a companion launcher raises `worker.port_count` ||
|#

On a shared-network host, fixed ports of neighboring jobs can collide. Without a network project, native C++ and JVM launches therefore default `port_count` to `2` for the controller and `4` for the worker; an explicit `0` selects fixed ports again unless a companion launcher raises `worker.port_count`. Allocated ports take priority over the node config: `YT_PORT_0` is `rpc_port` and `bus_server.port`, `YT_PORT_1` is `monitoring_port`, `YT_PORT_2` is `companion.port`, and `YT_PORT_3` is `companion.monitoring_port` (optional). A worker without a companion needs two ports; a worker with a companion needs at least three. If a task requests fewer ports than needed, some endpoints remain fixed or the companion cannot start; without the fourth port, only companion metrics are disabled.

The Python companion launcher sets `worker.port_count` to `3` when the field is omitted, set to the YSON entity `#`, or has a nonzero integer value below `3`, but preserves an explicit `0` so that the worker uses fixed ports. The Go launcher sets an omitted or integer value below `3` to `3`, including an explicit `0`. Their worker ports are allocated even with a network project unless Python receives the explicit zero; the controller follows the rules above. Java and Kotlin use the JVM runner and do not change `worker.port_count` automatically.

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

`com.example.pipeline.PipelineMain` in the Java commands is a placeholder. The Kotlin commands use the Word Count example’s entry-point class. In both the run and validation sections, replace the applicable name with the fully qualified class name of your pipeline. Run JVM commands from the directory containing `run.sh`, the program jar, and its dependency directory.

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

- Kotlin

  ```bash
  ./run.sh tech.ytsaurus.flow.examples.wordcount.WordCountApplication --config pipeline.yson --flow-bin flow_server
  ```

- Go

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server
  ```

{% endlist %}

After the start, the runner by default (`YT_FLOW_WAIT=1`) waits until the pipeline reaches the `completed` state, printing new records of the controller’s public log all the while — those that appeared since the wait began; earlier ones are not shown. You can interrupt the runner; that does not affect the running operation. With `YT_FLOW_WAIT=0`, the runner exits right after the launch.

### Validate the spec only {#validate-only}

To check the spec locally without sending it to the controller, pass `--validate-only` to the C++ or JVM runner. The runner parses and validates the static and dynamic spec, then exits without side effects. If the spec is invalid, it exits with an error; otherwise, it exits successfully. The JVM runner forwards the flag to `flow_server`, so this works for both Java and Kotlin. The Python and Go launchers do not forward the flag to `flow_server`; invoking them with it may start or update the pipeline instead.

For a C++ pipeline:

```bash
./pipeline --config pipeline.yson --validate-only
```

For a Java or Kotlin pipeline, provide the real entry-point class and `flow_server` path. The Java command keeps the placeholder used above, while the Kotlin command keeps the Word Count example class:

{% list tabs %}

- Java

  ```bash
  ./run.sh com.example.pipeline.PipelineMain --config pipeline.yson --flow-bin flow_server --validate-only
  ```

- Kotlin

  ```bash
  ./run.sh tech.ytsaurus.flow.examples.wordcount.WordCountApplication --config pipeline.yson --flow-bin flow_server --validate-only
  ```

{% endlist %}

## See also

- [Basic rollout rules](releases.md#release-and-configure-basic-rules)
- [Basic pipeline operations](pipeline-operations.md)
- [Updates and releases](releases.md)
- [Security and access](security.md)
- [Logs](diagnostics/logs.md)
- [Spec and DynamicSpec](../../concepts/spec.md)
