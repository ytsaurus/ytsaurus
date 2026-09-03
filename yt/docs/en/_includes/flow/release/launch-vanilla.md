# Run a pipeline in a Vanilla operation

This is the simplest way to run Flow: you don’t need a separate long-running deployment of controllers and workers — they start inside a single {{product-name}} [Vanilla operation](../../../user-guide/data-processing/operations/vanilla.md). To enable this type of run, just add the `vanilla` block to `pipeline.yson`.

## What you’ll need {#prerequisites}

You need a configuration file:

* `pipeline.yson` — [runner config](../../../flow/concepts/spec.md#config) with the pipeline spec. For the pipeline to run in a Vanilla operation, it must include a `vanilla` block with `enable = %true` (see [How to enable](#enable)). You don’t need a separate `config.yson` — the node config inside the jobs is built automatically.

And binaries — their roles depend on the language:

{% list tabs %}

- C++

  * `pipeline` — your pipeline binary. It also acts as `flow_server` (via `TSimpleRunnerProgram`) and works as a controller, worker, and runner.

- Python

  * `pipeline` — a lightweight Python binary: launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin`. The companion is delivered to the job automatically.

- Java

  * `pipeline` — a jar launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin`. The companion is delivered to the job automatically.

- Go

  * `pipeline` — a Go binary: launcher plus companion.
  * `flow_server` — the Flow server binary (`yt/yt/flow/bin/flow_server`) that works as a controller and worker; its path is passed to the runner via `--flow-bin`. The companion is delivered to the job automatically.

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

The required parameters are `pool` and `worker.count`. The remaining fields have sensible defaults: one controller job, and `cpu_limit = 6` and `memory_limit = 18 GiB` for every job of both the controller and the worker. The ports inside a job are fixed — `rpc_port = 10080` and `monitoring_port = 10081`: network isolation between jobs rules out collisions, and you can override the ports through a `node_config` patch if you have to. A host with a shared network is the exception, because it has no such isolation — see [Additional parameters](#advanced-config).

{% if audience == "internal" %}

In internal builds, Vanilla jobs run in the `yt_flow_common` network project by default. For an ordinary launch you don’t need to create a network project or set `network_project` yourself. If the pipeline needs its own network isolation or access to additional services, create a separate network project and name it in the config:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 5};
    "network_project" = "my_flow_project";
};
```

Setting `network_project = #` disables the internal default. Without a network project, the fixed ports can collide on a host with a shared network, so in that mode set [`port_count`](#advanced-config) for each task.

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

For the full list of fields, see [TVanillaConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) and [TVanillaTaskConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig).

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
|| `port_count` | How many ports the task requests from {{product-name}} instead of using the fixed ones. Needed on a host with a shared network ||
|#

On a host with a shared network, the fixed ports of neighboring jobs would collide, so there the ports have to be requested from {{product-name}} through the task’s `port_count` field. The granted ports arrive in the `YT_PORT_<i>` environment variables and take priority over the config: `YT_PORT_0` is `rpc_port` (and `bus_server.port`), `YT_PORT_1` is `monitoring_port`, and `YT_PORT_2` is `companion.port` (present only for Python, Java, and Go workers, which all run a companion process). The controller and the C++ worker therefore need `port_count = 2`, and Python, Java, and Go workers need `port_count = 3`; with a smaller value some of the ports stay fixed and can collide again. The Go runner raises `worker.port_count` to at least 3 automatically if the pipeline config sets a smaller value, so a Go pipeline gets a correct port count even without an explicit setting.

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
  ./pipeline RunnerMain --config pipeline.yson --flow-bin flow_server
  ```

- Go

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server
  ```

{% endlist %}

After the start, the runner by default (`YT_FLOW_WAIT=1`) waits until the pipeline reaches the `Completed` state, printing new records of the controller’s public log all the while — those that appeared since the wait began; earlier ones are not shown. You can interrupt the runner; that does not affect the running operation. With `YT_FLOW_WAIT=0`, the runner exits right after the launch.

### Validate the spec only {#validate-only}

This is supported only for the C++ `pipeline` binary. To locally check the spec’s correctness without sending it to the controller, pass the `--validate-only` flag to the runner. The runner will perform all checks (parsing and validating the static and dynamic spec) and then exit without side effects. If the spec is invalid, the runner exits with an error; otherwise, it exits successfully. The Python, Java, and Go launchers don’t accept `--validate-only`, and forwarding it to `flow_server` via `--flow-bin` doesn’t help either — the launcher passes through only `--config`, so `flow_server` starts the pipeline normally.

```bash
./pipeline --config pipeline.yson --validate-only
```

## See also

- [Basic rollout rules](../../../flow/release/basic-rules.md)
- [Basic pipeline operations](../../../flow/release/pipeline-operations.md)
- [Updates and releases](../../../flow/release/releases.md)
- [Security and access](../../../flow/release/security.md)
- [Logs](../../../flow/release/logs.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)