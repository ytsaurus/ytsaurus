# Running in a docker environment

A pipeline from the released Flow images runs in one of two ways:

- [Vanilla operation](#vanilla): the runner starts the controller and the workers as tasks of a vanilla operation whose jobs run in docker images.
- [Controller and workers in Kubernetes](#k8s): the controller and the workers run as long-lived containers outside {{product-name}} jobs — in Kubernetes or, as in the docker compose example, on one host; the runner only submits the spec to them.

Contents:

- [Released images](#images)
- [Vanilla operation](#vanilla)
  - [Job image](#job-environment)
  - [Launching from an image](#launch)
  - [Language specifics](#languages)
  - [Stopping](#stop)
  - [Cluster name resolution](#cluster-name)
- [Controller and workers in Kubernetes](#k8s)
  - [Node configs](#k8s-nodes)
  - [How the components connect](#k8s-network)
  - [Direct controller commands](#direct-controller-commands)
- [Reaching the cluster from outside](#external-access)
- [Building flow_server](#flow-server)
- [Troubleshooting](#troubleshooting)

## Released images {#images}

Every Flow release publishes three images at the release version.

#|
|| **Image** | **Contents** | **Entrypoint** ||
|| `ghcr.io/ytsaurus/flow:<version>` | `flow_server` | `/usr/bin/flow_server` ||
|| `ghcr.io/ytsaurus/flow-java:<version>` | `flow_server` and a Java 17 runtime | `java` ||
|| `ghcr.io/ytsaurus/flow-python:<version>` | `flow_server` and the Python SDK | `python3` ||
|#

Each image starts in `/app/pipeline` and sets `YT_FLOW_BIN=/usr/bin/flow_server`, so the Java, Python, and Go launchers run inside it find `flow_server` by themselves. One image serves both sides: the runner runs in it on your host, and the controller and the worker run in it in vanilla jobs or in Kubernetes.

## Vanilla operation {#vanilla}

The runner starts the controller and the workers as tasks of one vanilla operation and uploads everything the pipeline needs into it.

### Job image {#job-environment}

The environment of a vanilla task is set by the `docker_image` field of the task block. Set it on both tasks to the [released image](#images) the pipeline is launched from:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "controller" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-java:<version>";};
    "worker" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-java:<version>";};
};
```

`pool` is the scheduler pool the operation runs in; see [Scheduler and pools](../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

The Java and Python companions run on the runtime of the `flow-java` and `flow-python` images, so their jobs need the image. Static binaries — `flow_server`, C++ and Go pipelines — also run in the default job environment without one.

### Launching from an image {#launch}

The runner runs on your host in a container of the released image. Run it from the pipeline directory — the one with `pipeline.yson` and everything the pipeline ships — and mount that directory at the image's working directory `/app/pipeline`:

```bash
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" <image> <program arguments> --config pipeline.yson
```

* The container sees only the environment variables you pass: `-e YT_TOKEN` for the token, plus `-e NAME` for every variable the pipeline forwards to its jobs.
* The mounted directory is the only host path the container sees. Keep the jars, binaries, and `local_files` the pipeline ships inside it and refer to them by paths relative to it.
* The command streams the controller log. A pipeline with a finite source returns once the pipeline is `completed`; otherwise Ctrl-C only stops the log, and the pipeline keeps running until you [stop it](#stop).

The program arguments per language are in the next section.

### Language specifics {#languages}

{% list tabs %}

- C++

  A pipeline assembled from the stock classes needs no code of its own: the pipeline binary is the stock `flow_server`, which is the entrypoint of the `flow` image. Set `ghcr.io/ytsaurus/flow:<version>` in `docker_image` of both tasks and launch:

  ```bash
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow:<version> --config pipeline.yson
  ```

  A pipeline with its own C++ computations is one static binary (the runner, the controller, and the worker at once), built with `./ya make` from a {{product-name}} checkout. It needs no image and no extra config fields:

  ```bash
  ./pipeline --config pipeline.yson
  ```

- Java

  The worker spawns the companion inside the job, so the job needs a JRE, which the `flow-java` image delivers. Set `ghcr.io/ytsaurus/flow-java:<version>` in `docker_image` of both tasks (see [Job image](#job-environment)) and the entry-point class in the companion resource parameters:

  ```yson
  "resources" = {
      "CompanionManager" = {
          "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
          "parameters" = {
              "main_class" = "com.example.pipeline.PipelineMain";
          };
      };
  };
  ```

  The launcher ships the jars on its classpath into the worker job. So put the pipeline jar and its runtime dependencies on the launcher's classpath as separate jar files — the launcher does not ship class directories — and keep them inside the pipeline directory. For example, collect them into `lib/` with a Gradle task:

  ```kotlin
  tasks.register<Sync>("installLib") {
      dependsOn(tasks.jar)
      from(tasks.jar)
      from(configurations.runtimeClasspath)
      into(layout.projectDirectory.dir("lib"))
  }
  ```

  Build it in the official Gradle container, then launch the main class on the `lib/*` classpath in the `flow-java` image:

  ```bash
  podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 gradle -q installLib
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-java:<version> \
      -cp 'lib/*' com.example.pipeline.PipelineMain --config pipeline.yson
  ```

  The launcher takes the worker's `java` from the JVM it runs on — the image's own, the same as in the jobs. Neither `jdk_bin_path` in the resource parameters nor `YT_FLOW_JDK_BIN_PATH` is needed; either one, if set, overrides that path. If the jars are already in the job image, set `classpath` in the resource parameters, e.g. `/app/pipeline/lib/*`, and the launcher ships no jars for that resource.

- Python

  The worker spawns the companion inside the job, so the job needs the interpreter and the Flow SDK, which the `flow-python` image delivers. Set `ghcr.io/ytsaurus/flow-python:<version>` in `docker_image` of both tasks and launch the pipeline script, which ends in `app.run()`:

  ```bash
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-python:<version> \
      main.py --config pipeline.yson
  ```

  The script is both the launcher and the companion: the launcher ships it into the worker's `local_files` as `py_companion` and sets each generic `TCompanionManager` entrypoint to `./py_companion`. The worker executes that file itself; the image entrypoint plays no part here. So keep the `#!/usr/bin/python3` line at the top of the script and make the file executable.

  The launcher ships only this one script, so the pipeline's Python code must fit in it. For code in several modules or with extra dependencies, build your own image on top of `flow-python` that contains them, set it in `docker_image`, and declare the companion in the resource parameters.

  A Python pipeline built from source with `ya make` is a self-contained binary with its own interpreter and SDK; it needs no image and is launched as described in [Build the Python pipeline](../../../flow/python/getting-started.md#build).

- Go

  A Go pipeline is a static binary that combines the launcher and the companion; the launcher ships the binary itself into the worker job. Build it with `CGO_ENABLED=0`, e.g. in the official Go container:

  ```bash
  podman run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 docker.io/library/golang:1.24 \
      go build -o pipeline .
  ```

  Set `ghcr.io/ytsaurus/flow:<version>` in `docker_image` of both tasks and launch the binary in that image, overriding its entrypoint:

  ```bash
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" --entrypoint ./pipeline \
      ghcr.io/ytsaurus/flow:<version> --config pipeline.yson
  ```

  To run a companion binary of the image instead, declare it in the resource parameters, e.g. `"entrypoint" = {"executable" = "/app/pipeline/companion"}`: the launcher keeps a declared `executable` other than `./go_companion` and ships no binary when every companion resource declares one.

- YQL

  A YQL query is compiled into a Flow pipeline and launched as one vanilla operation — the pipeline's Cypress objects are created automatically. You need the `ytrun` client and the `ytflow_worker` from the {{product-name}} repository:

  ```bash
  ./ya make --build=release yt/yql/tools/ytrun yt/yql/tools/ytflow_worker
  ```

  Query syntax and the control pragmas are described in [YQL / Getting started](../../../flow/yql/getting-started.md).

{% endlist %}

### Stopping {#stop}

Stop the pipeline, then abort its vanilla operation. Take the operation id from the runner's log line printed at launch, `Started vanilla operation (..., OperationId: <operation-id>)`:

```bash
yt --proxy <cluster> flow stop-pipeline //path/to/pipeline
yt --proxy <cluster> abort-op <operation-id>
```

A pipeline that is already `completed` needs only the `abort-op`. `abort-op` takes the operation id, not the alias from the pipeline's `@current_vanilla_operation`: given the alias, it fails with `Operation alias cannot be resolved without using runtime information`. To remove the pipeline completely, see [Basic pipeline operations](../../../flow/devops/vanilla/pipeline-operations.md#remove).

### Cluster name resolution {#cluster-name}

This setting is needed only on some clusters — for example, in a typical opensource {{product-name}} installation in Kubernetes.

Rich paths in the spec (`<cluster=my-cluster>//path/to/queue`) and `cluster_url` are resolved by the controller and the workers **from inside** the jobs. If the cluster name does not resolve through the default DNS, declare the mapping in the `vanilla` block:

```yson
"vanilla" = {
    ...
    "proxy_url_aliasing_rules" = {"my-cluster" = "http://<http-proxy-address-inside-the-cluster>:80";};
};
```

The address must be reachable from the jobs, that is, from inside Kubernetes — not necessarily the same address the runner uses to reach the cluster from outside.

If the cluster DNS serves the jobs A records only (typical for Kubernetes), disable IPv6 resolution for the components inside the jobs:

```yson
"vanilla" = {
    ...
    "node_config" = {"address_resolver" = {"enable_ipv4" = %true; "enable_ipv6" = %false;};};
};
```

## Controller and workers in Kubernetes {#k8s}

The controller and the workers are long-lived `flow_server` processes in containers of the `flow` image: in Kubernetes pods or, as in the [`yt/yt/flow/examples/docker`](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/flow/examples/docker) example, in docker compose services on one host. You start and restart them, not the {{product-name}} scheduler. The runner from the same image only submits the spec from `pipeline.yson` to the controller and exits; its config needs no `vanilla` block.

A pipeline assembled from the stock classes, like the example, needs nothing but `flow_server`. The launcher ships companion code only into a vanilla operation, so a pipeline with a companion needs a worker image with the companion code and runtime, and paths inside that image in the resource parameters.

Before the controller starts, create the pipeline node and its system tables in Cypress — in the example, `yt_sync.py` does it with the `ytsaurus-flow-yt-sync-mini` package from PyPI.

### Node configs {#k8s-nodes}

The `YT_FLOW_MODE` environment variable sets the role of a `flow_server` process: `Controller` or `Worker`; without it, `flow_server` is the runner. Pass each process the token in `YT_TOKEN` and a node config:

```yson
{
    "cluster_url" = "<http-proxy-address>";
    "path" = "//path/to/pipeline";
    "rpc_port" = 9001;
    "monitoring_port" = 10001;
}
```

`cluster_url` and `path` are the same for the runner, the controller, and every worker. `rpc_port` is the port the process accepts RPC on; `monitoring_port` is the HTTP port with orchid and metrics (`/solomon_proxy/sensors`). Processes on one host need distinct ports.

### How the components connect {#k8s-network}

The controller and the workers publish their host's address and their ports in Cypress. The workers find the controller at that address, and by default the runner sends its commands through the cluster's RPC proxy, which connects to the controller's RPC port itself. The `yt flow` commands and the UI work the same way.

So the cluster's RPC proxy must reach the controller's RPC port at the published address. By default addresses resolve over IPv6 only. For an IPv4-only network, set in the configs of the runner, the controller, and the workers:

```yson
"address_resolver" = {
    "enable_ipv4" = %true;
    "enable_ipv6" = %false;
};
```

The runner config may enable both address families; a controller or worker config must enable exactly one.

While the controller has `require_proxy_signature = %false`, any host that reaches its RPC port runs commands without authentication. Open the controller and worker RPC ports only to the cluster and to hosts you trust.

### Direct controller commands {#direct-controller-commands}

If the cluster's RPC proxy cannot connect to the controller — a NAT, a firewall, a controller inside a Kubernetes network the cluster cannot reach — the runner's release fails with `Cannot connect to pipeline controller leader`. Switch to the direct mode, where the runner sends its commands to the controller itself (see [Direct runner commands](../../../flow/tools/cli.md#direct-controller-commands) for details):

1. In the runner config, enable the direct mode:

   ```yson
   "direct_controller_commands" = {
       "enabled" = %true;
   };
   ```

2. In the controller environment, set `YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1`. Without it, the controller keeps trying to confirm its leadership through the RPC proxy, which cannot succeed.
3. In `address_resolver` of the controller and worker configs, set `localhost_name_override` to the address the runner and the workers reach the controller at. It must belong to the one enabled address family. In the docker compose example all processes run on one host, so it is the loopback — `::1` with IPv6 or `127.0.0.1` with IPv4:

   ```yson
   "address_resolver" = {
       "localhost_name_override" = "::1";
   };
   ```

Only the runner has the direct mode: `yt flow` commands and the UI go through the RPC proxy and cannot reach such a controller.

## Reaching the cluster from outside {#external-access}

This setting is needed when the cluster runs in Kubernetes while the runner — or, in the [Kubernetes](#k8s) mode, the controller and the workers — runs outside its network. RPC proxy discovery returns in-cluster addresses, which are unreachable from outside. Turn discovery off and pass a proxy address reachable from outside (by default an RPC proxy listens on port 9013):

```yson
"clients_cache" = {
    "default_connection" = {
        "enable_proxy_discovery" = %false;
        "proxy_addresses" = ["<external-rpc-proxy-address>:9013"];
    };
};
```

For a vanilla operation, `cluster_url` still holds the HTTP proxy address reachable from inside the cluster: that is where the controller and the workers talk to it from their jobs.

## Building flow_server {#flow-server}

The released images carry `flow_server`. To launch without them — a launcher run on the host with `--flow-bin`, or a `flow_server` with your own changes — build it from the [{{product-name}} repository](https://github.com/ytsaurus/ytsaurus):

```bash
./ya make --build=release yt/yt/flow/bin/flow_server
strip -o flow_server.stripped yt/yt/flow/bin/flow_server/flow_server
```

## Troubleshooting {#troubleshooting}

#|
|| **Symptom** | **Cause and fix** ||
|| Components inside the jobs cannot connect to the cluster or to each other | The cluster name does not resolve from inside the jobs — set `proxy_url_aliasing_rules`; the DNS serves A records only — disable IPv6 in `node_config.address_resolver` (see [Cluster name resolution](#cluster-name)) ||
|| The launcher fails with `flow_server is not given` | The launcher runs outside the released images — launch it in the image (see [Launching from an image](#launch)) or pass `--flow-bin` ||
|| Java: the job fails with `JDK binary file does not exist` | The tasks do not use the `flow-java` image, or the runner runs outside it — use `flow-java` for both the tasks and the runner ||
|| `abort-op` fails with `Operation alias cannot be resolved without using runtime information` | It was given the operation alias — pass the operation id (see [Stopping](#stop)) ||
|| The runner's release fails with `Cannot connect to pipeline controller leader` | The cluster's RPC proxy cannot connect to the controller — enable the direct mode (see [Direct controller commands](#direct-controller-commands)) ||
|| Uploading the binary on deploy takes minutes | The binary is not stripped — use `strip` (see [Building flow_server](#flow-server)) ||
|#

## See also

- [Initial deployment](../../../flow/devops/vanilla/initial-deploy.md)
- [Basic pipeline operations](../../../flow/devops/vanilla/pipeline-operations.md)
- [The companion](../../../flow/concepts/companion.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)
