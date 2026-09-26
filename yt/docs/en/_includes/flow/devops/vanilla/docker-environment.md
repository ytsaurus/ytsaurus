# Running in a docker environment

The simplest way to run a Flow pipeline is a single {{product-name}} [vanilla operation](../../../../user-guide/data-processing/operations/vanilla.md) that hosts both the controller and the workers: add a `vanilla` block to `pipeline.yson`, and the runner validates the spec, creates the operation, and starts the pipeline. This page covers clusters where operation jobs execute in docker images (the CRI job environment) — how a typical opensource {{product-name}} installation runs in Kubernetes: there are no porto layers, and the cluster name does not resolve out of the box. All of this affects how a JRE or other OS dependencies get into the jobs and how the pipeline components find the cluster.

## Job environment {#job-environment}

The environment of a vanilla task is set by the `docker_image` field of the task block:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-java:<version>";};
    "controller" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-java:<version>";};
};
```

`pool` is the scheduler pool the operation runs in; see [Scheduler and pools](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

Two rules:

* An image name without a registry (`eclipse-temurin:17-jre`) resolves against the cluster's internal docker registry, and the operation fails to start if the image is not uploaded there. For Docker Hub images, use the full path with the `docker.io/library/` prefix.
* Static binaries — `flow_server`, C++ and Go pipelines — need no image: they run in the default job environment. The Python pipeline built with `ya make` carries its own interpreter and SDK. Use an image when the job needs a JRE for the Java companion or other OS dependencies.

## Cluster name resolution {#cluster-name}

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

## Reaching the cluster from outside {#external-access}

If the cluster runs in Kubernetes, RPC proxy discovery returns their in-cluster addresses, which the runner cannot reach from outside. Turn discovery off and pass a proxy address reachable from outside (by default an RPC proxy listens on port 9013):

```yson
"clients_cache" = {
    "default_connection" = {
        "enable_proxy_discovery" = %false;
        "proxy_addresses" = ["<external-rpc-proxy-address>:9013"];
    };
};
```

`cluster_url` still holds the HTTP proxy address reachable from inside the cluster: that is where the controller and the workers talk to it from their jobs.

## Building flow_server {#flow-server}

Every pipeline except C++ needs the `flow_server` server binary. Build it from the [{{product-name}} repository](https://github.com/ytsaurus/ytsaurus):

```bash
./ya make --build=release yt/yt/flow/bin/flow_server
strip -o flow_server.stripped yt/yt/flow/bin/flow_server/flow_server
```

The runner uploads the binary into the cluster's file cache on every deploy, so a stripped binary (hundreds of megabytes instead of gigabytes) makes deploys noticeably faster.

The released Flow images set `YT_FLOW_BIN=/usr/bin/flow_server`, so launchers run inside them need no `--flow-bin`.

## Language specifics {#languages}

{% list tabs %}

- C++

  The pipeline is one static binary (the runner, the controller, and the worker at once), built with the same `./ya make` from a {{product-name}} checkout. No extra config fields and no `docker_image` are required:

  ```bash
  ./pipeline --config pipeline.yson
  ```

- Java

  The worker spawns the companion inside the job, so the job needs a JRE. In a docker environment the image delivers it: set the `ghcr.io/ytsaurus/flow-java` image in `docker_image` of both tasks (see [Job environment](#job-environment)) and the entry-point class in the companion resource parameters. The image versions are listed in the [Flow releases](../../../../admin-guide/releases.md#flow).

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

  A `docker_image` in the config switches the runner into docker mode by itself — no `YT_FLOW_JDK_*` environment variables are needed.

  The runner runs in the same `flow-java` image: it takes the worker's `java` path from `jdk_bin_path` in the resource parameters, else from `YT_FLOW_JDK_BIN_PATH`, else it takes the `java` it runs on itself. The runner ships the classpath jars into the worker job itself; if the image already holds them, set `classpath` in the resource parameters, e.g. `/app/pipeline/lib/*`, and the runner ships no jars for that resource. Launch from the directory with `pipeline.yson` and the jars in `lib/`:

  ```bash
  docker run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-java:<version> \
      -cp "lib/*" com.example.pipeline.PipelineMain --config pipeline.yson
  ```

- Python

  The Python pipeline binary built with `ya make` acts as both launcher and companion. With `vanilla.enable = %true`, the runner ships that binary into the worker's `local_files` as `py_companion` and sets each generic `TCompanionManager` entrypoint to `./py_companion`. A resource that already declares an `entrypoint` with a non-empty `executable` other than `./py_companion` keeps it: that is how to take the companion from the image, e.g. `/usr/bin/python3` with `args = ["/app/pipeline/main.py"]`. When every companion resource declares one, the runner ships no binary.

  The self-contained launcher needs no image for Python or the Flow SDK. Set `docker_image` if the job needs other OS dependencies; without a declared `entrypoint` the companion remains `./py_companion`. [Build the Python pipeline](../../../../flow/python/getting-started.md#build) and launch it with:

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server.stripped
  ```

- Go

  A Go pipeline is a static binary that combines the launcher and the companion; it ships itself into the job. No runtime in the image and no `docker_image` are required. To run a companion binary of the image instead, declare it in the resource parameters, e.g. `"entrypoint" = {"executable" = "/app/pipeline/companion"}`: the runner keeps a declared `executable` other than `./go_companion` and ships no binary when every companion resource declares one:

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server.stripped
  ```

- YQL

  A YQL query is compiled into a Flow pipeline and launched as one vanilla operation — the pipeline's Cypress objects are created automatically. You need the `ytrun` client and the `ytflow_worker` from the {{product-name}} repository:

  ```bash
  ./ya make --build=release yt/yql/tools/ytrun yt/yql/tools/ytflow_worker
  ```

  Query syntax and the control pragmas are described in [YQL / Getting started](../../../../flow/yql/getting-started.md).

{% endlist %}

## Troubleshooting {#troubleshooting}

#|
|| **Symptom** | **Cause and fix** ||
|| The operation fails to start with a docker image resolution error | An image name without a registry resolves against the cluster's internal registry — add the `docker.io/library/` prefix (see [Job environment](#job-environment)) ||
|| Components inside the jobs cannot connect to the cluster or to each other | The cluster name does not resolve from inside the jobs — set `proxy_url_aliasing_rules`; the DNS serves A records only — disable IPv6 in `node_config.address_resolver` (see [Cluster name resolution](#cluster-name)) ||
|| Java: the job fails with `JDK binary file does not exist` | The tasks do not use the `flow-java` image, or the runner runs outside it — use `flow-java` for both the tasks and the runner ||
|| Uploading the binary on deploy takes minutes | The binary is not stripped — use `strip` (see [Building flow_server](#flow-server)) ||
|#

## See also

- [Initial deployment](../../../../flow/devops/vanilla/initial-deploy.md)
- [Basic pipeline operations](../../../../flow/devops/vanilla/pipeline-operations.md)
- [The companion](../../../../flow/concepts/companion.md)
- [Spec and DynamicSpec](../../../../flow/concepts/spec.md)
