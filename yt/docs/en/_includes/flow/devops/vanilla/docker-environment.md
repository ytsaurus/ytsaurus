# Running in a docker environment

The simplest way to run a Flow pipeline is a single {{product-name}} [vanilla operation](../../../../user-guide/data-processing/operations/vanilla.md) that hosts both the controller and the workers: add a `vanilla` block to `pipeline.yson`, and the runner validates the spec, creates the operation, and starts the pipeline. This page covers clusters where operation jobs execute in docker images (the CRI job environment) — how a typical opensource {{product-name}} installation runs in Kubernetes: there are no porto layers, and the cluster name does not resolve out of the box. All of this affects how runtimes (a JRE, a Python interpreter) get into the jobs and how the pipeline components find the cluster.

## Job environment {#job-environment}

The environment of a vanilla task is set by the `docker_image` field of the task block:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 1; "docker_image" = "docker.io/library/eclipse-temurin:17-jre";};
    "controller" = {"count" = 1; "docker_image" = "docker.io/library/eclipse-temurin:17-jre";};
};
```

`pool` is the scheduler pool the operation runs in; see [Scheduler and pools](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

Two rules:

* An image name without a registry (`eclipse-temurin:17-jre`) resolves against the cluster's internal docker registry, and the operation fails to start if the image is not uploaded there. For Docker Hub images, use the full path with the `docker.io/library/` prefix.
* Static binaries — `flow_server`, C++ and Go pipelines — need no image: they run in the default job environment. An image is needed when the job must contain a runtime: a JRE for the Java companion or a Python interpreter.

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

## Building flow_server {#flow-server}

Every pipeline except C++ needs the `flow_server` server binary. Build it from the [{{product-name}} repository](https://github.com/ytsaurus/ytsaurus):

```bash
./ya make --build=release yt/yt/flow/bin/flow_server
strip -o flow_server.stripped yt/yt/flow/bin/flow_server/flow_server
```

The runner uploads the binary into the cluster's file cache on every deploy, so a stripped binary (hundreds of megabytes instead of gigabytes) makes deploys noticeably faster.

## Language specifics {#languages}

{% list tabs %}

- C++

  The pipeline is one static binary (the runner, the controller, and the worker at once), built with the same `./ya make` from a {{product-name}} checkout. No extra config fields and no `docker_image` are required:

  ```bash
  ./pipeline --config pipeline.yson
  ```

- Java

  The worker spawns the companion JVM inside the job, so the job needs a JDK. In a docker environment the image delivers it: set a JRE image in `docker_image` of both tasks (see [Job environment](#job-environment)) and the path to `java` inside the image in the companion resource parameters:

  ```yson
  "resources" = {
      "CompanionManager" = {
          "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
          "parameters" = {
              "main_class" = "com.example.pipeline.PipelineMain";
              "jdk_bin_path" = "/opt/java/openjdk/bin/java";
          };
      };
  };
  ```

  A `docker_image` in the config switches the runner into docker mode by itself — no `YT_FLOW_JDK_*` environment variables are needed. The worker spawns `java` by the exact path (no `PATH` lookup), and different images place it differently: `/opt/java/openjdk/bin/java` is the path in the `eclipse-temurin` images. Any image with a JRE of the required version works.

  Launch (the runner ships the classpath jars into the worker job itself):

  ```bash
  java -cp "<jar-directory>/*" \
      com.example.pipeline.PipelineMain --config pipeline.yson --flow-bin flow_server.stripped
  ```

- Python

  The companion needs a Python interpreter inside the job. Two ways to deliver it:

  * **A self-contained launcher.** The launcher and the SDK archive are shipped into the job via the worker's `local_files`; no image is needed:

    ```yson
    "resources" = {
        "CompanionManager" = {
            "resource_class_name" = "NYT::NFlow::NCompanion::TCompanionManager";
            "parameters" = {"entrypoint" = {"executable" = "./py_companion";};};
        };
    };
    ```

  * **A docker image with Python and the SDK.** Set the image in the tasks' `docker_image`, ship the pipeline code via the worker's `local_files`, and start the image's interpreter:

    ```yson
    "resources" = {
        "CompanionManager" = {
            "resource_class_name" = "NYT::NFlow::NCompanion::TCompanionManager";
            "parameters" = {
                "entrypoint" = {
                    "executable" = "/usr/local/bin/python3";
                    "args" = ["main.py"];
                };
            };
        };
    };
    ```

  The launch is the same in both cases:

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server.stripped
  ```

- Go

  A Go pipeline is a static binary that combines the launcher and the companion; it ships itself into the job. No runtime in the image and no `docker_image` are required:

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
|| Java: the job fails with `JDK binary file does not exist` | `jdk_bin_path` does not match the location of `java` in the chosen image — check the path inside the image ||
|| Uploading the binary on deploy takes minutes | The binary is not stripped — use `strip` (see [Building flow_server](#flow-server)) ||
|#

## See also

- [The companion](../../../../flow/concepts/companion.md)
- [Spec and DynamicSpec](../../../../flow/concepts/spec.md)
