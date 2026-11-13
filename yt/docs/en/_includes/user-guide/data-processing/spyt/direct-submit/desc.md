# Direct submit in SPYT

With direct submit, a Spark application gets resources from the {{product-name}} scheduler without a pre-launched standalone cluster. The driver coordinates the application, and executors run its tasks.

This page explains how direct submit works and how it is configured. For launch steps, see the [guide](../../../../../user-guide/data-processing/spyt/launch.md#submit).

## Operating Principle {#explanation}

The key idea is that Spark delegates resource management to the {{product-name}} scheduler.

### Resource Allocation for the Driver {#driver-resources}

- **Cluster mode**: The driver is also launched as a separate Vanilla operation within the {{product-name}} cluster. In this mode, the driver's lifecycle (including logging, monitoring, and restart on failure) is fully controlled by YTsaurus. Suitable for production tasks.
- **Client mode**: The driver is launched on a client host — for example, on a developer's local machine or in a Jupyter notebook. This mode is convenient for debugging and interactive work, as it provides direct access to the driver's logs and state.

  {% note warning "Warning" %}

  In client mode, network access between executors and the driver must be ensured.

  {% endnote %}

### Resource Allocation for Executors {#executor-resources}

In this mode, the scheduler allocates resources to executors directly as part of a separate Vanilla operation on the {{product-name}} cluster. A job within the operation corresponds to one Spark executor. The specifications for executors (number of CPUs, amount of memory, disk type, etc.) are described in the Task section of the operation specification. It is possible to describe different resource profiles for Spark executors for tasks with heterogeneous workload during the lifecycle.

![](../../../../../../_images/spyt-direct-submit-operation-concept.png){ .center }

## Advantages of the Approach {#advantages}

- **Efficient resource usage** — allocation on demand, minimal downtime.
- **Simple management** — a single level of control instead of duplication (YTsaurus + Standalone).
- **Flexibility** — operation without losses at low load.
- **Fast task startup** — no need to pre-launch the cluster.
- **Easy migration** — compatibility with other clusters (including Hadoop) due to standard SparkAPI.

## Configuring {{product-name}} Operation {#operation-config}

In addition to the standard Spark parameters for the driver and executors, you can further configure operations and tasks through the corresponding conf parameters `spark.ytsaurus.{driver|executor}.{operation|task}.parameters`. The value is specified as a YSON string with valid Vanilla operation parameters. For example, you can set the type and amount of attached disks:

```bash
--conf spark.ytsaurus.executor.task.parameters='{"disk_request"={"disk_space"=1234567;
                                                 "account"="disk_account_name";
                                                 "medium_name"="ssd_slots_physical";};
                                                }'
```
The full list of parameters can be found in the section [operation options](../../../../../user-guide/data-processing/operations/operations-options.md).

## Using Docker Images and Porto Layers {#images}

When directly submitting Spark applications to {{product-name}}, you can use both Docker images and Porto layers for driver and executor tasks. This allows you to prepare the execution environment with the required system libraries, Python packages, and other dependencies.

You can find information on building your own Docker image in the [cluster configuration section](../../../../../user-guide/data-processing/spyt/cluster/configuration.md#build-image).

Docker images or Porto layers are passed via additional parameters:

```bash
OP_SPEC='{"docker_image"="[REGISTRY/]IMAGE:TAG";}'

spark-submit --master "ytsaurus://<cluster-host>:<port>" \
             --deploy-mode cluster \
             --num-executors 1 \
             --queue <pool> \
             --conf spark.ytsaurus.executor.task.parameters="$OP_SPEC" \
             --conf spark.ytsaurus.driver.task.parameters="$OP_SPEC" \
             ./spark-job.py
```

For more details, see the [root filesystem images section](../../../../../user-guide/data-processing/layers/layer-paths.md).

## RPC proxy roles { #proxy-roles }

A [proxy role](../../../../../admin-guide/cluster-access-proxy/index.md#proxy-role) is a string label that selects a group of RPC proxies during [proxy discovery](../../../../../admin-guide/cluster-access-proxy/index.md#discovery-roles). Different roles let you separate connections from the client host and from {{product-name}} cluster jobs.

A process can use several {{product-name}} clients: one for operation management and others for data access. The connection method depends on the client's purpose:

#|
|| **Process** | **Client purpose** | **Connection** ||
|| `spark-submit`, `spark-shell`, or a driver in `client` mode on the client host | Application submission, reading SPYT configurations, and operation management | Discovery ||
|| Driver in `client` mode on the client host without a local job socket | Reading and writing data | Discovery ||
|| Driver in `cluster` mode | Managing executor operations | Discovery ||
|| Driver and executors in {{product-name}} jobs | Reading and writing data | Local job proxy or discovery, under the conditions below ||
|#

To access the execution cluster, a data client uses the RPC proxy built into the job proxy when `spark.ytsaurus.rpc.job.proxy.enabled=true` (the default) and a local socket is available. This connection bypasses discovery and role selection. When reading data from another cluster, the client uses discovery with the configured role and bypasses the local socket.

For clients that use discovery, two parameters control the roles:

- `spark.hadoop.yt.proxyRole`: The role for the client on the application submission host.
- `spark.hadoop.yt.clusterProxyRole`: A separate role for processes inside the cluster. It replaces `spark.hadoop.yt.proxyRole` when these processes are prepared. If no separate role is set, the client role is retained.

The Data Source V1 write client on an executor uses the driver's saved configuration. [Its limitation in Spark 3.5.5](#v1-write-proxy-role) is described separately. For defaults, see the [parameter reference](../../../../../user-guide/data-processing/spyt/thesaurus/configuration.md#proxy-roles). The configuration order is described below.

If discovery cannot find suitable RPC proxies for the role, see [how to diagnose Cannot get rpc proxies](../../../../../user-guide/data-processing/spyt/problems.md#cannot-get-rpc-proxies).

## Configuration sources and priority { #config-sources }

During direct submit, SPYT augments the application's `SparkConf` from the following sources in descending priority order:

1. The `--conf` options of the `spark-submit` command and the application's `SparkConf`.
1. The `spark_conf` section of the SPYT release configuration. Its path is composed of `spark.ytsaurus.config.releases.path`, the SPYT version, and `spark.ytsaurus.config.launch.file` (see [Options for launching tasks directly](../../../../../user-guide/data-processing/spyt/thesaurus/configuration.md#direct-submit)).
1. The `spark_conf` section of the global configuration, `//home/spark/conf/global` by default (the `spark.ytsaurus.config.global.path` parameter).

Values from the user configuration are not overwritten: sources with lower priority only add missing parameters. The exception is Boolean flags from the `enablers` section of the cluster configurations: they can only disable a parameter, not enable it.

This order describes how `SparkConf` is merged. Parameters read before the merge do not change launch settings that have already been prepared. For example, `spark.ytsaurus.java.home` is read before the release and global settings are added to `SparkConf`; to change Java for launching the driver, specify it via `--conf`.

The client that reads the release and global configurations is created before they are applied. Its role comes from `--conf` or `SparkConf`. An incorrect role set only in the release configuration or `//home/spark/conf/global` therefore shows up later, in the driver or executors.

### Applying the cluster role { #cluster-proxy-role }

In `cluster` mode, you can set `spark.hadoop.yt.clusterProxyRole` for all users in the `spark_conf` section of `//home/spark/conf/global`. The driver uses it to start executors. A user can override the value with `--conf`.

In `client` mode, set a separate executor role in `--conf` or `SparkConf`. The `clusterProxyRole` substitution happens before the release and global configurations are loaded, so a value set only in those documents is not applied. For examples, see the [launch guide](../../../../../user-guide/data-processing/spyt/launch.md#proxy-roles-examples).

## Proxy role limitations { #proxy-role-limitations }

### Role when writing through Data Source V1 { #v1-write-proxy-role }

In Spark 3.5.5, writes through Data Source V1 (`spark.sql.sources.useV1SourceList=yt`) use the driver's saved Hadoop configuration on the executor. This configuration is created before the cluster role is substituted.

As a result, in `cluster` mode, the write client may retain `external` if `spark.hadoop.yt.proxyRole=external` is set in `--conf` and `spark.hadoop.yt.clusterProxyRole=internal` is set only in the global configuration. When the job proxy is disabled, this role is used for discovery.

Pass `--conf spark.hadoop.yt.clusterProxyRole=internal` when submitting the application. The role is then substituted before the Hadoop configuration is created. This removes the described cause of selecting the wrong role. Proxy availability and successful writes also depend on other settings and conditions.

## Executing Local Files {#submit-local}

Starting with SPYT version 2.4.0, when launching tasks directly in {{product-name}}, you can specify local files as executable modules and dependencies without pre-loading them to Cypress. In this case, local files will be pre-loaded into the file cache on Cypress and then used from there in the application. On re-launch, the cached files will be used. An example of the `spark-submit` command for this method is provided below:

```bash
spark-submit --master ytsaurus://<cluster name> \
             --deploy-mode cluster \
             --num-executors 5 \
             --executor-cores 4 \
             --py-files ~/path/to/my/dependencies.py \
             ~/path/to/my/script.py
```
