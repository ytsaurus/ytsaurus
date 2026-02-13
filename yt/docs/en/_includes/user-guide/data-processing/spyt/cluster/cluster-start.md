# Starting a Spark cluster

This section contains expanded instructions for starting a Spark cluster. Basic start operations are described in the [Quick start](../../../../../user-guide/data-processing/spyt/launch.md#standalone) section.

{% note warning "Attention!" %}

A started Spark cluster statically occupies the resources allocated to it. So, it is recommended that the cluster be started in a separate computational pool with guaranteed resources.  It makes sense to use one cluster for the command, and recycle the resources among several users.  We recommend [launching tasks directly](../../../../../user-guide/data-processing/spyt/launch.md#submit) if you do not plan to launch Spark tasks at a high intensity (more than once per hour).


{% endnote %}

## Auto-scaler { #auto-scale }

To save resources of the computational pool if the load is low, a special auto-scaler mode can be turned on in Spark, which proportionally decreases the resources used.

### How it works { #how-works }

The operation {{product-name}} has an `update_operation_parameters` method that enables the operation parameters to be changed.  The number of jobs in the operation can be changed via the `user_slots` parameter.  When the parameter is changed, the scheduler stops some of the jobs, or launches new ones (within the limit specified at the start of the operation).  Since the scheduler believes that all jobs in the operation are the same, this scaling method, performed in the regular mode in Spark, could lead to loss of the master or the history server, as well as of workers that Spark job drivers are being performed for.  To prevent disruptions to the Spark cluster's operation, it is started not as a single {{product-name}} operation, but as several. This way, one operation is allocated to the dynamically changing set of workers, and can scale within the limits configured at the start.  In one or two other operations, a master and history-server (one operation), or a driver (when drivers are launched on the cluster, two operations) are performed.


### Starting a cluster with an auto-scaler { #start-auto }

Additional parameters are used for the `spark-launch-yt` launch script, or similar parameters of the SPYT client library.

- The `autoscaler-period <period>` is the frequency of auto-scaler launches, and (potentially) of changes in the operation settings. The period is programmed in `<length><unit of measurement [d|h|min|s|ms|µs|ns]>`.
- `enable-multi-operation-mode` turns on Spark start mode in multiple {{product-name}} operations.
- `enable-dedicated-driver-operation-mode` launches workers for drivers in a separate {{product-name}} operation.
- `driver-num <number of workers>` allocates a certain number of workers for the driver.
- `autoscaler-max-free-workers` is the maximum value of free workers (all superfluous workers will be stopped).
- `autoscaler-slot-increment-step` is the increment in which the number of workers is increased when the cluster is automatically expanded.

Example:

```bash
$ spark-launch-yt
--proxy <cluster_name>
--autoscaler-period 1s
--enable-multi-operation-mode
--discovery-path //discovery/path
```



## Updating a cluster { #update-cluster }

To update a Spark cluster, the following actions must be performed:

1. Stop the operation with the current cluster in {{product-name}}. You can find a link to the operation using `spark-discovery-yt`. You can also use the `--abort-existing` flag of the `spark-launch-yt` command. In this case, the current cluster will be stopped before the new one starts.
2. Start a cluster using `spark-launch-yt`. The desired version can be specified in the `spark-cluster-version` argument.  If no version is specified, the last version will be started.


## spark-launch-yt parameters { #spark-launch-yt-params }

To start an internal Spark cluster, use the `spark-launch-yt` command and pass a number of parameters to it. They are described in the table below:

| **Parameter** | **Required** | **Default value** | **Description** | **Starting with version** |
| ------------ | ---------------- | ------------------------- | ------------ | ------------------ |
| `--discovery-path` | yes | - | Path to the directory for service data on Cypress (Discovery path). | - |
| `--proxy` | no | Value from the `YT_PROXY` environment variable. | {{product-name}} cluster address | - |
| `--pool` | no | - | {{product-name}} compute pool that will be used to start the cluster. | - |
| `--operation-alias` | no | - | Alias of a {{product-name}} operation with the Spark cluster. | - |
| `--params` | no | - | Additional {{product-name}} operation parameters specified as a YSON string. Learn more on the [configuration](../../../../../user-guide/data-processing/spyt/cluster/configuration.md#add) page. | - |
| `--spyt-version` | no | Version of the `ytsaurus-spyt` package on the client. | SPYT version that will be used to start the cluster. | - |
| `--preemption-mode` | no | normal | Preemption mode used by the {{product-name}} scheduler. Possible values: "normal" or "graceful". | - |
| `--enable-mtn`, `--disable-mtn` | no | `--disable-mtn` | Use Multi-Tenant Network (MTN) to start the cluster. When using MTN, you must also specify the network project via the `--network-project` parameter. | - |
| `--network-project` | no | - | Name of the network project where a cluster operation will run. | - |
| `--prefer-ipv6`, `--prefer-ipv4` | no | `--prefer-ipv4` | The type of IP addresses used for working with the cluster. If you use IPv6 addressing when working with Spark 3.4.0 and higher, set the `SPARK_PREFER_IPV6=true` environment variable. | - |
| `--enable-tmpfs`, `--disable-tmpfs` | no | `--disable-tmpfs` | Use part of the RAM allocated to the Spark worker to mount tmpfs. When using tmpfs, you must also specify the amount via the `--tmpfs-limit` parameter. | - |
| `--tmpfs-limit` | no | 8G | Amount of memory used for tmpfs. | - |
| `--enable-tcp-proxy`, `--disable-tcp-proxy` | no | `--disable-tcp-proxy` | Use a TCP proxy for external access to the cluster. | 1.72.0 |
| `--tcp-proxy-range-start` | no | 30000 | Start of the range of ports used for the TCP proxy. | 1.72.0 |
| `--tcp-proxy-range-size` | no | 100 | Size of the range of ports used for the TCP proxy. | 1.72.0 |
| `--enable-rpc-job-proxy`, `--disable-rpc-job-proxy` | no | `--enable-rpc-job-proxy` | Use an RPC proxy embedded in a job proxy. If this option is disabled, a general RPC proxy will be used, which may result in cluster performance degradation. | 1.77.0 |
| `--enable-ytsaurus-shuffle`, `--disable-ytsaurus-shuffle` | no | `--disable-ytsaurus-shuffle` | Use the [{{product-name}} Shuffle service](../../../../../user-guide/data-processing/spyt/shuffle.md) | 2.7.2 |
| `--rpc-job-proxy-thread-pool-size` | no | 4 | Size of a thread pool for the RPC job proxy. | 1.77.0 |
| `--group-id` | no | - | Discovery group ID. Used when starting a cluster with multiple operations. | - |
| `--enable-squashfs`, `--disable-squashfs` | no | `--disable-squashfs` | Use pre-configured SquashFS layers in {{product-name}} jobs. | 2.6.0 |
| `--cluster-java-home` | no | `/opt/jdk11` | Path to JAVA HOME in {{product-name}} cluster containers. | 2.6.0 |
| `--master-memory-limit` | no | 4G | Amount of memory allocated to the container with the Master server. | - |
| `--master-port` | no | 27001 | Port for Spark RPC calls on the Master server. | - |
| `--worker-cores` | yes | - | Number of CPU cores allocated to one worker. | - |
| `--worker-memory` | yes | - | Amount of memory allocated to a worker. This amount will further be distributed among Spark processes, such as the driver and executors running on that worker. | - |
| `--worker-num` | yes | - | Number of workers in the cluster. | - |
| `--worker-cores-overhead` | no | 0 | Extra CPU cores allocated to a worker. These cores will not be used directly by Spark applications, but are required for auxiliary processes launched along with the worker. | - |
| `--worker-memory-overhead` | no | 2G | Additional memory allocated to a worker. This amount is required for additional processes launched in the worker container, such as child processes run by executors. This amount is also needed when using `pyspark`, especially when working with `Python UDFs`. In this case, the executor will start a child `Python` process that will execute the `UDF` code and require additional memory. The standard recommended value for `--worker-memory-overhead` when working with Python UDFs is 40% of the amount allocated to the worker via the `--worker-memory` parameter. | - |
| `--worker-timeout` | no | 10 min | The maximum time a worker will wait to register on the Spark master. After this time, the worker process will terminate with an error. The following time units are available: s (seconds), m or min (minutes), h (hours), and d (days). If no unit is specified, defaults to seconds. | - |
| `--worker-gpu-limit` | no | 0 | Number of GPUs on a worker. | - |
| `--worker-disk-name` | no | default | Name of the disk requested in the operation specification. For more information, see [Disk requests for jobs](../../../../../user-guide/data-processing/operations/operations-options.md#disk_request). | - |
| `--worker-disk-limit` | no | - | Maximum disk capacity requested in the operation specification. For more information, see [Disk requests for jobs](../../../../../user-guide/data-processing/operations/operations-options.md#disk_request). | - |
| `--worker-disk-account` | no | - | [Account](../../../../../user-guide/storage/accounts.md) for requesting a disk in the operation specification. For more information, see [Disk requests for jobs](../../../../../user-guide/data-processing/operations/operations-options.md#disk_request). | - |
| `--worker-port` | no | 27001 | Port for Spark RPC calls on the Worker server. | - |
| `--enable-history-server`, `--disable-history-server` | no | `--enable-history-server` | Flag for running the History Server as part of the cluster. | - |
| `--history-server-memory-limit` | no | 4G | Amount of memory for the History Server process. | - |
| `--history-server-memory-overhead` | no | 2G | Extra memory allocated for auxiliary processes in the container with the History Server. | - |
| `--history-server-cpu-limit` | no | 1 | Number of CPU cores allocated to the History Server. | - |
| `--shs-location` | no | `--discovery-path`/logs | Path to the directory that contains event logs and is used in the History Server. | - |
| `--ssd-account` | no | - | {{product-name}} [account](../../../../../user-guide/storage/accounts.md) used for SSD allocation. | - |
| `--ssd-limit` | no | 0 | The SSD disk capacity allocated to a {{product-name}} job. | - |
| `--abort-existing` | no | - | Stopping a running Spark cluster on the specified `--discovery-path` before starting a new one. | - |
| `--cluster-log-level` | no | INFO | Cluster logging level. | - |
| `--enable-stderr-table`, `--disable-stderr-table` | no | `--disable-stderr-table` | Write {{product-name}} operation logs to a table. The table is located inside `--discovery-path`, in the `logs` directory. | - |
| `--enable-advanced-event-log`, `--disable-advanced-event-log` | no | --disable-advanced-event-log | Write Spark application event logs to dynamic {{product-name}} tables. | - |
| `--enable-worker-log-transfer`, `--disable-worker-log-transfer` | no | --disable-worker-log-transfer | Transfer Spark application logs from a worker container to a table on Cypress. | - |
| `--enable-worker-log-json-mode`, `--disable-worker-log-json-mode` | no | --disable-worker-log-json-mode | Write worker logs in JSON format. | - |
| `--worker-log-update-interval` | no | 10 min | Frequency of transferring Spark application logs from a worker to Cypress. | - |
| `--worker-log-table-ttl` | no | 7d | Lifetime of a log table for Spark applications on Cypress. | - |
| `--enable-dedicated-driver-operation-mode`, `--disable-dedicated-driver-operation-mode` | no | `--disable-dedicated-driver-operation-mode` | Run a dedicated {{product-name}} operation for a driver. | - |
| `--driver-cores` | no | `--worker-cores` | Number of CPU cores allocated to a worker used to run drivers. | - |
| `--driver-memory` | no | `--worker-memory` | The amount of memory allocated to a worker used to run drivers. | - |
| `--driver-num` | no | `--worker-num` | Number of workers running specifically for drivers. | - |
| `--driver-cores-overhead` | no | `--worker-cores-overhead` | Extra cores allocated to workers for drivers (see `--worker-cores-overhead`). | - |
| `--driver-timeout` | no | `--worker-timeout` | The maximum time a worker will await registration on the Spark master. After this time, the worker process will terminate with an error. | - |
| `--autoscaler-period`, `--autoscaler-metrics-port`, `--autoscaler-sliding-window`, `--autoscaler-max-free-workers`, `--autoscaler-slot-increment-step` | no | - | Parameters used for working with an [auto-scaler](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md#auto-scale). | - |
| `--enable-livy` | no | false | Run the Livy server as part of the cluster. You can learn more about the Livy server [here](../../../../../user-guide/data-processing/spyt/cluster/livy.md). | 1.74.0 |
| `--livy-driver-cores` | no | 1 | Number of CPU cores allocated to a driver started via Livy. | 1.74.0 |
| `--livy-driver-memory` | no | 1G | Amount of CPU memory allocated to a driver started via Livy. | 1.74.0 |
| `--livy-max-sessions` | no | 3 | Maximum number of sessions that can run on the Livy server simultaneously. | 1.74.0 |
| `--id` | no | - | Deprecated. Left for backward compatibility. | - |
| `--discovery-dir` | no | - | Deprecated. Left for backward compatibility. | - |
| `-h`, `--help` | no | - | Command parameter reference | - |
