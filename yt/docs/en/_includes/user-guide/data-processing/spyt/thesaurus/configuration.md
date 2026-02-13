# Configuration parameters for running Spark tasks

This section contains a list of configuration parameters that can be passed when launching Spark tasks. This is done by specifying additional parameters via the `--conf` option of basic Spark commands, such as `spark-submit` and `spark-shell`, as well as {{product-name}} wrappers for them, such as `spark-submit-yt` and `spark-shell-yt`.

## Basic options { #main }

Most of the options are available starting with version 1.23.0, unless otherwise specified.

| **Parameter** | **Default value** | **Description** | **Starting with version** |
| ------------------- | --------------- | ---------------------------|------------------------------ |
| `spark.yt.write.batchSize` | `500000` | Size of data sent in a single `WriteTable` operation. |
| `spark.yt.write.miniBatchSize` | `1000` | Size of a data block sent in `WriteTable`. |
| `spark.yt.write.timeout` | `120 seconds` | Write timeout limit for a single data block. |
| `spark.yt.write.typeV3.enabled` (`spark.yt.write.writingTypeV3.enabled` before 1.75.2) | `true` | Writing of tables with a schema in [type_v3](../../../../../user-guide/storage/data-types.md) format instead of `type_v1`. | 1.75.3 |
| `spark.yt.read.vectorized.capacity` | `1000` | Maximum number of rows in a batch for reading via the `wire` protocol. |
| `spark.yt.read.arrow.enabled` | `true` | Use the `arrow` format to read data (if possible). |
| `spark.hadoop.yt.timeout` | `300 seconds` | Timeout on reads from {{product-name}}. |
| `spark.yt.read.typeV3.enabled` (`spark.yt.read.parsingTypeV3.enabled` before 1.75.2) | `true` | Reading of tables with a schema in [type_v3](../../../../../user-guide/storage/data-types.md) format instead of `type_v1`. | 1.75.3 |
| `spark.yt.read.keyColumnsFilterPushdown.enabled` | `true` | Use Spark query filters to selectively read from {{product-name}}. |
| `spark.yt.read.keyColumnsFilterPushdown.union.enabled` | `false` | Combine all filters into a continuous range for selective reading. |
| `spark.yt.read.keyColumnsFilterPushdown.ytPathCount.limit` | `100` | Maximum number of table ranges for selective reading. |
| `spark.yt.transaction.timeout` | `5 minutes` | Write operation transaction timeout. |
| `spark.yt.transaction.pingInterval` | `30 seconds` | Pinging interval of a write operation transaction. |
| `spark.yt.globalTransaction.enabled` | `false` | Use a [global transaction](../../../../../user-guide/data-processing/spyt/read-transaction.md). |
| `spark.yt.globalTransaction.id` | `None` | Global transaction ID. |
| `spark.yt.globalTransaction.timeout` | `5 minutes` | Global transaction timeout. |
| `spark.hadoop.yt.user` | - | {{product-name}} user name. |
| `spark.hadoop.yt.token` | - | {{product-name}} user token. |
| `spark.yt.read.ytPartitioning.enabled` | `true` | Use table partitioning by {{product-name}}. | 1.72.0 |
| `spark.yt.read.planOptimization.enabled` | `false` | Optimize aggregations and joins on sorted input data. |
| `spark.yt.read.keyPartitioningSortedTables.enabled` | `true` | Use sorted table partitioning by key, required to optimize plans. |
| `spark.yt.read.keyPartitioningSortedTables.unionLimit` | `1` | Maximum number of partition joins when switching from reading by index to reading by key. |
| `spark.yt.read.transactional` | `true` | Use shapshot lock for reading if transaction is not specified. It is recommended to turn this option off when reading immutable data to improve reading perfomance | 2.6.0 |
| `spark.yt.read.ytDistributedReading.enabled` | `false` | Use distributed API for reading data from {{product-name}}. This method reduces the number of requests to the {{product-name}} master when reading data, but is not yet compatible with the `spark.yt.read.planOptimization.enabled` option | 2.8.0 |
| `spark.yt.write.distributed.enabled` | `false` | Use distributed API for writing data to {{product-name}}. This method reduces the number of requests to the {{product-name}} master when writing data, but is only applicable to working with static tables | 2.8.0 |

## Options for launching tasks directly { #direct-submit }

| **Parameter** | **Default value** | **Description** | **Starting with version** |
| ------------ | ------------------------- | ------------ | ------------------ |
| `spark.ytsaurus.config.global.path` | `//home/spark/conf/global` | Path to the document with a global Spark and SPYT configuration on the cluster. | 1.76.0 |
| `spark.ytsaurus.config.releases.path` | `//home/spark/conf/releases` for release versions, `//home/spark/conf/pre-releases` for pre-release versions. | Path to the SPYT release configuration. | 1.76.0 |
| `spark.ytsaurus.distributives.path` | `//home/spark/distrib` | Path to the directory with Spark distributions. Within this directory, the structure looks like `a/b/c/spark-a.b.c-bin-hadoop3.tgz`. | 2.0.0 |
| `spark.ytsaurus.config.launch.file` | `spark-launch-conf` | The name of the document with the release configuration located within the directory `spark.ytsaurus.config.releases.path`. | 1.76.0 |
| `spark.ytsaurus.spyt.version` | Matches the SPYT version on the client. | The SPYT version to be used on the cluster when launching a Spark application. | 1.76.0 |
| `spark.ytsaurus.driver.maxFailures` | 5 | Maximum allowable number of driver failures before the operation is considered failed. | 1.76.0 |
| `spark.ytsaurus.executor.maxFailures` | 10 | Maximum allowable number of executor failures before the operation is considered failed. | 1.76.0 |
| `spark.ytsaurus.executor.operation.shutdown.delay` | 10000 | Maximum allowable time in milliseconds to wait for executors to finish when stopping the application before aborting the operation with executors. | 1.76.0 |
| `spark.ytsaurus.pool` | - | The scheduler pool where driver and executor operations should be run. | 1.78.0 |
| `spark.ytsaurus.python.binary.entry.point` | - | The function used as an entry point when using compiled Python tasks. | 2.4.0 |
| `spark.ytsaurus.python.executable` | - | Path to the Python interpreter used in the driver and executors. | 1.78.0 |
| `spark.ytsaurus.tcp.proxy.enabled` | false | Whether a TCP proxy is used to access the operation. | 2.1.0 |
| `spark.ytsaurus.tcp.proxy.range.start` | 30000 | Minimum port number for a TCP proxy. | 2.1.0 |
| `spark.ytsaurus.tcp.proxy.range.size` | 1000 | Size of the range of ports that can be allocated for a TCP proxy. | 2.1.0 |
| `spark.ytsaurus.cuda.version` | - | CUDA version used for Spark applications. Makes sense if the computations consume GPU. | 2.1.0 |
| `spark.ytsaurus.redirect.stdout.to.stderr` | false | Redirect user script output from stdout to stderr. | 2.1.0 |
| `spark.ytsaurus.remote.temp.files.directory` | `//tmp/yt_wrapper/file_storage` | Path to cache on Cypress to load local scripts. | 2.4.0 |
| `spark.ytsaurus.annotations` | - | Annotations for driver and executor operations. | 2.2.0 |
| `spark.ytsaurus.driver.annotations` | - | Annotations for a driver operation. | 2.2.0 |
| `spark.ytsaurus.executors.annotations` | - | Annotations for an executor operation. | 2.2.0 |
| `spark.ytsaurus.driver.watch` | true | Flag for monitoring a driver operation executed in cluster mode. | 2.4.2 |
| `spark.ytsaurus.network.project` | - | Name of the network project where a Spark application is launched. | 2.4.3 |
| `spark.hadoop.yt.mtn.enabled` | false | Flag for enabling MTN support | 2.4.3 |
| `spark.ytsaurus.squashfs.enabled` | false | Use squashFS layers instead of porto layers in a {{product-name}} job. | 2.6.0 |
| `spark.ytsaurus.client.rpc.timeout` | - | Timeout used in an RPC client to start {{product-name}} operations. | 2.6.0 |
| `spark.ytsaurus.rpc.job.proxy.enabled` | true | Flag of using an RPC proxy embedded in a job proxy. | 2.6.0 |
| `spark.ytsaurus.java.home` | `/opt/jdk[11,17]` | Path to the JDK home directory used in cluster containers. Depends on the JDK used on the client side. Allowed versions: JDK11 and JDK17. | 2.6.0 |
| `spark.ytsaurus.shuffle.enabled` | false | Use the [{{product-name}} Shuffle service](../../../../../user-guide/data-processing/spyt/shuffle.md) | 2.7.2 |
| `spark.ytsaurus.executor.state.poll.interval` | 20s | The period for checking the state of the operation with executors. If the operation is in a final status, the driver will be stopped | 2.8.0 |


## Configuration options for the {{product-name}} Shuffle service { #shuffle }

| **Parameter** | **Default value** | **Description** | **Starting with version** |
| ------------ | ------------------------- | ------------ | ------------------ |
| `spark.ytsaurus.shuffle.transaction.timeout` | 5m | Timeout for the transaction processing shuffle chunk writes. In regular operation mode, the transaction is periodically pinged by the driver, and the timeout sets the time between the last ping and transaction rollback with chunk deletion. | 2.7.0 |
| `spark.ytsaurus.shuffle.account` | intermediate | [Account](../../../../../user-guide/storage/accounts.md) used for writing shuffle chunks. | 2.7.0 |
| `spark.ytsaurus.shuffle.medium` | - | [Medium](../../../../../user-guide/storage/media.md) used for writing shuffle chunks. Defaults to the system-wide setting. | 2.7.0 |
| `spark.ytsaurus.shuffle.replication.factor` | - | Shuffle chunk replication factor. Defaults to the system-wide setting. | 2.7.0 |
| `spark.ytsaurus.shuffle.partition.column` | partition | The name of the chunk column used to store the target partition index. | 2.7.0 |
| `spark.ytsaurus.shuffle.write.row.size` | 8m | The maximum size of a single row in a chunk containing shuffle data. This value is not directly related to the size of shuffle data rows but serves to partition serialized shuffle data into chunk rows. Reducing this value increases the number of rows in the chunk, while raising the value may result in exceeding the maximum allowed chunk row size. | 2.7.0 |
| `spark.ytsaurus.shuffle.write.buffer.size` | 10 | Shuffle data write buffer size (in rows) in {{product-name}}. Set this parameter together with `spark.ytsaurus.shuffle.write.row.size` to avoid RAM overflow. | 2.7.0 |
| `spark.ytsaurus.shuffle.write.config` | - | Additional parameters for writing shuffle data in {{product-name}} in [YSON](../../../../../user-guide/storage/yson.md) format. Matches the [TableWriter](../../../../../user-guide/storage/io-configuration.md#table_writer) configuration. | 2.7.0 |
| `spark.ytsaurus.shuffle.read.config` | - | Additional parameters for reading shuffle data in {{product-name}} in [YSON](../../../../../user-guide/storage/yson.md) format. Matches the [TableReader](../../../../../user-guide/storage/io-configuration.md#table_reader) configuration. | 2.7.0 |


## Options for running tasks in an internal cluster { #spark-submit-yt-conf }

To run tasks in an internal cluster, use the `spark-submit-yt` wrapper. Its parameters match those of the `spark-submit` command from the Spark distribution, with the following exception:

- Instead of `--master`, you should use the parameters `--proxy` and `--discovery-path`. They determine which {{product-name}} cluster will be used to run computations and which internal Spark cluster on that {{product-name}} cluster the task will be sent to, respectively.
