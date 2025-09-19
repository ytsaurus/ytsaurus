# Using the {{product-name}} Shuffle service

{% note info %}

The service is available starting with SPYT 2.7.3.

{% endnote %}

The {{product-name}} Shuffle service is used to store intermediate data between computation stages. In contrast to the standard Spark Shuffle service that stores intermediate data in RAM and temporary disk directories, the {{product-name}} Shuffle service stores them in {{product-name}} chunks. This approach improves resilience against failures in distributed Spark stages.

## Enabling { #enabling }

When [launching tasks directly in {{product-name}}](../../../../user-guide/data-processing/spyt/launch.md#submit), set the conf parameter `spark.ytsaurus.shuffle.enabled` to `true`.

When using an [inner standalone Spark cluster within a Vanilla operation](../../../../user-guide/data-processing/spyt/launch.md#standalone), add the `--enable-ytsaurus-shuffle` option for the `spark-launch-yt` command.

## How it works

Spark writes data to the {{product-name}} Shuffle service within a transaction. The life cycle of the transaction is controlled by the driver that periodically pings it. The transaction is initiated at the moment a new shuffle is registered, with each shuffle being written within a separate transaction. Upon termination of the operation (whether successful or unsuccessful) with the shuffle instance, the transaction is rolled back, leading to the deletion of all chunks written within it. In case of driver errors and crashes, the transaction is also rolled back after the timeout expires. You can set the timeout using the parameter `spark.ytsaurus.shuffle.transaction.timeout`.

Data written via the Shuffle service cannot be read via the {{product-name}} API.

## Recommendations for use { #recommendations }

Use the {{product-name}} Shuffle service in the following cases:

- For complex multi-stage computations. For example, when joining multiple large tables, each exceeding 10M rows in size. If, in addition to complex computations, there are also simple ones (such as simple aggregations or joining a large table with a small one) performed on the cluster, we still recommend enabling the Shuffle service.

- When [launching tasks directly in {{product-name}}](../../../../user-guide/data-processing/spyt/launch.md#submit). The Shuffle service writes intermediate data to {{product-name}} chunks, ensuring its survival in case of Spark executor failures or preemptions. This approach prevents re-computation of stages that have already been computed but were stored on lost executors.

## Configuration parameters { #configuration }

All parameters related to configuring the {{product-name}} Shuffle service are described on the [configuration parameters page](../../../../user-guide/data-processing/spyt/thesaurus/configuration.md#shuffle).
