# Using the {{product-name}} Shuffle service

{% note info %}

The service is available starting with SPYT 2.7.3.

{% endnote %}

The {{product-name}} Shuffle service is used to store intermediate data between computation stages. In contrast to the standard Spark Shuffle service that stores intermediate data in RAM and temporary disk directories, the {{product-name}} Shuffle service stores them in {{product-name}} chunks. This approach improves resilience against failures in distributed Spark stages.

## Enabling { #enabling }

### Direct task launch { #enabling-direct }

When [launching tasks directly in {{product-name}}](../../../../user-guide/data-processing/spyt/launch.md#submit), set the conf parameter `spark.ytsaurus.shuffle.enabled` to `true`. The application will use the {{product-name}} Shuffle service instead of the native Spark shuffle.

{% note warning "Warning" %}

When dynamic allocation is enabled, the {{product-name}} Shuffle service is mandatory: without `spark.ytsaurus.shuffle.enabled=true`, the launch will fail with the error `Dynamic allocation requires YTsaurus shuffle service`.

{% endnote %}

### Internal standalone cluster { #enabling-standalone }

When launching an [internal standalone cluster](../../../../user-guide/data-processing/spyt/launch.md#standalone) using the `spark-launch-yt` command, two independent shuffle services are available, each controlled by its own option:

| Option | Shuffle service | Default |
| --- | --- | --- |
| `--enable-spark-shuffle` / `--disable-spark-shuffle` | Native external Spark shuffle service | Enabled |
| `--enable-ytsaurus-shuffle` / `--disable-ytsaurus-shuffle` | {{product-name}} Shuffle service | Disabled |

The `--enable-ytsaurus-shuffle` option only makes the {{product-name}} Shuffle service available on workers. Which service the application uses is determined at launch time by the `spark.ytsaurus.shuffle.enabled` parameter (`true` — {{product-name}} Shuffle, otherwise — native Spark shuffle). Each application must use only one of the services.

{% note warning "Warning" %}

If an application is launched with `spark.ytsaurus.shuffle.enabled=true` and the cluster is started without the `--enable-ytsaurus-shuffle` option, there will be no error at the submit stage. The application will fail at runtime during the first shuffle stage.

{% endnote %}

{% note info %}

It is possible to start the cluster with both services disabled (`--disable-spark-shuffle --disable-ytsaurus-shuffle`). There will be no error, but intermediate data will be stored only on executors and will be lost if they fail.

{% endnote %}

## How it works { #how }

Spark writes data to the {{product-name}} Shuffle service within a transaction. The life cycle of the transaction is controlled by the driver that periodically pings it. The transaction is initiated at the moment a new shuffle is registered, with each shuffle being written within a separate transaction. Upon termination of the operation (whether successful or unsuccessful) with the shuffle instance, the transaction is rolled back, leading to the deletion of all chunks written within it. In case of driver errors and crashes, the transaction is also rolled back after the timeout expires. You can set the timeout using the parameter `spark.ytsaurus.shuffle.transaction.timeout`.

Data written via the Shuffle service cannot be read via the {{product-name}} API.

## Recommendations for use { #recommendations }

Use the {{product-name}} Shuffle service in the following cases:

- For complex multi-stage computations. For example, when joining multiple large tables, each exceeding 10M rows in size. If, in addition to complex computations, there are also simple ones (such as simple aggregations or joining a large table with a small one) performed on the cluster, we still recommend enabling the Shuffle service.

- When [launching tasks directly in {{product-name}}](../../../../user-guide/data-processing/spyt/launch.md#submit). The Shuffle service writes intermediate data to {{product-name}} chunks, ensuring its survival in case of Spark executor failures or preemptions. This approach prevents re‑computation of stages that have already been computed but were stored on lost executors.

## Configuration parameters { #configuration }

All parameters related to configuring the {{product-name}} Shuffle service are described on the [configuration parameters page](../../../../user-guide/data-processing/spyt/thesaurus/configuration.md#shuffle).