# Configurations

For a listing of all the options supported by Spark, see the [documentation](https://spark.apache.org/docs/latest/configuration.html).

## Basic options { #main }

Most options are available starting with version 1.23.0, while `spark.yt.write.writingTypeV3.enabled` is available as of version 1.24.0.

- `spark.yt.write.batchSize`: `500,000` by default, the amount of data transferred in a single `WriteTable` transaction.
- `spark.yt.write.miniBatchSize`: `1000` by default, the amount of data transmitted for writing to {{product-name}}.
- `spark.yt.write.timeout`: `60 seconds` by default, the timeout to write a single mini batch.
- `spark.yt.write.writingTypeV3.enabled`: `false` by default, writing of tables with schemas in [type_v3](../../../../../user-guide/storage/data-types.md) format, `type_v1` by default.
- `spark.yt.read.vectorized.capacity`: `1000` by default, the maximum number of rows in a batch for reading via `wire protocol ()`.
- `spark.yt.read.arrow.enabled`: `true` by default, reading to be performed in batches whenever possible.
- `spark.yt.read.parsingTypeV3.enabled`: `false` by default, reading of tables with schema in [type_v3](../../../../../user-guide/storage/data-types.md) format, `type_v1` by default.
- `spark.yt.read.keyColumnsFilterPushdown.enabled`: `false` or `Predicate pushdown` by default. Using Spark query filters to read from {{product-name}} optimizes the amount of data returned by {{product-name}} reducing the read delay accordingly. The range of the rows required is added when generating the path.
- `spark.yt.read.keyColumnsFilterPushdown.union.enabled`: `false` by default, in filter pushdown, all are merged into one, and a continuous range of rows is requested from a table.
- `spark.yt.read.keyColumnsFilterPushdown.ytPathCount.limit`: `100` be default, the maximum number of ranges a Spark read query is broken into.
- `spark.yt.transaction.timeout`: `5 minutes` by default, write operation transaction timeout.
- `spark.yt.transaction.pingInterval`: `30 seconds` by default.
- `spark.yt.globalTransaction.enabled`: `false` by default, the use of a [global transaction](../../../../../user-guide/data-processing/spyt/read-transaction.md).
- `spark.yt.globalTransaction.id`: `None` by default, id of resulting global transaction.
- `spark.yt.globalTransaction.timeout`: `2 minutes` by default, global transaction timeout.
- `spark.hadoop.yt.user`: first available, by default: `YT_SECURE_VAULT_YT_USER` or `YT_USER` environment variables, `user.name` from the system. properties, {{product-name}} user.
- `spark.hadoop.yt.user`: first available, by default: `YT_SECURE_VAULT_YT_TOKEN` or `YT_TOKEN` environment variables, contents of `~/.yt/token`, token for {{product-name}}.
- `spark.hadoop.yt.timeout`: `300 seconds` by default, {{product-name}} read timeout.

## Additional options { #add }

Additional options are passed in via `--params`:

```bash
spark-launch-yt \
  --proxy <cluster-name> \
  --discovery-path my_discovery_path \
  --params '{"spark_conf"={"spark.yt.jarCaching"="True";};"layer_paths"=["//.../ubuntu_xenial_app_lastest.tar.gz";...;];"operation_spec"={"max_failed_job_count"=100;};}' \
  --spark-cluster-version '1.36.0'
```

When using `spark-submit-yt` to configure a task, `spark_conf_args` is available as an option:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path my_discovery_path \
  --deploy-mode cluster \
  --conf spark.sql.shuffle.partitions=1 \
  --conf spark.cores.max=1 \
  --conf spark.executor.cores=1 \
  yt:///sys/spark/examples/grouping_example.py
```
```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path my_discovery_path \
  --deploy-mode cluster \
  --spark_conf_args '{"spark.sql.shuffle.partitions":1,"spark.cores.max":1,"spark.executor.cores"=1}' \
  yt:///sys/spark/examples/grouping_example.py
```

When launching programmatically, you can configure via `spark_session.conf.set("...", "...")`.

Python example:

```python
from spyt import spark_session

print("Hello world")
with spark_session() as spark:
    spark.conf.set("spark.yt.read.parsingTypeV3.enabled", "true")
    spark.read.yt("//sys/spark/examples/test_data").show()
```

Java example:

```java
protected void doRun(String[] args, SparkSession spark, CompoundClient yt) {
    spark.conf.set("spark.sql.adaptive.enabled", "false");
    spark.read().format("yt").load("/sys/spark/examples/test_data").show();
}
```


