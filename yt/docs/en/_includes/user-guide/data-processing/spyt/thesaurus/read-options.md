
# Read options

## Multi-cluster reads { #multicluster_reading }

By default, the computation process accesses data on the cluster that provides the computational resources (when running directly or when using a standalone cluster). SPYT runs its own RPC proxies to offload shared cluster proxies.

Version 2.2.0 introduced the option to read data from different {{product-name}} clusters. To do this, you must explicitly indicate the cluster address in the table path.

```python
spark.read.yt('<cluster="localhost:8000">//home/table').show() # Table on cluster localhost:8000

spark.read.yt('//home/table').show() # Table on home cluster
```

{% note info "Note" %}

Data is read from other {{product-name}} clusters through shared proxies, which may put a heavy strain on them when the volume of data is high.

{% endnote %}

## schema_hint { #schema_hint }

Hard-coded [column type](../../../../../user-guide/storage/data-types.md). Useful when a column is of type `any` (a composite data type serialized as `yson`).
The value will be deserialized as the specified type.

Python example:
```python
spark.read.schema_hint({"value": MapType(StringType(), LongType())}).yt("//sys/spark/examples/example_yson")
```

Scala example:
```scala
df.write
    .schemaHint(Map("a" ->
        YtLogicalType.VariantOverTuple(Seq(
          (YtLogicalType.String, Metadata.empty), (YtLogicalType.Double, Metadata.empty)))))
    .yt(tmpPath)
```

## transaction { #transaction }

Reading from a [transaction](../../../../../user-guide/storage/transactions.md). For more details, see [Reading and writing within a transaction](../../../../../user-guide/data-processing/spyt/read-transaction.md).

Scala example:

```scala
val transaction = YtWrapper.createTransaction(None, 10 minute)
df.write.transaction(transaction.getId.toString).yt(tmpPath)
transaction.commit().get(10, TimeUnit.SECONDS)
```

## Schema v3 { #type_v3 }

Read tables with schema in [type_v3](../../../../../user-guide/storage/data-types.md) instead of type_v1. It can be enabled in [Spark configuration](../../../../../user-guide/data-processing/spyt/cluster/configuration.md) or write option.

Python example:
```python
spark.read.option("parsing_type_v3", "true").yt("//sys/spark/examples/example_yson")
```

## readParallelism { #readParallelism }

Setting the target number of partitions when reading a table. Determines the maximum partition size as `totalBytes / readParallelism`, thereby setting the number of Spark tasks during reading.

Python example:
```python
spark.read.option("readParallelism", "5").yt("//home/table")
```

## recursiveFileLookup { #recursiveFileLookup }

This option controls how the file structure is traversed when reading data. It determines whether the system should recursively descend into nested directories.

The option can take the following values:

- `true`: The system recursively traverses all subdirectories starting from the specified point and finds all files in the tree.
- `false`: The system does not descend recursively into subdirectories and reads only the files in the specified directory.

In Apache Spark, when reading data from HDFS and S3, the `recursiveFileLookup` option is `false` by default. In SPYT, the default value is set to `true`. This ensures consistency with the behavior of YQL and CHYT.

If you're reading data with Hive‑compatible directory partitioning in Cypress, set the option to `false`. In this case, it's sufficient to process only the top-level files, and recursive traversal isn't required.

For example, for this directory structure:

```text
home
├── table
│   ├── dt=2026-01-01
│   ├── dt=2026-01-02
│   ├── dt=2026-01-03
│   └── dt=2026-01-04
│   ...
```

Python code example:

```python
df = spark.read.option("recursiveFileLookup", "false").yt("//home/table")
df.printSchema()
...
root
 |-- id: long (nullable = false)
 |-- value: long (nullable = false)
 ...
 |-- dt: string (nullable = false)
```
