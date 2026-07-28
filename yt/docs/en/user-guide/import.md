# Importing data from Hive, S3, MongoDB, and other systems

Data is imported from external systems to {{product-name}} with SPYT.

This page contains instructions for importing data using the [import.py](https://github.com/ytsaurus/ytsaurus/blob/main/connectors/import.py) script. With this script, you can import data from Hive, Hadoop, S3, and database management systems that support the JDBC protocol. To import data from other systems not supported by `import.py` — such as MongoDB — you can use SPYT directly, reading from the external system via the corresponding Spark Data Source.


## Configuring connection via Kerberos

To connect to Hadoop, you can use the Kerberos network authentication protocol. You can set up Kerberos authentication by taking the following steps:

1. Create a token on the Hadoop side using the following command:
```bash
$ fetchdt --renewer null /tmp/token
```
2. Copy the token to Cypress:
```bash
$ cat /tmp/token | yt write-file //home/spark/conf/token
```
3. When you start the application, specify the path to the token in Cypress by using the `--files` option

#### Client mode

In `--deploy-mode client` mode, you need to specify the path to a local
file with the token for the driver, since its process runs outside the {{product-name}} cluster.
For executors, specify `HADOOP_TOKEN_FILE_LOCATION=token````bash
```bash
$ spark-submit \
  --master ytsaurus:// \
  --deploy-mode client \
  --files yt://home/spark/conf/token \
  --conf spark.executorEnv.HADOOP_TOKEN_FILE_LOCATION=/tmp/token \
 ./script.py
```
#### Cluster mode

In `--deploy-mode cluster` mode, the path to the file with the token should be specified in the `spark.ytsaurus.config.global.path` configuration parameter.

You will also need to add the `kdc` and `realm` parameters for the driver. Example command:

```bash
$ spark-submit \
  --master ytsaurus:// \
  --deploy-mode cluster \
  --files yt://home/spark/conf/token \
  --conf spark.ytsaurus.config.global.path=//home/spark/conf/global \
  --conf spark.driver.extraJavaOption="-Djava.security.krb5.kdc=... -Djava.secutirykrb5.realm=..."  \
 ./script.py
```

## Installing dependencies { #dependencies }

jar dependencies for interacting with Hive are provided in the `pyspark` Python package. All of the packages that SPYT requires (including `pyspark`) must be installed on the system calling the import.

To read data from a database system that supports JDBC, download the JDBC driver for that system.

`connectors/pom.xml` is a maven config that includes JDBC drivers for MySQL and PostgreSQL as dependencies. To download these drivers, run the command:

```bash
~/yt/connectors$ mvn dependency:copy-dependencies
```

`mvn` will download jar files with JDBC drivers for PostgreSQL and MySQL to `target/dependency`.

To import data from a different database, add the JDBC driver for that database to `pom.xml` and run `$ mvn dependency:copy-dependencies`

{% note warning "JDBC driver must be visible to the driver process" %}

The JDBC driver must be available not only to executors (via `--jars`), but also on the **driver process** classpath. In `--deploy-mode client` mode, Spark does not automatically pick up the driver from `--jars` on the driver side, and reads fail with `java.sql.SQLException: No suitable driver`. Place the jar in the `jars/` directory of the installed `pyspark` (`$SPARK_HOME/jars/`), or set `export SPARK_DIST_CLASSPATH=/path/to/driver.jar` before running.

{% endnote %}

## Importing data { #how-to-import }

To import data, run the `import.py` script:

```bash
$ ./import.py \
   # Import arguments.
   # ...
```

The arguments must identify:
- Data source.
- Path to the imported data.
- Path within {{product-name}} where the imported data should be written.

You can find the complete list of arguments at the end of this page, under [Arguments](#script-options). Below are examples showing how to import data from various systems.

### Client and cluster modes { #run-modes }

`import.py` can be run in two modes:

- **Client (default)** — `python import.py ...`: The Spark driver starts locally on the machine where the script runs, while executors run in the {{product-name}} cluster.
- **Cluster** — `import.py` runs as a Spark application via `spark-submit ... --deploy-mode cluster`, and the driver also starts inside the {{product-name}} cluster. The mode is determined automatically (based on whether the script is started via `spark-submit`); no separate flag is needed:

  ```bash
  $ spark-submit \
      --master ytsaurus://<proxy> \
      --deploy-mode cluster \
      --jars yt:///path/to/jars/driver.jar \
      import.py \
      --jdbc postgresql --jdbc-server pg_host:5432 --jdbc-user user --jdbc-password '<password>' \
      --input jdbc:database_name.table_name \
      --output //path/in/yt/table
  ```

Cluster mode is convenient when submitting from an **external machine** (for example, outside the Kubernetes cluster where {{product-name}} is deployed): the driver goes inside the cluster, and no network connectivity between the driver and executors is required from the external machine.

#### Network configuration inside Kubernetes { #cluster-proxy }

If you submit from an external machine and {{product-name}} is deployed in Kubernetes, only the external proxy is typically available from the outside, while intra-cluster traffic (driver and executors to {{product-name}}) must go through an internal proxy. To enable this, set the `spark.hadoop.yt.clusterProxy` parameter to the address of the proxy accessible from inside the cluster; at the same time, `--master` specifies the external address used for submission:

```bash
$ spark-submit \
    --master ytsaurus://<external proxy> \
    --deploy-mode cluster \
    --conf spark.hadoop.yt.clusterProxy=<internal proxy in k8s> \
    import.py ...
```

In client mode, pass the same parameter via `--extra-conf spark.hadoop.yt.clusterProxy=...`.

### Hive

To import data from Hive:

1. Upload the `hadoop-aws-*.jar` and `aws-java-sdk-bundle-*.jar` files from your local directory `target/dependency` to Cypress.
2. Run the data import command:
   ```bash
   $ ./import.py \
     --metastore master_host:9083 \
     --warehouse-dir /path/to/hive/warehouse \
     --input hive:database_name.table_name \
     --output //path/in/yt/table \
     --proxy https://ytsaurus.company.net \
     --num-executors 5 \
     --executor-memory 4G \
     --executor-memory-overhead 2G \
     --jars yt:///path/to/jars/aws-java-sdk-bundle-1.11.901.jar \
            yt:///path/to/jars/hadoop-aws-3.3.1.jar
   ```
   [Go to argument descriptions](#script-options).

Alternatively, provide an SQL query to be executed by Hive using the `hive_sql` input specifier. Query results will be stored in YT.

```bash
$ ./import.py \
    ...
    --input hive_sql:database_name:SELECT * FROM action_log WHERE action_date > '2023-01-01' \
    ...
```

### HDFS

To import files from HDFS, use a specifier with the file's format and the address of the HDFS NameNode:

```bash
$ ./import.py \
    ...
    --input text:hdfs://namenode/path/to/text/file
    ...
```

`import.py` supports the `text`, `parquet`, and `orc` file formats.

### Database systems with JDBC support

To import data from JDBC-compatible systems, such as PostgreSQL, run the following command:
```bash
$ ./import.py \
    --jdbc postgresql \
    --jdbc-server pg_host:5432 \
    --jdbc-user user \
    --jdbc-password '' \  # Get password from terminal prompt
    --input jdbc:database_name.table_name \
    --output //path/in/yt/table
```
[Go to argument descriptions](#script-options).

To import the results of an SQL statement, use the `jdbc_sql` input specifier:

```bash
$ ./import.py \
    ...
    --input jdbc_sql:database_name:SELECT * FROM users WHERE signup_date > '2023-01-01' \
    ...
```

#### Parallel reading of large tables

By default, `import.py` reads a JDBC source in a single thread (with a single query). For large tables, this is slow and can lead to long-running query errors on the DBMS side (for example, `ORA-01555: snapshot too old` in Oracle). To read a table in parallel, specify a numeric or date partition column and its range:

```bash
$ ./import.py \
    --jdbc postgresql \
    --jdbc-server pg_host:5432 \
    --jdbc-user user \
    --jdbc-password '' \
    --input jdbc:database_name.table_name \
    --jdbc-partition-column id \
    --jdbc-lower-bound 1 \
    --jdbc-upper-bound 1000000 \
    --jdbc-num-partitions 16 \
    --output //path/in/yt/table
```

SPYT splits the read into `--jdbc-num-partitions` ranges of the form `column >= a AND column < b` and reads them in parallel. The `--jdbc-lower-bound`/`--jdbc-upper-bound` bounds only define the split step — they do not filter the data (values outside the bounds land in the edge partitions). The column type must be numeric or date; preferably indexed or a table partitioning key, so that each query reads only its own portion.

Partitioning options are global — they apply **identically to all** tables listed in `--input`. If you import multiple tables in a single run, the partition column and bounds must be suitable for each table; otherwise, run `import.py` separately for each table.

### S3

To import data from S3:

1. Upload the `hadoop-aws-*.jar` and `aws-java-sdk-bundle-*.jar` files from your local directory `target/dependency` to Cypress.
2. Run the data import command:

   ```bash
   ./import.py \
    --input parquet:s3a://bucket/path/to/data/sample-parquet \
    --output //home/tables/import_from_s3 \
    --s3-access-key <S3 Access Key> \
    --s3-secret-key <S3 Secret Key> \
    --s3-endpoint <S3 endpoint> \
    --proxy https://ytsaurus.company.net \
    --num-executors 5 \
    --executor-memory 4G \
    --executor-memory-overhead 2G \
    --jars yt:///path/to/jars/aws-java-sdk-bundle-1.11.901.jar \
         yt:///path/to/jars/hadoop-aws-3.3.1.jar
   ```
   The arguments are described below.

## Arguments { #script-options }

`import.py` supports the following arguments:

| **Argument** | **Description** |
| ----------| --------- |
| `--num-executors` | Number of executors for an import operation (1 by default). |
| `--cores-per-executor` | Number of reserved CPU cores per executor (1 by default). |
| `--ram-per-core` | Amount of RAM reserved, per core (2 GB by default). |
| `--jdbc` | Type of JDBC driver. For example, `mysql` or `postgresql`. |
| `--jdbc-server` | Database server host:port. |
| `--jdbc-user` | Username to log in to the database. |
| `--jdbc-password` | Password to log in to the database. If empty, read from terminal. |
| `--jdbc-partition-column` | Numeric or date column for parallel reading of the source (Spark `partitionColumn`). Requires `--jdbc-lower-bound`, `--jdbc-upper-bound`, `--jdbc-num-partitions`. |
| `--jdbc-lower-bound` | Lower bound of the `--jdbc-partition-column` range. |
| `--jdbc-upper-bound` | Upper bound of the `--jdbc-partition-column` range. |
| `--jdbc-num-partitions` | Number of parallel partitions (JDBC connections) for reading. |
| `--jars` | Additional jar libraries. By default, `target/dependency/jar/*.jar` |
| `--input` | Object to import, may be specified multiple times. |
| `--output` | Path to write to in {{product-name}}. For every `--input` flag, one output must be provided. |

To configure the SPYT cluster started as part of an import operation, use the following arguments:

| **Argument** | **Description** |
| ----------| --------- |
| `--proxy` | Path to the proxy in the {{product-name}} cluster where SPYT should run. |
| `--pool` | Resource pool in {{product-name}} to run SPYT in. |
| `--executor-timeout` | Idle timeout for Spark executors. |
| `--executor-tmpfs-limit` | Size of tmpfs partition for Spark executors. |
| `--executor-memory` | Amount of RAM to allocate to each executor. |
| `--executor-memory-overhead` | Amount of additional RAM to allocate to executors beyond the primary amount. For example, if the main memory is 4 GB and the additional memory is 2 GB, then the total amount of memory requested from the cluster will be 6 GB. |

If you're importing data from S3:

| **Argument** | **Description** |
| ----------| --------- |
| `--s3-access-key` | Access Key ID identifying the user or app in S3. |
| `--s3-secret-key` | Secret Access Key associated with the access key. |
| `--s3-endpoint` | Endpoint URL of the S3 storage. |

The following input specifiers are supported:

| **Specifier** | **Description** |
| ----------| --------- |
| `hive` | Table in Hive, in `db_name.table_name` format. |
| `hive_sql` | SQL query to run in Hive, in `db_name:sql statement` format. |
| `jdbc` | Table in JDBC database, in `db_name.table_name` format. |
| `jdbc_sql` | SQL query for JDBC database, in `db_name:sql statement` format. |
| `text` | Text file in HDFS. |
| `parquet` | Parquet file in HDFS. |
| `orc` | ORC file in HDFS. |

When writing a table to {{product-name}}, the default assumption is that the table doesn't already exist there. If the table does exist, you can overwrite or append it using the `overwrite` or `append` specifiers. For example: `--output overwrite:/path/to/yt`.

## Type conversions

Importing complex types is only supported partially. The {{product-name}} type system doesn't exactly match its counterparts in other storage systems. When importing data, SPYT will try to keep the type on a best-effort basis. However, the value may get converted to a string when no matching type in {{product-name}} could be inferred. When necessary, use SQL for proper type conversion.

Value ranges for a single type may be different for {{product-name}} and other systems. For example, the {{product-name}} `date` type only stores calendar dates starting with the Unix epoch, January 1, 1970. An attempt to write earlier dates in {{product-name}} will cause a runtime error. It is still possible to store earlier dates in {{product-name}} as strings (for example, applying a `to_char(date_value, 'YYYY-MM-DD')` in PostgreSQL), or as integers (`date_value - '1970-01-01'` in PostgreSQL).
