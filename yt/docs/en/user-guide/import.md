# Importing data

Importing data into {{product-name}} from external systems is performed with SPYT.

This document contains instructions for using the `import.py` script. This script imports data from Hadoop, Hive, or from database systems that support the JDBC protocol.

To import from other systems not supported by `import.py` (for example, from S3 or MongoDB), you can directly use SPYT, reading from the external system via the corresponding Spark Data Source.


## Starting SPYT

To start a SPYT cluster, follow the [instruction](./data-processing/spyt/launch.md).
Alternatively, `import.py` can launching the cluster for you before starting the import operation. To start a SPYT cluster using `import.py`, provide the following command line flags:

```bash
$ ./import.py \
    --start-spyt true \
    --discovery_path //path/to/discovery \
    --proxy yt_proxy_host:port
```
If `--start-spyt` flag was not set, `import.py` will expect SPYT cluster to be already up an running.

## Dependencies

Logic for reading data from external systems into Spark is implemented in external libraries, packaged as jar-files. jar-dependencies for interating with Hive are provided together with the `pyspark` Python package. `pyspark` is also required for interacting with SPYT.

To read data from a JDBC-supporting data, one has to download a JDBC-driver specific for that system.

`connectors/pom.xml` is a maven config that includes JDBC drivers for MySQL and PostgreSQL as dependencies. To download these drivers, run the commandline below:

```bash
~/yt/connectors$ mvn dependency:copy-dependency
```

`mvn` will download jar files with JDBC drivers for PostgreSQL and MySQL into `target/dependency`
To import from a different database, add the JDBC driver for that database into `pom.xml` and run `$ mvn dependency:copy-dependencies`

## import.py

When launching `import.py`, provide the SPYT discovery path as a commandline argument.

```bash
$ ./import.py --discovery_path //path/to/discovery \
    ... # all other options
```

Additional commandline arguments should identify the data source, path to imported data within that data source, and path in {{product-name}} where imported data should be stored.

To import from Hive, run:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    --metastore master_host:9083 \
    --warehouse-dir /path/to/hive/warehouse \
    --input hive:database_name.table_name \
    --output //path/in/yt/table
```

Alternatively, provide an SQL query to be executed by Hive, using `hive_sql` input specifier. Query results will be stored in YT.
```bash
$ ./import.py --discovery_path //path/to/discovery \
    ...
    --input hive_sql:database_name:SELECT * FROM action_log WHERE action_date > '2023-01-01' \
    ...
```

To import from a database using JDBC protocol (for example, from PostgreSQL), run:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    --jdbc postgresql \
    --jdbc-server pg_host:5432 \
    --jdbc-user user \
    --jdbc-password '' \  # Получить пароль из консольного ввода
    --input jdbc:database_name.table_name \
    --output //path/in/yt/table
```

To import results of a SQL statement, use `jdbc_sql` input specifier:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    ...
    --input jdbc_sql:database_name:SELECT * FROM users WHERE signup_date > '2023-01-01' \
    ...
```

To import a file from HDFS, use an input specifier indicating file format. Path to the file must include HDFS NameNode address:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    ...
    --input text:hdfs://namenode/path/to/text/file
    ...
```

`text`, `parquet` and `orc` file formats are supported.

## Command line arguments

import.py supports the following arguments:

| **Argument** | Description |
| ----------| --------- |
| `--discovery-path` | required - path that identifies the SPYT cluster |
| `--num-executors` | number of workers for distributed operations (1 by default) |
| `--cores-per-executor` | number of reserved CPU cores per worker (1 by default) |
| `--ram-per-core` | amount of RAM reserved, per core (2GB by default) |
| `--jdbc` | Type of JDBC driver. For example, `mysql` or `postgresql` |
| `--jdbc-server` | database host:port |
| `--jdbc-user` | user name to login to the database |
| `--jdbc-password` | password to login to the database. If empty, read from terminal |
| `--jars` | additional jar-dependencies. By default, `target/dependency/jar/*.jar` |
| `--input` | path to import. this flag may appear multiple times |
| `--output` | path to write in {{product-name}}. For every `--input` flag, one `--output` must be provided |

To configure the SPYT cluster started as part of import operation, use the following arguments:

| **Argument** | Description |
| ----------| --------- |
| `--start-spyt` | Instructs `import.py` to start a SPYT cluster |
| `--proxy` | Path to {{product-name}} proxy for the {{product-name}} cluster where SPYT should run |
| `--pool` | Resource pool in {{product-name}} to run SPYT in |
| `--spark-cluster-version` | SPYT version |
| `--executor-timeout` | Timeout for idle Spark workers |
| `--executor-tmpfs-limit` | Size of tmpfs partition for Spark workers |

The following input specifiers are supported:

| **Specifier** | Description |
| ----------| --------- |
| `hive` | table name in Hive with database name, in `db_name.table_name` format |
| `hive_sql` | SQL query to run in hive Hive, in `db_name:sql statement` format |
| `jdbc` | table in a JDBC-database, in `db_name.table_name` format |
| `jdbc_sql` | SQL query for a JDBC-database, in `db_name:sql statement' format |
| `text` | text file in HDFS |
| `parquet` | parquet file in HDFS |
| `orc` | orc file in HDFS |

## Type conversions

Importing complex types is only supported partially. The type system of {{product-name}} does not exactly match its counterparts in other storage systems. When importing data, SPYT will try to keep the type on a best effort basis. However, the value may get converted to a string when no matching type in {{product-name}} could be inferred. When necessary, use SQL to properly convert types.

Ranges of values that correspond for a single type may differ between {{product-name}} and other systems. For example, {{product-name}} `date` type only stores calendar dates starting with the Unix epoch, January 1st 1970. Attempt to write an earlier data will cause a runtime error. It is still possible to store earlier dates as strings (for example, applying a `to_char(date_value, 'YYY-MM-DD')` conversion in PostgreSQL), or as integers (`date_value - '1970-01-01'` in PostgreSQL).
