# Data transfer with SPYT

`import.py` transfers data between YTsaurus tables and external storage through
Spark. It works with a direct SPYT session (`python import.py --proxy ...`) or
with a session launched by `spark-submit`.

Supply one `--output` per `--input`. Pairs are processed in order within one
Spark session; a single invocation can contain imports and exports. All pairs
are validated before starting Spark. Transfers are not atomic across pairs.

## Sources and destinations

Inputs use `<format>:<path>`. Outputs use `[<mode>:][<format>:]<path>`, where
the optional mode is `overwrite` or `append`. Without a mode, Spark's default
behavior is to fail if the destination already exists.

| Format | Input | Output |
| --- | --- | --- |
| `yt` | YTsaurus table, e.g. `yt://home/user/table` | YTsaurus table |
| `parquet`, `orc`, `json`, `csv`, `text` | Files on a Spark-accessible filesystem | Files on a Spark-accessible filesystem |
| `hive` | Hive table name | — |
| `hive_sql` | `<database>:<SQL query>` | — |
| `jdbc` | `<database>.<table>` | — |
| `jdbc_sql` | `<database>:<SQL query>` | — |
| `local_parquet` | Local Parquet loaded through pandas/pyarrow on the driver | — |

Existing YT destinations such as `//home/user/table`,
`overwrite://home/user/table` and `append://home/user/table` remain supported.
An external destination requires a format prefix, e.g.
`parquet:s3a://bucket/path`; the URI alone does not specify a file format.
URI colons and any colons within the destination path are preserved.

File reads and writes use Spark's default format options. CSV reads do not infer
types or read a header by default. Text output requires a single string column.
When exporting to files, top-level YT `uint64` columns are written as
`decimal(20,0)` so every unsigned value is preserved. Nested `uint64` values
inside structs, arrays or maps are not converted; the chosen file writer must
support any other YT-specific types in the source schema. File outputs are
directories containing partition files, rather than a single named file.
Executors must be able to reach the chosen filesystem; local file paths refer
to executor filesystems rather than downloading data to the invoking machine.

Hive sources require `--metastore` and `--warehouse-dir`. JDBC sources require
`--jdbc` (driver type) and `--jdbc-server`; authentication and partitioned reads
are configured with the other `--jdbc-*` flags shown by `--help`.

## Download the JAR dependencies

Install Maven, then run this from the `yt/connectors` directory (the directory
containing `pom.xml`):

```sh
mvn -f pom.xml dependency:copy-dependencies
```

Maven downloads the dependencies declared in `pom.xml` and their transitive
dependencies into `target/dependency/`. The POM includes the PostgreSQL and
MySQL JDBC drivers plus `hadoop-aws` for S3A access. Pass the downloaded JARs
to the transfer script with `--jars 'target/dependency/*.jar'`, as in the
examples below. Keep the pattern quoted so the script can expand it when
building the Spark configuration.

The `pom.xml` currently specifies `hadoop-aws` 3.3.1. Ensure its Hadoop version
matches the Spark installation before using those JARs. See the
[Hadoop S3A documentation](https://hadoop.apache.org/docs/r3.3.1/hadoop-aws/tools/hadoop-aws/index.html)
for dependency and authentication requirements.

## Export to S3

```sh
python import.py --proxy <yt-proxy> \
  --input yt://home/user/source \
  --output parquet:s3a://my-bucket/export/run-1 \
  --s3-endpoint <s3-endpoint> \
  --jars 'target/dependency/*.jar'
```

Use `overwrite:parquet:s3a://my-bucket/export/run-1` to replace an existing
dataset or `append:parquet:s3a://my-bucket/export/run-1` to add partition files.
The same output syntax works for other supported file formats and filesystem
URIs, such as `orc:hdfs://namenode/path`.

The existing `--s3-access-key`, `--s3-secret-key` and `--s3-endpoint` flags apply
to both directions. Credentials can also come from Hadoop's configured S3A
credential providers. Additional Hadoop settings can be passed through
`--extra-conf`, e.g. `spark.hadoop.fs.s3a.path.style.access=true` for an endpoint
that requires path-style requests.

The Spark driver and executors need `hadoop-aws` and its compatible AWS SDK
dependencies to access S3A.

## Import from S3

```sh
python import.py --proxy <yt-proxy> \
  --input parquet:s3a://my-bucket/export/run-1 \
  --output //home/user/imported \
  --s3-endpoint <s3-endpoint> \
  --jars 'target/dependency/*.jar'
```

The underlying file writer is Spark's
[`DataFrameWriter.save`](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.save.html).

## Local checks

Run the local Spark regression tests in a Python environment with SPYT and
PySpark installed:

```sh
python3 -m unittest discover -s tests -v
```

These tests do not connect to YTsaurus or S3. They skip when SPYT or PySpark is
unavailable. A live transfer also depends on
the deployed SPYT/Spark versions, source schema, credentials, filesystem jars
and executor network access.
