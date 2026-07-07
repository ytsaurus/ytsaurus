# Apache Flink Connector {{product-name}}

Apache Flink Connector {{product-name}} is a connector for streaming and batch data processing on Apache Flink. It works with [sorted dynamic tables](../../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) in {{product-name}} and supports writes, reading bounded streams, and Lookup operations.

The connector source code is available on [GitHub](https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-connector-ytsaurus).

## Features {#features}

- writing to {{product-name}} dynamic tables — supports streaming writes; the basic scenario is covered in [Quick Start](#quick-start-guide);
- automatic table creation — creates tables automatically before writing if they do not exist;
- advanced table resharding — provides configurable resharding strategies for optimal performance. For details, see [Table Resharding](#table-resharding);
- data partitioning — supports multiple partitioning granularities: hour, day, week, month, year. For details, see [Data Partitioning](#data-partitioning);
- synchronous and asynchronous Lookup operations — supports both execution modes for Lookup operations on {{product-name}} dynamic tables. For details, see [Lookup Operations](#lookup-operations);
- Lookup caching — supports `FULL` and `PARTIAL` cache strategies to optimize performance. For details, see [Lookup Operations](#lookup-operations);
- multi-cluster Lookup support — lets you run Lookup operations against multiple {{product-name}} clusters depending on availability. See an example in [Examples](#examples);
- trackable fields — lets you track field values through metrics.

## Installation {#installation}

{% note info %}

The current version of the connector requires:
- Java 11
- Apache Flink {{flink-version}}

{% endnote %}

Replace `connectorVersion` with the latest version from [Maven Central](https://central.sonatype.com/artifact/tech.ytsaurus.flyt.connectors.ytsaurus/flink-connector-ytsaurus).

{% list tabs %}

- Maven

  ```xml
  <dependency>
      <groupId>tech.ytsaurus.flyt.connectors.ytsaurus</groupId>
      <artifactId>flink-connector-ytsaurus</artifactId>
      <version>${connectorVersion}</version>
      <classifier>all</classifier>
  </dependency>
  ```

- Gradle

  ```kotlin
  implementation("tech.ytsaurus.flyt.connectors.ytsaurus:flink-connector-ytsaurus:$connectorVersion:all")
  ```

{% endlist %}

## Quick Start {#quick-start-guide}

### 1. Install a {{product-name}} cluster {#quick-start-install-ytsaurus}

You can skip this step if you already have a cluster configured.

For local development and testing, follow the official documentation to {% if audience == "public" %}[install the {{product-name}} cluster via Kind](../../../../overview/try-yt.md){% else %}[install the {{product-name}} cluster via Kind](../../../../quickstart.md){% endif %}. For production deployments, follow the [{{product-name}} Admin Guide](../../../../admin-guide/install-ytsaurus.md).

{% note info %}

`flink-connector-ytsaurus` uses the [Java {{product-name}} client](../../../../api/java/examples.md). Before you start, verify which proxy address you need to specify in the `proxy` parameter for your environment.

{% endnote %}

### 2. Install an Apache Flink cluster {#quick-start-install-flink}

Install an Apache Flink cluster using the [official documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/local_installation/#downloading-flink). The connector requires Apache Flink version `{{flink-version}}`.

### 3. Install Flink Connector {{product-name}} in the Apache Flink cluster {#quick-start-install-connector}

Build the connector from source (see [Building from Source](https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-connector-ytsaurus#build-steps)) or download it from the [Maven repository](https://central.sonatype.com/artifact/tech.ytsaurus.flyt.connectors.ytsaurus/flink-connector-ytsaurus). After you build or download the connector, place the resulting JAR file in `${FLINK_ROOT}/lib`.

### 4. Change the Flink Web UI port {#quick-start-change-flink-port}

Open the `conf/config.yaml` file and change the `rest.port` parameter from `8081` to `8083` to avoid port conflicts with {{product-name}}.

### 5. Start the Apache Flink cluster with Flink SQL Client {#quick-start-start-flink}

Start the Apache Flink cluster:
```bash
./bin/start-cluster.sh
```

Start Flink SQL Client:
```bash
./bin/sql-client.sh
```

### 6. Run the demo job {#quick-start-run-demo-job}

1. Create a Datagen source:

   ```sql
   CREATE TABLE simple_datagen_source (
       id BIGINT,
       name STRING,
       age INT,
       salary DOUBLE,
       is_active BOOLEAN,
       created_at TIMESTAMP(3)
   ) WITH (
       'connector' = 'datagen',
       'rows-per-second' = '50',
       'number-of-rows' = '1000',
       'fields.id.kind' = 'sequence',
       'fields.id.start' = '1',
       'fields.id.end' = '100000',
       'fields.name.length' = '20',
       'fields.age.min' = '22',
       'fields.age.max' = '65',
       'fields.salary.min' = '30000.0',
       'fields.salary.max' = '150000.0'
   );
   ```

2. Create a {{product-name}} sink:

   ```sql
   CREATE TABLE ytsaurus_simple_sink (
       id BIGINT,
       name STRING,
       age INT,
       salary DOUBLE,
       is_active BOOLEAN,
       created_at TIMESTAMP(3),
       updated_at TIMESTAMP(3),
       PRIMARY KEY (id) NOT ENFORCED
   ) WITH (
       'connector' = 'ytsaurus',
       'proxy' = 'localhost:8081',
       'path' = '//tmp/flink_simple_test_table',
       'credentials-source' = 'options',
       'username' = 'admin',
       'token' = 'password',
       'schema' = '[
           {"name"="id";"type"="int64";"required"=%false;"sort_order"="ascending"};
           {"name"="name";"type"="string";"required"=%false};
           {"name"="age";"type"="int64";"required"=%false};
           {"name"="salary";"type"="double";"required"=%false};
           {"name"="is_active";"type"="boolean";"required"=%false};
           {"name"="created_at";"type"="string";"required"=%false};
           {"name"="updated_at";"type"="string";"required"=%false}
       ]'
   );
   ```

3. Run a job that writes the generated data to a {{product-name}} dynamic table:

   ```sql
   INSERT INTO ytsaurus_simple_sink
   SELECT
       id,
       name,
       age,
       salary,
       is_active,
       created_at,
       CURRENT_TIMESTAMP AS updated_at
   FROM simple_datagen_source;
   ```

4. Monitor job progress at [localhost:8083](http://localhost:8083).

   ![](../../../../../images/flyt-connector-simple-sink-job-ui.png)

5. The table `flink_simple_test_table` will be created in the `/tmp/flink_simple_test_table` directory and will contain the results of the Flink job.

   ![](../../../../../images/flyt-connector-simple-sink-result-ui.png)

Congratulations! You've launched your first job with {{product-name}} and Apache Flink.

## Configuration Options {#configuration-options}

The {{product-name}} connector supports a wide range of configuration options.

The options below are grouped by purpose. For most write and Lookup scenarios, specify the table path by using `path`. In multi-cluster configurations, use `path-map` instead of `path`.

This section describes:

- [Required Options](#required-options);
- [Path Configuration](#path-configuration);
- [Authentication Options](#auth-options);
- [Table Configuration](#table-configuration);
- [Partitioning Options](#partitioning-options);
- [Resharding Options](#resharding-options);
- [Transaction and Performance Options](#transaction-performance-options);
- [Lookup Options](#lookup-options);
- [Cache Options for Lookup Operations](#lookup-cache-options);
- [Other Options](#other-options).

#### Required Options {#required-options}

| Option | Type | Description|
|--------|------|------------|
| `proxy` | String | {{product-name}} proxy address|
| `schema` | String | YSON schema definition for the {{product-name}} table. See [Schema Definition](#schema-definition) |
| `credentials-source` | String | Authentication method (`options`, `env`, `your-custom-provider`) |

#### Path Configuration {#path-configuration}

| Option | Type | Default | Description|
|--------|------|---------|------------|
| `path` | String | - | Path to the {{product-name}} table|
| `path-map` | Map<String, String> | - | Mapping from cluster to table path for multi-cluster lookups |

Specify one of the following options:

- `path` - for a single table in a single cluster;
- `path-map` - for multi-cluster Lookup scenarios.

#### Authentication Options {#auth-options}

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `username` | String | - | {{product-name}} username (when using `options` credentials source) |
| `token` | String | - | {{product-name}} token (when using `options` credentials source) |

#### Table Configuration {#table-configuration}

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `optimize-for` | Enum | - | Table optimization mode (`LOOKUP`, `SCAN`) |
| `primary-medium` | Enum | - | Primary storage medium (`DEFAULT`, `SSD_BLOBS`) |
| `tablet-cell-bundle` | String | - | Tablet cell bundle name |
| `enable-dynamic-store-read` | Boolean | `true` | Enable dynamic store read attribute |
| `custom-attributes` | String | - | Custom table attributes in YSON format |

#### Partitioning Options {#partitioning-options}

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `partition-key` | String | - | Column name to use for partitioning |
| `partition-scale` | Enum | - | Partitioning granularity (`HOUR`, `HOUR_T`, `DAY`, `WEEK`, `MONTH`, `SHORT_MONTH`, `YEAR`, `SHORT_YEAR`) |
| `partition-ttl-day-cnt` | Integer | - | Number of days to keep partitions |
| `partition-ttl-in-days-from-creation` | Integer | - | TTL in days from partition creation |
| `min-partition-ttl` | Integer | `20` | Minimum partition TTL in days |

#### Resharding Options {#resharding-options}

| Option | Type | Default | Description|
|--------|------|---------|------------|
| `reshard.strategy` | Enum | `NONE` | Resharding strategy (`NONE`, `FIXED`, `LAST_PARTITIONS`) |
| `reshard.tablet-count` | Integer | - | Number of tablets for resharding|
| `reshard.uniform` | Boolean | `false` | Use uniform partitioning|
| `reshard.last-partitions-count` | Integer | `7` | Number of partitions to consider in `LAST_PARTITIONS` strategy |

#### Transaction and Performance Options {#transaction-performance-options}

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `commit-transaction-period` | Duration | - | Period for committing transactions |
| `transaction-timeout` | Duration | - | Transaction timeout |
| `transaction-atomicity` | Enum | - | Transaction atomicity level |
| `rows-in-transaction-limit` | Integer | - | Maximum rows per transaction |
| `rows-in-modification-limit` | Integer | - | Maximum rows per modification |
| `retry-strategy` | Enum | `EXPONENTIAL` | Retry strategy (`EXPONENTIAL`, `NO_RETRY`) |

#### Lookup Options {#lookup-options}

| Option | Type | Default | Description                                                                                                                                                           |
|--------|------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `lookup.async` | Boolean | `false` | Enable asynchronous lookup                                                                                                                                            |
| `lookup-method` | Enum | `LOOKUP` | Lookup method (`LOOKUP`, `SELECT`). `LOOKUP` method works only with key columns, but has better performance. `SELECT` method works with any columns, but works slowly. |
| `cluster-pick-strategy` | String | `FirstAvailableClusterPickStrategy` | Strategy for picking clusters in multi-cluster setup. You can choose your own implementation of the strategy.                                                         |

#### Cache Options for Lookup Operations {#lookup-cache-options}

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lookup.cache` | Enum | `NONE` | Cache type (`NONE`, `PARTIAL`, `FULL`) |
| `lookup.partial-cache.max-rows` | Long | - | Maximum rows in partial cache |
| `lookup.partial-cache.expire-after-write` | Duration | - | Cache expiration after write |
| `lookup.partial-cache.expire-after-access` | Duration | - | Cache expiration after access |
| `lookup.partial-cache.cache-missing-key` | Boolean | - | Cache missing keys |
| `lookup.full-cache.reload-strategy` | Enum | - | Full cache reload strategy (`PERIODIC`, `TIMED`) |
| `lookup.full-cache.periodic-reload-interval` | Duration | - | Periodic reload interval |
| `lookup.full-cache.timed-reload-iso-time` | String | - | Timed reload ISO time |

#### Other Options {#other-options}

| Option | Type | Default | Description                  |
|--------|------|---------|------------------------------|
| `trackable-field` | String | - | Field name to track |
| `proxy-role` | String | - | Proxy role |

The `trackable-field` parameter is useful when you need to observe the values of a specific field through connector metrics.

## Schema Definition {#schema-definition}

{{product-name}} tables require a YSON schema definition. Provide the schema as a YSON list of column definitions.

Schema format:

```yson
[
    {"name"="column_name";"type"="data_type";"required"=%false;"sort_order"="ascending"};
    {"name"="another_column";"type"="string";"required"=%false}
]
```

For more information about {{product-name}} schemas, see the [official documentation](../../../../user-guide/storage/static-schema.md).


## Authentication {#authentication}

The connector supports multiple authentication methods:

This section describes:

- [Options-based Authentication](#auth-via-options);
- [Environment-based Authentication](#auth-via-env);
- [Custom Authentication](#custom-auth).

#### Options-based Authentication {#auth-via-options}

Provide credentials directly in the table configuration:

```sql
'credentials-source' = 'options',
'username' = 'your-username',
'token' = 'your-token'
```

#### Environment-based Authentication {#auth-via-env}

Read credentials from environment variables:

```sql
'credentials-source' = 'env'
```

Set the following environment variables:
- `YT_USER` - {{product-name}} username
- `YT_TOKEN` - {{product-name}} token

#### Custom Authentication {#custom-auth}

To create your own authentication method, implement the [`CredentialsProvider`](https://github.com/ytsaurus/ytsaurus-flyt/blob/main/flink-connector-ytsaurus/src/main/java/tech/ytsaurus/flyt/connectors/ytsaurus/common/credentials/CredentialsProvider.java) interface.

## Data Partitioning {#data-partitioning}

The connector supports automatic data partitioning based on timestamp fields.

To configure partitioning, specify the date or time column in `partition-key` and choose the required granularity in `partition-scale`. The connector automatically calculates the partition value from the selected field. For a working configuration, see [Partitioning Example](#partitioning-example).

#### Supported Partition Scales {#supported-partition-scales}

- `HOUR` - Hourly partitions (format: `YYYY-MM-DD HH:00:00`)
- `HOUR_T` - Hourly partitions with T separator (format: `YYYY-MM-DDTHH:00:00`)
- `DAY` - Daily partitions (format: `YYYY-MM-DD`)
- `WEEK` - Weekly partitions (format: `YYYY-MM-DD`, where the date is the Monday of the corresponding week)
- `MONTH` - Monthly partitions (format: `YYYY-MM-01`)
- `SHORT_MONTH` - Short monthly partitions (format: `YYYY-MM`)
- `YEAR` - Yearly partitions (format: `YYYY-01-01`, that is, the first day of the year)
- `SHORT_YEAR` - Short yearly partitions (format: `YYYY`)

#### Partitioning Example {#partitioning-example}

```sql
CREATE TABLE partitioned_data_source (
id BIGINT,
data STRING,
event_time TIMESTAMP(3)
) WITH (
'connector' = 'datagen',
'rows-per-second' = '50',
'number-of-rows' = '1000',
'fields.id.kind' = 'sequence',
'fields.id.start' = '1',
'fields.id.end' = '100000',
'fields.data.length' = '20',
'fields.event_time.max-past' = '7d'
);
```

```sql
CREATE TABLE partitioned_table (
    id BIGINT,
    data STRING,
    event_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'proxy' = 'localhost:8081',
    'credentials-source' = 'options',
    'username' = 'admin',
    'token' = 'password',
    'path' = '//tmp/partitioned_table',
    'partition-key' = 'event_time',
    'partition-scale' = 'DAY',
    'partition-ttl-day-cnt' = '30',
    'schema' = '[
        {"name"="id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="data";"type"="string";"required"=%false};
        {"name"="event_time";"type"="string";"required"=%false}
    ]'
);
```

```sql
INSERT INTO partitioned_table
SELECT *
FROM partitioned_data_source;
```

Result:

![](../../../../../images/flyt-connector-partitioned-table-result-ui.png)

## Table Resharding {#table-resharding}

The connector supports automatic table resharding at table creation time. Resharding lets you set the number of tablets upfront — either as a fixed value or based on statistics from existing partitions. This helps distribute write load evenly from the start.

#### Resharding Strategies {#resharding-strategies}

- **`NONE`** - Disable resharding
- **`FIXED`** - Resharding with a fixed number of tablets
- **`LAST_PARTITIONS`** - Resharding based on the average number of tablets in the last N partitions

#### Resharding Example {#resharding-example}

```sql
'reshard.strategy' = 'FIXED',
'reshard.tablet-count' = '10',
'reshard.uniform' = 'true'
```

## Lookup Operations {#lookup-operations}

The connector supports both synchronous and asynchronous Lookup operations with caching.

Lookup operations enrich a stream with data from an external table by key. In SQL terms, this corresponds to the `Lookup Join` scenario. For more information about lookup joins, see [{#T}](../../../../user-guide/dynamic-tables/dyn-query-language.md#lookup-joins).

This section covers stream enrichment with data from {{product-name}}. If you only need the basic write scenario, the steps in [Quick Start](#quick-start-guide) are sufficient.

This section describes:

- [Lookup Methods](#lookup-methods);
- [Cache Types](#lookup-cache-types);
- [Lookup Example](#lookup-example).

#### Lookup Methods {#lookup-methods}

- **`LOOKUP`** - standard Lookup operation
- **`SELECT`** - Lookup operation based on a `SELECT` query

#### Cache Types {#lookup-cache-types}

- **`NONE`** - No caching
- **`PARTIAL`** - Partial caching with configurable size and TTL
- **`FULL`** - Full table caching with periodic or timed reload

#### Lookup Example {#lookup-example}

The Lookup connector requires a YSON formatter. Build or download the YSON formatter according to the [documentation](https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-yson) and place it in `$FLINK_ROOT/lib`.

Prepare data for the Lookup operation.

```sql
CREATE TABLE users_datagen (
    id BIGINT,
    name STRING,
    age INT,
    created_at TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '50',
    'number-of-rows' = '1000',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '1000'
);
```

```sql
CREATE TABLE lookup_table_sink (
    id BIGINT,
    name STRING,
    age INT,
    created_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'proxy' = 'localhost:8081',
    'path' = '//tmp/lookup_table',
    'credentials-source' = 'options',
    'username' = 'admin',
    'token' = 'password',
    'schema' = '[
        {"name"="id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="name";"type"="string";"required"=%false};
        {"name"="age";"type"="int64";"required"=%false};
        {"name"="created_at";"type"="string";"required"=%false}
    ]'
);
```

```sql
INSERT INTO lookup_table_sink
SELECT
    id,
    name,
    age,
    created_at
FROM users_datagen;
```

Join order data with user data.

```sql
CREATE TABLE orders_datagen (
    order_id BIGINT,
    user_id BIGINT,
    total_price INT,
    created_at TIMESTAMP(3),
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'number-of-rows' = '100',
    'fields.user_id.min' = '1',
    'fields.user_id.max' = '1000',
    'fields.total_price.min' = '1',
    'fields.total_price.max' = '10000'
);
```

```sql
CREATE TABLE lookup_table (
    id BIGINT,
    name STRING,
    age INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'format' = 'yson',
    'proxy' = 'localhost:8081',
    'credentials-source' = 'options',
    'username' = 'admin',
    'token' = 'password',
    'path' = '//tmp/lookup_table',
    'lookup.async' = 'true',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '10000',
    'lookup.partial-cache.expire-after-access' = '1h',
    'lookup-method' = 'LOOKUP',
    'schema' = '[
        {"name"="id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="name";"type"="string";"required"=%false}
    ]'
);
```

```sql
SELECT
    o.order_id,
    o.total_price,
    o.created_at,
    l.id as user_id,
    l.name,
    l.age
FROM orders_datagen o
LEFT JOIN lookup_table FOR SYSTEM_TIME AS OF o.proc_time AS l
ON o.user_id = l.id;
```

Open the Apache Flink UI at [localhost:8083](http://localhost:8083).

![](../../../../../images/flyt-connector-lookup-join-job-ui.png)

Flink SQL Client displays the results of the Lookup Join operation in real time.

![](../../../../../images/flyt-connector-lookup-join-flink-sql-result.png)


## Examples {#examples}

This section provides:

- [Basic Sink Example](#example-basic-sink);
- [Partitioned Table with Resharding](#example-partitioned-resharding);
- [Lookup Table with Full Cache](#example-lookup-full-cache);
- [Multi-cluster Configuration](#example-multicluster-lookup).

#### Basic Sink Example {#example-basic-sink}

```sql
CREATE TABLE ytsaurus_sink (
    user_id BIGINT,
    username STRING,
    email STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'proxy' = 'localhost:8081',
    'path' = '//home/your-user/users_table',
    'credentials-source' = 'options',
    'username' = 'your-username',
    'token' = 'your-token',
    'schema' = '[
        {"name"="user_id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="username";"type"="string";"required"=%false};
        {"name"="email";"type"="string";"required"=%false};
        {"name"="created_at";"type"="string";"required"=%false}
    ]'
);
```

#### Partitioned Table with Resharding {#example-partitioned-resharding}

```sql
CREATE TABLE events_table (
    event_id BIGINT,
    user_id BIGINT,
    event_type STRING,
    event_data STRING,
    event_time TIMESTAMP(3),
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'proxy' = 'localhost:8081',
    'path' = '//home/your-user/events',
    'credentials-source' = 'options',
    'username' = 'your-username',
    'token' = 'your-token',
    'partition-key' = 'event_time',
    'partition-scale' = 'DAY',
    'partition-ttl-day-cnt' = '90',
    'reshard.strategy' = 'LAST_PARTITIONS',
    'reshard.tablet-count' = '20',
    'reshard.last-partitions-count' = '7',
    'reshard.uniform' = 'true',
    'optimize-for' = 'SCAN',
    'schema' = '[
        {"name"="event_id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="user_id";"type"="int64";"required"=%false};
        {"name"="event_type";"type"="string";"required"=%false};
        {"name"="event_data";"type"="string";"required"=%false};
        {"name"="event_time";"type"="string";"required"=%false}
    ]'
);
```

#### Lookup Table with Full Cache {#example-lookup-full-cache}

```sql
CREATE TABLE user_lookup (
    user_id BIGINT,
    username STRING,
    email STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'proxy' = 'localhost:8081',
    'path' = '//home/your-user/users',
    'credentials-source' = 'options',
    'username' = 'your-username',
    'token' = 'your-token',
    'lookup.cache' = 'FULL',
    'lookup.full-cache.reload-strategy' = 'PERIODIC',
    'lookup.full-cache.periodic-reload-interval' = '1h',
    'optimize-for' = 'LOOKUP',
    'schema' = '[
        {"name"="user_id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="username";"type"="string";"required"=%false};
        {"name"="email";"type"="string";"required"=%false}
    ]'
);
```

#### Multi-cluster Configuration {#example-multicluster-lookup}

```sql
CREATE TABLE multi_cluster_table (
    id BIGINT,
    data STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'ytsaurus',
    'proxy' = 'localhost:8081',
    'path-map' = 'cluster1://tmp/table1,cluster2://tmp/table2',
    'cluster-pick-strategy' = 'FirstAvailableClusterPickStrategy',
    'credentials-source' = 'options',
    'username' = 'your-username',
    'token' = 'your-token',
    'schema' = '[
        {"name"="id";"type"="int64";"required"=%false;"sort_order"="ascending"};
        {"name"="data";"type"="string";"required"=%false}
    ]'
);
```

## What's Next {#whats-next}

- [Sorted dynamic tables](../../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) — learn more about the tables the connector works with;
- [YSON formatter for Flink](../../../../user-guide/data-processing/flyt/flink-yson.md) — if you need to work with YSON directly in Flink jobs;
- {% if audience == "public" %}[{{product-name}} Java client](../../../../api/java/examples.md){% else %}[{{product-name}} Java client](https://ytsaurus.tech/docs/ru/api/java/examples){% endif %} — the low-level API the connector is built on.
