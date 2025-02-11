This section describes the {{product-name}} queue ecosystem, specialized methods for working with queues, and how to configure and use them.

## Data model { #data_model }

In {{product-name}}, a *queue* refers to any ordered dynamic table. A queue partition is a tablet of a dynamic table, with its index matching the index of the tablet.

In {{product-name}}, a *consumer (queue_consumer)* refers to a sorted table that has a defined schema. It has a many-to-many relationship with queues and represents a consumer of one or more queues. A consumer's function is to store offsets across the partitions of readable queues.

Communication between consumers and queues is enabled by *registration* objects. {% if audience == "internal" %} Registrations use secure cross-cluster storage, which means that you can register a consumer with a queue from a different {{product-name}} cluster. {% endif %}


<small>Table 1 — Consumer table schema</small>

| Name | Type | Description |
|----------------------|--------|-----------------------------------------------------------------------------------------|
| queue_cluster | string | Queue cluster name |
| queue_path | string | Path to a dynamic queue table |
| partition_index | uint64 | Partition index, same as `tablet_index` |
| offset | uint64 | Index of the **first row that hasn't been processed** by the consumer within the specified partition of the specified queue |
| meta | any | System meta information |

In {{product-name}}, a *producer (queue_producer)* also refers to a sorted table that has a defined schema. A producer stores the indexes of the last rows added during queue write sessions, which helps prevent row duplication in the queue.

<small>Table 2 — Producer table schema</small>

| Name | Type | Description |
|----------------------|--------|-----------------------------------------------------------------------------------------|
| queue_cluster | string | Queue cluster name |
| queue_path | string | Path to a dynamic queue table |
| session_id | string | Session ID |
| sequence_number | int64 | Sequence number of the last written row |
| epoch | int64 | Number of the current epoch |
| user_meta | any | User meta information |
| system_meta | any | System meta information |


## API { #api }

{% if audience == "internal" %}
{% note info "Note" %}

If you are using [BigRT](https://{{doc-domain}}.{{internal-domain}}/big_rt/), we recommend reading [this description](https://{{doc-domain}}.{{internal-domain}}/big_rt/work_details/qyt#native-qyt) of native QYT queues and the [instructions](https://{{doc-domain}}.{{internal-domain}}/big_rt/configuration/yt_sync) for using native {{product-name}} consumers, which are supported by [BigB](https://abc.{{internal-domain}}/services/bigb/), in yt_sync.

If you plan to configure a large number of queues and consumers in your processes, we also recommend looking into yt_sync.

{% endnote %}
{% endif %}

### Creating a queue

Creating a queue is no different from creating a regular ordered dynamic table.

To ensure that your graphs and statistics are comprehensive, we recommend adding the [`$timestamp`](ordered-dynamic-tables.md#timestamp_column) and [`$cumulative_data_weight`](ordered-dynamic-tables.md#cumulative_data_weight_column) columns to the table schema.

You can verify that the system has recognized the queue by checking if the **Queue** tab has appeared on the page of the dynamic table object.

### Creating a consumer

You can create a consumer using the alias `create queue_consumer` or by explicitly creating a sorted dynamic table using the schema [provided above](#data_model) and setting the `@treat_as_queue_consumer = %true` attribute for it.

You can verify that the system has recognized the consumer by checking if the **Consumers** tab has appeared on the page of the dynamic table object.

### Registering a consumer with a queue

To register a consumer with a queue, use the `register_queue_consumer` method. Make sure to set a value for the `vital` parameter. It's a required boolean parameter that determines some of the settings for automatic queue trimming (see the [section](#automatic_trimming) on automatic trimming).

To execute the command above, you need a `register_queue_consumer` permission with `vital=True/False` for the directory that contains the queue. You can request it the same way as any other permission, on the directory page in the {{product-name}} UI. A `register_queue_consumer` permission with `vital=True` allows you to register both vital and non-vital consumers.

To delete a registration, use the `unregister_queue_consumer` method. Executing this command requires write access to the queue or the consumer.

Both Cypress arguments in both of these commands are of [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) type and can take cluster names. Otherwise, the commands use the cluster on which they were executed.

### Reading data

There are two similar methods for reading data from queues.

With the `pull_queue` method, you can read some of the rows from the specified partition of the specified queue by limiting the row count (`max_row_count`) or the size of the partition in bytes (`max_data_weight`). Executing this query requires read access to the queue.

The `pull_queue_consumer` method is similar but takes the path to the consumer table as its first argument. This query is executed under two conditions: the user must have read access to the consumer **and** the said consumer must be registered with the specified queue.
The queue parameter in this method is a [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) that can take the cluster on which the queue is located. Otherwise, the command uses the cluster on which it was executed.

In addition, you can read queue data the usual way, using the `select_rows` method.

{% note warning "Attention" %}

The `pull_queue` and `pull_queue_consumer` methods may return fewer rows than specified in the `max_row_count` parameter, even if there are enough rows in the queue partition and their combined size doesn't exceed `max_data_weight`. Only the return of an empty set of rows means that the partition doesn't have any rows at the specified offset.

{% endnote %}

### Working with a consumer

You can work with a consumer by using the `advance_queue_consumer` method, which moves the consumer offset in the specified partition of the specified queue. Within the passed transaction, the method updates the corresponding consumer offset row. You can also perform other operations with dynamic tables as part of the same transaction.

If you specify a non-null `old_offset` parameter value, the method first reads the current offset within the transaction and compares it with the passed value. If they don't match, the method throws an exception.

{{product-name}} queue offsets are interpreted as the index of the *first unread row*.

### Creating a producer

You can create a producer using the alias `create queue_producer` or by explicitly creating a sorted dynamic table using the schema [provided above](#data_model) and setting the `@treat_as_queue_producer = %true` attribute for it.

### Writing data

To write data to a queue, you can use either the `insert_rows` method from the dynamic tables API or the producer API to prevent row duplication during writes.

To make writes using a producer, you need `write` permissions for both the queue and the producer.

Before initiating a write, you need to call the `create_queue_producer_session` method, which takes the queue path, the producer path, and the ID of the write session (`session_id`). For `session_id`, you can pass any string: for example, you can use the name of the host from which the write is being made. Then, if no such session exists, a new session with an `epoch` of `0` and a `sequence_number` of `-1` is created in the producer table. If a session with this ID has already been created before, its `epoch` value is incremented.

The `create_queue_producer_session` method returns the updated epoch value as well as the index of the last message written, which should be the current write session status stored in the producer.

You can then write data to the queue using the `push_queue_producer` method. This method takes the queue path, producer path, session ID, epoch, and the data to write. Each row must be passed with the `$sequence_number` value corresponding to its index. Alternatively, instead of passing the index for each row in the data itself, you can specify only the index corresponding to the first row in the method options. In this case, we assume that the index should be incremented by one for each subsequent row.

You can use the session's `epoch` to prevent zombie processes.

## Queue Agent

Queue Agent is a dedicated microservice for monitoring queues, consumers, and registrations.

### Automatic queue trimming policies { #automatic_trimming }

You can configure an automatic queue cleanup (trimming) policy by setting the `@auto_trim_config` attribute with a configuration in the [appropriate format]({{source-root}}/yt/yt/client/queue_client/config.h?rev=11720161#L41).

Available options:
  - `enable: True` — Enables trimming by vital consumers. If there is at least one vital consumer, Queue Agent calls Trim for the partition at single-digit intervals in seconds until it reaches the smallest offset among vital consumers.<br> **NB:** If there are no vital consumers, no trimming occurs.
  - `retained_rows: x` — If set, this number of rows is guaranteed to be kept in each partition. This option is intended to be used in combination with the previous option.
  - `retained_lifetime_duration: x` — Ensures that each partition contains only those rows that were written to the queue no more than this number of milliseconds ago. The specified number of milliseconds must be a multiple of 1000. This option is intended to be used with trimming enabled (`enable: True`).

This setting doesn't conflict with the [existing](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md#remove_old_data) TTL settings for dynamic tables: you can configure trimming based on vital consumers and set `max_data_ttl=36h, min_data_versions=0` to trim data based on its offset as well as ensure that it won't be stored for more than three days.

### Graphs and statuses

{% if audience == "internal" %}
With Queue Agent, you can export queue and consumer graphs to Solomon. They appear on the queue and consumer table pages in the UI.

You can also use a general dashboard for a consumer-queue pair: [Example]({{nda-link}}/ymXONQWw6Tscmm).
{% endif %}

The `@queue_status` and `@queue_partitions` queue table attributes along with the `@queue_consumer_status` and `@queue_consumer_partitions` consumer table attributes provide the current status and meta information of queues and consumers from the perspective of the Queue Agent. You can get this data both for these objects as a whole as well as for their individual partitions.

These attributes aren't intended for high-load services and should only be used for introspection.

## Usage example

{% if audience == "internal" %}
These examples use [Hume](https://{{yt-domain}}.{{internal-domain}}/hume) and [Pythia](https://{{yt-domain}}.{{internal-domain}}/pythia) clusters.
{% else %}
These examples use a hypothetical configuration with multiple clusters, named `hume` and `pythia`.
{% endif %}

<small>Listing 1 — Example of using the Queue API</small>

```bash
# Create queue on pythia.
$ yt --proxy pythia create table //tmp/$USER-test-queue --attributes '{dynamic=true;schema=[{name=data;type=string};{name="$timestamp";type=uint64};{name="$cumulative_data_weight";type=int64}]}
'2826e-2b1e4-3f30191-dcd2013e

# Create queue_consumer on hume.
$ yt --proxy hume create queue_consumer //tmp/$USER-test-consumer

# OR: Create queue_consumer on hume as table with explicit schema specification.
$ yt --proxy hume create table //tmp/$USER-test-consumer --attributes '{dynamic=true;treat_as_queue_consumer=true;schema=[{name=queue_cluster;type=string;sort_order=ascending;required=true};{name=queue_path;type=string;sort_order=ascending;required=true};{name=partition_index;type=uint64;sort_order=ascending;required=true};{name=offset;type=uint64;required=true};{name=meta;type=any;required=false}]}
'18a5b-28931-3ff0191-35282540

# Register consumer for queue.
$ yt --proxy pythia register-queue-consumer //tmp/$USER-test-queue "<cluster=hume>//tmp/$USER-test-consumer" --vital

# Check registrations for queue.
$ yt --proxy pythia list-queue-consumer-registrations --queue-path //tmp/$USER-test-queue
[
  {
    "queue_path" = <
      "cluster" = "pythia";
    > "//tmp/bob-test-queue";
    "consumer_path" = <
      "cluster" = "hume";
    > "//tmp/bob-test-consumer";
    "vital" = %true;
    "partitions" = #;
  };
]

# Check queue status provided by Queue Agent.
$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_status
{
    "partition_count" = 1;
    "has_cumulative_data_weight_column" = %true;
    "family" = "ordered_dynamic_table";
    "exports" = {
        "progress" = {};
    };
    "alerts" = {};
    "queue_agent_host" = "yt-queue-agent-1.ytsaurus.tech";
    "has_timestamp_column" = %true;
    "write_row_count_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/bob-test-consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/bob-test-queue";
        };
    ];
    "write_data_weight_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
}

$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_partitions
[
    {
        "error" = {
            "attributes" = {
                "trace_id" = "21503942-85d82a3a-2c9ea16d-e2149d9c";
                "span_id" = 17862985281506116291u;
                "thread" = "Controller:4";
                "datetime" = "2025-01-23T13:42:21.839124Z";
                "tid" = 9166196934387883291u;
                "pid" = 481;
                "host" = "yt-queue-agent-1.ytsaurus.tech";
                "state" = "unmounted";
                "fid" = 18445202819181375616u;
            };
            "code" = 1;
            "message" = "Tablet 3d3c-50e7d-7db02be-7e178361 is not mounted or frozen";
        };
    };
]

# Check queue consumer status provided by Queue Agent.
$ yt --proxy hume get //tmp/$USER-test-consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/bob-test-queue" = {
            "error" = {
                "attributes" = {
                    "trace_id" = "623ba99c-b2dce5fe-50174949-5f508824";
                    "span_id" = 14498308957160432715u;
                    "thread" = "Controller:1";
                    "datetime" = "2025-01-23T13:42:55.747430Z";
                    "tid" = 627435960759374310u;
                    "pid" = 481;
                    "host" = "yt-queue-agent-1.ytsaurus.tech";
                    "fid" = 18442320640360156096u;
                };
                "code" = 1;
                "message" = "Queue \"pythia://tmp/bob-test-queue\" snapshot is missing";
            };
        };
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/bob-test-consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/bob-test-queue";
        };
    ];
    "queue_agent_host" = "yt-queue-agent-1.ytsaurus.tech";
}

# We can see some errors in the responses above, since both tables are unmounted.
# Mount queue and consumer tables.
$ yt --proxy pythia mount-table //tmp/$USER-test-queue
$ yt --proxy hume mount-table //tmp/$USER-test-consumer

# Check statuses again:
$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_partitions
[
    {
        "meta" = {
            "cell_id" = "2dd9a-d4f7-3f302bc-f21fc0c";
            "host" = "node-1.ytsaurus.tech:9022";
        };
        "lower_row_index" = 0;
        "cumulative_data_weight" = #;
        "upper_row_index" = 0;
        "available_row_count" = 0;
        "write_row_count_rate" = {
            "1m_raw" = 0.;
            "1h" = 0.;
            "current" = 0.;
            "1d" = 0.;
            "1m" = 0.;
        };
        "available_data_weight" = #;
        "trimmed_data_weight" = #;
        "last_row_commit_time" = "1970-01-01T00:00:00.000000Z";
        "write_data_weight_rate" = {
            "1m_raw" = 0.;
            "1h" = 0.;
            "current" = 0.;
            "1d" = 0.;
            "1m" = 0.;
        };
        "commit_idle_time" = 1737639851870;
    };
]

$ yt --proxy hume get //tmp/$USER-test-consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/bob-test-queue" = {
            "read_data_weight_rate" = {
                "1m_raw" = 0.;
                "1h" = 0.;
                "current" = 0.;
                "1d" = 0.;
                "1m" = 0.;
            };
            "read_row_count_rate" = {
                "1m_raw" = 0.;
                "1h" = 0.;
                "current" = 0.;
                "1d" = 0.;
                "1m" = 0.;
            };
            "partition_count" = 1;
        };
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/bob-test-consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/bob-test-queue";
        };
    ];
}

# Enable automatic trimming based on vital consumers for queue.
$ yt --proxy pythia set //tmp/$USER-test-queue/@auto_trim_config '{enable=true}'

# Write rows without exactly once semantics.
$ for i in {1..20}; do echo '{data=foo}; {data=bar}; {data=foobar}; {data=megafoo}; {data=megabar}' | yt insert-rows --proxy pythia //tmp/$USER-test-queue --format yson; done;

# Check that queue status reflects writes.
$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_status/write_row_count_rate
{
    "1m_raw" = 2.6419539762457456;
    "current" = 5.995956327053036;
    "1h" = 2.6419539762457456;
    "1d" = 2.6419539762457456;
    "1m" = 2.6419539762457456;
}

# Read data via consumer.
$ yt --proxy hume pull-queue-consumer //tmp/$USER-test-consumer "<cluster=pythia>//tmp/$USER-test-queue" --partition-index 0 --offset 0 --max-row-count 5 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=0;"data"="foo";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=20;};
{"$tablet_index"=0;"$row_index"=1;"data"="bar";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=40;};
{"$tablet_index"=0;"$row_index"=2;"data"="foobar";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=63;};
{"$tablet_index"=0;"$row_index"=3;"data"="megafoo";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=87;};
{"$tablet_index"=0;"$row_index"=4;"data"="megabar";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=111;};

# Advance queue consumer.
$ yt --proxy hume advance-queue-consumer //tmp/$USER-test-consumer "<cluster=pythia>//tmp/$USER-test-queue" --partition-index 0 --old-offset 0 --new-offset 42

# Since trimming is enabled and the consumer is the only vital consumer for the queue, soon the rows up to index 42 will be trimmed.
# Calling pull-queue-consumer now returns the next available rows.
$ yt --proxy hume pull-queue-consumer //tmp/$USER-test-consumer "<cluster=pythia>//tmp/$USER-test-queue"  --partition-index 0 --offset 0 --max-row-count 5 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=42;"data"="foobar";"$timestamp"=1865777485011069884u;"$cumulative_data_weight"=951;};
{"$tablet_index"=0;"$row_index"=43;"data"="megafoo";"$timestamp"=1865777485011069884u;"$cumulative_data_weight"=975;};
{"$tablet_index"=0;"$row_index"=44;"data"="megabar";"$timestamp"=1865777485011069884u;"$cumulative_data_weight"=999;};
{"$tablet_index"=0;"$row_index"=45;"data"="foo";"$timestamp"=1865777486084785133u;"$cumulative_data_weight"=1019;};
{"$tablet_index"=0;"$row_index"=46;"data"="bar";"$timestamp"=1865777486084785133u;"$cumulative_data_weight"=1039;};

# Create queue producer on pythia.
$ yt --proxy pythia create queue_producer //tmp/$USER-test-producer
309db-eb36-3f30191-f83f27c0

# Create queue producer session.
$ yt --proxy pythia create-queue-producer-session --queue-path //tmp/$USER-test-queue --producer-path //tmp/$USER-test-producer --session-id session_123
{
  "epoch" = 0;
  "sequence_number" = -1;
  "user_meta" = #;
}

# Write rows via queue producer.
$ echo '{data=value1;"$sequence_number"=1};{data=value2;"$sequence_number"=2}' | yt --proxy pythia push-queue-producer //tmp/$USER-test-producer //tmp/$USER-test-queue --session-id session_123 --epoch 0 --input-format yson
{
  "last_sequence_number" = 2;
  "skipped_row_count" = 0;
}

# Check written rows.
$ yt --proxy pythia pull-queue //tmp/$USER-test-queue --offset 100 --partition-index 0 --format "<format=pretty>yson"
{
    "$tablet_index" = 0;
    "$row_index" = 100;
    "data" = "value1";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2243;
};
{
    "$tablet_index" = 0;
    "$row_index" = 101;
    "data" = "value2";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2266;
};

# Write one more row batch with row duplicates.
$ echo '{data=value2;"$sequence_number"=2};{data=value3;"$sequence_number"=10}' | yt --proxy pythia push-queue-producer //tmp/$USER-test-producer //tmp/$USER-test-queue --session-id session_123 --epoch 0 --input-format yson
{
  "last_sequence_number" = 10;
  "skipped_row_count" = 1;
}

# Check that there is no row dublicates.
$ yt --proxy pythia pull-queue //tmp/$USER-test-queue --offset 100 --partition-index 0 --format "<format=pretty>yson"
{
    "$tablet_index" = 0;
    "$row_index" = 100;
    "data" = "value1";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2243;
};
{
    "$tablet_index" = 0;
    "$row_index" = 101;
    "data" = "value2";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2266;
};
{
    "$tablet_index" = 0;
    "$row_index" = 102;
    "data" = "value3";
    "$timestamp" = 1865777742709000317u;
    "$cumulative_data_weight" = 2289;
};
```

{% if audience == "internal" %}
You can see the results of the operations in graphs on the queue and consumer tab, as well as on the general [dashboard](https://{{monitoring-domain}}.{{internal-domain}}/projects/yt/dashboards/monuob4oi7lf8uddjs76?p%5Bconsumer_cluster%5D=hume&p%5Bconsumer_path%5D=%2F%2Ftmp%2Ftest_consumer&p%5Bqueue_cluster%5D=pythia&p%5Bqueue_path%5D=%2F%2Ftmp%2Ftest_queue&from=1687429383727&to=1687444244000&forceRefresh=1687444307283).

## Limitations and issues

  - Asynchronous computation of consumer lags may result in [small negative values during reactive processing](https://st.{{internal-domain}}/YT-19361).
{% endif %}


