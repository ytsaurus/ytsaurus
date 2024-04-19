This section describes the {{product-name}} queue ecosystem, specialized methods for working with queues, and how to configure and use them.

## Data model { #data_model }

In {{product-name}}, a *queue* refers to any ordered dynamic table. A queue partition is a tablet of a dynamic table, with its index matching the index of the tablet.

In {{product-name}}, a *consumer* refers to a sorted table that has a [defined schema]({{source-root}}/yt/python/yt/environment/init_queue_agent_state.py?rev=r11651845#L46).

A consumer has a many-to-many relationship with queues. The purpose of a consumer is to store offsets for partitions of the queues to be read.

<small>Table 1 — Consumer table schema</small>

| Name | Type | Description |
|----------------------|--------|-----------------------------------------------------------------------------------------|
| queue_cluster | string | Queue cluster name |
| queue_path | string | Path to the queue dynamic table |
| partition_index | uint64 | Partition index, same as `tablet_index` |
| offset | uint64 | Index of the **first row that hasn't been processed** by the consumer within the specified partition of the specified queue |

Communication between consumers and queues is enabled by *registration* objects. Registrations use secure cross-cluster storage, which means that you can register a consumer with a queue from a different {{product-name}} cluster.

## API { #api }

{% if audience == "internal" %}
{% note info "Note" %}

If you are using [BigRT](https://{{doc-domain}}.{{internal-domain}}/big_rt/), we recommend reading [this description](https://{{doc-domain}}.{{internal-domain}}/big_rt/work_details/qyt#native-qyt) of native QYT queues and the [instructions](https://{{doc-domain}}.{{internal-domain}}/big_rt/configuration/yt_sync) for using native {{product-name}} consumers, which are supported by [BigB](https://abc.{{internal-domain}}/services/bigb/), in yt_sync.

If you plan to configure a large number of queues and consumers in your processes, we also recommend looking into yt_sync.

{% endnote %}
{% endif %}

### Creating a queue

Creating a queue is no different from creating a regular ordered dynamic table.

For the most complete metrics/statistics, we recommend adding the [`$timestamp`](ordered-dynamic-tables.md#timestamp_column) and [`$cumulative_data_weight`](ordered-dynamic-tables.md#cumulative_data_weight_column) columns to the table schema.

You can determine whether the system has recognized the queue by checking the page of the dynamic table object for the presence of the Queue tab.

### Creating a consumer

Currently, to create a consumer, you need to create a sorted dynamic table with the [schema above](#data_model). To do this, set the `@treat_as_queue_consumer = %true` attribute for the table.

In the near future, you'll be able to create consumers via the `create consumer` alias.

You can determine whether the system has recognized the consumer by checking the page of the dynamic table object for the presence of the **Consumers** tab.

### Registering a consumer with a queue

To register a consumer with a queue, use the `register_queue_consumer` method. Make sure to set a value for the `vital` parameter. It's a required boolean parameter that determines some of the settings for automatic queue trimming (see the [section](#automatic_trimming) on automatic trimming).

To execute the command above, you need a `register_queue_consumer` permission with `vital=True/False` for the directory that contains the queue. You can request it the same way as any other permission, on the directory page in the {{product-name}} UI. A `register_queue_consumer` permission with `vital=True` allows you to register both vital and non-vital consumers.

To delete a registration, use the `unregister_queue_consumer` method. Executing this command requires write access to the queue or the consumer.

Both Cypress arguments in both of these commands are of [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) type and can take cluster names. Otherwise, the commands use the cluster on which they were executed.

### Reading data

There are two similar methods for reading data from queues.

With the `pull_queue` method, you can read some of the rows from the specified partition of the specified queue by limiting the row count (`max_row_count`) or the size of the partition in bytes (`max_data_weight`). Executing this query requires read access to the queue.

The `pull_consumer` method is similar but takes the path to the consumer table as its first argument. This query is executed under two conditions: the user must have read access to the consumer **and** the said consumer must be registered with the specified queue.
The queue parameter in this method is a [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) that can take the cluster on which the queue is located. Otherwise, the command uses the cluster on which it was executed.

In addition, you can read queue data the usual way, using the `select_rows` method.

{% note warning "Attention" %}

The `pull_queue` and `pull_consumer` methods may return fewer rows than specified in the `max_row_count` parameter, even if there are enough rows in the queue partition and their combined size doesn't exceed `max_data_weight`. Only the return of an empty set of rows means that the partition doesn't have any rows at the specified offset.

{% endnote %}


### Working with a consumer

You can work with a consumer by using the `advance_consumer` method, which moves the consumer offset in the specified partition of the specified queue. Within the passed transaction, the method updates the corresponding consumer offset row. You can also perform other operations with dynamic tables as part of the same transaction.

If you specify a non-null `old_offset` parameter value, the method first reads the current offset within the transaction and compares it with the passed value. If they don't match, the method throws an exception.

{{product-name}} queue offsets are interpreted as the index of the *first unread row*.

## Queue Agent

Queue Agent is a dedicated microservice for monitoring queues, consumers, and registrations.

### Automatic queue trimming policies { #automatic_trimming }

You can configure an automatic queue cleanup (trimming) policy by setting the `@auto_trim_config` attribute with a configuration in the [appropriate format]({{source-root}}/yt/yt/client/queue_client/config.h?rev=11720161#L41).

Available options:
- `enable: True` — Enables trimming by vital consumers. If there is at least one vital consumer, Queue Agent calls Trim for the partition at single-digit intervals in seconds until it reaches the smallest offset among vital consumers.<br> **NB:** If there are no vital consumers, no trimming occurs.
- `retained_rows: x` — If set, this number of rows is guaranteed to be kept in each partition. This option is intended to be used in combination with the previous one.
- `retained_lifetime_duration: x` — Ensures that each partition contains only those rows that were written to the queue no more than this number of milliseconds ago. The specified number of milliseconds must be a multiple of 1000. This option is intended to be used with trimming enabled (`enable: True`).

This setting doesn't conflict with [existing](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md#remove_old_data) TTL settings for dynamic tables: for example, you can configure trimming with respect to vital consumers and set `max_data_ttl=36h, min_data_versions=0`. This way, in addition to cleaning up data by offsets, you will only store data for the last three days.

### Graphs and statuses

{% if audience == "internal" %}
With Queue Agent, you can export queue and consumer graphs to Solomon. They appear on the queue and consumer table pages in the UI.

You can also use a general dashboard for a consumer-queue pair: [Example]({{nda-link}}/ymXONQWw6Tscmm).
{% endif %}

Using the `@queue_status` and `@queue_partitions` queue table attributes and the `@queue_consumer_status` and `@queue_consumer_partitions` consumer table attributes, you can look up the current status and meta-information of the queue or consumer from the Queue Agent perspective. This allows retrieving information both for the queue or consumer as a whole as well as for their individual partitions.

These attributes are not intended for high-load services and should only be used for introspection.

## Usage example


Since some of the commands and fixes are relatively recent, we currently recommend using the `yt` utility built from the latest version of the source code before attempting to run the commands below.
{% if audience == "internal" %}
These examples use [Hume](https://{{yt-domain}}.{{internal-domain}}/hume) and [Pythia](https://{{yt-domain}}.{{internal-domain}}/pythia) clusters.
{% else %}
These examples use a hypothetical configuration with multiple clusters, named `hume` and `pythia`.
{% endif %}

<small>Listing 1 — Example of using the Queue API</small>

```bash
# Create queue on pythia.
$ yt --proxy pythia create table //tmp/test_queue --attributes '{dynamic=true;schema=[{name=data;type=string};{name="$timestamp";type=uint64};{name="$cumulative_data_weight";type=int64}]}'
2826e-2b1e4-3f30191-dcd2013e

# Create consumer on hume.
yt --proxy hume create table //tmp/test_consumer --attributes '{dynamic=true;treat_as_queue_consumer=true;schema=[{name=queue_cluster;type=string;sort_order=ascending;required=true};{name=queue_path;type=string;sort_order=ascending;required=true};{name=partition_index;type=uint64;sort_order=ascending;required=true};{name=offset;type=uint64;required=true}]}'
18a5b-28931-3ff0191-35282540

# Register consumer for queue.
$ yt --proxy pythia register-queue-consumer //tmp/test_queue "<cluster=hume>//tmp/test_consumer" --vital true

# Check registrations for queue.
$ yt --proxy pythia list-queue-consumer-registrations --queue-path //tmp/test_queue
[
  {
    "queue_path" = <
      "cluster" = "pythia";
    > "//tmp/test_queue";
    "consumer_path" = <
      "cluster" = "hume";
    > "//tmp/test_consumer";
    "vital" = %true;
    "partitions" = #;
  };
]

# Check queue status provided by Queue Agent.
$ yt --proxy pythia get //tmp/test_queue/@queue_status
{
    "write_data_weight_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
    "write_row_count_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/test_consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/test_queue";
        };
    ];
    "family" = "ordered_dynamic_table";
    "has_cumulative_data_weight_column" = %true;
    "has_timestamp_column" = %true;
    "partition_count" = 1;
}

$ yt --proxy pythia get //tmp/test_queue/@queue_partitions
[
    {
        "error" = {
            "attributes" = {
                "trace_id" = "46143a68-3de2e728-eae939cd-ad813095";
                "span_id" = 12293679318704208190u;
                "datetime" = "2023-06-22T10:11:02.813875Z";
                "tid" = 15645118512615789701u;
                "pid" = 1484;
                "host" = "yt-queue-agent-testing-1.vla.yp-c.yandex.net";
                "state" = "unmounted";
                "fid" = 18443819153702847241u;
            };
            "code" = 1;
            "message" = "Tablet 2826e-2b1e4-3f302be-1d95616f is not mounted";
        };
    };
]

# Check consumer status provided by Queue Agent.
$ yt --proxy hume get //tmp/test_consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/test_queue" = {
            "error" = {
                "attributes" = {
                    "trace_id" = "ae5fd7bf-d5ab2d1b-10b3f8fe-baeadb24";
                    "span_id" = 12174663615949324932u;
                    "tablet_id" = "7588-33794-fb702be-a1dc6861";
                    "datetime" = "2023-06-22T10:07:32.213995Z";
                    "tid" = 14514940285303313587u;
                    "pid" = 1477;
                    "is_tablet_unmounted" = %true;
                    "host" = "yt-queue-agent-prestable-2.vla.yp-c.yandex.net";
                    "fid" = 18446093353932736666u;
                };
                "code" = 1702;
                "message" = "Cannot read from tablet 7588-33794-fb702be-a1dc6861 of table #18a5b-28931-3ff0191-35282540 while it is in \"unmounted\" state";
            };
        };
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/test_consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/test_queue";
        };
    ];
}

# We can see some errors in the responses above, since both tables are unmounted.
# Mount queue and consumer tables.
$ yt --proxy pythia mount-table //tmp/test_queue
$ yt --proxy hume mount-table //tmp/test_consumer

# Check statuses again:
$ $ yt --proxy pythia get //tmp/test_queue/@queue_partitions
[
    {
        "meta" = {
            "cell_id" = "185bc-b318a-3f302bc-ea8d7f14";
            "host" = "vla3-2329-node-pythia.vla.yp-c.yandex.net:9012";
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
        "commit_idle_time" = 1687428752830;
    };
]
$ yt --proxy hume get //tmp/test_consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/test_queue" = {
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
            "consumer" = "hume://tmp/test_consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/test_queue";
        };
    ];
}

# Enable automatic trimming based on vital consumers for queue.
$ yt --proxy pythia set //tmp/test_queue/@auto_trim_config '{enable=true}'

# Insert some data into queue.
$ for i in {1..20}; do echo '{data=foo}; {data=bar}; {data=foobar}; {data=megafoo}; {data=megabar}' | yt insert-rows --proxy pythia //tmp/test_queue --format yson; done;

# Check that queue status reflects writes.
$ yt --proxy pythia get //tmp/test_queue/@queue_status/write_row_count_rate
{
    "1m_raw" = 2.6419539762457456;
    "current" = 5.995956327053036;
    "1h" = 2.6419539762457456;
    "1d" = 2.6419539762457456;
    "1m" = 2.6419539762457456;
}

# Read data via consumer.
$ yt --proxy hume pull-consumer //tmp/test_consumer "<cluster=pythia>//tmp/test_queue"  --partition-index 0 --offset 0 --max-row-count 10 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=0;"data"="foo";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=20;};
{"$tablet_index"=0;"$row_index"=1;"data"="bar";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=40;};
{"$tablet_index"=0;"$row_index"=2;"data"="foobar";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=63;};
{"$tablet_index"=0;"$row_index"=3;"data"="megafoo";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=87;};
{"$tablet_index"=0;"$row_index"=4;"data"="megabar";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=111;};

# Advance consumer.
$ yt --proxy hume advance-consumer //tmp/test_consumer "<cluster=pythia>//tmp/test_queue" --partition-index 0 --old-offset 0 --new-offset 42

# Since trimming is enabled and the consumer is the only vital consumer for the queue, soon the rows up to index 42 will be trimmed.
# Calling pull-consumer now returns the next available rows.
$ yt --proxy hume pull-consumer //tmp/test_consumer "<cluster=pythia>//tmp/test_queue"  --partition-index 0 --offset 0 --max-row-count 5 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=42;"data"="foobar";"$timestamp"=1811865674679584351u;"$cumulative_data_weight"=951;};
{"$tablet_index"=0;"$row_index"=43;"data"="megafoo";"$timestamp"=1811865674679584351u;"$cumulative_data_weight"=975;};
{"$tablet_index"=0;"$row_index"=44;"data"="megabar";"$timestamp"=1811865674679584351u;"$cumulative_data_weight"=999;};
{"$tablet_index"=0;"$row_index"=45;"data"="foo";"$timestamp"=1811865674679607803u;"$cumulative_data_weight"=1019;};
{"$tablet_index"=0;"$row_index"=46;"data"="bar";"$timestamp"=1811865674679607803u;"$cumulative_data_weight"=1039;};
```

{% if audience == "internal" %}
You can see the results of the operations in graphs on the queue and consumer tab, as well as on the general [dashboard](https://{{monitoring-domain}}.{{internal-domain}}/projects/yt/dashboards/monuob4oi7lf8uddjs76?p%5Bconsumer_cluster%5D=hume&p%5Bconsumer_path%5D=%2F%2Ftmp%2Ftest_consumer&p%5Bqueue_cluster%5D=pythia&p%5Bqueue_path%5D=%2F%2Ftmp%2Ftest_queue&from=1687429383727&to=1687444244000&forceRefresh=1687444307283).

## Limitations and issues

- Asynchronous computation of consumer lags may result in [small negative values during reactive processing](https://st.{{internal-domain}}/YT-19361).
   {% endif %}


