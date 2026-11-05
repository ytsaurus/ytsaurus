# QYT Connector in {{product-name}} Flow

This connector works with {{product-name}} [queues](../../../user-guide/dynamic-tables/queues.md) (QYT).

You can find the connector code [here]({{source-root}}/yt/yt/flow/library/cpp/connectors/queue).

## Reading from a Queue

When you read from a queue using the simplest method, messages arrive in [`Computation`](../../../flow/concepts/glossary.md#stream-and-computation) with the same schema as the queue table rows.

By default, the message’s `event_time` and `system_time` are calculated based on the `$timestamp` column, which contains the YT timestamp of the row record (for more details, see the [documentation for ordered dynamic tables](../../../user-guide/dynamic-tables/queues.md)).
But if the queue has a column with metadata (see the static spec parameters for the source), the message’s `event_time` can be taken from the corresponding field. You can also get stream watermark information from the metadata.

The [source](../../../flow/concepts/glossary.md#source) class is `NYT::NFlow::TQueueSource`.

Below are the source settings.

{% note info %}

The source is defined by the cluster and the queue path (`queue_path`). If you change them, the partitions are recreated and read again — see [Changing the Source](../../../flow/connectors/about.md#source-change).

{% endnote %}

### Managing Consumer Lag

When you register a consumer for a queue, the consumer lag is set to the last stored data in the queue. So, after registration, if the policy for deleting old rows in the queue is 1 day, the lag will also be 1 day.

This behavior can lead to the following scenario: when you roll out a release, the data lag will be 1 day, and message processing will pause until the lag is processed.

You can avoid this in several ways:

#### 1. Using the Standard YT CLI

Use the standard YT CLI to set the consumer offsets to the desired values for each partition. This isn’t very convenient for the first read of a topic, because you need to know the value of the latest offset and set it for each [partition](../../../flow/concepts/glossary.md#partition).

Here’s an example command:
```bash
{{yt-cli}} advance-queue-consumer --proxy {{flow-consumer-cluster}} //home/service/stable/consumer {{flow-data-cluster}}://home/source_service/Data/queue --partition-index 0 --new-offset 486482013
```

For more advanced options, check the YT documentation or use the `-h` parameter.

{% if audience == "internal" %}

#### 2. Using the bigrtcli Utility

A more convenient option is to use the `bigrtcli` utility.

Here’s an example command:
```bash
YT_PROXY={{flow-data-cluster}} ya tool bigrtcli consumer update_offsets "<cluster={{flow-consumer-cluster}}>//home/service/prestable/consumer" "<cluster={{flow-data-cluster}};wrapped=%false>//home/source_service/queue" --shards "range(0, 256)" --value "-1"
```

{% note warning "Attention" %}

Before you run the command, make sure it’s correct. Using the `--value "-1"` parameter will skip all data in the input queue for the specified partitions.

{% endnote %}

In this example, we update the offsets for the consumer `//home/service/prestable/consumer` on the Pythia cluster, for the queue `//home/source_service/Data` on the Markov cluster, for all partitions from 0 to 255. This skips all data in the input queue — in effect, it sets the offset to the next unused value for each partition.

{% note info "Important" %}

You must set `YT_PROXY={{flow-data-cluster}}` for `bigrtcli` to work correctly; otherwise, an error will occur. The cluster for the queue and consumer is taken from the command itself.

{% endnote %}

For more information about usage, check the [code]({{source-root}}/bigrt/cli/lib/__init__.py?rev=r18281971#L656) or use the `-h` parameter.

{% endif %}

### Static spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TQueueSource.md) %}

### Dynamic spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TQueueSource.md) %}

## Writing to a Queue

To write messages to a queue in a [sink](../../../flow/concepts/glossary.md#sink), send messages with the same schema as the table where you plan to write. You can also configure writing metadata to a special column to pass information about the message’s `event_time` and the stream watermark to queue readers.

There are two sink options: synchronous (`NYT::NFlow::TSyncQueueSink`) and asynchronous (`NYT::NFlow::TAsyncQueueSink`). Writing to a synchronous sink happens in the main transaction of the [epoch](../../../flow/concepts/glossary.md#epoch). This is efficient, but you can only write to a queue on the main processing cluster. Writing to an asynchronous sink happens after the main epoch transaction, using messages stored in output messages. This is more resource-intensive, but you can write to a queue on any cluster.

### Parameters for synchronous sink specs

#### Static spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TSyncQueueSink.md) %}

#### Dynamic spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TSyncQueueSink.md) %}

### Parameters for asynchronous sink specs

#### Static spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TAsyncQueueSink.md) %}

#### Dynamic spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAsyncQueueSink.md) %}

{% if audience == "internal" %}

## BigRT Extension

To work with queues in BigRT format (batching and compressing into a single column), use the [BigRT Queue](../../../yandex-specific/flow/extensions/bigrt.md) extension.

{% endif %}

## See also

- [List of Connectors](../../../flow/connectors/about.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)