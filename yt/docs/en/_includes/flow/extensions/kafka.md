# Kafka Extension for {{product-name}} Flow

The Kafka extension reads from and writes to [Apache Kafka](https://kafka.apache.org) topics through [librdkafka](https://github.com/confluentinc/librdkafka). It provides:

- The source `NYT::NFlow::TKafkaSource`, which reads the partitions of a topic.
- The asynchronous sink `NYT::NFlow::TKafkaSink`.
- The synchronous sink `NYT::NFlow::TAtLeastOnceKafkaSink`.
- The resource `NYT::NFlow::TKafkaClient`, which holds the connection settings the source and sinks share.

You can find the extension code [here]({{source-root}}/yt/yt/flow/extensions/kafka).

## Connect to Kafka {#client}

Declare the client resource in the `resources` section of `Spec`. Kafka sources and sinks look it up by the ID `KafkaClient`, so the resource must have exactly this name:

```yson
"resources" = {
    "KafkaClient" = {
        "resource_class_name" = "NYT::NFlow::TKafkaClient";
        "parameters" = {
            "bootstrap_servers" = "kafka-1.example.net:9093,kafka-2.example.net:9093";
            "cluster_name" = "events";
            "security_protocol" = "SASL_SSL";
            "sasl_mechanism" = "SCRAM-SHA-512";
            "sasl_username" = "flow-pipeline";
            "ssl_ca_location" = "/etc/ssl/certs/ca-certificates.crt";
        };
        "dependencies" = {};
    };
};
```

Every computation with a Kafka source or sink lists the resource in `required_resource_ids`, on both the controller and the workers:

```yson
"required_resource_ids" = {
    "KafkaClient" = {
        "controller" = %true;
        "worker" = %true;
    };
};
```

The SASL password is never stored in `Spec`. The controller and worker processes read it from the environment variable named by `sasl_password_env`, `KAFKA_SASL_PASSWORD` by default. In a Vanilla operation, pass the variable to the jobs with `secret_env`; see [Secrets](../../../flow/devops/vanilla/security.md#secrets). Other librdkafka properties, for example `compression.type` or `partitioner`, go to `extra_config`. The properties the connector manages itself, such as `group.id`, `enable.idempotence`, or `bootstrap.servers`, are rejected there.

{% note warning %}

The partition keys of Kafka sources, and so the offsets the pipeline persists, are derived from `cluster_name`, or from `bootstrap_servers` while `cluster_name` is empty. Set `cluster_name` before the broker list changes: to keep the keys of a running pipeline, set it to the current value of `bootstrap_servers` first. Changing the broker list while `cluster_name` is empty makes the sources start over from the offsets of new keys.

{% endnote %}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TKafkaClientConfig.md) %}

## Read from Kafka {#source}

Declare the source in the `source_streams` section of a source computation, for example `NYT::NFlow::TProcessFunctionSourceComputation`:

```yson
"computations" = {
    "reader" = {
        "computation_class_name" = "NYT::NFlow::TProcessFunctionSourceComputation";
        "processing_function" = "TParseEvents";
        "output_stream_ids" = ["events"];
        "required_resource_ids" = {
            "KafkaClient" = {
                "controller" = %true;
                "worker" = %true;
            };
        };
        "source_streams" = {
            "kafka" = {
                "source_class_name" = "NYT::NFlow::TKafkaSource";
                "parameters" = {
                    "topic" = "events";
                    "group_id" = "flow-events";
                };
            };
        };
    };
};
```

Each Kafka partition is read by its own Flow partition. The controller refreshes the partition count of the topic every `update_partition_count_period`, and new Kafka partitions become new source partitions. `partition_filter` restricts the source to some of the partitions.

{% note warning %}

Each partition being read has its own librdkafka consumer and its own thread. A partition takes about one and a half `max_buffer_bytes` of memory: the connector's buffer plus librdkafka's internal queue, which is capped at half of it. Plan memory and the number of workers by the number of partitions.

{% endnote %}

The source emits one Flow message per Kafka record with the following columns:

#|
|| **Column** | **Type** | **Content** ||
|| `data` | `string` | Record value. Null for a record with a null value, which is a tombstone in a compacted topic, and for a record kept by `malformed_message_policy = keep`. ||
|| `key` | `string` | Record key. Null for a record without a key. ||
|| `unparsed` | `any` | For a record kept by `malformed_message_policy = keep`, a YSON map `{data = <record value>}` that holds the record value as is, without unpacking. Null otherwise. ||
|| `partition` | `int64` | Index of the Kafka partition of the record. ||
|| `offset` | `int64` | Offset of the record. ||
|| `sub_offset` | `int64` | Index of the message within its record. Always 0 for `TKafkaSource`, which emits one message per record; sources derived from it may split a record into several messages. ||
|| `timestamp` | `int64` | Record timestamp in seconds. ||
|#

### Offsets and the consumer group {#source-offsets}

The source keeps its position in the pipeline state: the offset of each partition is persisted with every epoch, and after a restart the source resumes from it. The consumer group `group_id` does not assign partitions. The source commits the persisted offsets to the group on a best-effort basis, so that standard Kafka tools such as `kafka-consumer-groups` show the lag of the pipeline.

With `use_consumer_group_offset` enabled, which is the default, the committed offset of the group can move a partition forward. When a partition starts, the source compares the group offset with the persisted one:

- A group offset ahead of the persisted one is adopted, and the records in between are skipped. This is how an operator moves a partition, for example with `kafka-consumer-groups --reset-offsets --to-latest`, while the pipeline is stopped.
- A group offset behind the persisted one is ignored, so it cannot move a partition back. To read records again, reset the read position; see [Reset the read position](#source-reset).
- A group offset past the end of the partition stops the partition until the group offset is reset.

Disable `use_consumer_group_offset` when several pipelines or other consumers share the group.

### Deleted and recreated data {#source-retention}

Records that retention deleted before the source read them are skipped: the source continues from the earliest available offset.

The source notices that a topic was recreated or truncated under the same name only when it sees the latest offset of a partition below the offset the pipeline has already read. The source checks the latest offset every `watermark_update_period`. When it notices this, the source stops reading the partition with an error instead of reading the new topic from a wrong position. While the partition is stopped, the sensor `/status_profiler/broken{path="/sources/<source_stream>/topic_identity"}` is `1`, where `<source_stream>` is the name of the source in `source_streams`; you can set an alert on it.

If the new topic gathers at least as many records as the pipeline has read from the old one before the check, the source does not notice the recreation: it continues from the persisted offset and skips the beginning of the new topic. Therefore, reset the read position right after recreating a topic instead of waiting for the error.

### Reset the read position {#source-reset}

To read records again or to recover partitions after a topic was recreated, reset the offsets the source keeps in the pipeline state. The procedure below applies to `TProcessFunctionSourceComputation` and requires a successful stop through drain.

If topic data loss prevents drain from rereading previously published messages, pause the old pipeline and create a new one with a separate `group_id`. A pipeline in the `Completed` state cannot be restarted either: create a new pipeline to read again.

1. Stop the pipeline and wait for the `Stopped` state.
2. Use `read-states` from the [Flow CLI](../../../flow/tools/cli.md) with the filters `--computation-id <computation with the source>`, `--target key_state`, and `--name '/$active_source/v0'`. Set `--limit` high enough to include all required partitions; the default is 10 rows. These states hold the offsets for `TProcessFunctionSourceComputation`. In the `key_states` section, select the partitions by `key`: the first element is the source name in `source_streams`, and the last is the Kafka partition index.
3. For each selected full `key`, run `delete-states` with the same `--computation-id`, `--target`, and `--name`, and add `--key` with the complete value from `read-states`. Without `--commit`, the command only counts the rows it would delete. Verify that it matches one row in `matched_states.key_states` and none in the other sections, then repeat with `--commit`. Keep the other computation states: other sources may use the same state name.
4. If `use_consumer_group_offset` is enabled, move the offsets of the group `group_id` to where reading should start, for example to the beginning of the partitions with `kafka-consumer-groups --reset-offsets --to-earliest` (see the [Kafka documentation](https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups)). Otherwise, the source returns to the offsets it committed last.
5. Start the pipeline and check the read position in the `partition` and `offset` columns of the source's output messages. A partition without a persisted offset starts from the group offset if it exists and using it is enabled, or from the earliest available offset otherwise.

### Finite reading {#source-finite}

With `finite = %true`, the source completes the stream once it catches up with the current end of the partitions. This is not a snapshot of the topic at startup: the source may also read newer records. If producers keep adding data, reading may never complete.

### Static spec {#source-static-spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TKafkaSource.md) %}

### Dynamic spec {#source-dynamic-spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TKafkaSource.md) %}

## Write to Kafka {#sinks}

Both sinks take one input stream. The bytes of the `payload_column` column become the record value, and a null value writes a Kafka null value, which is a tombstone in a compacted topic. The optional `key_column` sets the record key; without a key, librdkafka spreads records across partitions. `message_id_header` adds a header with the Flow message ID; see [At least once](#at-least-once) for deduplication limitations.

#|
|| **Sink** | **Writes** | **Guarantee** ||
|| `NYT::NFlow::TKafkaSink` | Asynchronously: after the epoch commits. | Exactly once by default, at least once with `delivery_guarantee = at_least_once`. ||
|| `NYT::NFlow::TAtLeastOnceKafkaSink` | Synchronously: every epoch waits until Kafka acknowledges its records. | At least once. ||
|#

Declare the sink in the `sinks` section of the computation that produces the stream it writes. For example:

```yson
"computations" = {
    "<computation>" = {
        "required_resource_ids" = {
            "KafkaClient" = {
                "controller" = %true;
                "worker" = %true;
            };
        };
        "sinks" = {
            "kafka" = {
                "sink_class_name" = "NYT::NFlow::TKafkaSink";
                "input_stream_ids" = ["results"];
                "parameters" = {
                    "topic" = "results";
                    "payload_column" = "value";
                    "key_column" = "key";
                    "message_id_header" = "flow-message-id";
                };
            };
        };
    };
};
```

{% note info %}

The connector keeps the librdkafka default partitioner, `consistent_random` (CRC32), with no special advantage for Flow. The Java client uses murmur2, so equal keys may land in different partitions. For compatibility with Java producers, set `"partitioner" = "murmur2_random"` in the client resource's `extra_config`.

{% endnote %}

### Static spec of TKafkaSink {#sink-static-spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TKafkaSink.md) %}

### Dynamic spec of TKafkaSink {#sink-dynamic-spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TKafkaSink.md) %}

### Static spec of TAtLeastOnceKafkaSink {#at-least-once-sink-static-spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TAtLeastOnceKafkaSink.md) %}

## Delivery guarantees {#guarantees}

### At least once {#at-least-once}

`TAtLeastOnceKafkaSink`, and `TKafkaSink` with `delivery_guarantee = at_least_once`, use an idempotent producer: retries are deduplicated only within one producer session. After a failure, the job restarts and retries unacknowledged writes, so Kafka may receive duplicates.

The header named by `message_id_header` contains the Flow message ID. Whether it supports deduplication depends on how the message reaches the sink again:

- The asynchronous `TKafkaSink` replays messages from committed epochs with their original IDs. Transform output is stored in `output_messages`; Swift recomputes it and must meet the [determinism requirements](../../../flow/concepts/guarantees.md#determinism). Consumers can use the header to drop these duplicates.
- The synchronous `TAtLeastOnceKafkaSink` may write a message before an epoch commit fails. On recomputation, a Transform computation may assign a new ID to the same result, so the header does not guarantee deduplication. For this case, include a stable application ID in the message payload and use it on the consumer side.

### Exactly once {#exactly-once}

`TKafkaSink` delivers exactly once by default. This requires transaction support on the brokers, extra permissions, and consumer settings; see the requirements below. If they cannot be met, set `delivery_guarantee = at_least_once`.

In exactly-once mode, the sink writes records in Kafka transactions:

1. Each sink partition gets a transactional ID: `transactional_id_prefix` followed by the partition's producer ID, which is persisted in the sink state and survives restarts.
2. When a sink partition starts, it fences the writers of its earlier sessions and aborts the transaction they left open. A stale job that tries to write afterwards fails.
3. Every transaction writes a batch of records together with a progress marker: the offset of the consumer group named after the transactional ID, on partition 0 of the topic. The offset is the sequence number of the last message the transaction wrote; the sink numbers the messages of a partition consecutively.
4. Flow learns that messages are delivered only after their transaction commits. After a restart, the sink reads the marker and skips the replayed messages that are already committed.

Requirements:

- Consumers must read with `isolation.level = read_committed`, or they also see the records of aborted transactions. librdkafka uses `read_committed` by default, but many other clients, including the Java client, default to `read_uncommitted`.
- The brokers must support transactions, and their `transaction.max.timeout.ms` must not be less than `transaction_timeout`.
- The principal of the sink needs the following permissions. Grant the first two with prefixed ACLs for transactional IDs and consumer groups whose names start with `transactional_id_prefix`.

  #|
  || **Resource** | **Operations** ||
  || Transactional IDs starting with `transactional_id_prefix` | `Write`, `Describe` ||
  || Consumer groups starting with `transactional_id_prefix` | `Read` ||
  || The topic | `Write`, `Describe`, `Read` ||
  |#

- Do not delete or reset the offsets of the marker groups.
- After a restart, Flow must hand the sink the same messages in the same order, because the marker identifies messages by their sequence numbers. Flow does so when the upstream computations meet the [determinism requirements](../../../flow/concepts/guarantees.md#determinism).

A transaction writes at most `max_transaction_record_count` records and `max_transaction_byte_size` bytes. The exception is a record that is larger than `max_transaction_byte_size` by itself: the sink writes it in a transaction of its own.

The sink handles errors by their kind:

- It retries a Kafka request that fails with a retriable error without aborting the transaction, up to ten attempts in a row.
- When an error requires aborting the transaction, the sink aborts it and writes the same records again, in half as large a batch. After ten such errors in a row, the sink fails the job.
- A fatal error fails the job immediately. For example, this happens when the producer rejects a record larger than its `message.max.bytes` or when a newer session has fenced the writer.

The restarted job recovers from the marker. A record the producer or the broker rejects therefore keeps failing the job until the limit or the record changes.

Change `delivery_guarantee` and `transactional_id_prefix` only while the pipeline is stopped. When a sink switches from `at_least_once` to `exactly_once`, the messages that were in flight at the switch may be written twice.

### Lost progress marker {#lost-marker}

The marker is lost when the offset of its consumer group expires, which happens after the broker's `offsets.retention.minutes` (7 days by default) without commits, or when the group is deleted or its offset reset. The sink then cannot tell which of the messages the pipeline has not acknowledged are already in Kafka.

If such messages may exist, the sink partition refuses to start with the error `Kafka sink progress marker cannot be trusted`, and its job keeps restarting without writing anything. This happens only when both conditions hold: the sink partition stopped with unacknowledged messages, for example after a crash, and its marker was lost before it started again. A graceful stop of the pipeline leaves no unacknowledged messages, and a sink partition without them starts normally even without a marker.

To continue, accept that those messages are written again and may be duplicated: set the dynamic parameter `allow_missing_progress_marker` to `%true` in `DynamicSpec` at `computations/<computation>/sinks/<sink>/parameters`. The parameter is read when a sink partition starts, and the failing partitions restart on their own. Once the pipeline has recovered, set it back to `%false`.

If long pipeline stops are expected, raise the broker's `offsets.retention.minutes` above their duration.

## Troubleshooting {#troubleshooting}

#|
|| **Error text** | **Cause and fix** ||
|| `The "extra_config" key "<key>" is managed by the connector and cannot be overridden` | `extra_config` sets a property the connector manages. Remove it from `extra_config` and use the corresponding parameter of the resource, if there is one. ||
|| `Kafka metadata request for topic "<topic>" failed` | The topic does not exist, or the principal lacks `Describe` on it. Create the topic or grant the permission. ||
|| `Kafka consumer group offset is beyond the topic end; reset the group offsets to restart the partition` | The committed offset of `group_id` is past the end of the partition, for example after the topic was recreated. Move the group offsets with `kafka-consumer-groups --reset-offsets` (see the [Kafka documentation](https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups)), or disable `use_consumer_group_offset`. ||
|| `Kafka high watermark is below the persisted offset; was the topic recreated?` | The topic was recreated or truncated under the same name, and the source stopped reading the partition. Reset the read position; see [Reset the read position](#source-reset). ||
|| `Kafka sink progress marker cannot be trusted` | The progress marker of an exactly-once sink partition is missing, reset, or inconsistent with the pipeline state while unacknowledged messages may be committed. See [Lost progress marker](#lost-marker). ||
|| `Kafka transaction failed` with `Message size too large` | A record exceeds a message size limit. If the error contains `Failed to produce Kafka message`, the producer rejected the record: raise `message.max.bytes` in the `extra_config` of the client resource (1000000 bytes by default). Otherwise, the broker rejected it: raise the topic's `max.message.bytes`. A record must fit both limits. You can also make the records smaller. ||
|| `This instance has been fenced by a newer instance` | A stale job of an exactly-once sink partition tried to write after its replacement started. No action is needed. ||
|#

## See also {#see-also}

- [List of extensions](../../../flow/extensions/about.md)
- [Kafka extension guarantees](../../../flow/concepts/guarantees.md#kafka-guarantees)
- [Resources](../../../flow/cpp/resources.md)
- [Spec and DynamicSpec](../../../flow/concepts/spec.md)
