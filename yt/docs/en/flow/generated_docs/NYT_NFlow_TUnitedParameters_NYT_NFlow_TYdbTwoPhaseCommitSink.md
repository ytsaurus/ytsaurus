## NYT::NFlow::TUnitedParameters&lt;NYT::NFlow::TYdbTwoPhaseCommitSink&gt; {#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbTwoPhaseCommitSink}
<!-- This page is generated. -->
Source: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Prepares a deferred publication in YDB and publishes messages after the Flow epoch commits. Requires server support for deferred publication; conventional topic transactions are insufficient.

#|
|| **Parameter** | **Description** ||
|| `endpoint` | **Type**: `std::string`
**Required parameter**
YDB server endpoint used to discover the topic. Specify it explicitly, for example `localhost:2135`. ||
|| `database` | **Type**: `std::string`
**Required parameter**
Path of the YDB database containing the topic, for example `/Root`. Must be specified explicitly. ||
|| `topic` | **Type**: `std::string`
**Required parameter**
Path of the topic to read or write, relative to `database`. ||
|| `client_controller_timeout` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `30s`
Timeout for controller requests to discover clusters and describe the topic through the YDB SDK. ||
|| `update_partition_count_period` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `1m`
Period at which the controller refreshes cluster and topic partition information. ||
|| `payload_column` | **Type**: `std::string`
**Default value**: `data`
Input-stream column containing the serialized bytes that the sink writes to YDB. ||
|| `codec` | **Type**: `std::string`
**Default value**: `zstd`
YDB message compression codec. The bundled public SDK supports `raw`, `gzip`, and `zstd` by default. The parser also accepts `lzop`, but the SDK has no default LZOP encoder; using it requires the application to register a compatible codec implementation through `NYdb::NTopic::TCodecMap::Set` and the server to support that codec. ||
|| `compression_level` | **Type**: `int`
**Default value**: `4`
Compression level that the YDB SDK uses with the selected `codec`. ||
|| `write_flow_queue_meta` | **Type**: `bool`
**Default value**: `false`
Write Flow metadata carrying the event timestamp and watermark into every message, and send pure heartbeat messages so the watermark can advance when there is no data. ||
|| `framing_format` | **Type**: `std::optional<`[NYT::NFlow::EFramingFormat](./all_yson_structs#NYT_NFlow_EFramingFormat)`>`
Optional format for packing several Flow messages into one YDB record. If `framing_format` is unset, each message is written separately and `attributes_column` may be configured. When `framing_format` is set, `attributes_column` is not supported. ||
|#


{% cut "**Additional parameters**" %}


#|
|| `attributes_column` | **Type**: `std::optional<std::string>`
Column holding a yson map of user attributes (`{name -> string}`) written into the YDB message attributes next to `flow_queue_meta_name`. Build its value with `NYT::NFlow::PackYdbTopicAttributes()`, which validates reserved names and the message metadata limits. The sink validates the final metadata again before sending the message.

If the parameter is not set or the column value is `null`, no user attributes are written. Flow-owned names (the `_ytflow_` prefix and legacy `flow_queue_meta`) and names with the YDB-interpreted `__` prefix are rejected. The exception is `__key`: YDB uses it as the Kafka record key, so it is allowed. When Flow metadata is enabled, a user attribute cannot collide with `flow_queue_meta_name`. Framing sinks reject `attributes_column`, because a YDB message assembled from multiple Flow messages has no unambiguous attribute set. ||
|| `max_memory_usage_bytes` | **Type**: `long`
**Default value**: `52428800`
YDB write-session memory limit for buffering messages awaiting delivery. ||
|| `flow_queue_meta_name` | **Type**: `std::string`
**Default value**: `_ytflow_queue_meta`
Name of the YDB message attribute into which the sink writes Flow metadata. The reading pipeline must use the same name. ||
|#


{% endcut %}
