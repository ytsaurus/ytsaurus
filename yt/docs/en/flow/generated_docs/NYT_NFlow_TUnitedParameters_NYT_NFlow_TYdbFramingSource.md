## NYT::NFlow::TUnitedParameters&lt;NYT::NFlow::TYdbFramingSource&gt; {#NYT_NFlow_TUnitedParameters_NYT_NFlow_TYdbFramingSource}
<!-- This page is generated. -->
Source: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Unpacks a YDB topic message into multiple Flow messages in the selected framing format.

#|
|| **Parameter** | **Description** ||
|| `finite` | **Type**: `bool`
**Default value**: `false`
Treat the source as finite: transition the stream to the completed state after reading all available messages. How the end of the data is determined depends on the connector. ||
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
|| `consumer` | **Type**: `std::string`
**Required parameter**
Name of an existing YDB consumer. If its committed position is ahead of the position in Flow state, reading may start from that later position. ||
|| `partition_filter` | **Type**: `std::optional<std::vector<std::pair<int, int>>>`
List of half-open partition-index intervals `[begin, end)` that the source should read. Every interval must satisfy `0 <= begin < end`; all other partitions are filtered out. ||
|| `framing_format` | **Type**: [NYT::NFlow::EFramingFormat](./all_yson_structs#NYT_NFlow_EFramingFormat)
**Default value**: `auto`
Format used to unpack one YDB message into multiple Flow messages. The `auto` value detects `protoseq` or `lenval` from the contents; it cannot detect `light_protoseq`. ||
|#


{% cut "**Additional parameters**" %}


#|
|| `update_info_period` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `15s`
Period for refreshing the source's auxiliary partition information. A connector may use this tick for status requests and read-session liveness checks. ||
|| `byte_size_alpha` | **Type**: `double`
**Default value**: `0.05`
Exponential smoothing coefficient for the average byte total and message count per offset: larger values make the estimate react to new data faster. ||
|| `max_memory_usage_bytes` | **Type**: `long`
**Default value**: `52428800`
YDB read-session memory limit for buffering messages that have not yet been processed. ||
|| `start_session_timeout` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `5m`
Maximum time allowed for establishing a read session before the attempt is considered failed. ||
|| `try_parse_flow_queue_meta` | **Type**: `bool`
**Default value**: `true`
Parse the `flow_queue_meta_name` attribute as Flow metadata and restore the event timestamp and watermark propagated by the preceding pipeline. ||
|| `flow_queue_meta_name` | **Type**: `std::string`
**Default value**: `_ytflow_queue_meta`
Name of the YDB message attribute containing Flow metadata. When the standard name is used, the source also accepts the legacy `flow_queue_meta` attribute, allowing the reading pipeline to be upgraded first. ||
|| `ignore_malformed_flow_queue_meta` | **Type**: `bool`
**Default value**: `false`
On a Flow metadata parsing error, log a warning and process the message without the metadata. If disabled, malformed metadata causes a read error. ||
|| `attributes_column` | **Type**: `std::optional<std::string>`
Column to materialize the attributes of the read YDB message into, as a yson map (`{name -> string}`). Read it with `NYT::NFlow::UnpackYdbTopicAttributes()`. Flow-owned attributes (the `_ytflow_` prefix and legacy `flow_queue_meta`) and attributes with the YDB-interpreted `__` prefix are omitted. The exception is `__key`: YDB stores the Kafka record key there, so the source preserves it. If a name occurs more than once, the first value is kept.

The column is not created unless the parameter is set. Its name must not collide with the fixed `data`, `unparsed`, `offset`, or `sub_offset` columns. ||
|#


{% endcut %}
