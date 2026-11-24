## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbAtLeastOnceFramingSink&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbAtLeastOnceFramingSink}
<!-- This page is generated. -->
Source: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Dynamic parameters of `TYdbAtLeastOnceFramingSink`.

#|
|| **Parameter** | **Description** ||
|| `flow_queue_meta_heartbeat_period` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `10s`
Period at which the controller writes a Flow metadata heartbeat to every topic partition. Used only when `write_flow_queue_meta` is enabled. ||
|| `max_rows_per_batch` | **Type**: `long`
**Default value**: `1000`
Maximum number of Flow messages packed directly by the framing sink into one YDB message. Reaching the threshold sends the batch; the valid range is 1 through 2000. ||
|| `max_bytes_per_batch` | **Type**: `long`
**Default value**: `5242880`
Threshold for the sum of `TOutputMessage::ByteSize` memory estimates of Flow messages packed into one YDB message. The sink checks the threshold after adding each message, so the final message can make the sum exceed it. The estimate includes metadata and all payload columns; it excludes framing overhead and is not a strict on-wire size limit. The valid range is 1 byte through 10 MiB. ||
|#

