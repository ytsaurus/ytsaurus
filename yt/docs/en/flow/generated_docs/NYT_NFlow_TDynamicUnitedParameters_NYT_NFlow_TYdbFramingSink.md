## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbFramingSink&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbFramingSink}
<!-- This page is generated. -->
Source: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Dynamic parameters of `TYdbFramingSink`.

#|
|| **Parameter** | **Description** ||
|| `flow_queue_meta_heartbeat_period` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `10s`
Period at which the controller writes a Flow metadata heartbeat to every topic partition. Used only when `write_flow_queue_meta` is enabled. ||
|| `max_rows_per_batch` | **Type**: `long`
**Default value**: `1000`
Maximum number of Flow messages in a batch: the batching sink flushes the accumulated batch when it reaches this threshold. The valid range is 1 through 2000. ||
|| `max_bytes_per_batch` | **Type**: `long`
**Default value**: `5242880`
Maximum total size of Flow messages in a batch: the batching sink flushes the accumulated batch when it reaches this threshold. The valid range is 1 byte through 10 MiB. ||
|#

