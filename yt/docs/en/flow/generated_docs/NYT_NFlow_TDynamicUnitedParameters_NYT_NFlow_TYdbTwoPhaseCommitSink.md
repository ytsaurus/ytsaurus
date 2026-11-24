## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbTwoPhaseCommitSink&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbTwoPhaseCommitSink}
<!-- This page is generated. -->
Source: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Dynamic parameters of `TYdbTwoPhaseCommitSink`.

#|
|| **Parameter** | **Description** ||
|| `flow_queue_meta_heartbeat_period` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `10s`
Period at which the controller writes a Flow metadata heartbeat to every topic partition. Used only when `write_flow_queue_meta` is enabled. ||
|| `max_rows_per_batch` | **Type**: `long`
**Default value**: `1000`
Maximum number of Flow messages packed into one YDB record when `framing_format` is set. Ignored without framing. The valid range is 1 through 2000. ||
|| `max_bytes_per_batch` | **Type**: `long`
**Default value**: `5242880`
Total Flow message size at which a framing batch is flushed. Ignored without framing. The valid range is 1 byte through 10 MiB. ||
|| `preparation_attempt_timeout` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `90s`
Time allowed to prepare one uncommitted publication and receive all write acknowledgements in a federation. Must be greater than zero. Ignored in single-cluster mode. ||
|| `preparation_target_backoff` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `30s`
How long the unavailable cluster is excluded after a preparation timeout. This does not delay switching to another target. Ignored in single-cluster mode. ||
|#

