## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbMultiLineSource&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbMultiLineSource}
<!-- This page is generated. -->
Source: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Dynamic parameters of `TYdbMultiLineSource`.

#|
|| **Parameter** | **Description** ||
|| `malformed_message_policy` | **Type**: [NYT::NFlow::EMalformedYdbMessagePolicy](./all_yson_structs#NYT_NFlow_EMalformedYdbMessagePolicy)
**Default value**: `fail`
Policy for a message that cannot be decompressed or split in the selected format: retain it in `unparsed`, drop it, or fail processing. ||
|| `read_from_instant` | **Type**: `std::optional<TInstant>`
ISO-8601 instant before which messages are skipped according to their `WriteTimestamp`. ||
|| `max_time_lag` | **Type**: `std::optional<`[TDuration](./all_yson_structs#TDuration)`>`
Maximum age of messages to read relative to the current time. The YDB read session skips older messages; specify the value as a `TDuration`, for example `1h`. ||
|| `commit_timeout` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `2m`
Time to wait for an offset commit acknowledgement through the read session before issuing a separate blocking `CommitOffset` as a recovery fallback. ||
|#


{% cut "**Additional parameters**" %}


#|
|| `unavailable_threshold` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `5m`
How long the source must be continuously unavailable for the partition to count as stably unavailable. Only time during which the job was running and seeing the failure counts: a gap between restarts is not charged, and any successful answer from the source resets the total. ||
|| `session_silence_timeout` | **Type**: [TDuration](./all_yson_structs#TDuration)
**Default value**: `1m`
How long an established read session may show no sign of life before it is considered wedged and recreated. A sign of life is any inbound YDB event: a message, an answer to a partition status request, or a commit acknowledgement. Status answers arrive once per `update_info_period`, so the greater of the configured value and three such periods is used. ||
|#


{% endcut %}
