## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbFramingSink&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbFramingSink}
<!-- This page is generated. -->
Источник: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Динамические параметры `TYdbFramingSink`.

#|
|| **Параметр** | **Описание** ||
|| `flow_queue_meta_heartbeat_period` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `10s`
Период, с которым контроллер пишет heartbeat с метой Flow во все партиции топика. Используется только при включённом `write_flow_queue_meta`. ||
|| `max_rows_per_batch` | **Тип**: `long`
**Значение по умолчанию**: `1000`
Максимальное число сообщений Flow в батче: batching sink сбрасывает накопленный батч при достижении этого порога. Допустимый диапазон — от 1 до 2000. ||
|| `max_bytes_per_batch` | **Тип**: `long`
**Значение по умолчанию**: `5242880`
Максимальный суммарный размер сообщений Flow в батче: batching sink сбрасывает накопленный батч при достижении этого порога. Допустимый диапазон — от 1 байта до 10 MiB. ||
|#

