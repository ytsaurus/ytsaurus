## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbTwoPhaseCommitSink&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbTwoPhaseCommitSink}
<!-- This page is generated. -->
Источник: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Динамические параметры `TYdbTwoPhaseCommitSink`.

#|
|| **Параметр** | **Описание** ||
|| `flow_queue_meta_heartbeat_period` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `10s`
Период, с которым контроллер пишет heartbeat с метой Flow во все партиции топика. Используется только при включённом `write_flow_queue_meta`. ||
|| `max_rows_per_batch` | **Тип**: `long`
**Значение по умолчанию**: `1000`
Максимальное число сообщений Flow в одной записи YDB при заданном `framing_format`. Без framing игнорируется. Допустимый диапазон — от 1 до 2000. ||
|| `max_bytes_per_batch` | **Тип**: `long`
**Значение по умолчанию**: `5242880`
Суммарный размер сообщений Flow, при достижении которого сбрасывается framing-батч. Без framing игнорируется. Допустимый диапазон — от 1 байта до 10 MiB. ||
|| `preparation_attempt_timeout` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `90s`
Время на подготовку одной ещё не опубликованной группы сообщений и получение всех подтверждений записи в федерации. Значение должно быть больше нуля. В однокластерном режиме параметр игнорируется. ||
|| `preparation_target_backoff` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `30s`
Время, на которое недоступный кластер исключается после превышения времени подготовки. Это не задерживает переход на другой кластер. В однокластерном режиме параметр игнорируется. ||
|#

