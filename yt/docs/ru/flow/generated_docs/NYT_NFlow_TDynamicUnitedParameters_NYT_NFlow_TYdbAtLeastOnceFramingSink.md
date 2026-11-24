## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbAtLeastOnceFramingSink&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbAtLeastOnceFramingSink}
<!-- This page is generated. -->
Источник: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Динамические параметры `TYdbAtLeastOnceFramingSink`.

#|
|| **Параметр** | **Описание** ||
|| `flow_queue_meta_heartbeat_period` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `10s`
Период, с которым контроллер пишет heartbeat с метой Flow во все партиции топика. Используется только при включённом `write_flow_queue_meta`. ||
|| `max_rows_per_batch` | **Тип**: `long`
**Значение по умолчанию**: `1000`
Максимальное число сообщений Flow, непосредственно упаковываемых framing sink-ом в одно сообщение YDB. При достижении порога батч отправляется; допустимый диапазон — от 1 до 2000. ||
|| `max_bytes_per_batch` | **Тип**: `long`
**Значение по умолчанию**: `5242880`
Порог суммы оценок памяти `TOutputMessage::ByteSize` для сообщений Flow, упаковываемых в одно сообщение YDB. Синк проверяет порог после добавления каждого сообщения, поэтому последнее сообщение может привести к его превышению. Оценка учитывает метаданные и все колонки payload, но не служебные байты framing и не задаёт строгий предел размера сообщения при передаче. Допустимый диапазон — от 1 байта до 10 MiB. ||
|#

