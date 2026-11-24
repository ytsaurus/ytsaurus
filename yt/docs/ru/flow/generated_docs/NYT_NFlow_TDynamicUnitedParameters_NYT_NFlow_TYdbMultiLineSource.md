## NYT::NFlow::TDynamicUnitedParameters&lt;NYT::NFlow::TYdbMultiLineSource&gt; {#NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TYdbMultiLineSource}
<!-- This page is generated. -->
Источник: [yt/yt/flow/library/cpp/common/registry-inl.h]({{source-root}}/yt/yt/flow/library/cpp/common/registry-inl.h)

Динамические параметры `TYdbMultiLineSource`.

#|
|| **Параметр** | **Описание** ||
|| `malformed_message_policy` | **Тип**: [NYT::NFlow::EMalformedYdbMessagePolicy](./all_yson_structs#NYT_NFlow_EMalformedYdbMessagePolicy)
**Значение по умолчанию**: `fail`
Политика обработки сообщения, которое не удалось распаковать или разделить в выбранном формате: сохранить в `unparsed`, пропустить либо завершить обработку ошибкой. ||
|| `read_from_instant` | **Тип**: `std::optional<TInstant>`
Момент в формате ISO-8601, раньше которого сообщения по их `WriteTimestamp` пропускаются при чтении. ||
|| `max_time_lag` | **Тип**: `std::optional<`[TDuration](./all_yson_structs#TDuration)`>`
Максимальный возраст читаемых сообщений относительно текущего времени. Сессия чтения YDB пропускает более старые сообщения; значение задаётся как `TDuration`, например `1h`. ||
|| `commit_timeout` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `2m`
Время ожидания подтверждения сохранения позиции через сессию чтения. Если подтверждение не пришло, выполняется отдельный блокирующий `CommitOffset`. ||
|#


{% cut "**Дополнительные параметры**" %}


#|
|| `unavailable_threshold` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `5m`
Сколько времени подряд источник должен быть недоступен, чтобы партиция считалась стабильно недоступной. Засчитывается только то время, когда джоб работал и видел ошибку: простой между перезапусками в него не попадает, а любой успешный ответ источника обнуляет накопленное. ||
|| `session_silence_timeout` | **Тип**: [TDuration](./all_yson_structs#TDuration)
**Значение по умолчанию**: `1m`
Сколько установленная сессия чтения может не подавать признаков жизни, прежде чем будет признана зависшей и пересоздана. Признак жизни — любое входящее событие от YDB: сообщение, ответ на запрос статуса партиции или подтверждение коммита. Ответы на запрос статуса приходят раз в `update_info_period`, поэтому фактически используется большее из заданного значения и трёх таких периодов. ||
|#


{% endcut %}
