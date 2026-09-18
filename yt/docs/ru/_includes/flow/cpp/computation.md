# Computation в {{product-name}} Flow (C++)

{% note info %}

На этой странице описаны режимы исполнения Computation на C++. Языконезависимое описание концепции см. в разделе [Computation](../../../flow/concepts/computation.md).

{% endnote %}

{% note warning %}

Новую пользовательскую логику на C++ реализуйте только как [process function](../../../flow/cpp/process-functions.md). Не создавайте классы-наследники от базовых классов `Computation`. Эта страница описывает режимы исполнения, встроенные компьютейшены и low-level API, необходимый для устройства фреймворка и сопровождения legacy-кода.

{% endnote %}

В этом разделе приведено описание базовых классов `Computation`, на которых работают встроенные адаптеры process function, и важные детали режимов исполнения.

Во Flow в данный момент реализовано четыре базовых класса `Computation`:

- `TTransformComputation`
- `TTransformOrderedSourceComputation`
- `TSwiftOrderedSourceComputation`
- `TSwiftMapComputation`

Классы, содержащие `Swift` в названии, реализуют принцип [Swift](../../../flow/concepts/swift.md). Подробнее — в разделе [Swift](../../../flow/concepts/swift.md).

## Общее

Следующие детали прямого API нужны при сопровождении фреймворка и существующего legacy-кода; они не являются инструкцией по созданию нового пользовательского компьютейшена.

- Параметры process function объявляйте обычной `TYsonStruct`, указывайте её тип в `YT_FLOW_DEFINE_PROCESS_FUNCTION` и передавайте значения через `processing_function_parameters`.

- В пользовательской process function сложную инициализацию выполняйте в `Init(const IRuntimeInitContextPtr&)`.
- Выберите одну гранулярность обработки: `IProcessFunction` для сообщений/таймеров/визитов по одному, `IBatchProcessFunction` для всей эпохи или `IKeyedBatchProcessFunction` для батча одного ключа.
- Для ручной записи в транзакцию transform-режима дополнительно реализуйте `ISyncProcessFunction`; в остальных случаях используйте [Sink](../../../flow/concepts/glossary.md#sink) или [ExternalState](../../../flow/cpp/state.md#external-state).
- Выходные сообщения и таймеры добавляйте через `IOutputCollector`; создавайте и конвертируйте их через `IRuntimeContext`.
- Код одной партиции выполняется строго однопоточно. Распараллеливайте обработку увеличением числа [партиций](../../../flow/concepts/glossary.md#partition).
- Есть возможность конвертировать входные сообщения в `NYTree::TYsonStruct`. Для этого необходимо:
  * Завести класс-наследник от `TYsonMessage` (это специальный наследник `NYTree::TYsonStruct`).
  * Зарегистрировать его в глобальном реестре с помощью `YT_FLOW_DEFINE_YSON_MESSAGE`.
  * В функции `main` необходимо будет создать объект типа `TSimpleSpecBuilder` и зарегистрировать в нём соответствующие `stream_id`.
  * При использовании `TSimpleRunnerProgram` можно передать данный `TSimpleSpecBuilder` сразу в конструктор `TSimpleRunnerProgram`.
  * При самостоятельной реализации `main` необходимо будет передать в `TSimpleSpecBuilder` спеки для обогащения информацией о потоках.
  * Заполнять `spec/streams` в случае использования `TYsonMessage` самостоятельно не нужно — вся информация будет выведена из зарегистрированных `TYsonMessage + stream_id` с помощью `TSimpleSpecBuilder`.
  * В process function используйте `context->ConvertToYsonMessage<T>(message)` и `context->ConvertToMessage(ysonMessage)` для преобразований `TMessage => TYsonMessage` и обратно.

### OutputCollector {#output-collector}

Объект `IOutputCollectorPtr output` передаётся в методы process function и предназначен для отправки результатов обработки:

| Метод | Описание |
| --- | --- |
| `output->AddMessage(message)` | Добавить выходное сообщение (объект `TMessage`, полученный через `context->MakeOutputMessageBuilder().Finish()`) |
| `output->AddTimer(timer)` | Добавить [таймер](../../../flow/concepts/glossary.md#timer) |
| `output->SetParents(parentIds)` | Задать parent ID для отслеживания [lineage](../../../flow/concepts/lineage.md). Возвращает новый `IOutputCollectorPtr` с привязанным контекстом lineage |

`SetParents` используйте, когда выходное сообщение логически произведено от конкретного подмножества входных, а не от всего батча. В небатчевых `ProcessMessage` и `ProcessTimer` lineage устанавливается автоматически.

### TMessage {#tmessage}

Структура `TMessage`, используемая в методах `ProcessMessage` и `AddMessage`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TMessageSerializer.md) %}

## TTransformComputation

Transform-режим предназначен для произвольных преобразований входных данных. Он не работает с `Source`. Результат обязательно сохраняется в YT, поэтому требований к детерминированности преобразований нет.

Свойства `TTransformComputation`:

- Может писать в YT «вхолостую», то есть без реальных изменений, перезаписывая существующее содержимое. Ожидается, что такой поток будет создавать незначительную нагрузку.

Для новой пользовательской логики этот режим выбирается адаптером `TProcessFunctionComputation`. Реализуйте `IProcessFunction`, `IBatchProcessFunction` или `IKeyedBatchProcessFunction`; для sync-фазы дополнительно реализуйте `ISyncProcessFunction`. Полный пример см. в разделе [Process function](../../../flow/cpp/process-functions.md).

### TTimer {#ttimer}

Структура `TTimer`, используемая в методах `ProcessTimer` и `AddTimer`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSerializer.md) %}

### ProcessingMode {#processing-mode}

У `TransformComputation` есть параметр `parameters/processing_mode`, позволяющий снижать гарантии процессинга в обмен на снижение нагрузки на {{product-name}}.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_EProcessingMode.md) %}

### TPassthroughComputation

Наследник `TTransformComputation`. Реализует [passthrough-компьютейшен](../../../flow/concepts/computation.md#passthrough) — создан в основном для демонстрации возможностей.

Особенности:

- Не имеет переопределяемых методов.
- Не может иметь более одного выхода.
- Все входящие `input` потоки превращаются в `output` путём конвертации сообщения с помощью `ConvertMessageToNewSchema`.

### TTransformOrderedSourceComputation {#ttransformorderedsourcecomputation}

Этот режим обрабатывает сообщения `source` произвольной пользовательской логикой: парсит, фильтрует или разворачивает одно входное сообщение в несколько выходных. Новую process function запускайте под `TProcessFunctionTransformOrderedSourceComputation`; он заменяет связку `TSwiftPassthroughOrderedSourceComputation` → `TProcessFunctionComputation`, когда единственная задача промежуточного компьютейшена — обработать данные источника.

Результат трансформации материализуется в {{product-name}} так же, как у `TTransformComputation`:

- Выходные сообщения получают уникальные `MessageId` и надёжно сохраняются в {{product-name}} до распределения в downstream. После рестарта ещё не доставленные материализованные сообщения дораспределяются с теми же `MessageId`, а не вычисляются заново, поэтому требований к детерминированности трансформации нет.
- Сообщение можно добавить в `output` с явным флагом `distribute`, например `output->AddMessage(std::move(message), /*distribute*/ false)`. Такое сообщение не публикуется в downstream, но участвует в оценке вотермарка наравне с публикуемыми: генератор вотермарка регистрирует чтение по полному набору выходных сообщений ещё до применения фильтра публикации, поэтому вотермарк можно корректно оценивать по полному потоку, даже когда значимая его часть отфильтровывается. Смещение источника в любом случае продвигается в транзакции эпохи.
- Смещение `source`, материализованный выход и стейты коммитятся в одной транзакции эпохи, поэтому обработка каждого сообщения источника применяется ровно один раз.

Для новой пользовательской логики реализуйте process function: `Init(const IRuntimeInitContextPtr&)` для инициализации, `ProcessMessage` или `Process` для обработки и, при необходимости, `ISyncProcessFunction::Sync` для ручной записи в транзакцию эпохи.

Собственный стейт process function хранит в поле `TMutableStateKeyClient<T>` (см. [Работа со стейтами](../../../flow/cpp/state.md#internal-state)), инициализирует через `initContext->InitClient(...)` и читает через `GetState(message->Key)`. Перед обработкой адаптер сам загружает стейт для ключей сообщений текущей эпохи. Инстанс компьютейшена всегда привязан к единственному `source`-ключу, поэтому все сообщения эпохи несут один и тот же ключ и обращаются к одной строке стейта.

Стейт-клиенты, созданные через `IRuntimeInitContext`, фреймворк синхронизирует в транзакции эпохи атомарно со смещением `source`, поэтому обычная мутация (например, инкремент счётчика) корректна exactly-once — дополнительная дедупликация по `MessageId` не нужна.

Спека компьютейшена проверяется при запуске; следующие поля приводят к ошибке валидации:

* `input`-стримы;
* [таймеры](../../../flow/concepts/glossary.md#timer);
* [key-visitor-стримы](../../../flow/concepts/key_visitor.md);
* непустой `group_by_schema`;
* `external_state_managers`;
* `external_state_joiners`, у которых не задан `join_on/key_schema_override` (ключ сообщения источника не описывается `group_by_schema`, поэтому схема ключа должна быть задана явно).

`watermark_strategy` поддерживается: `watermark_generator` оценивает вотермарки источника, `watermark_alignment` выравнивает чтение источника относительно других потоков (`read_delays` задерживают чтение, а не публикацию), `event_timestamp_assigner` назначает `event_timestamp` выходным сообщениям. Атомарность коммита смещения `source`, материализованных выходных сообщений и стейта от выравнивания не зависит. `skip_if_expression` также поддерживается.

`skip_if_expression` применяется до обработки, но после того, как входной батч учтён в метриках и в подсчёте опоздавших сообщений: отфильтрованное сообщение не попадает ни в стейт, ни в выход. На оценку вотермарка оно тоже не влияет: генератор регистрирует чтение только по выходным сообщениям, поэтому полностью отфильтрованный батч не сдвигает `EventWatermark`, а на длинной серии таких батчей вотермарк партиции стоит на месте. Маркеры `EventWatermark` во входных записях источник учитывает при чтении независимо от фильтра, но при `use_source_watermark = false` (значение по умолчанию) вотермарк источника только ограничивает оценку сверху и вперёд её не двигает; единственным источником вотермарка партиции он становится при `use_source_watermark = true`. Это полностью совпадает с поведением `TSwiftOrderedSourceComputation`.

Пользовательскую логику пишите как [process function](../../../flow/cpp/process-functions.md) и указывайте в спеке адаптер `NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation`: он исполняет функцию в этом режиме — с той же материализацией выхода, теми же стейтами и той же валидацией спеки.

Пример — `NYT::NFlow::NExample::TLogParserProcessFunction` из [`examples/cpp/log_parser`]({{source-root}}/yt/yt/flow/examples/cpp/log_parser): разбирает строку лога на записи, эмитит YSON-структуру `TLogRecordMessage` (`level`, `text`, `worst_level_so_far`) и ведёт стейт `TWorstSeverityState` — бегущий максимум severity по партиции источника. Подробнее, вместе с полным исходным кодом, — в разделе [Log Parser](../../../flow/cpp/examples/log_parser.md).

#### TProtoTransformOrderedSourceComputation {#tprototransformorderedsourcecomputation}

Для новой пользовательской логики используйте `TProtoParsingProcessFunctionBase<TProto>` из `yt/yt/flow/library/cpp/parsers/proto.h`. База читает строковую колонку, заданную параметром `processing_function_parameters/data_column` (по умолчанию `"data"`), разбирает её в `TProto` и вызывает `ProcessProto(message, proto, output, context)`. Ошибку чтения или разбора она передаёт в `ProcessUnparsed(message, error, output, context)`, который по умолчанию перебрасывает ошибку дальше.

Стейт храните в `TMutableStateKeyClient<T>` и инициализируйте в `Init`; ключ доступен как `message->Key`. Для materialized ordered-source режима запускайте функцию под `TProcessFunctionTransformOrderedSourceComputation`.

`TProtoTransformOrderedSourceComputation<TProto>` — low-level аналог для сопровождения существующего legacy-кода. Не используйте его как базу нового пользовательского класса.

Пример process function с тем же способом разбора — `NYT::NFlow::NExample::TProtoLogParserFunction` из [`examples/cpp/proto_parser`]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser). Она наследуется от `TProtoParsingProcessFunctionBase<TLogRecordProto>` и запускается под `TProcessFunctionTransformOrderedSourceComputation`: разбирает `TLogRecordProto`, эмитит `TLogRecordMessage` (`level`, `text`, `seen_at_level`) и ведёт стейт `TLevelCountsState` — счётчик записей каждого уровня по партиции источника. Счётчик неидемпотентен к повторной обработке и корректен ровно потому, что стейт коммитится в одной транзакции со смещением `source`. Подробнее — в разделе [Proto Parser](../../../flow/cpp/examples/proto_parser.md).

## TSwiftMapComputation

Swift-map режим реализует детерминированный простой `Map` без материализации результатов в YT. Пользовательскую process function запускайте под `TProcessFunctionSwiftMapComputation`.

Особенности:

- Не поддерживает `sources` и `sinks`.
- Поддерживает `timer_streams` и `key_visitor_streams` только для работы со стейтом: эмит выходных сообщений из обработки таймера или визита запрещён, поэтому output-стримы не могут зависеть от таймер- и visit-стримов в `streams_dependency`.
- Должен возвращать один и тот же результат (включая порядок) для каждой входной строки. Если результат меняется при повторных запусках, могут возникать разные отрицательные эффекты. Возможно, что отдельные части системы обработают разные версии выхода, вплоть до дубликатов, если меняется значение полей для последующих `group-by`.
- Как следствие, у каждого результирующего сообщения должен быть ровно один родитель.

### TSwiftPassthroughComputation

Наследник `TSwiftMapComputation`. Аналогичен `TPassthroughComputation`: просто превращает `input` в `output` путём приведения сообщений к новой схеме.

## TSwiftOrderedSourceComputation

Swift ordered-source режим читает данные из внешних источников и требует, чтобы поток данных из каждого инстанса был упорядочен. Пользовательскую process function запускайте под `TProcessFunctionSourceComputation`.

Особенности:

- Должен быть ровно один `source`
- `Source` должен быть наследником `IOrderedSource`.
- Может использовать `watermark_strategy/event_timestamp_assigner` для назначения `event_timestamp` выходным сообщениям при условии указания колонки. Иначе в качестве `event_timestamp` выходного сообщения будет взят `event_timestamp` сообщения из `source` — то есть время создания исходного сообщения.
- Использует `watermark_strategy/watermark_generator` для оценки [вотермарков](../../../flow/concepts/glossary.md#timestamps-and-watermarks) входных источников.
- Использует `watermark_strategy/watermark_alignment` для выравнивания чтения потока относительно других потоков.
- Позволяет отфильтровать часть событий: сообщение, добавленное в `output` с `distribute=false`, не публикуется, но всё равно учитывается при оценке вотермарка. Это позволяет оценивать вотермарк с использованием полного потока даже в случаях, когда значимая часть потока отфильтровывается.
- `system_timestamp` назначается в момент регистрации сообщения в `output`.
- Надёжно сохраняет часть данных в YT для гарантированного восстановления всей метаинформации. Сами сообщения в YT не сохраняет.
- Может писать в YT «вхолостую», то есть без реальных изменений. Предполагается, что такой поток должен создавать минимальную нагрузку.

### TSwiftPassthroughOrderedSourceComputation

Наследник `TSwiftOrderedSourceComputation`. Аналогичен `TPassthroughComputation`: преобразует `source` в `output` путём приведения сообщений к новой схеме.

## FAQ

### Как настроить Source и Sink? {#source-sink-configuration}

`Source` и `Sink` конфигурируются в спеке `Computation` через секции `sources` и `sinks` соответственно. Каждый `Source`/`Sink` задаётся отдельной подсекцией с указанием типа (например, `TQueueSource`{% if audience == "internal" %}, `TLogbrokerSource`, `TLogbrokerSink`{% endif %}) и параметров подключения.

Подробнее о доступных коннекторах см. раздел [Коннекторы](../../../flow/connectors/about.md).

### Как работает батчинг и партиционирование? {#batching-partitions}

Каждая партиция обрабатывается строго однопоточно. Многопоточность достигается за счёт увеличения числа партиций (`partition_count` в спеке). `IBatchProcessFunction::Process` получает все сообщения и таймеры за текущую [эпоху](../../../flow/concepts/glossary.md#epoch), что позволяет оптимизировать обработку.

Process function API намеренно не предоставляет `PoolInvoker`; распараллеливайте обработку числом партиций.

### Как оценить нагрузку на внутренние таблицы? {#internal-tables-load}

Нагрузка на внутренние таблицы {{product-name}} зависит от типа `Computation` и количества партиций. Ниже приведена приблизительная оценка:

#|
|| **Тип Computation** | **Записей в эпоху на партицию** | **Комментарий** ||
|| `TTransformComputation` | ~2-4 | Запись стейтов + коммит ||
|| `TTransformOrderedSourceComputation` | ~2-4 | Материализация выхода + оффсеты + стейты ||
|| `TSwiftMapComputation` | 0 | Не пишет в YT ||
|| `TSwiftOrderedSourceComputation` | ~1-2 | Метаинформация для восстановления ||
|#

Общая нагрузка = записей на партицию * количество партиций * частота эпох. Для [пайплайна](../../../flow/concepts/glossary.md#pipeline) с 1000 партиций и эпохой в 1 секунду `TTransformComputation` создаст ~2000-4000 записей/с.

## Состояния пайплайна {#pipeline-state}

Возможные состояния пайплайна (тип `EPipelineState`):

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_EPipelineState.md) %}

## См. также

- [Process function (C++)](../../../flow/cpp/process-functions.md)
- [Computation (концепция)](../../../flow/concepts/computation.md)
- [Работа со стейтами (C++)](../../../flow/cpp/state.md)
- [Быстрый старт (C++)](../../../flow/cpp/getting-started.md)
