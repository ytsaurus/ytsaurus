# Log Parser в {{product-name}} Flow (C++)

Пример показывает [`TTransformOrderedSourceComputation`](../../../../flow/concepts/computation.md#ttransformorderedsourcecomputation) (детали в [Computation (C++)](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)): [пайплайн]({{source-root}}/yt/yt/flow/examples/cpp/log_parser) читает строки лога из очереди и парсит их сразу при чтении источника, без промежуточного passthrough-компьютейшена и `TTransformComputation`. Сама логика разбора написана как [process function](../../../../flow/cpp/process-functions.md) и работает под встроенным адаптером. Дополнительно пример показывает чтение из `Source` и собственный durable-стейт, переживающий рестарты (см. [Стейт](#state)).

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/log_parser)

## Компоненты пайплайна

### TLogParserProcessFunction

Пользовательская логика написана как [process function](../../../../flow/cpp/process-functions.md) — наследник `IProcessFunction`, который не зависит от объекта `Computation`, поэтому его можно покрыть юнит-тестами без кластера ([unittest]({{source-root}}/yt/yt/flow/examples/cpp/log_parser/unittest/log_parser_process_function_ut.cpp)). Исполняет её встроенный адаптер `TProcessFunctionTransformOrderedSourceComputation`, он же задаёт режим — ordered source (см. [список адаптеров](../../../../flow/cpp/process-functions.md#how-it-works)).

В `ProcessMessage(const TInputMessageConstPtr& message, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context)` функция читает колонку `line` сырого сообщения `source` через `GetColumnValue<std::string>(message, "line")` и разбирает её с помощью `ParseLogLine` на записи вида `"level:text"`, разделённые `;`, отбрасывая записи без разделителя `:`, с пустым текстом или с уровнем, отличным от `info`, `warning` и `error`. На каждую валидную запись она обновляет стейт через аксессор `StateClient_.GetState(message->Key)` (см. [Стейт](#state)), собирает `TLogRecordMessage` и эмитит его в выходной стрим `records` вызовом `output->AddMessage(context->ConvertToMessage(outputRecord))`.

Результат трансформации — стрим `records` — материализуется в {{product-name}}, как у `TTransformComputation`, поэтому требований к детерминированности трансформации нет: после рестарта Flow дораспределяет уже материализованные сообщения с ранее назначенными им `MessageId`, а не вычисляет их заново.

Наследоваться от класса по-прежнему можно: так написан пример [Proto Parser](../../../../flow/cpp/examples/proto_parser.md) — на хелпере `TProtoTransformOrderedSourceComputation<TProto>` поверх `TTransformOrderedSourceComputation`. Валидатор спеки у адаптера тот же, что у базового класса: непустой `group_by_schema`, таймеры, key-visitor-стримы и `external_state_managers` отвергаются в любом варианте (полный [список ограничений](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)).

### Спека компьютейшена parser

Функцию с адаптером связывают два поля спеки компьютейшена `parser` (см. [Регистрация](../../../../flow/cpp/process-functions.md#registration)):

```yson
"computation_class_name" = "NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation";
"processing_function" = "NYT::NFlow::NExample::TLogParserProcessFunction";
```

Остальные поля записи `parser` описывают подключения: `source_streams.queue` — `TQueueSource` с путями до очереди и консьюмера, `sinks.queue` — прямой внешний `TSyncQueueSink` для стрима `records`, поэтому отдельного sink-компьютейшена не требуется. Полный файл — [pipeline.yson]({{source-root}}/yt/yt/flow/examples/cpp/log_parser/pipeline.yson).

## Типы сообщений

`TLogRecordMessage` — наследник `TYsonMessage` (YSON-структура, регистрируется через `YT_FLOW_DEFINE_YSON_MESSAGE`) с полями:

- `level` — уровень записи (`info`, `warning` или `error`);
- `text` — текст записи;
- `worst_level_so_far` — максимальный по серьёзности уровень (`info < warning < error`), встреченный в этой партиции источника на момент записи (см. [Стейт](#state)).

## Стейт {#state}

`TLogParserProcessFunction` — стейтовая. Стейт `TWorstSeverityState` она держит в поле `TMutableStateKeyClient<TWorstSeverityState> StateClient_` — ровно как `TTransformComputation` (см. [Работа со стейтами (C++)](../../../../flow/cpp/state.md#internal-state)). Остальное делает адаптер `TProcessFunctionTransformOrderedSourceComputation`: он вызывает `Init(const IRuntimeInitContextPtr& initContext)`, где клиент подключается к стейту вызовом `initContext->InitClient(StateClient_, WorstSeverityStateName)` (имя стейта — `worst_severity`), и `ProcessMessage`, где стейт читается аксессором `GetState(message->Key)`, а выходные записи приводятся к сообщениям через `context->ConvertToMessage(...)`.

Инстанс компьютейшена привязан к единственной партиции источника, поэтому все сообщения несут один и тот же ключ и обращаются к одной строке стейта: `state->WorstSeverity = std::max(state->WorstSeverity, SeverityRank(record.Level))`.

Фреймворк синхронизирует этот стейт в той же транзакции эпохи, что и продвижение смещения источника (см. [Computation](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)) — сама функция для этого ничего не делает. Поэтому пользовательский стейт корректен exactly-once и не обязан быть идемпотентен к повторной обработке — обычный, «наивный» счётчик обработанных записей был бы здесь так же корректен. Пример хранит именно бегущий максимум серьёзности просто потому, что это естественная агрегатная величина для такого пайплайна, а не потому, что она чем-то безопаснее счётчика.

## Функция main

В `main` выполняется:
1. `NYT::NFlow::Initialize(argc, argv)` — инициализация библиотеки Flow.
2. `TSimpleSpecBuilder` — билдер для регистрации потоков. Через `RegisterStream<TLogRecordMessage>("records")` регистрируется поток `records` с типом сообщений `TLogRecordMessage`.
3. `TSimpleRunnerProgram(std::move(builder)).Run(argc, argv)` — запуск пайплайна.

Регистрировать функцию в `main` не нужно: макрос `YT_FLOW_DEFINE_PROCESS_FUNCTION(TLogParserProcessFunction)` стоит на файловом уровне в `lib/log_parser_process_function.cpp`, а сам файл подключён к библиотеке как `GLOBAL`, поэтому запись в реестре появляется при инициализации бинаря.

## Исходный код

### TLogParserProcessFunction

{% code '/yt/yt/flow/examples/cpp/log_parser/lib/log_parser_process_function.h' lang='cpp' %}

{% code '/yt/yt/flow/examples/cpp/log_parser/lib/log_parser_process_function.cpp' lang='cpp' %}

### ParseLogLine

{% code '/yt/yt/flow/examples/cpp/log_parser/lib/log_line_parser.cpp' lang='cpp' %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Process function (C++)](../../../../flow/cpp/process-functions.md)
- [Computation (C++)](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)
- [Computation (концепция)](../../../../flow/concepts/computation.md#ttransformorderedsourcecomputation)
- [Работа со стейтами (C++)](../../../../flow/cpp/state.md)
