# Proto Parser в {{product-name}} Flow (C++)

Пример показывает [`TProtoTransformOrderedSourceComputation<TProto>`](../../../../flow/cpp/computation.md#tprototransformorderedsourcecomputation) — хелпер над [`TTransformOrderedSourceComputation`](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation), который берёт на себя разбор `Protobuf`-сообщений из `Source`: [пайплайн]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser) читает сериализованные `Protobuf`-записи лога из очереди, парсит их без ручного вызова `ParseFromStringOrThrow` и ведёт собственный стейт — счётчик разобранных записей каждого уровня.

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser)

## Компоненты пайплайна

### TProtoLogParserComputation

`TProtoLogParserComputation` — наследник `TProtoTransformOrderedSourceComputation<TLogRecordProto>`, где `TLogRecordProto` — `Protobuf`-сообщение с полями `level` и `text`. Базовый класс сам читает колонку `data_column` (по умолчанию `"data"`) сырого сообщения `source`, разбирает её в `TLogRecordProto` и вызывает один из пользовательских хуков:

- `DoProcessProto(const TInputMessageConstPtr& inputMessage, TLogRecordProto&& proto, IOutputCollectorPtr output)` — на успешный разбор: по ключу `inputMessage->Key` получает аксессор стейта, инкрементирует счётчик уровня записи, собирает `TLogRecordMessage` из полей `level` и `text` и текущего значения счётчика и эмитит его в выходной стрим `records`;
- `DoProcessUnparsed(const TInputMessageConstPtr& inputMessage, TError error, IOutputCollectorPtr output)` — значение колонки `data_column` отсутствует (`null`) либо `Protobuf` не разобрался: реализация пустая, такие сообщения молча отбрасываются. Пустая, но присутствующая строка сюда не попадает: у `TLogRecordProto` нет обязательных полей, поэтому она успешно разбирается в сообщение со значениями по умолчанию и обрабатывается в `DoProcessProto` как обычная запись.

Выходной стрим `records` сконфигурирован в спеке компьютейшена `parser` с прямым внешним `TSyncQueueSink` — отдельного sink-компьютейшена не требуется.

### Стейт TLevelCountsState

Стейт `TLevelCountsState` компьютейшен заводит ровно как `TTransformComputation` (см. [Работа со стейтами (C++)](../../../../flow/cpp/state.md#internal-state)): поле `TMutableStateKeyClient<TLevelCountsState> StateClient_`, в `DoInit(IJobInitContextPtr)` — `initContext->InitClient(StateClient_, "level_counts")`, в `DoProcessProto` — аксессор `StateClient_.GetState(inputMessage->Key)`. Стейт хранит `record_counts` — количество разобранных записей каждого уровня по партиции источника; текущее значение попадает в выходное сообщение как `seen_at_level`. Такой стейт неидемпотентен к повторной обработке: при перечитывании источника после рестарта наивный инкремент дал бы задвоенные значения. Он корректен, потому что фреймворк синхронизирует стейт в той же транзакции эпохи, что и продвижение смещения `source` (см. [`TTransformOrderedSourceComputation`](../../../../flow/cpp/computation.md#ttransformorderedsourcecomputation)).

## Типы сообщений

`TLogRecordMessage` — наследник `TYsonMessage` (YSON-структура, регистрируется через `YT_FLOW_DEFINE_YSON_MESSAGE`) с полями:

- `level` — уровень записи, скопированный из поля `level` входного `TLogRecordProto`;
- `text` — текст записи, скопированный из поля `text` входного `TLogRecordProto`;
- `seen_at_level` — сколько записей этого уровня уже разобрано на партиции источника, включая текущую.

## Функция main

В `main` выполняется:
1. `NYT::NFlow::Initialize(argc, argv)` — инициализация библиотеки Flow.
2. `YT_FLOW_DEFINE_COMPUTATION(TProtoLogParserComputation)` — регистрация компьютейшена.
3. `TSimpleSpecBuilder` — билдер для регистрации потоков. Через `RegisterStream<TLogRecordMessage>("records")` регистрируется поток `records` с типом сообщений `TLogRecordMessage`.
4. `TSimpleRunnerProgram` — запуск пайплайна.

## Исходный код

### TProtoLogParserComputation

{% code '/yt/yt/flow/examples/cpp/proto_parser/main.cpp' lang='cpp' %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md#tprototransformorderedsourcecomputation)
- [Computation (концепция)](../../../../flow/concepts/computation.md#ttransformorderedsourcecomputation)
- [Log Parser](../../../../flow/cpp/examples/log_parser.md)
