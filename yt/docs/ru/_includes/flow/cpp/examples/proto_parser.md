# Proto Parser в {{product-name}} Flow (C++)

Пример показывает `TProtoParsingProcessFunctionBase<TProto>` — базовый класс для [process functions](../../../../flow/cpp/process-functions.md), который берёт на себя разбор `Protobuf`-сообщений. [Пайплайн]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser) читает сериализованные записи лога из очереди, парсит их без ручного вызова `ParseFromStringOrThrow` и ведёт стейт — счётчик разобранных записей каждого уровня.

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser)

## Компоненты пайплайна

### TProtoLogParserFunction

`TProtoLogParserFunction` наследуется от `TProtoParsingProcessFunctionBase<TLogRecordProto>`, где `TLogRecordProto` — `Protobuf`-сообщение с полями `level` и `text`. Базовый класс читает колонку входного сообщения, имя которой задано параметром `data_column` (по умолчанию `"data"`), разбирает её в `TLogRecordProto` и вызывает один из пользовательских методов:

- `ProcessProto(...)` — при успешном разборе получает стейт по ключу сообщения, увеличивает счётчик уровня, собирает `TLogRecordMessage` и отправляет его в поток `records`;
- `ProcessUnparsed(...)` — если колонка входного сообщения, имя которой задано параметром `data_column`, отсутствует (`null`) или `Protobuf` не разобрался. Реализация пустая, поэтому такие сообщения молча отбрасываются. Пустая, но присутствующая строка успешно разбирается в `TLogRecordProto` со значениями по умолчанию, поскольку в сообщении нет обязательных полей.

Process function регистрируется через `YT_FLOW_DEFINE_PROCESS_FUNCTION`. В спеке `parser` запускает её адаптер `TProcessFunctionTransformOrderedSourceComputation`, а поле `processing_function` содержит имя `NYT::NFlow::NExample::TProtoLogParserFunction`. При необходимости имя входной колонки можно передать через `processing_function_parameters/data_column`.

Выходной поток `records` сконфигурирован с прямым внешним `TSyncQueueSink`, поэтому отдельный sink-компьютейшен не требуется.

### Стейт TLevelCountsState

`TProtoLogParserFunction` хранит `TMutableStateKeyClient<TLevelCountsState> StateClient_` и инициализирует его в `DoInit(const IRuntimeInitContextPtr&)` через `initContext->InitClient(StateClient_, "level_counts")`. Метод `ProcessProto` получает стейт через `StateClient_.GetState(message->Key)`. Стейт содержит `record_counts` — количество разобранных записей каждого уровня по партиции источника; текущее значение попадает в выходное сообщение как `seen_at_level`.

Адаптер `TProcessFunctionTransformOrderedSourceComputation` материализует выход и сохраняет стейт в той же транзакции эпохи, в которой продвигается смещение источника. Поэтому инкремент счётчика остаётся согласованным с чтением очереди при рестартах.

## Типы сообщений

`TLogRecordMessage` — наследник `TYsonMessage`, зарегистрированный через `YT_FLOW_DEFINE_YSON_MESSAGE`, с полями:

- `level` — уровень записи из `TLogRecordProto`;
- `text` — текст записи из `TLogRecordProto`;
- `seen_at_level` — число разобранных записей этого уровня на партиции источника, включая текущую.

## Функция main

Регистрация process function и типов вынесена в библиотеку примера. В `main` выполняется инициализация Flow, регистрация потока `records` через `TSimpleSpecBuilder` и запуск `TSimpleRunnerProgram`.

## Исходный код

### TProtoLogParserFunction

{% code '/yt/yt/flow/examples/cpp/proto_parser/lib/proto_parser_function.h' lang='cpp' %}

{% code '/yt/yt/flow/examples/cpp/proto_parser/lib/proto_parser_function.cpp' lang='cpp' %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Process functions (C++)](../../../../flow/cpp/process-functions.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)
- [Log Parser](../../../../flow/cpp/examples/log_parser.md)
