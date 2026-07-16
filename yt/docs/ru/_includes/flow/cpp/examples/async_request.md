# Async Request в {{product-name}} Flow (C++)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) демонстрирует паттерн асинхронных внешних запросов с использованием `TSwiftMapComputation`. События поступают на вход, преобразуются в запросы, обрабатываются детерминированным процессором и результаты накапливаются в [стейте](../../../../flow/concepts/glossary.md#state).

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/async_request)

## Компоненты пайплайна

### TStateKeeper

`TStateKeeper` наследуется от `TTransformComputation` и использует `TSimpleExternalStateManager` для работы с внешним стейтом. Он обрабатывает два вида входных сообщений:

- **Поток `event`**: при получении события создает `TRequestMessage` с уникальным `RequestId` и отправляет его в поток `request`.
- **Поток `response`**: при получении ответа обновляет стейт &mdash; суммирует `total_length` из всех полученных ответов.

Различение потоков происходит через `ysonMessage->Meta->StreamId`.

### TRequestProcessor

`TRequestProcessor` наследуется от `TSwiftMapComputation` &mdash; это детерминированный компьютейшен, который не сохраняет входные и выходные сообщения в {{product-name}}. Он получает `TRequestMessage`, выполняет обработку (в данном примере &mdash; вычисляет длину запроса) и генерирует `TResponseMessage`.

Использование `TSwiftMapComputation` обосновано тем, что обработка запроса является чистой функцией: при одинаковых входных данных всегда генерируется одинаковый результат.

## Типы сообщений

- **TEventMessage** &mdash; наследник `TYsonMessage`. Содержит поля `Key` и `Data`.
- **TRequestMessage** &mdash; наследник `TYsonMessage`. Содержит поля `RequestId`, `Key` и `Request`.
- **TResponseMessage** &mdash; наследник `TYsonMessage`. Содержит поля `RequestId`, `Key` и `Length`.

Все типы сообщений регистрируются через макрос `YT_FLOW_DEFINE_YSON_MESSAGE`.

## Ключевой паттерн: цикл запрос-ответ

Основная идея данного примера &mdash; построение цикла запрос-ответ внутри пайплайна с использованием нескольких потоков:

1. **events** &rarr; `TStateKeeper` &rarr; **request** (генерация запроса)
2. **request** &rarr; `TRequestProcessor` &rarr; **response** (обработка запроса)
3. **response** &rarr; `TStateKeeper` &rarr; стейт (накопление результатов)

`TStateKeeper` одновременно является и потребителем событий, и потребителем ответов. Он использует `input_stream_ids = ["event", "response"]` и определяет тип входного сообщения по `StreamId`.

## Управление стейтом

`TStateKeeper` использует `TSimpleExternalStateManager` для хранения суммы длин всех обработанных запросов. Клиент стейта (`TMutableStateKeyClient<TSimpleExternalState>`) привязывается в `DoInit()` через `InitExternalStateClient(StateClient_, "/state")`. Параметры стейта (`path` к таблице и т.п.) объявляются в секции `external_state_managers` [спеки](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) `Computation`.

## Функция main

В `main` регистрируются три потока:
- `RegisterStream<TEventMessage>("event")` &mdash; входные события
- `RegisterStream<TRequestMessage>("request")` &mdash; запросы к процессору
- `RegisterStream<TResponseMessage>("response")` &mdash; ответы от процессора

## Исходный код

### TRequestProcessor

{% code '/yt/yt/flow/examples/cpp/async_request/main.cpp' lang='cpp' lines='[BEGIN request_processor]-[END request_processor]' keep-indents %}

### TStateKeeper

{% code '/yt/yt/flow/examples/cpp/async_request/main.cpp' lang='cpp' lines='[BEGIN state_keeper]-[END state_keeper]' keep-indents %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)
