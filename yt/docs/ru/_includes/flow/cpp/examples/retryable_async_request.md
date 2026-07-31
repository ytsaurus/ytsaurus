# Retryable Async Request в {{product-name}} Flow (C++)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) аналогичен [Async Request](../../../../flow/cpp/examples/async_request.md), но добавляет логику повторных попыток (ретраев) с использованием [таймеров](../../../../flow/concepts/glossary.md#timer) и внутреннего `YsonState`.

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/retryable_async_request)

## Отличие от Async Request

Ключевое отличие &mdash; `TRequestProcessor` теперь наследуется от `TTransformComputation` (а не от `TSwiftMapComputation`), поскольку ему необходимо:
- хранить внутренний [стейт](../../../../flow/concepts/glossary.md#state) для отслеживания числа неудачных попыток;
- использовать таймеры для повторных попыток через заданный интервал.

## Компоненты пайплайна

### TRequestProcessor

`TRequestProcessor` использует `TMutableStateKeyClient<TDelayedRequestState>` для хранения внутреннего стейта (Internal YsonState). Стейт инициализируется в `DoInit(IJobInitContextPtr initContext)` через `initContext->InitClient<TDelayedRequestState>(RequestStateClient_, "request_state")`.

При обработке входного сообщения (`DoProcessMessage`):
1. Сохраняет запрос в стейт с `FailedAttempts = 0`
2. Вызывает `TryRequest` для выполнения попытки

Метод `TryRequest` содержит основную логику ретраев:
- Если запрос "не удался" (определяется через `IsRequestSucceed`), увеличивает счетчик `FailedAttempts` и ставит таймер через `output->AddTimer(GetNextAttempt())`
- Если запрос "удался", создает `TResponseMessage`, сбрасывает стейт через `state.Clear()` и отправляет ответ

При срабатывании таймера (`DoProcessTimer`) вызывается повторная попытка `TryRequest` с текущим стейтом.

### TStateKeeper

Полностью аналогичен `TStateKeeper` из примера [Async Request](../../../../flow/cpp/examples/async_request.md): принимает входные события и ответы, хранит аккумулированный результат во внешнем стейте.

## Паттерн ретраев

Логика ретраев основана на следующих элементах:

- **TDelayedRequestState** &mdash; наследник `NYTree::TYsonStruct`, хранит `FailedAttempts` и сам `Request`
- **TMutableStateKeyClient** &mdash; клиент для работы с внутренним YsonState. В отличие от `TSimpleExternalStateManager`, стейт хранится во внутренних таблицах Flow, а не во внешней пользовательской таблице
- **Таймеры** &mdash; при неудачной попытке устанавливается таймер с задержкой `Delay` через `output->AddTimer(GetNextAttempt())`
- **Константы** `MaxRetries = 3` и `Delay = 5` задают максимальное число повторных попыток и задержку между ними

Вычисление времени следующей попытки выполняется через `GetEpochWatermarkState()->GetCurrentTimestamp()`, что обеспечивает корректную работу с системным временем Flow.

## Структура пайплайна

1. **events** &rarr; `TStateKeeper` &rarr; **request** (генерация запроса)
2. **request** &rarr; `TRequestProcessor` &rarr; **response** (обработка с ретраями)
3. **response** &rarr; `TStateKeeper` &rarr; стейт (накопление результатов)

В [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) для `TRequestProcessor` необходимо зарегистрировать секцию `timers` для поддержки повторных попыток.

## Исходный код

### TRequestProcessor

{% code '/yt/yt/flow/examples/cpp/retryable_async_request/main.cpp' lang='cpp' lines='[BEGIN request_processor]-[END request_processor]' keep-indents %}

### TStateKeeper

{% code '/yt/yt/flow/examples/cpp/retryable_async_request/main.cpp' lang='cpp' lines='[BEGIN state_keeper]-[END state_keeper]' keep-indents %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Таймеры](../../../../flow/concepts/timers.md)
- [Пример: асинхронные запросы (C++)](../../../../flow/cpp/examples/async_request.md)
