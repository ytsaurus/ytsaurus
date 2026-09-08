# Retryable Async Request в {{product-name}} Flow (C++)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) аналогичен [Async Request](../../../../flow/cpp/examples/async_request.md), но добавляет логику повторных попыток (ретраев) с использованием [таймеров](../../../../flow/concepts/glossary.md#timer) и внутреннего `YsonState`.

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/retryable_async_request)

## Отличие от Async Request

Ключевое отличие &mdash; `TRequestProcessor` запускается адаптером `TProcessFunctionComputation` (а не `TProcessFunctionSwiftMapComputation`), поскольку ему необходимо:
- хранить внутренний [стейт](../../../../flow/concepts/glossary.md#state) для отслеживания числа неудачных попыток;
- использовать таймеры для повторных попыток через заданный интервал.

## Компоненты пайплайна

### TRequestProcessor

`TRequestProcessor` реализует `IProcessFunction` и использует `TMutableStateKeyClient<TDelayedRequestState>` для хранения внутреннего стейта (Internal YsonState). Стейт инициализируется в `Init(const IRuntimeInitContextPtr& initContext)` через `initContext->InitClient(RequestStateClient_, "request_state")`.

При обработке входного сообщения (`ProcessMessage`):
1. Сохраняет запрос в стейт с `FailedAttempts = 0`
2. Вызывает `TryRequest` для выполнения попытки

Метод `TryRequest` содержит основную логику ретраев:
- Если запрос "не удался" (определяется через `IsRequestSuccessful`), увеличивает счетчик `FailedAttempts` и ставит таймер через `output->AddTimer(GetNextAttempt(context))`
- Если запрос "удался", создает `TResponseMessage`, сбрасывает стейт через `state.Clear()` и отправляет ответ

При срабатывании таймера (`ProcessTimer`) вызывается повторная попытка `TryRequest` с текущим стейтом.

### TStateKeeper

Полностью аналогичен `TStateKeeper` из примера [Async Request](../../../../flow/cpp/examples/async_request.md): принимает входные события и ответы, хранит аккумулированный результат во внешнем стейте.

## Паттерн ретраев

Логика ретраев основана на следующих элементах:

- **TDelayedRequestState** &mdash; наследник `NYTree::TYsonStruct`, хранит `FailedAttempts` и сам `Request`
- **TMutableStateKeyClient** &mdash; клиент для работы с внутренним YsonState. В отличие от `TSimpleExternalStateManager`, стейт хранится во внутренних таблицах Flow, а не во внешней пользовательской таблице
- **Таймеры** &mdash; при неудачной попытке устанавливается таймер с задержкой `Delay` через `output->AddTimer(GetNextAttempt(context))`
- **Константа** `MaxRetries = 3` задаёт период симуляции успешной попытки: запрос выполняется сразу или не более чем после двух ретраев. `Delay = 5` задаёт задержку между попытками

Вычисление времени следующей попытки выполняется через `context->GetCurrentTimestamp()`, что обеспечивает корректную работу с системным временем Flow.

## Структура пайплайна

1. **events** &rarr; `TStateKeeper` &rarr; **request** (генерация запроса)
2. **request** &rarr; `TRequestProcessor` &rarr; **response** (обработка с ретраями)
3. **response** &rarr; `TStateKeeper` &rarr; стейт (накопление результатов)

В [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) для `TRequestProcessor` необходимо зарегистрировать секцию `timer_streams` для поддержки повторных попыток. Класс process function указывается в поле `processing_function`.

## Исходный код

### TRequestProcessor

{% code '/yt/yt/flow/examples/cpp/retryable_async_request/lib/retryable_async_request_functions.cpp' lang='cpp' lines='[BEGIN request_processor]-[END request_processor]' keep-indents %}

### TStateKeeper

{% code '/yt/yt/flow/examples/cpp/retryable_async_request/lib/retryable_async_request_functions.cpp' lang='cpp' lines='[BEGIN state_keeper]-[END state_keeper]' keep-indents %}
